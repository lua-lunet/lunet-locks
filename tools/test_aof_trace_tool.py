# The aof-trace-tool tests: fixture round-trip, envelope parsing, summary
# counts, and the acceptance numbers from the recorded nine-node traces.
import importlib.util
import json
import struct
import sys
import unittest
from pathlib import Path

_spec = importlib.util.spec_from_file_location(
    "aof_trace_tool", Path(__file__).parent / "aof-trace-tool.py")
tool = importlib.util.module_from_spec(_spec)
sys.modules["aof_trace_tool"] = tool
_spec.loader.exec_module(tool)

LIB = Path(__file__).parent.parent / (
    "ext/lunet-locks-aof/zig/zig-out/lib/liblunet_locks_aof.dylib")

REAL_TRACE = (Path(__file__).parent.parent / (
    ".tmp/telemetry/nine-node-2026-09-12/w1b/aof"))


def envelope(marker: int, ns: int, payload: bytes) -> bytes:
    return bytes([marker]) + struct.pack(">Q", ns) + payload


def hhmmss(ms):
    # Independent reference for t_fmt: divmod over the UTC day portion.
    secs = ms // 1000
    hh, rem = divmod(secs % 86400, 3600)
    mm, ss = divmod(rem, 60)
    return "%02d:%02d:%02d.%03d" % (hh, mm, ss, ms % 1000)


def wire(tag: int, era: int, view: int, slot: int, disc: int = 9) -> bytes:
    # marker-1 payload: 20 B BE header + body discriminant
    return struct.pack(">IIIQ", tag, era, view, slot) + bytes([disc]) + b"\0" * 8


class FixtureRoundTrip(unittest.TestCase):
    def test_write_then_read_the_full_series(self):
        with tool.FixtureDir() as d:
            path = d / "fixture.aof"
            f = tool.open_writer(path, LIB)
            tool.append(f, envelope(2, 1_000, b'{"phi":1.5}'))
            tool.append(f, envelope(1, 2_000, wire(4, 1, 0, 5)))
            tool.append(f, envelope(3, 3_000, b'{"event":"teardown"}'))
            tool.append(f, envelope(2, 4_000, b'{"phi":2.5}'))
            tool.append(f, envelope(1, 5_000, wire(2, 1, 0, 6)))
            tool.close_writer(f)
            s = tool.summarize(path, LIB)
            self.assertEqual(s["records"], 5)
            self.assertEqual(s["by_marker"], {"1": 2, "2": 2, "3": 1})
            self.assertEqual(s["tags"], {"4 Commit": 1, "2 Prepare": 1})
            self.assertEqual(s["first_ns"], 1_000)
            self.assertEqual(s["last_ns"], 5_000)
            self.assertEqual(s["malformed"], 0)


class EnvelopeParsing(unittest.TestCase):
    def test_ns_header_is_big_endian(self):
        self.assertEqual(tool.ns_of(envelope(1, 0x1122334455667788, b"")), 0x1122334455667788)

    def test_unknown_marker_counts_as_malformed(self):
        with tool.FixtureDir() as d:
            path = d / "fixture.aof"
            f = tool.open_writer(path, LIB)
            tool.append(f, bytes([9]) + struct.pack(">Q", 10) + b"junk")
            tool.close(f)
            s = tool.summarize(path, LIB)
            self.assertEqual(s["records"], 1)
            self.assertEqual(s["malformed"], 1, "unknown markers rejected, never guessed")


class AcceptanceRealTrace(unittest.TestCase):
    def test_all_records_of_the_recorded_standby_series_are_readable(self):
        if not REAL_TRACE.exists():
            self.skipTest("real rig traces not extracted")
        s = tool.summarize_series(REAL_TRACE, LIB)
        # The upstream reader's measured counts for w1b (two files):
        # 49,832 records = 33,285 wire + 16,547 telemetry.
        self.assertEqual(s["records"], 49_832, s)
        self.assertEqual(s["by_marker"].get("1"), 33_285)
        self.assertEqual(s["by_marker"].get("2", 0) + s["by_marker"].get("3", 0)
                         + s["by_marker"].get("4", 0), 16_547)
        self.assertEqual(s["malformed"], 0)

    def test_summary_json_is_the_parity_contract(self):
        if not REAL_TRACE.exists():
            self.skipTest("real rig traces not extracted")
        py = json.loads(tool.summarize_series_json(REAL_TRACE, LIB))
        self.assertIn("records", py)
        self.assertIn("by_marker", py)
        self.assertIn("first_ns", py)
        self.assertIn("last_ns", py)
        self.assertIn("span_s", py)


class ExportMode(unittest.TestCase):
    def setUp(self):
        d = tool.FixtureDir()
        self.dir = d
        self.addCleanup(d.cleanup)
        path = d / "fixture.aof"
        f = tool.open_writer(path, LIB)
        # A Set at slot 5, then a Commit closing it, trailer attached
        # (the leader's send clock rides every leader Commit).
        set_op = ('{"op":"set","message_id":"11111111-2222-3333-4444-555555555555",'
                  '"client_id":1,"request_num":1,"lock_id":7}')
        prepare = (struct.pack(">IIIQ", 2, 4, 1, 5)  # Prepare, era 4, view 1, slot 5
                   + bytes([2])          # body disc = Prepare
                   + struct.pack(">Q", 5)   # LogEntry.slot
                   + struct.pack(">I", 4)   # LogEntry.era
                   + bytes([1])              # payload disc = Operation
                   + struct.pack(">QQ", 0x1111, 0x2222)  # op id msb/lsb
                   + struct.pack(">I", len(set_op)) + set_op.encode()
                   + struct.pack(">Q", 0))   # committed frontier = 0
        commit = (struct.pack(">IIIQ", 4, 4, 1, 5)
                  + bytes([4])
                  + struct.pack(">Q", 5))
        trailer = b"\xc0\x0b" + struct.pack("<IIIQ", 4, 33, 9, 1789214915000)
        tool.append(f, envelope(1, 2000, prepare))
        tool.append(f, envelope(1, 2500, commit + trailer))
        tool.append(f, envelope(2, 3000, b'{"phi":1.7,"now_ms":100,"prev_wait_ms":900,"next_wait_ms":1000,"leader":33,"era":4,"view":1,"mean":22.0}'))
        tool.append(f, envelope(5, 4000, b'{"node":88,"era":4,"leader":33,"addr":"127.0.0.1:1","dt_ms":22,"ts_ms":1789214915000,"phi":0.123,"sent_at_ms":1789214915000}'))
        # a Join reconfig: System payload disc 2, op disc 7, node 77, pos 255
        join = (struct.pack(">IIIQ", 2, 4, 1, 6)
                + bytes([2])
                + struct.pack(">Q", 6)
                + struct.pack(">I", 4)
                + bytes([2, 7])
                + struct.pack(">I", 77)
                + struct.pack(">I", 255)
                + struct.pack(">Q", 0))
        tool.append(f, envelope(1, 5000, join))
        # the Join commits: a Commit covering slot 6 with its own trailer
        commit6 = (struct.pack(">IIIQ", 4, 4, 1, 6) + bytes([4])
                   + struct.pack(">Q", 6))
        tool.append(f, envelope(1, 5500, commit6 + trailer))
        tool.close(f)

    def test_export_all_kinds(self):
        out = tool.export_series(self.dir, LIB, kinds="all")
        kinds = [line["kind"] for line in out]
        self.assertEqual(kinds, ["locks", "phi", "phi-samples", "reconfig"])
        locks = [l for l in out if l["kind"] == "locks"][0]
        self.assertEqual(locks["op"], "set")
        self.assertEqual(locks["lock_id"], 7)
        self.assertEqual(locks["result"], "committed")
        self.assertEqual(locks["leader_ms"], 1789214915000)
        self.assertEqual(locks["aof_ns"], 2500, "the commit observation ns")
        reconf = [l for l in out if l["kind"] == "reconfig"][0]
        self.assertEqual(reconf["command"], "Join node=77 position=255")
        self.assertEqual(reconf["leader_ms"], 1789214915000)

    def test_marker5_sent_at_ms_round_trips_into_the_export(self):
        out = tool.export_series(self.dir, LIB, kinds="phi-samples")
        self.assertEqual(len(out), 1)
        sample = out[0]
        self.assertEqual(sample["node"], 88)
        self.assertEqual(sample["dt_ms"], 22)
        self.assertEqual(sample["phi"], 0.123)
        self.assertEqual(sample["sent_at_ms"], 1789214915000)

    def test_export_filters_one_kind(self):
        out = tool.export_series(self.dir, LIB, kinds="locks")
        self.assertEqual([l["kind"] for l in out], ["locks"])
        out = tool.export_series(self.dir, LIB, kinds="phi")
        self.assertEqual([l["kind"] for l in out], ["phi"])
        out = tool.export_series(self.dir, LIB, kinds="phi-samples")
        self.assertEqual([l["kind"] for l in out], ["phi-samples"])

    def test_both_means_reconfig_and_locks(self):
        out = tool.export_series(self.dir, LIB, kinds="both")
        self.assertEqual([l["kind"] for l in out], ["locks", "reconfig"])


class AnchoredTimelineExport(unittest.TestCase):
    # item02: --anchor, locks-timeline + takeover summaries, hb-spacing,
    # aof-noise. One rich fixture: lock ops on one lock (holder churn
    # 1 → 1 → break → 2 → 3), leader trailers with a sent_at_ms series,
    # marker-5 samples with dt_ms, and a ns arrival stall (9 ms gap).
    BASE_MS = 1789214915000
    BASE_NS = BASE_MS * 1_000_000

    @staticmethod
    def operation_prepare(slot, era, view, lock_json, committed=0):
        return (struct.pack(">IIIQ", 2, era, view, slot)
                + bytes([2])
                + struct.pack(">Q", slot)
                + struct.pack(">I", era)
                + bytes([1])
                + struct.pack(">QQ", slot, slot)
                + struct.pack(">I", len(lock_json)) + lock_json.encode()
                + struct.pack(">Q", committed))

    @staticmethod
    def commit_frame(slot, era, view, committed):
        return (struct.pack(">IIIQ", 4, era, view, slot)
                + bytes([4])
                + struct.pack(">Q", committed))

    @staticmethod
    def trailer(era, leader, seq, sent_at):
        return b"\xc0\x0b" + struct.pack("<IIIQ", era, leader, seq, sent_at)

    def setUp(self):
        B = self.BASE_NS
        d = tool.FixtureDir()
        self.dir, self.addCleanup = d, d.cleanup
        path = d / "fixture.aof"

        def lock_json(op, holder):
            mid = ("23456789-1234-5678-9123-67890123456{%d}" % holder)[:36]
            return ('{"op":"%s","message_id":"%s","client_id":%d,'
                    '"request_num":1,"lock_id":7}') % (op, mid, holder)

        f = tool.open_writer(path, LIB)
        # The wire trace, in ns order. The 4th delta is 9 ms: the stall.
        writes = [
            (B + 0, lambda: self.operation_prepare(5, 4, 1, lock_json("set", 1))),
            (B + 1_000_000, lambda: (self.commit_frame(5, 4, 1, 5)
                                     + self.trailer(4, 33, 1, self.BASE_MS))),
            (B + 2_000_000, lambda: self.operation_prepare(6, 4, 1, lock_json("set", 1))),
            (B + 3_000_000, lambda: (self.commit_frame(6, 4, 1, 6)
                                     + self.trailer(4, 33, 2, self.BASE_MS + 20))),
            (B + 12_000_000, lambda: self.operation_prepare(7, 4, 1, lock_json("break", 99))),
            (B + 13_000_000, lambda: (self.commit_frame(7, 4, 1, 7)
                                      + self.trailer(4, 33, 3, self.BASE_MS + 45))),
            (B + 14_000_000, lambda: self.operation_prepare(8, 4, 1, lock_json("set", 2))),
            (B + 15_000_000, lambda: (self.commit_frame(8, 4, 1, 8)
                                      + self.trailer(4, 33, 4, self.BASE_MS + 65))),
            (B + 16_000_000, lambda: self.operation_prepare(9, 4, 1, lock_json("set", 3))),
            (B + 17_000_000, lambda: (self.commit_frame(9, 4, 1, 9)
                                      + self.trailer(4, 33, 5, self.BASE_MS + 80)))]

        for ns, build in writes:
            tool.append(f, envelope(1, ns, build()))
        tool.close(f)
        # Telemetry in a later-sorted file keeps file order = ns order
        # across the dir (the export's ordering contract).
        f2 = tool.open_writer(d / "g.aof", LIB)
        tool.append(f2, envelope(2, B + 18_000_000, b'{"phi":1.7,"now_ms":100,'
                                 b'"prev_wait_ms":900,"next_wait_ms":1000,'
                                 b'"leader":33,"era":4,"view":1,"mean":22.0}'))
        tool.append(f2, envelope(5, B + 19_000_000,
                                 b'{"node":88,"era":4,"leader":33,'
                                 b'"addr":"127.0.0.1:1","dt_ms":21,"ts_ms":'
                                 + str(self.BASE_MS).encode() + b',"phi":0.123,'
                                 b'"sent_at_ms":' + str(self.BASE_MS).encode() + b"}"))
        tool.append(f2, envelope(5, B + 20_000_000,
                                 b'{"node":88,"era":4,"leader":33,'
                                 b'"addr":"127.0.0.1:1","dt_ms":23,"ts_ms":'
                                 + str(self.BASE_MS + 20_000).encode() + b',"phi":0.223,'
                                 b'"sent_at_ms":' + str(self.BASE_MS).encode() + b"}"))
        tool.close(f2)
        self.A1 = self.BASE_MS + 3        # the renew's commit ts: last sign of holder 1
        self.A2 = self.BASE_MS + 15       # next holder's acquire commit ts

    def test_anchors_add_time_columns_to_every_kind(self):
        out = tool.export_series(self.dir, LIB, kinds="locks",
                                 anchors=[self.A2, self.A1])
        locks = [l for l in out if l["kind"] == "locks"]
        first = locks[0]  # the acquire at ns BASE+1e6
        # No anchor is <= ts(first); the first anchor is authoritative.
        self.assertEqual(first["t_rel_ms"], -2.0)
        self.assertEqual(first["t_fmt"], hhmmss(self.BASE_MS + 1))
        tl = tool.export_series(self.dir, LIB, kinds="phi-samples",
                                anchors=[self.A1])
        sample = [l for l in tl if l["kind"] == "phi-samples"][0]
        self.assertEqual(sample["t_rel_ms"], -3.0,
                         "sample ts_ms rides its payload, not aof ns")
        self.assertEqual(sample["t_fmt"], hhmmss(self.BASE_MS))
        ph = tool.export_series(self.dir, LIB, kinds="phi", anchors=[self.A1])
        phi = [l for l in ph if l["kind"] == "phi"][0]
        self.assertEqual(phi["t_rel_ms"], 100.0 - self.A1,
                         "marker-2 ts falls back to its payload now_ms")
        self.assertEqual(phi["t_fmt"], hhmmss(100))

    def test_timeline_rows_ops_holders_and_ts(self):
        out = tool.export_series(self.dir, LIB, kinds="locks-timeline")
        self.assertEqual([l["kind"] for l in out],
                         ["locks-timeline"] * 5)
        self.assertEqual([l["op"] for l in out],
                         ["acquire", "renew", "break", "acquire", "acquire"],
                         "same holder re-set = renew; break clears; new holder = acquire")
        self.assertEqual([l["holder"] for l in out], [1, 1, 99, 2, 3])
        self.assertEqual([l["lock_id"] for l in out], [7] * 5)
        self.assertEqual([l["ts_ms"] for l in out],
                         [self.BASE_MS + k for k in (1, 3, 13, 15, 17)])
        self.assertEqual([l["aof_ns"] for l in out],
                         [self.BASE_NS + k * 1_000_000 for k in (1, 3, 13, 15, 17)])
        self.assertNotIn("t_rel_ms", out[0], "no anchor, no relative column")

    def test_takeover_summaries_per_anchor_as_export_lines(self):
        out = tool.export_series(self.dir, LIB, kinds="locks-timeline",
                                 anchors=[self.A1, self.A2])
        kinds = [l["kind"] for l in out]
        self.assertEqual(kinds[0], "anchor", "anchor echoes come first")
        self.assertEqual(kinds[1], "anchor")
        self.assertEqual(len(kinds), 9, "2 anchors + 5 timeline + 2 takeovers")
        a1, a2, *rest = out
        self.assertEqual(a1["anchor"], self.A1)
        self.assertEqual(a1["t_fmt"], hhmmss(self.A1))
        self.assertEqual(a2["t_fmt"], hhmmss(self.A2))
        tak = [l for l in out if l["kind"] == "takeover"]
        self.assertEqual([t["anchor"] for t in tak], [self.A1, self.A2])
        # Anchor 1: holder 1 renewed at the anchor, then the break; the
        # next acquire belongs to the NEXT anchor's window -> absent.
        self.assertEqual(tak[0], {"kind": "takeover", "anchor": self.A1,
                                  "last_holder": 1,
                                  "last_renew_t_rel": 0.0,
                                  "break_seen_t_rel": 10.0,
                                  "next_acquire_t_rel": "absent",
                                  "next_holder": "absent",
                                  "takeover_ms": "absent"})
        # Anchor 2: holder 2 acquired at the anchor, holder 3 at +1 ms
        # (no break row in the window -> the handover is the signal).
        self.assertEqual(tak[1], {"kind": "takeover", "anchor": self.A2,
                                  "last_holder": 2,
                                  "last_renew_t_rel": 0.0,
                                  "break_seen_t_rel": "absent",
                                  "next_acquire_t_rel": 2.0,
                                  "next_holder": 3,
                                  "takeover_ms": 2.0})

    def test_takeover_absent_when_nothing_in_window(self):
        out = tool.export_series(self.dir, LIB, kinds="locks-timeline",
                                 anchors=[self.BASE_MS + 50])
        tak = [l for l in out if l["kind"] == "takeover"]
        self.assertEqual(tak[0], {"kind": "takeover",
                                  "anchor": self.BASE_MS + 50,
                                  "last_holder": "absent",
                                  "last_renew_t_rel": "absent",
                                  "break_seen_t_rel": "absent",
                                  "next_acquire_t_rel": "absent",
                                  "next_holder": "absent",
                                  "takeover_ms": "absent"})

    def test_hb_spacing_stats_per_era_leader(self):
        out = tool.export_series(self.dir, LIB, kinds="hb-spacing")
        wire = [l for l in out if l["source"] == "wire"]
        sample = [l for l in out if l["source"] == "sample"]
        self.assertEqual(len(wire), 1)
        self.assertEqual(len(sample), 1)
        w = wire[0]
        self.assertEqual(w["kind"], "hb-spacing")
        self.assertEqual(w["era"], 4)
        self.assertEqual(w["leader"], 33)
        # sent_at gaps 20, 25, 20, 15 ms in trace order
        self.assertEqual(w["min_ms"], 15.0)
        self.assertEqual(w["mean_ms"], 20.0)
        self.assertEqual(w["p50_ms"], 20.0)
        self.assertEqual(w["p95_ms"], 25.0)
        self.assertEqual(w["max_ms"], 25.0)
        self.assertEqual(w["count"], 4)
        s = sample[0]
        self.assertEqual(s["era"], 4)
        self.assertEqual(s["leader"], 33)
        self.assertEqual(s["count"], 2)
        self.assertEqual(s["min_ms"], 21.0)
        self.assertEqual(s["mean_ms"], 22.0)
        self.assertEqual(s["max_ms"], 23.0)

    def test_aof_noise_stats_and_stalls(self):
        out = tool.export_series(self.dir, LIB, kinds="aof-noise")
        self.assertEqual(len(out), 1)
        row = out[0]
        self.assertEqual(row["kind"], "aof-noise")
        self.assertEqual(row["gaps"]["count"], 9)
        self.assertEqual(row["gaps"]["min_ms"], 1.0)
        self.assertEqual(row["gaps"]["max_ms"], 9.0)
        self.assertEqual(row["gaps"]["p50_ms"], 1.0)
        self.assertEqual(row["gaps"]["p95_ms"], 9.0)
        self.assertEqual(row["gaps"]["mean_ms"], 17.0 / 9.0)
        self.assertEqual(row["samples"]["count"], 2)
        self.assertEqual(row["samples"]["mean_ms"], 22.0)
        # threshold min(hb_ms=5, 5 * median 1 ms) = 5 ms; only the 9 ms gap stalls
        self.assertEqual(row["stalls"],
                         [{"a_ns": self.BASE_NS + 3_000_000,
                           "b_ns": self.BASE_NS + 12_000_000,
                           "gap_ms": 9.0}])

    def test_no_anchors_keeps_existing_rows_unchanged(self):
        out = tool.export_series(self.dir, LIB, kinds="locks")
        for row in out:
            self.assertNotIn("t_rel_ms", row)
            self.assertNotIn("t_fmt", row)


class CliAnchors(unittest.TestCase):
    def test_main_writes_anchor_echo_and_summary_lines(self):
        with tool.FixtureDir() as d:
            base_ms = AnchoredTimelineExport.BASE_MS
            path = d / "fixture.aof"
            f = tool.open_writer(path, LIB)
            tool.append(f, envelope(1, base_ms * 1_000_000 + 5_000_000,
                                    (AnchoredTimelineExport.commit_frame(5, 4, 1, 5)
                                     + AnchoredTimelineExport.trailer(4, 33, 1, base_ms))))
            tool.close(f)
            out_path = d / "out.jsonl"
            rc = tool.main(["prog", "--lib", str(LIB), "--anchor", str(base_ms + 1),
                            "--kind", "locks-timeline", str(d), str(out_path)])
            self.assertEqual(rc, 0)
            lines = [json.loads(line) for line in out_path.read_text().splitlines()]
            self.assertEqual(lines[0]["kind"], "anchor")
            self.assertEqual(lines[0]["anchor"], base_ms + 1)
            self.assertEqual(lines[0]["t_fmt"], hhmmss(base_ms + 1))


if __name__ == "__main__":
    unittest.main()
