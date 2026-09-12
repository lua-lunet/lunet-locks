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
        tool.append(f, envelope(5, 4000, b'{"node":88,"era":4,"leader":33,"addr":"127.0.0.1:1","dt_ms":22,"ts_ms":1789214915000}'))
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


if __name__ == "__main__":
    unittest.main()
