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


if __name__ == "__main__":
    unittest.main()
