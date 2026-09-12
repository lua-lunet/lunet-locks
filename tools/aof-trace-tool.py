# aof-trace-tool.py — the reference twin of the LuaJIT tool: read the
# standby AOF traces (vendored TigerBeetle 0.17.9 format) through the
# C ABI, one envelope per record. Python is the high-memory reference;
# the Teal/LuaJIT tool is the lean twin and their summaries must have
# PARITY (identical JSON). Reference sketch: the upstream reader gist
# (handover 337b313e, aof-reader-tool-sketch.md).
import ctypes
import json
import os
import struct
import sys
import tempfile
from pathlib import Path

RECORD_MAX = (1024 * 1024) - 256

# The envelope markers (item22 M1). Unknown markers are rejected, never guessed.
MARKERS = {1: "Wire", 2: "TelemetryTimeoutDecision", 3: "TelemetryStateTransition",
           4: "TelemetryOutbound", 5: "TelemetryIntervalSample"}

# The uVRR wire tags (marker-1 payload header, big-endian).
TAGS = {2: "2 Prepare", 3: "3 PrepareOk", 4: "4 Commit", 5: "5 StartViewChange",
        6: "6 DoViewChange", 7: "7 StartView", 8: "8 PlannedViewChange",
        9: "9 GetState", 10: "10 NewState", 13: "13 Reincarnation"}

DEFAULT_LIB = Path(__file__).parent.parent / (
    "ext/lunet-locks-aof/zig/zig-out/lib/liblunet_locks_aof.dylib")


class FixtureDir(tempfile.TemporaryDirectory):
    # A test-fixture directory that keeps its Path form for `/` joining.
    # (Python 3.13's TemporaryDirectory.__enter__ returns the NAME string.)
    def __enter__(self) -> "FixtureDir":
        return self

    @property
    def path(self) -> Path:
        return Path(self.name)

    def __truediv__(self, name: str) -> Path:
        return self.path / name


def as_lib(lib_or_path=None):
    # Accept a loaded CDLL, a lib path, or None (default discovery).
    if lib_or_path is None or isinstance(lib_or_path, (str, Path)):
        return load(lib_or_path)
    return lib_or_path


def load(lib_path=None):
    lib = ctypes.CDLL(str(lib_path or os.environ.get("AOF_LIB") or DEFAULT_LIB))
    lib.lunet_aof_iter_open.argtypes = [ctypes.c_char_p, ctypes.c_size_t,
                                        ctypes.POINTER(ctypes.c_void_p)]
    lib.lunet_aof_iter_next.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                        ctypes.c_size_t,
                                        ctypes.POINTER(ctypes.c_size_t),
                                        ctypes.POINTER(ctypes.c_uint64)]
    lib.lunet_aof_iter_next.restype = ctypes.c_int32
    lib.lunet_aof_iter_close.argtypes = [ctypes.c_void_p]
    lib.lunet_aof_open.argtypes = [ctypes.c_char_p, ctypes.c_size_t,
                                   ctypes.c_uint8, ctypes.POINTER(ctypes.c_void_p)]
    lib.lunet_aof_append.argtypes = [ctypes.c_void_p, ctypes.c_char_p,
                                     ctypes.c_size_t, ctypes.POINTER(ctypes.c_uint64)]
    lib.lunet_aof_close.argtypes = [ctypes.c_void_p]
    return lib


def open_writer(path, lib=None, force=False):
    lib = as_lib(lib)
    h = ctypes.c_void_p()
    p = str(path).encode()
    rc = lib.lunet_aof_open(p, len(p), 1 if force else 0, ctypes.byref(h))
    if rc != 0:
        raise IOError(f"lunet_aof_open rc={rc}")
    return h


def append(handle, data, lib=None):
    lib = as_lib(lib)
    op = ctypes.c_uint64(0)
    rc = lib.lunet_aof_append(handle, data, len(data), ctypes.byref(op))
    if rc != 0:
        raise IOError(f"lunet_aof_append rc={rc}")
    return op.value


def close_writer(handle, lib=None):
    as_lib(lib).lunet_aof_close(handle)


# The test fixture lives and dies inside a context; production readers are
# read-only and never touch the writer surface.
close = close_writer


def iterate(path, lib=None):
    # Stream: one reused buffer, a tuple per record yielded as read — the
    # caller keeps only what the analysis needs.
    lib = as_lib(lib)
    it = ctypes.c_void_p()
    p = str(path).encode()
    rc = lib.lunet_aof_iter_open(p, len(p), ctypes.byref(it))
    if rc != 0:
        raise IOError(f"lunet_aof_iter_open rc={rc}")
    buf = ctypes.create_string_buffer(RECORD_MAX)
    n, op = ctypes.c_size_t(0), ctypes.c_uint64(0)
    try:
        while True:
            rc = lib.lunet_aof_iter_next(it, buf, RECORD_MAX,
                                         ctypes.byref(n), ctypes.byref(op))
            if rc <= 0:  # 0 = EOF or torn tail; < 0 = error code
                break
            rec = buf.raw[: n.value]
            yield op.value, rec
    finally:
        lib.lunet_aof_iter_close(it)


def ns_of(record: bytes) -> int:
    return struct.unpack(">Q", record[1:9])[0]


def tag_of(record: bytes):
    if record[0] != 1 or len(record) < 21:
        return None
    tag = struct.unpack(">I", record[9:13])[0]
    return TAGS.get(tag)


def summarize(path, lib=None):
    lib = as_lib(lib)
    by_marker: dict = {}
    tags: dict = {}
    first_ns = last_ns = None
    records = malformed = 0
    for _op, rec in iterate(path, lib):
        records += 1
        marker = rec[0]
        if marker not in MARKERS or len(rec) < 9:
            malformed += 1
            continue
        key = str(marker)
        by_marker[key] = by_marker.get(key, 0) + 1
        ns = ns_of(rec)
        first_ns = ns if first_ns is None else min(first_ns, ns)
        last_ns = ns if last_ns is None else max(last_ns, ns)
        if marker == 1:
            t = tag_of(rec)
            if t:
                tags[t] = tags.get(t, 0) + 1
            else:
                malformed += 1
    span_s = round((last_ns - first_ns) / 1e9, 6) if first_ns is not None else 0.0
    return {"records": records, "by_marker": by_marker, "tags": tags,
            "first_ns": first_ns, "last_ns": last_ns, "span_s": span_s,
            "malformed": malformed}


def summarize_series(dir_path, lib=None):
    dir_path = Path(dir_path)
    files = sorted(dir_path.glob("*.aof"))
    total = {"files": len(files), "records": 0, "by_marker": {}, "tags": {},
             "first_ns": None, "last_ns": None, "span_s": 0.0, "malformed": 0}
    for f in files:
        s = summarize(f, lib)
        total["records"] += s["records"]
        total["malformed"] += s["malformed"]
        for k, v in s["by_marker"].items():
            total["by_marker"][k] = total["by_marker"].get(k, 0) + v
        for k, v in s["tags"].items():
            total["tags"][k] = total["tags"].get(k, 0) + v
        for key in ("first_ns", "last_ns"):
            if s[key] is not None:
                total[key] = s[key] if total[key] is None else (
                    min(total[key], s[key]) if key == "first_ns"
                    else max(total[key], s[key]))
    if total["first_ns"] is not None:
        total["span_s"] = round((total["last_ns"] - total["first_ns"]) / 1e9, 6)
    return total


def summarize_series_json(dir_path, lib=None):
    return json.dumps(summarize_series(dir_path, lib))


# -------------------------------------------------------------- export ----
# The trace of what happened, as JSONL: the ts at the AOF (the standby's
# local nanosecond clock), the ts at the leader (the phi trailer's
# sent_at_ms, carried on every leader Commit since item22's trailer
# retention), the command that enacted it, and the result.

TRAILER_MAGIC = b"\xc0\x0b"
TRAILER_BYTES = 22  # magic(2) + era(4) + leader(4) + seq(4) + sent_at_ms(8)

# The SystemOperation wire discriminants (configuration.rs, all BE fields).
SYS_VOID, SYS_INIT, SYS_INCREMENT, SYS_DECREMENT = 1, 2, 3, 4
SYS_DOUBLE, SYS_HALVE, SYS_JOIN, SYS_LEAVE, SYS_BATCH = 5, 6, 7, 8, 9

# The lock-op JSON kinds that count as committed lock work.
LOCK_OPS = {"get", "set", "release", "break"}

# Bare infinity in a telemetry JSON (the detector's saturation) is not
# JSON; decode it as null.
_INF_RE = __import__("re").compile(r"(:\s*)([-+]?inf\b|NaN)")


def split_trailer(frame: bytes):
    """(frame_without_trailer, leader_ms or None)."""
    if len(frame) >= TRAILER_BYTES and frame[-TRAILER_BYTES:-TRAILER_BYTES + 2] == TRAILER_MAGIC:
        sent_at = struct.unpack("<Q", frame[-8:])[0]
        return frame[:-TRAILER_BYTES], sent_at
    return frame, None


def parse_wire(frame: bytes):
    """Decode one uVRR wire frame into (tag, era, view, slot, body) where
    body is the parsed per-tag shape (never guessed)."""
    if len(frame) < 21:
        return None
    tag, era, view, slot = struct.unpack(">IIIQ", frame[:20])
    disc = frame[20]
    if disc != (tag & 0xFF):
        return None
    rest = frame[21:]
    if tag == 2:  # Prepare { entry, committed }
        if len(rest) < 8 + 4 + 1:
            return None
        entry_slot = struct.unpack(">Q", rest[0:8])[0]
        entry_era = struct.unpack(">I", rest[8:12])[0]
        cursor = 12
        payload_disc = rest[cursor]
        cursor += 1
        if payload_disc == 1:  # Operation { id, opaque json }
            if len(rest) < cursor + 16 + 4:
                return None
            op_id = struct.unpack(">QQ", rest[cursor:cursor + 16])
            cursor += 16
            jlen = struct.unpack(">I", rest[cursor:cursor + 4])[0]
            cursor += 4
            payload = rest[cursor:cursor + jlen]
            cursor += jlen
            entry = ("operation", op_id, payload)
        elif payload_disc == 2:  # System
            sysop, consumed = parse_system(rest[cursor:])
            if sysop is None:
                return None
            cursor += consumed
            entry = ("system", sysop)
        else:
            return None
        if len(rest) < cursor + 8:
            return None
        committed = struct.unpack(">Q", rest[cursor:cursor + 8])[0]
        return ("prepare", tag, era, view, slot, entry, committed)
    if tag == 4:  # Commit { committed }
        if len(rest) < 8:
            return None
        committed = struct.unpack(">Q", rest[0:8])[0]
        return ("commit", tag, era, view, slot, committed)
    if tag in (3, 5, 8):  # PrepareOk / StartViewChange / PlannedViewChange
        return ("empty", tag, era, view, slot)
    return ("other", tag, era, view, slot)  # DoViewChange/StartView/NewState/... skipped


def parse_system(buf: bytes):
    """One SystemOperation: (human_command, bytes_consumed) or (None, 0)."""
    if not buf:
        return None, 0
    disc = buf[0]
    if disc == SYS_VOID:
        return "Void", 1
    if disc == SYS_INIT:
        if len(buf) < 5:
            return None, 0
        count = struct.unpack(">I", buf[1:5])[0]
        if len(buf) < 5 + 4 * count:
            return None, 0
        nodes = [struct.unpack(">I", buf[5 + 4 * i:9 + 4 * i])[0] for i in range(count)]
        return "Init order=" + ",".join(str(n) for n in nodes), 5 + 4 * count
    if disc in (SYS_INCREMENT, SYS_DECREMENT, SYS_LEAVE):
        if len(buf) < 5:
            return None, 0
        node = struct.unpack(">I", buf[1:5])[0]
        name = {SYS_INCREMENT: "Increment", SYS_DECREMENT: "Decrement",
                SYS_LEAVE: "Leave"}[disc]
        return f"{name} node={node}", 5
    if disc in (SYS_DOUBLE, SYS_HALVE):
        return ({SYS_DOUBLE: "Double", SYS_HALVE: "Halve"}[disc], 1)
    if disc == SYS_JOIN:
        if len(buf) < 9:
            return None, 0
        node, pos = struct.unpack(">II", buf[1:9])
        return f"Join node={node} position={pos}", 9
    if disc == SYS_BATCH:
        if len(buf) < 5:
            return None, 0
        count = struct.unpack(">I", buf[1:5])[0]
        cursor = 5
        parts = []
        for _ in range(count):
            sub, consumed = parse_system(buf[cursor:])
            if sub is None:
                return None, 0
            parts.append(sub)
            cursor += consumed
        return "Batch[" + "; ".join(parts) + "]", cursor
    return None, 0


def lock_fields(payload: bytes):
    """The lock-op JSON's fields that name the work, or None."""
    try:
        value = json.loads(payload)
    except (ValueError, UnicodeDecodeError):
        return None
    if not isinstance(value, dict) or value.get("op") not in LOCK_OPS:
        return None
    return {"op": value["op"], "lock_id": value.get("lock_id"),
            "message_id": value.get("message_id"),
            "client_id": value.get("client_id"),
            "request_num": value.get("request_num")}


def export_series(dir_path, lib=None, kinds="all"):
    """The trace of what happened: one JSON dict per line-worthy event,
    ordered as the trace recorded them (file order = epoch order = ns
    order). Kinds: all | both (reconfig+locks) | phi | phi-samples |
    reconfig | locks."""
    dir_path = Path(str(getattr(dir_path, "path", dir_path)))
    wanted = set(kinds.split()) if kinds != "all" else {"locks", "reconfig", "phi", "phi-samples"}
    if kinds == "both":
        wanted = {"locks", "reconfig"}
    events = []
    pending: dict = {}  # slot -> lock fields awaiting a committed frontier
    for path in sorted(dir_path.glob("*.aof")):
        for _op, record in iterate(path, lib):
            marker = record[0]
            aof_ns = ns_of(record)
            if marker == 2:
                if "phi" in wanted:
                    line = {"kind": "phi", "aof_ns": aof_ns}
                    # The detector saturates: some emitters write bare `inf`
                    # (not JSON). Decode tolerantly, never guessing fields.
                    text = record[9:].decode("utf-8", "replace")
                    line.update(json.loads(_INF_RE.sub(r"\1null", text)))
                    events.append(line)
                continue
            if marker == 5:
                if "phi-samples" in wanted:
                    line = {"kind": "phi-samples", "aof_ns": aof_ns}
                    line.update(json.loads(_INF_RE.sub(r"\1null", record[9:].decode("utf-8", "replace"))))
                    events.append(line)
                continue
            if marker != 1:
                continue
            wire_frame, leader_ms = split_trailer(record[9:])
            parsed = parse_wire(wire_frame)
            if parsed is None:
                continue
            kind = parsed[0]
            if kind == "prepare":
                _, _tag, _era, _view, slot, entry, committed = parsed
                if entry[0] == "operation":
                    fields = lock_fields(entry[2])
                    if fields:
                        pending[slot] = fields
                elif entry[0] == "system":
                    # The reconfig commands count as enacted only when a
                    # committed frontier covers their slot (the export's
                    # record carries the committing frame's timestamps).
                    pending[slot] = {"kind": "reconfig", "command": entry[1]}
                # the frontier commits everything at or under it
                for s in [s for s in pending if s <= committed]:
                    fields = pending.pop(s)
                    if "op" in fields:
                        if "locks" in wanted:
                            events.append({"kind": "locks", "aof_ns": aof_ns,
                                           "leader_ms": leader_ms, "slot": s,
                                           "result": "committed", **fields})
                    elif "reconfig" in wanted:
                        events.append({**fields, "aof_ns": aof_ns,
                                       "leader_ms": leader_ms, "slot": s,
                                       "result": "committed"})
            elif kind == "commit":
                committed = parsed[5]
                for s in [s for s in pending if s <= committed]:
                    fields = pending.pop(s)
                    if "op" in fields:
                        if "locks" in wanted:
                            events.append({"kind": "locks", "aof_ns": aof_ns,
                                           "leader_ms": leader_ms, "slot": s,
                                           "result": "committed", **fields})
                    elif "reconfig" in wanted:
                        events.append({**fields, "aof_ns": aof_ns,
                                       "leader_ms": leader_ms, "slot": s,
                                       "result": "committed"})
    return events


KINDS = ("all", "both", "phi", "phi-samples", "reconfig", "locks")


def export_to(path_out: str, series_dir, lib=None, kinds="all"):
    events = export_series(series_dir, lib, kinds)
    with open(path_out, "w") as handle:
        for line in events:
            handle.write(json.dumps(line) + "\n")
    return len(events)


def main(argv):
    args = [a for a in argv[1:]]
    lib_path = None
    follow_json = False
    export_out = None
    export_kinds = "all"
    paths = []
    i = 0
    while i < len(args):
        if args[i] == "--lib":
            lib_path = args[i + 1]
            i += 2
        elif args[i] == "--json":
            follow_json = True
            i += 1
        elif args[i] == "--kind":
            export_kinds = args[i + 1]
            i += 2
        else:
            paths.append(args[i])
            i += 1
    export_out = paths[1] if len(paths) > 1 and not Path(paths[1]).exists() else None
    if export_out is not None:
        paths = paths[:1]
    if not paths:
        print("usage: aof-trace-tool.py [--lib PATH] [--json] FILE_OR_DIR ...")
        return 2
    lib = load(lib_path)
    if export_out is not None:
        n = export_to(export_out, Path(paths[0]), lib, export_kinds)
        print(f"exported {n} events to {export_out}")
        return 0
    for p in paths:
        target = Path(p)
        out = summarize(target, lib) if target.is_file() else summarize_series(target, lib)
        print(json.dumps(out) if follow_json else json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
