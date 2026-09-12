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
           4: "TelemetryOutbound"}

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


def main(argv):
    args = [a for a in argv[1:]]
    lib_path = None
    follow_json = False
    paths = []
    i = 0
    while i < len(args):
        if args[i] == "--lib":
            lib_path = args[i + 1]
            i += 2
        elif args[i] == "--json":
            follow_json = True
            i += 1
        else:
            paths.append(args[i])
            i += 1
    if not paths:
        print("usage: aof-trace-tool.py [--lib PATH] [--json] FILE_OR_DIR ...")
        return 2
    lib = load(lib_path)
    for p in paths:
        target = Path(p)
        out = summarize(target, lib) if target.is_file() else summarize_series(target, lib)
        print(json.dumps(out) if follow_json else json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
