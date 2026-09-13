# aof-trace-tool.py — the reference twin of the LuaJIT tool: read the
# standby AOF traces (vendored TigerBeetle 0.17.9 format) through the
# C ABI, one envelope per record. Python is the high-memory reference;
# the Teal/LuaJIT tool is the lean twin and their summaries must have
# PARITY (identical JSON). Reference sketch: the upstream reader gist
# (handover 337b313e, aof-reader-tool-sketch.md).
#
# The twins share two hard contracts (item03.5):
# - --kind is repeatable AND space-joined, with UNION semantics
#   ("--kind hb-spacing --kind aof-noise" == "--kind hb-spacing aof-noise");
#   the special names "all" and "both" expand to their kind sets and
#   union with any named kinds.
# - distribution means use one documented summation algorithm: Neumaier
#   compensated summation over the sorted values (_compensated_sum).
#
# The LuaJIT twin's CLI is tools/tl-driver.lua (tl gen of the .tl alone
# is NOT a CLI — it silently no-ops). Run it from the repo root:
#   LUA_PATH="./?.tl;./?.lua;.rocks/share/lua/5.1/?.lua;;" \
#     luajit -l tl tools/tl-driver.lua [--kind KIND ...] DIR OUT.jsonl
import ctypes
import json
import math
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


# ------------------------------------------------------------- anchors ----
# item02: readable times. An anchor is a kill/silence stamp in unix ms
# (the acting host's `date +%s%3N`), DIRECTLY comparable to a same-host
# AOF envelope ns stamp. Every exported row gains t_rel_ms (relative to
# the latest anchor at or before the row's ts; negative before the first
# anchor) and t_fmt (HH:MM:SS.mmm of the row's ts, UTC).

ABSENT = "absent"


def ns_to_ms(ns: int) -> float:
    # Exact-enough ms: the whole-second part is well under 2^53, and the
    # sub-ms remainder rides in as its own fraction.
    return float(ns // 10**6) + (ns % 10**6) / 1e6


def fmt_ts_ms(ts_ms) -> str:
    """HH:MM:SS.mmm (UTC) of a unix-ms stamp, epoch arithmetic only."""
    m = float(ts_ms)
    if m < 0:
        return "-" + fmt_ts_ms(-m)
    tod, frac = divmod(int(m), 1000)
    return "%02d:%02d:%02d.%03d" % ((tod // 3600) % 24, (tod // 60) % 60,
                                    tod % 60, frac)


def _anchor_of(ts_ms: float, anchors):
    # The latest anchor at or before the ts; the first anchor before any
    # ts earlier than it (negative t_rel is legitimate and allowed).
    chosen = anchors[0]
    for a in anchors:
        if a <= ts_ms:
            chosen = a
        else:
            break
    return chosen


def _row_ts_ms(row: dict):
    """The row's best ts in unix ms, or None when the row is an
    aggregate summary with no single ts."""
    kind = row.get("kind")
    if kind in ("locks", "locks-timeline", "reconfig"):
        return ns_to_ms(row["aof_ns"])
    if kind in ("phi", "phi-samples"):
        if "ts_ms" in row:
            return float(row["ts_ms"])
        if "now_ms" in row:
            return float(row["now_ms"])
        return ns_to_ms(row["aof_ns"])
    return None


def _compensated_sum(values) -> float:
    """The one summation algorithm both twins share (item03.5): Neumaier
    compensated summation over the sorted values. CPython >= 3.12's
    sum() already compensates; the explicit loop pins that behaviour
    version-independently so the LuaJIT twin has an exact recipe to
    mirror (naive accumulation drifted the aof-noise mean by ~1e-12 ms
    on the 28k-gap real-w1b trace)."""
    total = 0.0
    corr = 0.0
    for v in values:
        t = total + v
        if abs(total) >= abs(v):
            corr += (total - t) + v
        else:
            corr += (v - t) + total
        total = t
    return total + corr


def _stats(values) -> dict:
    s = sorted(float(v) for v in values)
    n = len(s)
    if n == 0:
        return {"min_ms": ABSENT, "mean_ms": ABSENT, "p50_ms": ABSENT,
                "p95_ms": ABSENT, "max_ms": ABSENT, "count": 0}
    return {"min_ms": s[0],
            "mean_ms": _compensated_sum(s) / n,
            "p50_ms": s[math.ceil(0.5 * n) - 1],
            "p95_ms": s[math.ceil(0.95 * n) - 1],
            "max_ms": s[-1],
            "count": n}


def _timeline_rows(dir_path, lib=None) -> list:
    # One row per lock-operation event in trace order. The lock op
    # decode speaks set/get/release/break; set resolves to acquire or
    # renew against the holder in force, and release/break clear it.
    locks = export_series(dir_path, lib, "locks")
    rows = []
    tenure: dict = {}
    for r in locks:
        lid, holder = r.get("lock_id"), r.get("client_id")
        if r["op"] == "set":
            op = "renew" if tenure.get(lid) == holder else "acquire"
            tenure[lid] = holder
        elif r["op"] == "release":
            op = "release"
            tenure.pop(lid, None)
        elif r["op"] == "break":
            op = "break"
            tenure.pop(lid, None)
        else:
            op = "get"
        rows.append({"kind": "locks-timeline", "op": op, "holder": holder,
                     "lock_id": lid, "ts_ms": ns_to_ms(r["aof_ns"]),
                     "aof_ns": r["aof_ns"]})
    return rows


def _takeover_row(window_rows, anchor) -> dict:
    exactly = lambda v: "absent" if v is None else round(v, 3)
    heres = [r for r in window_rows if r["op"] in ("acquire", "renew")]
    brk = [r for r in window_rows if r["op"] == "break"]
    last = nxt = None
    if brk:
        b = brk[0]
        pre = [r for r in heres if r["ts_ms"] < b["ts_ms"]]
        post = [r for r in heres if r["op"] == "acquire" and r["ts_ms"] > b["ts_ms"]]
        last = pre[-1] if pre else None
        nxt = post[0] if post else None
    else:
        # No break row: the handover is a holder change (natural TTL
        # out; server-side break not recorded in this window).
        for j in range(1, len(heres)):
            if heres[j]["holder"] != heres[j - 1]["holder"]:
                last, nxt = heres[j - 1], heres[j]
                break
        if last is None and heres:
            last = heres[-1]
    return {"kind": "takeover", "anchor": anchor,
            "last_holder": last["holder"] if last else ABSENT,
            "last_renew_t_rel": exactly(last["ts_ms"] - anchor) if last else ABSENT,
            "break_seen_t_rel": exactly(brk[0]["ts_ms"] - anchor) if brk else ABSENT,
            "next_acquire_t_rel": exactly(nxt["ts_ms"] - anchor) if nxt else ABSENT,
            "next_holder": nxt["holder"] if nxt else ABSENT,
            "takeover_ms": exactly(nxt["ts_ms"] - anchor) if nxt else ABSENT}


def _hb_spacing_rows(dir_path, lib=None) -> list:
    dir_path = Path(str(getattr(dir_path, "path", dir_path)))
    sent_by: dict = {}
    dt_by: dict = {}
    for path in sorted(dir_path.glob("*.aof")):
        for _op, rec in iterate(path, lib):
            if rec[0] == 1 and len(rec) >= TRAILER_BYTES + 9:
                frame, sent = split_trailer(rec[9:])
                if sent is None:
                    continue
                era, leader = struct.unpack("<II", rec[-20:-12])
                sent_by.setdefault((era, leader), []).append(float(sent))
            elif rec[0] == 5:
                try:
                    value = json.loads(_INF_RE.sub(r"\1null",
                                                   rec[9:].decode("utf-8", "replace")))
                except (ValueError, UnicodeDecodeError):
                    continue
                key = (value.get("era"), value.get("leader"))
                if value.get("dt_ms") is not None:
                    dt_by.setdefault(key, []).append(float(value["dt_ms"]))
    rows = []
    for (era, leader) in sorted(sent_by):
        sents = sent_by[(era, leader)]
        gaps = [b - a for a, b in zip(sents, sents[1:]) if b > a]
        if not gaps:
            continue
        rows.append({"kind": "hb-spacing", "dir": str(dir_path), "source": "wire",
                     "era": era, "leader": leader, **_stats(gaps)})
    for (era, leader) in sorted(dt_by):
        dts = dt_by[(era, leader)]
        if len(dts) < 2:
            continue
        rows.append({"kind": "hb-spacing", "dir": str(dir_path), "source": "sample",
                     "era": era, "leader": leader, **_stats(dts)})
    return rows


def _aof_noise_rows(dir_path, lib=None, hb_ms=5.0) -> list:
    dir_path = Path(str(getattr(dir_path, "path", dir_path)))
    ns_list: list = []
    dts: list = []
    for path in sorted(dir_path.glob("*.aof")):
        for _op, rec in iterate(path, lib):
            if rec[0] == 1:
                ns_list.append(ns_of(rec))
            elif rec[0] == 5:
                try:
                    value = json.loads(_INF_RE.sub(r"\1null",
                                                   rec[9:].decode("utf-8", "replace")))
                except (ValueError, UnicodeDecodeError):
                    continue
                if value.get("dt_ms") is not None:
                    dts.append(float(value["dt_ms"]))
    rows = []
    row = {"kind": "aof-noise", "dir": str(dir_path)}
    gaps_ms = [(b - a) / 1e6 for a, b in zip(ns_list, ns_list[1:])]
    if gaps_ms:
        row["gaps"] = _stats(gaps_ms)
        thr = min(hb_ms, 5 * _stats(gaps_ms)["p50_ms"])
        row["stalls"] = [{"a_ns": a, "b_ns": b, "gap_ms": g}
                         for g, a, b in zip(gaps_ms, ns_list, ns_list[1:])
                         if g > thr]
    if dts:
        row["samples"] = _stats(dts)
    rows.append(row)
    return rows


def _anchor_rows_and_takeovers(events, anchors):
    """The anchored export: time columns on every row, anchor echoes
    first, then takeover summary lines when the locks-timeline ran."""
    anchors = sorted(int(a) for a in anchors)
    timed = []
    for row in events:
        ts = _row_ts_ms(row)
        if ts is None:
            row["t_rel_ms"] = ABSENT
            row["t_fmt"] = ABSENT
        else:
            row["t_rel_ms"] = round(ts - _anchor_of(ts, anchors), 3)
            row["t_fmt"] = fmt_ts_ms(ts)
        timed.append(row)
    echoes = [{"kind": "anchor", "anchor": a, "ts_ms": float(a),
               "t_rel_ms": 0.0, "t_fmt": fmt_ts_ms(a)} for a in anchors]
    takeovers = []
    if "locks-timeline" in (r.get("kind") for r in timed):
        tl = [r for r in timed if r["kind"] == "locks-timeline"]
        for i, a in enumerate(anchors):
            hi = anchors[i + 1] if i + 1 < len(anchors) else None
            window = [r for r in tl if r["ts_ms"] >= a and (hi is None or r["ts_ms"] < hi)]
            takeovers.append(_takeover_row(window, a))
    return echoes + timed + takeovers

def export_series(dir_path, lib=None, kinds="all", anchors=None, hb_ms=5.0):
    """The trace of what happened: one JSON dict per line-worthy event,
    ordered as the trace recorded them (file order = epoch order = ns
    order). Kinds: all | both (reconfig+locks) | phi | phi-samples |
    reconfig | locks | locks-timeline | hb-spacing | aof-noise; the
    kinds string is a whitespace-joined UNION (the repeatable --kind
    CLI flag accumulates into it, one contract with the LuaJIT twin).
    With anchors (unix ms, repeatable), every exported row gains
    t_rel_ms / t_fmt and the locks-timeline export gains per-anchor
    takeover summary lines."""
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
    if "locks-timeline" in wanted:
        events += _timeline_rows(dir_path, lib)
    if "hb-spacing" in wanted:
        events += _hb_spacing_rows(dir_path, lib)
    if "aof-noise" in wanted:
        events += _aof_noise_rows(dir_path, lib, hb_ms)
    if anchors:
        events = _anchor_rows_and_takeovers(events, anchors)
    return events


KINDS = ("all", "both", "phi", "phi-samples", "reconfig", "locks",
         "locks-timeline", "hb-spacing", "aof-noise")


def export_to(path_out: str, series_dir, lib=None, kinds="all",
              anchors=None, hb_ms=5.0):
    events = export_series(series_dir, lib, kinds,
                           anchors=anchors, hb_ms=hb_ms)
    with open(path_out, "w") as handle:
        for line in events:
            handle.write(json.dumps(line) + "\n")
    return len(events)


def main(argv):
    args = [a for a in argv[1:]]
    lib_path = None
    follow_json = False
    export_out = None
    anchors = []
    hb_ms = 5.0
    paths = []
    kind_parts: list = []  # --kind is repeatable AND space-joined: union
    i = 0
    while i < len(args):
        if args[i] == "--lib":
            lib_path = args[i + 1]
            i += 2
        elif args[i] == "--json":
            follow_json = True
            i += 1
        elif args[i] == "--kind":
            for token in args[i + 1].split():
                if token == "all":
                    kind_parts.extend(("locks", "reconfig", "phi", "phi-samples"))
                elif token == "both":
                    kind_parts.extend(("locks", "reconfig"))
                else:
                    kind_parts.append(token)
            i += 2
        elif args[i] == "--anchor":
            anchors.append(int(args[i + 1]))
            i += 2
        elif args[i] == "--hb-ms":
            hb_ms = float(args[i + 1])
            i += 2
        else:
            paths.append(args[i])
            i += 1
    export_kinds = " ".join(kind_parts) if kind_parts else "all"
    export_out = paths[1] if len(paths) > 1 and not Path(paths[1]).exists() else None
    if export_out is not None:
        paths = paths[:1]
    if not paths:
        print("usage: aof-trace-tool.py [--lib PATH] [--json] "
              "[--kind KIND ...] [--anchor EPOCHMS ...] [--hb-ms N] "
              "FILE_OR_DIR [OUT.jsonl]")
        return 2
    lib = load(lib_path)
    if export_out is not None:
        for a in sorted(anchors):
            print(f"anchor {fmt_ts_ms(a)}")
        n = export_to(export_out, Path(paths[0]), lib, export_kinds,
                      anchors=anchors, hb_ms=hb_ms)
        print(f"exported {n} events to {export_out}")
        return 0
    for p in paths:
        target = Path(p)
        out = summarize(target, lib) if target.is_file() else summarize_series(target, lib)
        print(json.dumps(out) if follow_json else json.dumps(out, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
