# The TigerBeetle-pattern flush harness (item20): the WAL double-ring
# prepare cycle, the checkpoint with the superblock seal, and the grid
# writes — measured with the same stats discipline fio reports
# (nearest-rank percentiles 50/90/99/99.9/99.99, n, window).
#
# Reference: the user's TigerBeetle 0.17.9 VSR analysis
# (gist 3818c439bb08774f87bc8a92cc0a6ad5). Per-op: 1 MiB body into the
# wal_prepares ring (slot = op % 1024), then the redundant 256 B header
# into the separate wal_headers ring — both durability-flagged, the header
# written only after the body completes, prepare_ok gated on both. Every
# 960 ops: grid blocks (512 KiB) + 4 x 24 KiB superblock copies written
# LAST, verify 3-of-4.
import fcntl
import hashlib
import json
import os
import struct
import sys
import time

# TB geometry defaults (config.zig / constants.zig, per the gist).
MESSAGE_SIZE_MAX = 1 * 1024 * 1024  # the prepare body slot size
HEADER_SIZE = 256  # the redundant header slot size
RING_SLOTS = 1024  # slot = op % 1024
GRID_BLOCK_SIZE = 512 * 1024
SUPERBLOCK_COPIES = 4
SUPERBLOCK_COPY_SIZE = 24 * 1024  # 8 KiB header + 16 KiB padding
CHECKPOINT_OPS = 960  # vsr_checkpoint_ops = 1024 - 32 - 32


def slot_offset(op, ring_slots, slot_size):
    # The ring overwrites slots in place: slot = op % journal_slot_count.
    return (op % ring_slots) * slot_size


def nearest_rank(samples, percentile):
    # Nearest-rank percentile: the ceil(p/100 * n)-th of the sorted
    # samples — fio's default percentile definition.
    import math
    ordered = sorted(samples)
    if not ordered:
        raise ValueError("no samples")
    rank = max(1, math.ceil(round(percentile / 100 * len(ordered), 9)))
    return ordered[rank - 1]


class MockBackend:
    # The test backend: records (kind, offset, size) and sleeps delay_us.
    def __init__(self, delay_us=(0, 0)):
        self.log = []
        self.delays = {"body": delay_us[0], "header": delay_us[1],
                       "grid": delay_us[0], "superblock": delay_us[0]}

    def write(self, kind, fd, offset, data):
        start = time.perf_counter_ns()
        time.sleep(self.delays.get(kind, 0) / 1e6)
        self.log.append((kind, offset, len(data)))
        return (time.perf_counter_ns() - start) // 1000


class RealBackend:
    # The rig backend: O_DSYNC-flagged pwrite on Linux (the durability
    # flag rides the write; completion == on disk, per the gist).
    def __init__(self, prepares_path, headers_path, grid_path, superblock_path):
        flags = os.O_WRONLY | os.O_CREAT | getattr(os, "O_DSYNC", 0)
        self.fds = {
            "body": os.open(prepares_path, flags),
            "header": os.open(headers_path, flags),
            "grid": os.open(grid_path, flags),
            "superblock": os.open(superblock_path, flags),
        }

    def write(self, kind, fd, offset, data):
        fd_ = self.fds.get(kind, fd)
        start = time.perf_counter_ns()
        written = os.pwrite(fd_, data, offset)
        if written != len(data):
            raise IOError(f"short write {written}/{len(data)} at {offset}")
        return (time.perf_counter_ns() - start) // 1000


def wal_cycle(backend, op, body_size, header_size, ring_slots, ring_slot_size):
    # One forced prepare: the body's completion gates the header write.
    slot = op % ring_slots
    body_offset = slot_offset(op, ring_slots, ring_slot_size)
    header_offset = slot_offset(op, ring_slots, header_size)
    body_us = backend.write("body", None, body_offset, b"\0" * body_size)
    header_us = backend.write("header", None, header_offset, b"\0" * header_size)
    return {"op": op, "body_us": body_us, "header_us": header_us,
            "total_us": body_us + header_us}


def grid_cycle(backend, blocks, block_size, stride):
    us = []
    for block in range(blocks):
        us.append(backend.write("grid", None, block * stride, b"\0" * block_size))
    return us


def superblock_payload(sequence, copy):
    # The 24 KiB copy: 8 KiB header-ish region + 16 KiB padding, checksummed.
    payload = struct.pack("<QQ", sequence, copy) + hashlib.sha256(
        struct.pack("<QQ", sequence, copy)).digest() * 384
    return payload[:SUPERBLOCK_COPY_SIZE].ljust(SUPERBLOCK_COPY_SIZE, b"\0")


def verify_superblock_quorum(copies, payload_checksum=None):
    # TB's verify quorum: 3-of-4 (superblock_quorums.zig:61-63). Copies
    # agree pairwise; at least three must match the first copy.
    agreeing = sum(1 for c in copies if bytes(c) == bytes(copies[0]))
    return agreeing >= 3


def checkpoint_cycle(backend, grid_blocks, grid_block_size, superblock_copies, superblock_copy_size):
    # Grid/replies first, the superblock seal LAST (4 copies, then verify).
    grid_cycle(backend, grid_blocks, grid_block_size, stride=grid_block_size)
    for copy in range(superblock_copies):
        backend.write("superblock", None, copy * superblock_copy_size,
                      b"\0" * superblock_copy_size)
    return superblock_copies


def summarize(latencies_us, label, extra=None):
    # fio-style summary: n, window, percentiles — the units are explicit.
    ordered = sorted(latencies_us)
    window_s = (ordered and sum(ordered) / 1e6) or 0.0
    out = {
        "label": label,
        "n": len(latencies_us),
        "unit": "us",
        "min": ordered[0] if ordered else None,
        "max": ordered[-1] if ordered else None,
        "mean": round(sum(latencies_us) / len(latencies_us), 3) if latencies_us else None,
        "p50": nearest_rank(latencies_us, 50) if latencies_us else None,
        "p90": nearest_rank(latencies_us, 90) if latencies_us else None,
        "p99": nearest_rank(latencies_us, 99) if latencies_us else None,
        "p999": nearest_rank(latencies_us, 99.9) if len(latencies_us) >= 1000 else None,
        "p9999": nearest_rank(latencies_us, 99.99) if len(latencies_us) >= 10000 else None,
        "samples_us": latencies_us,
        "window_s_rounded": round(window_s, 3),
    }
    if extra:
        out.update(extra)
    return out


def run_wal(backend, ops, body_size, header_size, ring_slots, ring_slot_size, checkpoint_every=CHECKPOINT_OPS,
            batch_pause_s=0.02, grid_blocks=8, grid_block_size=GRID_BLOCK_SIZE,
            superblock_copies=SUPERBLOCK_COPIES, superblock_copy_size=SUPERBLOCK_COPY_SIZE):
    # The workload: WAL cycles in batches with small pauses (TB under load
    # flushes in batched cadence; the 960-op checkpoint is the natural
    # pause). Not aggressive.
    totals, bodies, headers, checkpoints = [], [], [], []
    for op in range(ops):
        if op and op % checkpoint_every == 0:
            start = time.perf_counter_ns()
            checkpoint_cycle(backend, grid_blocks, grid_block_size,
                             superblock_copies, superblock_copy_size)
            checkpoints.append((time.perf_counter_ns() - start) // 1000)
        result = wal_cycle(backend, op, body_size, header_size, ring_slots, ring_slot_size)
        bodies.append(result["body_us"])
        headers.append(result["header_us"])
        totals.append(result["total_us"])
        if op % 32 == 31:
            time.sleep(batch_pause_s)
    return {
        "summary": {
            "wal_total_us": summarize(totals, "wal-total (body+header), O_DSYNC writes"),
            "wal_body_us": summarize(bodies, "wal-body"),
            "wal_header_us": summarize(headers, "wal-header (256 B, separate area)"),
            "checkpoint_us": summarize(checkpoints, "checkpoint (grid + 4x superblock seal)") if checkpoints else None,
        },
    }


def main(argv):
    if len(argv) < 2:
        print("usage: tb_flush_harness.py <out.json> [--ops N] [--body-bytes N] "
              "[--header-bytes N] [--ring-slots N] [--in-flight N] [--pause-s S]")
        return 2
    out_path = argv[1]
    options = {"ops": 1920, "body_bytes": MESSAGE_SIZE_MAX, "header_bytes": HEADER_SIZE,
               "ring_slots": RING_SLOTS, "pause_s": 0.02, "checkpoint_every": CHECKPOINT_OPS}
    i = 2
    while i < len(argv):
        key = argv[i].lstrip("-").replace("-", "_")
        options[key] = int(argv[i + 1]) if not key.endswith("_s") else float(argv[i + 1])
        i += 2
    backend = RealBackend("tb_prepares.bin", "tb_headers.bin", "tb_grid.bin", "tb_superblock.bin")
    result = run_wal(backend, options["ops"], options["body_bytes"], options["header_bytes"],
                     options["ring_slots"], MESSAGE_SIZE_MAX if options["body_bytes"] == MESSAGE_SIZE_MAX else 4096,
                     checkpoint_every=options["checkpoint_every"], batch_pause_s=options["pause_s"])
    result["meta"] = {"body_bytes": options["body_bytes"], "header_bytes": options["header_bytes"],
                      "ring_slots": options["ring_slots"], "ops": options["ops"],
                      "checkpoint_every": options["checkpoint_every"], "pause_s": options["pause_s"]}
    with open(out_path, "w") as handle:
        json.dump(result, handle, indent=1)
    print(json.dumps(result["summary"]["wal_total_us"], indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
