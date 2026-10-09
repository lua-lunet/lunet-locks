# The WAL lock snapshot store

The lock table's persistence artefact is a **two-stream double-ring rotating
WAL**: a Zig engine in [tbio-core](https://github.com/lua-lunet/tbio-core)
behind a C ABI, wrapped by a thin Rust `StateStore` implementation, driven
by the libuv loop. It exists so a node's lock table survives a clean stop
and restart without replaying anything, at advisory-lock scale: ~100 live
leases, thousands of ops/sec, ephemeral generations of a few KiB on disk.

The two lock kinds (`lock-kinds.md`) persist through two separated ring
streams — the TigerBeetle journal's own pattern: one WAL, several rings,
each with one shape. The ephemeral stream is the hot path; the durable
stream carries the named locks' fat frames.

The persistence law is the contract seam's law
([contract-seam.md](contract-seam.md)) and is not restated here as policy;
the store below is one implementation of `StateStore`.

## The IO ruling

The node is sans-IO and single-threaded: one libuv loop owns the lock
table, serializes every record, and makes the syscalls. There are **no
threads** anywhere on this path. No data is ever passed across threads to
be serialized; the serialization happens on the loop and the kernel does
the rest asynchronously.

- **Linux**: the steady tick submits `pwrite` operations through **io_uring**
  and carries on. The ring's **eventfd** is registered with the ring and
  polled by libuv (`uv_poll`); when completions arrive, the loop drains the
  CQEs. This is the standard event-loop integration — gRPC-C++'s EventEngine,
  Swift System, and the liburing documentation all register an eventfd with
  the ring and let the host event loop poll it for readiness; the DBMS
  literature recommends io_uring precisely to *avoid* dedicated blocking-IO
  threads ("the asynchrony of io_uring enables us to avoid the approach of
  using dedicated I/O threads", one ring per thread, completions co-located
  with compute).
- **macOS (darwin)**: there is no kernel-asynchronous regular-file IO to
  buy — kqueue has no file-IO filter (`EVFILT_AIO` is documented
  "currently unsupported"), POSIX AIO is threadpool glibc-style machinery,
  and epoll-style readiness on regular files is not a thing. Upstream's own
  darwin backend states it outright
  (`src/io/darwin.zig`: "Darwin file IO … is *synchronous*"). The darwin
  path therefore completes each write inline on the loop at submit time:
  page-cached `pwrite` of a few KiB, microseconds, and the same completion
  callback fires immediately. No thread, no ring.
- **libuv's own async file API is not used**: `uv_fs_write` and friends are
  backed by libuv's internal threadpool (`uv_queue_work` under the hood) —
  that is exactly the cross-thread data handoff the ruling forbids.

Durability classes are explicit:

- **Steady-state generations**: page-cached writes, **zero fsync**, ever.
  A crash mid-period loses the periodic snapshot and that is acceptable:
  a crash is a dirty boot and the dirty boot never reads the artefact.
- **The shutdown generation**: the eager flush writes the whole live set
  in the foreground and fsyncs once — one fsync covers both streams (they
  share the one file descriptor) — blocking, before the stop completes.
  A stop that cannot flush is a failed stop. This is the only fsync in the
  store's steady life and it happens on the stop path, where blocking is
  the contract.

## The knowledge of last sync

Every completion callback runs on the loop thread (eventfd wake on Linux,
inline on darwin), so the counters are plain loop-owned integers — no
atomics, no locks; there is no other thread to be safe from:

| counter | meaning |
| --- | --- |
| `completed_generations` | generations fully written since open (per stream) |
| `completed_records` | records landed since open |
| `last_completed_unix_ns` | nanosecond timestamp of the last completed write |
| `in_flight` | submissions outstanding (0 or 1 generation's worth) |
| `refusals` | flushes refused (capacity) since open |

The status is readable through the C ABI (`wal_status`) and rides the
node's existing status surface, so an operator — or the console UI — can
answer "when did this node last get its lock table to disk".

## The two streams

**One file**, beside the node's state path: the state path's name with a
`.wal` suffix. Fixed-size, pre-laid-out; everything in it is at a fixed
offset. Two streams, each with its own two alternating pairs (the copy
collection), each ring ~1 MiB from its neighbours so no two rings share an
erase block:

```text
offset 0        ephemeral pair A data ring     (capacity × 64 B)
offset 1 MiB    ephemeral pair A header ring   (capacity × 64 B)
offset 2 MiB    ephemeral pair B data ring
offset 3 MiB    ephemeral pair B header ring
offset 4 MiB    durable pair A data ring       (capacity × 768 B)
offset 5 MiB    durable pair A header ring     (capacity × 64 B)
offset 6 MiB    durable pair B data ring
offset 7 MiB    durable pair B header ring
total file size 8 MiB (default capacity 1024 per stream)
```

One shape per ring. The ephemeral/durable id spaces are separated **by the
streams themselves**: an ephemeral lock is addressed by its caller-supplied
u64; a durable lock's u64 is assigned by the creating consensus write (its
slot). No shared space, no partition, no collision to prove.

## The ephemeral record (1 cache line)

64 bytes, fixed — exactly one cache line; the record is a layout-exact
`extern struct` (Zig) / `#[repr(C)]` struct (Rust), pinned by comptime and
const asserts on every build, so the on-disk bytes ARE the struct's bytes
and the round trip is a memcpy — the only "special" step is the checksum,
computed into the struct's own field:

```text
offset  0  stamp      u64  — the epoch selector, wall clock of the write
offset  8  expiry     u64  — absolute, in the leader's clock, milliseconds;
                          the engine's clock is nanoseconds and the wrapper
                          converts (ms→ns on write, ns→ms on read)
offset 16  lock id    u64  — the caller-supplied ephemeral address
offset 24  lease id   u64
offset 32  holder     [u8;16]
offset 48  lease_ms   u64
offset 56  tag        u32  — record kind: live | tombstone; version bits
offset 60  checksum   u32  — the low 32 bits of the vendored Aegis-128
                          checksum (vsr/checksum.zig), over the record with
                          the checksum field zeroed; little-endian
```

An ephemeral has no name, no path, no payload: it "is" its u64, and the
whole record is one cache line. The **data ring holds the bitwise NOT of
the header** — each u64 flipped — as the redundant second copy: the
torn-write/misdirected-write detector, the same shape as TigerBeetle's
redundant `wal_headers` write. Read-back demands the flipped copy's
un-flipping reproduce the header byte-exact plus the checksum.

## The durable record (12 cache lines, 768 B)

A **fat frame**, 12 CL fixed, one budget:

```text
CL0     the header — the same 64-byte struct as above, unflipped:
        stamp, expiry, lock id (the consensus-assigned u64), lease id,
        holder, lease_ms, tag, checksum.
        The tag's kinds: live | kept | delete.
        The checksum spans the WHOLE 12-CL frame (computed with the
        checksum field zeroed) — so an ephemeral header and a durable
        header are checksum-distinguishable without any type bit.
CL1-5   name — 320 B: a u16 byte length and the name bytes (the ≤128-byte
        validated path or longer operator names), zero-filled past the end
CL6-7   tags and counters — 128 B: taken_at_ms (u64), renew_count (u32),
        a u16 tag-bytes length, and the tag bytes (sorted, deduplicated,
        ≈100 B of labels after the counters) — zero-filled past the end
CL8-11  consistency area — 256 B: a SHA-256 content hash at a fixed offset,
        the rest zero-reserved
```

Unused bytes are zero. The durable stream's id is the **slot of the
creating consensus write** (`lock-kinds.md`); the record's counters ride
durably here — for durables only; an ephemeral's life is one cache line
and carries nothing else.

## The write path

- **Steady tick** (the ~1000 ms libuv timer, the lazy async snapshot): for
  every lock changed since the last completed generation, one record per
  stream — ephemeral changes to the ephemeral stream (live record for
  acquire/renew, **tombstone** for release; natural expiry writes
  nothing), durable changes to the durable stream (the fat frame: live,
  kept — released but not destroyed —, or delete). Each generation is
  submitted as contiguous runs: one data-ring write and one header-ring
  write per stream per generation. The syscall is made and the loop
  carries on.
- **Compaction on fill**: when the active pair's forward position reaches
  capacity, the current live set — for the durable stream including the
  kept records — is written to the inactive pair from its start, and the
  pairs flip. The copied generation is re-stamped with the copy's write
  time — the epoch selector must favour the compacted pair — and the
  compacted pair's header-ring tail is zeroed past the copied records so
  the scan's zero-termination stays exact. This is the rotating WAL's copy
  collection.
- **Shutdown**: the eager flush writes the full live set of both streams
  to the active pairs in the foreground, blocks, fsyncs once — the one
  fsync covers both streams (one file descriptor). The capacity condition
  is the remaining room: `position + live ≤ capacity`, per stream; a
  generation that overruns it is a refused flush = failed stop, per the
  law. The stop path submits the current changes through the tick path
  before the eager flush — the flush validates the set against the
  engine's own mirror, and a divergence is the loud failed stop. A
  steady-tick refusal is counted in `refusals` and leaves the serving
  state untouched. Never a silent partial write, never a truncation.

## The read path

- **Epoch selection at startup**, per stream: read the first header record
  of each pair's header ring; the newest nanosecond stamp names the active
  generation. Both empty = cold start (the ordinary answer; the store then
  behaves as if it never existed).
- **The scan**, per stream: a forward scan of the active pair's header
  ring only — the fast path is tiny headers, and only the live set is
  ever lifted:
  1. skip records already expired, with a clock-drift safety threshold
     (~3 s, a configuration parameter: the reader may sit behind or ahead
     of the leader's clock);
  2. apply tombstones and deletes (drop the lock; last record per lock id
     wins);
  3. hydrate survivors from the data ring — an ephemeral by un-flipping
     its NOT copy and demanding byte-exact agreement plus the header
     checksum; a durable by reading its 12-CL frame, demanding the CL0
     header's byte-exact agreement plus the frame checksum.
- **Load is lazy, clean-boot-only**: the boot fence (marker `stopped` →
  `flushed`) gates it as everywhere else; a dirty boot reads nothing and
  rebuilds from the replica stream — the "resync-from-commit" of issue
  #30. A loaded snapshot installs into an empty table, once, and never
  overwrites live state.

## Capacity honesty

Each ring holds a fixed count of records — the capacity, an operator
sizing knob (default 1024 per stream; ephemeral data ring 64 KiB, durable
data ring 768 KiB, header rings 64 KiB each). The record size is fixed,
so "how full" is arithmetic, never discovery. A generation that exceeds
capacity is a **refused flush**: on the shutdown path a failed stop; on
the steady tick a refusal counted in `refusals` with the serving path
unaffected. Never a silent partial write, never a truncation.

## The copy canary

The interop across the C ABI is proven by a build-gated canary: Rust packs
wide-bit unique sentinels into the real record struct, Zig copies it
through three caller-owned slots incrementing a unique field by one per
copy (checked increments — unsigned-only fields, comptime headroom, no
wrap), and Rust verifies the last slot byte-exact and field-true. It
proves the C layout, the size and shape, the endianness, and the
field-true interpretation across the boundary — the insurance against a
future debugger logging junk inside the engine. Memory is **caller-owned
end to end**: the engine allocates nothing, frees nothing, and touches
only the slots handed to it, for the duration of the call. The canary
rides every build and every architecture; a big-endian host fails it
loudly, by design.

## The seams

- **Zig engine** (tbio-core): the two-stream double-ring machine, the
  layout-exact record structs, the vendored upstream primitives it builds
  on (`checksum.zig` and friends), the Linux io_uring backend with
  eventfd, the darwin synchronous-completion backend. Exposed over a C ABI
  in the `aof_c.zig` pattern: `wal_open`, `wal_submit`, `wal_pump`,
  `wal_event_fd` (Linux), `wal_flush_sync` (the shutdown generation),
  `wal_load`, `wal_status`, `wal_close`, and the record exchange
  `wal_record_encode` / `wal_record_decode` — the engine's own codec,
  where the one checksum there is (the vendored Aegis-128 primitive,
  low 32 bits) is stamped and verified. All exchanges are **pointer-only**
  — never a struct by value across the C ABI (by-value classification
  differs between SysV and AAPCS64). The submit carries the caller's
  serialized batch — one generation of records — and one generation is
  outstanding at a time: a second submit before the pump is refused
  (`-EAGAIN`), the host pumps first. The counters move at completion
  time, inline on darwin and at the pump (or the flush) on Linux.
  `wal_flush_sync` takes the serialized live sets, validates them against
  the engine's own mirror (a divergence is the loud failed stop), drains
  any outstanding batch first, writes forward, and fsyncs exactly once.
  `wal_load` answers every content refusal as "none" — checksums, twins
  and expiry all refuse at open or in the scan, never as an error. A
  backend IO failure poisons the handle: every call returns `-EIO` until
  it is dropped, and nothing partial is ever silent. The Linux io_uring
  backend is compile-checked on every host by a cross-compile gate and
  exercised at runtime on Linux.
- **Rust wrapper** (this repo): a `WalSnapshotStore` implementing
  `StateStore` over that ABI, mounted where the node mounts its store
  today — nothing above the trait knows which store it holds. The
  wrapper carries no codec and no checksum code of its own: the record's
  fields cross the ABI through the `wal_record_encode` /
  `wal_record_decode` exports, and the codec and checksum live only in
  the engine behind the ABI (there is exactly one of each, ever — a
  second implementation on the host would be a permanent agreement
  burden).
- **Loop integration** (this repo): the ~1000 ms libuv timer submits and
  pumps through the node's FFI; on Linux a `uv_poll` on the eventfd fd
  wakes the loop for completions; the counters ride the status surface.
  The shutdown path calls `flush` eagerly as the law requires and the
  trait signature is untouched: the same `StateStore` is drivable sync
  (shutdown) and async (the tick) without changing the seam.
