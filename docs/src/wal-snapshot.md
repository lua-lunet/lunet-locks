# The WAL lock snapshot store

The lock table's persistence artefact is a **double-ring rotating WAL**: a
Zig engine in [tbio-core](https://github.com/lua-lunet/tbio-core) behind a
C ABI, wrapped by a thin Rust `StateStore` implementation, driven by the
libuv loop. It exists so a node's lock table survives a clean stop and
restart without replaying anything, at advisory-lock scale: ~100 live
leases, thousands of ops/sec, generations of ~11 KiB on disk.

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
  in the foreground and fsyncs once, blocking, before the stop completes.
  A stop that cannot flush is a failed stop. This is the only fsync in the
  store's steady life and it happens on the stop path, where blocking is
  the contract.

## The knowledge of last sync

Every completion callback runs on the loop thread (eventfd wake on Linux,
inline on darwin), so the counters are plain loop-owned integers — no
atomics, no locks; there is no other thread to be safe from:

| counter | meaning |
| --- | --- |
| `completed_generations` | generations fully written since open |
| `completed_records` | records landed since open |
| `last_completed_unix_ns` | nanosecond timestamp of the last completed write |
| `in_flight` | submissions outstanding (0 or 1 generation's worth) |
| `refusals` | flushes refused (capacity) since open |

The status is readable through the C ABI (`wal_status`) and rides the
node's existing status surface, so an operator — or the console UI — can
answer "when did this node last get its lock table to disk".

## The on-disk format

One file beside the node's state path: the state path's name with a `.wal`
suffix. The file is fixed-size and pre-laid-out; everything in it is at a
fixed offset.

**Record** — 72 bytes, fixed:

```text
header, 36 bytes:
  ns write stamp   u64  — the epoch selector, wall clock of the write
  expiry           u64  — absolute, always in the leader's clock
  lock id          u64
  lease id         u64
  tag              u32  — record kind: live | tombstone; format version bits
lease payload, 32 bytes:
  holder uuid      16 bytes
  lease window     u64  — lease_ms
  reserved         8 bytes — zero
checksum, 4 bytes:        — the low 32 bits of the vendored Aegis-128
                          checksum (vsr/checksum.zig) over the 68 bytes
                          before it; all integers little-endian
```

**Header record** — 40 bytes, fixed: the same 36-byte header plus its own
4-byte Aegis-low-32 checksum, little-endian throughout. The header ring
carries no payload; it is the torn-write / misdirected-write detector and
the fast scan path.

**The double ring (one WAL, two parts)**: the *data ring* holds full
records (header + payload + checksum); the *header ring* holds header
records only. The two rings sit ~1 MiB apart so they do not share an erase
block. Read-back reads both and demands agreement.

**Two alternating pairs** = the copy collection. Layout in the file:

```text
offset 0            pair A data ring
offset 1 MiB        pair A header ring
offset 2 MiB        pair B data ring
offset 3 MiB        pair B header ring
total file size     4 MiB
```

Each ring holds a fixed count of records — the capacity, an operator
sizing knob (default 1024 records; pair A data ring 72 KiB, header ring
40 KiB). The record size is fixed, so "how full" is arithmetic, never
discovery.

## The write path

- **Steady tick** (the ~1000 ms libuv timer, the lazy async snapshot of
  issue #30): for every lock changed since the last completed generation,
  one record is serialized on the loop — a live record for acquire/renew,
  a **tombstone** for release (tombstones are records too; natural expiry
  writes nothing — the lock is simply absent from the next generation) —
  and submitted forward through the active pair: one data-ring write, one
  header-ring write, same offsets, same position. The syscall is made and
  the loop carries on.
- **Compaction on fill**: when the active pair's forward position reaches
  capacity, the current live set is written to the inactive pair from its
  start, and the pairs flip: the compacted pair becomes active with its
  position past the copied records, the exhausted pair becomes the next
  compaction target. The copied generation is re-stamped with the copy's
  write time — the epoch selector must favour the compacted pair — and the
  compacted pair's header-ring tail is zeroed past the copied records so
  the scan's zero-termination stays exact. This is the rotating WAL's copy
  collection.
- **Shutdown**: the eager flush writes the full live set to the active
  pair in the foreground, blocks, fsyncs once. The capacity condition is
  the remaining room: `position + live ≤ capacity`; a generation that
  overruns it is a refused flush = failed stop, per the law. A
  steady-tick refusal is counted in `refusals` and leaves the serving
  state untouched. Never a silent partial write, never a truncation.

## The read path

- **Epoch selection at startup**: read the first header record of each
  pair's header ring; the newest nanosecond stamp names the active
  generation. Both empty = cold start (the ordinary answer; the store then
  behaves as if it never existed).
- **The scan**: a forward scan of the active pair's header ring only —
  the fast path is tiny headers, and only the live set is ever lifted:
  1. skip records already expired, with a clock-drift safety threshold
     (~3 s, a configuration parameter: the reader may sit behind or ahead
     of the leader's clock);
  2. apply tombstones (drop the lock; last record per lock id wins);
  3. hydrate survivors' payloads from the data ring, each demanding its
     header checksum, its data checksum, and twin agreement between the
     rings.
- **Load is lazy, clean-boot-only**: the boot fence (marker `stopped` →
  `flushed`) gates it as everywhere else; a dirty boot reads nothing and
  rebuilds from the replica stream — the "resync-from-commit" of issue
  #30. A loaded snapshot installs into an empty table, once, and never
  overwrites live state.

## Capacity honesty

The ring holds a fixed count of records. A generation that exceeds
capacity is a **refused flush**: on the shutdown path a failed stop; on
the steady tick a refusal counted in `refusals` with the serving path
unaffected (the tick simply does not land until capacity is restored by a
compaction or an operator resize). Never a silent partial write, never a
truncation. Sizing is an operator decision and the numbers are public:
capacity × 72 B data ring, capacity × 40 B header ring per pair.

## The seams

- **Zig engine** (tbio-core): the double-ring machine, the record codecs,
  the vendored upstream primitives it builds on (`checksum.zig` and
  friends), the Linux io_uring backend with eventfd, the darwin
  synchronous-completion backend. Exposed over a C ABI in the
  `aof_c.zig` pattern: `wal_open`, `wal_submit`, `wal_pump`,
  `wal_event_fd` (Linux), `wal_flush_sync` (the shutdown generation),
  `wal_load`, `wal_status`, `wal_close`.
- **Rust wrapper** (this repo): a `WalSnapshotStore` implementing
  `StateStore` over that ABI, mounted where the node mounts its store
  today — nothing above the trait knows which store it holds.
- **Loop integration** (this repo): the ~1000 ms libuv timer submits and
  pumps through the node's FFI; on Linux a `uv_poll` on the eventfd fd
  wakes the loop for completions; the counters ride the status surface.
  The shutdown path calls `flush` eagerly as the law requires and the
  trait signature is untouched: the same `StateStore` is drivable sync
  (shutdown) and async (the tick) without changing the seam.
