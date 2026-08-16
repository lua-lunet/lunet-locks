# Lock-event journal

The lock-event journal is an append-only, per-replica binary log of committed
lock transitions. It exists to give the console and other observers a durable,
replayable view of what the cluster has done. The journal is observability
data: it never participates in replication, consensus, or recovery, and a
journal failure never affects the service path.

## What is journaled

The journal records only committed lock transitions produced by the lock state
machine: **hold**, **renew**, and **release**. Denied SETs, GETs, and any
other client operation that does not result in a committed transition are not
journaled. Duplicate applies of the same committed operation (the core may
re-deliver an already-applied slot during recovery replay) never re-append a
record; the adapter journals each committed transition exactly once.

Each record carries the applying replica's host timestamp. Timestamps are the
applying replica's view of time; replicas do not synchronize clocks for the
journal, and cross-replica timestamp comparisons are not meaningful.

## Record format

Every event is a fixed 61-byte big-endian binary record. The layout, quoted
from `ext/advisory_lock/src/journal.rs`:

```text
magic "LKE1" (4B) | len u32 (= 53) | kind u8 | ts u64 | lock_id u64 |
lease_id u64 | holder [u8;16] | expiry u64 | crc32 u32
```

Field semantics:

- **magic**: the ASCII bytes `LKE1`.
- **len**: payload length in bytes after the magic and length fields, before
  the CRC. Fixed at 53 for v1; retained for forward compatibility.
- **kind**: `1` = hold, `2` = renew, `3` = release.
- **ts**: milliseconds since the Unix epoch at the moment the applying
  replica executed the transition.
- **lock_id**: the lock identifier.
- **lease_id**: the lease identifier within that lock.
- **holder**: 16-byte opaque holder identity.
- **expiry**: absolute expiry in milliseconds since the Unix epoch. Release
  records carry the released lease's holder, lease_id, and expiry.
- **crc32**: CRC-32 IEEE over the 49 bytes from `kind` through `expiry`
  inclusive.

All multi-byte integers are big-endian. Records are fixed-length; there is no
padding and no variable-width encoding.

## Metafile format

When an event file rolls, a fixed-size 40-byte metafile is written atomically
alongside it. The layout, quoted from `ext/advisory_lock/src/journal.rs`:

```text
magic "LKM1" (4B) | op_min u64 | op_max u64 | expiry_min u64 |
expiry_max u64 | count u32
```

- **magic**: the ASCII bytes `LKM1`.
- **op_min / op_max**: minimum and maximum `ts` values across all records in
  the rolled file.
- **expiry_min / expiry_max**: minimum and maximum `expiry` values across all
  records in the rolled file.
- **count**: number of records in the rolled file.

The metafile is written as a single atomic operation: write to a temporary
file, fsync, rename into place, then fsync the parent directory. A metafile
with bad magic or insufficient length is rejected on read.

## File naming and rolling

Event files live under a per-replica directory configured via
`--event-journal-dir`. Two naming forms exist:

- **Open (still-appending)**: `ev-open-<ts_started_ms>.bin`. No metafile
  accompanies an open file. The timestamp in the name is the millisecond time
  at which the file was created.
- **Rolled**: `ev-<op_min>-<op_max>-<expiry_min>-<expiry_max>.bin` with a
  companion `.meta` file sharing the same stem. The four numeric components
  are the metafile's window bounds.

Rolling is byte-threshold driven. When the cumulative written bytes in the
current open file reach `--event-journal-roll-bytes` (default 8 MiB), the
adapter closes the file, renames it to its final name using the accumulated
window bounds, writes the metafile atomically, and opens a fresh
`ev-open-*.bin` for subsequent appends.

## Resume on reopen

If the process restarts while an open file exists, `Journal::open` finds the
existing `ev-open-*.bin`, scans its valid records up to the roll threshold to
recompute the in-memory metadata window, seeks to end-of-file, and resumes
appending. The resumed file retains its original start timestamp in the
filename. This ensures that a crash mid-file does not lose the window
accumulation or create duplicate open files.

## Corrupt-tail tolerance

The parser (`parse_file`) reads records sequentially from the front of a file
buffer and stops cleanly at the first invalid or short record. A record is
rejected if the buffer is too short, the magic does not match `LKE1`, the
CRC-32 does not verify, or the kind discriminant is not one of hold, renew, or
release. Valid records preceding the corruption are returned; the corrupt tail
is silently discarded. This means a partial write at crash time loses at most
the trailing incomplete record, and readers never fail on a torn file.

## Durability posture

The journal is **observability data**. Its design invariant is that it must
never block, poison, or fail the replication path:

- A journal I/O error logs once to stderr and disables journaling for the
  remainder of the process lifetime. The node continues serving clients and
  participating in replication normally.
- Journal errors are never propagated to the core, never trigger a fault, and
  never cause a view change.
- The journal directory and roll threshold are optional. An empty
  `--event-journal-dir` disables journaling entirely.

### Never-delete policy

No component in this system deletes journal files. Rolled `.bin` and `.meta`
files accumulate indefinitely. Pruning is a sysadmin job, performed outside
the service. The lock-feed server and the console SPA read files but never
remove them.

## Serving to the console

### lock-feed

The `lock-feed` binary (`ext/lock_feed/src/main.rs`) serves a journal
directory to the console over HTTP and WebSocket. It is started by
`consolectl` when `FEED_DIR` is set, binding to `FEED_PORT` (default 8482).
The nginx reverse proxy maps `/feed/` to the lock-feed backend with WebSocket
upgrade support and a long read timeout.

REST endpoints:

- `GET /health` — returns `{"ok":true}`.
- `GET /files` — returns a JSON array of file entries sorted by `opMin`
  ascending. Each entry is either a rolled file (with `opMin`, `opMax`,
  `expiryMin`, `expiryMax`, `count`, `size`, `open: false`) or an open file
  (with `name`, `size`, `open: true`). Only rolled files with a valid
  companion `.meta` are included.
- `GET /files/<name>` — returns the raw bytes of the named file. Path
  traversal is rejected; only files directly within the journal directory are
  served.

WebSocket endpoint:

- `GET /ws` — upgrades to a WebSocket. On connect, the server sends the
  backlog of events from the current open file, then streams live events as
  they are appended. Messages are JSON objects with `"type": "event"` or
  `"type": "rolled"`. Event messages carry `kind`, `ts`, `lockId`, `leaseId`,
  `holder` (hex-encoded 16 bytes), and `expiry`. Rolled messages carry `file`,
  `opMin`, `opMax`, `expiryMin`, `expiryMax`, `count`, and `next` (the new
  open file name).

A background rescan task polls the journal directory at a configurable interval
(default 200 ms). It detects rolls by watching for a changed open-file name,
reports newly appeared `.meta` files as `rolled` messages, and tails the
current open file by reading new bytes past the last-seen offset and parsing
records from them.

### SPA catch-up model

The console SPA consumes the journal through three cooperating layers:

1. **Meta-file poll.** A Web Worker (`journal-loader.mjs`) polls
   `GET /feed/files` for the list of rolled files. It filters out open files
   and sorts by `opMin` ascending.

2. **Whole-file pull.** For each rolled file not yet loaded, the worker
   fetches `GET /feed/files/<name>`, parses the binary records in-browser using
   a JavaScript mirror of the Rust parser (`parse.mjs`), and posts the parsed
   events to the main thread. The worker tracks loaded file names in a
   session-scoped set; the main thread additionally persists loaded-file names
   to IndexedDB so a page reload skips already-ingested files.

3. **Live tail.** The main thread opens a WebSocket to `/feed/ws`. On
   connect, it receives the open-file backlog, then streams live events. A
   `rolled` message triggers the worker to re-poll and pick up the newly
   closed file.

Parsed events are persisted to an IndexedDB object store keyed by
`[ts, lockId, leaseId]`. This composite key makes ingestion idempotent:
re-pulling a file or receiving a duplicate live event produces an upsert, not
a duplicate row.

### Merge engine and tombstone-ahead rule

The merge engine (`merge.mjs`) maintains the active-lock set, per-second rate
buckets, and a bounded recent-events list. It accepts events from two sources:
historical file pulls from the worker and live WebSocket events.

Because file pulls and live events can arrive out of order relative to each
other, the merge engine implements a **tombstone-ahead** rule: when a release
event arrives for a `(lockId, leaseId)` pair whose acquisition has not yet
been seen, the release is stored as a pending tombstone. When the matching
hold event later arrives from a file pull, the pending tombstone is consumed
and the lock is immediately removed from the active set rather than being
added. This ensures the active-lock view is correct regardless of ingestion
order.

### Session-storage live buffer

Live WebSocket events are also mirrored to `sessionStorage` under the key
`lock-admin-live`, bounded to the most recent 500 events. On page load, the
merge engine replays this buffer through the same tombstone-aware apply logic
before IndexedDB catch-up begins, providing continuity across navigations
within the same browser session.
