# The lock telemetry capture file: the write-behind series

The lock telemetry capture file is the cluster's public telemetry: the
console follows a non-voting telemetry node's own async write-behind log
(the **AOF**, append-only file) instead of any replica's replication-path
journal. The telemetry node is a zero-voting-weight member: it follows
the replication stream, applies every committed lock transition, and
defers its own disk writes so the cluster's latency cadence is untouched.
The mechanism follows the TigerBeetle AOF write-behind pattern: bytes
leave the hot path immediately, are never force-flushed there, and are
fsync'd only on a periodic timer, a checkpoint, or shutdown. (This is the
public, wire-visible plane; the per-node internal trace is the separate
[Flight Recorder](flight-recorder.md).)

## The writer

The AOF writer (`ext/advisory_lock/src/aof.rs`) is a dedicated writer thread
behind a bounded queue:

- **Producers never wait on disk.** The adapter's committed-transition hook
  enqueues the complete 61-byte `LKE1` record into the writer's queue and
  returns. When the queue is full the event is **dropped** and the drop
  counter increments — the same drop-on-overflow contract the tracing stack
  follows. Nothing on the replication path blocks, and an AOF failure never
  disables the node.
- **Linux**: buffered appends are submitted through an `io_uring` ring (the
  `io-uring` feature, default on for Linux targets). The writes are
  page-cached buffered appends: no `O_DIRECT`, no `O_SYNC`, no fsync on the
  append path. If the ring cannot be created on the running kernel the
  writer falls back to plain buffered `write_all` and logs once.
- **macOS and other non-Linux targets**: a plain dedicated writer thread
  with buffered `write_all`.
- **fsync policy**: none on the append path. The file is fsync'd on a
  configurable periodic timer (the operator knob), on an explicit checkpoint
  request, and on graceful shutdown.
- **Loss window**: the last unflushed bytes may be lost on power loss. The
  AOF is best-effort telemetry; nothing in the cluster depends on it for
  safety, and no recovery path reads it.

The record bytes are exactly the lock-event journal format (the same
`LKE1`/`LKM1` codecs) but written to a different file series: the AOF is
the deferred durability target, not the journal.

## Rolling at one erasure block

The active file rolls at exactly **2 MiB** (2 × 1024 × 1024 bytes). Because
records are fixed 61 bytes and 61 does not divide 2 MiB, the writer
zero-pads the tail before rolling, so **every finalized AOF file is exactly
2 MiB** — write-position aligned to one erasure block. The zero padding
fails record parsing (bad magic), so the fixed-size readers' stop-at-first-
invalid-record rule terminates cleanly at the pad.

File naming and the metafile match the lock-event journal conventions, so
the same feed server reads both series:

- Open (still-appending): `ev-open-<ts_started_ms>.bin`.
- Finalized: `ev-<op_min>-<op_max>-<expiry_min>-<expiry_max>.bin` with a
  companion atomically-written 40-byte `.meta` file (tmp + fsync + rename +
  directory sync).

At each roll the writer flushes the buffer, fsyncs the finalized file,
renames it to its final name, and writes the metafile.

**Reader-valid-prefix rule.** Finalized files may be deleted freely, at any
time, in any order: every prefix of the rolling series is a valid reader
view. The feed server lists whatever files remain and the console merges
idempotently (events are keyed `[ts, lockId, leaseId]` in IndexedDB with a
tombstone-ahead release rule), so deleting old blocks only shrinks the
history the console shows. No component deletes finalized files by itself.

On restart the writer finds the existing `ev-open-*.bin` file, scans its
valid records to recompute the write position and metadata window, truncates
any torn tail, and resumes appending.

## Rotation at (re)start

The capture series the non-voting telemetry nodes write through the
vendored TigerBeetle store (`ext/lunet-locks-aof`, the
`{unix_epoch_seconds}.aof` naming) ROTATES at every (re)start: a NEW
active file opens under a fresh unix-epoch name and the pre-existing
series is left in place for the ADMIN to prune — no startup sweep deletes rotated files on
this path. The prior `{epoch}.aof` files are the between-restart history;
during a run the in-run rollover keeps exactly the current file plus one
closed old. The telemetry nodes stop and start cleanly, so an admin
prunes old capture files between runs.

## The telemetry node

The telemetry node is a regular cluster member that joined at weight 0 (the
`--aof-dir` option turns the embedded node into the AOF host). It participates
in replication as any member does — it receives prepares, applies committed
operations — but it holds no vote and writes its lock-event records to the
AOF writer instead of the blocking journal. Its lease driver converses with
the leader like every node's driver does: the round-trip traffic is what
carries the era evidence that keeps the telemetry node tracking the cluster while
it applies the stream. Its AOF directory is therefore a complete copy of the
cluster's committed lock-transition stream, produced without taxing the
quorum path.

## Serving the console

The `lock-feed` process serves the telemetry node's capture directory over REST and
WebSocket exactly as it serves a journal directory (`/files`, `/files/:name`,
`/health`, `/ws`). Its follower tails the series — inotify-driven on Linux,
poll-driven elsewhere, with the poll loop retained as a safety net on both —
so the console stays live while the writer rolls.

The console edge (nginx) maps `/feed/` to lock-feed and serves the console
SPA as static assets. The console UI never reads a cluster replica's journal
files; it reads the telemetry node's deferred capture series through the feed.
The three headless assertions the capture demo makes are that committed
events land in the AOF files, that the feed serves them, and that the
console's data endpoint (`/feed/files` through the edge) returns them.

Cluster cadence is unaffected: the stability assertions (renew ~250 ms, poll
~2× renewal) hold while the telemetry node's writer and the feed run.

## The replay tape

The capture series also feeds the replay story: `skaffold_aof_tape`
streams a telemetry AOF directory as the `from,to,{json}` CSV tape — one
line per record, in file order (epoch order = ns order) — and the trivial
shell filter `... | grep "^66,99,"` reads any peer pair's raw jsonl
stream. The filtered lines force-feed a node in the unit tests through
the same playback engine the Flight Recorder's tape feeds
([the Flight Recorder](flight-recorder.md)); its streamer
(`skaffold_flight_tape`) renders the identical CSV shape over the
internal recordings. The telemetry capture file is the stable-ish,
prod, UI-facing plane; the Flight Recorder is the feature-flagged,
debug-only plane whose internal format is unstable and whose deep read
is same-commit-readable only. Each streamer's from/to derivation rules
are printed in its `--help`.
