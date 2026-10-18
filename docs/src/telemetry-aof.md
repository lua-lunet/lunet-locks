# The witness's capture: the wire tape and the lock-event series

The cluster's public telemetry and its recovery substrate live on the
**witness**: a static, non-voting process outside the roster that the
running cluster never depends on and the console is served from. The
members push every VRR datagram to the witness out of band — one UDP
sender on the hot path — and the witness does its own book work: it tapes
the raw wire and lifts the lock events. (The per-node internal trace is
the separate, build-flag [Flight Recorder](flight-recorder.md).)

## The two series, one process, one directory

The witness writes two series beside each other:

- **The wire tape** (`{unix_epoch_seconds}.aof`, the vendored TigerBeetle
  store `ext/lunet-locks-aof`): every VRR datagram the witness receives,
  one envelope `Wire` record, byte-identical to what the network carried.
  This is the recovery substrate — replayed through the real
  `Service::execute` by the witness's own lift and by the driven
  `witness-replay` tool (never automatic).
- **The lock-event series** (`ev-*.bin` + the 40-byte `LKM1` metafiles,
  the `LKE1` 61-byte record): the lift's output — the committed lock
  transitions derived from the tape replay. This is what the console's
  journal reads.

## The flush discipline

Bytes leave the receive path immediately; nothing on it waits on disk.
Flushes fire on the periodic timer (`--flush-ms`), when a sync-point
record forces the batch in hand (the clean-stop ceremony's
acknowledgement is the certificate the flushed marker vouches for), and
at shutdown. The timer cadence is the staleness bound the console's
readers live under. Any write failure crashes the witness — the
[let-it-crash policy](architecture.md); the tape is never defended by
soldiering on.

## Rolling and retention

- The wire tape rolls at the vendored engine's epoch naming; the
  operator's byte budget (`--retention-mib`) keeps the newest files
  (the active file is never a deletion candidate; the floor is the
  active file plus one older), swept on the maintenance tick only.
- The lock-event series rolls at 8 MiB; finalized files whose every
  lease has expired (the metafile's `expiry_max`) drop on the
  maintenance tick — the one deletion exception, documented in
  [event-journal](event-journal.md).

**Reader-valid-prefix rule.** Finalized files may be deleted freely, at
any time, in any order: every prefix of the rolling series is a valid
reader view. The feed server lists whatever files remain and the console
merges idempotently (events are keyed `[ts, lockId, leaseId]` in
IndexedDB with a tombstone-ahead release rule), so deleting old blocks
only shrinks the history the console shows.

On restart the witness finds the existing active files, recomputes the
write positions, truncates any torn tail, and resumes appending; the
durable lift cursor (`lift-cursor.bin`) boots the lift past the series'
tail, so a restart never re-writes the history the series already
carries.

## Serving the console

The `lock-feed` process serves a lock-event series directory over REST
and WebSocket (`/files`, `/files/:name`, `/health`, `/ws`). Its follower
tails the series — inotify-driven on Linux, poll-driven elsewhere, with
the poll loop retained as a safety net on both — so the console stays
live while the writer rolls.

The console edge (nginx) maps `/feed/` to lock-feed, re-points `/api/v1/`
at the witness's own served cache, and serves the console SPA as static
assets. The console UI never reads a cluster replica's journal files; it
reads the witness's series through the feed and the lock state from the
witness's served cache.
