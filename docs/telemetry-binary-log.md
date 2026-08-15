# Telemetry binary log

Each node optionally appends compact binary counter records to a local file.
The admin console's nginx edge reads that file directly off local disk on the
node it is co-located with, so the console's `/metrics/series` chart is served
without JSON parsing on the read path, without a time-series database, and
without any cross-node calls.

## Goals

- "Cheap and cheerful": a node *optionally* appends compact binary counters
  to a local file. No network calls for telemetry, no JSON parsing on the
  read path, no third-party time-series database.
- The admin console's nginx edge reads that file **directly off local disk**
  on the node it is co-located with. It never calls out to other nodes to
  assemble a cluster-wide view — that would be "a huge amount of plumbing"
  for what is fundamentally a local operational readout.
- Off by default. Reclaiming old segments is an explicit admin task, not
  automatic log-rolling logic.

## Feature flag

`telemetry.enabled` (default `false`), `telemetry.log_dir`,
`telemetry.rotate_min_bytes` (dev default: 64 KiB; prod default: 256 MiB).

The env-var overrides `LUNET_LOCKS_TELEMETRY_LOG` and
`LUNET_LOCKS_TELEMETRY_MIN_BYTES` take precedence over the config file and
are the convenient knob for local testing.

Recommended default posture: on for whichever node is co-located with the
admin console (so there's something to read), off elsewhere. In production,
every node may enable it if the admin console is expected to fail over to
read a different node's log. In local testing, turn it on everywhere.

## Binary record format

Fixed-size where possible; ints, not JSON, so the reader never parses text
to draw a chart.

**File header** (24 bytes, once per segment file):

| field     | type      | bytes |
|-----------|-----------|-------|
| magic     | `"LLOCKTEL"` | 8   |
| version   | u16       | 2     |
| node_id   | u32       | 4     |
| reserved  | -         | 10    |

**Per-record** (fixed 37-byte prefix + variable-length name):

| field         | type | bytes | notes |
|---------------|------|-------|-------|
| record_magic  | u8   | 1     | `0xLL` sentinel, lets a reader resync after a torn write |
| record_len    | u16  | 2     | total record length including this header |
| kind          | u8   | 1     | acquire=1, renew=2, release=3, cas=4, expire=5, break=6, deny=7 |
| ts_ms         | u64  | 8     | epoch ms |
| lock_id       | u32  | 4     | matches the `Lock.id` in openapi.yaml |
| fencing_token | u64  | 8     | mirrors §2's `fencingToken` |
| renew_count   | u32  | 4     | mirrors §2's u32 `renewCount` mutation rule |
| held_gauge    | u32  | 4     | cluster-wide held-lock count at this instant, for the chart |
| name_len      | u8   | 1     | ≤128 per the `name` field's max length (see openapi.yaml) |
| name          | bytes| name_len | UTF-8, not NUL-terminated |
| crc32c        | u32  | 4     | trailer, covers everything from `record_magic` |

Total: 37 + name_len + 4 = up to ~169 bytes/record (128-byte name is the
worst case; most records are well under 60 bytes).

## Segments and rotation

`telemetry-<nodeId>-<epochMsAtOpen>-<seq6>.bin`. A segment rotates only
after it has reached `telemetry.rotate_min_bytes` (a *minimum*, not a
maximum) — this avoids the segment-count explosion you'd get from rotating
on every small size overshoot. Rotation is atomic: write to
`<name>.bin.tmp`, `fsync`, then `rename` into place.

No automatic deletion. Reclaiming disk (deleting old segments) is an
explicit, separate admin action — e.g. running a
`lunet-locks-telemetry-gc` tool by hand — not logic embedded in the writer.
If the disk fills up, the writer logs a warning and disables itself; it
never crashes or blocks the lock service's request path.

## Where this plugs in (and where it doesn't)

The hook point is a pure Teal module, `src/telemetry_log.tl`, called from
the Teal service layer *after* a request has been committed and applied
(i.e. downstream of the code that turns `Node:request()` /
`Node:receive()` / `Node:idle()` results into client replies) — never inside
`src/advisory_lock.tl`'s FFI boundary, and never inside the pinned vrr-core
dependency itself.

**vrr-core boundary**: telemetry only observes already-committed outputs; it
does not need vrr-core to expose anything beyond what it already returns.
If a revision of this format ever needs replication slot/era/view captured per
record, that requires a vrr-core adapter surface — per `AGENTS.md`, any such
change is staged locally only, never committed or pushed, backed by an
upstream GitHub issue, and reported to the coordinator.

## nginx read path

A tiny long-lived LuaJIT FastCGI reader process
(`console/telemetry-reader.lua`, unix socket, not a port) runs alongside
`make up`, `fastcgi_pass`ed from `location = /api/v1/metrics/series` in
`console/nginx.conf.template` when telemetry is enabled. It `mmap`s or seeks
the current segment and decodes fixed-size records directly into JSON
buckets for the `/metrics/series` response shape — no general binary-log
query language, just the one shape the chart needs. When telemetry is
disabled, nginx proxies the mock backend instead, so the console works
identically either way.

## Relation to the cluster deployment descriptor

`src/cluster_config.tl` uses the same "minimal, dependency-free,
fixed-schema binary/flattened-text encode+decode" philosophy as the record
format above — no general-purpose JSON or binary serialization library, just
a small hand-rolled codec for one known shape. That module is unrelated to
telemetry functionally; it's cited here only as the sibling precedent for
"define exactly the fields you need, encode/decode them by hand, keep it
small."
