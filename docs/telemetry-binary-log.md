# Telemetry binary log — design (not yet implemented)

Status: design note only. Nothing in this document is wired up; there is no
`src/telemetry_log.tl` or `console/telemetry-reader.lua` yet. This exists so
the admin console's `/metrics/series` mock and `console/openapi.yaml` reflect
a realistic, WAL-conscious eventual backend rather than an arbitrary shape.

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

`src/config.tl` has no generic node-config struct today — it only parses
`--node`/`--member` membership flags (see `config.members()`). This flag has
nowhere to live yet; add it once a general node-config struct exists, rather
than bolting it onto the membership parser. Until then, local testing can
use env-var overrides: `LUNET_LOCKS_TELEMETRY_LOG`,
`LUNET_LOCKS_TELEMETRY_MIN_BYTES`.

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
explicit, separate admin action — e.g. a future `lunet-locks-telemetry-gc`
tool, not logic embedded in the writer. If the disk fills up, the writer
logs a warning and disables itself; it must never crash or block the lock
service's request path.

## Where this plugs in (and where it doesn't)

The intended hook point is a new pure Teal module, `src/telemetry_log.tl`,
called from the Teal service layer *after* a request has been committed and
applied (i.e. downstream of whatever currently turns `Node:request()` /
`Node:receive()` / `Node:idle()` results into client replies) — never inside
`src/advisory_lock.tl`'s FFI boundary, and never inside the pinned vrr-core
dependency itself.

**This has no caller today.** There is no committed-event service layer in
this repo yet that emits a stream of "this lock's state just changed"
events for a listener to consume. Building `telemetry_log.tl` is blocked on
that existing (or being added) first — flagging as a dependency, not
building it speculatively.

**vrr-core boundary check**: not triggered by this design. Telemetry only
observes already-committed outputs; it does not need vrr-core to expose
anything it doesn't already return. If a future revision needs replication
index/term captured per record, and vrr-core doesn't already surface those
at the point telemetry hooks in, that would need a vrr-core adapter surface
— per `AGENTS.md`, any such change must be staged locally only, backed by an
upstream GitHub issue, and reported to the coordinator. Not needed for the
scope above.

## nginx read path

A tiny long-lived LuaJIT FastCGI reader process
(`console/telemetry-reader.lua`, unix socket, not a port) started alongside
`make up`, `fastcgi_pass`ed from a new `location = /api/v1/metrics/series`
in `console/nginx.conf.template` when telemetry is enabled. It `mmap`s or
seeks the current segment, decodes fixed-size records directly into JSON
buckets for the existing `/metrics/series` response shape — no general
binary-log query language, just the one shape the chart needs. When
telemetry is disabled, nginx falls back to today's proxy to the bun mock, so
the console works identically either way.

## Relation to the cluster deployment descriptor

`src/cluster_config.tl` (new, see its own file header) uses the same
"minimal, dependency-free, fixed-schema binary/flattened-text encode+decode"
philosophy as the record format above — no general-purpose JSON or binary
serialization library, just a small hand-rolled codec for one known shape.
That module is unrelated to telemetry functionally; it's cited here only as
the sibling precedent for "define exactly the fields you need, encode/decode
them by hand, keep it small."
