# lease-sequencer — the embedded-sequencer lease demo

The Corfu-style downstream design draft: six storage nodes, two per three
datacenters, each process embedding `lunet_advisory_lock::Node` (the same
adapter the LuaJIT host drives through the C ABI) and binding a UDP peer
port plus a TCP client NDJSON port with the same wire protocol as
`src/server.tl` (the LAL peer envelope, the membership/v3 genesis
fingerprint, NDJSON framing, the forward/redirect application channel).
The advisory lock holds the lease to be the SEQUENCER.

## The lease policy

The client never states an expiry. The wire request says "hold/renew
this lock for me for a duration X" (`lease_ms` in the rented `lease`
object), and MAY stamp `sent_at_ms` on the request for latency
measurement only — no protocol decision reads it. The executing host
(the leader, wherever the core runs the op) checks instantaneously on
its own clock whether the lock is free (or held by the same holder),
and only then stamps `expiry = its_execution_time + X` and runs
consensus. The reply names the input `lease_ms`, the leader-stamped
`expiry`, and its `executed_at`. The granted window is therefore
measured in exactly one clock — the leader's — so client clock skew
cannot lengthen a lease; a desynced client can only fail to hold, and
fencing across re-delivery is identity (`message_id`, the
`(client, request_num)` exactly-once contract), not staleness, because
staleness no longer exists on the wire.

Every node runs the same driver:

- if the lease is free, try to hold it (SET) with a 500 ms duration;
- if it holds it, renew 250 ms before the deadline (a fresh grant renews at
  250 ms after the grant);
- if another node holds it, poll it as a GET and schedule the next poll at
  the leader-echoed remaining time plus `rand()*100 ms` (thinned by the
  polite probe floor under `--model polite`).

Every attempt is logged at `info` as

```
lease-attempt ts=<ms> node=<id> op=set|renew|get|steal expiry=<ms>
```

with further `info` events for boot, membership, leader changes, grants,
the reincarnation remap, and periodic status, and `warn` events for the
unexpected-but-survivable.

## Tracing

The node logs through `tracing`; the library (`lunet_advisory_lock`)
emits events and the binary owns the subscriber stack, exactly the
tokio-rs guidance: a downstream embedder chooses its own subscriber.
This binary installs `tracing_subscriber::fmt` with
`EnvFilter::from_default_env()` (the `RUST_LOG` variable), ANSI off, no
line timestamp (events carry their own `ts=` fields), writing through
`tracing_appender`'s `NonBlocking` writer to a per-node **daily rolling**
file (the `--log` path's stem becomes the file prefix under the same
directory). The `WorkerGuard` is held for the process lifetime and
flushes on an orderly shutdown.

**Loss window.** `NonBlocking` is drop-on-overflow: when a node writes
faster than the worker drains, events are dropped, never backpressured
and never blocking the datagram path. Events written in the last moments
before a `SIGKILL` (the kill cycles in `run.sh`) are likewise lost — the
worker had no chance to flush. The stability check therefore asserts on
steady-state streams, never on a tail. Under the default `RUST_LOG=info`
the per-datagram `trace!` events are compiled in but filtered before any
formatting, and the cadence assertions hold (measured: renewal ~250 ms,
poll ~2x renewal).

## Host policy: the client stream pauses during an era transition

A committed reconfiguration's establishing era completes only through the
§8.7.8 fence into the established era, and the primary's client stream is
exactly the activity that keeps that fence from arming (the PrepareOk
baseline refresh is gated while the transition is outstanding, but the
stream must actually stop). Every node therefore holds its lease driver —
and its forwarded traffic — while its view era differs from the folded
configuration era or it is not `Normal`; the lease lapses for the
transition's bounded window and a fresh grant re-acquires it. The measured
cost is one lease lapse per join, and the joins commit and complete.

## The client channel: every voter serves the lock verbs

A lock verb (get/set) addressed to a voter's TCP client port is always
executed on the leader: the leader's own port proposes the request
in-process and answers on the same connection when it commits; a
non-leader's port forwards the request to the leader over the peer
application channel (the same `FORWARD_REQUEST`/`FORWARD_RESPONSE` wire
the lease driver and the embedded clients use), pends the connection on
the correlation id at the lead of the ordinary client deadline, and
answers once with the leader's committed reply. A leader-side refusal
mid-flight (the leader stood down in the window) answers
`{"error":"not_leader"}` and the client retries or rotates. There is no
leader requirement on the addressed node: a client speaks to its LOCAL
voter, whatever the replication state of the rest of the cluster.

## The load models

`lease-load` runs one of two load models, selected with
`--model polite|aggressive`. Polite is the default and the experiment's
cadence: one contender (`--clients 1`), no getters (`--getters 0`
unless explicitly overridden), and the foreign-incumbent probe thinned
by a 1000 ms floor — a live three-client chase across three
datacentres stays low-volume metadata. Aggressive is the parked stress
shape, opt-in only (`--model aggressive`): the probe rides just past
the leader-echoed expiry and the default getters return. The free-lock
SET race and the holder's renewal cadence are identical in both models:
the race is the takeover measurement, and the renewal is a correctness
knob — flooring either would hide the service's own behavior.

## The embedded lock client

Launched with `--embedded-client N --lock LOCK_ID`, the node runs N
contender loops against its own embedded `Node` in-process — the same
chase machine the `lease-load` binary drives over the wire (GET probe →
free/expired ⇒ SET race; live foreign incumbent ⇒ poll past the
leader-echoed expiry with jitter; holder ⇒ BUMP renewal one
renewal-margin inside the deadline), with no client→cluster TCP. The
cadence knobs mirror the load client's defaults: `--client-ttl-ms 500`
and `--renew-fraction 0.5`. Every op is submitted through the node's
own request path — proposed locally when this node leads, forwarded to
the leader over the peer application channel otherwise — so the
committed lock transitions are the same Service calls the wire clients'
verbs exercise and the AOF records identical evidence (lock events and
lookups, byte-for-byte the same request JSON).

The loops share the host's client gate: every embedded client boots OFF
and is silent until the first SIGUSR2; SIGUSR1 stops all outbound
operations, forgets holdership, resets the lease-id/request-num
bookkeeping, and abandons any in-flight op (a reply that arrives late is
ignored); a restarted client re-enters as a NON-holder — its first
action is a GET probe, never a blind BUMP. Transitions are logged on the
process stdout stream (`client stop (SIGUSR1) at wall=<ms>` / `client
start (SIGUSR2) at wall=<ms>`).

## The cluster

`config/cluster.jsonl`: one genesis voter per DC (`dc1-node1`, `dc2-node1`,
`dc3-node1`) and one joiner per DC (`dc1-node2`, `dc2-node2`, `dc3-node2`)
that enters at weight 0 through the join verb. `run.sh` promotes `dc1-node2`
and `dc2-node2` with the increment verb; `dc3-node2` stays at weight 0 —
the zero-voting-weight member the downstream design wants. The TCP client
port is the descriptor's UDP peer port + 1000.

`config/cluster-genesis.jsonl` is the 7th node's acceptance descriptor:
the founding membership plus its own appended line — a boot on the
genesis descriptor against a live, reconfigured cluster, the shape the
membership snapshots serve.

The descriptor is a hint list, not membership law. A `--name` the
descriptor carries boots exactly as documented above; a `--name` it omits
is NOT a refusal — the node boots as a weight-0 joining member whose
identity comes from `--join-id N` and `--join-endpoint [HOST]:PORT`,
appended after the hint rows. The hint rows say where the cluster is;
the node stays fenced until the leader's committed configuration
carries its row (the join verb at the leader is the act of entry), then
folds its admitting era and serves. Nothing about the file gates who may
exist — the cross-environment boundary is a future PSK, never the
descriptor.

## The standby telemetry node

`check-standby.sh` runs the same six-node cluster with `dc1-node2` as the
**standby telemetry node**: it enters at weight 0 through the join verb —
first, so its learner fold is the clean one — is never promoted, and holds
no vote. The `--aof-dir` option turns the node into the AOF host: its
committed lock transitions enqueue to the async write-behind writer
(dedicated thread, drop-on-overflow, `io_uring` on Linux / buffered
`write_all` elsewhere, fsync only on the periodic `--aof-flush-ms` timer,
at roll, and on shutdown) instead of the blocking journal, and the active
file rolls at exactly 2 MiB (zero-padded, so every finalized file is one
erasure block). The standby's lease driver still converses with the leader
like every node's does — the round-trip traffic carries the era evidence
that keeps the standby tracking the cluster while it applies the stream.

## The telemetry AOF (the envelope series)

Any node can carry a **telemetry AOF** — `--telemetry-aof-dir`, falling
back to `--aof-dir` — a `{epoch}.aof` series written through the vendored
TigerBeetle AOF (item21) with the typed record envelope (item22) from
`ext/lunet-locks-aof`: every record is `marker(1) | local-clock-ns(8) |
payload`, and the marker names the subsystem — `Wire` (the raw uVRR wire
message, its serialization reused as-is), `TelemetryTimeoutDecision`,
`TelemetryStateTransition`, or `TelemetryOutbound` (JSON). The wire
protocol itself is untouched.

The series follows the node's **voting weight** (the hard requirement):
weight 0 — or the boot Recovering/Joining phase, where the weight is not
yet known — keeps the AOF ON; weight > 0 turns it OFF; 1→0 re-arms it (a
trace gap since the disarm is expected). While ON, the node writes its
boot trace (the Recovering/Restarting/Joining decision and every outbound
message), every VRR datagram it receives as a Wire record, and the
phi-informed timeout decisions; the same weight sequence on a voting node
leaves just the boot trace. A background flush fsyncs every
`--aof-flush-ms` (default 1000) and earlier whenever the vendored
entry-window cap fills — and it STOPS when the gate disarms. A clean stop
(SIGTERM/SIGINT) writes the teardown record LAST and flushes
unconditionally; SIGKILL loses the last unflushed window. The active file
rolls at `--telemetry-rollover-mib` (default 4) and the series keeps
exactly the current file plus one closed old.

The phi-informed election wait: the host tick loop derives the
election/suspicion wait from the current leader's sketch — `safety *
max(heartbeat, learned mean interval)`, clamped to
`--phi-timeout-min-ms` / `--phi-timeout-max-ms` (defaults 500 / 1000) —
falling back to the clamped fixed gate while the sketch has fewer than
two intervals. Every changed wait logs one `TelemetryTimeoutDecision`
record (phi, now, previous wait, next wait) plus the tracing line.

The script also starts `lock-feed` against the standby's AOF directory and
the console stack (static SPA + nginx edge with `/feed/` mapped to the
feed), then asserts, headless (curl/grep only):

1. committed events land in the standby's AOF files;
2. the feed serves them (REST listing and file bytes);
3. the console's data endpoint (`/feed/files` through the nginx edge,
   basic-auth loopback) returns them;
4. the cluster cadence is unaffected: the holder's renewal cadence is
   ~250 ms and each non-holder polls at ~2x renewal, measured from the
   voting nodes' logs while the standby's AOF writer and feed run.

Every spawned process (nodes, feed, mock, nginx) is killed on exit.

## Running

```
./run.sh             # the stability check (also as ./check.sh)
./check-standby.sh   # the standby AOF + console demo check
./run-acceptance.sh  # the membership-snapshot acceptance run
```

`run.sh` builds the crate, starts the six nodes with fresh state (each
node's file at `RUST_LOG=info` unless the operator exports another
`RUST_LOG` — e.g. `RUST_LOG=debug` or
`RUST_LOG="lunet_advisory_lock=trace,info"` opts a run into per-datagram
detail), drives the joins and the two increments at the leader, runs the
stability window
(cadence and poll assertions from the logs), and then three kill/restart
cycles: SIGKILL the current holder, wait 2000 ms, assert a survivor steals
the lease, restart the killed leader on the same state file, assert the
reincarnation rejoin (identity bump, the peers' remap notice), and assert
the cluster re-stabilizes. Every spawned process is killed on exit.

`run-acceptance.sh` boots a 7th node on the GENESIS descriptor
(`config/cluster-genesis.jsonl` — the founding membership and the
joining node's own line only) while the live cluster is at era 6, after
the three joins and the two increments. The node escalates through the
era chain with era-qualified discovery, adopts a quorum of agreeing
snapshots in memory, joins through the ordinary fenced boot, and writes
the adopted facts behind; the run asserts all of it from the node's log
and the membership sidecar's exact content, through the leader's
post-commit disseminations. See
[membership snapshots](../../docs/src/membership-snapshots.md) for the protocol.

## Downstream consumption

This crate is the reference embedder. Downstream pins git revisions, exactly
as this repo does:

- `lunet-advisory-lock` has no release tag: pin the `lua-lunet/lunet-locks`
  commit the integration was validated against (`f5f8373` and later).
- The core comes in through the advisory-lock crate's dependency:
  `uvrr-core` upstream commit `0fc6380` plus the `lunet-locks/learner-era-fold`
  patch branch (submodule head `aacecda`), which carries the learner
  acquisition rule and the stop-the-world-under-stream completion. The
  `[patch]` section in `ext/advisory_lock/Cargo.toml` builds it from the
  vendored submodule; downstream mirrors that section with its own pin.
- The embedded surface is `Node::open` / `request` / `receive` / `idle` /
  `leader_timeout` / `recover` / `reconfigure` / `own_id` / `status` /
  `next_output` (`NodeStatus` carries both the view era and the folded
  configuration era). The C ABI is untouched.

## Layout

- `src/main.rs` — the node binary: host loop (heartbeat, election,
  fenced-boot recovery drive, output drain, receive pump), the TCP client
  NDJSON server, and the lease driver. `--aof-dir` switches the node to the
  standby telemetry mode: the committed-transition hook feeds the async AOF
  writer instead of the blocking journal and the lease driver stays idle.
- `src/transport.rs` — the peer envelope, the membership/v3 genesis
  fingerprint, the `Reincarnation(old, new)` addressing notice, and the
  forward/redirect application channel.
- `src/bin/lease-client.rs` — the control client the orchestrator drives
  (lock and admin verbs over a node's TCP port).
- `src/embedded_client.rs` — the contender decision machinery (the
  `lease-load` chase machine as a shared module) and the host-side
  embedded runner: the `--embedded-client N` loops, the SIGUSR1/SIGUSR2
  process gate, and the message-id reply correlation for both
  in-process and forwarded ops.
- `config/cluster.jsonl` — the six-node deployment descriptor.
- `run.sh`, `check.sh` — the stability check.
- `check-standby.sh` — the standby AOF + console demo check.
