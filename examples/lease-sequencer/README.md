# lease-sequencer — the embedded-sequencer lease demo

The Corfu-style downstream design draft: six storage nodes, two per three
datacenters, each process embedding `lunet_advisory_lock::Node` (the same
adapter the LuaJIT host drives through the C ABI) and binding a UDP peer
port plus a TCP client NDJSON port with the same wire protocol as
`src/server.tl` (the LAL peer envelope, the membership/v3 genesis
fingerprint, NDJSON framing, the forward/redirect application channel).
The advisory lock holds the lease to be the SEQUENCER.

## The lease policy

Every node runs the same driver:

- if the lease is free, try to hold it (SET) with a 500 ms lease;
- if it holds it, renew 250 ms before the deadline (a fresh grant renews at
  250 ms after the grant);
- if another node holds it, poll it as a GET and schedule the next poll at
  the reported expiry plus `rand()*100 ms`.

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
- `config/cluster.jsonl` — the six-node deployment descriptor.
- `run.sh`, `check.sh` — the stability check.
- `check-standby.sh` — the standby AOF + console demo check.
