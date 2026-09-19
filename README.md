# lunet-locks

`lunet-locks` is a runnable advisory-lock service with live, reconfigurable
membership. It orders lock requests with
[uvrr-core](https://github.com/lua-lunet/uvrr-core), a sans-IO
Viewstamped-Replication-Revisited core written in Rust, serves
newline-delimited JSON over TCP, and uses expiring leases rather than
mandatory locking. A small Rust adapter drives the core and owns the lock
state machine; the service process is LuaJIT and libuv (Lunet). Protocol
state lives in quorum memory under `Stability::Volatile`; the only bytes a
replica persists locally are its durable state file, which carries the
replica's incarnation.

Membership comes from a JSONL deployment descriptor: a sparse,
admin-assigned set of integer ids that are never recycled, with the
descriptor's genesis lines, in line order, as the founding membership and
the genesis succession sequence. Membership changes are live operations:
the four admin verbs — `join`, `increment`, `decrement`, `leave` — ride the
same TCP NDJSON client channel as the lock operations, and the cluster's
era advances exactly at each establishing operation's commit. A transition
runs the core's non-stop overlap path whenever a pivot exists for the
leader; otherwise it takes the stop-the-world fallback, a latency outcome,
never an error. A restarted process whose durable state file shows the
running sentinel reincarnates: its identity bumps, it announces the
`(old, new)` pair to the cluster, and the leader drives the two-era
resurrection that seats the new identity at weight 1 in the old succession
position and evicts the old one.

Termination follows the uVRR termination obligations. A graceful stop —
SIGTERM or SIGINT on the embedded-sequencer hosts, `lunet_lock_node_stop`
through the C ABI — closes the wire before any marker write, writes the
`stopped` marker, drains the committed-transition sink to quiescence, and
writes `flushed` only after the drain; the next boot continues under the
same incarnation with no resurrection. A process killed outright
(SIGKILL) leaves the running sentinel behind and reincarnates on its next
boot: the run-sheets' node kills therefore use `kill -9` wherever crash
semantics are what is being exercised.

Run a three-replica smoke test with the project-local Lunet runtime:

```console
make smoke
```

The command fetches the official Lunet `v0.10.0` release into
`.lunet/v0.10.0/`. The smoke covers acquire, GET, contention, RELEASE,
reacquisition, and lease-expiry takeover; it restarts one replica against
the live quorum — a dirty restart and a reincarnation — and then runs a
live-reconfiguration stage in which a fourth replica joins at weight 0, is
promoted to a voting member, and departs, all while a client keeps
acquiring, renewing, releasing, and reading a lock without interruption.

See the [documentation](docs/src/index.md) for the deployment descriptor,
the client and admin protocols, and operational limits.

## Testing On Cloud

Cloud testing is gated: a CLEAN COMMIT and a passing [sanity
build](docs/src/testing-on-cloud.md) on the colima build host before
every deploy; the cloud binaries carry `maybe!` tripwires and the
[Flight Recorder](docs/src/flight-recorder.md) enabled. No test run on
a dirty commit, ever. The release pipeline is documented in
[build-and-release.md](docs/src/build-and-release.md).

Run the thirty-second three-datacenter lease-failover demonstration with:

```console
make simulation
```

It drives the service over TCP NDJSON, stores logs under `.tmp/`, and always
stops its three local node processes. Use `make simulation SIM_DURATION=10`
for a shorter development run (the maximum is 30 seconds).

On a local Colima Docker daemon, run the same dynamic-client simulation against
three stable containers with `make docker-simulation`. It uses a plain Docker
build and named Docker volumes—never BuildKit or bind mounts.

## The embedded-sequencer demo

`examples/lease-sequencer` is the Corfu-style downstream design draft: six
storage nodes, two per three datacenters, each process embedding the
advisory-lock adapter directly in Rust (`Node::open`, no C ABI, no Lunet)
and binding a UDP peer port plus a TCP client NDJSON port. One node holds
the sequencer lease (500 ms, renewed at 250 ms); every other node polls the
lease and re-polls at the reported expiry plus `rand()*100 ms`. The stability
check (`examples/lease-sequencer/run.lua`) drives the three joins and two
increments, asserts the renewal and poll cadences from the per-node logs,
and runs three SIGKILL/restart cycles asserting the lease steal and the
reincarnation rejoin. See that directory's README for the downstream
consumption story.

## The ordering core

The core is vendored as the `ext/uvrr-core` git submodule on the branch
`lunet-locks/learner-era-fold`: upstream commit `0fc6380` plus two patch
commits — learner acquisition, which lets a weight-0 member fold the era
that admitted it so a joined learner converges, and fence-under-load, which
lets a stop-the-world era transition complete under the leader's own client
stream. The adapter manifest pins upstream `0fc6380` and its `[patch]`
section builds the dependency from the submodule, so this tree always
builds against the patched branch.

## The lock telemetry capture file

The public telemetry plane is the **lock telemetry capture file**: the
`{epoch}.aof` series the separate non-voting telemetry nodes write through
the vendored TigerBeetle store. The capture series rotates at every
(re)start to a fresh unix-epoch-named file and leaves the prior files for
admin pruning; during a run the rollover keeps exactly the current file
plus one closed old. (The old "telemetry AOF" naming refers to this same
file.) The per-node INTERNAL trace — everything the capture file never
sees — is the separate [Flight
Recorder](docs/src/flight-recorder.md), a debug-level feature-flagged
build, not a prod artifact.

## Two capture planes, one replay tape

The run produces two kinds of capture files, and both stream as the same
`from,to,{json}` replay tape — one CSV line per record, the first two
fields the endpoints, the JSON starting after the second comma — so the
trivial shell filter `... | grep "^${from},${to},"` works on either plain
output and the filtered lines force-feed a unit test's node through the
same playback engine (`examples/lease-sequencer/tests/scenario/mod.rs`).

| | The lock telemetry capture file | The Flight Recorder |
| --- | --- | --- |
| What it holds | the public, wire-visible events (wire datagrams, timeout decisions, state transitions, outbound queue, interval samples) | the node's private story: every drive outcome, fault, maybe, journal flush, and stop marker, plus the wire events byte-exact |
| Where it runs | the separate non-voting telemetry nodes, off the critical path — no performance impact on the quorum | feature-flagged build (`flight-recorder`), never a prod release |
| Format stability | stable-ish file formats for the UI | unstable internal format; captures hidden state that never goes on the wire |
| Reader gate | none | the deep read requires reader commit == recording commit |

The telemetry plane is the prod surface: always available, read by the
console UI. The Flight Recorder is debug-only and same-commit-readable
only: cross-commit, `skaffold_flight_tape` offers only the stable
`from,to,jsonl` slice, while the deep read (`--deep`, or any kind beyond
`wire`) is loudly refused — see [the Flight
Recorder](docs/src/flight-recorder.md) for the from/to derivation rules
each streamer prints in its `--help`.

`ext/lunet-locks-aof` vendors the AOF (append-only write-behind log) from
[tigerbeetle/tigerbeetle](https://github.com/tigerbeetle/tigerbeetle)
release tag 0.17.9 as a stripped Zig source tree behind a C ABI and this
safe Rust wrapper: the standby learner streams the leader's
heartbeat/commit records into a hash-chained, checksum-validated
`{unixepoch}.aof` series with an optional force knob (default OFF) and a
10 MiB startup retention sweep. Upstream licence: Apache-2.0 — permissive,
no copyleft obligation on the combined work (this corrects the item spec's
AGPL-3.0 premise; the pinned release's `LICENSE` is Apache-2.0). For
telemetry alone this is overkill — it is built for full disaster recovery
of a database — and it is incubated here because downstream uvrr-core
applications will want exactly this standby/DR shape. Full system
description, attribution, and the licence facts:
[`ext/lunet-locks-aof/AOF.md`](ext/lunet-locks-aof/AOF.md).
