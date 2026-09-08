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

Run a three-replica smoke test with the project-local Lunet runtime:

```console
make smoke
```

The command fetches the official Lunet `v0.8.0` release into
`.lunet/v0.8.0/`. The smoke covers acquire, GET, contention, RELEASE,
reacquisition, and lease-expiry takeover; it restarts one replica against
the live quorum — a dirty restart and a reincarnation — and then runs a
live-reconfiguration stage in which a fourth replica joins at weight 0, is
promoted to a voting member, and departs, all while a client keeps
acquiring, renewing, releasing, and reading a lock without interruption.

See the [documentation](docs/src/index.md) for the deployment descriptor,
the client and admin protocols, and operational limits.

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
check (`examples/lease-sequencer/run.sh`) drives the three joins and two
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
