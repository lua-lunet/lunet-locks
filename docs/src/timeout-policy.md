# The timeout policy

The flavoured-timeout model (`ext/uvrr-core/src/timeout.rs`) is a total
function over the node's protocol conditions: given a state, a timeout
flavour, and the host's clock event that a waiting time is exceeded, the
matcher returns one opinion — `retransmit`, `do-nothing`, `heartbeat`,
`start-view-change`, or `sorry` carrying the runbook statement. The state
enum and the timeout enum are closed at eight variants each, and the
matcher is exhaustive over every pair with no default arm: a new variant
cannot enter either enum without the matcher being revisited. The model
performs no clock reads; the durations are host policy
(`docs/src/failure-detection.md`) and never prescribed — what the node
may do when the waiting time is exceeded is protocol.

The opinions' normative sources are the upstream architecture's liveness
section (`ext/uvrr-core/docs/architecture.md`, "Liveness: the resend, the
heartbeat, and the retransmit"): the leader's resend policy (resend what
has not received a response on, a relay of what the core already
released), the heartbeat (a leader with no outstanding matters and
matching frontiers may send its last commit as the frontier announcement
and proof of life, one option among many), and the view-change retransmit
(a node that times out on its view-change request set retransmits the
datagrams of that set, the attempt being volatile and nothing else
re-asking).

## The matcher's table, tool-produced

The upstream `timeout-policy` tool (a `[[bin]]` of `uvrr-core`, pinned
`v0.13.1`) prints the opinion for one pair, and its usage enumerates the
domain: eight states, eight timeout flavours. The table below is the
tool's output over the whole domain, each cell being the tool's own
verdict:

```console
$ (cd ext/uvrr-core && cargo build --release --bin timeout-policy)
$ target/release/timeout-policy <state> <timeout>
```

| state \ timeout | cluster | witness | unknown | booted | crashed | steady | stopping | stopping-not-flushed |
|---|---|---|---|---|---|---|---|---|
| in-the-cluster | retransmit | do-nothing | do-nothing | do-nothing | do-nothing | heartbeat | do-nothing | do-nothing |
| witness | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing |
| unknown | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing |
| booted | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing |
| crashed | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing |
| steady | retransmit | do-nothing | do-nothing | do-nothing | do-nothing | start-view-change | do-nothing | do-nothing |
| stopping | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing | do-nothing |
| stopping-not-flushed | sorry | sorry | sorry | sorry | sorry | sorry | sorry | sorry |

The matcher's rule, exhausted: a clock event on a non-agent (a witness, a
node unknown to the others, a booted node, a crashed node, a stopping
node) is not protocol and the opinion is `do-nothing`; an agent (a member
seated in the cluster, a steady member) answers the flavour it met — the
cluster wait resends the un-acknowledged traffic, the seated member's
steady quiet with nothing outstanding is the heartbeat option, and the
steady member's own wait is the suspicion that starts the view change; a
foreign non-agent flavour on an agent is not protocol for it either.

## The Sorry verdict

Every `(stopping-not-flushed, *)` pair answers `sorry`, and the runbook
statement is the whole of the opinion (`UNFLUSHED_RUNBOOK`,
`ext/uvrr-core/src/timeout.rs:190`; the tool prints it under every such
pair):

> a node stopping but not flushed presents a stall the operator may
> resolve by freeing disk space and retrying the flush, or by a hard
> kill, and which of those is wanted is a policy we do not comprehend
> and will never decide

## The pairs our host acts on

The hosts drive a subset of the table. The census below is enumerated
from the hosts' own timer code — every cadence, deadline and forced
transition in the adapter, the Rust example host and the Teal host — and
asserted as a test
(`ext/advisory_lock/tests/timeout_policy_audit_test.rs`). A pair no host
acts on is not a row: the witness, the unknown node and the crashed node
carry no clock-driven drive at all, a witness being a passive data sink,
an unknown node having no addressing row, and a crashed node's clock
belonging to whatever the next boot does about the marker. The adapter's
status plane (`status`, `ext/advisory_lock/src/ffi.rs`) reports the
node's replication state, leader, era, view and folded configuration
era, and the hosts read it to choose the drive.

Each row names the matcher's opinion for the pair, the host's own drive,
and its disposition: the drive is the opinion, or it is one of the named
divergences below with its reason.

| Pair | Opinion | The drive | Where |
|---|---|---|---|
| `(in-the-cluster, cluster)` | `retransmit` | the lease driver's op correlation deadline: the pending op is dropped and a fresh one scheduled on the backoff | `examples/lease-sequencer/src/main.rs:1547` (`driver_step`), the deadline at `main.rs:1562-1568` |
| `(in-the-cluster, cluster)` | `retransmit` | the embedded client's op deadline: the overdue pending expires into the backoff and the next due action is submitted afresh | `examples/lease-sequencer/src/embedded_client.rs:473` |
| `(in-the-cluster, cluster)` | `retransmit` | the client connection's pending deadline: the lock reply's correlation window closes the connection, the admin verb's answers `deadline` | `examples/lease-sequencer/src/main.rs:2886` (`pending_deadline`), the 30 s pendings at `main.rs:2568`, `main.rs:2960`, `main.rs:2978` |
| `(in-the-cluster, steady)` | `heartbeat` | the leader's idle beat: a synthetic client `get` proposed through the full phase-2 path, whose commit fan-out the followers observe | `examples/lease-sequencer/src/main.rs:940` (`heartbeat_op`), the cadence at `main.rs:2238-2243` |
| `(steady, steady)` | `start-view-change` | the leader-failure detector's conclusion that its primary is dead: the §14.2 host-forced view at the next view number, then the timeout toggle | `examples/lease-sequencer/src/main.rs:807` (`leader_timeout_step`), the drive at `main.rs:874-886` |
| `(steady, steady)` | `start-view-change` | the tick loop's own election wait: the core's ordinary suspicion input, then the timeout toggle | `examples/lease-sequencer/src/main.rs:2219` (`timers`), the wait at `main.rs:2315-2337` |
| `(steady, cluster)` | `retransmit` | the cluster viewchange poll while the timeout toggle holds: inside the view-change limbo the §14.2 host-forced view at the next view number | `examples/lease-sequencer/src/main.rs:2264-2300` |
| `(booted, booted)` | `do-nothing` | the recovery cadence: the fenced-boot drive's §8 re-announce of the `Reincarnation(old, new)` pair, then the tick | `examples/lease-sequencer/src/main.rs:2392-2399` |
| `(booted, booted)` | `do-nothing` | the rejoin gossip's resend: the entry ticket to every peer, until the cluster's answer installs | `examples/lease-sequencer/src/main.rs:2411-2416`, the send at `main.rs:1103` |
| `(booted, booted)` | `do-nothing` | the boot-time discovery rounds on the 100 ms cadence, bounded by the 15 s deadline, after which the ordinary fenced boot proceeds | `examples/lease-sequencer/src/main.rs:1347` (`discovery_step`), driven at `main.rs:2186` |
| `(booted, booted)` | `do-nothing` | the fenced-boot drive: the §8 re-announce of the reincarnation pair on every fenced drive until the node stops being fenced, then the tick | `ext/advisory_lock/src/ffi.rs:1669` (`Node::recover`) |
| `(stopping, stopping)` | `do-nothing` | the stop contract: the wire closes before any marker write, the first marker round, the drain window, the drain-proven second round | `ext/advisory_lock/src/ffi.rs:1814` (`Node::stop`), taken by the host at `examples/lease-sequencer/src/main.rs:2165` and `main.rs:2206` |
| `(stopping-not-flushed, stopping-not-flushed)` | `sorry` | the stop failure surface: every refusal arm prints the failure it hit with the runbook statement and reports `SERVICE`; no timer re-drives the flush | `ext/advisory_lock/src/ffi.rs:1814-1964` |
| `(in-the-cluster, steady)` | `heartbeat` | the idle beat: the cadence drives the core's liveness input and flushes its outputs | `src/server.tl:1244` (`heartbeat_loop`) |
| `(steady, steady)` | `start-view-change` | the election loop: the staggered election wait drives the leader-silence detection | `src/server.tl:1254` (`election_loop`) |
| `(booted, booted)` | `do-nothing` | the recovery loop: the fenced-boot drive while the replica is recovering | `src/server.tl:1277` (`recovery_loop`) |
| `(booted, booted)` | `do-nothing` | the boot-time discovery rounds across the remembered set, bounded by the discovery deadline | `src/server.tl:511` (`discovery_loop`) |
| `(stopping, stopping)` | `do-nothing` | the stop hook: the runtime's teardown machinery runs the hosted node's graceful stop exactly once, synchronous only | `src/server.tl:329` |

The pair is the host's own: a node reading its status plane and firing
its own timer names the condition it is in and the wait that expired, and
that is the pair the matcher is asked about. Where a host has no clock
of its own for a state, the row says so by naming the flavour the state
would have to be waited on — a stopping node's own wait never fires
because no timer re-drives the drain, which is the `do-nothing` the
matcher answers on a stopping node.

### The named divergences

Five of the seventeen rows do not do what the matcher answers. Each is
named here with its reason; the audit test refuses a divergence that is
not named, so a silent exception cannot enter the census.

| Divergence | Rows | The reason |
|---|---|---|
| `client-deadline-retry` | the three `(in-the-cluster, cluster)` rows | the correlation deadline retires the pending operation and schedules a fresh one; the un-acknowledged protocol traffic is never relayed |
| `barren-heartbeat` | the Rust host's `(in-the-cluster, steady)` row | the heartbeat option is taken with a synthetic client transaction rather than with the leader's last commit |
| `silent-leader-heartbeat` | the Teal host's `(in-the-cluster, steady)` row | the heartbeat option is not taken at all: the cadence drives a bare tick and no heartbeat reaches the wire |
| `fresh-attempt-manufacture` | the `(steady, cluster)` row | the poll fires a new view change at the next view number instead of re-asking the attempt already armed |
| `boot-machine-reask` | the five `(booted, booted)` rows | a booted node's cadence re-asks what the boot machine owes, and the matcher answers do-nothing on every flavour of a booted node because the machine owns its own progress |

The remaining twelve rows are the matcher's own opinion, and the audit
test proves the three the seam can reach by observation: the steady
member's wait puts the exact view-change fence exchange on the wire
(`StartViewChange` out, the leader's own back, `DoViewChange` with the
evidence, `StartView` installing the ballot) and advances the ballot by
exactly one view; the host-forced view advances the ballot to the view
it names and the core refuses one that does not strictly advance; the
booted node's cadence puts the `Reincarnation(old, new)` re-announce on
the wire to every peer; and the stopping node's own wait emits nothing
at all while its marker rounds land.
