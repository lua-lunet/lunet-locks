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
`v0.11.0`) prints the opinion for one pair, and its usage enumerates the
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

The hosts drive a subset of the table. Where the host names a pair, the
drive is:

| Pair | The drive | Where |
|---|---|---|
| `(in-the-cluster\|steady, cluster)` | the client op's correlation deadline: the pending is dropped and a fresh op scheduled on the backoff; no un-acknowledged datagram is relayed | `examples/lease-sequencer/src/main.rs:1333-1338`, the deadline at `main.rs:1191`; the TCP conn's 30 s pending at `main.rs:2541`; the Teal host's client deadline at `src/server.tl:1279-1300` |
| `(in-the-cluster, steady)` | the leader's idle beat: a synthetic client GET proposed through the full phase-2 path, whose commit fan-out the followers observe | `examples/lease-sequencer/src/main.rs:883-901` |
| `(steady, steady)` | the suspicion: the leader-failure detector's §14.2 forced view, then the core's suspicion input | `examples/lease-sequencer/src/main.rs:779-854`; the harness's `leader_timeout_step` at `src/uds_harness.rs:609-657`; the Teal host's election loop at `src/server.tl:1234-1239` |
| `(steady, steady)` while `timedout` holds | the cluster viewchange poll: inside the view-change limbo the poll drives the §14.2 forced view at the next view number, a fresh commit disarming it | `examples/lease-sequencer/src/main.rs:2000-2019` |
| `(booted, *)` | the recovery drive on `recovery_ms`: the §8 re-announce of the `Reincarnation(old, new)` pair, then the tick | `examples/lease-sequencer/src/main.rs:2087-2094`; the adapter's drive at `ext/advisory_lock/src/ffi.rs:1503-1524`; the Teal host at `src/server.tl:1244-1258` |
| `(booted, booted)` | the rejoin gossip's resend on `GOSSIP_RESEND_MS`: the entry ticket to every peer, until the cluster's answer installs | `examples/lease-sequencer/src/main.rs:2106-2111`, the send at `main.rs:948-964` |
| `(booted, booted)` | the boot-time discovery rounds on the 100 ms cadence, bounded by the 15 s deadline, after which the ordinary fenced boot proceeds | `examples/lease-sequencer/src/main.rs:1149-1172`; the Teal host at `src/server.tl:492-527` |
| `(stopping, *)` | none: the stop is signal-driven, the loop break is the drain point, and no timer re-drives the drain | `examples/lease-sequencer/src/main.rs:1437-1481` (`Lifecycle::register`), the stop at `main.rs:1949`; the Teal host's stop hook at `src/server.tl:310-321` |
| `(stopping-not-flushed, *)` | the stop failure surface: the failed round or drain prints its failure and reports `SERVICE`; no timer re-drives the flush | `ext/advisory_lock/src/ffi.rs:1636-1716`; the host's code report at `examples/lease-sequencer/src/main.rs:1949-1958` |

The witness, unknown, and crashed states carry no host drive: a witness
is a passive data sink, a node unknown to the others has no addressing
row, and a crashed node's clock belongs to whatever the next boot does
about the marker. The adapter's status plane (`status`,
`ext/advisory_lock/src/ffi.rs:1732-1743`) reports the node's
replication state, leader, era, view, and folded configuration era, and
the hosts read it to pick the drive; the pairing above is exhaustive
over the drives the hosts own.
