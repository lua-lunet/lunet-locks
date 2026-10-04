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
| `(in-the-cluster, cluster)` | `retransmit` | the lease driver's op correlation deadline: the un-acknowledged traffic is relayed and the still-in-flight op is re-armed on a fresh window, never re-proposed | `examples/lease-sequencer/src/main.rs:1627` (`driver_step`), the relay at `main.rs:1656` (`Host::cluster_timeout`, `main.rs:983`) |
| `(in-the-cluster, cluster)` | `retransmit` | the embedded client's op deadline: the overdue pending is counted for the host, which relays the un-acknowledged traffic before the next due action goes out | `examples/lease-sequencer/src/embedded_client.rs:476` (`Client::step`), the relay at `main.rs:2539` |
| `(in-the-cluster, cluster)` | `retransmit` | the client connection's pending deadline: the verb's own protocol traffic is relayed, then the lock reply's correlation window closes the connection and the admin verb's answers `deadline` | `examples/lease-sequencer/src/main.rs:3012` (`pending_deadline`), the 30 s pendings at `main.rs:2659`, `main.rs:3061`, `main.rs:3079` |
| `(in-the-cluster, steady)` | `heartbeat` | the leader's idle beat: the leader's LAST COMMIT re-announced byte for byte, which every follower's suspicion gate reads as the frontier announcement and the proof of life in one datagram, at no new slot | `examples/lease-sequencer/src/main.rs:1001` (`heartbeat_op`), the cadence at `main.rs:2332-2337`, the frontier from the relay ledger at `examples/lease-sequencer/src/relay.rs:237` |
| `(steady, steady)` | `start-view-change` | the leader-failure detector's conclusion that its primary is dead: the §14.2 host-forced view at the next view number, then the timeout toggle | `examples/lease-sequencer/src/main.rs:816` (`leader_timeout_step`), the drive at `main.rs:883-895` |
| `(steady, steady)` | `start-view-change` | the tick loop's own election wait: the core's ordinary suspicion input, then the timeout toggle | `examples/lease-sequencer/src/main.rs:2298` (`timers`), the wait at `main.rs:2394-2416` |
| `(steady, cluster)` | `retransmit` | the cluster viewchange poll while the timeout toggle holds: inside the view-change limbo the ARMED ATTEMPT is re-asked — the same ballot, the same fence votes and the same evidence, re-sent byte for byte — and no view is manufactured | `examples/lease-sequencer/src/main.rs:2362-2412`, the retransmit at `main.rs:2371-2386` |
| `(booted, booted)` | `do-nothing` | the recovery cadence: the fenced-boot drive's §8 re-announce of the `Reincarnation(old, new)` pair, then the tick | `examples/lease-sequencer/src/main.rs:2499-2506` |
| `(booted, booted)` | `do-nothing` | the rejoin gossip's resend: the entry ticket to every peer, until the cluster's answer installs | `examples/lease-sequencer/src/main.rs:2511-2517`, the send at `main.rs:1112` |
| `(booted, booted)` | `do-nothing` | the boot-time discovery rounds on the 100 ms cadence, bounded by the 15 s deadline, after which the ordinary fenced boot proceeds | `examples/lease-sequencer/src/main.rs:1427` (`discovery_step`), driven at `main.rs:2281` |
| `(booted, booted)` | `do-nothing` | the fenced-boot drive: the §8 re-announce of the reincarnation pair on every fenced drive until the node stops being fenced, then the tick | `ext/advisory_lock/src/ffi.rs:1669` (`Node::recover`) |
| `(stopping, stopping)` | `do-nothing` | the stop contract: the wire closes before any marker write, the first marker round, the drain window, the drain-proven second round | `ext/advisory_lock/src/ffi.rs:1814` (`Node::stop`), taken by the host at `examples/lease-sequencer/src/main.rs:2244` and `main.rs:2285` |
| `(stopping-not-flushed, stopping-not-flushed)` | `sorry` | the stop failure surface: every refusal arm prints the failure it hit with the runbook statement and reports `SERVICE`; no timer re-drives the flush | `ext/advisory_lock/src/ffi.rs:1814-1964` |
| `(in-the-cluster, steady)` | `heartbeat` | the idle beat: the cadence drives the core's liveness input, and a leader with no Commit in the last interval re-announces the last commit it released, per peer and per ballot | `src/server.tl:1290` (`heartbeat_loop`), the frontier from the sends at `src/server.tl:868-884` |
| `(steady, steady)` | `start-view-change` | the election loop: the staggered election wait drives the leader-silence detection | `src/server.tl:1340` (`election_loop`) |
| `(booted, booted)` | `do-nothing` | the recovery loop: the fenced-boot drive while the replica is recovering | `src/server.tl:1363` (`recovery_loop`) |
| `(booted, booted)` | `do-nothing` | the boot-time discovery rounds across the remembered set, bounded by the discovery deadline | `src/server.tl:544` (`discovery_loop`) |
| `(stopping, stopping)` | `do-nothing` | the stop hook: the runtime's teardown machinery runs the hosted node's graceful stop exactly once, synchronous only | `src/server.tl:355` |

The pair is the host's own: a node reading its status plane and firing
its own timer names the condition it is in and the wait that expired, and
that is the pair the matcher is asked about. Where a host has no clock
of its own for a state, the row says so by naming the flavour the state
would have to be waited on — a stopping node's own wait never fires
because no timer re-drives the drain, which is the `do-nothing` the
matcher answers on a stopping node.

### The named divergences

One of the eighteen rows does not do what the matcher answers. It is
named here with its reason; the audit test refuses a divergence that is
not named, so a silent exception cannot enter the census.

| Divergence | Rows | The reason |
|---|---|---|
| `boot-machine-reask` | the five `(booted, booted)` rows | a booted node's cadence re-asks what the boot machine owes, and the matcher answers do-nothing on every flavour of a booted node because the machine owns its own progress |

The other seventeen rows are the matcher's own opinion, and the audit
test proves each one it can reach by observation or by the drive's own
anchors: the resend multiset the cluster wait owes a leader whose
follower never answered, re-sent byte for byte with nothing re-proposed
and no slot moved (`examples/lease-sequencer/tests/relay_test.rs`); the
heartbeat of a commit, the leader's last commit re-announced, which
advances no journal slot across any number of beats
(`examples/lease-sequencer/tests/relay_test.rs`); the armed view-change
attempt re-asked at one unchanged ballot across ten timeouts
(`examples/lease-sequencer/tests/relay_test.rs`); the steady member's
wait puts the exact view-change fence exchange on the wire
(`StartViewChange` out, the leader's own back, `DoViewChange` with the
evidence, `StartView` installing the ballot) and advances the ballot by
exactly one view; the host-forced view advances the ballot to the view it
names and the core refuses one that does not strictly advance; the booted
node's cadence puts the `Reincarnation(old, new)` re-announce on the wire
to every peer; and the stopping node's own wait emits nothing at all while
its marker rounds land.

## The relay: what a repeated send may be

A repeated send is a relay of what the core has already released, never a
message the host composes (`ext/uvrr-core/docs/architecture.md`, the
authorization rule for replayed messages; retransmission is host transport
policy). The hosts keep that discipline in one ledger
(`examples/lease-sequencer/src/relay.rs`): every datagram the node
released that the protocol gives a response to is remembered with its
peer, ballot, slot and bytes, and an arriving datagram retires the
entries its tag answers. The ledger's answer table is the protocol's own
— a `Prepare` is answered by its recipient's `PrepareOk` at that slot or
later, a `Commit` by a later `PrepareOk` (the proof it was applied), a
`StartViewChange` fence vote by the recipient's own fence vote, and
`DoViewChange` evidence by the designated primary installing a view — and
every relay re-sends the recorded bytes to the recorded peer, so a relay
is a repeat a receiver absorbs (`ext/uvrr-core/docs/architecture.md`: a
repeated `Prepare` re-acknowledges without re-applying, the commit handler
takes the frontier each time).

Three waits draw on it, and each names its own log event so an operator
reading the JSON lines can tell a repeat from a first send: `relay-out`
carries the peer, the ballot, the slot, the tag and the byte length the
original carried, beside the `why` that fired it — `op-deadline`,
`lock-deadline`, `admin-deadline`, `embedded-deadline`,
`viewchange-poll` or `heartbeat`. The leader's own last commit is kept
beside the un-answered set, because the heartbeat re-announces the
frontier whether or not a response ever retired it.
