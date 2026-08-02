# Corrected Formalism

This reviewer walkthrough specifies the fixed-membership crash-fault protocol independently of its
wire encoding and implementation. Read each section as a required invariant or transition, not as a
test-coverage claim.

## Terminology mapping

Paper names appear here only to help readers cross-reference the source protocol.

| Project term | Paper term |
|---|---|
| epoch | view-number / view |
| epoch change | view change |
| leader | primary |
| slot | op-number / log index |
| commit slot | commit-number |
| `PREPARE_OK` | `PREPAREOK` |
| `START_EPOCH_CHANGE` | `STARTVIEWCHANGE` |
| `DO_EPOCH_CHANGE` | `DOVIEWCHANGE` |
| `START_EPOCH` | `STARTVIEW` |
| `RECOVERY_RESPONSE` | `RECOVERYRESPONSE` |
| configuration generation | reconfiguration epoch-number |

## Fixed membership and quorums

Every replica has the same immutable, strictly sorted array `M` of unique identities. A node ID is a
zero-based `u32` index in `M`. For any odd or even `K = |M| >= 3`:

```text
f = floor((K - 1) / 2)
Q = floor(K / 2) + 1 = K - f
leader(epoch) = M[epoch mod K]
```

`Q` is a strict majority, so two `Q`-member quorums intersect. At most `f` replicas may be failed at
once. With `K = 3`, `f = 1` and `Q = 2`; with `K = 4`, `f = 1` and `Q = 3`. The even-sized group
therefore needs two backup acknowledgements after leader self-acceptance, not one.

## Failure and network model

Replicas crash but do not equivocate, forge identities, or behave arbitrarily. Messages may be lost,
delayed, duplicated, or reordered; silence only creates suspicion. Sender membership is validated,
and one sender counts at most once per quorum. Correctness does not require timely delivery or a
foreground disk write while the crash bound preserves enough volatile replicated state.

Progress additionally requires eventual delivery of retries, at most `f` simultaneous failures, a
nonfailed leader able to communicate with `Q - 1` backups, and timers that eventually remain long
enough for useful work.

## Ordered state

The log is contiguous. Slot `0` denotes the empty prefix and the first entry uses slot `1`:

```text
0 <= executed_slot <= commit_slot <= last_slot
```

Slots continue across epoch changes. If an uncommitted assignment is replaced, `(epoch, slot)` is
the proposal token that distinguishes the assignments. The slot remains the only log ordering
coordinate; tokens never authorize a merge.

Each entry stores the complete client envelope:

```text
(client_id, request_num, message_id, opaque value, predicted execution value)
```

`message_id` is unchanged application correlation data. Duplicate suppression uses only
`(client_id, request_num)`.

Replica state includes membership, node ID, epoch, `normal | epoch-change | recovering` status,
latest normal epoch, log, all three slot frontiers, the client table, and deterministic service state.
Only `normal` replicas process normal traffic. Stale epochs are dropped; learning a greater epoch
stops normal processing until authoritative state is acquired.

## Client table

For each `client_id`, the table contains exactly the largest accepted `request_num` and, if that
request executed, its result:

1. A greater request number is admitted and recorded with no result.
2. A lower request number is dropped.
3. An equal request number without a result is dropped because it is already pending.
4. An equal request number with a result resends that result without append or execution.
5. Executing the latest request stores its result before a leader reply is emitted.

Clients use a stable ID, monotonically increasing request numbers, at most one outstanding request,
unchanged-envelope retries, and response matching against that outstanding request. Installing a log
rebuilds largest accepted request numbers. Executing only its committed prefix rebuilds results.

## Message state

The protocol state transitions use these forms:

```text
PREPARE(epoch, slot, envelope, predicted_value, leader_commit_slot)
PREPARE_OK(epoch, slot, node_id)
COMMIT(epoch, commit_slot)
START_EPOCH_CHANGE(epoch, node_id)
DO_EPOCH_CHANGE(epoch, latest_normal_epoch, log, last_slot, commit_slot, node_id)
START_EPOCH(epoch, log, last_slot, commit_slot)
RECOVERY(node_id, nonce)
RECOVERY_RESPONSE(epoch, nonce, node_id, optional leader_state)
```

Only the deterministic leader for a named epoch may originate `PREPARE`, `COMMIT`, or
`START_EPOCH`. Duplicate messages are idempotent.

## Normal operation

For a fresh request, the leader selects any required predicted execution value, appends the complete
entry at `last_slot + 1`, updates the client table with no result, and sends `PREPARE` to all backups.
The append is immediate self-acceptance and counts as one quorum member.

A backup accepts only the next contiguous slot in its current epoch. It obtains every missing entry
before acknowledging and applies the client-table admission update. Its
`PREPARE_OK(epoch, slot, node_id)` certifies acceptance of the entire prefix through that slot.

The leader commits after self-acceptance plus `Q - 1` matching acknowledgements from distinct
backups. It first advances `commit_slot`, then executes newly committed slots exactly once in
increasing order, advances `executed_slot`, stores results, and replies. Backups receive commitment
through `PREPARE` piggybacking or `COMMIT`; they obtain every missing entry through the advertised
frontier, advance `commit_slot`, and only then execute. No replica executes an uncommitted entry or
skips a gap.

## Two-exchange epoch change

On timeout or evidence of a greater epoch, a replica adopts that epoch, enters `epoch-change`, stops
normal processing, and broadcasts `START_EPOCH_CHANGE`. This first exchange is the quorum fence.

A replica may send `DO_EPOCH_CHANGE` only after its own `START_EPOCH_CHANGE` plus matching messages
from `Q - 1` distinct peers. The prospective leader qualifies its own state by the same rule; its
position does not qualify the report implicitly.

The prospective leader waits for `Q` qualified reports from distinct replicas, including its own.
From exactly those reports it:

1. Selects the single report with lexicographically maximal `(latest_normal_epoch, last_slot)`.
2. Adopts that report's entire log, with no per-slot vote and no merge of entries or suffixes.
3. Sets `last_slot` from the selected log.
4. Sets `commit_slot` to the maximum reported commit slot after ensuring the selected log contains
   that committed prefix.
5. Installs the one selected log with `START_EPOCH`.

The installed client table is reconstructed in two stages: accepted request numbers from the whole
log, then cached results by executing only the committed prefix in slot order. Slots do not reset;
the next proposal is `last_slot + 1`. An installed uncommitted suffix may be acknowledged with
`PREPARE_OK(epoch, last_slot, node_id)` but cannot execute before commitment. Entering a greater
epoch clears old acknowledgement state keyed by `(epoch, slot)`.

## Isolated recovery

A restarting replica enters `recovering` with a host-supplied fresh nonce that cannot repeat across
attempts, including across process restarts, and sends `RECOVERY` to every other member. The host
obtains that nonce from durable monotonic state or another source with the same nonrepetition
guarantee. The replica ignores mismatched nonces. Until completion it accepts no `PREPARE`, sends
no `PREPARE_OK`, contributes to neither epoch-change exchange, answers no other recovery, and
cannot count itself as a responder.

Only `normal` replicas respond. Every `RECOVERY_RESPONSE` carries epoch, nonce, and sender node ID.
Only the deterministic leader of that response's exact epoch includes leader state: its log,
`last_slot`, and `commit_slot`.

The recoverer needs `Q` distinct other normal responders. It determines the exact maximum epoch in
that collected quorum and requires the leader of exactly that epoch, carrying state, among those
responses. Thus `Q + 1` replicas communicate including the recoverer. It installs that one
authoritative log and frontiers, reconstructs accepted request numbers, executes the committed
prefix in slot order to reconstruct results, sets epoch and latest normal epoch, and only then enters
`normal`.

## Predicted execution values

Any nondeterministic input is selected before replication and stored in the entry. Every executor
uses that unchanged predicted execution value; none samples a local clock or random source during
execution. The public Teal API is `Node:request(execution_time, json)`: predicted execution metadata
precedes the opaque payload under NOMA Collected Ordering.

## Reviewer obligations

A conforming execution preserves one installed history per epoch, every committed slot assignment,
prefix commitment, exactly-once in-order committed execution, client deduplication, epoch fencing,
qualified whole-log state selection, and exact-maximum-epoch recovery. The key intersection argument
is that every commit, epoch-change report set, and relevant recovery set has `Q` members.

Safety does not imply progress in a fully asynchronous run. Liveness requires continued client and
protocol retries, eventual delivery among enough replicas, a usable deterministic leader, stable
timing long enough to form quorums, and `Q` other normal responders for recovery.

## Optimizations and scope

Commit piggybacking, batching, checkpoints, persistence, and compact state transfer may change cost
but not quorum thresholds, epoch fencing, whole-log selection, execution ordering, client-table
semantics, or recovery completion. Compact transfer must reconstruct exactly one authoritative log
and committed prefix before activation.

Dynamic membership and configuration generations are out of scope, as are witnesses, leader leases,
backup reads, Byzantine faults, and concrete checkpoint or garbage-collection formats. In explicit
paper-reference terminology, its reconfiguration epoch-number is a configuration generation, not
this project's fixed-membership epoch.
