# Internal VRR Protocol

Replica messages are binary strings at the Teal boundary. Each message is one fixed header followed
by a serde JSON body.

## Membership and leadership

Membership is one immutable, identical, strictly sorted array `M` of unique `hostname:port`
identities at every replica. `K = |M|` may be odd or even and must be at least 3. A node ID is a
zero-based `u32` index in `M`. Define:

```text
f = floor((K - 1) / 2)
Q = floor(K / 2) + 1 = K - f
```

`Q` is a strict majority and the protocol tolerates at most `f` simultaneous crashes. For `K = 4`,
`f = 1` and `Q = 3`; the extra replica increases the quorum without increasing the failure bound.
The leader of epoch `e` is `M[e mod K]`. Sender identities come from `receive(from, bytes)`, are
validated against `M`, and count at most once in a quorum.

Project `epoch` maps to the paper's fixed-membership view-number. The paper's Section 7 epoch-number
is a distinct configuration generation; reconfiguration and configuration generations are outside
current scope.

## Header and messages

Every peer message starts with exactly 16 bytes in network byte order:

| Offset | Width | Type | Meaning |
|---:|---:|---|---|
| 0 | 4 | `u32` | Message tag |
| 4 | 4 | `u32` | Epoch |
| 8 | 8 | `u64` | Slot |

The body variant must agree with the header tag. A mismatch is rejected.

| Tag | Message |
|---:|---|
| `0x10` | `PREPARE` |
| `0x11` | `PREPARE_OK` |
| `0x12` | `COMMIT` |
| `0x20` | `START_EPOCH_CHANGE` |
| `0x21` | `DO_EPOCH_CHANGE` |
| `0x22` | `START_EPOCH` |
| `0x30` | `RECOVERY` |
| `0x31` | `RECOVERY_RESPONSE` |

## Log entry and frontiers

A `PREPARE` carries the leader's current commit slot and one log entry containing the complete client
envelope plus the leader-selected deterministic predicted execution value:

| Field | Meaning |
|---|---|
| `slot` | Position assigned by the leader |
| `client_id` | Stable client-table key |
| `request_num` | Per-client monotonic request number |
| `message_id` | Correlation identifier; not a duplicate-suppression key |
| `execution_time` | Predicted execution value selected before replication |
| `payload` | Original client bytes, opaque to replication |

`last_slot` is the final contiguous log position, `commit_slot` is the highest committed position,
and `executed_slot` is the highest position applied to the service. They satisfy:

```text
0 <= executed_slot <= commit_slot <= last_slot
```

Slots continue across epoch changes. Proposal identity is `(epoch, slot)` when an uncommitted slot is
reused; this token does not create another ordering coordinate or authorize merging logs.

## Normal operation

Only `normal` replicas process normal traffic. Stale epochs are dropped. Learning a greater epoch
stops normal processing until epoch change or state acquisition installs authoritative state.

```mermaid
sequenceDiagram
    actor C as Client
    participant L as Leader
    participant B as Backup
    participant S as Service black box

    C->>L: complete envelope and opaque payload
    L->>L: deduplicate; predict; append next slot; self-accept
    L->>B: PREPARE(epoch, slot, entry, commit_slot)
    B->>B: obtain gaps; append contiguous entry
    B->>B: advance advertised commit_slot only with complete prefix
    B->>S: execute newly committed slots in order
    B->>L: PREPARE_OK(epoch, slot, node_id)
    Note over L: self-accept plus Q-1 distinct backup acknowledgements
    L->>L: advance commit_slot through acknowledged slot
    L->>S: execute newly committed slots exactly once in order
    S-->>C: store result, then reply through leader
    opt leader becomes idle
        L->>B: COMMIT(epoch, commit_slot)
    end
```

The leader's local append is immediate self-acceptance. Commitment requires `Q - 1` matching
`PREPARE_OK` messages from distinct backups. A backup acknowledges only after accepting every entry
through the named slot, so its acknowledgement certifies a complete contiguous prefix. Before a
backup advances through an advertised commit slot, it obtains every missing entry through that
slot. It executes only after commitment and never skips a gap.

Entering a greater epoch clears acknowledgement state for older `(epoch, slot)` tokens. Old
`PREPARE_OK` acknowledgements can never commit a different entry that later occupies the same slot.

## Epoch change

Epoch change has two exchanges. The first is a quorum fence; the second transfers qualified state
to the prospective leader.

```mermaid
sequenceDiagram
    participant R as Replicas
    participant L as Prospective leader
    participant S as Service black box

    R->>R: adopt greater epoch; status=epoch-change
    R-->>R: START_EPOCH_CHANGE(epoch, node_id)
    Note over R: own message plus Q-1 distinct peers qualify a report
    R->>L: DO_EPOCH_CHANGE(epoch, latest_normal_epoch, whole log, frontiers)
    L->>L: qualify and include its own report by the same rule
    Note over L: require Q qualified reports including its own
    L->>L: select whole log by max(latest_normal_epoch, last_slot)
    L->>L: use maximum reported commit_slot; never merge logs
    L-->>R: START_EPOCH(epoch, selected_log, last_slot, commit_slot)
    R->>R: install one log; rebuild accepted request numbers
    R->>S: execute only committed prefix; rebuild cached results
    opt installed uncommitted suffix exists
        R->>L: PREPARE_OK(epoch, last_slot, node_id)
    end
```

A replica qualifies its `DO_EPOCH_CHANGE` only after its own `START_EPOCH_CHANGE` plus matching
messages from `Q - 1` distinct peers. The prospective leader's report is not implicitly qualified.
It waits for `Q` qualified reports from distinct replicas, including its own qualified report.

From exactly those `Q` reports, the leader selects the single report with lexicographically maximal
`(latest_normal_epoch, last_slot)`. It adopts that report's entire log without merging entries or
suffixes from any other report, sets `last_slot` from that log, and sets `commit_slot` to the maximum
reported commit slot after ensuring the selected log contains that committed prefix.
`START_EPOCH(epoch, selected_log, last_slot, commit_slot)` installs that one selected log. The next
proposal uses `last_slot + 1`. An installed uncommitted suffix is acknowledged with
`PREPARE_OK(epoch, last_slot, node_id)` but is not executed until committed.

Installing a log first rebuilds the largest accepted request number for each client. Executing only
the committed prefix in slot order then rebuilds cached results.

## Recovery

A restarting replica enters `recovering` with a host-supplied fresh nonce that cannot repeat across
attempts, including across process restarts, and sends `RECOVERY(node_id, nonce)` to all other
members. The host obtains the nonce from durable monotonic state or another source with the same
nonrepetition guarantee. Mismatched nonces are ignored. While
recovering, it accepts no `PREPARE`, sends no `PREPARE_OK`, contributes to neither epoch-change
exchange, does not answer another recovery, and does not count among its own responders.

Only a `normal` replica sends `RECOVERY_RESPONSE`. Every response carries its epoch, the current
recovery nonce, and sender node ID. Only the deterministic leader of that response's exact epoch
carries leader state: its one authoritative log, last slot, and commit slot.

```mermaid
sequenceDiagram
    participant X as Recovering replica
    participant R as Normal replicas
    participant L as Leader of exact maximum epoch
    participant S as Service black box

    X-->>R: RECOVERY(node_id, fresh nonce)
    R-->>X: RECOVERY_RESPONSE(epoch, nonce, node_id)
    L-->>X: RECOVERY_RESPONSE(exact maximum epoch, nonce, node_id, leader state)
    Note over X: Q distinct other normal responders
    X->>X: install exact-maximum-epoch leader log and frontiers
    X->>X: rebuild accepted request numbers
    X->>S: execute committed prefix; rebuild cached results
    X->>X: set epoch and latest_normal_epoch; status=normal
```

The recoverer collects `Q` distinct normal responders other than itself, determines the exact maximum
epoch among those responses, and requires the deterministic leader of exactly that epoch to be among
the `Q` with state. Recovery therefore involves `Q + 1` communicating replicas including the
recoverer. It remains isolated until reconstruction and committed execution complete; only then may
it become `normal`.

## Datagram bound

Inbound and outbound peer messages are limited to 65,507 bytes, the maximum UDP payload. The leader
computes the exact encoded `PREPARE` size before mutating its log, so an oversized request cannot
consume a slot that backups never receive.
