# Glossary

Project prose uses the terms below. Paper aliases are confined to the mapping so that epoch, slot,
and configuration generation remain distinct.

## Paper mapping

| Project term | Paper term |
|---|---|
| epoch | view-number / view |
| epoch change | view change |
| leader | primary |
| slot | op-number / log index |
| commit slot | commit-number |
| configuration generation | reconfiguration epoch-number |
| `PREPARE_OK` | `PREPAREOK` |
| `START_EPOCH_CHANGE` | `STARTVIEWCHANGE` |
| `DO_EPOCH_CHANGE` | `DOVIEWCHANGE` |
| `START_EPOCH` | `STARTVIEW` |
| `RECOVERY_RESPONSE` | `RECOVERYRESPONSE` |

The paper's op-number/log index means **slot**, its view-number means **epoch**, and its
reconfiguration epoch-number means the out-of-scope **configuration generation**.

## Terms

- **Advisory lock (lock; plural locks):** A cooperative lock identified by `lock_id`. The service
  orders ownership decisions, but clients must voluntarily honor the result; it does not forcibly
  exclude unrelated external systems.
- **Backup:** A non-leader replica that accepts only the next contiguous slot, refuses gaps, and
  executes only committed entries.
- **Client envelope:** The unchanged retry unit containing `client_id`, `request_num`, `message_id`,
  and opaque value.
- **Client table:** Per-client state containing the largest accepted request number and, after that
  request executes, its result.
- **Commit slot:** The highest committed slot; all earlier slots are committed too.
- **Configuration generation:** An out-of-scope identifier for a membership configuration, distinct
  from a fixed-membership epoch.
- **Contiguous acceptance:** A backup acknowledges slot `s` only after accepting every entry through
  `s`. A proposal that would skip entries is refused: there is no gap-filling exchange, so a gapped
  backup stalls until an epoch change or recovery installs authoritative state.
- **Epoch:** A monotonically numbered fixed-membership leadership regime.
- **Epoch change:** The two-exchange procedure that fences an old epoch and installs a later one.
- **Executed slot:** The highest slot applied exactly once to the service.
- **`f`:** The simultaneous crash bound, `floor((K - 1) / 2)`.
- **`K`:** Membership size, any odd or even value at least 3.
- **Last slot:** The final index in a replica's contiguous log.
- **Latest normal epoch:** The greatest epoch in which a replica entered `normal`; the first key in
  epoch-change state selection.
- **Leader:** The replica `M[epoch mod K]`, solely responsible for ordering new requests.
- **Leader state:** The exact-epoch leader's recovery payload containing its log and slot frontiers.
- **Lease:** The value representing a granted advisory lock: `lease_id`, holder UUID, and expiry.
  The lease is live only while its expiry is greater than the replicated predicted execution value.
- **Locking service:** The lightweight replicated service that orders GET and SET operations for
  advisory locks and returns lease decisions.
- **Nonce:** A host-supplied, fresh nonrepeating identifier binding responses to one recovery
  attempt; its source must remain nonrepeating across process restarts.
- **Predicted execution value:** A nondeterministic input chosen before replication, stored in the
  log, and supplied unchanged to every executor.
- **`Q`:** The strict-majority quorum `floor(K / 2) + 1 = K - f`.
- **Qualified report:** A `DO_EPOCH_CHANGE` sent after the sender's own
  `START_EPOCH_CHANGE` plus `Q - 1` matching messages from distinct peers.
- **Recovery isolation:** A recoverer performs no normal, epoch-change, or response work and counts
  in no quorum until reconstruction and committed execution finish; the replay phase carries the
  same isolation.
- **Replay phase (`replaying`):** The explicit recovery phase after state installation in which
  host-driven completions execute the installed committed prefix in slot order, fenced from all
  normal and epoch-change work, until the executed slot reaches the installed commit slot and the
  replica activates as `normal`.
- **Self-accept:** The leader's local append counts immediately, leaving `Q - 1` distinct backup
  acknowledgements for commitment.
- **Slot:** The one-based ordering coordinate for log entries; slot zero denotes the empty prefix and
  slots continue across epoch changes.
- **Token:** `(epoch, slot)`, used to distinguish competing assignments to a reused uncommitted slot,
  not to order or merge logs.
- **Whole-log state selection:** Selection of one complete report with maximal
  `(latest_normal_epoch, last_slot)`, never a merge.

## Messages

- **`PREPARE`:** The leader's proposal carrying epoch, slot, complete envelope, predicted value, and
  leader commit slot.
- **`PREPARE_OK`:** A backup's acknowledgement of `(epoch, slot)` and the complete prefix through it.
- **`COMMIT`:** The leader's idle-time advertisement of its commit slot.
- **`START_EPOCH_CHANGE`:** The first-exchange announcement that fences an earlier epoch.
- **`DO_EPOCH_CHANGE`:** A qualified complete-log report to the prospective leader.
- **`START_EPOCH`:** The installation of one selected log and its slot frontiers.
- **`RECOVERY`:** A recoverer's nonce-bearing state solicitation.
- **`RECOVERY_RESPONSE`:** A normal replica's epoch-and-nonce response; only that exact epoch's
  leader supplies leader state.
