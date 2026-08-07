//! Source 01 R1 / F02 regression: recovery evidence must be monotonic per sender.
//!
//! `Replica::receive` stores each admissible `RecoveryResponse` with
//! `self.recovery.insert(from, (epoch, state))` (`src/vrr.rs`), unconditionally
//! replacing any earlier response from the same sender. Under the permitted
//! network model (duplicated and reordered messages — e.g. a duplicated
//! `Recovery` broadcast draws one response from a node before and one after it
//! advances, delivered newest-first) a delayed older same-sender response
//! overwrites newer evidence and quorum completion then installs stale state.
//!
//! The first two tests assert the required monotonic contract and fail on the
//! defective core. The last two are controls (fortunate ordering, exact
//! duplicates) that pass before and after a fix, pinning that the defect is
//! specifically the nonmonotonic overwrite.

mod support;

use support::{
    assert_replica_unchanged, complete_committed_replay, entry, message, node, receive, state,
    ReplicaSnapshot,
};
use vrr::vrr::{Body, Input, LogEntry, LogState, Message, NodeId, Output, Replica, Status};

const NONCE: u64 = 7;
const CLIENT: u64 = 10;
/// Recoverer: node 0 of a 3-member cluster; quorum is 2 distinct responses.
const RECOVERER: usize = 0;
/// Node 2 leads epoch 2 (`leader_of(e) = e % 3`).
const LEADER: NodeId = 2;
/// Node 1 leads epoch 1 and is a nonleader at epoch 2.
const OTHER: NodeId = 1;

fn committed_log() -> Vec<LogEntry> {
    vec![
        entry(1, CLIENT, 1),
        entry(2, CLIENT, 2),
        entry(3, CLIENT, 3),
    ]
}

/// The leader's newest committed state: slot 3, commit 3.
fn committed_state() -> LogState {
    state(committed_log(), 3)
}

/// An older committed prefix of the same log: slot 1, commit 1.
fn prefix_state() -> LogState {
    state(committed_log()[..1].to_vec(), 1)
}

fn response(epoch: u32, state: Option<LogState>) -> Message {
    let slot = state.as_ref().map_or(0, |state| state.slot);
    message(
        epoch,
        slot,
        Body::RecoveryResponse {
            nonce: NONCE,
            state,
        },
    )
}

fn recovering() -> Replica {
    let mut replica = node(3, RECOVERER);
    assert_eq!(
        replica.step(Input::Recover { nonce: NONCE }),
        vec![Output::Broadcast(message(
            0,
            0,
            Body::Recovery { nonce: NONCE }
        ))]
    );
    replica
}

/// R1 reorder, epoch arm: the epoch-2 leader's committed state arrives first,
/// then a delayed older response from the same sender (sent while it was an
/// epoch-1 nonleader, so it honestly carries no state) must not overwrite it.
/// Quorum completion must install the newest evidence, not the stale slot-0
/// state. Fails on the defective core: the overwrite drops epoch 2 to epoch 1.
#[test]
fn newer_epoch_then_delayed_older_same_sender_response_does_not_overwrite() {
    let mut replica = recovering();

    // Newest evidence first: epoch-2 leader, slot 3, commit 3.
    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "one response is not a quorum"
    );

    // Delayed older same-sender response: epoch 1, no state (not the epoch-1 leader).
    assert!(receive(&mut replica, LEADER, response(1, None)).is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "still one distinct sender"
    );

    // The epoch-1 leader's empty state completes the quorum.
    receive(&mut replica, OTHER, response(1, Some(state(vec![], 0))));

    assert_eq!(
        replica.status(),
        Status::Replaying,
        "quorum completes recovery into the replay phase: the committed prefix is unexecuted"
    );
    assert_eq!(
        replica.epoch(),
        2,
        "recovery must select the maximum epoch from monotonic per-sender evidence"
    );
    assert_eq!(
        replica.slot(),
        3,
        "quorum must not install the stale slot-0 state"
    );
    assert_eq!(
        replica.commit(),
        3,
        "committed state must survive reordering"
    );
    assert_eq!(
        replica.log(),
        committed_log().as_slice(),
        "adopted log must be the newest evidence"
    );
    complete_committed_replay(&mut replica);
}

/// R1 reorder, same-epoch arm: the leader's newest committed state arrives
/// first, then a delayed same-epoch response from the same leader carrying an
/// older committed prefix must not overwrite it. Fails on the defective core:
/// the quorum installs slot 1 / commit 1 instead of slot 3 / commit 3.
#[test]
fn same_epoch_newer_state_then_delayed_older_state_does_not_overwrite() {
    let mut replica = recovering();

    // Newest committed state first.
    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    // Delayed same-sender, same-epoch response carrying an older committed prefix.
    assert!(receive(&mut replica, LEADER, response(2, Some(prefix_state()))).is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "still one distinct sender"
    );

    // A nonleader response at epoch 2 completes the quorum.
    receive(&mut replica, OTHER, response(2, None));

    assert_eq!(
        replica.status(),
        Status::Replaying,
        "quorum completes recovery into the replay phase: the committed prefix is unexecuted"
    );
    assert_eq!(replica.epoch(), 2);
    assert_eq!(
        replica.slot(),
        3,
        "same-epoch older state must not overwrite the newer committed state"
    );
    assert_eq!(replica.commit(), 3, "committed prefix must not roll back");
    assert_eq!(
        replica.log(),
        committed_log().as_slice(),
        "adopted log must be the newest evidence"
    );
    complete_committed_replay(&mut replica);
}

/// Order control: with fortunate ordering (older same-sender response first,
/// newer second) even the unconditional insert retains the newest evidence, so
/// recovery installs the committed state. Passes before and after a fix and
/// pins that the defect is arrival-order-dependent, not blanket breakage.
#[test]
fn older_then_newer_same_sender_response_recovers_the_newest_state() {
    let mut replica = recovering();

    assert!(receive(&mut replica, LEADER, response(1, None)).is_empty());
    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "still one distinct sender"
    );

    receive(&mut replica, OTHER, response(2, None));

    assert_eq!(
        replica.status(),
        Status::Replaying,
        "quorum completes recovery into the replay phase: the committed prefix is unexecuted"
    );
    assert_eq!(replica.epoch(), 2);
    assert_eq!(replica.slot(), 3);
    assert_eq!(replica.commit(), 3);
    assert_eq!(replica.log(), committed_log().as_slice());
    complete_committed_replay(&mut replica);
}

/// Duplicate control: an exact same-sender duplicate is idempotent — no
/// observable mutation, and quorum completion still installs the newest state.
/// Passes before and after a fix.
#[test]
fn exact_duplicate_same_sender_response_is_idempotent() {
    let mut replica = recovering();

    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    let before = ReplicaSnapshot::capture(&replica);
    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    assert_replica_unchanged("exact duplicate leader response", &before, &replica);

    receive(&mut replica, OTHER, response(2, None));

    assert_eq!(
        replica.status(),
        Status::Replaying,
        "quorum completes recovery into the replay phase: the committed prefix is unexecuted"
    );
    assert_eq!(replica.epoch(), 2);
    assert_eq!(replica.slot(), 3);
    assert_eq!(replica.commit(), 3);
    assert_eq!(replica.log(), committed_log().as_slice());
    complete_committed_replay(&mut replica);
}
