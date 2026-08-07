//! Item 04 verification matrix: same-sender recovery evidence is monotonic.
//!
//! Independent verification of the F02 fix (`Replica::stronger_recovery_response`
//! in `src/vrr.rs`), complementing the item 03 reorder/duplicate regressions in
//! `recovery_evidence_overwrite_regression.rs`. Every fed message is admissible
//! under the core's own recovery admission rules (nonce match, provenance
//! `state.is_some() == (from == leader_of(epoch))`, `valid_state_message`), so
//! each outcome is decided by the per-sender monotonicity gate alone:
//!
//! - epoch arm: a delayed older-epoch response cannot overwrite newer evidence,
//!   even when it carries strictly stronger state content (epoch dominates);
//! - same-epoch frontier arm: leader evidence cannot regress slot or commit —
//!   a higher slot with a lower commit is rejected, and an equal frontier with
//!   different content (a tie) never overwrites;
//! - same-epoch prefix arm: a forked log is rejected even with a strictly
//!   higher slot/commit frontier;
//! - advance arm: older-to-newer same-epoch evidence is accepted and installed;
//! - duplicate arm: exact same-sender duplicates (state or nonleader) are
//!   idempotent no-ops;
//! - completion arm: a stale response against a quorum-sized but blocked map
//!   produces no output and no mutation (no spurious `finish_recovery`), and
//!   the blocked attempt still completes once newer same-sender evidence
//!   arrives.

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
/// Node 2 leads epoch 2 (`leader_of(e) = e % 3`) and trails epochs 0, 1, 4.
const LEADER: NodeId = 2;
/// Node 1 leads epochs 1 and 4 and is a nonleader at epoch 2.
const OTHER: NodeId = 1;

fn committed_log() -> Vec<LogEntry> {
    vec![
        entry(1, CLIENT, 1),
        entry(2, CLIENT, 2),
        entry(3, CLIENT, 3),
    ]
}

/// The leader's committed state: slot 3, commit 3.
fn committed_state() -> LogState {
    state(committed_log(), 3)
}

/// An older committed prefix of the same log: slot 1, commit 1.
fn prefix_state() -> LogState {
    state(committed_log()[..1].to_vec(), 1)
}

/// The same log extended by one committed entry: slot 4.
fn extended_log() -> Vec<LogEntry> {
    let mut log = committed_log();
    log.push(entry(4, CLIENT, 4));
    log
}

/// A fork: the first two entries match `committed_log`, slot 3 onward differ.
fn forked_log() -> Vec<LogEntry> {
    vec![
        entry(1, CLIENT, 1),
        entry(2, CLIENT, 2),
        entry(3, CLIENT + 1, 1),
        entry(4, CLIENT + 1, 2),
    ]
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

/// Asserts a rejected same-sender response: no outputs (so no spurious
/// `finish_recovery`), no observable mutation, still recovering.
fn assert_stale_rejected(
    context: &'static str,
    replica: &mut Replica,
    from: NodeId,
    stale: Message,
) {
    let before = ReplicaSnapshot::capture(replica);
    assert!(
        receive(replica, from, stale).is_empty(),
        "{context}: stale response produced outputs"
    );
    assert_replica_unchanged(context, &before, replica);
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "{context}: stale response finished recovery"
    );
}

/// Asserts quorum completion installed the newest leader evidence and entered
/// the replay phase (the installed committed prefix is unexecuted), then
/// drives `Input::Complete` through the committed frontier and asserts the
/// replica activates with the installed evidence intact.
fn assert_recovered(replica: &mut Replica, epoch: u32, log: &[LogEntry], commit: u64) {
    assert_eq!(
        replica.status(),
        Status::Replaying,
        "quorum completes recovery into the replay phase while the committed prefix is unexecuted"
    );
    assert_eq!(replica.epoch(), epoch, "recovery selects the maximum epoch");
    assert_eq!(replica.slot(), log.len() as u64, "installed log frontier");
    assert_eq!(replica.commit(), commit, "installed commit frontier");
    assert_eq!(replica.log(), log, "installed log content");
    assert_eq!(
        replica.executed(),
        0,
        "installed committed prefix is entirely unexecuted"
    );
    complete_committed_replay(replica);
    assert_eq!(
        replica.epoch(),
        epoch,
        "replay preserves the recovered epoch"
    );
    assert_eq!(
        replica.slot(),
        log.len() as u64,
        "replay preserves the installed log frontier"
    );
    assert_eq!(
        replica.commit(),
        commit,
        "replay preserves the installed commit frontier"
    );
    assert_eq!(replica.log(), log, "replay preserves the installed log");
}

/// Epoch arm, strengthened: node 2's newer epoch-4 nonleader evidence arrives
/// first, then a delayed epoch-2 response from the same sender carrying a
/// strictly stronger committed leader state. Epoch dominates content strength:
/// the older-epoch state must not overwrite. A quorum then completed by
/// lower-epoch evidence alone must stay blocked (the maximum observed epoch is
/// still 4 and its leader's state is absent); only the epoch-4 leader's state
/// may finish the attempt. Fails on the defective core: the overwrite drags
/// the maximum epoch down to 2 and the third response completes with the
/// stale epoch-2 state.
#[test]
fn older_epoch_stronger_state_cannot_overwrite_newer_epoch_evidence() {
    let mut replica = recovering();

    assert!(receive(&mut replica, LEADER, response(4, None)).is_empty());
    assert_stale_rejected(
        "older epoch with stronger state",
        &mut replica,
        LEADER,
        response(2, Some(committed_state())),
    );

    // Node 1's epoch-1 leader state reaches a quorum of two, but cannot
    // complete: the maximum epoch remains 4 and its leader's state is absent.
    assert!(receive(&mut replica, OTHER, response(1, Some(state(vec![], 0)))).is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "stale epoch-2 overwrite would let this quorum complete at epoch 2"
    );

    // Node 1 leads epoch 4; its leader state completes the quorum at epoch 4.
    receive(
        &mut replica,
        OTHER,
        response(4, Some(state(extended_log(), 4))),
    );
    assert_recovered(&mut replica, 4, &extended_log(), 4);
}

/// Same-epoch commit arm: a delayed same-sender response with a higher slot
/// but a lower commit (slot 4 / commit 2 over slot 3 / commit 3) is a
/// lexicographic frontier advance that still regresses commit evidence. It
/// must not overwrite; quorum completion must install commit 3.
#[test]
fn same_epoch_higher_slot_lower_commit_does_not_overwrite() {
    let mut replica = recovering();

    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    assert_stale_rejected(
        "commit regression at higher slot",
        &mut replica,
        LEADER,
        response(2, Some(state(extended_log(), 2))),
    );

    receive(&mut replica, OTHER, response(2, None));
    assert_recovered(&mut replica, 2, &committed_log(), 3);
}

/// Same-epoch prefix arm: a delayed same-sender response with a strictly
/// higher slot/commit frontier but a forked log (diverging at slot 3) is
/// incompatible with the stored evidence and must not overwrite, even though
/// every scalar frontier component advances.
#[test]
fn same_epoch_incompatible_prefix_with_higher_frontier_does_not_overwrite() {
    let mut replica = recovering();

    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    assert_stale_rejected(
        "incompatible prefix at higher frontier",
        &mut replica,
        LEADER,
        response(2, Some(state(forked_log(), 4))),
    );

    receive(&mut replica, OTHER, response(2, None));
    assert_recovered(&mut replica, 2, &committed_log(), 3);
}

/// Same-epoch tie arm: a delayed same-sender response with an identical
/// slot/commit frontier but different log content is not an advance, so it
/// must not overwrite; only exact duplicates and genuine advances leave the
/// stored evidence intact.
#[test]
fn same_epoch_equal_frontier_different_log_does_not_overwrite() {
    let mut replica = recovering();

    let retied = state(forked_log()[..3].to_vec(), 3);
    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    assert_stale_rejected(
        "equal frontier with different content",
        &mut replica,
        LEADER,
        response(2, Some(retied)),
    );

    receive(&mut replica, OTHER, response(2, None));
    assert_recovered(&mut replica, 2, &committed_log(), 3);
}

/// Advance arm, same epoch: the leader's older committed prefix arrives first,
/// its newer extended state second. The newer evidence must replace the older
/// and quorum completion must install it — monotonicity gates staleness, not
/// progress.
#[test]
fn same_epoch_older_to_newer_state_advances_and_is_installed() {
    let mut replica = recovering();

    assert!(receive(&mut replica, LEADER, response(2, Some(prefix_state()))).is_empty());
    assert!(receive(&mut replica, LEADER, response(2, Some(committed_state()))).is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "still one distinct sender"
    );

    receive(&mut replica, OTHER, response(2, None));
    assert_recovered(&mut replica, 2, &committed_log(), 3);
}

/// Duplicate arm, nonleader: an exact duplicate of a state-free response is an
/// idempotent no-op (no outputs, no mutation), and the quorum still completes
/// with the leader's committed state.
#[test]
fn exact_duplicate_nonleader_response_is_idempotent() {
    let mut replica = recovering();

    assert!(receive(&mut replica, OTHER, response(2, None)).is_empty());
    assert_stale_rejected(
        "exact duplicate nonleader response",
        &mut replica,
        OTHER,
        response(2, None),
    );

    receive(&mut replica, LEADER, response(2, Some(committed_state())));
    assert_recovered(&mut replica, 2, &committed_log(), 3);
}

/// Completion arm: with a quorum-sized map blocked because the maximum-epoch
/// leader's stored evidence trails (node 2's stale epoch-1 nonleader response
/// vs node 1's epoch 2), a delayed even-older same-sender response must be a
/// pure no-op — no outputs, no mutation, no spurious `finish_recovery`. The
/// attempt then unblocks when the leader's newer same-epoch evidence arrives,
/// proving the stale rejection wedged nothing.
#[test]
fn stale_response_against_blocked_quorum_neither_finishes_nor_wedges() {
    let mut replica = recovering();

    assert!(receive(&mut replica, OTHER, response(2, None)).is_empty());
    assert!(receive(&mut replica, LEADER, response(1, None)).is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "quorum-sized map blocked: maximum-epoch leader state trails"
    );

    assert_stale_rejected(
        "stale response against blocked quorum",
        &mut replica,
        LEADER,
        response(0, None),
    );

    receive(&mut replica, LEADER, response(2, Some(committed_state())));
    assert_recovered(&mut replica, 2, &committed_log(), 3);
}
