//! Source 01 R3 / F03 regression: recovery must not activate `Status::Normal`
//! before committed replay finishes.
//!
//! `Replica::finish_recovery` (`src/vrr.rs`) installs the recovered state with
//! `adopt`, then runs `activate_epoch` — status becomes `Normal` — and only
//! afterwards emits the first reconstruction work via `pending_execution`.
//! The replica therefore spends the whole committed-replay window (every
//! `Input::Complete` from the first emitted `Output::Execute` up to the final
//! committed slot) observable as `Normal`, so its `status == Status::Normal`
//! guards admit normal and epoch-change traffic while the deterministic
//! service state and the cached client results are still incomplete.
//!
//! The first three tests assert the required contract — activation only after
//! the final committed completion, with non-completion traffic fenced during
//! reconstruction — and fail on the defective core. The last is a control
//! (completion inputs drive replay to the committed frontier, after which
//! normal traffic is admitted) that passes before and after a fix, pinning
//! that the defect is premature activation, not the replay mechanism itself.

mod support;

use support::{assert_replica_unchanged, entry, message, node, receive, state, ReplicaSnapshot};
use vrr::vrr::{Body, Input, LogEntry, LogState, Message, NodeId, Output, Replica, Status};

const NONCE: u64 = 7;
const CLIENT: u64 = 10;
/// Recoverer: node 0 of a 3-member cluster; quorum is 2 distinct responses.
const RECOVERER: usize = 0;
/// Node 2 leads epoch 2 (`leader_of(e) = e % 3`).
const LEADER: NodeId = 2;
/// Node 1 is a nonleader at epoch 2.
const OTHER: NodeId = 1;
/// Recovery completes into epoch 2.
const EPOCH: u32 = 2;

fn committed_log() -> Vec<LogEntry> {
    vec![
        entry(1, CLIENT, 1),
        entry(2, CLIENT, 2),
        entry(3, CLIENT, 3),
    ]
}

/// The leader's committed state: slot 3, commit 3 — entirely unexecuted for a
/// fresh recoverer (`executed == 0`).
fn committed_state() -> LogState {
    state(committed_log(), 3)
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

/// Drive a fresh recoverer to quorum completion and return the replica plus
/// the outputs of the completing response.
fn complete_recovery() -> (Replica, Vec<Output>) {
    let mut replica = node(3, RECOVERER);
    assert_eq!(
        replica.step(Input::Recover { nonce: NONCE }),
        vec![Output::Broadcast(message(
            0,
            0,
            Body::Recovery { nonce: NONCE }
        ))]
    );
    assert!(receive(
        &mut replica,
        LEADER,
        response(EPOCH, Some(committed_state()))
    )
    .is_empty());
    assert_eq!(
        replica.status(),
        Status::Recovering,
        "one response is not a quorum"
    );
    let out = receive(&mut replica, OTHER, response(EPOCH, None));
    (replica, out)
}

/// The leader's next normal-operation prepare after the recovered log:
/// slot 4, committing nothing new.
fn next_prepare() -> Message {
    message(
        EPOCH,
        4,
        Body::Prepare {
            commit: 3,
            entry: entry(4, CLIENT, 4),
        },
    )
}

/// R3 headline: quorum completion installs a state whose committed prefix is
/// unexecuted (`executed 0 < commit 3`) and emits the first reconstruction
/// `Output::Execute`, yet the same step already activated `Status::Normal`.
/// Activation must be deferred until the final committed completion. Fails on
/// the defective core: status is `Normal` from the completing response on.
#[test]
fn recovery_emits_replay_execute_but_must_not_activate_normal_before_replay_finishes() {
    let (replica, out) = complete_recovery();
    let first = entry(1, CLIENT, 1);

    assert_eq!(
        out,
        vec![Output::Execute {
            slot: 1,
            client_id: CLIENT,
            request_num: 1,
            message_id: first.message_id,
            execution_time: first.execution_time,
            payload: first.payload,
        }],
        "quorum completion must emit the first committed-replay execution"
    );
    assert_eq!(replica.epoch(), EPOCH);
    assert_eq!(replica.slot(), 3);
    assert_eq!(replica.commit(), 3, "committed prefix installed");
    assert_eq!(
        replica.executed(),
        0,
        "installed committed prefix is entirely unexecuted"
    );
    assert_ne!(
        replica.status(),
        Status::Normal,
        "recovery must not activate Normal before committed replay finishes"
    );
}

/// R3 normal-traffic arm: in the exposed intermediate state (replay execution
/// for slot 1 emitted, no completion fed yet) the leader's next `Prepare` is
/// processed — the log grows and a `PrepareOk` is acked — although the
/// committed prefix is still unexecuted and the cached client results are
/// still unreconstructed. Non-completion traffic must remain fenced until
/// replay finishes. Fails on the defective core.
#[test]
fn leader_prepare_is_processed_while_committed_replay_is_unfinished() {
    let (mut replica, _) = complete_recovery();
    let before = ReplicaSnapshot::capture(&replica);

    let out = receive(&mut replica, LEADER, next_prepare());

    assert!(
        out.is_empty(),
        "Prepare must be fenced while committed replay is unfinished; got {out:?}"
    );
    assert_replica_unchanged("Prepare during committed replay", &before, &replica);
}

/// R3 epoch-change arm: in the same exposed intermediate state, epoch-change
/// traffic for a later epoch is processed — the replica leaves the replay
/// window for `Status::EpochChange` and broadcasts its own `StartEpochChange`.
/// Non-completion traffic must remain fenced until replay finishes. Fails on
/// the defective core.
#[test]
fn start_epoch_change_is_processed_while_committed_replay_is_unfinished() {
    let (mut replica, _) = complete_recovery();
    let before = ReplicaSnapshot::capture(&replica);

    let out = receive(
        &mut replica,
        OTHER,
        message(EPOCH + 1, 3, Body::StartEpochChange),
    );

    assert!(
        out.is_empty(),
        "StartEpochChange must be fenced while committed replay is unfinished; got {out:?}"
    );
    assert_replica_unchanged(
        "StartEpochChange during committed replay",
        &before,
        &replica,
    );
}

/// Control: completion inputs — the one input class that must stay enabled
/// throughout reconstruction — drive the committed replay to the committed
/// frontier, and only then is normal traffic admitted. Passes before and
/// after a fix, pinning that the regression is premature activation, not the
/// completion/replay path itself.
#[test]
fn completions_finish_replay_and_then_normal_traffic_is_admitted() {
    let (mut replica, _) = complete_recovery();

    for slot in 1..=3u64 {
        let out = replica.step(Input::Complete {
            slot,
            result: b"result".to_vec(),
        });
        assert_eq!(replica.executed(), slot);
        if slot < 3 {
            assert!(
                matches!(out.as_slice(), [Output::Execute { slot: next, .. }] if *next == slot + 1),
                "completion must emit the next committed-replay execution; got {out:?}"
            );
        }
    }
    assert_eq!(replica.executed(), replica.commit(), "replay caught up");

    // After the final committed completion the replica is fully activated:
    // the leader's next Prepare is admitted and acked.
    let out = receive(&mut replica, LEADER, next_prepare());
    assert_eq!(
        out,
        vec![Output::To(LEADER, message(EPOCH, 4, Body::PrepareOk))],
        "post-replay Prepare must be acked"
    );
    assert_eq!(replica.slot(), 4);
}
