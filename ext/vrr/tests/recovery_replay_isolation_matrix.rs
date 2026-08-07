//! Item 06 verification matrix: the explicit `Status::Replaying` phase keeps
//! recovery isolated through committed reconstruction and activates exactly
//! when `executed` reaches `commit`.
//!
//! Independent verification of the foreground F03 fix (`Status::Replaying`,
//! the `in_recovery()` fencing of leader-timeout and epoch-change paths, and
//! frontier-exact activation in `Replica::complete` / `finish_recovery` in
//! `src/vrr.rs`), complementing the item 05 exposure regression in
//! `recovery_early_activation_regression.rs` and the `Status::Recovering`
//! matrices in `recovery_isolation_completion_matrix.rs`:
//!
//! - local-input arm: every non-completion local input is fenced during the
//!   replay phase (`Recover` restarts the attempt, as during `Recovering`);
//!   `Complete` is the only replay-progress input;
//! - peer-body arm: every peer body — normal-operation, epoch-change, and
//!   recovery forms — is fenced without mutation while replay is unfinished;
//! - activation arm: only completions matching the in-flight committed slot
//!   advance replay, and activation happens exactly when `executed` reaches
//!   `commit` — never earlier, never later;
//! - zero-commit arm: a quorum installing no unexecuted committed work
//!   activates immediately;
//! - prefix arm: committed reconstruction resumes at the preserved executed
//!   prefix, re-executing nothing already executed;
//! - cache arm: replayed results are cached and reply from cache once the
//!   replica later leads, and subsequent normal-operation prepares work.

mod support;

use support::{
    assert_complete_cases, assert_replica_unchanged, complete_committed_replay, entry, id, message,
    node, receive, request, state, ReplicaSnapshot,
};
use vrr::vrr::{Body, Input, LogEntry, LogState, Message, NodeId, Output, Replica, Status};

const NONCE: u64 = 7;
const CLIENT: u64 = 10;
/// Recoverer: node 0 of a 3-member cluster; quorum is 2 distinct responses.
const RECOVERER: usize = 0;
/// Node 2 leads epoch 2 (`leader_of(e) = e % 3`); recovery completes there.
const LEADER: NodeId = 2;
/// Node 1 is a nonleader at epoch 2 and leads epoch 1.
const OTHER: NodeId = 1;
/// Recovery completes into epoch 2; the recoverer then leads epoch 3.
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

/// Drives a fresh recoverer into the replay phase: quorum completion installs
/// slot 3 / commit 3 with `executed == 0`, emits the first committed-replay
/// `Output::Execute`, and leaves the replica in `Status::Replaying`.
fn replaying() -> Replica {
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
    let out = receive(&mut replica, OTHER, response(EPOCH, None));
    assert!(
        matches!(out.as_slice(), [Output::Execute { slot: 1, .. }]),
        "quorum completion emits the first committed-replay execution; got {out:?}"
    );
    assert_eq!(replica.status(), Status::Replaying);
    assert_eq!(replica.epoch(), EPOCH);
    assert_eq!(replica.executed(), 0);
    assert_eq!(replica.commit(), 3);
    replica
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum LocalInput {
    Request,
    Idle,
    LeaderTimeout,
    Complete,
    Recover,
}

impl LocalInput {
    const ALL: [Self; 5] = [
        Self::Request,
        Self::Idle,
        Self::LeaderTimeout,
        Self::Complete,
        Self::Recover,
    ];
}

/// Local-input arm: during the replay phase every local input except
/// `Complete` (the replay-progress input) and host `Recover` (which restarts
/// the recovery attempt, exactly as during `Status::Recovering`) is refused
/// without mutation.
#[test]
fn replaying_local_input_matrix_fences_everything_except_complete_and_host_recover() {
    assert_complete_cases("replaying local inputs", 5, LocalInput::ALL);

    for input in LocalInput::ALL {
        let mut replica = replaying();
        let before = ReplicaSnapshot::capture(&replica);
        match input {
            LocalInput::Request => {
                assert!(replica.step(request(1, 1)).is_empty());
                assert_replica_unchanged(input, &before, &replica);
            }
            LocalInput::Idle => {
                assert!(replica.step(Input::Idle).is_empty());
                assert_replica_unchanged(input, &before, &replica);
            }
            LocalInput::LeaderTimeout => {
                assert!(replica.step(Input::LeaderTimeout).is_empty());
                assert_replica_unchanged(input, &before, &replica);
            }
            LocalInput::Complete => {
                let out = replica.step(Input::Complete {
                    slot: 1,
                    result: b"result".to_vec(),
                });
                assert_eq!(
                    replica.executed(),
                    1,
                    "Complete is the replay-progress input"
                );
                assert!(
                    matches!(out.as_slice(), [Output::Execute { slot: 2, .. }]),
                    "completion emits the next committed execution; got {out:?}"
                );
                assert_eq!(
                    replica.status(),
                    Status::Replaying,
                    "a non-final completion does not activate"
                );
            }
            LocalInput::Recover => {
                assert_eq!(
                    replica.step(Input::Recover { nonce: NONCE + 1 }),
                    vec![Output::Broadcast(message(
                        EPOCH,
                        3,
                        Body::Recovery { nonce: NONCE + 1 },
                    ))],
                    "host recovery restarts the recovery attempt"
                );
                assert_eq!(replica.status(), Status::Recovering);
            }
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum PeerBody {
    Prepare,
    PrepareOk,
    Commit,
    StartEpochChange,
    DoEpochChange,
    StartEpoch,
    Recovery,
    RecoveryResponse,
}

impl PeerBody {
    const ALL: [Self; 8] = [
        Self::Prepare,
        Self::PrepareOk,
        Self::Commit,
        Self::StartEpochChange,
        Self::DoEpochChange,
        Self::StartEpoch,
        Self::Recovery,
        Self::RecoveryResponse,
    ];
}

/// Peer-body arm: during the replay phase every peer body is fenced without
/// mutation. Each message is shaped to be admissible in the phase where the
/// body is normally processed (correct epoch, provenance, slot, and a
/// structurally valid state where carried), so the refusal is attributable to
/// the replay phase alone.
#[test]
fn replaying_peer_body_matrix_fences_every_body() {
    assert_complete_cases("replaying peer bodies", 8, PeerBody::ALL);

    for body in PeerBody::ALL {
        let mut replica = replaying();
        let before = ReplicaSnapshot::capture(&replica);
        let (from, message) = match body {
            // Would be admitted in `Status::Normal` (item 05 pins this pair).
            PeerBody::Prepare => (
                LEADER,
                message(
                    EPOCH,
                    4,
                    Body::Prepare {
                        commit: 3,
                        entry: entry(4, CLIENT, 4),
                    },
                ),
            ),
            PeerBody::PrepareOk => (OTHER, message(EPOCH, 3, Body::PrepareOk)),
            PeerBody::Commit => (LEADER, message(EPOCH, 3, Body::Commit)),
            // Would be admitted outside recovery (item 05 pins this one).
            PeerBody::StartEpochChange => (OTHER, message(EPOCH + 1, 3, Body::StartEpochChange)),
            // The recoverer leads epoch 3, so only the recovery fence keeps
            // this report from being collected.
            PeerBody::DoEpochChange => (
                OTHER,
                message(
                    EPOCH + 1,
                    3,
                    Body::DoEpochChange {
                        latest_normal: EPOCH,
                        state: committed_state(),
                    },
                ),
            ),
            // Node 1 leads epoch 4, so provenance is valid.
            PeerBody::StartEpoch => (
                OTHER,
                message(
                    EPOCH + 2,
                    3,
                    Body::StartEpoch {
                        state: committed_state(),
                    },
                ),
            ),
            // Would be answered in `Status::Normal`.
            PeerBody::Recovery => (
                OTHER,
                message(EPOCH, 3, Body::Recovery { nonce: NONCE + 1 }),
            ),
            // A late quorum response: would be admissible in
            // `Status::Recovering` (nonce match, valid provenance and state).
            PeerBody::RecoveryResponse => (LEADER, response(EPOCH, Some(committed_state()))),
        };
        assert!(receive(&mut replica, from, message).is_empty(), "{body:?}");
        assert_replica_unchanged(body, &before, &replica);
        assert_eq!(
            replica.status(),
            Status::Replaying,
            "{body:?} must not leave the replay phase"
        );
    }
}

/// Activation arm: completions that do not match the in-flight committed slot
/// — a gap or a slot beyond the committed frontier — are refused without
/// mutation; matching completions drive replay, and activation happens
/// exactly when `executed` reaches `commit` (asserted per step by the shared
/// replay helper).
#[test]
fn only_matching_completions_advance_replay_and_activation_is_exact() {
    let mut replica = replaying();

    for slot in [2u64, 4] {
        let before = ReplicaSnapshot::capture(&replica);
        assert!(replica
            .step(Input::Complete {
                slot,
                result: b"early".to_vec(),
            })
            .is_empty());
        assert_replica_unchanged(slot, &before, &replica);
        assert_eq!(replica.status(), Status::Replaying);
    }

    complete_committed_replay(&mut replica);
}

/// Zero-commit arm: a quorum installing no unexecuted committed work
/// activates immediately — no replay phase, no completion input — and normal
/// traffic is admitted at once.
#[test]
fn zero_commit_recovery_activates_immediately() {
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
        response(EPOCH, Some(state(vec![], 0)))
    )
    .is_empty());
    let out = receive(&mut replica, OTHER, response(EPOCH, None));
    assert!(out.is_empty(), "zero-commit recovery has nothing to replay");
    assert_eq!(
        replica.status(),
        Status::Normal,
        "zero-commit recovery activates immediately at quorum completion"
    );
    assert_eq!(replica.epoch(), EPOCH);

    let out = receive(
        &mut replica,
        LEADER,
        message(
            EPOCH,
            1,
            Body::Prepare {
                commit: 0,
                entry: entry(1, CLIENT, 1),
            },
        ),
    );
    assert_eq!(
        out,
        vec![Output::To(LEADER, message(EPOCH, 1, Body::PrepareOk))],
        "post-activation Prepare is admitted and acked without any completion"
    );
}

/// Prefix arm: the locally executed committed prefix survives recovery, and
/// reconstruction resumes exactly at that frontier — only the unexecuted
/// committed suffix is emitted, and completions for already-executed slots
/// are refused without mutation.
#[test]
fn replay_resumes_from_the_preserved_executed_prefix() {
    let mut replica = node(3, RECOVERER);
    // Install and execute a two-slot committed prefix in epoch 1 (node 1 leads).
    let prefix = state(committed_log()[..2].to_vec(), 2);
    let out = receive(
        &mut replica,
        OTHER,
        message(1, 2, Body::StartEpoch { state: prefix }),
    );
    assert!(matches!(out.as_slice(), [Output::Execute { slot: 1, .. }]));
    let out = replica.step(Input::Complete {
        slot: 1,
        result: b"prefix".to_vec(),
    });
    assert!(matches!(out.as_slice(), [Output::Execute { slot: 2, .. }]));
    assert!(replica
        .step(Input::Complete {
            slot: 2,
            result: b"prefix".to_vec(),
        })
        .is_empty());
    assert_eq!(replica.executed(), 2);
    assert_eq!(replica.status(), Status::Normal);

    // Recovery installs one more committed slot over the preserved prefix.
    assert_eq!(
        replica.step(Input::Recover { nonce: NONCE }),
        vec![Output::Broadcast(message(
            1,
            2,
            Body::Recovery { nonce: NONCE }
        ))]
    );
    assert!(receive(
        &mut replica,
        LEADER,
        response(EPOCH, Some(committed_state()))
    )
    .is_empty());
    let out = receive(&mut replica, OTHER, response(EPOCH, None));
    assert!(
        matches!(out.as_slice(), [Output::Execute { slot: 3, .. }]),
        "replay resumes at the executed frontier; got {out:?}"
    );
    assert_eq!(replica.status(), Status::Replaying);
    assert_eq!(
        replica.executed(),
        2,
        "executed prefix is preserved across recovery"
    );

    // Already-executed slots are not re-completed.
    for slot in 1..=2u64 {
        let before = ReplicaSnapshot::capture(&replica);
        assert!(replica
            .step(Input::Complete {
                slot,
                result: b"replay".to_vec(),
            })
            .is_empty());
        assert_replica_unchanged(slot, &before, &replica);
    }

    complete_committed_replay(&mut replica);
    assert_eq!(replica.executed(), 3);
}

/// Cache arm: completions during replay rebuild the cached client results
/// without emitting replies, the cache replies once the replica later leads,
/// and subsequent normal-operation prepares work after replay.
#[test]
fn cached_results_and_new_prepares_work_after_replay() {
    let mut replica = replaying();
    for slot in 1..=3u64 {
        replica.step(Input::Complete {
            slot,
            result: format!("result-{slot}").into_bytes(),
        });
    }
    assert_eq!(replica.status(), Status::Normal);
    assert_eq!(replica.executed(), 3);

    // Epoch change into epoch 3, which this replica leads.
    assert_eq!(
        replica.step(Input::LeaderTimeout),
        vec![Output::Broadcast(message(3, 3, Body::StartEpochChange))]
    );
    assert!(receive(&mut replica, OTHER, message(3, 3, Body::StartEpochChange)).is_empty());
    let out = receive(
        &mut replica,
        LEADER,
        message(
            3,
            3,
            Body::DoEpochChange {
                latest_normal: EPOCH,
                state: committed_state(),
            },
        ),
    );
    assert!(out.iter().any(|output| matches!(
        output,
        Output::Broadcast(Message {
            body: Body::StartEpoch { .. },
            ..
        })
    )));
    assert_eq!(replica.status(), Status::Normal);
    assert!(replica.is_leader());
    assert_eq!(replica.epoch(), 3);
    assert_eq!(
        replica.executed(),
        3,
        "epoch change preserves the executed frontier"
    );

    // The replayed client results are cached: the client's final request
    // number replies from cache without re-execution.
    let cached = replica.step(Input::Request {
        client_id: CLIENT,
        request_num: 3,
        message_id: id(3),
        execution_time: 100,
        payload: b"retry".to_vec(),
    });
    assert_eq!(cached, vec![Output::Reply(b"result-3".to_vec())]);
    assert_eq!(replica.slot(), 3, "cached reply does not re-prepare");

    // Subsequent normal behavior: a new request is prepared at the next slot.
    let out = replica.step(Input::Request {
        client_id: CLIENT,
        request_num: 4,
        message_id: id(4),
        execution_time: 100,
        payload: b"new".to_vec(),
    });
    assert!(matches!(
        out.as_slice(),
        [Output::Broadcast(Message {
            slot: 4,
            body: Body::Prepare { commit: 3, .. },
            ..
        })]
    ));
    assert_eq!(replica.slot(), 4);
}
