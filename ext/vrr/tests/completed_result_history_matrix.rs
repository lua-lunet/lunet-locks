//! Item 08a verification matrix: the completed log entry's result is recorded
//! unconditionally, and the client-table match gates only the immediate leader
//! reply.
//!
//! Independent verification of the foreground completion-order remedy in
//! `Replica::complete` (`src/vrr.rs`): the freshly computed result is inserted
//! into the `(client_id, request_num)` history map regardless of whether the
//! client table entry still matches the completed entry, while the immediate
//! `Output::Reply` stays gated on that match (plus leader status and
//! `Status::Normal`). `Replica::adopt` restores results from the history map
//! first, so a rolled-back request's exact result is reconstructed. This
//! resumes the deferred item 08 matrix after the item 08 stop-line (completion
//! AFTER same-client successor acceptance dropped the result under the old
//! `matched` guard):
//!
//! - completion-order arm (leader side): a completion delivered AFTER a newer
//!   same-client request was accepted emits NO immediate reply — the client
//!   moved on — but the result is retained and replays with exact bytes once
//!   the newer uncommitted request is rolled back and the replica leads again;
//! - completion-order control: a completion delivered BEFORE the successor
//!   acceptance replies immediately and also survives the rollback;
//! - multiple-clients arm: two clients' latest results survive a rolled-back
//!   two-client suffix with exact bytes, and non-latest retries stay silent
//!   both while a successor is pending and after it is re-prepared;
//! - multiple-suffixes arm: a result completed after TWO same-client successor
//!   acceptances (suffix of three entries, two clients) is restored by the
//!   rollback;
//! - recovery arm: results computed during `Status::Replaying` (which never
//!   reply) are preserved through epoch adoption and rollback, and replay with
//!   exact bytes once the recoverer leads.
//!
//! The completion-order and multiple-suffixes arms fail on the pre-remedy core
//! (the `matched` guard dropped post-acceptance completions from every
//! structure); the remaining arms pin that the reply gating, rollback
//! reconstruction, and recovery replay keep their exact-byte contracts.

mod support;

use support::{entry, message, node, receive, state};
use vrr::vrr::{Body, Input, LogEntry, LogState, Output, Replica, Status};

const CLIENT_A: u64 = 7;
const CLIENT_B: u64 = 9;
const NONCE: u64 = 7;
/// Completion-order arm: result of the older request, completed after the
/// successor acceptance.
const RETAINED_AFTER_ACCEPTANCE: &[u8] = b"retained-after-acceptance";
/// Completion-order control: result completed before the successor acceptance.
const REPLIED_BEFORE_ACCEPTANCE: &[u8] = b"replied-before-acceptance";
/// Multiple-clients arm results.
const CLIENT_A_RESULT: &[u8] = b"client-a-exact-result";
const CLIENT_B_RESULT: &[u8] = b"client-b-exact-result";
/// Multiple-suffixes arm result.
const MULTI_SUFFIX_RESULT: &[u8] = b"multi-suffix-result-one";
/// Recovery arm result, computed during the replay phase.
const REPLAYED_RESULT: &[u8] = b"replayed-exact-result-one";

/// A client request (first attempt or retry) identical to the logged entry.
fn request_of(entry: &LogEntry) -> Input {
    Input::Request {
        client_id: entry.client_id,
        request_num: entry.request_num,
        message_id: entry.message_id,
        execution_time: entry.execution_time,
        payload: entry.payload.clone(),
    }
}

fn execute_output(entry: &LogEntry) -> Output {
    Output::Execute {
        slot: entry.slot,
        client_id: entry.client_id,
        request_num: entry.request_num,
        message_id: entry.message_id,
        execution_time: entry.execution_time,
        payload: entry.payload.clone(),
    }
}

/// Drives a node-0 replica of a 3-member cluster out of epoch 0: the epoch-1
/// leader (node 1) installs `installed` — rolling the uncommitted suffix back —
/// the epoch-2 leader (node 2) installs it again, and the replica then leads
/// the epoch-3 change itself, activating with the same installed state.
/// Asserts the exact message contract at every step, including that the
/// replica's own epoch-1 report still carries its pre-rollback state.
fn rollback_and_relead_epoch_three(replica: &mut Replica, installed: LogState) {
    let reported = LogState {
        slot: replica.slot(),
        commit: replica.commit(),
        log: replica.log().to_vec(),
    };
    let out = replica.step(Input::LeaderTimeout);
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            1,
            reported.slot,
            Body::StartEpochChange
        ))],
        "timeout starts the epoch-1 change"
    );
    let out = receive(
        replica,
        1,
        message(1, reported.slot, Body::StartEpochChange),
    );
    assert_eq!(
        out,
        vec![Output::To(
            1,
            message(
                1,
                reported.slot,
                Body::DoEpochChange {
                    latest_normal: 0,
                    state: reported,
                },
            ),
        )],
        "the replica reports its pre-rollback state to the epoch-1 leader"
    );
    let out = receive(
        replica,
        1,
        message(
            1,
            installed.slot,
            Body::StartEpoch {
                state: installed.clone(),
            },
        ),
    );
    assert!(out.is_empty(), "the rolled-back state installs quietly");
    assert_eq!(replica.epoch(), 1);
    assert_eq!(replica.log(), installed.log.as_slice());
    assert_eq!(replica.commit(), installed.commit);
    assert_eq!(replica.slot(), installed.slot);

    let out = replica.step(Input::LeaderTimeout);
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            2,
            installed.slot,
            Body::StartEpochChange
        ))],
        "timeout starts the epoch-2 change"
    );
    let out = receive(
        replica,
        2,
        message(
            2,
            installed.slot,
            Body::StartEpoch {
                state: installed.clone(),
            },
        ),
    );
    assert!(out.is_empty(), "the same state installs again quietly");
    assert_eq!(replica.epoch(), 2);

    let out = replica.step(Input::LeaderTimeout);
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            3,
            installed.slot,
            Body::StartEpochChange
        ))],
        "timeout starts the epoch-3 change the replica leads"
    );
    let out = receive(
        replica,
        1,
        message(3, installed.slot, Body::StartEpochChange),
    );
    assert!(out.is_empty(), "the leader waits for a report quorum");
    let out = receive(
        replica,
        1,
        message(
            3,
            installed.slot,
            Body::DoEpochChange {
                latest_normal: 2,
                state: installed.clone(),
            },
        ),
    );
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            3,
            installed.slot,
            Body::StartEpoch {
                state: installed.clone(),
            },
        ))],
        "quorum activates epoch 3 and broadcasts the installed state"
    );
    assert_eq!(replica.epoch(), 3);
    assert_eq!(replica.status(), Status::Normal);
    assert!(replica.is_leader(), "the replica leads epoch 3");
}

/// Completion-order arm (leader side): request 1 is committed and its
/// execution is in flight when the same client's request 2 is accepted; only
/// then does the host complete slot 1. The completion must NOT emit an
/// immediate `Reply` — the client table's latest entry already describes
/// request 2 — but the result must be retained in the history map. When a
/// later valid epoch state rolls the uncommitted request 2 back, the rebuilt
/// table entry carries the retained result, and the exact-byte leader retry
/// replays it. Fails on the pre-remedy core: the `matched` guard dropped the
/// post-acceptance completion from every replica structure, so the retry
/// earned no `Reply`.
#[test]
fn completion_after_newer_same_client_request_replies_nothing_and_is_replayed_after_rollback() {
    let mut replica = node(3, 0);
    let a1 = entry(1, CLIENT_A, 1);
    let a2 = entry(2, CLIENT_A, 2);

    let out = replica.step(request_of(&a1));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            1,
            Body::Prepare {
                commit: 0,
                entry: a1.clone(),
            },
        ))],
        "request 1 is prepared at slot 1"
    );
    let out = receive(&mut replica, 1, message(0, 1, Body::PrepareOk));
    assert_eq!(
        out,
        vec![execute_output(&a1)],
        "the ack quorum commits and executes request 1"
    );
    assert_eq!(replica.commit(), 1);

    // The leader pipelines the same client's request 2 while the host's
    // execution of slot 1 is still in flight.
    let out = replica.step(request_of(&a2));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            2,
            Body::Prepare {
                commit: 1,
                entry: a2.clone(),
            },
        ))],
        "request 2 is accepted at slot 2 before slot 1 completes"
    );
    assert_eq!(replica.slot(), 2);

    // The completion-order remedy: the older completion emits NO immediate
    // reply while the newer request is pending, but is recorded in history.
    let out = replica.step(Input::Complete {
        slot: 1,
        result: RETAINED_AFTER_ACCEPTANCE.to_vec(),
    });
    assert_eq!(
        out,
        Vec::new(),
        "no immediate reply for the older completion while request 2 is pending"
    );
    assert_eq!(replica.executed(), 1, "request 1 stays executed");

    // While request 2 is pending: the non-latest retry is dropped and the
    // latest retry earns nothing (no result yet) without re-preparing.
    assert!(replica.step(request_of(&a1)).is_empty());
    assert!(replica.step(request_of(&a2)).is_empty());
    assert_eq!(replica.slot(), 2, "retries do not re-prepare");

    rollback_and_relead_epoch_three(&mut replica, state(vec![a1.clone()], 1));

    let retry = replica.step(request_of(&a1));
    assert_eq!(
        retry,
        vec![Output::Reply(RETAINED_AFTER_ACCEPTANCE.to_vec())],
        "the rollback rebuilds request 1 with its retained exact result"
    );
    assert_eq!(
        replica.step(request_of(&a1)),
        vec![Output::Reply(RETAINED_AFTER_ACCEPTANCE.to_vec())],
        "the replayed reply is stable across retries"
    );

    // A re-prepared successor makes request 1 non-latest again: its retry is
    // dropped and the pending successor still earns no reply.
    let out = replica.step(request_of(&a2));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            3,
            2,
            Body::Prepare {
                commit: 1,
                entry: a2.clone(),
            },
        ))],
        "request 2 is re-prepared in epoch 3 after its rollback"
    );
    assert!(replica.step(request_of(&a1)).is_empty());
    assert!(replica.step(request_of(&a2)).is_empty());
    assert_eq!(replica.slot(), 2, "retries do not re-prepare");
}

/// Completion-order control: the identical scenario with the completion
/// delivered BEFORE the successor acceptance — the client table entry still
/// matches, so the leader replies immediately, and the cached result survives
/// the same rollback. Pins that the arm above loses the reply solely to the
/// newer pending request, not to the history recording itself.
#[test]
fn completion_before_successor_acceptance_replies_immediately_and_survives_rollback() {
    let mut replica = node(3, 0);
    let a1 = entry(1, CLIENT_A, 1);
    let a2 = entry(2, CLIENT_A, 2);

    let out = replica.step(request_of(&a1));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            1,
            Body::Prepare {
                commit: 0,
                entry: a1.clone(),
            },
        ))],
        "request 1 is prepared at slot 1"
    );
    let out = receive(&mut replica, 1, message(0, 1, Body::PrepareOk));
    assert_eq!(out, vec![execute_output(&a1)]);

    // The completion lands while the table entry still describes request 1:
    // the match holds and the leader replies immediately with the exact bytes.
    let out = replica.step(Input::Complete {
        slot: 1,
        result: REPLIED_BEFORE_ACCEPTANCE.to_vec(),
    });
    assert_eq!(
        out,
        vec![Output::Reply(REPLIED_BEFORE_ACCEPTANCE.to_vec())],
        "a matched latest completion replies immediately"
    );
    assert_eq!(replica.executed(), 1);

    let out = replica.step(request_of(&a2));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            2,
            Body::Prepare {
                commit: 1,
                entry: a2.clone(),
            },
        ))],
        "request 2 is accepted after the completion"
    );
    assert!(replica.step(request_of(&a1)).is_empty());

    rollback_and_relead_epoch_three(&mut replica, state(vec![a1.clone()], 1));

    assert_eq!(
        replica.step(request_of(&a1)),
        vec![Output::Reply(REPLIED_BEFORE_ACCEPTANCE.to_vec())],
        "the result cached before acceptance survives the rollback"
    );
}

/// Multiple-clients arm: both clients' latest committed results are preserved
/// with exact bytes when the rolled-back suffix holds one uncommitted request
/// per client; non-latest retries stay silent both while each successor is
/// pending and after a successor is re-prepared post-rollback.
#[test]
fn multiple_clients_results_survive_rollback_and_non_latest_requests_stay_silent() {
    let mut replica = node(3, 0);
    let a1 = entry(1, CLIENT_A, 1);
    let b1 = entry(2, CLIENT_B, 1);
    let a2 = entry(3, CLIENT_A, 2);
    let b2 = entry(4, CLIENT_B, 2);

    let out = replica.step(request_of(&a1));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            1,
            Body::Prepare {
                commit: 0,
                entry: a1.clone(),
            },
        ))]
    );
    let out = replica.step(request_of(&b1));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            2,
            Body::Prepare {
                commit: 0,
                entry: b1.clone(),
            },
        ))]
    );
    let out = receive(&mut replica, 1, message(0, 1, Body::PrepareOk));
    assert_eq!(out, vec![execute_output(&a1)]);
    let out = replica.step(Input::Complete {
        slot: 1,
        result: CLIENT_A_RESULT.to_vec(),
    });
    assert_eq!(
        out,
        vec![Output::Reply(CLIENT_A_RESULT.to_vec())],
        "client A's latest completion replies"
    );
    let out = receive(&mut replica, 1, message(0, 2, Body::PrepareOk));
    assert_eq!(out, vec![execute_output(&b1)]);
    let out = replica.step(Input::Complete {
        slot: 2,
        result: CLIENT_B_RESULT.to_vec(),
    });
    assert_eq!(
        out,
        vec![Output::Reply(CLIENT_B_RESULT.to_vec())],
        "client B's latest completion replies"
    );
    assert_eq!(replica.executed(), 2);

    // Both clients pipeline an uncommitted successor; the suffix {3, 4} spans
    // two clients.
    let out = replica.step(request_of(&a2));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            3,
            Body::Prepare {
                commit: 2,
                entry: a2.clone(),
            },
        ))]
    );
    let out = replica.step(request_of(&b2));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            4,
            Body::Prepare {
                commit: 2,
                entry: b2.clone(),
            },
        ))]
    );
    assert_eq!(replica.slot(), 4);
    // Non-latest retries are dropped while the successors are pending.
    assert!(replica.step(request_of(&a1)).is_empty());
    assert!(replica.step(request_of(&b1)).is_empty());

    rollback_and_relead_epoch_three(&mut replica, state(vec![a1.clone(), b1.clone()], 2));

    assert_eq!(
        replica.step(request_of(&a1)),
        vec![Output::Reply(CLIENT_A_RESULT.to_vec())],
        "client A's exact result survives the two-client suffix rollback"
    );
    assert_eq!(
        replica.step(request_of(&b1)),
        vec![Output::Reply(CLIENT_B_RESULT.to_vec())],
        "client B's exact result survives the two-client suffix rollback"
    );

    // Re-preparing client A's successor makes its request 1 non-latest again:
    // the retry is dropped, and the pending successor earns no reply.
    let out = replica.step(request_of(&a2));
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            3,
            3,
            Body::Prepare {
                commit: 2,
                entry: a2.clone(),
            },
        ))],
        "client A's request 2 is re-prepared in epoch 3"
    );
    assert!(replica.step(request_of(&a1)).is_empty());
    assert!(replica.step(request_of(&a2)).is_empty());
    assert_eq!(
        replica.step(request_of(&b1)),
        vec![Output::Reply(CLIENT_B_RESULT.to_vec())],
        "client B's latest retry still replays from cache"
    );
    assert_eq!(replica.slot(), 3, "retries do not re-prepare");
}

/// Multiple-suffixes arm (backup side): request 1's completion arrives after
/// THREE further suffix entries were accepted — two same-client successors
/// and another client's request — so the table entry is two requests ahead at
/// completion time. The rollback of the whole multi-entry suffix restores the
/// older request with its exact retained result. Fails on the pre-remedy
/// core: the `matched` guard compared the completed entry against the
/// successor's table entry and dropped the result.
#[test]
fn multi_entry_suffix_rollback_restores_result_completed_after_successor_acceptances() {
    const FIRST_LEADER: u32 = 0;
    const SECOND_LEADER: u32 = 1;
    // The replica is node 2 of a 3-member cluster; it leads epoch 2 itself.
    const REPLICA: usize = 2;

    let mut replica = node(3, REPLICA);
    let a1 = entry(1, CLIENT_A, 1);
    let a2 = entry(2, CLIENT_A, 2);
    let b1 = entry(3, CLIENT_B, 1);
    let a3 = entry(4, CLIENT_A, 3);
    let rolled_back = state(vec![a1.clone()], 1);

    let out = receive(
        &mut replica,
        FIRST_LEADER,
        message(
            0,
            1,
            Body::Prepare {
                commit: 1,
                entry: a1.clone(),
            },
        ),
    );
    assert_eq!(
        out,
        vec![
            execute_output(&a1),
            Output::To(FIRST_LEADER, message(0, 1, Body::PrepareOk)),
        ],
        "committed request 1 is executed and acked"
    );
    // Three suffix entries land before the host completes slot 1: two
    // same-client successors and one other-client request, all uncommitted.
    for (slot, suffix) in [(2, &a2), (3, &b1), (4, &a3)] {
        let out = receive(
            &mut replica,
            FIRST_LEADER,
            message(
                0,
                slot,
                Body::Prepare {
                    commit: 1,
                    entry: suffix.clone(),
                },
            ),
        );
        assert_eq!(
            out,
            vec![Output::To(FIRST_LEADER, message(0, slot, Body::PrepareOk))],
            "uncommitted suffix entry at slot {slot} is accepted"
        );
    }
    assert_eq!(replica.slot(), 4);

    let out = replica.step(Input::Complete {
        slot: 1,
        result: MULTI_SUFFIX_RESULT.to_vec(),
    });
    assert_eq!(
        out,
        Vec::new(),
        "a backup completion never replies; the result goes to history"
    );
    assert_eq!(replica.executed(), 1, "request 1 executed");

    // The epoch-1 leader installs slot 1 / commit 1 holding only request 1:
    // the three-entry suffix rolls back.
    let out = receive(
        &mut replica,
        SECOND_LEADER,
        message(
            1,
            1,
            Body::StartEpoch {
                state: rolled_back.clone(),
            },
        ),
    );
    assert!(out.is_empty(), "nothing committed remains to execute");
    assert_eq!(replica.epoch(), 1);
    assert_eq!(replica.log(), [a1.clone()].as_slice());
    assert_eq!(replica.commit(), 1);
    assert_eq!(replica.executed(), 1, "request 1 stays executed");

    // The replica leads epoch 2.
    let out = replica.step(Input::LeaderTimeout);
    assert_eq!(
        out,
        vec![Output::Broadcast(message(2, 1, Body::StartEpochChange))],
        "timeout starts the epoch-2 change"
    );
    let out = receive(
        &mut replica,
        FIRST_LEADER,
        message(2, 1, Body::StartEpochChange),
    );
    assert!(out.is_empty(), "quorum is not yet reached");
    let out = receive(
        &mut replica,
        FIRST_LEADER,
        message(
            2,
            1,
            Body::DoEpochChange {
                latest_normal: 1,
                state: rolled_back.clone(),
            },
        ),
    );
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            2,
            1,
            Body::StartEpoch {
                state: rolled_back.clone(),
            },
        ))],
        "quorum activates epoch 2 and broadcasts the installed state"
    );
    assert!(replica.is_leader(), "the replica leads epoch 2");

    assert_eq!(
        replica.step(request_of(&a1)),
        vec![Output::Reply(MULTI_SUFFIX_RESULT.to_vec())],
        "the exact result completed after the successor acceptances is restored"
    );
}

/// Recovery arm: results computed during the `Status::Replaying` phase never
/// reply and are recorded unconditionally; epoch adoption then preserves them
/// (here: the epoch-3 install rolls the recovered uncommitted suffix back),
/// and once the recoverer leads epoch 4 the exact-byte retry replays the
/// replayed result.
#[test]
fn recovery_replay_and_epoch_adoption_preserve_exact_result_bytes() {
    // The recoverer is node 1 of a 3-member cluster: node 2 leads epoch 2,
    // node 0 leads epoch 3, and the recoverer leads epoch 4.
    let mut replica = node(3, 1);
    let a1 = entry(1, CLIENT_A, 1);
    let a2 = entry(2, CLIENT_A, 2);
    let recovered = state(vec![a1.clone(), a2.clone()], 1);
    let rolled_back = state(vec![a1.clone()], 1);

    let out = replica.step(Input::Recover { nonce: NONCE });
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            0,
            0,
            Body::Recovery { nonce: NONCE }
        ))],
        "the recovery attempt broadcasts its nonce"
    );
    // The epoch-2 leader reports a committed request 1 plus the uncommitted
    // same-client request 2; a nonleader reports no state.
    let out = receive(
        &mut replica,
        2,
        message(
            2,
            2,
            Body::RecoveryResponse {
                nonce: NONCE,
                state: Some(recovered.clone()),
            },
        ),
    );
    assert!(out.is_empty(), "the quorum is not yet complete");
    let out = receive(
        &mut replica,
        0,
        message(
            2,
            0,
            Body::RecoveryResponse {
                nonce: NONCE,
                state: None,
            },
        ),
    );
    assert_eq!(
        out,
        vec![execute_output(&a1)],
        "quorum completion enters the replay phase at the committed frontier"
    );
    assert_eq!(replica.status(), Status::Replaying);
    assert_eq!(replica.epoch(), 2);
    assert_eq!(replica.commit(), 1);
    assert_eq!(replica.executed(), 0);
    assert_eq!(replica.log(), recovered.log.as_slice());

    // The replay completion must not reply — the replay phase emits no
    // replies — and activates the replica exactly at the committed frontier.
    let out = replica.step(Input::Complete {
        slot: 1,
        result: REPLAYED_RESULT.to_vec(),
    });
    assert_eq!(
        out,
        Vec::new(),
        "a replay completion emits no reply and no further execution"
    );
    assert_eq!(
        replica.status(),
        Status::Normal,
        "reaching the committed frontier activates the replica"
    );
    assert_eq!(replica.executed(), 1);

    // The epoch-3 leader installs the rolled-back state: the recovered
    // uncommitted request 2 is gone, and adoption must keep request 1's
    // replayed result.
    let out = receive(
        &mut replica,
        0,
        message(
            3,
            1,
            Body::StartEpoch {
                state: rolled_back.clone(),
            },
        ),
    );
    assert!(out.is_empty(), "the rolled-back state installs quietly");
    assert_eq!(replica.epoch(), 3);
    assert_eq!(replica.log(), rolled_back.log.as_slice());
    assert_eq!(replica.executed(), 1, "request 1 stays executed");

    // The recoverer leads epoch 4.
    let out = replica.step(Input::LeaderTimeout);
    assert_eq!(
        out,
        vec![Output::Broadcast(message(4, 1, Body::StartEpochChange))],
        "timeout starts the epoch-4 change"
    );
    let out = receive(&mut replica, 0, message(4, 1, Body::StartEpochChange));
    assert!(out.is_empty(), "quorum is not yet reached");
    let out = receive(
        &mut replica,
        0,
        message(
            4,
            1,
            Body::DoEpochChange {
                latest_normal: 3,
                state: rolled_back.clone(),
            },
        ),
    );
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            4,
            1,
            Body::StartEpoch {
                state: rolled_back.clone(),
            },
        ))],
        "quorum activates epoch 4 and broadcasts the installed state"
    );
    assert!(replica.is_leader(), "the recoverer leads epoch 4");

    assert_eq!(
        replica.step(request_of(&a1)),
        vec![Output::Reply(REPLAYED_RESULT.to_vec())],
        "the replayed result survives rollback and adoption with exact bytes"
    );
}
