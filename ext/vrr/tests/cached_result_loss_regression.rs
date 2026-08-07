//! Source 01 R2 / F04 regression: suffix rollback after an uncommitted
//! same-client request permanently loses an executed request's cached result.
//!
//! `Replica::adopt` (`src/vrr.rs`) rebuilds the client table from the
//! installed log with `result: None` and then restores a cached result only
//! when the old table's single latest request for that client exactly matches
//! the rebuilt latest request (same `request_num` and `message_id`). Accepting
//! the same client's uncommitted request 2 has already replaced that table
//! entry (`request_num` 2, no result), so when a later valid epoch state rolls
//! request 2 back, the rebuilt entry describes request 1 again but the restore
//! loop's exact-match guard cannot revive request 1's cached result. When this
//! replica later becomes leader, a retry of the executed request 1 hits the
//! cache arm with `result: None` and earns no `Reply` — a correct client can
//! wait forever.
//!
//! The first test follows `.tmp/review-repro/src/bin/result_loss.rs` step by
//! step and fails on the defective core: the leader retry of request 1 emits
//! nothing instead of replaying the exact cached result. The second is a
//! control — the identical rollback without the uncommitted request 2 — that
//! passes before and after a fix, pinning that the defect is the lost cached
//! result, not the suffix-rollback or cache-replay mechanisms themselves.

mod support;

use support::{entry, message, node, receive, state};
use vrr::vrr::{Body, Input, LogEntry, LogState, Message, NodeId, Output, Replica, Status};

const CLIENT: u64 = 1;
const CACHED_RESULT: &[u8] = b"cached-result";
/// Node 0 leads epoch 0 and later contributes epoch-change evidence.
const FIRST_LEADER: NodeId = 0;
/// Node 1 leads epoch 1 and installs the rolled-back state.
const SECOND_LEADER: NodeId = 1;
/// The replica is node 2 of a 3-member cluster; it leads epoch 2 itself.
const REPLICA: usize = 2;

fn request_entry(slot: u64, request_num: u64) -> LogEntry {
    entry(slot, CLIENT, request_num)
}

/// The later epochs' valid installed state: slot 1 / commit 1 holding only
/// request 1 — the uncommitted request 2 suffix is rolled back.
fn rolled_back_state() -> LogState {
    state(vec![request_entry(1, 1)], 1)
}

/// Request 1 prepared and committed at slot 1 by the epoch-0 leader.
fn prepare_request_one() -> Message {
    message(
        0,
        1,
        Body::Prepare {
            commit: 1,
            entry: request_entry(1, 1),
        },
    )
}

/// A client retry of the already-executed request 1 (same `request_num` and
/// `message_id`).
fn retry_request_one() -> Input {
    Input::Request {
        client_id: CLIENT,
        request_num: 1,
        message_id: request_entry(1, 1).message_id,
        execution_time: 0,
        payload: vec![],
    }
}

/// Executes request 1 at slot 1 and caches its result, asserting the
/// prepare/execute/ack contract of the epoch-0 backup path.
fn execute_and_cache_request_one(replica: &mut Replica) {
    let first = request_entry(1, 1);
    let out = receive(replica, FIRST_LEADER, prepare_request_one());
    assert_eq!(
        out,
        vec![
            Output::Execute {
                slot: 1,
                client_id: CLIENT,
                request_num: 1,
                message_id: first.message_id,
                execution_time: first.execution_time,
                payload: first.payload.clone(),
            },
            Output::To(FIRST_LEADER, message(0, 1, Body::PrepareOk)),
        ],
        "committed request 1 must be executed and acked"
    );
    let out = replica.step(Input::Complete {
        slot: 1,
        result: CACHED_RESULT.to_vec(),
    });
    assert!(out.is_empty(), "a backup completion emits no reply");
    assert_eq!(replica.executed(), 1, "request 1 executed");
}

/// Installs the rolled-back state via the epoch-1 leader's `StartEpoch`,
/// asserting the uncommitted suffix is gone and request 1 stays executed.
fn install_rolled_back_state(replica: &mut Replica) {
    let out = receive(
        replica,
        SECOND_LEADER,
        message(
            1,
            1,
            Body::StartEpoch {
                state: rolled_back_state(),
            },
        ),
    );
    assert!(out.is_empty(), "nothing committed remains to execute");
    assert_eq!(replica.epoch(), 1);
    assert_eq!(replica.status(), Status::Normal);
    assert_eq!(
        replica.log(),
        [request_entry(1, 1)].as_slice(),
        "the installed state holds only request 1"
    );
    assert_eq!(replica.commit(), 1);
    assert_eq!(replica.executed(), 1, "request 1 stays executed");
}

/// Drives the replica through the epoch change into epoch 2, which it leads
/// (`leader_of(2) == 2`), and asserts activation with the rolled-back state.
fn become_leader_of_epoch_two(replica: &mut Replica) {
    let out = replica.step(Input::LeaderTimeout);
    assert_eq!(
        out,
        vec![Output::Broadcast(message(2, 1, Body::StartEpochChange))],
        "timeout starts the epoch change"
    );
    let out = receive(replica, FIRST_LEADER, message(2, 1, Body::StartEpochChange));
    assert!(out.is_empty(), "quorum is not yet reached");
    let out = receive(
        replica,
        FIRST_LEADER,
        message(
            2,
            1,
            Body::DoEpochChange {
                latest_normal: 1,
                state: rolled_back_state(),
            },
        ),
    );
    assert_eq!(
        out,
        vec![Output::Broadcast(message(
            2,
            1,
            Body::StartEpoch {
                state: rolled_back_state()
            }
        ))],
        "quorum activates epoch 2 and broadcasts the installed state"
    );
    assert_eq!(replica.epoch(), 2);
    assert_eq!(replica.status(), Status::Normal);
    assert!(replica.is_leader(), "the replica leads epoch 2");
}

/// R2 headline: request 1 is executed and its result cached, the same client's
/// uncommitted request 2 is accepted, a later valid epoch state rolls request
/// 2 back, and once this replica leads epoch 2 the retry of request 1 must
/// replay the exact cached result. Fails on the defective core: the cache arm
/// finds `result: None` and the retry earns no `Reply`.
#[test]
fn leader_retry_of_executed_request_must_replay_cached_result_after_suffix_rollback() {
    let mut replica = node(3, REPLICA);
    execute_and_cache_request_one(&mut replica);

    // The same client's uncommitted request 2 is accepted at slot 2.
    let out = receive(
        &mut replica,
        FIRST_LEADER,
        message(
            0,
            2,
            Body::Prepare {
                commit: 1,
                entry: request_entry(2, 2),
            },
        ),
    );
    assert_eq!(
        out,
        vec![Output::To(FIRST_LEADER, message(0, 2, Body::PrepareOk))],
        "uncommitted request 2 is accepted"
    );
    assert_eq!(replica.slot(), 2);

    install_rolled_back_state(&mut replica);
    become_leader_of_epoch_two(&mut replica);

    let retry = replica.step(retry_request_one());
    assert_eq!(
        retry,
        vec![Output::Reply(CACHED_RESULT.to_vec())],
        "executed retry must replay its cached result"
    );
}

/// Control: the identical rollback without the uncommitted request 2 — the old
/// client table entry still describes request 1, so the adopt restore loop
/// revives its cached result and the leader retry replays it. Passes before
/// and after a fix, pinning that the defect is the overwritten client entry,
/// not suffix rollback or cache replay themselves.
#[test]
fn leader_retry_replays_cached_result_when_no_uncommitted_successor_is_rolled_back() {
    let mut replica = node(3, REPLICA);
    execute_and_cache_request_one(&mut replica);

    install_rolled_back_state(&mut replica);
    become_leader_of_epoch_two(&mut replica);

    let retry = replica.step(retry_request_one());
    assert_eq!(
        retry,
        vec![Output::Reply(CACHED_RESULT.to_vec())],
        "cached result survives the rollback of an untouched prefix"
    );
}
