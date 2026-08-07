//! Item 08 stop-line: the staged F04 fix is incomplete — a committed request's
//! result is still lost when its completion arrives AFTER the same client's
//! uncommitted successor has been accepted, and a suffix rollback exposes it.
//!
//! The staged fix preserves cached results in `Replica::results` keyed by
//! `(client_id, request_num)` and restores them in `adopt`, but it only feeds
//! that map from `complete()` when the client table entry still exactly
//! matches the completed entry (`src/vrr.rs:644-657`: the `matched` guard).
//! On the backup path the same client's uncommitted request 2 is legitimately
//! accepted BEFORE the host completes slot 1 — the leader pipelines
//! `Prepare(slot 2)` while the backup's execution of slot 1 is still in
//! flight — and acceptance overwrites the table entry (`src/vrr.rs:487-494`).
//! When `Complete { slot: 1 }` then arrives, the guard finds `request_num` 2
//! in the entry, so `matched` is false and the freshly computed result is
//! dropped: it reaches neither the table entry nor the `results` history map.
//! A later valid epoch state that rolls request 2 back rebuilds the entry for
//! request 1 with `result: None`, and once this replica leads, the retry of
//! the executed request 1 hits the cache arm (`src/vrr.rs:403-409`) and earns
//! no `Reply`. The log prefix is intact and `executed == commit`, so nothing
//! ever re-executes request 1 — the same permanent loss as R2/F04, via the
//! completion-order interleaving the item 07 record warned about ("the fix
//! must preserve result history for committed requests across same-client
//! successor acceptance").
//!
//! The headline test drives that interleaving and fails on the staged core:
//! the leader retry emits nothing instead of replaying the exact result. The
//! control delivers the identical scenario with the completion BEFORE the
//! successor acceptance; it passes on the staged core, pinning that the loss
//! is the completion-time drop, not the suffix-rollback, epoch-change, or
//! cache-replay mechanisms themselves.

mod support;

use support::{entry, message, node, receive, state};
use vrr::vrr::{Body, Input, LogEntry, LogState, Output, Replica};

const CLIENT: u64 = 7;
const RESULT_ONE: &[u8] = b"completion-order-result-one";
/// Node 0 leads epoch 0 and later contributes epoch-change evidence.
const FIRST_LEADER: u32 = 0;
/// Node 1 leads epoch 1 and installs the rolled-back state.
const SECOND_LEADER: u32 = 1;
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
fn prepare_request_one() -> vrr::vrr::Message {
    message(
        0,
        1,
        Body::Prepare {
            commit: 1,
            entry: request_entry(1, 1),
        },
    )
}

/// The epoch-0 leader pipelines the same client's uncommitted request 2 at
/// slot 2 (commit stays 1).
fn accept_uncommitted_request_two(replica: &mut Replica) {
    let out = receive(
        replica,
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
}

/// The host completes slot 1 with request 1's computed result.
fn complete_request_one(replica: &mut Replica) {
    let out = replica.step(Input::Complete {
        slot: 1,
        result: RESULT_ONE.to_vec(),
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
    assert_eq!(replica.log(), [request_entry(1, 1)].as_slice());
    assert_eq!(replica.commit(), 1);
    assert_eq!(replica.executed(), 1, "request 1 stays executed");
}

/// Drives the replica through the epoch change into epoch 2, which it leads
/// (`leader_of(2) == 2`), activating with the rolled-back state.
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
    assert!(replica.is_leader(), "the replica leads epoch 2");
}

/// A client retry of the already-executed request 1 (same `request_num` and
/// `message_id`).
fn retry_request_one(replica: &mut Replica) -> Vec<Output> {
    replica.step(Input::Request {
        client_id: CLIENT,
        request_num: 1,
        message_id: request_entry(1, 1).message_id,
        execution_time: 0,
        payload: vec![],
    })
}

/// Stop-line headline: request 1 is committed and its execution completes
/// AFTER the same client's uncommitted request 2 was accepted — the normal
/// backup-side interleaving when the leader pipelines prepares while the
/// host's execution of slot 1 is still in flight. A later valid epoch state
/// rolls request 2 back, and once this replica leads epoch 2 the retry of
/// request 1 must replay the exact computed result. Fails on the staged core:
/// `complete()`'s `matched` guard drops the result because the table entry
/// already describes request 2, so the retry earns no `Reply`.
#[test]
fn retry_must_replay_result_completed_after_uncommitted_successor_acceptance() {
    let mut replica = node(3, REPLICA);

    let first = request_entry(1, 1);
    let out = receive(&mut replica, FIRST_LEADER, prepare_request_one());
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

    // The pipelined acceptance lands BEFORE the host's completion of slot 1.
    accept_uncommitted_request_two(&mut replica);
    complete_request_one(&mut replica);

    install_rolled_back_state(&mut replica);
    become_leader_of_epoch_two(&mut replica);

    assert_eq!(
        retry_request_one(&mut replica),
        vec![Output::Reply(RESULT_ONE.to_vec())],
        "executed retry must replay its cached result"
    );
}

/// Control: the identical scenario with the completion delivered BEFORE the
/// uncommitted successor acceptance — the result enters the `results` history
/// map while the table entry still describes request 1, survives the rollback,
/// and the leader retry replays it. Passes on the staged core, pinning that
/// the headline loss is the completion-time drop under the `matched` guard,
/// not the suffix-rollback, epoch-change, or cache-replay mechanisms.
#[test]
fn retry_replays_result_completed_before_uncommitted_successor_acceptance() {
    let mut replica = node(3, REPLICA);

    let first = request_entry(1, 1);
    let out = receive(&mut replica, FIRST_LEADER, prepare_request_one());
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

    // The completion lands while the table entry still describes request 1.
    complete_request_one(&mut replica);
    accept_uncommitted_request_two(&mut replica);

    install_rolled_back_state(&mut replica);
    become_leader_of_epoch_two(&mut replica);

    assert_eq!(
        retry_request_one(&mut replica),
        vec![Output::Reply(RESULT_ONE.to_vec())],
        "result cached before acceptance survives the rollback"
    );
}
