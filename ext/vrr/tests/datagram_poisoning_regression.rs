//! Regression for the whole-log datagram-poisoning boundary defect (review triage T0,
//! consolidated finding F01, P0).
//!
//! A public-path drive grows the log of an FFI node until the whole-log
//! `DO_EPOCH_CHANGE` generated during epoch change encodes just beyond
//! `MAX_DATAGRAM`. `Node::peer` (`ffi.rs:83`) rejects the datagram only after
//! `Replica::step` has already applied the core mutations (`qualify_change` sets
//! `sent_do_change` and records the peer's `StartEpochChange`), and `Node::step`
//! (`ffi.rs:109`) then poisons the node permanently.
//!
//! The boundary assertions (largest fitting log / first oversize log) hold both
//! before and after remediation: the one-datagram ceiling stays. The contract
//! assertions (oversize output refused, node not poisoned) express the required
//! post-fix behavior from item02: refusal before observable mutation and no
//! permanent poisoning.
//!
//! Observed current behavior at b971e8a:
//! - the qualifying `StartEpochChange` receive returns `TOO_LARGE` (-6),
//! - every subsequent public entry point returns permanent `SERVICE` (-7),
//!   so the two "must keep serving" assertions fail today with `left: -7`.
//!
//! `START_EPOCH` and `RECOVERY_RESPONSE` share the same whole-log encoding path
//! (`Node::peer` / `Node::run`); driving one path pins the shared ceiling.

use std::ffi::c_void;

use uuid::Uuid;
use vrr::locks::{Lease, Request};
use vrr::vrr::{Body, LogEntry, LogState, Message, MAX_DATAGRAM};

const OK: i32 = 0;
const TOO_LARGE: i32 = -6;

unsafe extern "C" {
    fn vrr_node_new(
        members_len: usize,
        members_data: *const u8,
        own_len: usize,
        own_data: *const u8,
        out: *mut *mut c_void,
    ) -> i32;
    fn vrr_node_free(node: *mut c_void);
    fn vrr_node_request(
        node: *mut c_void,
        execution_time_len: usize,
        execution_time: *const u8,
        json_len: usize,
        json: *const u8,
    ) -> i32;
    fn vrr_node_receive(node: *mut c_void, from: u32, len: usize, data: *const u8) -> i32;
    fn vrr_node_idle(node: *mut c_void) -> i32;
    fn vrr_node_leader_timeout(node: *mut c_void) -> i32;
    fn vrr_node_recover(node: *mut c_void, nonce_len: usize, nonce: *const u8) -> i32;
}

struct Node(*mut c_void);

impl Drop for Node {
    fn drop(&mut self) {
        unsafe { vrr_node_free(self.0) };
    }
}

/// The exact client payload the core stores for request `slot`.
fn json(slot: u64) -> Vec<u8> {
    serde_json::to_vec(&Request::Set {
        message_id: Uuid::from_u128(slot as u128),
        client_id: 7,
        request_num: slot,
        lock_id: 11,
        lease: Lease {
            lease_id: 13,
            holder: Uuid::from_u128(17),
            expiry: 1000,
        },
    })
    .unwrap()
}

/// The exact log entry the core appends for request `slot`.
fn entry(slot: u64) -> LogEntry {
    LogEntry {
        slot,
        client_id: 7,
        request_num: slot,
        message_id: Uuid::from_u128(slot as u128),
        execution_time: 100,
        payload: json(slot),
    }
}

/// Encoded size of the `DO_EPOCH_CHANGE` this node generates in epoch 1 with a
/// log of `entries` requests: header slot = log length, `latest_normal` = 0,
/// `commit` = 0 (no PREPARE_OK quorum is ever delivered in this drive).
fn do_epoch_change_datagram(entries: u64) -> usize {
    Message {
        epoch: 1,
        slot: entries,
        body: Body::DoEpochChange {
            latest_normal: 0,
            state: LogState {
                slot: entries,
                commit: 0,
                log: (1..=entries).map(entry).collect(),
            },
        },
    }
    .encode()
    .unwrap()
    .len()
}

#[test]
fn oversize_whole_log_transfer_is_refused_without_poisoning_the_node() {
    // Pin the ceiling deterministically: the first log length whose generated
    // DO_EPOCH_CHANGE exceeds MAX_DATAGRAM, with the previous length still fitting.
    let oversize = (1u64..)
        .find(|&entries| do_epoch_change_datagram(entries) > MAX_DATAGRAM)
        .expect("a finite whole-log datagram ceiling exists");
    assert!(
        do_epoch_change_datagram(oversize - 1) <= MAX_DATAGRAM,
        "boundary is not exact: {} entries must still fit",
        oversize - 1
    );
    eprintln!(
        "whole-log DO_EPOCH_CHANGE ceiling: {oversize} entries encode to {} bytes \
         (> {MAX_DATAGRAM}); {} entries encode to {} bytes",
        do_epoch_change_datagram(oversize),
        oversize - 1,
        do_epoch_change_datagram(oversize - 1),
    );

    // Node 0 of three members leads epoch 0; peers are 1 and 2.
    let members = b"0:1\0"
        .iter()
        .chain(b"1:1\0")
        .chain(b"2:1")
        .copied()
        .collect::<Vec<_>>();
    let own = b"0:1";
    let mut raw = std::ptr::null_mut();
    assert_eq!(
        unsafe {
            vrr_node_new(
                members.len(),
                members.as_ptr(),
                own.len(),
                own.as_ptr(),
                &mut raw,
            )
        },
        OK
    );
    assert!(!raw.is_null());
    let node = Node(raw);

    // Grow the log to the first oversize length through the public request path.
    for slot in 1..=oversize {
        let json = json(slot);
        assert_eq!(
            unsafe { vrr_node_request(node.0, 3, b"100".as_ptr(), json.len(), json.as_ptr()) },
            OK,
            "request {slot}"
        );
    }

    // Leader timeout moves the node into epoch 1, where node 1 leads and this
    // node is a backup. One peer START_EPOCH_CHANGE then qualifies the
    // whole-log DO_EPOCH_CHANGE.
    assert_eq!(unsafe { vrr_node_leader_timeout(node.0) }, OK);
    let start_epoch_change = Message {
        epoch: 1,
        slot: 0,
        body: Body::StartEpochChange,
    }
    .encode()
    .unwrap();
    assert!(start_epoch_change.len() <= MAX_DATAGRAM);
    assert_eq!(
        unsafe {
            vrr_node_receive(
                node.0,
                1,
                start_epoch_change.len(),
                start_epoch_change.as_ptr(),
            )
        },
        TOO_LARGE,
        "oversize whole-log DO_EPOCH_CHANGE must be refused"
    );

    // The refusal must not poison the node: public entry points keep serving.
    // Both assertions fail today with SERVICE (-7): the refusal happens after
    // the core mutation and Node::step marks the node permanently poisoned.
    assert_eq!(
        unsafe { vrr_node_idle(node.0) },
        OK,
        "node must keep serving after an oversize refusal"
    );
    assert_eq!(
        unsafe { vrr_node_recover(node.0, 1, b"9".as_ptr()) },
        OK,
        "node must accept recovery after an oversize refusal"
    );
}
