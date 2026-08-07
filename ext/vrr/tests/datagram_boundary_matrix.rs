//! Boundary matrix around the one-datagram ceiling for generated peer output
//! (item02 verification of the F01 transactional FFI fix).
//!
//! `datagram_poisoning_regression.rs` pins the refusal and the keep-serving
//! contract at the first oversize log length. This matrix covers the two arms
//! around the exact boundary end-to-end through the public FFI output queue:
//!
//! - just-fits (largest log whose whole-log `DO_EPOCH_CHANGE` still fits): the
//!   step commits exactly once — the queued datagram is at most `MAX_DATAGRAM`,
//!   decodes to the exact expected message, and a repeat trigger produces no
//!   duplicate output.
//! - just-over (first oversize log): the refusal is atomic — a second trigger
//!   refuses again (proving the core mutation rolled back instead of being
//!   retained), every previously queued output survives both refusals in order,
//!   and no partial output from the failed steps leaks into the queue.
//!
//! The ceiling itself is a liveness limit, not corruption: a node whose whole
//! log cannot move in one datagram refuses that transfer and keeps serving, but
//! cannot complete that transfer while the log stays whole.

use std::ffi::c_void;
use std::ptr;

use uuid::Uuid;
use vrr::locks::{Lease, Request};
use vrr::vrr::{Body, LogEntry, LogState, Message, Tag, MAX_DATAGRAM};

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
    fn vrr_node_next(
        node: *mut c_void,
        out_kind: *mut u32,
        out_to: *mut u32,
        out_tag: *mut u32,
        out_epoch: *mut u32,
        out_slot_hi: *mut u32,
        out_slot_lo: *mut u32,
        capacity: usize,
        out_len: *mut usize,
        out_data: *mut u8,
    ) -> i32;
}

struct Node(*mut c_void);

impl Node {
    fn new() -> Self {
        let members = b"0:1\0"
            .iter()
            .chain(b"1:1\0")
            .chain(b"2:1")
            .copied()
            .collect::<Vec<_>>();
        let own = b"0:1";
        let mut node = ptr::null_mut();
        // Every pointer passed here is derived from a live Rust slice; `Node`
        // frees the successful handle on drop.
        let status = unsafe {
            vrr_node_new(
                members.len(),
                members.as_ptr(),
                own.len(),
                own.as_ptr(),
                &mut node,
            )
        };
        assert_eq!(status, OK);
        assert!(!node.is_null());
        Self(node)
    }

    fn request(&self, json: &[u8]) -> i32 {
        unsafe { vrr_node_request(self.0, 3, b"100".as_ptr(), json.len(), json.as_ptr()) }
    }

    fn receive(&self, from: u32, bytes: &[u8]) -> i32 {
        unsafe { vrr_node_receive(self.0, from, bytes.len(), bytes.as_ptr()) }
    }

    fn next(&self) -> Option<Output> {
        let mut output = Output::default();
        let mut bytes = vec![0; MAX_DATAGRAM];
        let status = unsafe {
            vrr_node_next(
                self.0,
                &mut output.kind,
                &mut output.to,
                &mut output.tag,
                &mut output.epoch,
                &mut output.slot_hi,
                &mut output.slot_lo,
                bytes.len(),
                &mut output.len,
                bytes.as_mut_ptr(),
            )
        };
        match status {
            0 => None,
            1 => {
                bytes.truncate(output.len);
                output.bytes = bytes;
                Some(output)
            }
            other => panic!("vrr_node_next failed with {other}"),
        }
    }

    fn drain(&self) -> Vec<Output> {
        std::iter::from_fn(|| self.next()).collect()
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        unsafe { vrr_node_free(self.0) };
    }
}

#[derive(Default)]
struct Output {
    kind: u32,
    to: u32,
    tag: u32,
    epoch: u32,
    slot_hi: u32,
    slot_lo: u32,
    len: usize,
    bytes: Vec<u8>,
}

impl Output {
    fn slot(&self) -> u64 {
        ((self.slot_hi as u64) << 32) | self.slot_lo as u64
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

/// The exact whole-log `DO_EPOCH_CHANGE` this node generates in epoch 1 with a
/// log of `entries` requests: header slot = log length, `latest_normal` = 0,
/// `commit` = 0 (no PREPARE_OK quorum is ever delivered in this drive).
fn do_epoch_change(entries: u64) -> Message {
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
}

/// The first log length whose generated `DO_EPOCH_CHANGE` exceeds one
/// datagram, asserting the boundary is exact: the previous length still fits.
fn first_oversize_log() -> u64 {
    let oversize = (1u64..)
        .find(|&entries| do_epoch_change(entries).encode().unwrap().len() > MAX_DATAGRAM)
        .expect("a finite whole-log datagram ceiling exists");
    assert!(
        do_epoch_change(oversize - 1).encode().unwrap().len() <= MAX_DATAGRAM,
        "boundary is not exact: {} entries must still fit",
        oversize - 1
    );
    oversize
}

fn start_epoch_change() -> Vec<u8> {
    let bytes = Message {
        epoch: 1,
        slot: 0,
        body: Body::StartEpochChange,
    }
    .encode()
    .unwrap();
    assert!(bytes.len() <= MAX_DATAGRAM);
    bytes
}

/// Node 0 leads epoch 0; `entries` requests queue that many PREPARE broadcasts,
/// then a leader timeout enters epoch 1 (node 1 leads) and queues one
/// START_EPOCH_CHANGE broadcast.
fn grow(node: &Node, entries: u64) {
    for slot in 1..=entries {
        assert_eq!(node.request(&json(slot)), OK, "request {slot}");
    }
    assert_eq!(unsafe { vrr_node_leader_timeout(node.0) }, OK);
}

/// Asserts the `expected`-th drained output is the queued START_EPOCH_CHANGE
/// broadcast for epoch 1 carrying the current log length in its header slot.
fn assert_start_epoch_change(output: &Output, slot: u64) {
    assert_eq!(output.kind, 1, "START_EPOCH_CHANGE is a broadcast");
    assert_eq!(output.tag, Tag::StartEpochChange as u32);
    assert_eq!(output.epoch, 1);
    assert_eq!(output.slot(), slot);
}

#[test]
fn just_fits_transfer_commits_exactly_once_within_one_datagram() {
    let fits = first_oversize_log() - 1;
    let node = Node::new();
    grow(&node, fits);

    assert_eq!(node.receive(1, &start_epoch_change()), OK);

    // The queue holds the ordinary outputs first, then the committed transfer:
    // `fits` PREPARE broadcasts, one START_EPOCH_CHANGE broadcast, one
    // DO_EPOCH_CHANGE to the epoch-1 leader.
    let drained = node.drain();
    assert_eq!(
        drained.len(),
        usize::try_from(fits).unwrap() + 2,
        "queue must hold every ordinary output plus the single transfer"
    );
    for (index, output) in drained[..drained.len() - 2].iter().enumerate() {
        let slot = u64::try_from(index).unwrap() + 1;
        assert_eq!(output.kind, 1, "PREPARE {slot} is a broadcast");
        assert_eq!(output.tag, Tag::Prepare as u32, "PREPARE {slot}");
        assert_eq!(output.epoch, 0, "PREPARE {slot}");
        assert_eq!(output.slot(), slot, "PREPARE order is preserved");
    }
    assert_start_epoch_change(&drained[drained.len() - 2], fits);

    let transfer = &drained[drained.len() - 1];
    assert_eq!(transfer.kind, 2, "DO_EPOCH_CHANGE goes to the leader only");
    assert_eq!(transfer.to, 1, "node 1 leads epoch 1");
    assert_eq!(transfer.tag, Tag::DoEpochChange as u32);
    assert_eq!(transfer.epoch, 1);
    assert_eq!(transfer.slot(), fits);
    assert!(
        transfer.bytes.len() <= MAX_DATAGRAM,
        "just-fits datagram is {} bytes (> {MAX_DATAGRAM})",
        transfer.bytes.len()
    );
    assert_eq!(
        Message::decode(&transfer.bytes),
        Some(do_epoch_change(fits)),
        "queued datagram must decode to the exact whole-log DO_EPOCH_CHANGE"
    );

    // The success committed exactly once: a second qualifying trigger adds no
    // duplicate transfer, and the node keeps serving.
    assert_eq!(node.receive(2, &start_epoch_change()), OK);
    assert!(
        node.next().is_none(),
        "a committed sent_do_change must not retransmit"
    );
    assert_eq!(unsafe { vrr_node_idle(node.0) }, OK);
}

#[test]
fn just_over_refusal_rolls_back_and_preserves_the_output_queue() {
    let oversize = first_oversize_log();
    let node = Node::new();
    grow(&node, oversize);

    assert_eq!(
        node.receive(1, &start_epoch_change()),
        TOO_LARGE,
        "oversize whole-log DO_EPOCH_CHANGE must be refused"
    );
    assert_eq!(
        node.receive(2, &start_epoch_change()),
        TOO_LARGE,
        "rollback proof: the refused step retained neither the peer's \
         StartEpochChange nor sent_do_change, so a new trigger refuses again"
    );

    // Both refusals are atomic for the queue: every ordinary output enqueued
    // before them survives in order, and no partial output from either failed
    // step leaked in.
    let drained = node.drain();
    assert_eq!(
        drained.len(),
        usize::try_from(oversize).unwrap() + 1,
        "queue must hold exactly the pre-refusal outputs"
    );
    for (index, output) in drained[..drained.len() - 1].iter().enumerate() {
        let slot = u64::try_from(index).unwrap() + 1;
        assert_eq!(output.kind, 1, "PREPARE {slot} is a broadcast");
        assert_eq!(output.tag, Tag::Prepare as u32, "PREPARE {slot}");
        assert_eq!(output.epoch, 0, "PREPARE {slot}");
        assert_eq!(output.slot(), slot, "PREPARE order is preserved");
    }
    assert_start_epoch_change(&drained[drained.len() - 1], oversize);
    assert!(
        drained
            .iter()
            .all(|output| output.tag != Tag::DoEpochChange as u32),
        "no partial DO_EPOCH_CHANGE may leak from a refused step"
    );

    assert_eq!(
        unsafe { vrr_node_idle(node.0) },
        OK,
        "node must keep serving after the refusals"
    );
}
