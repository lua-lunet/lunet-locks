use std::collections::HashSet;
use std::ffi::c_void;
use std::ptr;

use uuid::Uuid;
use vrr::locks::{Lease, Request};
use vrr::vrr::{Body, LogEntry, LogState, Tag, MAX_DATAGRAM};

const OK: i32 = 0;
const INVALID: i32 = -1;
const CONFIG: i32 = -2;
const NUMBER: i32 = -3;
const CLIENT_JSON: i32 = -4;
const VRR_MESSAGE: i32 = -5;
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
    fn new(index: u8) -> Self {
        let members = b"0:1\0"
            .iter()
            .chain(b"1:1\0")
            .chain(b"2:1")
            .copied()
            .collect::<Vec<_>>();
        let own = [b'0' + index, b':', b'1'];
        let mut node = ptr::null_mut();
        // Every pointer passed here is either derived from a live Rust slice or is
        // deliberately null for a contract test; `Node` frees successful handles.
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

    fn request(&self, time: &[u8], json: &[u8]) -> i32 {
        unsafe { vrr_node_request(self.0, time.len(), time.as_ptr(), json.len(), json.as_ptr()) }
    }

    fn receive(&self, from: u32, bytes: &[u8]) -> i32 {
        unsafe { vrr_node_receive(self.0, from, bytes.len(), bytes.as_ptr()) }
    }

    fn next(&self, capacity: usize) -> (i32, Output) {
        let mut output = Output::default();
        let mut bytes = vec![0; capacity];
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
        bytes.truncate(output.len.min(bytes.len()));
        output.bytes = bytes;
        (status, output)
    }
}

impl Drop for Node {
    fn drop(&mut self) {
        unsafe { vrr_node_free(self.0) };
    }
}

#[derive(Default, Debug)]
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

fn request(message: u8, request_num: u64) -> Vec<u8> {
    serde_json::to_vec(&Request::Set {
        message_id: Uuid::from_bytes([message; 16]),
        client_id: 7,
        request_num,
        lock_id: 11,
        lease: Lease {
            lease_id: 13,
            holder: Uuid::from_bytes([17; 16]),
            expiry: 1_000,
        },
    })
    .unwrap()
}

fn wire(tag: Tag, epoch: u32, slot: u64, body: &Body) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(16);
    bytes.extend_from_slice(&(tag as u32).to_be_bytes());
    bytes.extend_from_slice(&epoch.to_be_bytes());
    bytes.extend_from_slice(&slot.to_be_bytes());
    bytes.extend(serde_json::to_vec(body).unwrap());
    bytes
}

fn entry(slot: u64) -> LogEntry {
    LogEntry {
        slot,
        client_id: 7,
        request_num: 9,
        message_id: Uuid::from_bytes([3; 16]),
        execution_time: 100,
        payload: request(3, 9),
    }
}

fn assert_cases<T: std::fmt::Debug + Eq + std::hash::Hash>(expected: usize, cases: Vec<T>) {
    assert_eq!(
        cases.len(),
        expected,
        "matrix has an unexpected cardinality: {cases:?}"
    );
    assert_eq!(
        HashSet::<_>::from_iter(cases.iter()).len(),
        expected,
        "matrix has duplicate cases: {cases:?}"
    );
}

#[test]
fn c_abi_null_zero_nonzero_and_decimal_contracts_are_finite() {
    let members = b"0:1\0"
        .iter()
        .chain(b"1:1\0")
        .chain(b"2:1")
        .copied()
        .collect::<Vec<_>>();
    let own = b"0:1";
    let mut cases = Vec::new();
    let mut out = ptr::null_mut();
    let status = unsafe {
        vrr_node_new(
            members.len(),
            members.as_ptr(),
            own.len(),
            own.as_ptr(),
            ptr::null_mut(),
        )
    };
    assert_eq!(status, INVALID);
    cases.push("new-null-out");
    let status = unsafe { vrr_node_new(1, ptr::null(), own.len(), own.as_ptr(), &mut out) };
    assert_eq!(status, INVALID);
    cases.push("new-null-nonzero");
    let status = unsafe { vrr_node_new(0, ptr::null(), own.len(), own.as_ptr(), &mut out) };
    assert_eq!(status, CONFIG);
    cases.push("new-null-zero");

    let node = Node::new(0);
    let json = request(1, 1);
    for (name, decimal, expected) in [
        ("zero", b"0".as_slice(), OK),
        ("maximum", b"18446744073709551615".as_slice(), OK),
        ("empty", b"".as_slice(), NUMBER),
        ("non-decimal", b"one".as_slice(), NUMBER),
        ("overflow", b"18446744073709551616".as_slice(), NUMBER),
    ] {
        assert_eq!(
            node.request(decimal, &json),
            expected,
            "request time {name}"
        );
        cases.push(name);
    }
    assert_eq!(
        unsafe { vrr_node_request(node.0, 1, ptr::null(), json.len(), json.as_ptr()) },
        INVALID
    );
    cases.push("request-null-nonzero");
    assert_eq!(
        unsafe { vrr_node_request(node.0, 0, ptr::null(), json.len(), json.as_ptr()) },
        NUMBER
    );
    cases.push("request-null-zero");
    assert_eq!(
        unsafe { vrr_node_request(node.0, 1, b"1".as_ptr(), 1, ptr::null()) },
        INVALID
    );
    cases.push("json-null-nonzero");
    assert_eq!(
        unsafe { vrr_node_request(node.0, 1, b"1".as_ptr(), 0, ptr::null()) },
        CLIENT_JSON
    );
    cases.push("json-null-zero");
    assert_eq!(
        unsafe { vrr_node_receive(node.0, 0, 1, ptr::null()) },
        INVALID
    );
    cases.push("receive-null-nonzero");
    assert_eq!(
        unsafe { vrr_node_receive(node.0, 0, 0, ptr::null()) },
        VRR_MESSAGE
    );
    cases.push("receive-null-zero");
    for (name, nonce, expected) in [
        ("recover-valid", b"12".as_slice(), OK),
        ("recover-invalid", b"-1".as_slice(), NUMBER),
        (
            "recover-overflow",
            b"18446744073709551616".as_slice(),
            NUMBER,
        ),
    ] {
        assert_eq!(
            unsafe { vrr_node_recover(node.0, nonce.len(), nonce.as_ptr()) },
            expected,
            "{name}"
        );
        cases.push(name);
    }
    assert_eq!(unsafe { vrr_node_recover(node.0, 1, ptr::null()) }, INVALID);
    cases.push("recover-null-nonzero");
    assert_eq!(unsafe { vrr_node_idle(ptr::null_mut()) }, INVALID);
    cases.push("idle-null");
    assert_eq!(unsafe { vrr_node_leader_timeout(ptr::null_mut()) }, INVALID);
    cases.push("timeout-null");
    assert_cases(20, cases);
}

#[test]
fn wire_codec_accepts_each_canonical_tag_and_refuses_non_messages() {
    let state = LogState {
        slot: 1,
        commit: 0,
        log: vec![entry(1)],
    };
    let cases = vec![
        (
            Tag::Prepare,
            Body::Prepare {
                commit: 0,
                entry: entry(1),
            },
        ),
        (Tag::PrepareOk, Body::PrepareOk),
        (Tag::Commit, Body::Commit),
        (Tag::StartEpochChange, Body::StartEpochChange),
        (
            Tag::DoEpochChange,
            Body::DoEpochChange {
                latest_normal: 0,
                state: state.clone(),
            },
        ),
        (
            Tag::StartEpoch,
            Body::StartEpoch {
                state: state.clone(),
            },
        ),
        (Tag::Recovery, Body::Recovery { nonce: 9 }),
        (
            Tag::RecoveryResponse,
            Body::RecoveryResponse {
                nonce: 9,
                state: Some(state),
            },
        ),
    ];
    assert_cases(8, cases.iter().map(|(tag, _)| *tag as u32).collect());
    for (tag, body) in &cases {
        let node = Node::new(1);
        assert_eq!(
            node.receive(0, &wire(*tag, 0x0102_0304, 0x0102_0304_0506_0708, body)),
            OK,
            "{tag:?}"
        );
    }

    let node = Node::new(1);
    let valid = wire(Tag::PrepareOk, 7, 8, &Body::PrepareOk);
    let mut unknown = valid.clone();
    unknown[..4].copy_from_slice(&0xffff_ffffu32.to_be_bytes());
    let mut mismatch = valid.clone();
    mismatch[..4].copy_from_slice(&(Tag::Commit as u32).to_be_bytes());
    let mut trailing = valid.clone();
    trailing.push(b'x');
    for (name, bytes) in [
        ("short-header", valid[..15].to_vec()),
        ("unknown-tag", unknown),
        ("header-body-mismatch", mismatch),
        ("short-body", valid[..16].to_vec()),
        ("trailing-body", trailing),
    ] {
        assert_eq!(node.receive(0, &bytes), VRR_MESSAGE, "{name}");
    }
}

#[test]
fn datagram_boundaries_output_retry_order_and_slots_are_preserved() {
    let node = Node::new(1);
    let mut exact = wire(Tag::Recovery, 3, 4, &Body::Recovery { nonce: 5 });
    exact.extend(std::iter::repeat_n(b' ', MAX_DATAGRAM - exact.len()));
    assert_eq!(exact.len(), MAX_DATAGRAM);
    assert_eq!(node.receive(0, &exact), OK);
    exact.push(b' ');
    assert_eq!(node.receive(0, &exact), TOO_LARGE);

    let backup = Node::new(1);
    let prepare = wire(
        Tag::Prepare,
        0,
        1,
        &Body::Prepare {
            commit: 0,
            entry: entry(1),
        },
    );
    assert_eq!(backup.receive(0, &prepare), OK);
    assert_eq!(unsafe { vrr_node_leader_timeout(backup.0) }, OK);
    let (status, small) = backup.next(0);
    assert_eq!(status, TOO_LARGE);
    assert!(small.len > 0);
    let (status, first) = backup.next(small.len);
    assert_eq!(status, 1);
    assert_eq!(first.kind, 2);
    assert_eq!(first.to, 0);
    assert_eq!(first.tag, Tag::PrepareOk as u32);
    assert_eq!(first.epoch, 0);
    assert_eq!(((first.slot_hi as u64) << 32) | first.slot_lo as u64, 1);
    let (status, second) = backup.next(MAX_DATAGRAM);
    assert_eq!(status, 1);
    assert_eq!(second.kind, 1);
    assert_eq!(second.tag, Tag::StartEpochChange as u32);
    assert_eq!(backup.next(MAX_DATAGRAM).0, 0);
}

#[test]
fn valid_nul_delimited_membership_and_invalid_wire_are_observable() {
    let members = b"0:1\0"
        .iter()
        .chain(b"1:1\0")
        .chain(b"2:1")
        .copied()
        .collect::<Vec<_>>();
    let own = b"0:1";
    let mut raw = ptr::null_mut();
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
    unsafe { vrr_node_free(raw) };

    let node = Node::new(1);
    let bad = LogEntry {
        payload: br#"{"op":"get","message_id":"not-a-uuid"}"#.to_vec(),
        ..entry(1)
    };
    let bytes = wire(
        Tag::Prepare,
        0,
        1,
        &Body::Prepare {
            commit: 0,
            entry: bad,
        },
    );
    assert_eq!(node.receive(0, &bytes), VRR_MESSAGE);
    assert_eq!(unsafe { vrr_node_idle(node.0) }, OK);
}
