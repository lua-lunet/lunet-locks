//! The §13.1 gap-served chunk must apply its whole committed range in one
//! drive, in slot order. The adapter feeds each `Effect::Apply` completion
//! back as `Input::Applied` (§11.1); the core's `plan_applied` refuses any
//! report but the next expected slot — so the feedback order is load-bearing.
//! A chunk carrying several operation slots (the live shape: any client
//! stream produces one) published its install and then failed on its own
//! feedback while the reports drained last-in-first-out.

use lunet_advisory_lock::{Node, OK};
use std::time::{SystemTime, UNIX_EPOCH};

struct TestNode {
    node: Node,
}

impl TestNode {
    fn open(id: u32, name: &str, root: &std::path::Path) -> TestNode {
        let dir = root.join(format!("node{id}"));
        std::fs::create_dir_all(&dir).expect("state dir");
        let members = "65537:a\0 131073:b\0 196609:c".replace("\0 ", "\0");
        let node = Node::open(
            &members,
            name,
            dir.join("state").to_str().expect("path"),
            None,
            0,
            lunet_advisory_lock::PRIMARY_TIMEOUT_MS,
        )
        .expect("the node boots");
        TestNode { node }
    }
}

fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_millis() as u64
}

fn request_json(byte: u8, client: u64, request_num: u64, lease_ms: u64) -> String {
    format!(
        "{{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-{byte:012x}\",\
         \"client_id\":{client},\"request_num\":{request_num},\"lock_id\":7,\
         \"lease\":{{\"lease_id\":1,\"holder\":\"00000000-0000-0000-0000-0000000000{byte:02x}\",\
         \"lease_ms\":{lease_ms}}}}}"
    )
}

/// A live three-node cluster: n1 leads, n3 follows, and n2 is partitioned
/// from the proposal stream. The partitioned node is then handed only the
/// newest `Prepare` — the gap ruling fetches the missing range, the chunk
/// carries every committed operation, and the install must apply them in
/// order without refusing its own acknowledgements.
#[test]
fn a_gap_served_chunk_applies_its_whole_committed_range() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// Drains every queued send output as `(to, packed bytes)`.
fn drain_sends(node: &mut TestNode) -> Vec<(u32, Vec<u8>)> {
    let mut sends = Vec::new();
    while let Some(out) = node.node.next_output() {
        if out.kind == 1 {
            sends.push((out.to, out.bytes));
        }
    }
    sends
}
