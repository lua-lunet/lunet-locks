//! The wiped-state first boot: the exact rig shape. The whole state tree
//! is absent (wiped), the host recreates the state directories during the
//! boot, and every node boots over a fresh marker — the first quorum
//! write fsyncs the containing directory and must not abort the boot.
//! The crashed-state classification is unchanged: reopening without a
//! stop bumps the incarnation (the running sentinel is a crash).

use lunet_advisory_lock::{Node, OK};
use std::time::{SystemTime, UNIX_EPOCH};

struct TestNode {
    node: Node,
}

impl TestNode {
    fn open(id: u32, name: &str, root: &std::path::Path) -> TestNode {
        let dir = root.join(format!("node{id}"));
        // The host's dir creation: the wiped state tree is recreated here,
        // before the marker write touches it.
        std::fs::create_dir_all(&dir).expect("state dir");
        let members = ["65537:a", "131073:b", "196609:c"].join("\0");
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

/// Drains every queued output: the sends as `(to, packed bytes)`, the
/// client replies alongside.
fn drain(node: &mut TestNode) -> (Vec<(u32, Vec<u8>)>, Vec<lunet_advisory_lock::NodeOutput>) {
    let mut sends = Vec::new();
    let mut replies = Vec::new();
    while let Some(out) = node.node.next_output() {
        match out.kind {
            1 => sends.push((out.to, out.bytes)),
            2 => replies.push(out),
            _ => {}
        }
    }
    (sends, replies)
}

/// Drains every queued send output as `(to, packed bytes)`.
fn drain_sends(node: &mut TestNode) -> Vec<(u32, Vec<u8>)> {
    drain(node).0
}

/// A first boot over a wiped state dir (the whole tree absent, the host
/// recreating the directories) boots clean, serves a committed client
/// operation, and the crashed-state reopen bumps the incarnation — the
/// running sentinel is a crash, exactly as before the dir-sync fix.
#[test]
fn a_first_boot_over_a_wiped_state_dir_boots_serves_and_a_crash_bumps() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
