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
        let members = ["1:a", "2:b", "3:c"].join("\0");
        let node = Node::open(
            &members,
            name,
            dir.join("state").to_str().expect("path"),
            None,
            0,
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
    let root = std::env::temp_dir().join(format!(
        "lunet-wiped-boot-{}-{}",
        std::process::id(),
        millis()
    ));
    std::fs::create_dir_all(&root).expect("scratch root");

    let mut n1 = TestNode::open(1, "a", &root);
    let mut n3 = TestNode::open(3, "c", &root);

    // Genesis: the primary self-promotes on the first tick.
    assert_eq!(n1.node.idle(), OK);
    let announces: Vec<(u32, Vec<u8>)> = drain_sends(&mut n1);
    assert!(!announces.is_empty(), "the primary announces itself");
    for (_, bytes) in &announces {
        assert_eq!(n3.node.receive(1, bytes), OK, "n3 adopts the view");
        for (_, bytes) in drain_sends(&mut n3) {
            assert_eq!(n1.node.receive(3, &bytes), OK);
            drain_sends(&mut n1);
        }
    }

    // One client operation commits on n1 + n3 alone; the reply is served.
    let json = request_json(7, 1, 1, 60_000);
    assert_eq!(n1.node.request(json.as_bytes()), OK, "the op proposed");
    for (to, bytes) in drain_sends(&mut n1) {
        if to == 2 {
            continue;
        }
        assert_eq!(n3.node.receive(1, &bytes), OK);
    }
    // Route until quiet: the PrepareOks, the Commit, and the apply's
    // publish ride back and forth until the cluster is still.
    let mut replies = Vec::new();
    for _ in 0..100 {
        let mut moved = false;
        let (sends, mut round_replies) = drain(&mut n1);
        replies.append(&mut round_replies);
        for (to, bytes) in sends {
            if to == 3 {
                assert_eq!(n3.node.receive(1, &bytes), OK);
                moved = true;
            }
        }
        let (sends, mut round_replies) = drain(&mut n3);
        replies.append(&mut round_replies);
        for (to, bytes) in sends {
            if to == 1 {
                assert_eq!(n1.node.receive(3, &bytes), OK);
                moved = true;
            }
        }
        if !moved {
            break;
        }
    }
    assert_eq!(
        replies.len(),
        1,
        "the committed operation's reply is served"
    );

    // The first boot seeded the quorum copies next to the marker file.
    let superblock = root.join("node1/state.superblock");
    assert!(superblock.exists(), "the marker copies exist");
    assert_eq!(
        std::fs::read_to_string(root.join("node1/state")).unwrap(),
        "0 unflushed\n",
        "the projection records the first boot's identity"
    );

    // A crash (no stop): the reopen classifies the running sentinel as a
    // crash and bumps — unchanged.
    drop(n1);
    drop(n3);
    let mut n1 = TestNode::open(1, "a", &root);
    assert_eq!(n1.node.idle(), OK);
    drain_sends(&mut n1);
    assert_eq!(
        std::fs::read_to_string(root.join("node1/state")).unwrap(),
        "1 unflushed\n",
        "the crashed-state boot bumps the incarnation"
    );

    let _ = std::fs::remove_dir_all(&root);
}
