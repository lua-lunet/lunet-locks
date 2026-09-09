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
        let members = "1:a\0 2:b\0 3:c".replace("\0 ", "\0");
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

fn request_json(byte: u8, client: u64, request_num: u64, expiry: u64) -> String {
    format!(
        "{{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-{byte:012x}\",\
         \"client_id\":{client},\"request_num\":{request_num},\"lock_id\":7,\
         \"lease\":{{\"lease_id\":1,\"holder\":\"00000000-0000-0000-0000-0000000000{byte:02x}\",\
         \"expiry\":{expiry}}}}}"
    )
}

/// A live three-node cluster: n1 leads, n3 follows, and n2 is partitioned
/// from the proposal stream. The partitioned node is then handed only the
/// newest `Prepare` — the gap ruling fetches the missing range, the chunk
/// carries every committed operation, and the install must apply them in
/// order without refusing its own acknowledgements.
#[test]
fn a_gap_served_chunk_applies_its_whole_committed_range() {
    let root = std::env::temp_dir().join(format!(
        "lunet-gap-apply-{}-{}",
        std::process::id(),
        millis()
    ));
    std::fs::create_dir_all(&root).expect("scratch root");

    let mut n1 = TestNode::open(1, "a", &root);
    let mut n2 = TestNode::open(2, "b", &root);
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

    // Three client operations commit on n1 + n3 alone; n2 hears nothing.
    let expiry = millis() + 60_000;
    let mut held_prepare: Option<Vec<u8>> = None;
    for op in 1..=3u8 {
        let json = request_json(op, 1, u64::from(op), expiry);
        assert_eq!(n1.node.request(json.as_bytes()), OK, "op {op} proposed");
        for (to, bytes) in drain_sends(&mut n1) {
            if to == 2 {
                // The partition: n2 receives nothing yet — the newest
                // Prepare is kept for the gap trigger below.
                held_prepare = Some(bytes);
                continue;
            }
            assert_eq!(n3.node.receive(1, &bytes), OK);
            for (_, ack) in drain_sends(&mut n3) {
                assert_eq!(n1.node.receive(3, &ack), OK, "the vote is counted");
                drain_sends(&mut n1);
            }
        }
    }
    let held_prepare = held_prepare.expect("a Prepare was held back from n2");

    // The gap ruling at n2: the Prepare past the accepted frontier's
    // successor opens the fetch.
    assert_eq!(n2.node.receive(1, &held_prepare), OK, "the gap is named");
    let fetch: Vec<(u32, Vec<u8>)> = drain_sends(&mut n2);
    assert!(!fetch.is_empty(), "the fetch rides the ruling");
    for (to, bytes) in &fetch {
        assert_eq!(*to, 1, "the fetch asks the primary");
        assert_eq!(n1.node.receive(2, bytes), OK, "the primary serves");
    }
    let answers: Vec<(u32, Vec<u8>)> = drain_sends(&mut n1);
    assert!(!answers.is_empty(), "the chunk is queued");

    // The chunk answers: it carries every committed operation slot, and
    // the install applies them in slot order in one drive.
    for (to, bytes) in &answers {
        assert_eq!(*to, 2, "the answer routes to the requester");
        assert_eq!(
            n2.node.receive(1, bytes),
            OK,
            "the multi-operation chunk applies without refusing its own acknowledgements"
        );
        drain_sends(&mut n2);
    }

    // The node is whole again: the next operation is accepted and
    // acknowledged like any member's.
    let json = request_json(4, 1, 4, expiry);
    assert_eq!(n1.node.request(json.as_bytes()), OK);
    let mut next_prepare = None;
    for (to, bytes) in drain_sends(&mut n1) {
        if to == 2 {
            next_prepare = Some(bytes);
            continue;
        }
        assert_eq!(n3.node.receive(1, &bytes), OK);
        for (_, ack) in drain_sends(&mut n3) {
            assert_eq!(n1.node.receive(3, &ack), OK);
            drain_sends(&mut n1);
        }
    }
    let next_prepare = next_prepare.expect("the rejoined member's Prepare");
    assert_eq!(n2.node.receive(1, &next_prepare), OK, "n2 accepts the tail");
    let acks = drain_sends(&mut n2);
    assert!(!acks.is_empty(), "n2 acknowledges again");

    let _ = std::fs::remove_dir_all(&root);
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
