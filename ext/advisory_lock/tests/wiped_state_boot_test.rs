//! The wiped-state first boot: the exact rig shape. The whole state tree
//! is absent (wiped), the host recreates the state directories during the
//! boot, and every node boots over a fresh marker — the first quorum
//! write fsyncs the containing directory and must not abort the boot.
//! The crashed-state classification is unchanged: reopening without a
//! stop bumps the incarnation (the running sentinel is a crash).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::{Node, OK};

const MEMBERS: &str = "65537:n1";
const OWN: &str = "n1";

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/wiped-state-boot");
    fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&dir).expect("the case directory creates");
    dir
}

fn settle(node: &mut Node) {
    let mut clock = 0u64;
    for _ in 0..1_000 {
        clock += 1;
        node.set_compliance_clock(clock);
        node.idle();
        while node.next_output().is_some() {}
        if node.status().state == 0 {
            return;
        }
    }
    panic!("the single node did not settle inside the bound");
}

/// A first boot over a wiped state dir (the whole tree absent, the host
/// recreating the directories) boots clean and the crashed-state reopen
/// bumps the incarnation — the running sentinel is a crash, exactly as
/// before the dir-sync fix.
///
/// The one-node rig shape here exercises the MARKER mechanics only: the
/// directory fsync on the first quorum write, the incarnation bump, and
/// the stop drain. Serving a client operation is a consensus claim and a
/// single node cannot make one — a cluster of one has no quorum, so a
/// proposal it accepts can never commit. The ruling is in
/// docs/src/decisions.md: serving requires three voting nodes.
#[test]
fn a_first_boot_over_a_wiped_state_dir_boots_and_a_crash_bumps() {
    let dir = scratch("wiped");
    let state_dir = dir.join("state");
    let _ = fs::remove_dir_all(&state_dir);
    // The host recreates the state directories during the boot.
    fs::create_dir_all(&state_dir).expect("the state directory recreates");
    let state = state_dir.join("n1.state");
    // The first boot: no marker anywhere. The first quorum write fsyncs
    // the containing directory and must not abort the boot.
    let mut node = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the first boot over the wiped tree boots clean");
    assert_eq!(node.own_id(), 65_537, "the genesis pair (system 1, life 1)");
    // The node seats itself (a 1-node cluster elects its own leader).
    settle(&mut node);
    // The crash: dropped without the stop contract — the running
    // sentinel is a crash, and the reopen bumps the incarnation.
    drop(node);
    let mut life_two = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the crashed reopen bumps and boots");
    assert_eq!(life_two.own_id(), 65_538, "the crash counter bumped");
    // The unseated window's stop drains and exits (a 1-node rig has no
    // live leader to seat the new life); the boot itself completed.
    assert_eq!(life_two.stop(), OK);
}
