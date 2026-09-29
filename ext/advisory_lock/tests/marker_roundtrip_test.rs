//! The marker pair, the view record, and the projection roundtrip at the
//! boot-gate boundary, through the real Zig superblock store in the
//! repo's scratch tree: the identity pair in == the pair out, the state
//! transitions Unflushed -> Stopped -> Flushed, the checksum-rot and
//! regress and foreign-system refusals, and — through the adapter's
//! boot gate — the view ballot written in the drain window read back at
//! the clean classification, and the `<system> <crash> <state>`
//! projection's refusals.
//!
//! The old-format (`INCOMPATIBLE`) refusal is proven on the Zig side
//! (`ext/lunet-locks-aof/zig/src/marker.zig`, the old-format test): a
//! checksum-valid old-format copy is not forgeable from Rust — the Aegis
//! checksum covers the version field — so the Rust adapter's
//! INCOMPATIBLE branch rests on that suite.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::{Node, OK};
use lunet_locks_aof::marker::{self, CORRUPT, MarkerState, NodeIdentity};

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/marker-roundtrip");
    fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&dir).expect("the case directory creates");
    dir
}

fn superblock(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".superblock");
    PathBuf::from(os)
}

/// The view record's path for a state path (`<state>.view`).
fn view_record(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".view");
    PathBuf::from(os)
}

fn classify(state: &Path) -> Result<(u16, u16, MarkerState), i32> {
    marker::classify(&superblock(state)).map(|classified| {
        (
            classified.identity.system_identifier(),
            classified.identity.crash_counter(),
            classified.state,
        )
    })
}

#[test]
fn format_write_classify_roundtrips_the_pair_and_the_states() {
    let dir = scratch("pair-roundtrip");
    let state = dir.join("node.state");
    let identity = NodeIdentity::new(7, 2).expect("the pair spells");

    // The format: a fresh store at the named (identity, state).
    marker::format(&superblock(&state), identity, MarkerState::Unflushed)
        .expect("the store formats");
    assert_eq!(classify(&state), Ok((7, 2, MarkerState::Unflushed)));

    // The transitions: the same life carries stopped/flushed; a bump
    // advances the counter and returns the sentinel.
    marker::write(&superblock(&state), identity, MarkerState::Stopped)
        .expect("the first round writes");
    assert_eq!(classify(&state), Ok((7, 2, MarkerState::Stopped)));
    marker::write(&superblock(&state), identity, MarkerState::Flushed)
        .expect("the second round writes");
    assert_eq!(classify(&state), Ok((7, 2, MarkerState::Flushed)));
    let bumped = NodeIdentity::new(7, 3).expect("the pair spells");
    marker::write(&superblock(&state), bumped, MarkerState::Unflushed).expect("the bump writes");
    assert_eq!(classify(&state), Ok((7, 3, MarkerState::Unflushed)));

    // The regress guard: a counter that would go back refuses.
    assert_eq!(
        marker::write(&superblock(&state), identity, MarkerState::Stopped),
        Err(lunet_locks_aof::ffi::INVALID),
        "the crash counter never regresses"
    );
    // A different system on an existing marker is corruption, not an
    // overwrite.
    let foreign = NodeIdentity::new(8, 4).expect("the pair spells");
    assert_eq!(
        marker::write(&superblock(&state), foreign, MarkerState::Stopped),
        Err(CORRUPT),
        "a foreign system half refuses as CORRUPT"
    );
}

#[test]
fn the_pair_packs_and_unpacks_at_both_edges_and_refuses_zero_halves() {
    for (system, counter) in [(1u16, 1u16), (1, 65_535), (65_535, 1), (65_535, 65_535)] {
        let identity = NodeIdentity::new(system, counter).expect("the pair spells");
        assert_eq!(identity.system_identifier(), system);
        assert_eq!(identity.crash_counter(), counter);
        assert_eq!(NodeIdentity::from_packed(identity.packed()), Some(identity));
    }
    assert_eq!(
        NodeIdentity::new(0, 5),
        None,
        "a zero system half is no identity"
    );
    assert_eq!(
        NodeIdentity::new(5, 0),
        None,
        "a zero counter half is no identity"
    );
    assert_eq!(NodeIdentity::from_packed(0), None);
    assert_eq!(
        NodeIdentity::from_packed(0x0000_0005),
        None,
        "system half zero"
    );
    assert_eq!(
        NodeIdentity::from_packed(0x0005_0000),
        None,
        "counter half zero"
    );
}

#[test]
fn a_checksum_rotted_store_refuses_corrupt_and_an_absent_file_refuses_the_read() {
    let dir = scratch("corrupt");
    let state = dir.join("node.state");
    let identity = NodeIdentity::new(7, 2).expect("the pair spells");
    marker::write(&superblock(&state), identity, MarkerState::Unflushed).expect("the store writes");
    let superblock = superblock(&state);
    let length = fs::metadata(&superblock).expect("the length reads").len();

    // Zeroing every copy rots every checksum: the read refuses with the
    // distinct code and nothing else happens to the file.
    fs::write(&superblock, vec![0u8; length as usize]).expect("the store rots");
    assert_eq!(classify(&state), Err(CORRUPT));
    // The rotted store is never healed by a read.
    assert_eq!(classify(&state), Err(CORRUPT), "the rot stands");

    // A marker file that never existed is no verdict: the read refuses
    // (the caller distinguishes the first life by the file's absence).
    let missing = dir.join("never.state");
    assert!(
        classify(&missing).is_err(),
        "an absent marker file refuses the classification"
    );
}

// ----------------------------------------------------------------------
// The boot gate's boundaries: the projection line and the view record,
// driven through the adapter's Node over the real store.
// ----------------------------------------------------------------------

const MEMBERS: &str = "65537:n1";
const OWN: &str = "n1";

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

/// The projection file's line: `<system> <crash> <state-word>`.
fn projection(state: &Path) -> String {
    fs::read_to_string(state).expect("the projection reads")
}

#[test]
fn the_projection_line_roundtrips_the_pair_and_the_states() {
    let dir = scratch("projection");
    let state = dir.join("node.state");
    let mut node = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the first life boots");
    assert_eq!(node.own_id(), 65_537, "the genesis pair (system 1, life 1)");

    // The first latch: the running sentinel, both stores.
    assert_eq!(projection(&state), "1 1 unflushed\n");
    let superblock = superblock(&state);
    let classified = marker::classify(&superblock).expect("the store classifies");
    assert_eq!(classified.identity.packed(), 65_537);
    assert_eq!(classified.state, MarkerState::Unflushed);

    // The graceful stop: the halt's two rounds, the drain proven.
    settle(&mut node);
    assert_eq!(node.stop(), OK);
    assert_eq!(projection(&state), "1 1 flushed\n");
    let classified = marker::classify(&superblock).expect("the store classifies");
    assert_eq!(classified.identity.packed(), 65_537);
    assert_eq!(classified.state, MarkerState::Flushed);

    // The clean classification continues under the SAME identity.
    let resumed = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the clean start boots");
    assert_eq!(resumed.own_id(), 65_537);
}

#[test]
fn the_view_record_roundtrips_the_stopped_ballot_through_the_clean_classification() {
    let dir = scratch("view-record");
    let state = dir.join("node.state");
    let mut node = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the first life boots");
    settle(&mut node);

    // Force a view change so the stopped ballot is not the genesis one.
    let before = node.status();
    assert_eq!(node.force_view(before.era, before.view + 1), OK);
    while node.next_output().is_some() {}
    settle(&mut node);
    let after = node.status();
    assert_eq!(after.view, before.view + 1, "the fence advanced the view");

    // The stop writes the view record in the drain window, strictly
    // between the halt's two marker rounds.
    assert_eq!(node.stop(), OK);
    let record = fs::read_to_string(view_record(&state)).expect("the view record reads");
    assert_eq!(
        record,
        format!("{} {}\n", after.era, after.view),
        "the drain window's line is the stopped ballot"
    );

    // The clean classification reads it back: the node resumes at the
    // view it stopped at.
    let resumed = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the clean start boots");
    assert_eq!(resumed.own_id(), 65_537);
    let status = resumed.status();
    assert_eq!(status.era, after.era, "the era survives the roundtrip");
    assert_eq!(status.view, after.view, "the view survives the roundtrip");
}

/// A clean stop first, then the view record tampered with, then the
/// refusal: the boot gate reads the record at the clean classification.
fn stopped_clean_store(name: &str) -> (PathBuf, PathBuf) {
    let dir = scratch(name);
    let state = dir.join("node.state");
    let mut node = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the first life boots");
    settle(&mut node);
    assert_eq!(node.stop(), OK);
    (state.clone(), view_record(&state))
}

#[test]
fn a_view_record_naming_an_era_the_table_does_not_carry_refuses_the_boot() {
    let (state, record) = stopped_clean_store("era-mismatch");
    fs::write(&record, "99 5\n").expect("the mismatch writes");
    let refused = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50);
    assert_eq!(
        refused.err(),
        Some(lunet_advisory_lock::CONFIG),
        "the clean start refuses rather than resuming over an era it cannot name"
    );
}

#[test]
fn a_torn_view_record_refuses_the_clean_boot() {
    let (state, record) = stopped_clean_store("torn-record");
    fs::write(&record, "1\n").expect("the torn record writes");
    let refused = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50);
    assert_eq!(
        refused.err(),
        Some(lunet_advisory_lock::CONFIG),
        "an unreadable record is an error, never a guessed view"
    );
}

#[test]
fn an_absent_view_record_boots_at_the_genesis_view() {
    let (state, record) = stopped_clean_store("absent-record");
    fs::remove_file(&record).expect("the record removes");
    let resumed = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the clean start boots without a record");
    assert_eq!(resumed.status().view, 0, "no record, no view to resume");
}

#[test]
fn the_projection_refuses_a_foreign_system_half() {
    let dir = scratch("foreign-projection");
    let state = dir.join("node.state");
    fs::write(&state, "9 3 unflushed\n").expect("the foreign projection writes");
    let refused = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50);
    assert_eq!(
        refused.err(),
        Some(lunet_advisory_lock::CONFIG),
        "the projection naming another system never boots"
    );
}

#[test]
fn the_projection_refuses_a_zero_half_at_both_edges() {
    for line in ["0 1 unflushed\n", "1 0 unflushed\n"] {
        let dir = scratch("zero-projection");
        let state = dir.join("node.state");
        fs::write(&state, line).expect("the zero-half projection writes");
        let refused = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50);
        assert_eq!(
            refused.err(),
            Some(lunet_advisory_lock::CONFIG),
            "a zero half is no identity: {line}"
        );
    }
}
