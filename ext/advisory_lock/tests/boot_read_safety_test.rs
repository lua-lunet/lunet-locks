//! The boot-read safety law, proven end to end in the repo's scratch
//! tree: the full cluster cycle (clean halt, clean start, crash
//! restart) completes without lockup; corrupt bytes in a superblock
//! copy panic the next boot (never hang, never self-heal); a torn
//! spread resolves by the stated thresholds with the non-unanimity
//! visible in the store's per-copy facts. Real code, real superblock
//! files.

use std::fs;
use std::io::{Read as _, Seek, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::{Node, OK, PANIC};
use lunet_locks_aof::marker::{self, MarkerState};

const MEMBERS: &str = "65537:n1";
const OWN: &str = "n1";

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/boot-read-safety");
    fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{name}-{}-{}",
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

fn classify(state: &Path) -> Result<(u16, u16, MarkerState), i32> {
    marker::classify(&superblock(state)).map(|classified| {
        (
            classified.identity.system_identifier(),
            classified.identity.crash_counter(),
            classified.state,
        )
    })
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

/// A clean stop, the artifact every case below starts from: the halted
/// node's marker reads flushed at the genesis pair.
fn stopped_clean_store(name: &str) -> (PathBuf, PathBuf) {
    let dir = scratch(name);
    let state = dir.join("node.state");
    let mut node = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the first life boots");
    settle(&mut node);
    assert_eq!(node.stop(), OK);
    assert_eq!(classify(&state), Ok((1, 1, MarkerState::Flushed)));
    (state.clone(), superblock(&state))
}

/// The full cluster cycle — clean halt, clean start, crash restart —
/// completes without lockup: every boot boots inside the settle bound,
/// the clean start continues at the same identity, and the crash
/// restart (dropped without the stop contract) bumps the incarnation.
#[test]
fn the_full_cycle_halts_starts_and_crash_restarts_without_lockup() {
    let dir = scratch("full-cycle");
    let state = dir.join("node.state");
    // The clean halt: settle, stop, the marker at flushed.
    let mut node = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the first life boots");
    settle(&mut node);
    assert_eq!(node.stop(), OK);
    assert_eq!(classify(&state), Ok((1, 1, MarkerState::Flushed)));
    // The clean start: the same identity, no bump.
    let mut resumed = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the clean start boots");
    assert_eq!(resumed.own_id(), 65_537, "the clean start never bumps");
    settle(&mut resumed);
    // The crash restart: dropped without the stop contract, the running
    // sentinel is a crash, and the reopen bumps the incarnation.
    drop(resumed);
    assert_eq!(classify(&state), Ok((1, 1, MarkerState::Unflushed)));
    let mut third = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the crashed boot bumps and starts");
    assert_eq!(third.own_id(), 65_538, "the crash counter bumped");
    // The reincarnated node drives without lockup: it waits for the
    // leader's forced sequence (a 1-node rig has no live leader to seat
    // it), never self-arrests, every drive completing inside the bound.
    let mut clock = 0u64;
    for _ in 0..2_000 {
        clock += 1;
        third.set_compliance_clock(clock);
        third.idle();
        while third.next_output().is_some() {}
        assert!(
            !third.status().poisoned,
            "the node never self-arrests across the cycle"
        );
    }
    // The unseated window's stop: the sink drains and exits, no marker
    // round — the running sentinel stands, and the next boot derives the
    // next life.
    assert_eq!(third.stop(), OK);
    assert_eq!(classify(&state), Ok((1, 2, MarkerState::Unflushed)));
    let mut fourth = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the next boot derives the next life");
    assert_eq!(fourth.own_id(), 65_539, "the next life derived");
    assert_eq!(fourth.stop(), OK);
}

/// THE BOOT-READ SAFETY LAW: garbage over one superblock copy's zone is
/// a checksum failure on a readable copy — the next boot panics inside
/// the boot gate (the boundary reports PANIC: never a hang, never a
/// guessed start), and the corrupt bytes stand exactly as written: the
/// store is never cleared, never repaired, never fallen back.
#[test]
fn a_corrupted_copy_panics_the_next_boot_and_is_never_healed() {
    let (state, record) = stopped_clean_store("corrupt-copy");
    let before = fs::read(&record).expect("the store reads");
    let geometry = marker::geometry().expect("the geometry reports");
    assert_eq!(geometry.copies, 4, "four superblock copies");
    // The copies are the file's four uniform zones.
    let zone = before.len() / geometry.copies;
    assert_eq!(before.len(), zone * geometry.copies, "four fixed zones");
    // Garbage over copy 2's whole zone: a checksum failure on ANY copy.
    let mut rotted = before.clone();
    let start = 2 * zone;
    for byte in &mut rotted[start..start + zone] {
        *byte = 0xA5;
    }
    fs::write(&record, &rotted).expect("the rot writes");
    let copies = marker::inspect(&record).expect("the store inspects");
    assert_eq!(copies.len(), 4);
    assert_eq!(copies[2].valid_checksum, 0, "copy 2 rotted");
    assert!(
        copies
            .iter()
            .enumerate()
            .all(|(index, one)| index == 2 || one.valid_checksum != 0),
        "the other three copies stand"
    );
    assert_eq!(
        marker::classify(&record),
        Err(marker::CORRUPT),
        "the read refuses with the distinct code"
    );
    // The next boot panics inside the boot gate; the boundary reports
    // the PANIC code — the call returns, it never hangs.
    let refused = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50);
    assert_eq!(
        refused.err(),
        Some(PANIC),
        "the boot panics loudly, never hangs"
    );
    // The rot stands after the refused boot: no self-heal.
    let after = fs::read(&record).expect("the store reads");
    assert_eq!(after, rotted, "the corrupt bytes stand exactly as written");
    assert_eq!(
        marker::classify(&record),
        Err(marker::CORRUPT),
        "the read refuses again: nothing was healed"
    );
}

/// A torn spread — checksum-valid copies at differing generations —
/// resolves by the stated thresholds: the read works from the `.open`
/// threshold (2/4), the resolution goes to the highest-sequence copies
/// (a stale copy cannot drag the classification back), and the
/// non-unanimity is visible in the store's per-copy facts (the Zig
/// store logs it in full at the moment of resolution). The boot acts on
/// the resolved verdict — the running sentinel reads as a crash —
/// without refusing and without lockup.
#[test]
fn a_torn_spread_resolves_by_thresholds_with_the_logged_non_unanimity() {
    let (state, record) = stopped_clean_store("torn-spread");
    let geometry = marker::geometry().expect("the geometry reports");
    // The copies are the file's four uniform zones.
    let zone = fs::metadata(&record).expect("the length reads").len() as usize / geometry.copies;
    // Snapshot copy 3 at the current generation.
    let stale: Vec<u8> = {
        let mut file = fs::File::open(&record).expect("the store opens");
        let mut zone_bytes = vec![0u8; zone];
        file.seek(SeekFrom::Start((3 * zone) as u64))
            .expect("the zone seeks");
        file.read_exact(&mut zone_bytes).expect("the zone reads");
        zone_bytes
    };
    // The next transition: a full quorum write to the running sentinel.
    let identity = marker::NodeIdentity::new(1, 1).expect("the pair spells");
    marker::write(&record, identity, MarkerState::Unflushed).expect("the transition writes");
    // The tear: restore the snapshot — copy 3 checksum-valid at the
    // older generation while copies 0-2 carry the newer one.
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&record)
            .expect("the store opens for the restore");
        file.seek(SeekFrom::Start((3 * zone) as u64))
            .expect("the zone seeks");
        file.write_all(&stale).expect("the stale zone restores");
    }
    // The per-copy facts: every copy checksum-valid (the tear is not
    // rot), the states differ, and the newer generation outnumbers the
    // stale one three to one.
    let copies = marker::inspect(&record).expect("the store inspects");
    assert_eq!(copies.len(), 4);
    assert!(
        copies.iter().all(|one| one.valid_checksum != 0),
        "every copy is checksum-valid: the tear is not rot"
    );
    assert!(
        states(&copies).contains(&MarkerState::Unflushed.code())
            && states(&copies).contains(&MarkerState::Flushed.code()),
        "the spread carries differing states"
    );
    let top = copies
        .iter()
        .map(|one| one.sequence)
        .max()
        .expect("a sequence");
    assert_eq!(
        copies.iter().filter(|one| one.sequence == top).count(),
        3,
        "three copies at the newer generation"
    );
    // The read resolves by the thresholds: the highest-sequence copies'
    // verdict, never the stale copy's.
    assert_eq!(
        classify(&state),
        Ok((1, 1, MarkerState::Unflushed)),
        "the read resolves to the newer generation"
    );
    // The boot acts on the resolved verdict: the running sentinel reads
    // as a crash — the boot bumps and starts, never refuses, never
    // hangs. The reincarnated node drives without lockup (a 1-node rig
    // has no live leader to seat it), and the unseated window's stop
    // leaves the running sentinel for the next boot's derivation.
    let mut node = Node::open_compliance(MEMBERS, OWN, &state.to_string_lossy(), 50)
        .expect("the torn spread resolves at the boot");
    assert_eq!(
        node.own_id(),
        65_538,
        "the resolved verdict is a crash: the boot bumps"
    );
    let mut clock = 0u64;
    for _ in 0..2_000 {
        clock += 1;
        node.set_compliance_clock(clock);
        node.idle();
        while node.next_output().is_some() {}
        assert!(
            !node.status().poisoned,
            "the node never self-arrests on the resolved boot"
        );
    }
    assert_eq!(node.stop(), OK);
    assert_eq!(classify(&state), Ok((1, 2, MarkerState::Unflushed)));
}

fn states(copies: &[marker::CopyInfo]) -> Vec<u32> {
    copies.iter().map(|one| one.state).collect()
}
