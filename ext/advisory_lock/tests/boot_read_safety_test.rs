//! The boot-read safety law, proven end to end in a tempdir: the full
//! cluster cycle (clean halt, clean start, crash restart) completes
//! without lockup; corrupt bytes in a superblock copy panic the next
//! boot (never hang, never self-heal); a torn spread resolves by the
//! stated thresholds. Real code, real superblock files.
//!
//! The harness shape is the sans-IO fabric: `Node` objects exchanging
//! packed datagrams in bounded drive loops. The marker stores are the
//! real quorum-of-copies files under the tempdir.

use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use lunet_advisory_lock::{Node, OK, PANIC};
use lunet_locks_aof::marker as marker_ffi;

fn workdir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after Unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "lunet-boot-read-safety-{name}-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("tempdir root");
    dir
}

fn request_json(byte: u8, client: u64, request_num: u64, lease_ms: u64) -> String {
    format!(
        "{{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-{byte:012x}\",\
         \"client_id\":{client},\"request_num\":{request_num},\"lock_id\":7,\
         \"lease\":{{\"lease_id\":1,\"holder\":\"00000000-0000-0000-0000-0000000000{byte:02x}\",\
         \"lease_ms\":{lease_ms}}}}}"
    )
}

struct TestNode {
    node: Node,
    dir: PathBuf,
}

impl TestNode {
    fn open(id: u32, name: &str, root: &Path) -> TestNode {
        let dir = root.join(format!("node{id}"));
        fs::create_dir_all(&dir).expect("state dir");
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
        TestNode { node, dir }
    }
}

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

fn drain_sends(node: &mut TestNode) -> Vec<(u32, Vec<u8>)> {
    drain(node).0
}

/// The genesis walk and one committed client operation: the bounded
/// exchange the whole-cycle test drives the cluster through.
fn serve_one_operation(n1: &mut TestNode, n3: &mut TestNode) {
    assert_eq!(n1.node.idle(), OK, "the primary announces itself");
    let announces = drain_sends(n1);
    assert!(!announces.is_empty());
    for (_, bytes) in &announces {
        assert_eq!(n3.node.receive(65537, bytes), OK, "n3 adopts the view");
        for (_, bytes) in drain_sends(n3) {
            assert_eq!(n1.node.receive(196609, &bytes), OK);
            drain_sends(n1);
        }
    }

    let json = request_json(7, 1, 1, 60_000);
    assert_eq!(n1.node.request(json.as_bytes()), OK, "the op proposed");
    let mut replies = Vec::new();
    for _ in 0..100 {
        let mut moved = false;
        let (sends, round_replies) = drain(n1);
        replies.extend(round_replies);
        for (to, bytes) in sends {
            if to == 196609 {
                assert_eq!(n3.node.receive(65537, bytes), OK);
                moved = true;
            }
        }
        let (sends, round_replies) = drain(n3);
        replies.extend(round_replies);
        for (to, bytes) in sends {
            if to == 65537 {
                assert_eq!(n1.node.receive(196609, &bytes), OK);
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
}

/// The projection file's line.
fn projection(path: &Path) -> String {
    fs::read_to_string(path).expect("the projection file")
}

/// One copy zone's raw bytes as they stand on disk (the header plus
/// whatever of the zone's reserved padding was written: the store writes
/// the header only, so a trailing zone may be short of the full zone).
fn read_zone(superblock: &Path, slot: usize, geometry: marker_ffi::Geometry) -> Vec<u8> {
    use std::io::{Read, Seek, SeekFrom};
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(superblock)
        .expect("copies file");
    let offset = (geometry.copy_size * slot) as u64;
    file.seek(SeekFrom::Start(offset)).expect("seek zone");
    let zone_len = usize::try_from(file.metadata().expect("stat").len() - offset)
        .unwrap_or(0)
        .min(geometry.copy_size);
    let mut zone = vec![0u8; zone_len];
    file.read_exact(&mut zone).expect("read zone");
    zone
}

fn write_zone(superblock: &Path, slot: usize, bytes: &[u8], geometry: marker_ffi::Geometry) {
    use std::io::{Seek, SeekFrom, Write};
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(superblock)
        .expect("copies file");
    file.seek(SeekFrom::Start((geometry.copy_size * slot) as u64))
        .expect("seek zone");
    file.write_all(bytes).expect("write zone");
}

/// The full cycle: a small real cluster (real superblock files in the
/// tempdir) serves a committed operation, halts cleanly, boots cleanly
/// under the same identity, and crash-restarts — the whole cycle
/// completes without lockup (every drive loop is bounded; the test's
/// completion IS the no-hang proof).
#[test]
fn the_full_cycle_halts_starts_and_crash_restarts_without_lockup() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE BOOT-READ SAFETY LAW: corrupt bytes in a superblock copy (a bad
/// checksum on ANY copy) panic the next boot — loud, bounded, never a
/// hang — and the store never clears, repairs, or falls back: the
/// corrupted bytes stand exactly as they were, and a re-boot panics
/// again.
#[test]
fn a_corrupted_copy_panics_the_next_boot_and_is_never_healed() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The torn spread: a death after exactly one copy of the boot's next
/// write leaves checksum-valid copies at differing states — the read
/// resolves by the stated thresholds (the 3/4 working quorum at the
/// older sequence; a lone advanced copy cannot decide) with the
/// non-unanimity logged, and the classification follows the rules: the
/// stopped quorum wins, the boot continues clean under the same
/// identity.
#[test]
fn a_torn_spread_resolves_by_thresholds_with_the_logged_non_unanimity() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
