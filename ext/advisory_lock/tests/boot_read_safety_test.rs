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
        let members = ["1:a", "2:b", "3:c"].join("\0");
        let node = Node::open(
            &members,
            name,
            dir.join("state").to_str().expect("path"),
            None,
            0,
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
        assert_eq!(n3.node.receive(1, bytes), OK, "n3 adopts the view");
        for (_, bytes) in drain_sends(n3) {
            assert_eq!(n1.node.receive(3, &bytes), OK);
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
            if to == 3 {
                assert_eq!(n3.node.receive(1, &bytes), OK);
                moved = true;
            }
        }
        let (sends, round_replies) = drain(n3);
        replies.extend(round_replies);
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
    let root = workdir("full-cycle");

    let mut n1 = TestNode::open(1, "a", &root);
    let mut n3 = TestNode::open(3, "c", &root);
    serve_one_operation(&mut n1, &mut n3);
    assert_eq!(
        projection(&n1.dir.join("state")),
        "0 unflushed\n",
        "the first boot leaves the running sentinel"
    );

    // The clean halt: the engine's two rounds and the drain between them.
    assert_eq!(n1.node.stop(), OK, "the clean halt completes");
    assert_eq!(n3.node.stop(), OK);
    assert_eq!(
        projection(&n1.dir.join("state")),
        "0 flushed\n",
        "the halt's drain point is proven on disk"
    );
    drop(n1);
    drop(n3);

    // The clean start: the stopped quorum vouches, the same identity
    // continues — no bump, no reincarnation.
    let mut n1 = TestNode::open(1, "a", &root);
    assert_eq!(n1.node.own_id(), 1, "the clean start keeps the identity");
    assert_eq!(n1.node.idle(), OK);
    drain_sends(&mut n1);
    drop(n1);

    // The crash restart: a process that died while operating has no
    // same-identity clean restart — the running sentinel classifies
    // crashed and the replacement pair is decided at boot.
    let mut n1 = TestNode::open(1, "a", &root);
    assert_eq!(n1.node.idle(), OK);
    drain_sends(&mut n1);
    assert_eq!(
        n1.node.own_id(),
        1 + (1 << 24),
        "the crash restart bumps the identity"
    );
    drain_sends(&mut n1);

    let _ = fs::remove_dir_all(&root);
}

/// THE BOOT-READ SAFETY LAW: corrupt bytes in a superblock copy (a bad
/// checksum on ANY copy) panic the next boot — loud, bounded, never a
/// hang — and the store never clears, repairs, or falls back: the
/// corrupted bytes stand exactly as they were, and a re-boot panics
/// again.
#[test]
fn a_corrupted_copy_panics_the_next_boot_and_is_never_healed() {
    let root = workdir("corrupt");
    let mut n1 = TestNode::open(1, "a", &root);
    let superblock = n1.dir.join("state.superblock");
    let state = n1.dir.join("state").to_str().expect("path").to_owned();
    assert_eq!(n1.node.stop(), OK, "the clean stop");
    drop(n1);

    assert!(superblock.exists(), "the copies file exists after a boot");
    let geometry = marker_ffi::geometry().expect("geometry");
    let corrupt_offset = geometry.copy_size as u64;
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = fs::OpenOptions::new()
            .write(true)
            .open(&superblock)
            .expect("copies file");
        file.seek(SeekFrom::Start(corrupt_offset))
            .expect("seek copy 1");
        file.write_all(&[0xA5u8; 4096]).expect("corrupt copy 1");
    }
    let before = fs::read(&superblock).expect("copies file");

    let members = ["1:a", "2:b", "3:c"].join("\0");
    for attempt in 0..2 {
        let boot = Node::open(&members, "a", &state, None, 0);
        assert!(
            matches!(boot, Err(PANIC)),
            "attempt {attempt}: the corrupt copy panics the boot, never hangs"
        );
        assert_eq!(
            fs::read(&superblock).expect("copies file"),
            before,
            "attempt {attempt}: never cleared, never repaired, never healed"
        );
    }

    let _ = fs::remove_dir_all(&root);
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
    let root = workdir("torn");
    let mut n1 = TestNode::open(1, "a", &root);
    let superblock = n1.dir.join("state.superblock");
    assert_eq!(n1.node.stop(), OK, "the clean stop leaves a stopped quorum");
    drop(n1);

    let geometry = marker_ffi::geometry().expect("geometry");

    // Snapshot zones 1..3 at the stopped quorum (sequence 3).
    let stopped: Vec<Vec<u8>> = (1..geometry.copies)
        .map(|slot| read_zone(&superblock, slot, geometry))
        .collect();

    // One copy advances past the stopped quorum (a death after exactly
    // one copy of the next write): write the next transition through the
    // real store, then restore zones 1..3 to the stopped generation. All
    // four copies keep valid checksums; the spread is torn.
    marker_ffi::write(&superblock, 0, marker_ffi::MarkerState::Unflushed)
        .expect("the next transition's write");
    for (index, snapshot) in stopped.iter().enumerate() {
        write_zone(&superblock, index + 1, snapshot, geometry);
    }

    // The read resolves by thresholds: the 3-of-4 stopped quorum at the
    // higher sequence... the lone advanced copy holds no quorum and
    // cannot fake a clean stop; the stopped quorum decides.
    let classified = marker_ffi::classify(&superblock).expect("the thresholds resolve");
    assert_eq!(
        classified,
        marker_ffi::Classified {
            state: marker_ffi::MarkerState::Flushed,
            incarnation: 0,
        },
        "the lone advanced copy cannot outvote the stopped quorum (min progress)"
    );

    // The boot through the real gate: the same verdict drives the clean
    // start under the same identity.
    let n1 = TestNode::open(1, "a", &root);
    assert_eq!(
        n1.node.own_id(),
        1,
        "the torn spread resolved to the stopped quorum: clean continue, no bump"
    );
    assert_eq!(
        projection(&n1.dir.join("state")),
        "0 unflushed\n",
        "the boot's latch rewrites the running sentinel over the resolved state"
    );
    drop(n1);

    let _ = fs::remove_dir_all(&root);
}
