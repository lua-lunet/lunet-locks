//! The state seam at the disk boundary: the eager flush's bytes and the
//! lazy load's guard.
//!
//! Three claims, each against the real `FileStateStore` over the real
//! `StdDisk` — no fake of the trait under test, because a fake proves
//! only that the fake agrees with itself:
//!
//! (a) a flushed table roundtrips byte-exactly: the loaded snapshot is
//!     the flushed one, and applied to a fresh `Service` it reproduces
//!     the same table.
//! (b) a corrupted file loads as `Ok(None)`, never as an error — state
//!     is a fallback and a fallback that blocks the boot is an outage.
//! (c) the crashed verdict skips the load entirely: a perfectly readable
//!     file goes unread, and only a clean verdict loads it.
//!
//! Real files in the repo's scratch tree (`.tmp` is scratch).

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::disk::std_disk;
use lunet_advisory_lock::locks::{Lease, Request, Service, StateSnapshot};
use lunet_advisory_lock::state::{FileStateStore, StateStore, lazy_load};
use lunet_advisory_lock::{Node, PRIMARY_TIMEOUT_MS, census_paths};
use uuid::Uuid;

/// The framed file's layout, restated here so the test's damages address
/// the right bytes: version byte | length (u32 BE) | payload | checksum
/// (u32 BE). `FileStateStore::frame`/`unframe` are the layout's single
/// source; `HEADER_BYTES` is where the payload starts.
const HEADER_BYTES: usize = 1 + 4;

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/state-store");
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

/// A node state path inside `dir`. The store's file is this path plus
/// `.state`; the marker projection and the `.view` record sit beside it
/// and are not this seam's business.
fn state_path(dir: &Path) -> PathBuf {
    dir.join("node.marker")
}

/// A stored record: `lock_id` varies the holder so the table's records
/// are distinguishable one from another in the loaded bytes.
fn lease(lock_id: u64, lease_id: u64, expiry: u64) -> Lease {
    Lease {
        lease_id,
        holder: Uuid::from_u128(
            0x0102_0304_0506_0708_090a_0b0c_0d0e_0f00u128 ^ u128::from(lock_id),
        ),
        expiry,
        lease_ms: 5_000,
        name: Some(format!("/lock-{lock_id}")),
        labels: Some(vec!["lane-a".to_string(), "lane-b".to_string()]),
        taken_at_ms: 1_700_000_000_000 + lock_id,
        renew_count: 2,
    }
}

/// A table with three records — one of each shape the lock service
/// stores: a live hold, a renewed hold (counters non-zero), and the
/// expired keeper record (`expiry: 0`, nil holder) a break leaves
/// behind. Every record is non-empty state, so an empty table is
/// distinguishable from a loaded one.
fn table() -> StateSnapshot {
    StateSnapshot {
        locks: [
            (7u64, lease(7, 70, 1_700_000_010_000)),
            (9, lease(9, 91, 1_700_000_020_000)),
            (
                11,
                Lease {
                    expiry: 0,
                    lease_ms: 0,
                    holder: Uuid::nil(),
                    ..lease(11, 110, 0)
                },
            ),
        ]
        .into_iter()
        .collect(),
    }
}

fn get_request(lock_id: u64) -> Request {
    Request::Get {
        message_id: Uuid::from_u128(0xabcd_0000_0000_0000u128 ^ u128::from(lock_id)),
        client_id: 1,
        request_num: 1,
        lock_id,
    }
}

/// One `Service::execute` against a live `Service`, at a fixed execution
/// tick: the state machine's reply bytes for one Get.
fn get_reply(service: &mut Service, lock_id: u64, at: u64) -> Vec<u8> {
    let request = get_request(lock_id);
    let payload = serde_json::to_vec(&request).expect("the request encodes");
    let (message_id, client_id, request_num) = request.ids();
    service
        .execute(message_id, client_id, request_num, at, &payload)
        .expect("the state machine executes")
        .0
}

/// A `Service` holding exactly `snapshot`'s table, through the real
/// decode/execute path and the real restore the lazy-load seam takes.
fn service_holding(snapshot: &StateSnapshot) -> Service {
    let mut service = Service::default();
    assert!(service.is_empty(), "a fresh table is empty");
    service.restore(snapshot.clone());
    service
}

/// (a) The flush/load roundtrip: the bytes survive, the loaded table is
/// the flushed one, and it answers identically.
#[test]
fn flush_then_load_roundtrips_the_table() {
    let dir = scratch("roundtrip");
    let path = state_path(&dir);
    let flushed = table();
    let mut store = FileStateStore::new_on(std_disk(), &path);
    store.flush(&flushed).expect("the flush writes");
    // The store's file is the node state path plus `.state`, beside the
    // marker projection — named by the store, not guessed by the test.
    assert_eq!(
        store.path(),
        Path::new(&path).with_extension("marker.state"),
        "the state file is the state path plus .state"
    );

    let reopened = FileStateStore::new_on(std_disk(), &path);
    let loaded = reopened
        .load()
        .expect("the load reads")
        .expect("the flushed file is loadable");
    assert_eq!(
        loaded, flushed,
        "the loaded snapshot is the flushed one, field for field"
    );

    // Byte-exactness, not just equality of the decoded values: the
    // loaded table re-flushed over the same path is the same bytes.
    let mut again = FileStateStore::new_on(std_disk(), &path);
    again.flush(&loaded).expect("the second flush writes");
    assert_eq!(
        reopened.load().expect("the load reads"),
        Some(flushed.clone()),
        "a re-flush of the loaded snapshot is the same table"
    );

    // And the loaded table is one the state machine answers from
    // identically: the same Get, at the same tick, over both.
    let mut from_loaded = service_holding(&loaded);
    let mut from_flushed = service_holding(&flushed);
    for lock_id in [7u64, 9, 11] {
        assert_eq!(
            get_reply(&mut from_loaded, lock_id, 1_700_000_005_000),
            get_reply(&mut from_flushed, lock_id, 1_700_000_005_000),
            "the loaded table answers lock {lock_id} exactly as the flushed one did"
        );
    }
    // The expiry stamps came back with the records: the two live locks
    // answer with a lease, and the broken lock's keeper record — expiry 0,
    // nil holder — is present in the loaded table but reads as not live,
    // so the roundtrip restored the record without resurrecting a lease.
    let live_reply = String::from_utf8(get_reply(&mut from_loaded, 7, 1_700_000_005_000))
        .expect("a reply is JSON");
    assert!(
        live_reply.contains("\"lease\":{"),
        "a live lock answers with its lease, expiry stamp included"
    );
    assert!(
        live_reply.contains(&lease(7, 70, 1_700_000_010_000).expiry.to_string()),
        "the loaded record's expiry came back with it"
    );
    assert!(
        String::from_utf8_lossy(&get_reply(&mut from_loaded, 11, 1_700_000_005_000))
            .contains("\"lease\":null"),
        "the broken lock's keeper record reads as no live lease"
    );
    assert_eq!(
        from_loaded.snapshot(),
        flushed,
        "the table the loaded service serves is the table that was flushed"
    );
}

/// (b) A rotted file loads as nothing. Every kind of damage the framing
/// can suffer is tried: a flipped payload byte, a flipped checksum, a
/// truncated tail, a length prefix that disagrees with the bytes
/// present, a version byte this build does not know, and an empty file.
#[test]
fn corrupted_file_loads_as_none_not_an_error() {
    let dir = scratch("corrupt");
    let path = state_path(&dir);
    let mut store = FileStateStore::new_on(std_disk(), &path);
    store.flush(&table()).expect("the flush writes");
    let good = fs::read(store.path()).expect("the framed file reads");

    let flipped = |mut bytes: Vec<u8>, at: usize| {
        bytes[at] ^= 0x20;
        bytes
    };
    let cases: Vec<(&str, Vec<u8>)> = vec![
        (
            "a flipped payload byte",
            flipped(good.clone(), HEADER_BYTES),
        ),
        ("a flipped checksum", flipped(good.clone(), good.len() - 1)),
        ("a truncated tail", good[..good.len() - 3].to_vec()),
        (
            "a length prefix that disagrees with the bytes present",
            flipped(good.clone(), 1),
        ),
        (
            "a version byte this build does not know",
            flipped(good.clone(), 0),
        ),
        ("an empty file", Vec::new()),
    ];
    for (what, bytes) in cases {
        fs::write(store.path(), &bytes).expect("the damaged file writes");
        let reopened = FileStateStore::new_on(std_disk(), &path);
        assert_eq!(
            reopened.load().expect("a damaged file is not an I/O error"),
            None,
            "{what} loads as nothing, never as an error"
        );
    }

    // The good bytes still load after all that damage: the refusals above
    // are refusals of damage, not of the file.
    fs::write(store.path(), &good).expect("the good file rewrites");
    assert!(
        FileStateStore::new_on(std_disk(), &path)
            .load()
            .expect("the load reads")
            .is_some(),
        "the undamaged file loads"
    );
}

/// (c) The lazy-load guard. A crashed boot's verdict reads nothing — not
/// even a perfectly good file — and only a clean verdict loads. Driven
/// through `lazy_load`, the seam's own guard, against the real store
/// over a real file: the guard is the thing under test, so the test
/// names the guard.
#[test]
fn a_crashed_verdict_skips_the_load() {
    let dir = scratch("verdict");
    let path = state_path(&dir);
    let flushed = table();
    let mut store = FileStateStore::new_on(std_disk(), &path);
    store.flush(&flushed).expect("the flush writes");
    // A fresh handle, so nothing is cached on the store between cases.
    let reopened = FileStateStore::new_on(std_disk(), &path);

    assert_eq!(
        lazy_load(&reopened, false, true).expect("the load does not fail"),
        None,
        "a crashed boot's verdict distrusts the state file"
    );
    assert_eq!(
        lazy_load(&reopened, true, true).expect("the load does not fail"),
        Some(flushed.clone()),
        "a clean verdict over an empty table loads the file"
    );
    assert_eq!(
        lazy_load(&reopened, true, false).expect("the load does not fail"),
        None,
        "a populated table is never overwritten by a load"
    );
    assert_eq!(
        lazy_load(&reopened, false, false).expect("the load does not fail"),
        None,
        "a crashed verdict over a populated table loads nothing either"
    );

    // A missing file on a clean verdict is the same answer a crashed
    // verdict gives: nothing to load.
    let empty_dir = scratch("verdict-absent");
    let absent = FileStateStore::new_on(std_disk(), &state_path(&empty_dir));
    assert_eq!(
        lazy_load(&absent, true, true).expect("the load does not fail"),
        None,
        "a clean verdict with no file loads nothing"
    );
}

/// The eager flush, driven through the real `Node` stop path: the flush
/// is in the halt schedule at the one seat the law allows it, and the
/// file it leaves behind is loadable.
///
/// The census tape is the stop path's own naming of every path it took
/// (see the `trace_line!` convention), so the schedule this asserts is
/// the schedule the node actually walked — not a reconstruction of it.
#[test]
fn the_stop_flushes_eagerly_inside_the_halt_schedule() {
    let dir = scratch("stop-flush");
    let marker = state_path(&dir);
    let mut node = Node::open(
        &members(),
        "a",
        marker.to_str().expect("the scratch path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
    )
    .expect("a three-member node boots on a fresh marker");
    let _ = census_paths();

    assert_eq!(node.stop(), 0, "a seated node stops clean");
    let tape = census_paths();
    let at = |name: &str| {
        tape.iter()
            .position(|line| *line == name)
            .unwrap_or_else(|| panic!("the tape carries {name}: {tape:?}"))
    };
    assert_eq!(
        (
            at("stop.drain-window.open") < at("state.flush"),
            at("state.flush") < at("stop.round.finish"),
            at("state.flush") < at("stop.complete"),
        ),
        (true, true, true),
        "the flush sits in the drain window: after it opens, before the \
         drain-proven Stopped round and before the stop completes — {tape:?}"
    );

    // The clean stop's file is loadable, and it is this node's own state
    // path's file — the store's default, built where the disk's default
    // is built.
    let loaded = FileStateStore::new(&marker)
        .load()
        .expect("the load reads")
        .expect("a clean stop's flush is loadable");
    assert!(
        loaded.locks.is_empty(),
        "no lock was taken on this path, so the flushed table is empty"
    );
}

/// The crashed verdict, driven through the real `Node` open path: a node
/// that dies without stopping re-classifies as crashed, and that verdict
/// is the one the lazy-load guard refuses. The file from the dead life's
/// stop is still sitting there — the guard is what leaves it unread.
#[test]
fn a_crashed_boot_is_the_verdict_the_load_guard_refuses() {
    let dir = scratch("crashed-verdict");
    let marker = state_path(&dir);
    let boot = || {
        Node::open(
            &members(),
            "a",
            marker.to_str().expect("the scratch path is UTF-8"),
            None,
            0,
            PRIMARY_TIMEOUT_MS,
        )
    };

    // First life: a fresh marker, the genesis provision.
    let node = boot().expect("the first life boots");
    // The life dies without a stop: the node is dropped, not stopped.
    drop(node);
    let _ = census_paths();

    // Second life: no stopped quorum was ever written, so the boot gate
    // classifies CRASHED.
    let _reopened = boot().expect("the crashed life reopens as its replacement");
    let tape = census_paths();
    assert!(
        tape.contains(&"boot.crashed"),
        "the second life is classified crashed: {tape:?}"
    );
    assert!(
        !tape.contains(&"boot.clean"),
        "a life that never stopped is not clean: {tape:?}"
    );
    // And the guard is what that verdict feeds. A crashed life's own
    // file is left behind by an earlier clean stop, and it is perfectly
    // readable — the crashed verdict is the ONLY thing keeping it
    // unread, which is exactly the law's claim. So: flush a table into
    // this marker path's file, then ask the guard the crashed question.
    let mut writer = FileStateStore::new_on(std_disk(), &marker);
    writer.flush(&table()).expect("the flush writes");
    let store = FileStateStore::new(&marker);
    assert!(
        store.load().expect("the load reads").is_some(),
        "the file the crashed verdict refuses is a readable file"
    );
    assert_eq!(
        lazy_load(&store, false, true).expect("the load does not fail"),
        None,
        "the crashed verdict reads nothing even when a readable file is there"
    );
}

/// A three-member descriptor: distinct provisioned identities, the
/// genesis succession order, NUL-separated, no trailing NUL.
fn members() -> String {
    [1u32, 2, 3]
        .iter()
        .map(|system| {
            format!(
                "{}:{}\0",
                (*system << 16) | 1,
                (b'a' + (*system - 1) as u8) as char
            )
        })
        .collect::<String>()
        .trim_end_matches('\0')
        .to_string()
}
