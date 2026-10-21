use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
};

use lunet_advisory_lock::disk::std_disk;
use lunet_advisory_lock::locks::{Lease, Request, Service, StateSnapshot};
use lunet_advisory_lock::spi::NoOpHook;
use lunet_advisory_lock::state::{FileStateStore, StateStore, lazy_load};
use lunet_advisory_lock::{Node, PRIMARY_TIMEOUT_MS, SERVICE, census_paths};
use uuid::Uuid;

struct FailingFlushStore {
    inner: FileStateStore,
}

impl StateStore for FailingFlushStore {
    fn flush(&mut self, _snap: &StateSnapshot) -> io::Result<()> {
        Err(io::Error::new(
            io::ErrorKind::StorageFull,
            "the store under test refuses every flush",
        ))
    }

    fn load(&self) -> io::Result<Option<StateSnapshot>> {
        self.inner.load()
    }
}

const HEADER_BYTES: usize = 1 + 4;

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

fn state_path(dir: &Path) -> PathBuf {
    dir.join("node.marker")
}

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
        generations: [(7u64, 70), (9, 91), (11, 110)].into_iter().collect(),
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

fn get_reply(service: &mut Service, lock_id: u64, at: u64) -> Vec<u8> {
    let request = get_request(lock_id);
    let payload = serde_json::to_vec(&request).expect("the request encodes");
    let (message_id, client_id, request_num) = request.ids();
    service
        .execute(message_id, client_id, request_num, at, &payload)
        .expect("the state machine executes")
        .0
}

fn service_holding(snapshot: &StateSnapshot) -> Service {
    let mut service = Service::default();
    assert!(service.is_empty(), "a fresh table is empty");
    service.restore(snapshot.clone());
    service
}

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

#[test]
fn the_defaulted_seam_answers_are_the_no_ops() {
    let dir = scratch("defaults");
    let path = state_path(&dir);
    let mut store = FileStateStore::new_on(std_disk(), &path);
    StateStore::note(&mut store, 1);
    assert_eq!(
        StateStore::tick(&mut store, &table()).expect("the default tick"),
        0,
        "the default submits nothing"
    );
    StateStore::pump(&mut store).expect("the default pump drains");
    assert_eq!(
        StateStore::state_status(&store),
        None,
        "the file store reports no counters, never zeros"
    );
    StateStore::forget(&mut store);
    assert_eq!(
        store.load().expect("the load reads"),
        None,
        "nothing was ever written, and the defaults wrote nothing either"
    );
}

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

#[test]
fn a_stop_that_cannot_flush_is_a_failed_stop() {
    let dir = scratch("flush-refused");
    let marker = state_path(&dir);
    // The store under test wraps the real file store so the refusal is
    // the only difference from the passing schedule above.
    let failing = FailingFlushStore {
        inner: FileStateStore::new(&marker),
    };
    let mut node = Node::open_with_seams(
        &members(),
        "a",
        marker.to_str().expect("the scratch path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
        std_disk(),
        Box::new(failing),
        Arc::new(Mutex::new(NoOpHook)),
        None,
    )
    .expect("a three-member node boots with a mounted store");
    let _ = census_paths();

    assert_eq!(node.stop(), SERVICE, "a refused flush fails the stop");

    // The tape names the failure where the law says it lands: past the
    // drain window, at the flush, and the schedule never resumes — no
    // flush line, no Stopped round, no completion.
    let tape = census_paths();
    assert!(
        tape.contains(&"stop.drain-window.close"),
        "the schedule reached the flush's seat: {tape:?}"
    );
    assert!(
        tape.contains(&"stop.refuse.state-flush"),
        "the refusal is named: {tape:?}"
    );
    assert!(
        !tape.contains(&"state.flush"),
        "a refused flush is never reported as flushed: {tape:?}"
    );
    assert!(
        !tape.contains(&"stop.round.finish"),
        "the Stopped round never runs behind a failed flush: {tape:?}"
    );
    assert!(
        !tape.contains(&"stop.complete"),
        "a failed stop does not complete: {tape:?}"
    );

    // No torn half-state: the staged write never landed, so the state
    // file is simply absent — nothing to distrust later, nothing to
    // half-read.
    assert!(
        FileStateStore::new(&marker)
            .load()
            .expect("the read is answered")
            .is_none(),
        "a failed flush leaves no state file"
    );

    // The next boot on this marker classifies crashed — the markers hold
    // the halt's first round — and the guard reads nothing, exactly as
    // the law's crashed clause states.
    let _reopened = Node::open(
        &members(),
        "a",
        marker.to_str().expect("the scratch path is UTF-8"),
        None,
        0,
        PRIMARY_TIMEOUT_MS,
    )
    .expect("the crashed life reopens as its replacement");
    let tape = census_paths();
    assert!(
        tape.contains(&"boot.crashed"),
        "the markers held at the first round, so the next boot is crashed: {tape:?}"
    );
    assert_eq!(
        lazy_load(&FileStateStore::new(&marker), false, true).expect("the load does not fail"),
        None,
        "the crashed boot reads nothing"
    );
}

#[test]
fn a_one_byte_label_decodes() {
    let request = r#"{"op":"set","message_id":"b0000000-0000-4000-8000-000000000001","client_id":6,"request_num":1,"lock_id":9306,"labels":["x"],"lease":{"lease_id":6,"holder":"33333333-3333-3333-3333-333333333333","lease_ms":30000}}"#;
    let decoded =
        Service::decode(request.as_bytes()).expect("a one-byte label is a valid operator label");
    match decoded {
        Request::Set { labels, .. } => {
            assert_eq!(labels, Some(vec!["x".to_string()]));
        }
        _ => panic!("the set decodes as a set"),
    }
}

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
