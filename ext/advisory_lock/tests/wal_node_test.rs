//! The WAL store at the node's construction, driven through the real C
//! ABI: the mounted store's counters through `lunet_lock_node_state_tick`
//! and `lunet_lock_node_state_status`, the stop path's
//! submit-before-flush order on the census tape, and the
//! construction-refusal fallback that boots without the artefact.
//!
//! The rig is three in-process nodes wired by hand: the sends one node's
//! output queue releases are delivered to the addressed peer's `receive`,
//! round by round, until the leader commits a client lock. The same C
//! ABI the Lunet runtime drives, no smaller a fake.

use std::ffi::c_void;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::spi::StateStore;
use lunet_advisory_lock::wal_snapshot::WalSnapshotStore;
use lunet_advisory_lock::{OUTPUT_REPLY, OUTPUT_SEND, StateStatusRaw, census_paths};

unsafe extern "C" {
    fn lunet_lock_node_new(
        members_len: usize,
        members_data: *const u8,
        own_len: usize,
        own_data: *const u8,
        state_len: usize,
        state_data: *const u8,
        journal_dir_len: usize,
        journal_dir_data: *const u8,
        roll_bytes: u32,
        out: *mut *mut c_void,
    ) -> i32;
    fn lunet_lock_node_free(node: *mut c_void);
    fn lunet_lock_node_own_id(node: *mut c_void, out_id: *mut u32) -> i32;
    fn lunet_lock_node_request(node: *mut c_void, json_len: usize, json: *const u8) -> i32;
    fn lunet_lock_node_receive(node: *mut c_void, from: u32, len: usize, data: *const u8) -> i32;
    fn lunet_lock_node_idle(node: *mut c_void) -> i32;
    fn lunet_lock_node_status(
        node: *mut c_void,
        out_status: *mut u32,
        out_leader: *mut u32,
        out_era: *mut u32,
        out_view: *mut u32,
    ) -> i32;
    fn lunet_lock_node_state_tick(node: *mut c_void) -> i32;
    fn lunet_lock_node_state_status(node: *mut c_void, out_status: *mut StateStatusRaw) -> i32;
    fn lunet_lock_node_stop(node: *mut c_void) -> i32;
    fn lunet_lock_node_next(
        node: *mut c_void,
        out_kind: *mut u32,
        out_to: *mut u32,
        out_era: *mut u32,
        out_view: *mut u32,
        out_slot_hi: *mut u32,
        out_slot_lo: *mut u32,
        out_message_id: *mut u8,
        capacity: usize,
        out_len: *mut usize,
        out_data: *mut u8,
    ) -> i32;
}

const DATAGRAM: usize = 65507;

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/wal-node");
    fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&dir).expect("the case directory creates");
    dir
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

struct AbiNode {
    handle: *mut c_void,
    id: u32,
}

impl AbiNode {
    /// A node through the C ABI's own construction — the WAL store's
    /// mount seat. The handle is freed on drop.
    fn new(root: &Path, name: &str) -> AbiNode {
        unsafe {
            let state = root.join(format!("{name}.nonce"));
            let state_text = state.to_str().expect("the scratch path is UTF-8");
            let members = members();
            let mut out: *mut c_void = std::ptr::null_mut();
            let rc = lunet_lock_node_new(
                members.len(),
                members.as_ptr(),
                name.len(),
                name.as_ptr(),
                state_text.len(),
                state_text.as_ptr(),
                0,
                b"".as_ptr(),
                0,
                &mut out,
            );
            assert_eq!(rc, 0, "the node constructs ({rc})");
            let mut id = 0u32;
            assert_eq!(lunet_lock_node_own_id(out, &mut id), 0);
            AbiNode { handle: out, id }
        }
    }

    fn idle(&self) -> i32 {
        unsafe { lunet_lock_node_idle(self.handle) }
    }

    fn receive(&self, from: u32, bytes: &[u8]) -> i32 {
        unsafe { lunet_lock_node_receive(self.handle, from, bytes.len(), bytes.as_ptr()) }
    }

    fn request(&self, json: &str) -> i32 {
        unsafe { lunet_lock_node_request(self.handle, json.len(), json.as_ptr()) }
    }

    fn state_tick(&self) -> i32 {
        unsafe { lunet_lock_node_state_tick(self.handle) }
    }

    fn state_status(&self) -> StateStatusRaw {
        unsafe {
            let mut status = StateStatusRaw::default();
            assert_eq!(lunet_lock_node_state_status(self.handle, &mut status), 0);
            status
        }
    }

    fn stop(&self) -> i32 {
        unsafe { lunet_lock_node_stop(self.handle) }
    }

    fn leader(&self) -> u32 {
        unsafe {
            let mut status = 0u32;
            let mut leader = 0u32;
            let mut era = 0u32;
            let mut view = 0u32;
            assert_eq!(
                lunet_lock_node_status(self.handle, &mut status, &mut leader, &mut era, &mut view),
                0
            );
            leader
        }
    }

    /// The output queue, drained: `(kind, to, bytes)` per output.
    fn outputs(&self, buf: &mut [u8]) -> Vec<(u32, u32, Vec<u8>)> {
        unsafe {
            let mut outputs = Vec::new();
            loop {
                let mut kind = 0u32;
                let mut to = 0u32;
                let mut era = 0u32;
                let mut view = 0u32;
                let mut slot_hi = 0u32;
                let mut slot_lo = 0u32;
                let mut message_id = [0u8; 16];
                let mut len = 0usize;
                let rc = lunet_lock_node_next(
                    self.handle,
                    &mut kind,
                    &mut to,
                    &mut era,
                    &mut view,
                    &mut slot_hi,
                    &mut slot_lo,
                    message_id.as_mut_ptr(),
                    buf.len(),
                    &mut len,
                    buf.as_mut_ptr(),
                );
                assert!(rc >= 0, "the output queue drives clean here ({rc})");
                if rc == 0 {
                    return outputs;
                }
                outputs.push((kind, to, buf[..len].to_vec()));
            }
        }
    }
}

impl Drop for AbiNode {
    fn drop(&mut self) {
        unsafe { lunet_lock_node_free(self.handle) };
    }
}

/// One delivery round: idle every node (the heartbeat's tick), drain
/// every queue, hand each send to its addressee's `receive` attributed
/// to the sender's live id, and hand every reply back to the caller.
fn round(rig: &mut [AbiNode], buf: &mut [u8]) -> Vec<Vec<u8>> {
    let mut replies = Vec::new();
    let mut deliveries: Vec<(u32, u32, Vec<u8>)> = Vec::new();
    for node in rig.iter_mut() {
        node.idle();
    }
    for node in rig.iter() {
        let outputs = node.outputs(buf);
        for (kind, to, bytes) in outputs {
            if kind == OUTPUT_REPLY {
                replies.push(bytes);
                continue;
            }
            assert_eq!(kind, OUTPUT_SEND, "the queue carries send and reply only");
            deliveries.push((node.id, to, bytes));
        }
    }
    for (from, to, bytes) in deliveries {
        let target = rig
            .iter_mut()
            .find(|node| node.id == to)
            .expect("every send names a known peer id");
        target.receive(from, &bytes);
    }
    replies
}

/// Wire the rig until some node is leader, then return the leader's
/// index. The genesis primary is seated from provision; the rounds ride
/// the ordinary idle ticks.
fn elect(rig: &mut [AbiNode], buf: &mut [u8]) -> usize {
    for _ in 0..64 {
        round(rig, buf);
        for (index, node) in rig.iter().enumerate() {
            if node.leader() == node.id {
                return index;
            }
        }
    }
    panic!("no leader emerged in 64 rounds");
}

/// Drive one client lock through the rig until the leader's reply
/// arrives, delivering every protocol output along the way. The reply
/// bytes are the caller's to assert on.
fn take_lock(rig: &mut [AbiNode], leader: usize, json: &str, buf: &mut [u8]) -> Vec<u8> {
    assert_eq!(rig[leader].request(json), 0, "the leader proposes the lock");
    for _ in 0..64 {
        let replies = round(rig, buf);
        if let Some(reply) = replies.into_iter().find(|bytes| contains(bytes, GRANTED)) {
            return reply;
        }
    }
    panic!("the lock never committed in 64 rounds");
}

/// The reply marker the rig's matcher waits for: `"granted":true`.
const GRANTED: &[u8] = b"\"granted\":true";

/// The byte-level membership test the reply matcher runs: a window of
/// the needle's length compared for equality — no string allocation on
/// the rig's hot path.
fn contains(bytes: &[u8], needle: &[u8]) -> bool {
    bytes.windows(needle.len()).any(|window| window == needle)
}

/// A set request the state machine grants: an unnamed lock (the
/// ephemeral class) with a canonical message id.
fn set_json(message_id: &str, lock_id: u64, lease_id: u64, holder: &str) -> String {
    format!(
        "{{\"op\":\"set\",\"message_id\":\"{message_id}\",\"client_id\":1,\"request_num\":1,\
\"lock_id\":{lock_id},\"lease\":{{\"lease_id\":{lease_id},\"holder\":\"{holder}\",\
\"lease_ms\":30000}}}}"
    )
}

/// The mounted WAL store, end to end: the node constructs with the store
/// beside its state path, a committed lock rides `state_tick` into the
/// ephemeral stream (the per-stream counters move through the FFI), the
/// store's artefact sits pre-laid-out at the `.wal` path, and the stop
/// walks its submit-before-flush seat — `state.tick` strictly inside the
/// drain window, before the eager flush — and completes clean.
#[test]
fn the_wal_store_mounts_ticks_and_stops_through_the_ffi() {
    let root = scratch("mount-tick-stop");
    let mut buf = vec![0u8; DATAGRAM];
    let mut rig = vec![
        AbiNode::new(&root, "a"),
        AbiNode::new(&root, "b"),
        AbiNode::new(&root, "c"),
    ];
    let _ = census_paths();

    // The mount: a fresh boot's counters are zero and the store
    // reports itself present.
    let status = rig[0].state_status();
    assert_eq!(status.present, 1, "the WAL store is mounted");
    assert_eq!(status.eph_completed_generations, 0);
    assert_eq!(status.eph_completed_records, 0);
    assert_eq!(status.eph_refusals, 0);
    assert_eq!(status.dur_completed_generations, 0);
    assert_eq!(status.dur_completed_records, 0);

    // The artefact sits pre-laid-out: 8 MiB, the design doc's layout.
    let wal = root.join("a.nonce.wal");
    assert_eq!(
        fs::metadata(&wal).expect("the artefact exists").len(),
        8 * 1024 * 1024,
        "the WAL artefact is pre-laid-out at its fixed size"
    );

    // A tick with nothing changed is a no-op success.
    assert_eq!(rig[0].state_tick(), 0);

    // The leader takes a lock; the commit notes it; the tick submits
    // it; the ephemeral counters move through the FFI.
    let leader = elect(&mut rig, &mut buf);
    let holder = "11111111-1111-1111-1111-111111111111";
    let reply = take_lock(
        &mut rig,
        leader,
        &set_json("00000000-0000-4000-8000-000000000001", 9001, 1, holder),
        &mut buf,
    );
    assert!(contains(&reply, GRANTED), "the lock grants");
    // The steady loop's cadence, compressed. On the linux ring the
    // tick SUBMITS the generation and the completion lands on a LATER
    // pump — "the submit cadence is the drain point", the IO ruling's
    // own words — while the darwin backend completes synchronously
    // inside the one call. The test drives the loop the way
    // production does: a beat, real time, a beat — bounded at four
    // seconds against an observed ~200 ms landing.
    let mut status = rig[leader].state_status();
    for _ in 0..40 {
        if status.eph_completed_records == 1 {
            break;
        }
        std::thread::sleep(std::time::Duration::from_millis(100));
        rig[leader].state_tick();
        status = rig[leader].state_status();
    }
    assert_eq!(
        status.eph_completed_records, 1,
        "the committed lock's live record landed"
    );
    assert!(status.eph_completed_generations >= 1);
    assert!(
        status.last_completed_unix_ns > 0,
        "the completion is stamped"
    );
    assert_eq!(status.dur_completed_records, 0, "the lock is unnamed");

    // The stop: the tick-before-flush seat, on the census tape, and a
    // clean completion — the flush reconciled against the caught-up
    // mirror.
    let _ = census_paths();
    assert_eq!(rig[leader].stop(), 0, "the stop is clean");
    let tape = census_paths();
    let at = |name: &str| {
        tape.iter()
            .position(|line| *line == name)
            .unwrap_or_else(|| panic!("the tape carries {name}: {tape:?}"))
    };
    assert!(
        at("stop.drain-window.open") < at("state.tick") && at("state.tick") < at("state.flush"),
        "the tick sits between the drain window's open and the eager flush: {tape:?}"
    );
}

/// The stop-before-demand case: a clean boot that never served a client
/// op never took the lazy load — and its stop still owes the eager
/// flush the table the artefact holds. The stop takes the load itself
/// (the same guard: clean verdict, empty table, once), notes the loaded
/// locks, ticks, and the flush reconciles — and the artefact is
/// rewritten with the same table, not destroyed by an empty flush.
#[test]
fn the_stop_takes_the_load_it_owes_the_flush() {
    let root = scratch("stop-before-demand");
    let wal = root.join("a.nonce.wal");
    let mut buf = vec![0u8; DATAGRAM];
    let mut rig = vec![
        AbiNode::new(&root, "a"),
        AbiNode::new(&root, "b"),
        AbiNode::new(&root, "c"),
    ];
    let _ = census_paths();

    // First life: one lock, ticked.
    let leader = elect(&mut rig, &mut buf);
    let holder = "33333333-3333-3333-3333-333333333333";
    let reply = take_lock(
        &mut rig,
        leader,
        &set_json("00000000-0000-4000-8000-000000000003", 9003, 3, holder),
        &mut buf,
    );
    assert!(contains(&reply, GRANTED), "the lock grants");
    assert_eq!(rig[leader].state_tick(), 0);
    // The trio stops cleanly. The rig drops first: the engine holds
    // its artefacts exclusively (a busy file refuses open), exactly
    // like a dead process's replacement booting after it.
    let _ = census_paths();
    for node in rig.iter() {
        assert_eq!(node.stop(), 0, "the first life's stop is clean");
    }
    drop(rig);
    let held = {
        let store = WalSnapshotStore::new(&wal).expect("the artefact reopens");
        store.load().expect("the load reads")
    };
    assert!(held.is_some(), "the clean stop's artefact carries the lock");

    // Second life: the clean resume, no demand, straight to stop. The
    // stop takes the load it owes the flush; the flush reconciles
    // against the caught-up mirror; the artefact survives the round
    // trip with the same table.
    let rig = vec![
        AbiNode::new(&root, "a"),
        AbiNode::new(&root, "b"),
        AbiNode::new(&root, "c"),
    ];
    let _ = census_paths();
    for node in rig.iter() {
        assert_eq!(node.stop(), 0, "the stop before any demand is clean");
    }
    drop(rig);
    let reheld = {
        let store = WalSnapshotStore::new(&wal).expect("the artefact reopens");
        store.load().expect("the load reads")
    };
    assert_eq!(
        reheld, held,
        "the stop-before-demand rewrote the artefact with the same table"
    );
}

/// The construction-refusal fallback (R4): a corrupt artefact refuses
/// the store's open, the node boots anyway — the WAL is a fallback,
/// never a safety mechanism — and the fallback store reports no
/// counters. The node remains correct: it elects, serves, and stops
/// clean.
#[test]
fn a_refused_wal_mount_boots_without_the_artefact() {
    let root = scratch("mount-refused");
    // A wrong-size artefact is the simplest open refusal: the file is
    // fixed-size, pre-laid-out, and 10 bytes is not 8 MiB.
    fs::write(root.join("a.nonce.wal"), [0xFFu8; 10]).expect("the corrupt artefact writes");

    let mut buf = vec![0u8; DATAGRAM];
    let mut rig = vec![
        AbiNode::new(&root, "a"),
        AbiNode::new(&root, "b"),
        AbiNode::new(&root, "c"),
    ];
    let _ = census_paths();

    // The boot did not block: the node constructs, and its store is the
    // fallback (no counters to report).
    let status = rig[0].state_status();
    assert_eq!(status.present, 0, "the fallback store reports no counters");
    assert_eq!(rig[0].state_tick(), 0, "the tick is a no-op");

    // The node remains correct: it elects, serves, and stops clean.
    let leader = elect(&mut rig, &mut buf);
    let holder = "22222222-2222-2222-2222-222222222222";
    let reply = take_lock(
        &mut rig,
        leader,
        &set_json("00000000-0000-4000-8000-000000000002", 9002, 2, holder),
        &mut buf,
    );
    assert!(contains(&reply, GRANTED), "the fallback node serves locks");
    let _ = census_paths();
    for node in rig.iter() {
        assert_eq!(node.stop(), 0, "the stop is clean");
    }
}
