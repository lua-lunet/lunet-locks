//! The WAL snapshot store at the real boundary: the Rust `StateStore`
//! over the real Zig engine's C ABI — no fake of either side, because a
//! fake proves only that the fake agrees with itself.
//!
//! The claims:
//!
//! (a) a flushed table roundtrips: the loaded snapshot reproduces the
//!     flushed table's durable fields, and the record codec behind the
//!     ABI (the engine's own) reproduces the engine's on-disk bytes;
//! (b) a release's tombstone drops the lock on load;
//! (c) the read path's expiry skip (the ~3 s drift threshold) drops a
//!     lease expired far past it and keeps a live one;
//! (d) a capacity overrun refuses the flush — the failed stop — and
//!     counts;
//! (e) the counters move when the flush completes;
//! (f) corrupt content (a zeroed data record under a live header, a twin
//!     disagreement) refuses at open — the frozen ABI refuses content
//!     before `wal_load`, so the wrapper surfaces it as a construction
//!     `Err` whose mount-site fallback is the cold start.
//!
//! Real files in the repo's scratch tree (`.tmp` is scratch).

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::locks::{Lease, StateSnapshot};
use lunet_advisory_lock::state::StateStore;
use lunet_advisory_lock::wal_snapshot::{
    RECORD_SIZE, Record, WalRecord, WalSnapshotStore, record_as_bytes, record_decode, record_encode,
};
use uuid::Uuid;

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/wal-snapshot");
    fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{name}-{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&dir).expect("the case directory creates");
    dir
}

fn wal_path(dir: &Path) -> PathBuf {
    // The state path's name with a `.wal` suffix — the marker path's own
    // naming discipline, restated here for the test's file.
    dir.join("node.marker.wal")
}

/// A stored record with the display fields set: the WAL record carries
/// none of them, and the test leans on that (the loaded view is the
/// record format's content and nothing more).
fn lease(lock_id: u64, lease_id: u64, expiry_ms: u64) -> Lease {
    Lease {
        lease_id,
        holder: Uuid::from_u128(
            0x0102_0304_0506_0708_090a_0b0c_0d0e_0f00u128 ^ u128::from(lock_id),
        ),
        expiry: expiry_ms,
        lease_ms: 5_000,
        name: Some(format!("/lock-{lock_id}")),
        labels: Some(vec!["lane-a".to_string(), "lane-b".to_string()]),
        taken_at_ms: 1_700_000_000_000 + lock_id,
        renew_count: 2,
    }
}

fn snapshot(locks: impl IntoIterator<Item = (u64, Lease)>) -> StateSnapshot {
    StateSnapshot {
        locks: locks.into_iter().collect::<BTreeMap<_, _>>(),
    }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is past the epoch")
        .as_millis() as u64
}

/// The record's byte layout is exact: the struct's byte image (the one
/// auditable as-bytes helper) places every field at its binding offset —
/// the same offsets the engine's comptime asserts pin, 64 bytes, one
/// cache line. This is the Rust half of the layout pin; a drift here is
/// a test failure, a drift in the engine a compile error.
#[test]
fn record_layout_is_exact() {
    let record = WalRecord {
        stamp: 0x0102030405060708,
        expiry: 0x1112131415161718,
        lock_id: 0x2122232425262728,
        lease_id: 0x3132333435363738,
        holder: *b"holder-uuid-0001",
        lease_ms: 0x4142434445464748,
        tag: 0x5152,
        checksum: 0x6162,
    };
    let bytes = record_as_bytes(&record);
    assert_eq!(record.stamp.to_le_bytes(), bytes[0..8]);
    assert_eq!(record.expiry.to_le_bytes(), bytes[8..16]);
    assert_eq!(record.lock_id.to_le_bytes(), bytes[16..24]);
    assert_eq!(record.lease_id.to_le_bytes(), bytes[24..32]);
    assert_eq!(record.holder, bytes[32..48]);
    assert_eq!(record.lease_ms.to_le_bytes(), bytes[48..56]);
    assert_eq!(record.tag.to_le_bytes(), bytes[56..60]);
    assert_eq!(record.checksum.to_le_bytes(), bytes[60..64]);
}

/// The codec: encode then decode reproduces the record, and the record
/// bytes refuse a rotted checksum, a foreign format version, or an
/// unknown kind. The codec is the engine's own, behind the ABI — the
/// wrapper computes no checksum of its own (there is exactly one, in the
/// engine over the vendored primitive; the vendored vectors are asserted
/// in the engine's suite, and the byte-exactness below is the
/// cross-check).
#[test]
fn record_codec_roundtrip_and_refusals() {
    let record = Record::live(123, 456, 7, 70, *Uuid::nil().as_bytes(), 5_000);
    let bytes = record_encode(&record).expect("the record encodes");
    assert_eq!(record_decode(&bytes), Some(record));

    // A rotted checksum is a refusal.
    let mut rotted = bytes;
    rotted[40] ^= 0xFF;
    assert_eq!(record_decode(&rotted), None);

    // A foreign format version is a refusal (the tag rides offset 56; its
    // second byte, offset 57, is the format version).
    let mut foreign = bytes;
    foreign[57] = 9;
    assert_eq!(record_decode(&foreign), None);

    // An unknown kind is a refusal (the tag's first byte, offset 56).
    let mut unknown = bytes;
    unknown[56] = 7;
    assert_eq!(record_decode(&unknown), None);

    // A zero lock id refuses both ways: no encode, and no decode of one
    // forged under a valid checksum.
    let zero = Record::live(123, 456, 0, 70, *Uuid::nil().as_bytes(), 5_000);
    assert_eq!(record_encode(&zero), None);

    // A tombstone roundtrips too.
    let tombstone = Record::tombstone(9, 7, 70);
    assert_eq!(
        record_decode(&record_encode(&tombstone).expect("the tombstone encodes")),
        Some(tombstone)
    );
}

/// (a) The round trip: the tick carries the table's changes, the
/// shutdown flush lands the whole live set, and the loaded snapshot
/// reproduces it — while the wrapper's codec is byte-identical to the
/// engine's own on-disk bytes (decode validates the engine's checksum,
/// re-encode must reproduce it bit for bit).
#[test]
fn flush_load_roundtrip_byte_exact() {
    let dir = scratch("roundtrip");
    let path = wal_path(&dir);

    let table = snapshot([
        (7u64, lease(7, 70, now_millis() + 60_000)),
        (9, lease(9, 91, now_millis() + 60_000)),
        (11, lease(11, 110, now_millis() + 60_000)),
    ]);

    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        assert_eq!(
            store.load().expect("the cold load succeeds"),
            None,
            "a fresh artefact is the cold start"
        );
        // The steady tick: every lock the table holds, submitted.
        for lock_id in table.locks.keys() {
            store.note(*lock_id);
        }
        assert_eq!(store.submit(&table).expect("the submit lands"), 3);
        store.pump().expect("the pump drains");
        store.flush(&table).expect("the flush lands");
    }

    let store = WalSnapshotStore::new(&path).expect("the store reopens");
    let loaded = store
        .load()
        .expect("the load succeeds")
        .expect("the set loads");
    assert_eq!(loaded.locks.len(), 3);
    for (lock_id, original) in &table.locks {
        let loaded = loaded.locks.get(lock_id).expect("the lock loads");
        assert_eq!(loaded.lease_id, original.lease_id);
        assert_eq!(loaded.holder, original.holder);
        assert_eq!(loaded.expiry, original.expiry);
        assert_eq!(loaded.lease_ms, original.lease_ms);
        // The record format carries the lease's identity only.
        assert_eq!(loaded.name, None);
        assert_eq!(loaded.labels, None);
        assert_eq!(loaded.taken_at_ms, 0);
        assert_eq!(loaded.renew_count, 0);
    }
    drop(store);

    // Byte-exact against the engine's own bytes: the fresh file's active
    // pair is A (cold start, tie); the submit generation rides slots
    // 0..2 and the shutdown flush generation re-stamps and rides slots
    // 3..5. Every slot's bytes decode through the engine's codec behind
    // the ABI (the checksum gate), and re-encoding the decoded record
    // must reproduce the engine's bytes bit for bit — the wrapper's
    // fields went to the engine's codec and the on-disk struct's own
    // bytes came back, the full Rust-fields → engine encode → on-disk
    // bytes → engine decode round trip.
    let file = fs::read(&path).expect("the artefact reads");
    assert_eq!(file.len(), 4 * 1024 * 1024, "the artefact is fixed-size");
    let mut stamps = BTreeMap::new();
    for (slot, original) in table.locks.iter().enumerate() {
        let bytes: &[u8; RECORD_SIZE] = file[(3 + slot) * RECORD_SIZE..][..RECORD_SIZE]
            .try_into()
            .expect("a whole record reads");
        let decoded = record_decode(bytes).expect("the engine's record decodes");
        assert_eq!(
            record_encode(&decoded).expect("the record re-encodes"),
            *bytes,
            "the codec is byte-exact"
        );
        assert_eq!(decoded.lock_id, *original.0);
        assert_eq!(decoded.lease_id, original.1.lease_id);
        assert_eq!(decoded.holder, *original.1.holder.as_bytes());
        assert_eq!(decoded.lease_ms, original.1.lease_ms);
        assert_eq!(decoded.expiry, original.1.expiry * 1_000_000);
        stamps.insert(slot, decoded.stamp);
    }
    // The submit generation's slots decode to the same locks with the
    // same byte-exactness (the tick path's records, before the re-stamp).
    for (slot, original) in table.locks.iter().enumerate() {
        let bytes: &[u8; RECORD_SIZE] = file[slot * RECORD_SIZE..][..RECORD_SIZE]
            .try_into()
            .expect("a whole record reads");
        let decoded = record_decode(bytes).expect("the engine's record decodes");
        assert_eq!(
            record_encode(&decoded).expect("the record re-encodes"),
            *bytes,
            "the codec is byte-exact on the submit generation too"
        );
        assert_eq!(decoded.lock_id, *original.0);
        assert_eq!(decoded.lease_id, original.1.lease_id);
    }
    // The flush generation's re-stamp: one wall write time, shared.
    assert!(stamps.values().all(|stamp| *stamp > 0));
    assert!(
        stamps.values().all(|stamp| *stamp == stamps[&0]),
        "one generation, one stamp"
    );
}

/// (b) The release path: the lock's live record lands, the release's
/// tombstone lands after it, and the reopen finds nothing — the
/// tombstone drops the lock, the empty live set is the cold start.
#[test]
fn release_tombstone_drops_the_lock() {
    let dir = scratch("tombstone");
    let path = wal_path(&dir);

    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        store.note(1);
        assert_eq!(
            store
                .submit(&snapshot([(1u64, lease(1, 10, now_millis() + 60_000))]))
                .expect("the submit lands"),
            1,
        );
        store.pump().expect("the pump drains");
        // The release: the lock leaves the table, the tick submits again.
        store.note(1);
        store
            .submit(&snapshot(Vec::<(u64, Lease)>::new()))
            .expect("the tombstone lands");
        store.pump().expect("the pump drains");

        let status = store.status().expect("the status reads");
        assert_eq!(status.completed_records, 2, "one live, one tombstone");
        assert_eq!(status.completed_generations, 2);
    }

    let store = WalSnapshotStore::new(&path).expect("the store reopens");
    assert_eq!(
        store.load().expect("the load succeeds"),
        None,
        "the tombstone dropped the lock"
    );
}

/// (c) The read path's expiry skip: a lease expired far past the ~3 s
/// drift threshold drops on load; a live lease survives.
#[test]
fn expiry_drift_skip_honored() {
    let dir = scratch("expiry-drift");
    let path = wal_path(&dir);
    let now = now_millis();

    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        store.note(1);
        store.note(2);
        store
            .submit(&snapshot([
                (1u64, lease(1, 10, now - 60_000)),
                (2, lease(2, 20, now + 60_000)),
            ]))
            .expect("the submit lands");
        store.pump().expect("the pump drains");
    }

    let store = WalSnapshotStore::new(&path).expect("the store reopens");
    let loaded = store
        .load()
        .expect("the load succeeds")
        .expect("the set loads");
    assert_eq!(
        loaded.locks.get(&1),
        None,
        "the lease expired far past the drift threshold"
    );
    let kept = loaded.locks.get(&2).expect("the live lease survives");
    assert_eq!(kept.lease_id, 20);
}

/// (d) The capacity refusal: the live set overrunning the pair's
/// remaining room refuses the flush — the failed stop — and the counter
/// says so.
#[test]
fn capacity_refusal_fails_stop() {
    let dir = scratch("capacity");
    let path = wal_path(&dir);
    let table = snapshot([
        (1u64, lease(1, 10, now_millis() + 60_000)),
        (2, lease(2, 20, now_millis() + 60_000)),
    ]);

    let mut store = WalSnapshotStore::new_with_capacity(&path, 2).expect("the store opens");
    store.note(1);
    store.note(2);
    assert_eq!(store.submit(&table).expect("the submit lands"), 2);
    store.pump().expect("the pump drains");

    let error = store.flush(&table).expect_err("the flush refuses");
    assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
    let status = store.status().expect("the status reads");
    assert_eq!(status.refusals, 1, "the refusal is counted");
    assert_eq!(status.completed_generations, 1, "the submit, not the flush");
    assert_eq!(status.completed_records, 2);

    // The tick path's refusal discipline: the changed set survives a
    // refused submit so the tick retries, and every retry is counted.
    store.note(3);
    assert!(store.submit(&table).is_err(), "the ring is still full");
    assert!(store.submit(&table).is_err(), "the set was not dropped");
    assert_eq!(store.status().expect("the status reads").refusals, 3);
}

/// (e) The counters move when the shutdown generation completes.
#[test]
fn status_counters_move_after_flush() {
    let dir = scratch("counters");
    let path = wal_path(&dir);
    let table = snapshot([
        (1u64, lease(1, 10, now_millis() + 60_000)),
        (2, lease(2, 20, now_millis() + 60_000)),
    ]);

    let mut store = WalSnapshotStore::new(&path).expect("the store opens");
    let before = store.status().expect("the status reads");
    assert_eq!(before.completed_generations, 0);
    assert_eq!(before.completed_records, 0);
    assert_eq!(before.in_flight, 0);
    assert_eq!(before.refusals, 0);

    // The steady tick first (the mirror tracks the table through the
    // tick path), then the shutdown generation.
    for lock_id in table.locks.keys() {
        store.note(*lock_id);
    }
    store.submit(&table).expect("the submit lands");
    store.pump().expect("the pump drains");
    store.flush(&table).expect("the flush lands");
    let after = store.status().expect("the status reads");
    assert_eq!(after.completed_generations, 2, "the tick and the flush");
    assert_eq!(after.completed_records, 4);
    assert!(after.last_completed_unix_ns > 0);
    assert_eq!(after.in_flight, 0);
    assert_eq!(after.refusals, 0);

    // The eventfd passthrough: the ring's descriptor on Linux, nothing
    // where the platform completes inline.
    if cfg!(target_os = "linux") {
        assert!(store.event_fd().expect("the ring has an eventfd") >= 0);
    } else {
        assert_eq!(store.event_fd(), None);
    }
}

/// The changed set carries only what changed: a noted lock submits one
/// record, a re-noted lock submits one more, and an unchanged set
/// submits nothing.
#[test]
fn submit_carries_only_the_changed_locks() {
    let dir = scratch("changed-set");
    let path = wal_path(&dir);
    let table = snapshot([
        (1u64, lease(1, 10, now_millis() + 60_000)),
        (2, lease(2, 20, now_millis() + 60_000)),
    ]);

    let mut store = WalSnapshotStore::new(&path).expect("the store opens");
    assert_eq!(store.submit(&table).expect("nothing changed"), 0);
    store.note(1);
    store.note(2);
    assert_eq!(store.submit(&table).expect("the submit lands"), 2);
    store.pump().expect("the pump drains");
    store.note(1);
    assert_eq!(store.submit(&table).expect("the renew lands"), 1);
    store.pump().expect("the pump drains");
    assert_eq!(store.submit(&table).expect("the set is clear"), 0);
    assert_eq!(
        store.status().expect("the status reads").completed_records,
        3
    );
}

/// (f) Corrupt content refuses at open — the frozen ABI's content
/// refusals (a zeroed data record under a live header, a twin
/// disagreement) never reach `wal_load`; the wrapper surfaces them as a
/// construction `Err`, and the mount site's fallback is the cold start.
#[test]
fn content_refusals_refuse_open() {
    let good = scratch("content-good");
    let good_path = wal_path(&good);
    let table = snapshot([
        (1u64, lease(1, 10, now_millis() + 60_000)),
        (2, lease(2, 20, now_millis() + 60_000)),
    ]);
    {
        let mut store = WalSnapshotStore::new(&good_path).expect("the store opens");
        for lock_id in table.locks.keys() {
            store.note(*lock_id);
        }
        store.submit(&table).expect("the submit lands");
        store.pump().expect("the pump drains");
        store.flush(&table).expect("the flush lands");
    }

    // Shape 1: a zeroed data record under a live header. The scan
    // hydrates the LAST record per lock — the flush generation's slots —
    // so the zeroed twin must be one of those (slot 2): its header in
    // the ring survives (live expiry, valid checksum) and the data
    // record's checksum fails — the hydration refuses.
    let zeroed_dir = scratch("content-zeroed");
    let zeroed_path = wal_path(&zeroed_dir);
    fs::copy(&good_path, &zeroed_path).expect("the artefact copies");
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(&zeroed_path)
        .expect("the artefact opens for writing");
    std::io::Seek::seek(&mut file, std::io::SeekFrom::Start(2 * RECORD_SIZE as u64))
        .expect("the slot seeks");
    std::io::Write::write_all(&mut file, &[0u8; RECORD_SIZE]).expect("the record zeroes");
    drop(file);
    assert!(
        WalSnapshotStore::new(&zeroed_path).is_err(),
        "a zeroed data record under a live header refuses at open"
    );

    // Shape 2: a twin disagreement with valid checksums on both sides —
    // the flush generation's slot 3 data record is re-encoded with a
    // mutated field (the wrapper's codec stamps a valid data checksum)
    // while the header ring still carries the original.
    let twin_dir = scratch("content-twin");
    let twin_path = wal_path(&twin_dir);
    fs::copy(&good_path, &twin_path).expect("the artefact copies");
    let mut file = fs::read(&twin_path).expect("the artefact reads");
    let start = 3 * RECORD_SIZE;
    let slot: &[u8; RECORD_SIZE] = file[start..][..RECORD_SIZE]
        .try_into()
        .expect("slot 3 reads");
    let mut forged = record_decode(slot).expect("the engine's record decodes");
    forged.lease_id += 1;
    let forged_bytes = record_encode(&forged).expect("the forged record encodes");
    file[start..][..RECORD_SIZE].copy_from_slice(&forged_bytes);
    fs::write(&twin_path, &file).expect("the forged record writes");
    assert!(
        WalSnapshotStore::new(&twin_path).is_err(),
        "a twin disagreement refuses at open"
    );

    // The good artefact still opens: the refusals are the content's, not
    // the path's.
    assert!(
        WalSnapshotStore::new(&good_path).is_ok(),
        "the good reopens"
    );
}

// ---------------------------------------------------------------------------
// The copy canary: the cross-ABI interop proof. See the engine's
// wal_canary.zig for the full contract and the mirror-side sentinels.
// ---------------------------------------------------------------------------

// The copy canary's export. The contract (wal_canary.zig is the
// authority): caller-owned memory end to end — the engine allocates
// nothing, frees nothing, and touches only the 64 bytes handed to it,
// only for the duration of the call. It copies one 64-byte record from
// `src` to `dst` and bumps the bump fields (stamp, lease id) by exactly
// 1, interpreted as the struct engine-side (never offset math on raw
// bytes), increments checked (`std.math.add` — a wrap is an error code,
// never a silent wrap). 0 ok; negative on a null pointer, a misaligned
// buffer (both sides must be 8-aligned), overlapping slots, or a would-
// be wrap.
unsafe extern "C" {
    fn wal_canary_bump_copy(src: *const u8, dst: *mut u8) -> i32;
}

// The sentinels, mirrored from wal_canary.zig — keep both sides equal.
// Unsigned-only (u64/u32/[u8;16]; never i32/i64 anywhere in the record),
// with compile-time headroom: sentinel + 2 < maxInt, so neither of the
// two bumps can wrap even in a release build.
const BUMP_COUNT: u64 = 2;
const SENTINEL_HEAD: u64 = 0x7B1D_5EED_0000_0000;
const STAMP_SENTINEL: u64 = SENTINEL_HEAD | 1;
const EXPIRY_SENTINEL: u64 = SENTINEL_HEAD | 10;
const LOCK_ID_SENTINEL: u64 = SENTINEL_HEAD | 20;
const LEASE_ID_SENTINEL: u64 = SENTINEL_HEAD | 30;
const LEASE_MS_SENTINEL: u64 = SENTINEL_HEAD | 40;
const TAG_SENTINEL: u32 = 0x7FFF_FFF1;
const CHECKSUM_SENTINEL: u32 = 0x7FFF_FFF2;
const HOLDER_SENTINEL: [u8; 16] = [
    0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, //
    0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10,
];

const _: () = assert!(
    STAMP_SENTINEL + BUMP_COUNT < u64::MAX
        && LEASE_ID_SENTINEL + BUMP_COUNT < u64::MAX
        && EXPIRY_SENTINEL + BUMP_COUNT < u64::MAX
        && LOCK_ID_SENTINEL + BUMP_COUNT < u64::MAX
        && LEASE_MS_SENTINEL + BUMP_COUNT < u64::MAX
        && (TAG_SENTINEL as u64) + BUMP_COUNT < u32::MAX as u64
        && (CHECKSUM_SENTINEL as u64) + BUMP_COUNT < u32::MAX as u64
);

/// The sentinel record with the bump fields already applied (`bump` of
/// the two bumps), as the raw 64-byte image — the caller's slot content.
fn canary_record(bump: u64) -> WalRecord {
    WalRecord {
        stamp: STAMP_SENTINEL + bump,
        expiry: EXPIRY_SENTINEL,
        lock_id: LOCK_ID_SENTINEL,
        lease_id: LEASE_ID_SENTINEL + bump,
        holder: HOLDER_SENTINEL,
        lease_ms: LEASE_MS_SENTINEL,
        tag: TAG_SENTINEL,
        checksum: CHECKSUM_SENTINEL,
    }
}

/// The three slots, one caller-owned array: 8-aligned (the export's
/// documented alignment requirement), each slot exactly one record.
#[repr(C, align(8))]
struct Slots([[u8; RECORD_SIZE]; 3]);

/// The cross-ABI copy canary, against the real cdylib: Rust packs the
/// wide-bit sentinels into the real `WalRecord`, Zig copies-and-bumps
/// through three caller-owned slots (+1 per copy, checked, no wrap), and
/// Rust verifies the last slot byte-exact and field-true. What it proves
/// on this build (darwin/arm64 here; every CI arch builds it): the C
/// layout is the same struct on both sides, the size/shape/C field math
/// agree, little-endian carries through the boundary, and the Zig side
/// interprets the fields for what they are (a plain copy proves nothing;
/// the bump is the proof). Memory-only by design — no disk, no flush.
#[test]
fn copy_canary_cross_abi_three_slots() {
    let mut slots = Slots([[0u8; RECORD_SIZE]; 3]);
    let original = *record_as_bytes(&canary_record(0));
    slots.0[0] = original;

    // Two copies through the export, +1 per copy on the bump fields.
    let rc1 = unsafe { wal_canary_bump_copy(slots.0[0].as_ptr(), slots.0[1].as_mut_ptr()) };
    assert_eq!(rc1, 0, "the first bump copy succeeds");
    let rc2 = unsafe { wal_canary_bump_copy(slots.0[1].as_ptr(), slots.0[2].as_mut_ptr()) };
    assert_eq!(rc2, 0, "the second bump copy succeeds");

    // Slot 0 untouched: caller-owned memory is never written behind the
    // caller's back.
    assert_eq!(slots.0[0], original, "slot 0 is untouched");

    // Slot 1: bumped fields are sentinel + 1 EXACTLY — a wrap, a lost
    // bit, or a +1 caught as +0/+2 is a loud failure — and every
    // non-bumped field byte-equal to its sentinel, field-true at the
    // binding offsets.
    let one = &slots.0[1];
    assert_eq!(one[0..8], (STAMP_SENTINEL + 1).to_le_bytes());
    assert_eq!(one[8..16], EXPIRY_SENTINEL.to_le_bytes());
    assert_eq!(one[16..24], LOCK_ID_SENTINEL.to_le_bytes());
    assert_eq!(one[24..32], (LEASE_ID_SENTINEL + 1).to_le_bytes());
    assert_eq!(one[32..48], HOLDER_SENTINEL);
    assert_eq!(one[48..56], LEASE_MS_SENTINEL.to_le_bytes());
    assert_eq!(one[56..60], TAG_SENTINEL.to_le_bytes());
    assert_eq!(one[60..64], CHECKSUM_SENTINEL.to_le_bytes());
    assert_eq!(one, record_as_bytes(&canary_record(1)), "slot 1 byte-exact");

    // Slot 2: byte-exact against the locally computed expectation, and
    // field-true at the binding offsets — the bumped fields are sentinel
    // + 2 EXACTLY.
    let two = &slots.0[2];
    assert_eq!(two, record_as_bytes(&canary_record(2)), "slot 2 byte-exact");
    assert_eq!(two[0..8], (STAMP_SENTINEL + 2).to_le_bytes());
    assert_eq!(two[24..32], (LEASE_ID_SENTINEL + 2).to_le_bytes());

    // Unsigned semantics: every field compares as its unsigned type, no
    // negative anywhere, and the wide prefix keeps every value far from
    // any wrap (the headroom property, observable).
    let wide = [
        STAMP_SENTINEL + 2,
        EXPIRY_SENTINEL,
        LOCK_ID_SENTINEL,
        LEASE_ID_SENTINEL + 2,
        LEASE_MS_SENTINEL,
    ];
    for value in wide {
        assert_eq!(
            value & 0x8000_0000_0000_0000,
            0,
            "the wide sentinel stays positive"
        );
    }

    // Distinctness: no two fields collide after the bumps (the wide
    // prefix plus spaced low bits, +1/+2).
    let mut values = wide.to_vec();
    values.push(TAG_SENTINEL as u64);
    values.push(CHECKSUM_SENTINEL as u64);
    for (i, a) in values.iter().enumerate() {
        for b in &values[i + 1..] {
            assert_ne!(a, b, "no two fields collide after the bumps");
        }
    }
}

/// The canary's refusals: a null pointer, overlapping slots, and a
/// misaligned buffer are refused with a negative code — never
/// dereferenced, never copied, never wrapped.
#[test]
fn copy_canary_refusals() {
    let mut slots = Slots([[0u8; RECORD_SIZE]; 3]);
    let base = slots.0.as_mut_ptr().cast::<u8>();

    let null_src = unsafe { wal_canary_bump_copy(std::ptr::null(), base.add(RECORD_SIZE)) };
    assert!(null_src < 0, "a null src is refused");
    let null_dst = unsafe { wal_canary_bump_copy(base, std::ptr::null_mut()) };
    assert!(null_dst < 0, "a null dst is refused");

    // Overlapping slots (dst inside src's 64-byte span, and the mirror
    // direction) are refused, both pointers aligned.
    let overlap = unsafe { wal_canary_bump_copy(base, base.add(8)) };
    assert!(overlap < 0, "overlapping slots are refused");
    let overlap_back = unsafe { wal_canary_bump_copy(base.add(8), base) };
    assert!(
        overlap_back < 0,
        "overlapping slots (backwards) are refused"
    );

    // A misaligned buffer is refused (the struct's alignment is 8):
    // dst at a 4-byte offset.
    let misaligned =
        unsafe { wal_canary_bump_copy(base.add(RECORD_SIZE), base.add(RECORD_SIZE + 4)) };
    assert!(misaligned < 0, "a misaligned dst is refused");
    let misaligned_src = unsafe { wal_canary_bump_copy(base.add(1), base.add(RECORD_SIZE)) };
    assert!(misaligned_src < 0, "a misaligned src is refused");

    // Adjacent slots (64 bytes apart, the real scenario) are NOT
    // overlap: they copy fine.
    let ok = unsafe { wal_canary_bump_copy(base, base.add(RECORD_SIZE)) };
    assert_eq!(ok, 0, "adjacent slots copy fine");
}
