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

fn beat(store: &mut WalSnapshotStore) {
    store.pump().expect("the pump drains");
    for _ in 0..80 {
        let status = store.status().expect("the status reads");
        if status.ephemeral.in_flight == 0 && status.durable.in_flight == 0 {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(25));
        store.pump().expect("the pump drains");
    }
    panic!("the store's batch never completed");
}

fn flip_bytes(bytes: &mut [u8]) {
    for b in bytes.iter_mut() {
        *b = !*b;
    }
}

fn lease(lock_id: u64, lease_id: u64, expiry_ms: u64) -> Lease {
    Lease {
        lease_id,
        holder: Uuid::from_u128(
            0x0102_0304_0506_0708_090a_0b0c_0d0e_0f00u128 ^ u128::from(lock_id),
        ),
        expiry: expiry_ms,
        lease_ms: 5_000,
        name: None,
        labels: None,
        taken_at_ms: 0,
        renew_count: 0,
    }
}

fn named_lease(lock_id: u64, lease_id: u64, expiry_ms: u64) -> Lease {
    Lease {
        name: Some(format!("/lock-{lock_id}")),
        labels: Some(vec!["lane-a".to_string(), "lane-b".to_string()]),
        taken_at_ms: 1_700_000_000_000 + lock_id,
        renew_count: 2,
        ..lease(lock_id, lease_id, expiry_ms)
    }
}

fn snapshot(locks: impl IntoIterator<Item = (u64, Lease)>) -> StateSnapshot {
    let locks = locks.into_iter().collect::<BTreeMap<_, _>>();
    let generations = locks
        .iter()
        .map(|(&lock_id, lease)| (lock_id, lease.lease_id))
        .collect();
    StateSnapshot { locks, generations }
}

fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("the clock is past the epoch")
        .as_millis() as u64
}

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
        beat(&mut store);
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
    assert_eq!(file.len(), 8 * 1024 * 1024, "the artefact is fixed-size");
    let mut stamps = BTreeMap::new();
    for (slot, original) in table.locks.iter().enumerate() {
        let raw: [u8; RECORD_SIZE] = file[(3 + slot) * RECORD_SIZE..][..RECORD_SIZE]
            .try_into()
            .expect("a whole record reads");
        // The data ring's slot is the record's NOT — un-flip to decode.
        let mut bytes = raw;
        flip_bytes(&mut bytes);
        let decoded = record_decode(&bytes).expect("the engine's record decodes");
        assert_eq!(
            record_encode(&decoded).expect("the record re-encodes"),
            bytes,
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
        let raw: [u8; RECORD_SIZE] = file[slot * RECORD_SIZE..][..RECORD_SIZE]
            .try_into()
            .expect("a whole record reads");
        let mut bytes = raw;
        flip_bytes(&mut bytes);
        let decoded = record_decode(&bytes).expect("the engine's record decodes");
        assert_eq!(
            record_encode(&decoded).expect("the record re-encodes"),
            bytes,
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
        beat(&mut store);
        // The release: the lock leaves the table, the tick submits again.
        store.note(1);
        store
            .submit(&snapshot(Vec::<(u64, Lease)>::new()))
            .expect("the tombstone lands");
        beat(&mut store);

        let status = store.status().expect("the status reads");
        assert_eq!(
            status.ephemeral.completed_records, 2,
            "one live, one tombstone"
        );
        assert_eq!(status.ephemeral.completed_generations, 2);
        assert_eq!(status.durable.completed_records, 0);
    }

    let store = WalSnapshotStore::new(&path).expect("the store reopens");
    assert_eq!(
        store.load().expect("the load succeeds"),
        None,
        "the tombstone dropped the lock"
    );
}

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
        beat(&mut store);
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
    beat(&mut store);

    let error = store.flush(&table).expect_err("the flush refuses");
    assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
    let status = store.status().expect("the status reads");
    assert_eq!(status.ephemeral.refusals, 1, "the refusal is counted");
    assert_eq!(
        status.ephemeral.completed_generations, 1,
        "the submit, not the flush"
    );
    assert_eq!(status.ephemeral.completed_records, 2);
    assert_eq!(status.durable.refusals, 0);

    // The tick path's refusal discipline: the changed set survives a
    // refused submit so the tick retries, and every retry is counted.
    store.note(3);
    assert!(store.submit(&table).is_err(), "the ring is still full");
    assert!(store.submit(&table).is_err(), "the set was not dropped");
    assert_eq!(
        store.status().expect("the status reads").ephemeral.refusals,
        3
    );
}

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
    assert_eq!(before.ephemeral.completed_generations, 0);
    assert_eq!(before.ephemeral.completed_records, 0);
    assert_eq!(before.ephemeral.in_flight, 0);
    assert_eq!(before.ephemeral.refusals, 0);
    assert_eq!(before.durable.completed_records, 0);
    assert_eq!(before.durable.refusals, 0);

    // The steady tick first (the mirror tracks the table through the
    // tick path), then the shutdown generation.
    for lock_id in table.locks.keys() {
        store.note(*lock_id);
    }
    store.submit(&table).expect("the submit lands");
    beat(&mut store);
    store.flush(&table).expect("the flush lands");
    let after = store.status().expect("the status reads");
    assert_eq!(
        after.ephemeral.completed_generations, 2,
        "the tick and the flush"
    );
    assert_eq!(after.ephemeral.completed_records, 4);
    assert!(after.last_completed_unix_ns > 0);
    assert_eq!(after.ephemeral.in_flight, 0);
    assert_eq!(after.ephemeral.refusals, 0);
    assert_eq!(
        after.durable.completed_generations, 1,
        "the flush writes both streams"
    );
    assert_eq!(after.durable.completed_records, 0);

    // The eventfd passthrough: the ring's descriptor on Linux, nothing
    // where the platform completes inline.
    if cfg!(target_os = "linux") {
        assert!(store.event_fd().expect("the ring has an eventfd") >= 0);
    } else {
        assert_eq!(store.event_fd(), None);
    }
}

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
    beat(&mut store);
    store.note(1);
    assert_eq!(store.submit(&table).expect("the renew lands"), 1);
    beat(&mut store);
    assert_eq!(store.submit(&table).expect("the set is clear"), 0);
    assert_eq!(
        store
            .status()
            .expect("the status reads")
            .ephemeral
            .completed_records,
        3
    );
}

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
        beat(&mut store);
        store.flush(&table).expect("the flush lands");
    }

    // Shape 1: a zeroed data record under a live header. The scan
    // hydrates the LAST record per lock — the flush generation's slots —
    // so the zeroed twin must be one of those (slot 2): its header in
    // the ring survives (live expiry, valid checksum) while the data
    // slot's NOT — all-zeros un-flipped to all-0xFF — fails the record
    // checksum: the hydration refuses.
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
    // while the header ring still carries the original. The data slot is
    // the record's NOT: un-flip to decode and re-encode, flip back to
    // write.
    let twin_dir = scratch("content-twin");
    let twin_path = wal_path(&twin_dir);
    fs::copy(&good_path, &twin_path).expect("the artefact copies");
    let mut file = fs::read(&twin_path).expect("the artefact reads");
    let start = 3 * RECORD_SIZE;
    let mut slot_bytes: [u8; RECORD_SIZE] = file[start..][..RECORD_SIZE]
        .try_into()
        .expect("slot 3 reads");
    flip_bytes(&mut slot_bytes);
    let mut forged = record_decode(&slot_bytes).expect("the engine's record decodes");
    forged.lease_id += 1;
    let mut forged_bytes = record_encode(&forged).expect("the forged record encodes");
    flip_bytes(&mut forged_bytes);
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

#[test]
fn tick_and_flush_classify_expiry_zero_named_leases_alike() {
    let dir = scratch("kept-classification");
    let path = wal_path(&dir);

    // A loaded kept lock: the table holds the named record with no
    // expiry (what the lazy load installs; what a break leaves).
    let kept = Lease {
        expiry: 0,
        lease_ms: 0,
        holder: Uuid::nil(),
        ..named_lease(5, 50, 0)
    };
    let table = snapshot([(5u64, kept.clone())]);

    let mut store = WalSnapshotStore::new(&path).expect("the store opens");
    store.note(5);
    assert_eq!(store.submit(&table).expect("the tick lands"), 1);
    beat(&mut store);
    let status = store.status().expect("the status reads");
    assert_eq!(status.durable.completed_records, 1, "one kept frame");
    assert_eq!(status.ephemeral.completed_records, 0);

    // The shutdown generation classifies the same lease the same way
    // and reconciles against the mirror the tick built.
    store.flush(&table).expect("the flush reconciles");
    let status = store.status().expect("the status reads");
    assert_eq!(status.durable.completed_records, 2);
    assert_eq!(status.durable.refusals, 0);

    // The kept lock survives the restart exactly as it stood.
    drop(store);
    let reopened = WalSnapshotStore::new(&path).expect("the store reopens");
    assert_eq!(
        reopened.load().expect("the load succeeds"),
        Some(snapshot([(5u64, kept)]))
    );
}

#[test]
fn a_flush_without_a_prior_tick_is_the_loud_failed_stop() {
    let dir = scratch("tick-before-flush");
    let path = wal_path(&dir);
    let held = snapshot([(1u64, lease(1, 10, now_millis() + 60_000))]);
    let released = snapshot(Vec::<(u64, Lease)>::new());

    let mut store = WalSnapshotStore::new(&path).expect("the store opens");
    // Acquire rides the tick: the mirror holds lock 1.
    store.note(1);
    store.submit(&held).expect("the tick lands");
    beat(&mut store);

    // Release — but the changed set never rode a tick before the flush.
    // The mirror still holds lock 1; the caller's set is empty; the
    // reconciliation is the loud failed stop, never a silent drift.
    store.note(1);
    let error = store.flush(&released).expect_err("the flush refuses");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::InvalidInput,
        "a diverged set is the loud failed stop"
    );

    // The submit-before-flush order: the tombstone catches the mirror
    // up, and the flush reconciles.
    store.submit(&released).expect("the tick lands");
    beat(&mut store);
    store.flush(&released).expect("the flush reconciles");
}

#[test]
fn the_loaded_table_rides_the_tick_before_a_clean_flush() {
    let dir = scratch("loaded-catch-up");
    let path = wal_path(&dir);
    let held = snapshot([
        (7u64, named_lease(7, 70, now_millis() + 60_000)),
        (9, lease(9, 90, now_millis() + 60_000)),
    ]);

    // First life: the ticks carry both locks, then the clean stop's
    // flush.
    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        for lock_id in held.locks.keys() {
            store.note(*lock_id);
        }
        store.submit(&held).expect("the tick lands");
        beat(&mut store);
        store.flush(&held).expect("the flush lands");
    }

    // Second life: the mirrors start empty. The lazy load hands the
    // table back; the node notes every loaded lock; the first tick
    // submits the whole loaded set; the stop's flush reconciles against
    // the caught-up mirror.
    let mut reopened = WalSnapshotStore::new(&path).expect("the store reopens");
    let loaded = reopened
        .load()
        .expect("the load succeeds")
        .expect("the clean stop's artefact loads");
    assert_eq!(loaded, held, "the loaded table is the flushed one");
    for lock_id in loaded.locks.keys() {
        reopened.note(*lock_id);
    }
    assert_eq!(
        reopened.submit(&loaded).expect("the catch-up tick lands"),
        2,
        "both loaded locks ride the first tick"
    );
    beat(&mut reopened);
    reopened.flush(&loaded).expect("the flush reconciles");
}

#[test]
fn the_forgotten_store_reconciles_against_the_rebuilt_mirrors() {
    let dir = scratch("forget");
    let path = wal_path(&dir);
    let pre_crash = snapshot([(1u64, lease(1, 10, now_millis() + 60_000))]);
    let post_crash = snapshot([(2u64, lease(2, 20, now_millis() + 60_000))]);

    // First life: lock 1 rides the ticks and the clean stop's flush —
    // the artefact carries it.
    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        store.note(1);
        store.submit(&pre_crash).expect("the tick lands");
        beat(&mut store);
        store.flush(&pre_crash).expect("the flush lands");
    }

    // The dirty boot: the store forgets the artefact (the load guard's
    // write half) — the stale mirror is gone, the load is the cold
    // start's answer.
    let mut store = WalSnapshotStore::new(&path).expect("the store reopens");
    assert_eq!(
        store.load().expect("the load reads"),
        Some(pre_crash.clone()),
        "the artefact is there until the verdict forgets it"
    );
    // Through the TRAIT dispatch — the node holds the store boxed, and
    // the trait's default forget is a no-op: the delegation below is
    // the wiring the dirty boot actually drives.
    {
        let sealed: &mut dyn lunet_advisory_lock::spi::StateStore = &mut store;
        sealed.forget();
    }
    assert_eq!(
        store.load().expect("the load reads"),
        None,
        "a forgotten artefact loads as nothing"
    );

    // The rebuilt life: lock 2 (the replica stream's own commit) rides
    // the tick, and the clean stop's flush reconciles — against the
    // rebuilt mirror, never against the pre-crash artefact. Without the
    // forget, the stale mirror would hold lock 1 and this flush would
    // be the loud failed stop.
    store.note(2);
    store.submit(&post_crash).expect("the tick lands");
    beat(&mut store);
    store.flush(&post_crash).expect("the flush reconciles");
    let status = store.status().expect("the status reads");
    assert_eq!(
        status.ephemeral.completed_records, 2,
        "lock 2's record: the tick's and the flush's — lock 1's pre-crash \
         records were forgotten with the artefact"
    );
    assert_eq!(status.ephemeral.refusals, 0);
}

#[test]
fn a_dead_store_refuses_other_and_loads_none() {
    let dir = scratch("dead-store");
    let path = wal_path(&dir);
    let held = snapshot([(1u64, lease(1, 10, now_millis() + 60_000))]);
    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        store.note(1);
        store.submit(&held).expect("the tick lands");
        beat(&mut store);
        store.flush(&held).expect("the flush lands");
    }

    // The reopen the forget runs removes the artefact and reopens the
    // path — the one way it is refused is the filesystem's: the parent
    // directory is gone, so the reopen fails and the store dies.
    let mut store = WalSnapshotStore::new(&path).expect("the store reopens");
    fs::remove_dir_all(&dir).expect("the directory goes");
    {
        let sealed: &mut dyn lunet_advisory_lock::spi::StateStore = &mut store;
        sealed.forget();
    }

    let error = store.pump().expect_err("the dead pump refuses");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::Other,
        "the dead pump is the EIO class"
    );
    let error = store.status().expect_err("the dead status refuses");
    assert_eq!(
        error.kind(),
        std::io::ErrorKind::Other,
        "the dead status is the EIO class"
    );
    assert_eq!(
        store.load().expect("the dead load answers"),
        None,
        "a dead store loads as nothing, never an Err"
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

#[repr(C, align(8))]
struct Slots([[u8; RECORD_SIZE]; 3]);

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

// ---------------------------------------------------------------------------
// The durable stream: the fat frame's binding mirror, its codec round
// trip through the engine, and the tag-bytes convention.
// ---------------------------------------------------------------------------

use lunet_advisory_lock::wal_snapshot::{
    DurableChange, DurableKind, FAT_FRAME_LAYOUT, FAT_FRAME_SIZE, FAT_NAME_LEN_OFFSET,
    FAT_NAME_OFFSET, FAT_TAG_BYTES_OFFSET, FAT_TAG_LEN_OFFSET, FatFrame, frame_as_bytes,
    frame_decode, frame_encode, labels_to_tag_bytes,
};

#[test]
fn fat_frame_layout_is_exact() {
    let _: () = FAT_FRAME_LAYOUT;
    let frame = FatFrame {
        header: WalRecord {
            stamp: 0x0102030405060708,
            expiry: 0x1112131415161718,
            lock_id: 0x2122232425262728,
            lease_id: 0x3132333435363738,
            holder: *b"holder-uuid-0001",
            lease_ms: 0x4142434445464748,
            tag: 0x5152,
            checksum: 0x6162,
        },
        data: [0u8; FAT_FRAME_SIZE - RECORD_SIZE],
    };
    let bytes = frame_as_bytes(&frame);
    assert_eq!(bytes.len(), 768);
    assert_eq!(frame.header.stamp.to_le_bytes(), bytes[0..8]);
    assert_eq!(frame.header.tag.to_le_bytes(), bytes[56..60]);
    assert_eq!(frame.header.checksum.to_le_bytes(), bytes[60..64]);
    // The name length sits at the first byte past the header.
    let mut with_name = frame;
    with_name.data[0] = 0xAB;
    with_name.data[1] = 0xCD;
    let raw = frame_as_bytes(&with_name);
    assert_eq!(&raw[64..66], &[0xAB, 0xCD]);
    // The counters sit at CL6's start; the consistency area at CL8's.
    with_name.data[FAT_TAKEN_AT_AREA] = 0x42;
    assert_eq!(frame_as_bytes(&with_name)[384], 0x42);
    assert_eq!(FAT_FRAME_SIZE % 64, 0, "twelve whole cache lines");
}

const FAT_TAKEN_AT_AREA: usize = 384 - RECORD_SIZE;

#[repr(C, align(8))]
struct FatSlots([[u8; FAT_FRAME_SIZE]; 3]);

#[test]
fn frame_codec_roundtrip_byte_exact() {
    let change = DurableChange {
        lock_id: 7,
        lease_id: 70,
        holder: *Uuid::nil().as_bytes(),
        expiry_ns: 456 * 1_000_000,
        lease_ms: 5_000,
        kind: DurableKind::Live,
        name_bytes: b"/locks/seven".to_vec(),
        taken_at_ms: 1_700_000_000_123,
        renew_count: 3,
        tag_bytes: labels_to_tag_bytes(&["lane-a".to_string(), "lane-b".to_string()]),
    };
    assert_eq!(
        change.tag_bytes,
        b"lane-a\nlane-b".to_vec(),
        "the convention"
    );
    let bytes = frame_encode(123, &change).expect("the frame encodes");

    let decoded = frame_decode(&bytes).expect("the frame decodes");
    assert_eq!(decoded.lock_id, 7);
    assert_eq!(decoded.lease_id, 70);
    assert_eq!(decoded.expiry, 456 * 1_000_000);
    assert_eq!(decoded.lease_ms, 5_000);
    assert_eq!(decoded.kind, DurableKind::Live);
    assert_eq!(decoded.holder, *Uuid::nil().as_bytes());
    assert_eq!(decoded.name, b"/locks/seven".to_vec());
    assert_eq!(decoded.taken_at_ms, 1_700_000_000_123);
    assert_eq!(decoded.renew_count, 3);
    assert_eq!(decoded.tag_bytes, b"lane-a\nlane-b".to_vec());
    assert_eq!(
        decoded.labels(),
        vec!["lane-a".to_string(), "lane-b".to_string()]
    );

    // A rotted consistency area is a refusal (the sha gate).
    let mut rotted = bytes;
    rotted[516] ^= 0xFF;
    assert_eq!(frame_decode(&rotted), None);

    // A rotted name byte is a refusal (the frame checksum gate).
    let mut rotted_name = bytes;
    rotted_name[70] ^= 0xFF;
    assert_eq!(frame_decode(&rotted_name), None);

    // A zero lock id refuses the encode.
    assert_eq!(
        frame_encode(
            123,
            &DurableChange {
                lock_id: 0,
                ..change.clone()
            }
        ),
        None
    );
}

// ---------------------------------------------------------------------------
// The fat-frame copy canary: the cross-ABI interop proof over the
// durable stream's 768-byte frame. The contract (wal_canary.zig is the
// authority): caller-owned memory end to end; three slots; the engine
// copies slot n to slot n+1 bumping stamp, lease id, taken-at and renew
// by exactly 1, field-true; slot 3 verifies.
// ---------------------------------------------------------------------------

unsafe extern "C" {
    fn wal_canary_fat_bump_copy(src: *const u8, dst: *mut u8) -> i32;
}

// The sentinels, mirrored from wal_canary.zig — keep both sides equal.
const FAT_TAKEN_AT_SENTINEL: u64 = SENTINEL_HEAD | 50;
const FAT_RENEW_SENTINEL: u32 = 0x7FFF_FFF3;
const FAT_NAME_LEN_SENTINEL: u16 = 0x7BEF;
const FAT_TAG_LEN_SENTINEL: u16 = 0x5EED;
const FAT_NAME_SENTINEL: [u8; 8] = [0xF0, 0xF1, 0xF2, 0xF3, 0xF4, 0xF5, 0xF6, 0xF7];
const FAT_TAG_SENTINEL: [u8; 8] = [0xA0, 0xA1, 0xA2, 0xA3, 0xA4, 0xA5, 0xA6, 0xA7];

const _: () = assert!(FAT_FRAME_SIZE == 768);

fn canary_fat_frame(bump: u64) -> FatFrame {
    let mut frame = FatFrame {
        header: WalRecord {
            stamp: STAMP_SENTINEL + bump,
            expiry: EXPIRY_SENTINEL,
            lock_id: LOCK_ID_SENTINEL,
            lease_id: LEASE_ID_SENTINEL + bump,
            holder: HOLDER_SENTINEL,
            lease_ms: LEASE_MS_SENTINEL,
            tag: TAG_SENTINEL,
            checksum: CHECKSUM_SENTINEL,
        },
        data: [0u8; FAT_FRAME_SIZE - RECORD_SIZE],
    };
    frame.data[0..2].copy_from_slice(&FAT_NAME_LEN_SENTINEL.to_le_bytes());
    frame.data[2..10].copy_from_slice(&FAT_NAME_SENTINEL);
    frame.data[FAT_TAKEN_AT_AREA..][..8]
        .copy_from_slice(&(FAT_TAKEN_AT_SENTINEL + bump).to_le_bytes());
    frame.data[FAT_TAKEN_AT_AREA + 8..][..4]
        .copy_from_slice(&(FAT_RENEW_SENTINEL + bump as u32).to_le_bytes());
    let tag_len_at = 396 - RECORD_SIZE;
    let tag_bytes_at = 398 - RECORD_SIZE;
    frame.data[tag_len_at..][..2].copy_from_slice(&FAT_TAG_LEN_SENTINEL.to_le_bytes());
    frame.data[tag_bytes_at..][..8].copy_from_slice(&FAT_TAG_SENTINEL);
    frame.data[(512 - RECORD_SIZE + 8)..].fill(0x5C);
    frame
}

#[test]
fn fat_canary_cross_abi_three_slots_field_true() {
    const _: () = assert!(FAT_TAKEN_AT_SENTINEL + 2 < u64::MAX);
    const _: () = assert!(FAT_TAKEN_AT_SENTINEL != STAMP_SENTINEL);
    const _: () = assert!(FAT_NAME_LEN_SENTINEL != FAT_TAG_LEN_SENTINEL);

    let mut slots = FatSlots([[0u8; FAT_FRAME_SIZE]; 3]);
    slots.0[0].copy_from_slice(frame_as_bytes(&canary_fat_frame(0)));

    let rc1 = unsafe { wal_canary_fat_bump_copy(slots.0[0].as_ptr(), slots.0[1].as_mut_ptr()) };
    let rc2 = unsafe { wal_canary_fat_bump_copy(slots.0[1].as_ptr(), slots.0[2].as_mut_ptr()) };
    assert_eq!(rc1, 0);
    assert_eq!(rc2, 0);

    // Slot 0 untouched.
    assert_eq!(
        &slots.0[0][..],
        frame_as_bytes(&canary_fat_frame(0)).as_slice()
    );

    // Slot 1: the four bump fields are sentinel + 1 EXACTLY, field-true
    // through the mirror's accessors — a lost bit or a wrap is loud.
    let one: &[u8; FAT_FRAME_SIZE] = &slots.0[1];
    let decoded = frame_decode_raw_for_test(one);
    assert_eq!(decoded.header.stamp, STAMP_SENTINEL + 1);
    assert_eq!(decoded.header.lease_id, LEASE_ID_SENTINEL + 1);
    assert_eq!(decoded.taken_at_ms(), FAT_TAKEN_AT_SENTINEL + 1);
    assert_eq!(decoded.renew_count(), FAT_RENEW_SENTINEL + 1);
    assert_eq!(decoded.header.expiry, EXPIRY_SENTINEL);
    assert_eq!(decoded.header.lock_id, LOCK_ID_SENTINEL);
    assert_eq!(decoded.header.lease_ms, LEASE_MS_SENTINEL);
    assert_eq!(decoded.header.tag, TAG_SENTINEL);
    assert_eq!(decoded.header.checksum, CHECKSUM_SENTINEL);
    assert_eq!(
        &decoded.data[FAT_NAME_LEN_OFFSET - RECORD_SIZE..][..2],
        &FAT_NAME_LEN_SENTINEL.to_le_bytes(),
        "the name length sentinel"
    );
    assert_eq!(
        &decoded.data[FAT_NAME_OFFSET - RECORD_SIZE..][..8],
        &FAT_NAME_SENTINEL,
        "the name area byte-exact"
    );
    assert_eq!(
        &decoded.data[FAT_TAG_LEN_OFFSET - RECORD_SIZE..][..2],
        &FAT_TAG_LEN_SENTINEL.to_le_bytes(),
        "the tag length sentinel"
    );
    assert_eq!(
        &decoded.data[FAT_TAG_BYTES_OFFSET - RECORD_SIZE..][..8],
        &FAT_TAG_SENTINEL,
        "the tag bytes byte-exact"
    );
    assert!(
        decoded.data[(512 - RECORD_SIZE + 8)..]
            .iter()
            .all(|&b| b == 0x5C)
    );

    // Slot 3 (the third slot): byte-exact against the locally computed
    // expectation AND field-true once more — the proof of the proof.
    assert_eq!(
        &slots.0[2][..],
        frame_as_bytes(&canary_fat_frame(2)).as_slice()
    );
    let two: &[u8; FAT_FRAME_SIZE] = &slots.0[2];
    let last = frame_decode_raw_for_test(two);
    assert_eq!(last.taken_at_ms(), FAT_TAKEN_AT_SENTINEL + 2);
    assert_eq!(last.renew_count(), FAT_RENEW_SENTINEL + 2);
    assert_eq!(last.header.stamp, STAMP_SENTINEL + 2);
    assert_eq!(last.header.lease_id, LEASE_ID_SENTINEL + 2);

    // Distinctness: no two bumped values collide.
    let bumped = [last.header.stamp, last.taken_at_ms()];
    assert_ne!(bumped[0], bumped[1]);
}

fn frame_decode_raw_for_test(bytes: &[u8; FAT_FRAME_SIZE]) -> FatFrame {
    let mut out = [0u8; FAT_FRAME_SIZE];
    out.copy_from_slice(bytes);
    unsafe { std::ptr::read(out.as_ptr() as *const FatFrame) }
}

#[test]
fn fat_canary_refusals() {
    let mut slots = FatSlots([[0u8; FAT_FRAME_SIZE]; 3]);
    let base = slots.0.as_mut_ptr().cast::<u8>();
    let src = base;

    let null_src = unsafe { wal_canary_fat_bump_copy(std::ptr::null(), base) };
    let null_dst = unsafe { wal_canary_fat_bump_copy(src, std::ptr::null_mut()) };
    assert_eq!(null_src, -22);
    assert_eq!(null_dst, -22);

    let overlap = unsafe { wal_canary_fat_bump_copy(src, base.add(8)) };
    let overlap_back = unsafe { wal_canary_fat_bump_copy(base.add(8), base) };
    assert_eq!(overlap, -22);
    assert_eq!(overlap_back, -22);

    // Adjacent slots are not overlap.
    let ok = unsafe { wal_canary_fat_bump_copy(src, slots.0[1].as_mut_ptr()) };
    assert_eq!(ok, 0);

    let misaligned_src = unsafe { wal_canary_fat_bump_copy(src.add(1), slots.0[1].as_mut_ptr()) };
    let misaligned_dst = unsafe { wal_canary_fat_bump_copy(src, base.add(FAT_FRAME_SIZE + 4)) };
    assert_eq!(misaligned_src, -22);
    assert_eq!(misaligned_dst, -22);
}

// ---------------------------------------------------------------------------
// The two-stream store: the durable path through WalSnapshotStore.
// ---------------------------------------------------------------------------

#[test]
fn durable_submit_load_roundtrip_byte_exact() {
    let dir = scratch("durable-roundtrip");
    let path = wal_path(&dir);
    let table = snapshot([
        (5u64, named_lease(5, 50, now_millis() + 60_000)),
        (6, named_lease(6, 60, now_millis() + 60_000)),
        (7u64, lease(7, 70, now_millis() + 60_000)),
    ]);

    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        assert_eq!(store.load().expect("the cold load succeeds"), None);
        for lock_id in table.locks.keys() {
            store.note(*lock_id);
        }
        // Two frames and one record: three members, one generation.
        assert_eq!(store.submit(&table).expect("the submit lands"), 3);
        beat(&mut store);

        let status = store.status().expect("the status reads");
        assert_eq!(status.ephemeral.completed_records, 1);
        assert_eq!(status.ephemeral.completed_generations, 1);
        assert_eq!(status.durable.completed_records, 2);
        assert_eq!(status.durable.completed_generations, 1);

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
        if original.name.is_some() {
            // The durable class: every frame-area field restored.
            assert_eq!(loaded.name, original.name);
            assert_eq!(loaded.labels, original.labels);
            assert_eq!(loaded.taken_at_ms, original.taken_at_ms);
            assert_eq!(loaded.renew_count, original.renew_count);
        } else {
            // The ephemeral class: the record format's content only.
            assert_eq!(loaded.name, None);
            assert_eq!(loaded.labels, None);
            assert_eq!(loaded.taken_at_ms, 0);
            assert_eq!(loaded.renew_count, 0);
        }
        assert_eq!(loaded.lease_id, original.lease_id);
        assert_eq!(loaded.holder, original.holder);
        assert_eq!(loaded.expiry, original.expiry);
        assert_eq!(loaded.lease_ms, original.lease_ms);
    }
}

#[test]
fn durable_release_keeps_the_identity() {
    let dir = scratch("durable-kept");
    let path = wal_path(&dir);
    let table = snapshot([(5u64, named_lease(5, 50, now_millis() + 60_000))]);

    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        store.note(5);
        store.submit(&table).expect("the live frame lands");
        beat(&mut store);

        // The release: the lock leaves the table, the tick submits the
        // kept frame — the identity retained.
        store.note(5);
        store
            .submit(&snapshot(Vec::<(u64, Lease)>::new()))
            .expect("the kept frame lands");
        beat(&mut store);

        let status = store.status().expect("the status reads");
        assert_eq!(status.durable.completed_records, 2, "one live, one kept");
        assert_eq!(status.ephemeral.completed_records, 0);

        // The shutdown flush: the kept frame stands in the store's set.
        store
            .flush(&snapshot(Vec::<(u64, Lease)>::new()))
            .expect("the flush lands");
    }

    let store = WalSnapshotStore::new(&path).expect("the store reopens");
    let loaded = store
        .load()
        .expect("the load succeeds")
        .expect("the kept lock loads");
    let kept = loaded.locks.get(&5).expect("the kept lock loads");
    assert_eq!(kept.name, Some("/lock-5".to_string()));
    assert_eq!(
        kept.labels,
        Some(vec!["lane-a".to_string(), "lane-b".to_string()])
    );
    assert_eq!(kept.taken_at_ms, 1_700_000_000_000 + 5);
    assert_eq!(kept.renew_count, 2);
    assert_eq!(kept.lease_id, 50, "the fencing token survives");
    assert_eq!(kept.expiry, 0, "a kept record carries no expiry");
    assert_eq!(kept.holder, Uuid::nil(), "no holder");
}

#[test]
fn durable_retake_after_kept() {
    let dir = scratch("durable-retake");
    let path = wal_path(&dir);

    {
        let mut store = WalSnapshotStore::new(&path).expect("the store opens");
        store.note(5);
        store
            .submit(&snapshot([(
                5u64,
                named_lease(5, 50, now_millis() + 60_000),
            )]))
            .expect("the live frame lands");
        beat(&mut store);
        store.note(5);
        store
            .submit(&snapshot(Vec::<(u64, Lease)>::new()))
            .expect("the kept frame lands");
        beat(&mut store);
        store.note(5);
        // Built once: the flush must carry byte-for-byte what the tick
        // submitted — the reconciliation compares them, so a fresh
        // `now_millis()` here would be a real divergence, not a test
        // artifact.
        let retaken = named_lease(5, 51, now_millis() + 120_000);
        store
            .submit(&snapshot([(5u64, retaken.clone())]))
            .expect("the live frame lands again");
        beat(&mut store);
        store
            .flush(&snapshot([(5u64, retaken)]))
            .expect("the flush lands");
    }

    let store = WalSnapshotStore::new(&path).expect("the store reopens");
    let loaded = store.load().expect("the load succeeds").expect("loads");
    let live = loaded.locks.get(&5).expect("the lock loads");
    assert_eq!(live.lease_id, 51);
    assert_eq!(live.renew_count, 2);
    assert!(live.expiry > now_millis(), "the live expiry is back");
}

#[test]
fn durable_capacity_refusal_counts_per_stream() {
    let dir = scratch("durable-capacity");
    let path = wal_path(&dir);
    let table = snapshot([
        (1u64, named_lease(1, 10, now_millis() + 60_000)),
        (2, named_lease(2, 20, now_millis() + 60_000)),
    ]);

    let mut store = WalSnapshotStore::new_with_capacity(&path, 2).expect("the store opens");
    store.note(1);
    store.note(2);
    assert_eq!(store.submit(&table).expect("the submit lands"), 2);
    beat(&mut store);

    // The third durable lock: the ring cannot take it — counted on the
    // durable row, the serving state untouched.
    store.note(3);
    let error = store
        .submit(&snapshot([
            (1u64, named_lease(1, 10, now_millis() + 60_000)),
            (2, named_lease(2, 20, now_millis() + 60_000)),
            (3, named_lease(3, 30, now_millis() + 60_000)),
        ]))
        .expect_err("the durable submit refuses");
    assert_eq!(error.kind(), std::io::ErrorKind::StorageFull);
    let status = store.status().expect("the status reads");
    assert_eq!(status.durable.refusals, 1, "the refusal is counted");
    assert_eq!(status.ephemeral.refusals, 0);
    assert_eq!(status.durable.completed_records, 2, "the landed frames");
}
