//! The AOF records' roundtrip at the disk boundary: append, the stop
//! drain, and the reopen — the record bytes survive byte-exact, a torn
//! tail truncates to the last complete record, and the metadata window
//! rebuilds over the pre-restart records so the next roll names the full
//! window. Real files in the repo's scratch tree, the writer thread and
//! all.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::aof::{AofConfig, AofWriter, ROLL_BYTES};
use lunet_advisory_lock::journal::{
    self, JournalEvent, KIND_BREAK, KIND_HOLD, KIND_RELEASE, KIND_RENEW, RECORD_SIZE,
};

/// The writer's test knobs: no periodic fsync (the drain's own fsync is
/// the durability proof) and a queue deep enough that every enqueue of a
/// test lands (overflow drops would corrupt the counted assertions).
fn config() -> AofConfig {
    AofConfig {
        flush_interval: None,
        queue_cap: 65_536,
        ..AofConfig::default()
    }
}

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/aof-roundtrip");
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

fn event(kind: u8, ts: u64, lock: u64, expiry: u64) -> JournalEvent {
    JournalEvent {
        kind,
        ts,
        lock_id: lock,
        lease_id: lock * 10,
        holder: [kind; 16],
        expiry,
    }
}

/// The one active (still-appending) file of the series.
fn open_file(dir: &Path) -> PathBuf {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).expect("the series directory reads") {
        let path = entry.expect("the entry reads").path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("ev-open-") {
            found.push(path);
        }
    }
    assert_eq!(found.len(), 1, "exactly one open file");
    found.remove(0)
}

/// The one finalized (rolled) file of the series.
fn finalized_file(dir: &Path) -> PathBuf {
    let mut found = Vec::new();
    for entry in fs::read_dir(dir).expect("the series directory reads") {
        let path = entry.expect("the entry reads").path();
        let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
        if name.starts_with("ev-")
            && !name.starts_with("ev-open-")
            && path.extension().is_some_and(|e| e == "bin")
        {
            found.push(path);
        }
    }
    assert_eq!(found.len(), 1, "exactly one finalized file");
    found.remove(0)
}

#[test]
fn append_drain_reopen_roundtrips_the_record_bytes() {
    let dir = scratch("roundtrip");
    let events = vec![
        event(KIND_HOLD, 1_000, 11, 5_000),
        event(KIND_RENEW, 1_100, 11, 6_000),
        event(KIND_RELEASE, 1_200, 11, 6_000),
        event(KIND_HOLD, 1_300, 12, 7_000),
        event(KIND_BREAK, 1_400, 12, 7_000),
    ];
    let writer = AofWriter::open(&dir, config()).expect("the series opens");
    for one in &events {
        writer.enqueue(one.clone());
    }
    // The stop drain: when it answers, every enqueued record is appended
    // AND fsynced — reading the file back now, with the writer still
    // alive, proves the drain itself landed the bytes.
    writer.drain().expect("the drain lands every queued record");
    assert_eq!(writer.drops(), 0, "no record drops at this depth");
    let open_path = open_file(&dir);
    let bytes = fs::read(&open_path).expect("the open file reads");
    assert_eq!(
        journal::parse_file(&bytes),
        events,
        "the drain landed every record byte-exact"
    );
    drop(writer);

    // The reopen: the resume scan rebuilds the position and the window
    // and keeps every complete record.
    let reopened = AofWriter::open(&dir, config()).expect("the series reopens");
    drop(reopened);
    let bytes = fs::read(&open_path).expect("the open file reads");
    assert_eq!(bytes.len(), events.len() * RECORD_SIZE);
    assert_eq!(
        journal::parse_file(&bytes),
        events,
        "the records survive the reopen byte-exact"
    );
}

#[test]
fn a_torn_tail_truncates_at_the_last_complete_record_on_reopen() {
    let dir = scratch("torn-tail");
    let events = vec![
        event(KIND_HOLD, 2_000, 21, 6_000),
        event(KIND_RENEW, 2_100, 21, 7_000),
        event(KIND_RELEASE, 2_200, 21, 7_000),
    ];
    let writer = AofWriter::open(&dir, config()).expect("the series opens");
    for one in &events {
        writer.enqueue(one.clone());
    }
    writer.drain().expect("the drain lands every queued record");
    drop(writer);

    // The crash shape: a whole zero-filled record-sized tail (the roll
    // pad's bytes) rides past the last complete record. It fails record
    // parsing, and the reopen truncates it away.
    let open_path = open_file(&dir);
    let mut file = fs::OpenOptions::new()
        .append(true)
        .open(&open_path)
        .expect("the open file appends");
    std::io::Write::write_all(&mut file, &[0u8; RECORD_SIZE]).expect("the torn tail appends");
    drop(file);
    assert_eq!(
        fs::metadata(&open_path).expect("the length reads").len(),
        (events.len() as u64 + 1) * RECORD_SIZE as u64,
        "the torn tail is on disk before the reopen"
    );

    let reopened = AofWriter::open(&dir, config()).expect("the series reopens");
    drop(reopened);
    let bytes = fs::read(&open_path).expect("the open file reads");
    assert_eq!(
        bytes.len(),
        events.len() * RECORD_SIZE,
        "the torn tail truncated to the last complete record"
    );
    assert_eq!(
        journal::parse_file(&bytes),
        events,
        "every complete record survived the truncation"
    );
}

#[test]
fn the_window_rebuilds_and_the_roll_names_the_full_window() {
    let dir = scratch("window");
    let rebuilt = vec![
        event(KIND_HOLD, 1_000, 31, 5_000),
        event(KIND_RENEW, 2_000, 31, 6_000),
    ];
    let writer = AofWriter::open(&dir, config()).expect("the series opens");
    for one in &rebuilt {
        writer.enqueue(one.clone());
    }
    writer.drain().expect("the drain lands every queued record");
    drop(writer);

    // The reopen rebuilds the window over the two scanned records; the
    // records that follow push the file past the 2 MiB roll mark and the
    // roll names the FULL window — the rebuilt minimum included.
    let mut stream = Vec::new();
    for index in 0..34_378u64 {
        stream.push(event(KIND_HOLD, 3_000 + index, 40 + index, 8_000));
    }
    let reopened = AofWriter::open(&dir, config()).expect("the series reopens");
    for one in &stream {
        reopened.enqueue(one.clone());
    }
    reopened
        .drain()
        .expect("the drain lands every queued record");
    assert_eq!(reopened.drops(), 0, "no record drops at this depth");
    drop(reopened);

    let finalized = finalized_file(&dir);
    assert_eq!(
        fs::metadata(&finalized).expect("the length reads").len(),
        ROLL_BYTES,
        "every finalized file is exactly one erasure block"
    );
    let finalized_bytes = fs::read(&finalized).expect("the finalized file reads");
    let mut expected = rebuilt.clone();
    // The roll fired at the record that would pass the mark; the records
    // before it (the two rebuilt ones and 34,377 of the stream) are the
    // finalized window, and the last one starts the fresh file.
    expected.extend(stream[..34_377].iter().cloned());
    assert_eq!(
        journal::parse_file(&finalized_bytes),
        expected,
        "the rebuilt records roll with the stream, byte-exact"
    );

    let meta_bytes = fs::read(finalized.with_extension("meta")).expect("the metafile reads");
    let meta = journal::Meta::decode(&meta_bytes).expect("the metafile decodes");
    assert_eq!(meta.count as usize, expected.len());
    assert_eq!(
        meta.op_min, 1_000,
        "the rebuilt minimum survived the reopen"
    );
    assert_eq!(meta.op_max, 3_000 + 34_376);
    assert_eq!(meta.expiry_min, 5_000);
    assert_eq!(meta.expiry_max, 8_000);

    // The fresh open file carries the one record that rolled over.
    let bytes = fs::read(open_file(&dir)).expect("the open file reads");
    assert_eq!(journal::parse_file(&bytes), stream[34_377..]);
}
