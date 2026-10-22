//! The ui-tape's visibility contract: a corrupt record names itself (the
//! file, the record index and the TRUE byte offset) and crashes the
//! bridge; an incomplete final record on the series' last file is
//! not-yet-written weather the replay tolerates; and a lease that was
//! continuously renewed survives the replay's expiry classification on
//! the record's own timeline.
//!
//! The fixtures drive the REAL pipeline: the tbio writer (the same Zig
//! append path the UI server's telemetry log drives) frames every record
//! into a checksum-chained Prepare entry, and the bridge reads the
//! cassettes back through the checksum-validating iterator. A tbio entry
//! on disk is the 16-byte magic number, the 256-byte VSR Prepare header,
//! then the record payload — so a 70-byte envelope record (the 9-byte
//! envelope around the 61-byte `LKE1` payload) rides a 342-byte entry
//! stride, and the second record starts at byte 342.

use lunet_advisory_lock::journal::{JournalEvent, KIND_HOLD};
use lunet_advisory_lock::locks::{LeaseCandidate, Request};
use std::io::{Read as _, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};
use tbio::envelope::{Marker, Record};
use uuid::Uuid;
use vrr::ids::{Ballot, Era, OperationId, Slot, View};
use vrr::journal::{LogEntry, Payload};
use vrr::message::{Body, Message};
use vrr::wire::{Pack, Tag};

const CASSETTE: &str = "1792587872833.aof";
/// The on-disk entry stride for a 70-byte envelope record:
/// 16 (the magic number) + 256 (the VSR Prepare header) + 70.
const STRIDE_70: u64 = 16 + 256 + 70;

fn scratch(name: &str) -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp/ui-tape-visibility")
        .join(format!(
            "{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map_or(0, |d| d.as_nanos())
        ));
    std::fs::create_dir_all(&dir).expect("the scratch dir creates");
    dir
}

fn event(lock_id: u64) -> JournalEvent {
    JournalEvent {
        kind: KIND_HOLD,
        ts: 1_000,
        lock_id,
        lease_id: 42,
        holder: *b"test-holder-0001",
        expiry: 9_000,
    }
}

/// One 70-byte envelope record around the 61-byte LKE1 payload.
fn envelope_record(ns: u64) -> Vec<u8> {
    Record {
        marker: Marker::TelemetryStateTransition,
        ns,
        payload: event(100).encode().to_vec(),
    }
    .encode()
}

/// Append the records through the real writer: the same Zig append path
/// the UI server's telemetry log drives (the checksum chain, the Prepare
/// headers, the magic numbers are the writer's product, never the test's).
fn append_records(path: &Path, records: &[Vec<u8>]) {
    let mut file = unsafe { tbio::ffi::RawFile::open(path.as_os_str().as_encoded_bytes(), false) }
        .expect("the cassette opens through the real AOF writer");
    for record in records {
        file.append(record).expect("the record appends");
    }
    file.close().expect("the cassette closes");
}

fn write_cassette(dir: &Path, name: &str, events: usize) {
    let records: Vec<Vec<u8>> = (0..events)
        .map(|index| envelope_record(1_000 + index as u64))
        .collect();
    append_records(&dir.join(name), &records);
}

/// The bridge child's outcome: died by SIGABRT naming the corruption, or
/// the stderr that says otherwise.
fn aborting_bridge_stderr(dir: &Path) -> String {
    let outcome = Command::new(env!("CARGO_BIN_EXE_aof-console-bridge"))
        .arg("--aof-dir")
        .arg(dir)
        .arg("--bind")
        .arg("127.0.0.1:0")
        .stderr(Stdio::piped())
        .stdout(Stdio::null())
        .output()
        .expect("the bridge child spawns");
    use std::os::unix::process::ExitStatusExt as _;
    assert_eq!(
        outcome.status.signal(),
        Some(6),
        "the bridge must die by SIGABRT, got {outcome:?}"
    );
    String::from_utf8_lossy(&outcome.stderr).into_owned()
}

#[test]
fn a_corrupt_record_names_its_file_and_offset_and_aborts_the_bridge() {
    let dir = scratch("corrupt");
    write_cassette(&dir, CASSETTE, 5);
    let cassette = dir.join(CASSETTE);
    // The real writer's entry geometry: 342 bytes a record (the 16-byte
    // magic number, the 256-byte Prepare header, the 70-byte envelope).
    // The SECOND record starts at byte 342, and its header checksum rides
    // at frame bytes 342..358 — the first field the iterator validates.
    // Damaging it fails the validation on a complete, in-chain frame: the
    // corrupt record, not an incomplete tail.
    let mut tape = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cassette)
        .unwrap();
    use std::io::Seek as _;
    tape.seek(std::io::SeekFrom::Start(STRIDE_70)).unwrap();
    tape.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    drop(tape);

    let stderr = aborting_bridge_stderr(&dir);
    assert!(
        stderr.contains(CASSETTE),
        "the abort names the file: {stderr}"
    );
    assert!(
        stderr.contains("record 1 at"),
        "the abort names the record index: {stderr}"
    );
    assert!(
        stderr.contains("offset 342"),
        "the abort names the TRUE byte offset: {stderr}"
    );
}

#[test]
fn a_writer_cassette_replays_every_record() {
    let dir = scratch("replay");
    write_cassette(&dir, CASSETTE, 5);
    let (_, _, metrics) = lease_sequencer::bridge::replay_series(&dir);
    assert_eq!(metrics.records, 5, "{metrics:?}");
    assert_eq!(metrics.undecodable, 0, "{metrics:?}");
}

#[test]
fn an_incomplete_final_record_replays_the_good_prefix() {
    // The series' last (active) file's final record whose bytes end
    // mid-envelope — the not-yet-written weather: the replay reads up to
    // the last good record and the undecodable metric states the rest.
    // (All records ride ONE writer handle: the checksum chain runs
    // through the handle, and a reopened file's fresh chain is the
    // iterator's chain-mismatch corruption.)
    let dir = scratch("torn-envelope");
    append_records(
        &dir.join(CASSETTE),
        &[
            envelope_record(1_000),
            envelope_record(1_001),
            vec![0x03, 0x00, 0x00],
        ],
    );
    let (_, _, metrics) = lease_sequencer::bridge::replay_series(&dir);
    assert_eq!(metrics.records, 2, "{metrics:?}");
    assert_eq!(metrics.undecodable, 1, "{metrics:?}");
}

#[test]
fn a_truncated_entry_tail_replays_the_good_prefix_and_waits() {
    // The writer died mid-entry (the fsync cadence is 1 s): the file ends
    // before the record's framing completes. The iterator's torn-tail
    // read ends the iteration at the last good record — the follow loop
    // picks the file up again when the writer extents grow.
    let dir = scratch("torn-entry");
    write_cassette(&dir, CASSETTE, 2);
    let cassette = dir.join(CASSETTE);
    let mut tape = std::fs::OpenOptions::new()
        .append(true)
        .open(&cassette)
        .unwrap();
    // Half of the next envelope record: the entry's framing never
    // completes, so the bytes end mid-record.
    tape.write_all(&envelope_record(9_999)[..35]).unwrap();
    drop(tape);
    let (_, _, metrics) = lease_sequencer::bridge::replay_series(&dir);
    assert_eq!(metrics.records, 2, "{metrics:?}");
    assert_eq!(metrics.undecodable, 0, "{metrics:?}");
}

#[test]
fn a_mid_file_decode_failure_aborts_the_bridge() {
    // A checksum-valid entry the typed decode refuses, with another
    // record after it: a mid-file failure is a corrupt record, never
    // weather — the same named abort as an iterator error.
    let dir = scratch("mid-file");
    append_records(
        &dir.join(CASSETTE),
        &[vec![0x03, 0x00, 0x00], envelope_record(2_000)],
    );
    let stderr = aborting_bridge_stderr(&dir);
    assert!(
        stderr.contains(CASSETTE),
        "the abort names the file: {stderr}"
    );
    assert!(
        stderr.contains("record 0 at"),
        "the abort names the record index: {stderr}"
    );
    assert!(
        stderr.contains("offset 0"),
        "the abort names the byte offset: {stderr}"
    );
}

#[test]
fn a_final_record_in_a_finalized_predecessor_aborts_the_bridge() {
    // The tolerance is the series' LAST file's alone: a finalized
    // predecessor's incomplete final record cannot grow back — the same
    // named abort as any other corrupt record.
    let dir = scratch("predecessor");
    append_records(&dir.join("1792587872830.aof"), &[vec![0x03, 0x00, 0x00]]);
    let records: Vec<Vec<u8>> = (0..1)
        .map(|index| envelope_record(1_000 + index as u64))
        .collect();
    append_records(&dir.join(CASSETTE), &records);
    let stderr = aborting_bridge_stderr(&dir);
    assert!(
        stderr.contains("1792587872830.aof"),
        "the abort names the corrupt predecessor: {stderr}"
    );
}

/// A genuine uVRR Prepare frame carrying one lock request, wrapped in the
/// envelope record the telemetry log appends. The request's message id is
/// the operation id the bridge matches it against.
fn wire_record(ns: u64, request: &Request, op: OperationId) -> Vec<u8> {
    let payload = serde_json::to_vec(request).expect("the request serializes");
    let entry = LogEntry {
        slot: Slot(1),
        era: Era(1),
        payload: Payload::Operation {
            id: op,
            payload: payload.into_boxed_slice(),
        },
    };
    let message = Message {
        header: vrr::wire::Header {
            tag: Tag::Prepare,
            view: Ballot {
                era: Era(1),
                view: View(1),
            },
            slot: Slot(1),
        },
        body: Body::Prepare {
            entry,
            committed: Slot(0),
        },
    };
    let mut buf = vec![0u8; message.packed_len()];
    let written = message.pack_into(&mut buf).expect("the frame packs");
    assert_eq!(written, buf.len(), "pack_into writes exactly packed_len");
    Record::wire(ns, &buf).encode()
}

fn operation_id(high: u64) -> OperationId {
    OperationId {
        msb: 0x7665_6374_0000_0000 + high,
        lsb: 0x0000_0000_0000_0042,
    }
}

fn uuid_of(op: OperationId) -> Uuid {
    Uuid::from_bytes(
        [
            op.msb.to_be_bytes().as_slice(),
            op.lsb.to_be_bytes().as_slice(),
        ]
        .concat()
        .try_into()
        .expect("16 bytes"),
    )
}

/// The demo's shape, synthetic: a lease acquired, then renewed well
/// inside its window, the renewal trail crossing the cassettes' boundary
/// (the acquire in the older cassette, the renew in the series' last
/// file). The replay's clock for each record is the record's own commit
/// time, so the continuously renewed lease survives the replay — the
/// console's lock view classifies it on the record's timeline, never on
/// the serve-time wall clock whose lag behind the tape culled fresh
/// leases in the live demo.
#[test]
fn a_continuously_renewed_lease_survives_the_replay() {
    let dir = scratch("renewed-lease");
    const LOCK: u64 = 55;
    const T0_MS: u64 = 1_792_431_782_539;
    const LEASE_MS: u64 = 5_000;
    let holder = Uuid::from_bytes(*b"renewed-holdr-01");

    let acquire = Request::Set {
        message_id: uuid_of(operation_id(1)),
        client_id: 7,
        request_num: 1,
        lock_id: LOCK,
        lease: LeaseCandidate {
            lease_id: 0,
            holder,
            lease_ms: LEASE_MS,
        },
        name: Some("/lock/55".to_string()),
        labels: None,
        sent_at_ms: None,
    };
    let renew = Request::Set {
        message_id: uuid_of(operation_id(2)),
        client_id: 7,
        request_num: 2,
        lock_id: LOCK,
        lease: LeaseCandidate {
            lease_id: 1,
            holder,
            lease_ms: LEASE_MS,
        },
        name: None,
        labels: None,
        sent_at_ms: None,
    };
    // The acquire in the older cassette; the renew in the series' last
    // file, two and a half seconds in — well inside the 5 s window.
    append_records(
        &dir.join("1792587872830.aof"),
        &[wire_record(T0_MS * 1_000_000, &acquire, operation_id(1))],
    );
    append_records(
        &dir.join(CASSETTE),
        &[wire_record(
            (T0_MS + 2_500) * 1_000_000,
            &renew,
            operation_id(2),
        )],
    );

    let (events, state, _) = lease_sequencer::bridge::replay_series(&dir);
    let kinds: Vec<&str> = events.iter().map(|event| event.kind.as_str()).collect();
    assert_eq!(kinds, ["acquire", "renew"], "{events:?}");
    let lock = state.locks.get(&LOCK).expect("the lease survives");
    assert_eq!(lock.fencing_token, 1, "the renewal keeps the generation");
    assert_eq!(lock.renew_count, 1, "{lock:?}");
    assert_eq!(lock.expires_at_ms, Some(T0_MS + 2_500 + LEASE_MS));
    // The record's timeline classifies the lease held: at the replay's
    // frontier (the renew's commit time) the lease is live, whatever the
    // serve-time wall clock reads — the wall clock of a pass long after
    // the tape (today, against the demo's tapes) is exactly what culled
    // the demo's fresh leases, so the bridge must never classify by it.
    // The console's surface proves the wiring: /locks serves the
    // classification on the record's timeline.
    let server = lease_sequencer::bridge::Server::spawn(&dir, None, "127.0.0.1:0", false)
        .expect("the bridge server spawns");
    let mut stream =
        std::net::TcpStream::connect(("127.0.0.1", server.port())).expect("the bridge connects");
    stream
        .write_all(b"GET /api/v1/locks HTTP/1.1\r\nHost: bridge\r\n\r\n")
        .expect("the request writes");
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .expect("the response reads");
    let body = response
        .split("\r\n\r\n")
        .nth(1)
        .expect("the response carries a body");
    let body: serde_json::Value = serde_json::from_str(body).expect("the body is json");
    assert_eq!(body["locks"][0]["state"], "held", "{body}");
    assert_eq!(body["locks"][0]["renewCount"], 1, "{body}");
    server.shutdown();
}

#[test]
fn the_inventory_names_the_cassettes_on_disk() {
    let dir = scratch("inventory");
    write_cassette(&dir, "1792587872830.aof", 5);
    write_cassette(&dir, CASSETTE, 5);
    std::fs::File::create(dir.join("ev-open-1792587873530.bin")).unwrap();
    let inventory = lease_sequencer::bridge::series_inventory(&dir);
    assert!(
        inventory.contains(&dir.display().to_string()),
        "{inventory}"
    );
    assert!(
        inventory.contains("2 finalized cassettes on disk"),
        "{inventory}"
    );
    // The real writer's stride: 5 records at 342 bytes a record.
    let size = 5 * STRIDE_70;
    for name in ["1792587872830.aof", CASSETTE] {
        let path = dir.join(name);
        assert!(
            inventory.contains(&path.display().to_string()),
            "the inventory names {name}: {inventory}"
        );
        assert!(
            inventory.contains(&format!("{} ({size} bytes, finalized)", path.display())),
            "each finalized cassette names its path and size: {inventory}"
        );
    }
    assert!(inventory.contains("active file"), "{inventory}");
    assert!(inventory.contains("not yet finalized"), "{inventory}");
}

#[test]
fn an_empty_series_inventories_as_no_cassettes() {
    let dir = scratch("empty");
    let inventory = lease_sequencer::bridge::series_inventory(&dir);
    assert!(inventory.contains("no cassettes on disk"), "{inventory}");
}
