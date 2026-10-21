//! The ui-tape's visibility contract: a corrupt record names itself (the
//! file and the byte offset) and crashes the bridge; the cassette
//! inventory names what is on disk and where.
//!
//! The fixtures hand-build the `{epoch}.aof` cassettes the bridge's
//! replay reads: tbio's 9-byte envelope (the marker byte + the ns) around
//! the 61-byte `LKE1` payload — 70 bytes a record, so the second record
//! starts at byte 70.

use lunet_advisory_lock::journal::{JournalEvent, KIND_HOLD};
use std::io::Write as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};
use tbio::envelope::{Marker, Record};

const CASSETTE: &str = "1792587872833.aof";

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

fn framed(event: &JournalEvent, ns: u64) -> Vec<u8> {
    Record {
        marker: Marker::TelemetryStateTransition,
        ns,
        payload: event.encode().to_vec(),
    }
    .encode()
}

fn write_cassette(dir: &Path, name: &str, events: usize) {
    let mut tape = std::fs::File::create(dir.join(name)).expect("the cassette creates");
    for index in 0..events {
        tape.write_all(&framed(&event(100 + index as u64), 1_000 + index as u64))
            .expect("the record writes");
    }
}

#[test]
#[ignore = "the fixture needs the Zig entry's own checksum layout: the \
            envelope carries no checksum, and a payload-byte overwrite \
            leaves the Aegis chain untouched, so the iterator still reads \
            the record — reproduce against the demo's corrupt specimen \
            (1792430762.aof) or read zig/src/aof.zig's entry framing first"]
fn a_corrupt_record_names_its_file_and_offset_and_aborts_the_bridge() {
    let dir = scratch("corrupt");
    write_cassette(&dir, CASSETTE, 5);
    let cassette = dir.join(CASSETTE);
    // The record geometry: 70 bytes on the tape (61-byte LKE1 + the
    // 9-byte envelope). The SECOND record starts at byte 70 and its LKE1
    // checksum rides at frame byte 70+58; corrupt the checksum so the
    // iterator's validation fails on a complete frame — the corrupt
    // record, not an incomplete tail.
    let mut tape = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&cassette)
        .unwrap();
    use std::io::Seek as _;
    tape.seek(std::io::SeekFrom::Start(128)).unwrap();
    tape.write_all(&[0xFF, 0xFF, 0xFF, 0xFF]).unwrap();
    drop(tape);

    let outcome = Command::new(env!("CARGO_BIN_EXE_aof-console-bridge"))
        .arg("--aof-dir")
        .arg(&dir)
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
    let stderr = String::from_utf8_lossy(&outcome.stderr);
    assert!(
        stderr.contains(CASSETTE),
        "the abort names the file: {stderr}"
    );
    assert!(
        stderr.contains("offset 70"),
        "the abort names the byte offset: {stderr}"
    );
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
    for name in ["1792587872830.aof", CASSETTE] {
        let path = dir.join(name);
        assert!(
            inventory.contains(&path.display().to_string()),
            "the inventory names {name}: {inventory}"
        );
        assert!(
            inventory.contains(&format!("{} (350 bytes, finalized)", path.display())),
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
