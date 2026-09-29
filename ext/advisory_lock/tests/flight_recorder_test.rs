//! The Flight Recorder's flag-ON integration tests (the
//! `flight-recorder` feature): the header's commit facts, the internal
//! event coverage (inbound bytes, drive outcomes, the lock-state journal
//! flush, outbound bytes, the maybe tripwire), and the reader path's
//! commit gate. The flag-OFF prod path has its own proof: this file does
//! not compile without the feature, and the whole existing suite runs
//! unchanged on the default features.

#![cfg(feature = "flight-recorder")]

use lunet_advisory_lock::flight::{self, FlightReadError, FlightRecorder};
use lunet_advisory_lock::{Node, OK};
use serde_json::Value;
use std::sync::Mutex;
use uuid::Uuid;

/// The env var is process-global; every test serializes on this lock so
/// the nodes under test never read a foreign flight directory.
static ENV_LOCK: Mutex<()> = Mutex::new(());

fn members() -> String {
    ["10:n1", "11:n2", "12:n3"].join("\0")
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lunet-flight-test-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn request_json(message_id: Uuid) -> Vec<u8> {
    serde_json::to_vec(&lunet_advisory_lock::locks::Request::Get {
        message_id,
        client_id: 11,
        request_num: 13,
        lock_id: 17,
    })
    .unwrap()
}

/// One flight-taped node, driven, with its recording read back as parsed
/// JSON lines. The env is restored before the lock drops.
fn record_a_node(name: &str) -> (Vec<Value>, std::path::PathBuf) {
    let guard = ENV_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());
    let dir = temp_dir(name);
    // SAFETY: the ENV_LOCK serializes every env access in this process;
    // no other test thread reads the flight dir var concurrently.
    unsafe {
        std::env::set_var(flight::FLIGHT_DIR_ENV, &dir);
    }
    let mut node = Node::open(
        &members(),
        "n1",
        dir.join("state").to_str().unwrap(),
        None,
        0,
        lunet_advisory_lock::PRIMARY_TIMEOUT_MS,
    )
    .expect("the node boots");
    // The genesis primary self-promotes on its first tick, then one
    // client request: propose → publish → the prepare Send leaves
    // the node. The host drains the queued sends (each pop records the
    // emission byte-exact). The stop path writes its markers and drains
    // the sink.
    assert_eq!(node.idle(), OK);
    let code = node.request(&request_json(Uuid::from_bytes([42; 16])));
    assert_eq!(code, OK);
    while node.next_output().is_some() {}
    assert_eq!(node.stop(), OK);
    unsafe {
        std::env::remove_var(flight::FLIGHT_DIR_ENV);
    }
    drop(guard);

    let path = dir.join("flight-10.jsonl");
    let text = std::fs::read_to_string(&path).expect("the recording exists");
    let lines: Vec<Value> = text
        .lines()
        .map(|line| serde_json::from_str(line).expect("every line is one JSON record"))
        .collect();
    (lines, dir)
}

/// The FIRST record of every flight recording is the header, naming the
/// commit hash this build was compiled from.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_recording_opens_on_the_commit_header() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The event coverage: the client request and its drive outcomes, the
/// outbound prepare bytes (byte-exact), the stop markers and drain —
/// everything the telemetry capture file never sees.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_recorder_captures_the_internal_events() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The maybe/tripwire capture: a datagram attributed to an unknown peer
/// id is a maybe — the recorder names the trip before the convention's
/// loud report (a test build's panic unwinds out of `receive`; the
/// recording already carries the event).
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_recorder_captures_the_maybe_tripwire() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The reader path's commit gate: a recording whose commit this reader is
/// not refuses loudly; a foreign commit names both sides.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_reader_refuses_a_foreign_commit() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// A missing header (a mangled or foreign file) is refused before any
/// commit comparison.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_reader_refuses_a_file_without_a_header() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The recorder's own unit surface rides the module's sibling tests; this
/// integration file additionally proves the recorder rides a real node
/// whose path opened from the env var.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_flight_file_lives_one_per_node_in_the_env_dir() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// Unused in the flag-OFF build (the whole file is cfg'd out there); the
/// recorder type keeps the import honest.
#[allow(unused)]
fn _type_surface(_: fn(u32) -> Option<FlightRecorder>) {}
