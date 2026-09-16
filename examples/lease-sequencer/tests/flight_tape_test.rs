//! The Flight Recorder's tape acceptance (item12): a recording read by
//! the reader path, the commit gate's refusal, the tape lines feeding the
//! SAME playback engine the telemetry tape feeds (`scenario/mod.rs`), and
//! the red/green story pinned.
//!
//! Green shape: the scenario opens one node, the flight-derived tape's
//! `frame_hex` bytes are force-fed in order through `feed_tape`, and the
//! deterministic outcome is asserted — the exact message sequence from
//! the recording, no guessing. Red (kept): the commit gate refuses a
//! recording whose commit this reader is not.

#[path = "scenario/mod.rs"]
mod scenario;

use lease_sequencer::flight_tape::{
    FlightError, FlightTapeOptions, READER_COMMIT, check_commit, read_header, stream_recording,
};
use lease_sequencer::tape::tag_name;
use scenario::{Scenario, TapeFrame, feed_tape, parse_tape_line, tape_frame};
use serde_json::{Value, json};
use std::path::PathBuf;
use vrr::journal::{LogEntry, Payload};
use vrr::message::{Body, Message};
use vrr::wire::{Header, Pack, Tag, Unpack};

/// Writes one recording fixture: a header line plus event lines, the
/// recorder's JSONL shape.
fn write_recording(path: &PathBuf, commit: &str, node: u32, events: &[Value]) {
    use std::io::Write;
    let mut file = std::fs::File::create(path).unwrap();
    writeln!(
        file,
        "{}",
        json!({
            "kind": "flight-header",
            "format": 1,
            "commit": commit,
            "dirty": false,
            "node": node,
            "ts_ms": 1789214915000u64,
        })
    )
    .unwrap();
    for event in events {
        writeln!(file, "{event}").unwrap();
    }
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lease-sequencer-flight-tape-{name}-{}-{}",
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

/// One real uVRR Prepare frame (era-1 genesis view, slot 1, a valid lock
/// verb payload), packed with the core's own wire encoder: the operation
/// id derives from the message_id exactly as the adapter derives it (the
/// peer-carried payload gate requires the match).
fn prepare_frame() -> Vec<u8> {
    let message_id = uuid::Uuid::from_bytes([7; 16]);
    let id_bytes = message_id.as_bytes();
    let operation_id = vrr::ids::OperationId {
        msb: u64::from_be_bytes(id_bytes[..8].try_into().unwrap()),
        lsb: u64::from_be_bytes(id_bytes[8..].try_into().unwrap()),
    };
    let message = Message {
        header: Header {
            tag: Tag::Prepare,
            view: vrr::ids::ViewId {
                era: vrr::ids::Era(1),
                view: vrr::ids::View(0),
            },
            slot: vrr::ids::Slot(1),
        },
        body: Body::Prepare {
            entry: LogEntry {
                slot: vrr::ids::Slot(1),
                era: vrr::ids::Era(1),
                payload: Payload::Operation {
                    id: operation_id,
                    payload: serde_json::to_vec(&lunet_advisory_lock::locks::Request::Get {
                        message_id,
                        client_id: 11,
                        request_num: 13,
                        lock_id: 17,
                    })
                    .unwrap()
                    .into(),
                },
            },
            committed: vrr::ids::Slot(0),
        },
    };
    let mut bytes = vec![0u8; message.packed_len()];
    message.pack_into(&mut bytes).expect("the frame packs");
    bytes
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

/// The recording header parses and the commit gate passes for this
/// reader's own commit — and refuses a foreign one loudly.
#[test]
fn the_reader_gate_passes_this_build_and_refuses_a_foreign_commit() {
    let dir = temp_dir("gate");
    let recording = dir.join("flight-44.jsonl");
    write_recording(&recording, READER_COMMIT, 44, &[]);
    let header = read_header(&recording).expect("the header reads");
    assert_eq!(header.node, 44);
    assert_eq!(header.commit, READER_COMMIT);
    check_commit(&header, READER_COMMIT).expect("this reader is this build");

    let foreign = dir.join("flight-foreign.jsonl");
    write_recording(
        &foreign,
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
        44,
        &[],
    );
    let header = read_header(&foreign).expect("the header reads");
    let error = check_commit(&header, READER_COMMIT).expect_err("a foreign commit refuses");
    assert!(matches!(error, FlightError::CommitMismatch { .. }));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The default extraction keeps only the wire kinds and renders the
/// scenario engine's `from,to,{json}` shape: the inbound sender is the
/// host's attribution (exact), the emission names its target.
#[test]
fn the_extraction_renders_the_playback_surface() {
    let dir = temp_dir("extraction");
    let recording = dir.join("flight-44.jsonl");
    let frame = prepare_frame();
    write_recording(
        &recording,
        READER_COMMIT,
        44,
        &[
            json!({"seq": 1, "kind": "receive-in", "ts_ms": 1789214915001u64,
                   "detail": {"from": 66, "len": frame.len(), "hex": hex(&frame)}}),
            json!({"seq": 2, "kind": "drive-out", "ts_ms": 1789214915002u64,
                   "detail": {"code": 0}}),
            json!({"seq": 3, "kind": "emit", "ts_ms": 1789214915003u64,
                   "detail": {"kind": 1, "to": 66, "era": 1, "view": 0, "slot": 1,
                              "len": 3, "hex": "aabb00"}}),
        ],
    );
    let mut capture: Vec<u8> = Vec::new();
    let (lines, mangled) =
        stream_recording(&recording, &FlightTapeOptions::default(), &mut capture)
            .expect("the recording streams");
    assert_eq!(mangled, 0);
    let text = String::from_utf8(capture).unwrap();
    let tape: Vec<&str> = text.lines().collect();
    assert_eq!(lines, 2, "the wire kinds only, by default: {text:?}");
    let (from, to, json) = parse_tape_line(tape[0]).expect("the line parses");
    assert_eq!((from.as_str(), to.as_str()), ("66", "44"));
    assert_eq!(json["kind"], "receive-in");
    assert_eq!(json["frame_hex"], hex(&frame));
    let (from, to, json) = parse_tape_line(tape[1]).expect("the line parses");
    assert_eq!((from.as_str(), to.as_str()), ("44", "66"));
    assert_eq!(json["kind"], "emit");
    assert_eq!(json["frame_hex"], "aabb00");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The internal kinds ride `--kinds internal` — the node's private story
/// (drives, faults, journal flushes, markers) is the reader's surface on
/// request, never by default.
#[test]
fn the_internal_kinds_are_explicit_only() {
    let dir = temp_dir("internal");
    let recording = dir.join("flight-44.jsonl");
    write_recording(
        &recording,
        READER_COMMIT,
        44,
        &[
            json!({"seq": 1, "kind": "drive-out", "ts_ms": 1789214915001u64,
                   "detail": {"code": 0}}),
            json!({"seq": 2, "kind": "fault", "ts_ms": 1789214915002u64,
                   "detail": {"what": "panic: tick regression", "arrest": true}}),
            json!({"seq": 3, "kind": "journal", "ts_ms": 1789214915003u64,
                   "detail": {"what": "internal lock-state flush", "kind": 1, "lock_id": 17}}),
        ],
    );
    let mut default_capture: Vec<u8> = Vec::new();
    let (lines, _) = stream_recording(
        &recording,
        &FlightTapeOptions::default(),
        &mut default_capture,
    )
    .expect("the recording streams");
    assert_eq!(lines, 0, "the internal events are not the default surface");
    let mut options = FlightTapeOptions::default();
    options.kinds = vec!["internal".to_string()];
    let mut capture: Vec<u8> = Vec::new();
    let (lines, _) = stream_recording(&recording, &options, &mut capture).expect("streams");
    assert_eq!(lines, 3, "every internal event on request");
    let text = String::from_utf8(capture).unwrap();
    assert!(text.contains("panic: tick regression"));
    assert!(text.contains("internal lock-state flush"));
    let _ = std::fs::remove_dir_all(&dir);
}

/// END-TO-END through the SAME playback engine the telemetry tape feeds:
/// the scenario opens the node, the flight-derived tape's frames are
/// force-fed in order through `feed_tape`, and the deterministic outcome
/// is asserted. This is the exact message-sequence extraction → unit
/// test force-feeding a node, the corfu inverse-paste shape.
#[test]
fn given_a_scenario_the_flight_tape_feeds_the_playback_engine() {
    let dir = temp_dir("engine");
    let recording = dir.join("flight-12.jsonl");
    let frame = prepare_frame();
    write_recording(
        &recording,
        READER_COMMIT,
        12,
        &[
            json!({"seq": 1, "kind": "receive-in", "ts_ms": 1789214915001u64,
                   "detail": {"from": 10, "len": frame.len(), "hex": hex(&frame)}}),
        ],
    );
    let mut capture: Vec<u8> = Vec::new();
    let mut options = FlightTapeOptions::default();
    options.node = Some(12);
    stream_recording(&recording, &options, &mut capture).expect("the recording streams");

    // The scenario is the node's initial condition; the tape is the
    // filtered-to-one-node message sequence.
    let scenario = Scenario::parse(
        r#"{
          "node_id": 12,
          "name": "n3",
          "membership": [
            {"id": 10, "name": "n1", "weight": 1},
            {"id": 11, "name": "n2", "weight": 1},
            {"id": 12, "name": "n3", "weight": 1}
          ],
          "era": 1,
          "view": 0
        }"#,
    )
    .expect("the scenario parses");
    let mut node = scenario.open_node(&dir).expect("the scenario node boots");
    let frames: Vec<scenario::TapeFrame> = String::from_utf8(capture)
        .unwrap()
        .lines()
        .filter_map(|line| {
            let (from, _to, json) = parse_tape_line(line)?;
            assert_eq!(json["kind"], "receive-in");
            tape_frame(from, json)
        })
        .collect();
    assert_eq!(frames.len(), 1, "the flight tape's frame force-feeds");
    let decoded = &frames[0];
    assert_eq!(decoded.tag, 2, "the Prepare tag decodes");
    assert_eq!(tag_name(decoded.tag), "prepare");
    let result = feed_tape(&mut node, &frames);
    assert_eq!(result.fed, 1);
    assert_eq!(
        result.skipped_no_sender, 0,
        "the flight tape attributes every frame"
    );
    assert_eq!(
        result.codes.get(&0).copied().unwrap_or(0),
        1,
        "the era-1 genesis frame digests with OK: {:?}",
        result.codes
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The commit gate runs BEFORE any extraction: a foreign-commit recording
/// streams nothing.
#[test]
fn the_gate_refuses_before_any_extraction() {
    let dir = temp_dir("gate-first");
    let recording = dir.join("flight-44.jsonl");
    write_recording(
        &recording,
        "cccccccccccccccccccccccccccccccccccccccc",
        44,
        &[],
    );
    let mut capture: Vec<u8> = Vec::new();
    let error = stream_recording(&recording, &FlightTapeOptions::default(), &mut capture)
        .expect_err("the gate refuses first");
    assert!(matches!(error, FlightError::CommitMismatch { .. }));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Silence the unused-import warning when a helper is only used by one
/// test above.
#[allow(unused)]
fn _type_surface(_: fn(u32) -> Value) {}
