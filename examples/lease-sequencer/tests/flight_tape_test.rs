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
use scenario::{Scenario, feed_tape, parse_tape_line, tape_frame};
use serde_json::{Value, json};
use std::path::PathBuf;
use vrr::journal::{LogEntry, Payload};
use vrr::message::{Body, Message};
use vrr::wire::{Header, Pack, Tag};

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
    let options = FlightTapeOptions {
        kinds: vec!["internal".to_string()],
        ..Default::default()
    };
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
    let recording = dir.join("flight-786433.jsonl");
    let frame = prepare_frame();
    write_recording(
        &recording,
        READER_COMMIT,
        786433,
        &[
            json!({"seq": 1, "kind": "receive-in", "ts_ms": 1789214915001u64,
                   "detail": {"from": 655361, "len": frame.len(), "hex": hex(&frame)}}),
        ],
    );
    let mut capture: Vec<u8> = Vec::new();
    let options = FlightTapeOptions {
        node: Some(786433),
        ..Default::default()
    };
    stream_recording(&recording, &options, &mut capture).expect("the recording streams");

    // The scenario is the node's initial condition; the tape is the
    // filtered-to-one-node message sequence.
    let scenario = Scenario::parse(
        r#"{
          "node_id": 786433,
          "name": "n3",
          "membership": [
            {"id": 655361, "name": "n1", "weight": 1},
            {"id": 720897, "name": "n2", "weight": 1},
            {"id": 786433, "name": "n3", "weight": 1}
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

/// The commit gate runs before any DEEP extraction: a foreign-commit
/// recording refuses the deep read — `--deep` and every kind beyond
/// `wire` alike — naming both sides.
#[test]
fn the_deep_read_refuses_a_foreign_commit_before_any_extraction() {
    let dir = temp_dir("gate-first");
    let recording = dir.join("flight-44.jsonl");
    write_recording(
        &recording,
        "cccccccccccccccccccccccccccccccccccccccc",
        44,
        &[
            json!({"seq": 1, "kind": "drive-out", "ts_ms": 1789214915001u64,
                 "detail": {"code": 0}}),
        ],
    );
    let options = FlightTapeOptions {
        kinds: vec!["internal".to_string()],
        ..Default::default()
    };
    let mut capture: Vec<u8> = Vec::new();
    let error = stream_recording(&recording, &options, &mut capture)
        .expect_err("the deep read refuses first");
    assert!(matches!(error, FlightError::CommitMismatch { .. }));
    let options = FlightTapeOptions {
        deep: true,
        ..Default::default()
    };
    let error =
        stream_recording(&recording, &options, &mut Vec::new()).expect_err("--deep is a deep read");
    assert!(matches!(error, FlightError::CommitMismatch { .. }));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The stable `from,to,jsonl` slice is offered cross-commit: the wire
/// kinds of a foreign-commit recording still stream (best-effort —
/// mangled lines are counted, never guessed), while the deep read of the
/// same recording refuses.
#[test]
fn the_stable_slice_streams_cross_commit_and_the_deep_read_does_not() {
    let dir = temp_dir("cross-commit-slice");
    let recording = dir.join("flight-44.jsonl");
    let frame = prepare_frame();
    write_recording(
        &recording,
        "dddddddddddddddddddddddddddddddddddddddd",
        44,
        &[
            json!({"seq": 1, "kind": "receive-in", "ts_ms": 1789214915001u64,
                   "detail": {"from": 66, "len": frame.len(), "hex": hex(&frame)}}),
            json!({"seq": 2, "kind": "emit", "ts_ms": 1789214915002u64,
                   "detail": {"kind": 1, "to": 66, "era": 1, "view": 0, "slot": 1,
                              "len": 3, "hex": "aabb00"}}),
        ],
    );
    let mut capture: Vec<u8> = Vec::new();
    let (lines, mangled) =
        stream_recording(&recording, &FlightTapeOptions::default(), &mut capture)
            .expect("the slice streams cross-commit");
    assert_eq!(mangled, 0);
    assert_eq!(lines, 2, "both wire lines, cross-commit");
    let text = String::from_utf8(capture).unwrap();
    assert!(text.starts_with("66,44,"), "the CSV shape: {text:?}");
    assert!(text.contains("44,66,"), "the emit's rendered endpoints");

    let options = FlightTapeOptions {
        kinds: vec!["internal".to_string()],
        ..Default::default()
    };
    let error = stream_recording(&recording, &options, &mut Vec::new())
        .expect_err("the deep read of a foreign commit refuses");
    assert!(matches!(error, FlightError::CommitMismatch { .. }));
    let _ = std::fs::remove_dir_all(&dir);
}

/// The operator's law on the replay: the tape keeps the raw numbers the
/// recorder captured, and the extraction spells every name a human would
/// otherwise have to look up — the emit's output kind, the journal
/// flush's kind, and the node's replication state where an event carries
/// one (the marker events' raw codes).
#[test]
fn the_extraction_stringifies_the_names_beside_the_raw_numbers() {
    let dir = temp_dir("stringify");
    let recording = dir.join("flight-44.jsonl");
    write_recording(
        &recording,
        READER_COMMIT,
        44,
        &[
            json!({"seq": 1, "kind": "marker", "ts_ms": 1789214915001u64,
                   "detail": {"what": "the halt's first round (Stopping) begins",
                              "identity": 42, "state": 1}}),
            json!({"seq": 2, "kind": "emit", "ts_ms": 1789214915002u64,
                   "detail": {"kind": 1, "to": 66, "era": 1, "view": 0, "slot": 1,
                              "len": 3, "hex": "aabb00"}}),
            json!({"seq": 3, "kind": "journal", "ts_ms": 1789214915003u64,
                   "detail": {"what": "internal lock-state flush", "kind": 2,
                              "lock_id": 17}}),
            json!({"seq": 4, "kind": "marker", "ts_ms": 1789214915004u64,
                   "detail": {"what": "the drain-proven second round (Stopped) begins",
                              "identity": 42, "state": 2}}),
            json!({"seq": 5, "kind": "status", "ts_ms": 1789214915005u64,
                   "detail": {"state": 0, "leader": 10}}),
        ],
    );
    let options = FlightTapeOptions {
        kinds: vec!["internal".to_string(), "emit".to_string()],
        ..Default::default()
    };
    let mut capture: Vec<u8> = Vec::new();
    let (lines, _) = stream_recording(&recording, &options, &mut capture).expect("streams");
    assert_eq!(lines, 5);
    let tape: Vec<Value> = String::from_utf8(capture)
        .unwrap()
        .lines()
        .map(|line| parse_tape_line(line).expect("the line parses").2)
        .collect();

    // The marker events: the raw codes stay, the names ride beside them —
    // the MARKER lifecycle namespace (unflushed/stopped/flushed), not the
    // replication-state words.
    assert_eq!(tape[0]["state"], 1, "the tape keeps the raw number");
    assert_eq!(
        tape[0]["state_name"], "stopped",
        "the extraction spells the name"
    );
    assert_eq!(tape[3]["state"], 2);
    assert_eq!(tape[3]["state_name"], "flushed");
    // The status event: the raw word stays, the name rides beside it —
    // the REPLICATION state namespace (normal/view_change/…).
    assert_eq!(tape[4]["state"], 0);
    assert_eq!(tape[4]["state_name"], "normal");
    // The emit: the raw output kind stays, the name rides beside it.
    assert_eq!(tape[1]["out_kind"], 1, "the tape keeps the raw number");
    assert_eq!(tape[1]["out_kind_name"], "send");
    // The journal flush: the raw kind stays, the name rides beside it.
    assert_eq!(tape[2]["out_kind"], 2, "the tape keeps the raw number");
    assert_eq!(tape[2]["out_kind_name"], "renew");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The deep read on the SAME commit: the full internal event log — every
/// event rendered with its `seq`, the internal story included, and the
/// default read of the same recording staying the slice only.
#[test]
fn the_deep_read_streams_the_full_internal_log_on_the_same_commit() {
    let dir = temp_dir("deep");
    let recording = dir.join("flight-44.jsonl");
    write_recording(
        &recording,
        READER_COMMIT,
        44,
        &[
            json!({"seq": 1, "kind": "receive-in", "ts_ms": 1789214915001u64,
                   "detail": {"from": 66, "len": 5, "hex": "aabb00ccdd"}}),
            json!({"seq": 2, "kind": "drive-out", "ts_ms": 1789214915002u64,
                   "detail": {"code": 0}}),
            json!({"seq": 3, "kind": "fault", "ts_ms": 1789214915003u64,
                   "detail": {"what": "panic: tick regression", "arrest": true}}),
            json!({"seq": 4, "kind": "journal", "ts_ms": 1789214915004u64,
                   "detail": {"what": "internal lock-state flush", "kind": 1,
                              "lock_id": 17}}),
        ],
    );
    let options = FlightTapeOptions {
        deep: true,
        ..Default::default()
    };
    let mut capture: Vec<u8> = Vec::new();
    let (lines, mangled) = stream_recording(&recording, &options, &mut capture)
        .expect("the deep read streams on the same commit");
    assert_eq!(mangled, 0);
    assert_eq!(lines, 4, "every event, wire and internal");
    let tape: Vec<(String, String, Value)> = String::from_utf8(capture)
        .unwrap()
        .lines()
        .map(|line| parse_tape_line(line).expect("the line parses"))
        .collect();
    for (index, (from, _to, json)) in tape.iter().enumerate() {
        assert_eq!(json["seq"], (index + 1) as u64, "the deep read stamps seq");
        assert_eq!(
            json["kind"],
            ["receive-in", "drive-out", "fault", "journal"][index]
        );
        assert_eq!(
            *from,
            if index == 0 { "66" } else { "?" },
            "the record's own 'from' renders, else '?'"
        );
    }
    assert_eq!(tape[2].2["what"], "panic: tick regression");
    assert_eq!(tape[3].2["lock_id"], 17);

    let mut slice: Vec<u8> = Vec::new();
    let (lines, _) = stream_recording(&recording, &FlightTapeOptions::default(), &mut slice)
        .expect("the slice streams");
    assert_eq!(lines, 1, "the default read stays the playback surface");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The `?` endpoints drop under `--from`/`--to` unless `--from-any`/
/// `--to-any` — the telemetry tape's filter semantics (item03's shape).
#[test]
fn the_question_mark_endpoints_drop_unless_any() {
    let dir = temp_dir("any-filters");
    let recording = dir.join("flight-44.jsonl");
    write_recording(
        &recording,
        READER_COMMIT,
        44,
        &[
            // A receive-in with no recorded sender renders from='?'.
            json!({"seq": 1, "kind": "receive-in", "ts_ms": 1789214915001u64,
                   "detail": {"len": 3, "hex": "aabb00"}}),
            json!({"seq": 2, "kind": "emit", "ts_ms": 1789214915002u64,
                   "detail": {"kind": 1, "to": 66, "len": 3, "hex": "aabb00"}}),
        ],
    );
    let options = FlightTapeOptions {
        from: Some(44),
        ..Default::default()
    };
    let mut capture: Vec<u8> = Vec::new();
    let (lines, _) = stream_recording(&recording, &options, &mut capture).expect("streams");
    assert_eq!(lines, 1, "the '?'-from line drops, the emit stays");
    let options = FlightTapeOptions {
        from: Some(44),
        from_any: true,
        ..Default::default()
    };
    let mut capture: Vec<u8> = Vec::new();
    let (lines, _) = stream_recording(&recording, &options, &mut capture).expect("streams");
    assert_eq!(lines, 2, "--from-any keeps the '?'-from line");
    let _ = std::fs::remove_dir_all(&dir);
}

/// The endpoint filters apply to the RENDERED endpoints — what the
/// trivial shell filter `grep "^10,66,"` keeps is exactly what
/// `--from 10 --to 66` keeps, and `--from 10` alone keeps every rendered
/// `10,...` line (the emission surface included).
#[test]
fn the_endpoint_filter_matches_the_rendered_line() {
    let dir = temp_dir("grep-equivalence");
    let recording = dir.join("flight-44.jsonl");
    write_recording(
        &recording,
        READER_COMMIT,
        44,
        &[
            json!({"seq": 1, "kind": "receive-in", "ts_ms": 1789214915001u64,
                   "detail": {"from": 655361, "len": 5, "hex": "aabb00ccdd"}}),
            json!({"seq": 2, "kind": "emit", "ts_ms": 1789214915002u64,
                   "detail": {"kind": 1, "to": 10, "len": 3, "hex": "aabb00"}}),
            json!({"seq": 3, "kind": "emit", "ts_ms": 1789214915003u64,
                   "detail": {"kind": 1, "to": 11, "len": 3, "hex": "aabb00"}}),
        ],
    );
    let mut all: Vec<u8> = Vec::new();
    stream_recording(&recording, &FlightTapeOptions::default(), &mut all)
        .expect("the slice streams");
    let rendered: Vec<String> = String::from_utf8(all)
        .unwrap()
        .lines()
        .map(|line| line.to_string())
        .collect();

    for (from, to) in [
        (10u32, Some(11u32)),
        (10, Some(44)),
        (44, Some(10)),
        (10, None),
    ] {
        let options = FlightTapeOptions {
            from: Some(from),
            to,
            ..Default::default()
        };
        let mut capture: Vec<u8> = Vec::new();
        stream_recording(&recording, &options, &mut capture).expect("streams");
        let kept: Vec<String> = String::from_utf8(capture)
            .unwrap()
            .lines()
            .map(|line| line.to_string())
            .collect();
        let grepped: Vec<String> = rendered
            .iter()
            .filter(|line| {
                let (line_from, line_to, _) = parse_tape_line(line).expect("the line parses");
                line_from == from.to_string() && to.is_none_or(|want| line_to == want.to_string())
            })
            .cloned()
            .collect();
        assert_eq!(kept, grepped, "the filter matches the shell grep");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The item18 acceptance over a REAL flight recording: the feature-ON
/// build records a live node (boot, promote, client request, emit,
/// stop), the recording streams as the stable `from,to,jsonl` slice, the
/// trivial shell filter's `grep "^10,11,"` matches the bin's endpoint
/// filter exactly, and the extracted frames force-feed a second node
/// through the SAME playback engine the telemetry tape feeds — the
/// deterministic outcome asserted. Runs under the `flight-recorder`
/// feature only (the capture needs the recorder compiled in).
#[cfg(feature = "flight-recorder")]
mod real_capture {
    use super::*;
    use lunet_advisory_lock::flight::FLIGHT_DIR_ENV;
    use lunet_advisory_lock::{Node, OK};
    use std::sync::Mutex;

    /// The env var is process-global; serialized like the recorder
    /// suite's own tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn given_a_feature_on_run_the_flight_tape_extracts_and_force_feeds_a_node() {
        let guard = ENV_LOCK
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let dir = temp_dir("real-capture");
        // SAFETY: the ENV_LOCK serializes every env access in this
        // process; no other test in this suite reads the flight dir var.
        unsafe {
            std::env::set_var(FLIGHT_DIR_ENV, &dir);
        }
        let mut node = Node::open(
            &["10:n1", "11:n2", "12:n3"].join("\0"),
            "n1",
            dir.join("state").to_str().unwrap(),
            None,
            0,
        )
        .expect("the recorded node boots");
        assert_eq!(node.idle(), OK);
        assert_eq!(
            node.request(
                &serde_json::to_vec(&lunet_advisory_lock::locks::Request::Get {
                    message_id: uuid::Uuid::from_bytes([42; 16]),
                    client_id: 11,
                    request_num: 13,
                    lock_id: 17,
                })
                .unwrap()
            ),
            OK
        );
        while node.next_output().is_some() {}
        assert_eq!(node.stop(), OK);
        unsafe {
            std::env::remove_var(FLIGHT_DIR_ENV);
        }
        drop(guard);

        let recording = dir.join("flight-655361.jsonl");
        // The stable slice, streamed from the REAL recording.
        let mut capture: Vec<u8> = Vec::new();
        let (lines, mangled) =
            stream_recording(&recording, &FlightTapeOptions::default(), &mut capture)
                .expect("the recording streams");
        assert_eq!(mangled, 0, "the recording parses clean");
        let text = String::from_utf8(capture).unwrap();
        let tape: Vec<&str> = text.lines().collect();
        assert!(lines > 0, "the run recorded events: {tape:?}");
        for line in &tape {
            let (from, to, json) = parse_tape_line(line).expect("every line parses");
            assert!(from.parse::<u32>().is_ok() || from == "?");
            assert!(to.parse::<u32>().is_ok() || to == "?");
            assert!(
                ["receive-in", "request-in", "emit"]
                    .contains(&json["kind"].as_str().expect("the kind")),
                "the default read stays the playback surface: {json}"
            );
        }

        // The trivial shell filter's equivalence: what
        // `skaffold_flight_tape --file F | grep "^10,11,"` yields is
        // exactly what `--from 10 --to 11` keeps.
        let grepped: Vec<&&str> = tape
            .iter()
            .filter(|line| line.starts_with("655361,720897,"))
            .collect();
        assert!(
            !grepped.is_empty(),
            "the leader emitted to 720897: {tape:?}"
        );
        let options = FlightTapeOptions {
            from: Some(655361),
            to: Some(720897),
            ..Default::default()
        };
        let mut filtered: Vec<u8> = Vec::new();
        let (kept, _) = stream_recording(&recording, &options, &mut filtered)
            .expect("the filtered stream runs");
        assert_eq!(
            kept,
            grepped.len(),
            "the filter matches the shell grep: {} vs {:?}",
            kept,
            grepped
        );

        // Extraction → force-feed a second node through the SAME
        // playback engine the telemetry tape feeds.
        let frames: Vec<scenario::TapeFrame> = grepped
            .iter()
            .filter_map(|line| {
                let (from, _to, json) = parse_tape_line(line)?;
                tape_frame(from, json.clone())
            })
            .collect();
        let scenario = Scenario::parse(
            r#"{
              "node_id": 720897,
              "name": "n2",
              "membership": [
                {"id": 655361, "name": "n1", "weight": 1},
                {"id": 720897, "name": "n2", "weight": 1},
                {"id": 786433, "name": "n3", "weight": 1}
              ],
              "era": 1,
              "view": 0
            }"#,
        )
        .expect("the scenario parses");
        let mut fed_node = scenario.open_node(&dir).expect("the fed node boots");
        let result = feed_tape(&mut fed_node, &frames);
        assert_eq!(
            result.skipped_no_sender, 0,
            "the flight tape attributes every frame"
        );
        assert!(result.fed >= 1, "the leader's emits force-feed: {result:?}");
        assert!(
            result.codes.get(&0).copied().unwrap_or(0) >= 1,
            "the era-1 genesis prepares digest with OK: {:?}",
            result.codes
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}
