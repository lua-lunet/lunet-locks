//! The Flight Recorder's reader-path unit tests: the header's commit
//! facts, the commit gate's refusal, and the recorder's tape mechanics.
//! These run under the `flight-recorder` feature only.

use super::*;

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lunet-flight-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    dir
}

/// The first record of every flight recording is the header, and it names
/// the commit hash this very build was compiled from.
#[test]
fn the_first_record_is_the_header_naming_this_builds_commit() {
    let dir = temp_dir("header");
    let mut recorder = FlightRecorder::open(&dir, 44).unwrap();
    recorder.event("probe", json!({"n": 1}));
    let header = read_header(recorder.path()).expect("the header reads");
    assert_eq!(header.commit, FLIGHT_COMMIT);
    assert_eq!(header.dirty, flight_dirty());
    assert_eq!(header.node, 44);
    assert_eq!(header.format, FLIGHT_FORMAT);
    assert_eq!(
        check_commit(&header, FLIGHT_COMMIT).expect("this reader is this build"),
        FLIGHT_COMMIT
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The commit gate refuses a recording whose commit the reader is not:
/// a recording is readable ONLY by the code as-at its commit.
#[test]
fn the_commit_gate_refuses_a_foreign_commit() {
    let header = FlightHeader {
        commit: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        dirty: false,
        node: 44,
        format: FLIGHT_FORMAT,
    };
    let error = check_commit(&header, FLIGHT_COMMIT).expect_err("a foreign commit refuses");
    match error {
        FlightReadError::CommitMismatch { recorded, reader } => {
            assert_eq!(recorded, header.commit);
            assert_eq!(reader, FLIGHT_COMMIT);
        }
        other => panic!("expected the commit mismatch, got {other:?}"),
    }
}

/// Events ride one JSONL line each, monotonically sequenced, each line
/// flushed (a crash must not lose its own evidence).
#[test]
fn events_are_monotonic_one_line_each_and_flushed() {
    let dir = temp_dir("events");
    let mut recorder = FlightRecorder::open(&dir, 44).unwrap();
    recorder.event("receive-in", json!({"from": 11, "len": 3, "hex": "aabb00"}));
    recorder.event("drive-out", json!({"code": 0}));
    let text = std::fs::read_to_string(recorder.path()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(lines.len(), 3, "header + two events: {lines:?}");
    let seqs: Vec<u64> = lines
        .iter()
        .filter_map(|line| serde_json::from_str::<Value>(line).unwrap()["seq"].as_u64())
        .collect();
    assert_eq!(seqs, vec![1, 2], "events count from 1 after the header");
    let kinds: Vec<String> = lines
        .iter()
        .map(|line| {
            serde_json::from_str::<Value>(line).unwrap()["kind"]
                .as_str()
                .unwrap()
                .to_string()
        })
        .collect();
    assert_eq!(kinds, vec!["flight-header", "receive-in", "drive-out"]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// A node opts in per directory: one file per node under the env-named
/// dir; the recorder never fails the node (an unwritable dir just runs
/// unrecorded via open_from_env's None path).
#[test]
fn open_from_env_names_one_file_per_node() {
    let dir = temp_dir("env");
    // SAFETY: unit tests here run single-threaded over the env var; no
    // other thread in this process reads the flight dir var.
    unsafe {
        std::env::set_var(FLIGHT_DIR_ENV, &dir);
    }
    let recorder = FlightRecorder::open_from_env(77).expect("the env names a dir");
    assert!(recorder.path().ends_with("flight-77.jsonl"));
    let none = {
        unsafe {
            std::env::remove_var(FLIGHT_DIR_ENV);
        }
        FlightRecorder::open_from_env(77)
    };
    assert!(none.is_none(), "an unset env runs the node unrecorded");
    unsafe {
        std::env::set_var(FLIGHT_DIR_ENV, "/proc/definitely/not/writable");
    }
    let poisoned = FlightRecorder::open_from_env(78);
    assert!(poisoned.is_none(), "an open failure never fails the node");
    unsafe {
        std::env::remove_var(FLIGHT_DIR_ENV);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The hex encoding matches the tape's frame_hex convention.
#[test]
fn hex_is_lowercase_two_digits_per_byte() {
    assert_eq!(hex(&[0x00, 0xab, 0xff]), "00abff");
}
