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

/// The history sweep plan mirrors the telemetry retention's boundary
/// semantics: sum == threshold keeps everything; one byte over deletes
/// the OLDEST; the newest rotated file is never deleted (min retention:
/// one active plus one rotated).
#[test]
fn the_sweep_plan_keeps_everything_at_the_threshold_and_rolls_the_oldest_over_it() {
    let series = |sizes: &[u64]| -> Vec<FlightHistoryFile> {
        sizes
            .iter()
            .enumerate()
            .map(|(index, size)| FlightHistoryFile {
                epoch: 1000 + index as u64,
                seq: 0,
                name: flight_history_name(44, 1000 + index as u64, 0),
                size: *size,
            })
            .collect()
    };
    // Under the threshold: nothing rolls away.
    assert!(flight_sweep_plan(&series(&[100]), 100, 200).is_empty());
    // Exactly at the threshold: everything stays (no roll-away).
    assert!(flight_sweep_plan(&series(&[60, 40]), 100, 200).is_empty());
    // One byte over: the OLDEST rolls away, and only it.
    let oldest = series(&[60, 40]);
    assert_eq!(
        flight_sweep_plan(&oldest, 101, 200),
        vec![flight_history_name(44, 1000, 0)]
    );
    // The newest rotated file is the floor: never in the deletions, even
    // when the sum stays over the threshold without it.
    let two = series(&[500, 500]);
    assert_eq!(
        flight_sweep_plan(&two, 600, 200),
        vec![flight_history_name(44, 1000, 0)]
    );
    // Empty history: nothing to sweep.
    assert!(flight_sweep_plan(&series(&[]), 500, 200).is_empty());
}

/// The epoch-named history names round-trip through the parser, and
/// everything else (the active tape, another node's series, junk) is
/// refused — the sweep never touches a foreign file.
#[test]
fn history_names_round_trip_and_leave_foreign_files_alone() {
    assert_eq!(
        flight_history_name(44, 1758012345, 0),
        "flight-44-1758012345.jsonl"
    );
    assert_eq!(
        flight_history_name(44, 1758012345, 3),
        "flight-44-1758012345-3.jsonl"
    );
    assert_eq!(
        parse_flight_history_name("flight-44-1758012345.jsonl", 44),
        Some((1758012345, 0))
    );
    assert_eq!(
        parse_flight_history_name("flight-44-1758012345-3.jsonl", 44),
        Some((1758012345, 3))
    );
    // The active tape is not history.
    assert_eq!(parse_flight_history_name("flight-44.jsonl", 44), None);
    // Another node's series is not this node's history.
    assert_eq!(
        parse_flight_history_name("flight-4-1758012345.jsonl", 44),
        None
    );
    // Junk is not history.
    assert_eq!(parse_flight_history_name("flight-44-abc.jsonl", 44), None);
    assert_eq!(parse_flight_history_name("junk.jsonl", 44), None);
}

/// The cap in action on a small tape: the active tape rotates to an
/// epoch-named history file when it passes the rotation threshold, the
/// fresh tape opens on its own header, the event sequence continues
/// unbroken across files, and the rolled-away history keeps the series
/// under the cap — the newest rotated file always surviving.
#[test]
fn the_tape_rotates_to_epoch_named_files_and_the_cap_bounds_the_series() {
    let dir = temp_dir("cap");
    // A tiny cap: rotation at half of it, history budget the other half.
    let retention: u64 = 4096;
    let mut recorder = FlightRecorder::open_capped(&dir, 44, retention).unwrap();
    // The first rotated history file's name: once enough tape has
    // streamed past it, the cap must roll IT away — that is the sweep
    // doing its oldest-first work.
    let mut first_history: Option<String> = None;
    for n in 0..400u64 {
        recorder.event("probe", json!({"n": n, "pad": "x".repeat(64)}));
        let history = list_flight_history(&dir, 44).unwrap();
        if first_history.is_none() {
            first_history = history.first().map(|file| file.name.clone());
        }
    }
    let history = list_flight_history(&dir, 44).unwrap();
    let first = first_history.expect("at least one rotation produced history");
    assert!(
        !history.iter().any(|file| file.name == first),
        "the FIRST history file was rolled away by the cap: {first}"
    );
    // The cap rolled history away: the retained count stays tiny while
    // hundreds of events streamed.
    assert!(
        history.len() <= 2,
        "the cap rolls the oldest away: {history:?}"
    );
    // Every file on disk — active and history — opens on a header and
    // every line is one complete JSON record (flush-per-line intact:
    // the rename happened between flushed lines).
    let active_path = dir.join("flight-44.jsonl");
    let tapes: Vec<PathBuf> = history
        .iter()
        .map(|file| dir.join(&file.name))
        .chain(std::iter::once(active_path.clone()))
        .collect();
    for path in &tapes {
        let header = read_header(path).expect("every tape file opens on a header");
        assert_eq!(header.node, 44);
        let text = std::fs::read_to_string(path).unwrap();
        for line in text.lines() {
            serde_json::from_str::<Value>(line).expect("every line is one JSON record");
        }
    }
    // The series is bounded: active + history never exceeds the cap by
    // more than the one in-flight line that triggers the next rotation.
    let active_size = std::fs::metadata(&active_path).unwrap().len();
    let total = active_size + history.iter().map(|file| file.size).sum::<u64>();
    assert!(
        total <= retention + 512,
        "the series stays under the cap: {total} vs {retention}"
    );
    // The sequence continues unbroken: strictly increasing across the
    // surviving files (oldest history first, active last), and the
    // active tape picks up exactly where the newest history file left
    // off — the rolled-away middle is the cap's trade, not a gap in
    // what remains.
    let mut seqs = Vec::new();
    for file in history.iter().rev() {
        let text = std::fs::read_to_string(dir.join(&file.name)).unwrap();
        seqs.extend(seqs_of(&text));
    }
    let active_text = std::fs::read_to_string(&active_path).unwrap();
    seqs.extend(seqs_of(&active_text));
    let mut sorted = seqs.clone();
    sorted.sort_unstable();
    assert_eq!(seqs, sorted, "strictly increasing in tape order");
    let newest_history_last = history.last().map(|file| {
        let text = std::fs::read_to_string(dir.join(&file.name)).unwrap();
        seqs_of(&text).last().copied().unwrap_or(0)
    });
    let active_first = seqs_of(&active_text)
        .first()
        .copied()
        .expect("events exist");
    assert_eq!(
        newest_history_last,
        Some(active_first - 1),
        "the active tape continues the newest history file"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Restart discipline: an open on a directory whose active tape already
/// sits over the rotation threshold rotates it to an epoch-named history
/// file FIRST — the fresh boot records into a fresh tape, and the
/// pre-existing history is swept under the cap at open.
#[test]
fn an_open_over_the_rotation_threshold_rotates_before_the_fresh_header() {
    let dir = temp_dir("open-rotate");
    // A pre-existing active tape far over the small rotation threshold:
    // JSONL lines of known content, well over half the tiny cap.
    let legacy: String = (0..20)
        .map(|n| {
            format!(
                "{{\"kind\":\"probe\",\"n\":{n},\"pad\":\"{}\"}}\n",
                "x".repeat(200)
            )
        })
        .collect();
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("flight-44.jsonl"), &legacy).unwrap();
    let retention: u64 = 4096;
    let mut recorder = FlightRecorder::open_capped(&dir, 44, retention).unwrap();
    // The legacy tape became an epoch-named history file, byte-identical.
    let history = list_flight_history(&dir, 44).unwrap();
    assert_eq!(history.len(), 1, "the legacy tape rotated: {history:?}");
    let carried = std::fs::read_to_string(dir.join(&history[0].name)).unwrap();
    assert_eq!(carried, legacy, "history moves whole, never truncated");
    // The fresh active tape opens on this boot's header.
    let text = std::fs::read_to_string(recorder.path()).unwrap();
    let lines: Vec<&str> = text.lines().collect();
    assert_eq!(
        lines.len(),
        1,
        "a fresh tape opens on its header: {lines:?}"
    );
    let header = read_header(recorder.path()).expect("the fresh header reads");
    assert_eq!(header.node, 44);
    recorder.event("probe", json!({"n": 1}));
    let text = std::fs::read_to_string(recorder.path()).unwrap();
    assert_eq!(
        seqs_of(&text),
        vec![1],
        "events count from 1 on the new tape"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// The seq values of one tape's event records (the header carries none).
fn seqs_of(text: &str) -> Vec<u64> {
    text.lines()
        .filter_map(|line| serde_json::from_str::<Value>(line).unwrap()["seq"].as_u64())
        .collect()
}
