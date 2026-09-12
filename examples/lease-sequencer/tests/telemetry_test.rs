//! The AOF lifecycle gate (item22 M2) and the phi-informed timeout
//! estimator (item22 M3): red/green tests for the weight-driven gate, the
//! 1000 ms flusher's stop-when-off rule, the teardown record, the
//! rollover's keep-exactly-two rule, and the phi wait derivation.

use lease_sequencer::telemetry::{
    Gate, TelemetryLog, TimeoutKnobs, marker_bytes, prune_plan, teardown_record,
};
use lunet_locks_aof::envelope::{Marker, Record};
use lunet_locks_aof::{AofFile, Options};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lease-sequencer-telemetry-{name}-{}-{}",
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

// ----------------------------------------------------------------- gate ----

/// The gate starts ON (the boot Recovering/Joining phase always logs) and
/// follows the node's voting weight: weight 0 or not-yet-a-member (None)
/// keeps it ON; weight > 0 turns it OFF. The 0→1→2→0 sequence toggles
/// active off, (stays) off, then back on.
#[test]
fn gate_follows_the_weight_sequence_0_1_2_0() {
    let mut gate = Gate::new(0, 1000, 4 * 1024 * 1024);
    assert!(gate.active(), "boot phase: active before any weight");

    gate.on_weight(Some(0));
    assert!(gate.active(), "weight 0: AOF ON");

    gate.on_weight(Some(1));
    assert!(!gate.active(), "weight 1: AOF OFF (the hard requirement)");

    gate.on_weight(Some(2));
    assert!(!gate.active(), "weight 2: still OFF");

    gate.on_weight(Some(0));
    assert!(gate.active(), "weight 1→0 re-arms the AOF");
}

/// A member with unknown weight (None, the boot phase) never disarms.
#[test]
fn gate_stays_active_while_weight_unknown() {
    let mut gate = Gate::new(0, 1000, 4 * 1024 * 1024);
    gate.on_weight(None);
    assert!(gate.active());
}

/// Disarm and re-arm are REPORTED so the host flushes on disarm and notes
/// the trace gap on re-arm.
#[test]
fn gate_reports_disarm_and_rearm_transitions() {
    let mut gate = Gate::new(0, 1000, 4 * 1024 * 1024);
    assert_eq!(gate.on_weight(Some(0)), None, "no event when already on");
    assert_eq!(gate.on_weight(Some(1)), Some(false), "disarm reported");
    assert_eq!(gate.on_weight(Some(2)), None, "no event when already off");
    assert_eq!(gate.on_weight(Some(0)), Some(true), "re-arm reported");
}

/// The 1000 ms forced flusher runs only while the gate is active: it
/// stops when the AOF turns off, and resumes (fresh interval) on re-arm.
#[test]
fn flusher_stops_when_gate_inactive() {
    let mut gate = Gate::new(0, 1000, 4 * 1024 * 1024);
    assert!(gate.flush_due(1500), "past the interval while active");
    gate.note_flush(1500);
    assert!(!gate.flush_due(2000), "inside the interval");

    gate.on_weight(Some(1));
    assert!(
        !gate.flush_due(5000),
        "the flusher is OFF with the AOF, whatever the elapsed time"
    );

    gate.on_weight(Some(0));
    gate.note_rearm(5000);
    assert!(!gate.flush_due(5500), "fresh interval after re-arm");
    assert!(gate.flush_due(6100), "due again a full interval later");
}

// ------------------------------------------------------------- rollover ----

/// The prune plan keeps exactly the current file plus one closed old:
/// everything older goes, oldest first.
#[test]
fn prune_plan_keeps_exactly_two_files() {
    let files = vec![
        ("100.aof".to_string(), (100, 0)),
        ("200.aof".to_string(), (200, 0)),
        ("300.aof".to_string(), (300, 0)),
        ("400.aof".to_string(), (400, 0)),
    ];
    assert_eq!(
        prune_plan(&files, "500.aof"),
        vec!["100.aof", "200.aof", "300.aof"]
    );
}

/// Nothing to prune under the two-file floor.
#[test]
fn prune_plan_keeps_everything_at_or_under_two() {
    let files = vec![("100.aof".to_string(), (100, 0))];
    assert!(prune_plan(&files, "200.aof").is_empty());
}

/// The full rollover cycle: append past the threshold, roll, append past
/// it again, roll — the directory ends with EXACTLY two `.aof` files
/// (the current active + one closed old), never more, whatever the
/// retention sweep's sum-based rule would have kept.
#[test]
fn rollover_produces_exactly_two_files_at_the_threshold() {
    let dir = temp_dir("rollover");
    let record = Record::wire(1, b"x");
    let mut log = TelemetryLog::open(
        &dir,
        1000,
        /* rollover_bytes */ 512,
        /* retention_bytes */ 10 * 1024 * 1024,
    )
    .expect("open");
    // 27 records ≈ 27 × (9 + 1 + 272 overhead estimate) — past 512 bytes.
    for _ in 0..40 {
        log.append(record.clone());
        if log.rollover_due() {
            log.rollover().expect("first rollover");
        }
    }
    log.rollover().expect("second rollover");
    drop(log);
    let count = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            name.ends_with(".aof").then_some(name)
        })
        .count();
    assert_eq!(count, 2, "exactly the current file + one closed old");
    std::fs::remove_dir_all(&dir).unwrap();
}

// -------------------------------------------------------------- teardown ---

/// The clean-stop teardown record: marker TelemetryStateTransition with
/// the teardown event JSON, and it is the LAST record in the file after a
/// clean close.
#[test]
fn teardown_record_present_after_clean_stop() {
    let dir = temp_dir("teardown");
    let mut log = TelemetryLog::open(&dir, 1000, 4 * 1024 * 1024, 10 * 1024 * 1024).expect("open");
    log.append(Record::wire(1, b"a wire message"));
    log.teardown().expect("teardown flushes and closes");
    assert!(log.is_closed(), "the file is closed after teardown");

    let active = std::fs::read_dir(&dir)
        .unwrap()
        .filter_map(|entry| {
            let name = entry.unwrap().file_name().to_string_lossy().to_string();
            name.ends_with(".aof").then_some(dir.join(name))
        })
        .next()
        .expect("one active file");
    let mut it =
        unsafe { lunet_locks_aof::ffi::RawIter::open(active.as_os_str().as_encoded_bytes()) }
            .expect("iterator opens");
    let mut last: Option<Record> = None;
    while let Some(entry) = it.next_entry().unwrap() {
        last = Record::decode(&entry.bytes);
    }
    let last = last.expect("the last record decodes");
    assert_eq!(last.marker, Marker::TelemetryStateTransition);
    let value: serde_json::Value = serde_json::from_slice(&last.payload).unwrap();
    assert_eq!(value["event"], "teardown");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The teardown record's shape, standalone.
#[test]
fn teardown_record_shape() {
    let record = teardown_record(77);
    assert_eq!(record.marker, Marker::TelemetryStateTransition);
    assert_eq!(record.ns, 77);
    let value: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
    assert_eq!(value["event"], "teardown");
}

/// The marker names the telemetry subsystems expose (the envelope table's
/// runtime mirror).
#[test]
fn marker_bytes_table() {
    assert_eq!(marker_bytes(Marker::Wire), 1);
    assert_eq!(marker_bytes(Marker::TelemetryTimeoutDecision), 2);
    assert_eq!(marker_bytes(Marker::TelemetryStateTransition), 3);
    assert_eq!(marker_bytes(Marker::TelemetryOutbound), 4);
}

// -------------------------------------------------- phi timeout (M3) -------

/// The phi-informed wait: `safety * max(heartbeat, learned mean)`, clamped
/// to the [min, max] knobs. A settled but fast sketch clamps UP to min.
#[test]
fn phi_wait_derivation_clamps_to_min() {
    let knobs = TimeoutKnobs {
        min_ms: 500,
        max_ms: 5000,
        fixed_ms: 1000,
    };
    let wait = telemetry_phi_wait(Some(12.0), 10, 2.0, &knobs);
    assert_eq!(wait, Some(500), "2*12=24ms clamps up to the 500ms min");
}

/// A slow learned interval above the max knob clamps DOWN to max.
#[test]
fn phi_wait_derivation_clamps_to_max() {
    let knobs = TimeoutKnobs {
        min_ms: 500,
        max_ms: 5000,
        fixed_ms: 1000,
    };
    let wait = telemetry_phi_wait(Some(100_000.0), 10, 2.0, &knobs);
    assert_eq!(wait, Some(5000), "2*100000 clamps down to the 5000ms max");
}

/// An unsettled sketch (<2 intervals: the caller passes None) falls back
/// to the old fixed gate, clamped into [min, max] — never earlier than a
/// settled phi allows, never later than the old fixed gate.
#[test]
fn phi_wait_unsettled_sketch_uses_the_clamped_fixed_gate() {
    let knobs = TimeoutKnobs {
        min_ms: 500,
        max_ms: 5000,
        fixed_ms: 1000,
    };
    assert_eq!(telemetry_phi_wait(None, 10, 2.0, &knobs), Some(1000));
    let wide = TimeoutKnobs {
        min_ms: 500,
        max_ms: 5000,
        fixed_ms: 10_000,
    };
    assert_eq!(
        telemetry_phi_wait(None, 10, 2.0, &wide),
        Some(5000),
        "a fixed gate above max clamps down"
    );
}

/// The decision record carries everything the failure-detection story
/// needs: phi estimate, now, the previous wait, the next wait.
#[test]
fn timeout_decision_record_fields() {
    let record = lease_sequencer::telemetry::timeout_decision_record(123, 1.75, 900, 1000, 2500);
    assert_eq!(record.marker, Marker::TelemetryTimeoutDecision);
    assert_eq!(record.ns, 123);
    let value: serde_json::Value = serde_json::from_slice(&record.payload).unwrap();
    assert_eq!(value["phi"], 1.75);
    assert_eq!(value["now_ms"], 900);
    assert_eq!(value["prev_wait_ms"], 1000);
    assert_eq!(value["next_wait_ms"], 2500);
}

// The test's alias for the estimator (keeps the import list above honest
// without exporting through the lib twice).
use lease_sequencer::telemetry::phi_wait_ms as telemetry_phi_wait;

/// The vendored AOF's Options construction used by TelemetryLog (the
/// force knob stays OFF: amortized buffered writes, flush on demand).
#[test]
fn telemetry_log_opens_with_force_off_and_retention() {
    let dir = temp_dir("openopts");
    let mut log = TelemetryLog::open(&dir, 1000, 4 * 1024 * 1024, 10 * 1024 * 1024).expect("open");
    assert!(!log.is_closed());
    log.teardown().unwrap();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// Silence an unused-import warning when AofFile/Options are only used by
/// the compile-time contract above.
#[allow(unused)]
fn _type_surface(_: fn(&std::path::Path) -> Result<AofFile, lunet_locks_aof::Error>, _: Options) {}
