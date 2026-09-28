//! The AOF lifecycle gate (item22 M2) and the phi-informed timeout
//! estimator (item22 M3): red/green tests for the weight-driven gate, the
//! 1000 ms flusher's stop-when-off rule, the teardown record, the
//! rollover's keep-exactly-two rule, and the phi wait derivation.

use lease_sequencer::telemetry::{
    Gate, TelemetryLog, TimeoutKnobs, marker_bytes, prune_plan, teardown_record,
};
use lunet_locks_aof::envelope::{Marker, Record};

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
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// A member with unknown weight (None, the boot phase) never disarms.
#[test]
fn gate_stays_active_while_weight_unknown() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// Disarm and re-arm are REPORTED so the host flushes on disarm and notes
/// the trace gap on re-arm.
#[test]
fn gate_reports_disarm_and_rearm_transitions() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The 1000 ms forced flusher runs only while the gate is active: it
/// stops when the AOF turns off, and resumes (fresh interval) on re-arm.
#[test]
fn flusher_stops_when_gate_inactive() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// ------------------------------------------------------------- rollover ----

/// The prune plan keeps exactly the current file plus one closed old:
/// everything older goes, oldest first.
#[test]
fn prune_plan_keeps_exactly_two_files() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// Nothing to prune under the two-file floor.
#[test]
fn prune_plan_keeps_everything_at_or_under_two() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The full rollover cycle: append past the threshold, roll, append past
/// it again, roll — the directory ends with EXACTLY two `.aof` files
/// (the current active + one closed old), never more, whatever the
/// retention sweep's sum-based rule would have kept.
#[test]
fn rollover_produces_exactly_two_files_at_the_threshold() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// ------------------------------------------------------- rotation (item C) -

/// Every (re)start ROTATES: a fresh epoch-named active file opens and the
/// pre-existing series SURVIVES for the admin to prune — the startup
/// retention sweep no longer deletes on the telemetry open path.
#[test]
fn open_rotates_and_leaves_the_prior_series_for_the_admin() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// -------------------------------------------------------------- teardown ---
/// The clean-stop teardown record: marker TelemetryStateTransition with
/// the teardown event JSON, and it is the LAST record in the file after a
/// clean close.
#[test]
fn teardown_record_present_after_clean_stop() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The teardown record's shape, standalone.
#[test]
fn teardown_record_shape() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The marker names the telemetry subsystems expose (the envelope table's
/// runtime mirror).
#[test]
fn marker_bytes_table() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// -------------------------------------------------- phi timeout (M3) -------

/// The sampled heartbeat arrival + learned interval lands as an
/// IntervalSample record through the gated log (the exported phi-sample
/// kind's evidence).
#[test]
fn interval_sample_record_passes_the_gate() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The phi-informed wait: `safety * max(heartbeat, learned mean)`, clamped
/// to the [min, max] knobs. A settled but fast sketch clamps UP to min.
#[test]
fn phi_wait_derivation_clamps_to_min() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// A slow learned interval above the max knob clamps DOWN to max.
#[test]
fn phi_wait_derivation_clamps_to_max() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// An unsettled sketch (<2 intervals: the caller passes None) falls back
/// to the old fixed gate, clamped into [min, max] — never earlier than a
/// settled phi allows, never later than the old fixed gate.
#[test]
fn phi_wait_unsettled_sketch_uses_the_clamped_fixed_gate() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The decision record carries everything the failure-detection story
/// needs: phi estimate, now, the previous wait, the next wait.
#[test]
fn timeout_decision_record_fields() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// The test's alias for the estimator (keeps the import list above honest
// without exporting through the lib twice).
use lease_sequencer::telemetry::phi_wait_ms as telemetry_phi_wait;

/// The vendored AOF's Options construction used by TelemetryLog (the
/// force knob stays OFF: amortized buffered writes, flush on demand).
#[test]
fn telemetry_log_opens_with_force_off_and_retention() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
