//! The Flight Recorder's reader-path unit tests: the header's commit
//! facts, the commit gate's refusal, and the recorder's tape mechanics.
//! These run under the `flight-recorder` feature only.

/// The first record of every flight recording is the header, and it names
/// the commit hash this very build was compiled from.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_first_record_is_the_header_naming_this_builds_commit() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The commit gate refuses a recording whose commit the reader is not:
/// a recording is readable ONLY by the code as-at its commit.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_commit_gate_refuses_a_foreign_commit() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// Events ride one JSONL line each, monotonically sequenced, each line
/// flushed (a crash must not lose its own evidence).
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn events_are_monotonic_one_line_each_and_flushed() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// A node opts in per directory: one file per node under the env-named
/// dir; the recorder never fails the node (an unwritable dir just runs
/// unrecorded via open_from_env's None path).
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn open_from_env_names_one_file_per_node() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The hex encoding matches the tape's frame_hex convention.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn hex_is_lowercase_two_digits_per_byte() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The history sweep plan mirrors the telemetry retention's boundary
/// semantics: sum == threshold keeps everything; one byte over deletes
/// the OLDEST; the newest rotated file is never deleted (min retention:
/// one active plus one rotated).
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_sweep_plan_keeps_everything_at_the_threshold_and_rolls_the_oldest_over_it() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The epoch-named history names round-trip through the parser, and
/// everything else (the active tape, another node's series, junk) is
/// refused — the sweep never touches a foreign file.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn history_names_round_trip_and_leave_foreign_files_alone() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The cap in action on a small tape: the active tape rotates to an
/// epoch-named history file when it passes the rotation threshold, the
/// fresh tape opens on its own header, the event sequence continues
/// unbroken across files, and the rolled-away history keeps the series
/// under the cap — the newest rotated file always surviving.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_tape_rotates_to_epoch_named_files_and_the_cap_bounds_the_series() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// Restart discipline: an open on a directory whose active tape already
/// sits over the rotation threshold rotates it to an epoch-named history
/// file FIRST — the fresh boot records into a fresh tape, and the
/// pre-existing history is swept under the cap at open.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn an_open_over_the_rotation_threshold_rotates_before_the_fresh_header() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
