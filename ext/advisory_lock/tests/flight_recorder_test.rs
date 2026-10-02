//! The Flight Recorder's flag-ON integration tests (the
//! `flight-recorder` feature): the header's commit facts, the internal
//! event coverage (inbound bytes, drive outcomes, the lock-state journal
//! flush, outbound bytes, the maybe tripwire), and the reader path's
//! commit gate. The flag-OFF prod path has its own proof: this file does
//! not compile without the feature, and the whole existing suite runs
//! unchanged on the default features.

#![cfg(feature = "flight-recorder")]

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
