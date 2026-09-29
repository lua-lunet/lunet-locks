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

/// The recording header parses and the commit gate passes for this
/// reader's own commit — and refuses a foreign one loudly.
#[test]
fn the_reader_gate_passes_this_build_and_refuses_a_foreign_commit() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The default extraction keeps only the wire kinds and renders the
/// scenario engine's `from,to,{json}` shape: the inbound sender is the
/// host's attribution (exact), the emission names its target.
#[test]
fn the_extraction_renders_the_playback_surface() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The internal kinds ride `--kinds internal` — the node's private story
/// (drives, faults, journal flushes, markers) is the reader's surface on
/// request, never by default.
#[test]
fn the_internal_kinds_are_explicit_only() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// END-TO-END through the SAME playback engine the telemetry tape feeds:
/// the scenario opens the node, the flight-derived tape's frames are
/// force-fed in order through `feed_tape`, and the deterministic outcome
/// is asserted. This is the exact message-sequence extraction → unit
/// test force-feeding a node, the corfu inverse-paste shape.
#[test]
fn given_a_scenario_the_flight_tape_feeds_the_playback_engine() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The commit gate runs before any DEEP extraction: a foreign-commit
/// recording refuses the deep read — `--deep` and every kind beyond
/// `wire` alike — naming both sides.
#[test]
fn the_deep_read_refuses_a_foreign_commit_before_any_extraction() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The stable `from,to,jsonl` slice is offered cross-commit: the wire
/// kinds of a foreign-commit recording still stream (best-effort —
/// mangled lines are counted, never guessed), while the deep read of the
/// same recording refuses.
#[test]
fn the_stable_slice_streams_cross_commit_and_the_deep_read_does_not() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The operator's law on the replay: the tape keeps the raw numbers the
/// recorder captured, and the extraction spells every name a human would
/// otherwise have to look up — the emit's output kind, the journal
/// flush's kind, and the node's replication state where an event carries
/// one (the marker events' raw codes).
#[test]
fn the_extraction_stringifies_the_names_beside_the_raw_numbers() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The deep read on the SAME commit: the full internal event log — every
/// event rendered with its `seq`, the internal story included, and the
/// default read of the same recording staying the slice only.
#[test]
fn the_deep_read_streams_the_full_internal_log_on_the_same_commit() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The `?` endpoints drop under `--from`/`--to` unless `--from-any`/
/// `--to-any` — the telemetry tape's filter semantics (item03's shape).
#[test]
fn the_question_mark_endpoints_drop_unless_any() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The endpoint filters apply to the RENDERED endpoints — what the
/// trivial shell filter `grep "^10,66,"` keeps is exactly what
/// `--from 10 --to 66` keeps, and `--from 10` alone keeps every rendered
/// `10,...` line (the emission surface included).
#[test]
fn the_endpoint_filter_matches_the_rendered_line() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
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
    #[test]
    fn given_a_feature_on_run_the_flight_tape_extracts_and_force_feeds_a_node() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }
}
