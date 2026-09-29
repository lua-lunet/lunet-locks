//! The tape acceptance: the corpus is RECORDED IN-TEST (record-once-
//! then-replay) — the former gitignored run pulls were dead for any
//! fresh clone — so the test records its own era-4 view-13 leader-66
//! telemetry AOF into the repository's `.tmp/` scratch through the
//! vendored record layer, streams it as the tape, and replays it in the
//! same run.
//!
//! Red (kept): a fresh genesis node force-fed the higher-era tape
//! digests every datagram with an `OK` return code yet never replays a
//! committed transition — the core drops datagrams naming an era outside
//! its configuration table's retention window (`uvrr-core`
//! `src/replica/normal.rs`, the `EraUnevaluable` gate), and a mid-stream
//! window carries no slots for a fresh node's commit fold to walk. This
//! pins WHY the replay layer for a mid-stream window is the lock
//! Service — the node's committed state machine — fed the verbs
//! extracted byte-exactly from the tape's `frame_hex` payloads.
//!
//! Green: the scenario + the leader-66 window replay the recorded
//! committed transitions byte-exactly — the renewal chain holds once per
//! holder run and renews the same holder on every later regrant, at the
//! recorded execution clocks, from the recorded wire bytes.

/// RED, kept: the fresh node digests the whole tape with `OK` codes and
/// replays nothing — the era wall, pinned.
#[test]
fn given_a_fresh_genesis_node_the_higher_era_tape_never_replays() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// GREEN: the scenario + the era-4 view-13 leader-66 window: the
/// committed verbs extracted from the tape's own bytes replay the
/// recorded committed transitions byte-exactly — each holder run opens
/// with a Hold and renews the same holder thereafter, at the recorded
/// clocks.
#[test]
fn given_the_scenario_and_tape_the_committed_verbs_replay_the_recorded_transitions() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
