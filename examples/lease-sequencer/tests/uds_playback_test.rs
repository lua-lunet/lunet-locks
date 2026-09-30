//! The message-playback tests: the corpus is RECORDED IN-TEST (record-
//! once-then-replay) — the former gitignored rig pull
//! (`.tmp/telemetry/threenode-2026-09-14/`) was dead for any fresh clone
//! — so each test records its own wire stream into the repository's
//! `.tmp/` scratch through the vendored record layer and replays it in
//! the same run. The recorded stream's grammar mirrors the 2026-09-14
//! three-node rig pull's shape: era 4, leader 66, six members (44/55/66
//! voters + 77/88/99 weight-0 standbys), a loud heartbeat GET noise
//! floor on the sequencer's sentinel lock, the voters' renewal SETs on
//! that lock at the polite ~251 ms cadence, and the polite lock carrying
//! GETs only — the polite clients' SETs never entered the commit stream.

/// The recorded stream's facts, pinned: the heartbeat GET noise floor is
/// loud, and the polite lock carries GETs only — no polite SET ever
/// enters the commit stream.
#[test]
fn given_the_recorded_stream_the_polite_lock_carries_no_sets() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// GIVEN the recorded renewal SETs (the sentinel lock's driver chase)
/// replayed in record order at their recorded clocks, EXPECT the first
/// grant to be a Hold and every same-holder regrant a Renew with the
/// renew counter climbing — the polite cadence machinery working
/// server-side on the recorded bytes.
#[test]
fn given_the_recorded_renewal_sets_service_holds_then_renews() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// GIVEN a recorded heartbeat GET at its recorded clock, EXPECT the
/// Service's GET reply shape with the leader's execution tick echoed.
#[test]
fn given_the_recorded_heartbeat_get_reply_shape() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// GIVEN a recorded SET re-delivered through the harness cluster (the
/// same core, the UDS transport), EXPECT the fresh identity to be
/// granted on the live node, and the re-delivery of the SAME op
/// (message_id reused) to replay the first reply byte-exactly — nobody
/// re-executes, nobody double-grants. The fencing story across the
/// transport swap: staleness does not exist on the wire, identity does.
#[test]
fn given_a_recorded_set_through_the_harness_cluster_the_replay_is_deduped() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The recorded stream's committed-op census by lock: printed for the
/// record.
#[test]
fn the_recorded_corpus_census() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
