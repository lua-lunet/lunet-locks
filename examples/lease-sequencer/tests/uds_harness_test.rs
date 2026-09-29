//! The UDS harness stage scenarios: one genesis node speaks anything
//! (stage 1); a two-node quorum of a three-member cluster stabilizes and
//! serves polite lock traffic, and the third client joins politely
//! (stage 2); the full three-node cluster takes over from a PAUSED holder
//! inside the honest bound and re-enters cleanly (stage 3); three polite
//! contenders started together race one free lock — the two denied
//! contenders must return to the probe cadence and take over through
//! the probe→SET-race path (stage 4). Every stage asserts its invariants
//! on the cluster-wide trace AOF the driver writes.

#[test]
fn stage1_single_node_speaks_get_and_set() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
#[test]
fn stage2_two_nodes_of_three_stabilize_and_serve() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
#[test]
fn stage3_pause_holder_takeover() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The simultaneous bring-up race: three polite contenders against one
/// free lock. The two denied contenders must withdraw their stakes,
/// re-probe, and take a paused holder's lease through the probe→SET
/// race — the regression for the renewal loop that misread the leader's
/// `granted:false` refusal as a renewal and never probed again.
///
/// Disposition: the sustain, denial, and re-probe verdicts are
/// ordering facts and hold under load. The paused-holder takeover
/// needs a leader that stays stable through a ~1-2 s window after the
/// pause: under a heavily loaded host the in-process cluster's phi
/// detector can churn views for the whole wait (observed 2026-09-15 at
/// sustained load averages 4-6 while the rest of the suite stayed
/// green), and the 20 s liveness bound does not cover a storm that
/// long. The verdict's truth is the successor's op mix (a second set
/// op), which no amount of waiting can fake; re-measure on a quiet
/// host when it goes red alongside a view-change storm.
#[test]
fn stage4_three_clients_race_one_free_lock() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The driver-hiccup tolerance: the driver is the cluster's only switch
/// fabric and runs on the test thread, so a scheduling stall of the test
/// thread IS wire silence to every follower — even though every node host
/// stayed up and every heartbeat was emitted on time. The phi detector
/// must not read that manufactured silence as leader death: the observed
/// red runs (item09: a 422 ms frame gap, then view churn 1→16 through the
/// 3 s takeover window, the successors' ops refused not_leader mid-churn)
/// were exactly this — a driver-side stall storming the cluster. The
/// harness's phi timeout knobs must exceed the hiccup a loaded host
/// produces, so the view stays put, the fence stays silent, and the
/// takeover machinery runs on the lease clock, not the churn.
#[test]
fn driver_hiccup_is_not_leader_death() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
