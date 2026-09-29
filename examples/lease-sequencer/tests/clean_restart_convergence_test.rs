//! The clean-restart wedge regression (upstream uvrr-core issue #57's
//! shape): a leader cleanly stopped and restarted while the cluster
//! advances to a HIGHER view must converge — the restarted node
//! re-synchronises in-cluster exactly as though a network partition had
//! healed — never wedging in mutual ViewMismatch/StaleTransfer drops.
//!
//! The intake pins the classification the convergence rides on: the
//! stop's Stopped quorum classifies CLEAN through the boot gate
//! (`lifecycle::boot`), the same identity resumes behind the engine's
//! `Vouched` token, and the returning node chases the cluster's advanced
//! view to Normal. A boot that mis-classified the stop (the pre-intake
//! DIRTY reading) would restart the leader under a bumped high-band
//! identity the cluster drops by name — the wedge.

/// The #57 shape: elect a leader, serve, stop it CLEANLY (the Stopped
/// quorum lands), let the survivors advance the cluster to a HIGHER
/// view, then restart the leader on the same boot line — it must resume
/// the same identity (no bump) and converge to Normal at the cluster's
/// current view.
#[test]
fn clean_restart_while_the_cluster_advances_converges() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
