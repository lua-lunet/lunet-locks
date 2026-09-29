//! The P2 wedge regression (local-softball2-2026-09-17): a leader TERM
//! followed by a same-boot-line restart wedged the cluster — the
//! survivors sat fenced at the dead primary's next view for minutes
//! while their randomized viewchange poll fired `leader_timeout`
//! (an ordinary tick) every 100-200 ms and the tick cannot advance a
//! `ViewChange`-status node (ext/uvrr-core/src/replica/mod.rs:1546-1547).
//! The poll in the limbo must carry the §14.2 host-forced view instead
//! (`docs/src/phi-and-timeouts.md`: the viewchange timer takes over).

/// The limbo's poll carries the §14.2 forced view; every other case
/// keeps the ordinary suspicion tick.
#[test]
fn the_limbos_poll_carries_the_forced_view() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The full P2 shape: elect node 2, serve, TERM it, restart it clean on
/// the same boot line, and converge with the poll carrying the §14.2
/// forced view. As recorded the survivors sit fenced at the dead
/// primary's next view; the poll's forced advance walks the cluster out.
#[test]
fn leader_kill_restart_converges_through_the_limbos_poll() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
