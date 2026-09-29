//! The live-process signal gate: every catchable termination signal
//! (SIGTERM, SIGINT, SIGQUIT) drives the same clean shutdown — the serve
//! loop's drain point closes the wire, the `Stopped`→`flushed` marker
//! rounds run across the four superblock copies, and the next boot
//! continues under the SAME incarnation — while SIGKILL is the negative
//! (the running sentinel stands, the next boot bumps into the high band)
//! and SIGHUP is a logged no-op that never stops the process. Each test
//! spawns real `lease-sequencer` serve processes on loopback over a
//! three-genesis-member descriptor and speaks the real client wire.
//!
//! Scratch discipline: every run directory lives under the repo's
//! `.tmp/` (resolved from `CARGO_MANIFEST_DIR`), never the OS temp dir.

#[test]
fn sigterm_drives_the_clean_stop_and_same_incarnation_reboot() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
#[test]
fn sigint_drives_the_clean_stop_and_same_incarnation_reboot() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
#[test]
fn sigquit_drives_the_clean_stop_and_same_incarnation_reboot() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The negative: SIGKILL skips the stop path entirely — the running
/// sentinel stands and the next boot announces the marker's next life
/// (error-on-crashed), the documented crash shape.
#[test]
fn sigkill_leaves_the_running_sentinel_and_next_boot_bumps() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// SIGHUP is a logged no-op: the process survives, the noop record
/// appears, and a following ops attempt succeeds — the node keeps
/// serving.
#[test]
fn sighup_is_a_logged_noop_and_the_node_keeps_serving() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
