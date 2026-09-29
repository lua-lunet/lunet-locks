//! The wiped-state first boot: the exact rig shape. The whole state tree
//! is absent (wiped), the host recreates the state directories during the
//! boot, and every node boots over a fresh marker — the first quorum
//! write fsyncs the containing directory and must not abort the boot.
//! The crashed-state classification is unchanged: reopening without a
//! stop bumps the incarnation (the running sentinel is a crash).

/// A first boot over a wiped state dir (the whole tree absent, the host
/// recreating the directories) boots clean, serves a committed client
/// operation, and the crashed-state reopen bumps the incarnation — the
/// running sentinel is a crash, exactly as before the dir-sync fix.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn a_first_boot_over_a_wiped_state_dir_boots_serves_and_a_crash_bumps() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
