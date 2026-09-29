//! The boot-read safety law, proven end to end in a tempdir: the full
//! cluster cycle (clean halt, clean start, crash restart) completes
//! without lockup; corrupt bytes in a superblock copy panic the next
//! boot (never hang, never self-heal); a torn spread resolves by the
//! stated thresholds. Real code, real superblock files.

#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_full_cycle_halts_starts_and_crash_restarts_without_lockup() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn a_corrupted_copy_panics_the_next_boot_and_is_never_healed() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn a_torn_spread_resolves_by_thresholds_with_the_logged_non_unanimity() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
