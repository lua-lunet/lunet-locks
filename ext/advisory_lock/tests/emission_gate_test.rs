//! The durability pins at the boot gate's emission gate: the crash bump's
//! durable marker round (the next life at the running sentinel) completes
//! before the driver releases the first announcement. Real markers, real
//! quorum-of-copies files, real store failures.

/// The emission gate: a failed marker write at the crash bump emits
/// nothing. The store seam fails (a copy-free crashed projection inside
/// an unwritable directory: the classification reads the projection, and
/// the bump round's copies write cannot), so the boot gate's bump round
/// cannot complete: the boot refuses, there is no node, and no
/// announcement is ever released.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn a_failed_marker_write_at_the_crash_bump_emits_nothing() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// A double crash never re-derives the same identity: two consecutive
/// crash boots against the same marker announce strictly successive lives
/// of the same system, both lawful.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn a_double_crash_never_rederives_the_same_identity() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The announced identity's halves: the system half is the descriptor's
/// system id and the crash half is the marker's next life.
#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn the_announced_identity_names_the_descriptor_system_and_the_markers_next_life() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
