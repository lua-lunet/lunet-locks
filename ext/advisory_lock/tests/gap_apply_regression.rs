//! The §13.1 gap-served chunk must apply its whole committed range in one
//! drive, in slot order. The adapter feeds each `Effect::Apply` completion
//! back as `Input::Applied` (§11.1); the core's `plan_applied` refuses any
//! report but the next expected slot — so the feedback order is load-bearing.
//! A chunk carrying several operation slots (the live shape: any client
//! stream produces one) published its install and then failed on its own
//! feedback while the reports drained last-in-first-out.

#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn a_gap_served_chunk_applies_its_whole_committed_range() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
