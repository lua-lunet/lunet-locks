// Confirmation test for F12 under the duration shape: the lock service
// rejects a non-positive lease window. A SET with lease_ms == 0 returns
// granted: false on a free lock or over an expired incumbent, and does not
// mutate lock state. (The old absolute-expiry refusal — expiry <=
// execution_time — has no client-expressible successor: the client cannot
// name an expiry at all.)

#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn sets_with_zero_duration_rejected_on_free_lock() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

#[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
#[test]
fn sets_with_zero_duration_rejected_over_expired_incumbent() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
