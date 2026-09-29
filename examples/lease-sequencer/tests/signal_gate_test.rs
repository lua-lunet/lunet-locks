//! The process signal gate under real OS signals. SIGUSR1/SIGUSR2 are
//! process-global; the gates are per-worker — the discipline under test
//! is that every worker follows every process transition, whichever
//! worker's apply() ran first (one signal, two workers: the first drain
//! must not consume it ahead of the other's). These tests raise real
//! signals in this test process, and a raise latches every Signals
//! registered in the process — so they are `#[ignore]`d from the normal
//! suite and must run SOLO, serialized:
//!
//!     cargo test --release --test signal_gate_test -- --ignored --test-threads=1

/// Two workers both awake and draining: one SIGUSR2, one SIGUSR1, and
/// both gates must follow both transitions.
#[test]
#[ignore = "raises real SIGUSR1/SIGUSR2 in this process; run solo (see module docs)"]
fn a_real_signal_is_a_process_transition_every_worker_follows() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The rig shape: one fast drainer and one asleep inside its 200 ms
/// renew-sleep window when the SIGUSR1 lands. The sleeper cannot drain
/// the signal while asleep — its next apply, after the wake, must still
/// follow the process silence. (The chase is entered through the seam
/// so both workers are provably mid-chase before the real signal; the
/// silence is the real-signal path under test.)
#[test]
#[ignore = "raises a real SIGUSR1 in this process; run solo (see module docs)"]
fn a_worker_sleeping_through_the_silence_follows_it_on_wake() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
