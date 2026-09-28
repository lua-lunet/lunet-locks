//! The phi-accrual leader-failure detector (item19): red/green tests for
//! the trailer codec, the per-(era, leader) sketch table, the phi math
//! wrapper, the FFI C ABI surface, and the detection decision with the
//! 2x-interval safety floor. Runs ONLY under `experimental-phi` — the
//! sketches, the detector math, and the FFI surface compile only into
//! the flagged build.

#![cfg(feature = "experimental-phi")]

use lease_sequencer::phi::{self, PhiConfig, SketchKey, Trailer};

// ------------------------------------------------------------- trailer ----

/// The trailer rides the back of one heartbeat Commit: magic + era +
/// leader id + sequence + the leader's send clock. The leader endpoint
/// and the monitoring node id are NOT on the wire — the receiver derives
/// both (the socket it answered from; itself).
#[test]
fn trailer_is_little_constant_size() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn trailer_round_trips_at_the_back_of_a_commit() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn strip_rejects_a_packet_without_a_trailer() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn strip_rejects_a_bad_magic() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn strip_tolerates_a_truncated_trailer() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// -------------------------------------------------------------- sketch ----

fn key(era: u32, leader: u32) -> SketchKey {
    SketchKey {
        era,
        leader,
        leader_addr: "127.0.0.1:41101".to_string(),
        monitor: 7,
    }
}

/// Steady ~10 ms arrivals with the jitter a real UDP + 5 ms tick loop
/// produces (a perfectly regular stream has zero variance and the phi
/// math degenerates — the loop's timer granularity guarantees jitter).
fn feed_steady_10ms(sketch: &mut phi::Sketch, t: &mut u64, count: usize) {
    const JITTER: [u64; 8] = [0, 1, 3, 0, 2, 1, 0, 3];
    for i in 0..count {
        *t += 10 + JITTER[i % JITTER.len()];
        sketch.observe(*t);
    }
}

#[test]
fn sketch_starts_at_phi_zero_and_learns_the_interval() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[cfg(feature = "experimental-phi")]
#[test]
fn phi_rises_with_silence_and_crosses_threshold() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn era_change_resets_the_sketch() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn leader_change_resets_the_sketch() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[cfg(feature = "experimental-phi")]
#[test]
fn detection_respects_the_two_interval_safety_floor() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[cfg(feature = "experimental-phi")]
#[test]
fn safety_floor_tracks_the_learned_interval_not_the_configured_one() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// -------------------------------------------------------------- toggle ----

/// The `timedout` toggle and the cluster viewchange timeout
/// (`docs/src/phi-and-timeouts.md`): the phi detector is a steady-state
/// leader-failure detector — it neither updates nor checks while the
/// node is timed out on its leader, and the viewchange timer polls on
/// its own randomized schedule instead. The naming law: "timeout"
/// unqualified is the leader timeout; the viewchange timeout is the
/// distinct mechanism.

#[test]
fn suspicion_toggles_timedout_true_and_stays_true() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn a_fresh_commit_toggles_false_and_the_next_tick_resumes() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn the_toggle_record_carries_state_ts_and_last_toggle_ts() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// ---------------------------------------------------- viewchange timer ----

#[test]
fn the_viewchange_timer_rejects_min_above_max() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn the_randomized_delay_stays_within_the_min_max_bounds() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn the_viewchange_timer_polls_on_its_own_schedule_independent_of_phi() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// ------------------------------------------------- sketch gap protection ----

/// The phi-sketch protection across failover (approach (a)): the gap
/// between the old leader's last commit and the new leader's first
/// commit is not adjacent heartbeats under the same leader, so it never
/// enters the sketch. The sequence: steady observation under the old
/// leader, the view change (timedout=true, no observations across the
/// gap), the fresh commit (toggle false + table reset), and the
/// post-resume arrivals — the learned mean stays the real heartbeat
/// cadence and the gap interval never appears.
#[test]
fn the_old_to_new_leader_commit_gap_never_enters_the_sketch() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

// ------------------------------------------------------------------ ffi ----

#[cfg(feature = "experimental-phi")]
mod ffi {
    use lease_sequencer::phi::ffi::PhiHandle;

    #[test]
    fn c_abi_create_observe_query_free() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn c_abi_null_safety() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }
}
