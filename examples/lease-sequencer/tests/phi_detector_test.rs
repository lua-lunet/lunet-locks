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
    assert_eq!(Trailer::WIRE_BYTES, 2 + 4 + 4 + 4 + 8);
}

#[test]
fn trailer_round_trips_at_the_back_of_a_commit() {
    let commit = [0u8; 24]; // 20-byte header + 4 bytes of a Commit body
    let trailer = Trailer {
        era: 3,
        leader: 42,
        seq: 1000,
        sent_at_ms: 1789186905082,
    };
    let mut packet = commit.to_vec();
    trailer.append_to(&mut packet);
    assert_eq!(packet.len(), commit.len() + Trailer::WIRE_BYTES);

    let (front, decoded) = Trailer::strip_from(&packet).expect("trailer decodes");
    assert_eq!(front, &commit);
    assert_eq!(decoded, trailer);
}

#[test]
fn strip_rejects_a_packet_without_a_trailer() {
    let short = [1u8; 30];
    assert!(Trailer::strip_from(&short).is_none());
    assert!(Trailer::strip_from(&[]).is_none());
}

#[test]
fn strip_rejects_a_bad_magic() {
    let mut packet = vec![0u8; 24];
    packet.extend_from_slice(&[0xEE; Trailer::WIRE_BYTES]); // wrong magic
    assert!(Trailer::strip_from(&packet).is_none());
}

#[test]
fn strip_tolerates_a_truncated_trailer() {
    let mut packet = vec![0u8; 30];
    let t = Trailer {
        era: 1,
        leader: 2,
        seq: 3,
        sent_at_ms: 4,
    };
    let mut full = packet.clone();
    t.append_to(&mut full);
    packet.extend_from_slice(&full[packet.len()..Trailer::WIRE_BYTES - 2 + packet.len()]);
    assert!(Trailer::strip_from(&packet).is_none());
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
    let cfg = PhiConfig::default();
    let mut sk = phi::Sketch::new(cfg.clone());
    assert_eq!(sk.phi(0), 0.0, "empty sketch reports phi 0");

    let mut t = 1_000u64;
    sk.observe(t);
    feed_steady_10ms(&mut sk, &mut t, 50);
    let mean = sk.mean_interval_ms();
    assert!(
        (mean - 10.0).abs() < 2.0,
        "mean interval learned {mean}, expected ~10"
    );
}

#[cfg(feature = "experimental-phi")]
#[test]
fn phi_rises_with_silence_and_crosses_threshold() {
    let cfg = PhiConfig::default();
    let mut sk = phi::Sketch::new(cfg.clone());
    let mut t = 1_000u64;
    sk.observe(t);
    feed_steady_10ms(&mut sk, &mut t, 50);
    let phi_0 = sk.phi(t);
    let phi_20 = sk.phi(t + 20);
    let phi_100 = sk.phi(t + 100);
    assert!(phi_20 > phi_0, "phi must rise with silence");
    // Saturation is legitimate: a tight learned interval makes large
    // silence infinitely unlikely, and the phi curve caps there. The
    // property that matters is monotone non-decreasing.
    assert!(phi_100 >= phi_20, "phi must not fall");
    assert!(
        sk.phi(t + 100) >= cfg.phi_threshold,
        "100 ms of silence must cross phi {} (got {})",
        cfg.phi_threshold,
        phi_100
    );
}

#[test]
fn era_change_resets_the_sketch() {
    let cfg = PhiConfig::default();
    let mut table = phi::Table::new(cfg);
    let mut t = 1_000u64;
    let _ = table.observe(&key(1, 42), t);
    for _ in 0..30 {
        t += 10;
        let _ = table.observe(&key(1, 42), t);
    }
    assert!(table.get(&key(1, 42)).is_some());

    // A new era is a fresh sketch: the old one is gone and the new one
    // has learned nothing (its first arrival seeds the clock only).
    let _ = table.observe(&key(2, 42), t + 10);
    assert!(table.get(&key(1, 42)).is_none(), "old era dropped");
    let sk = table.get(&key(2, 42)).expect("new era present");
    assert_eq!(sk.sample_count(), 0, "new-era sketch is fresh");
    assert_eq!(sk.phi(t + 10), 0.0);
}

#[test]
fn leader_change_resets_the_sketch() {
    let cfg = PhiConfig::default();
    let mut table = phi::Table::new(cfg.clone());
    let mut t = 1_000u64;
    let _ = table.observe(&key(1, 42), t);
    for _ in 0..30 {
        t += 10;
        let _ = table.observe(&key(1, 42), t);
    }
    let _ = table.observe(&key(1, 43), t + 10);
    assert!(table.get(&key(1, 42)).is_none(), "old leader dropped");
}

#[cfg(feature = "experimental-phi")]
#[test]
fn detection_respects_the_two_interval_safety_floor() {
    let cfg = PhiConfig {
        phi_threshold: 0.001, // effectively always crossed on silence
        ..PhiConfig::default()
    };
    let mut sk = phi::Sketch::new(cfg.clone());
    let mut t = 1_000u64;
    sk.observe(t);
    feed_steady_10ms(&mut sk, &mut t, 50);
    // 15 ms of silence: phi over an ~10 ms learned interval with tiny
    // threshold is already high, but 15 < 2 x 10 = the safety floor, so
    // the decision must hold.
    assert!(sk.phi(t + 15) >= 0.001);
    assert!(
        !phi::decide(&sk, t + 15, &cfg),
        "the safety floor must veto a phi crossing below 2x the interval"
    );
    // 25 ms >= the floor: the decision fires.
    assert!(phi::decide(&sk, t + 25, &cfg), "past the floor, phi rules");
}

#[cfg(feature = "experimental-phi")]
#[test]
fn safety_floor_tracks_the_learned_interval_not_the_configured_one() {
    // The leader heartbeats every ~22 ms (the observed localhost cadence:
    // the 10 ms configured interval rides the host tick plus a full
    // Prepare round). The floor must be 2x the LEARNED interval — 2x the
    // configured 10 ms sits INSIDE the normal distribution and fires on
    // ordinary jitter, view-changing to a healthy leader.
    let cfg = PhiConfig {
        phi_threshold: 0.001,
        ..PhiConfig::default()
    };
    let mut sk = phi::Sketch::new(cfg.clone());
    let mut t = 1_000u64;
    sk.observe(t);
    for _ in 0..50 {
        t += 22;
        sk.observe(t);
    }
    assert!((sk.mean_interval_ms() - 22.0).abs() < 1.0, "learned ~22 ms");
    // Ordinary jitter: a 30 ms gap (< 2x22=44) must NOT fire, though phi
    // is already over the tiny threshold and it clears the 20 ms
    // configured floor.
    assert!(sk.phi(t + 30) >= 0.001);
    assert!(
        !phi::decide(&sk, t + 30, &cfg),
        "a 30 ms gap on a ~22 ms stream is jitter, not death"
    );
    // Past 2x the learned interval: the decision fires.
    assert!(
        phi::decide(&sk, t + 50, &cfg),
        "past the learned floor, phi rules"
    );
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
    let mut toggle = phi::TimeoutToggle::new();
    assert!(!toggle.timed_out(), "a settled node is not timed out");

    let first = toggle
        .on_suspicion(1_000)
        .expect("the first suspicion toggles");
    assert!(
        first.timedout,
        "the view-change issue toggles timedout=true"
    );
    assert!(toggle.timed_out());
    // A steady `true` logs nothing: the toggle record rides only a state
    // CHANGE, so a view change in progress never re-logs per poll.
    assert!(
        toggle.on_suspicion(1_100).is_none(),
        "steady true is silent"
    );
    assert!(
        toggle.timed_out(),
        "still timed out while no commit arrives"
    );
}

#[test]
fn a_fresh_commit_toggles_false_and_the_next_tick_resumes() {
    let mut toggle = phi::TimeoutToggle::new();
    assert!(toggle.on_suspicion(1_000).is_some());
    let record = toggle
        .on_commit(1_400)
        .expect("the fresh commit toggles the resume");
    assert!(!record.timedout, "the commit flips timedout=false");
    assert!(!toggle.timed_out(), "the next tick of the timer resumes");
    assert!(
        toggle.on_commit(1_500).is_none(),
        "steady commits do not re-log the toggle"
    );
}

#[test]
fn the_toggle_record_carries_state_ts_and_last_toggle_ts() {
    let mut toggle = phi::TimeoutToggle::new();
    let first = toggle.on_suspicion(1_000).expect("first toggle");
    assert_eq!(first.timedout, true);
    assert_eq!(first.at_ms, 1_000, "the current ts rides the record");
    assert_eq!(first.previous_ms, None, "no earlier toggle exists");

    let second = toggle.on_commit(1_400).expect("second toggle");
    assert_eq!(second.timedout, false);
    assert_eq!(second.at_ms, 1_400);
    assert_eq!(
        second.previous_ms,
        Some(1_000),
        "the ts of the LAST toggle is kept in memory"
    );
}

// ---------------------------------------------------- viewchange timer ----

#[test]
fn the_viewchange_timer_rejects_min_above_max() {
    assert!(
        phi::ViewChangeTimer::new(200, 100).is_err(),
        "min > max refuses"
    );
    assert!(
        phi::ViewChangeTimer::new(100, 100).is_ok(),
        "min == max is a fixed poll"
    );
    assert!(phi::ViewChangeTimer::new(100, 200).is_ok());
}

#[test]
fn the_randomized_delay_stays_within_the_min_max_bounds() {
    // The injected RNG's unit sample spans the whole schedule:
    // unit 0 -> the floor, unit 1 -> the ceiling, mid -> the midpoint,
    // and a hostile out-of-range sample clamps into the bounds.
    assert_eq!(phi::random_wait_ms(100, 200, 0.0), 100);
    assert_eq!(phi::random_wait_ms(100, 200, 0.5), 150);
    assert_eq!(phi::random_wait_ms(100, 200, 1.0), 200);
    assert_eq!(phi::random_wait_ms(100, 200, -3.0), 100, "clamped low");
    assert_eq!(phi::random_wait_ms(100, 200, 9.0), 200, "clamped high");
    assert_eq!(phi::random_wait_ms(100, 200, 0.25), 125);
}

#[test]
fn the_viewchange_timer_polls_on_its_own_schedule_independent_of_phi() {
    // A DIFFERENT timer from the phi timer: the deadline is the timer's
    // own state, armed with the injected RNG's unit sample, and no phi
    // input moves it. At unit 0 the poll lands exactly at the floor.
    let mut timer = phi::ViewChangeTimer::new(100, 200).expect("valid bounds");
    timer.arm(1_000, 0.0);
    assert_eq!(timer.deadline_ms(), 1_100);
    assert!(!timer.due(1_050), "the poll waits out the randomized delay");
    assert!(timer.due(1_100), "the poll fires on the schedule");
    // A second sample re-arms wherever the RNG lands — the schedule is
    // the timer's, never the phi timer's.
    timer.arm(1_100, 1.0);
    assert_eq!(timer.deadline_ms(), 1_300);
    assert!(!timer.due(1_250));
    assert!(timer.due(1_300));
    // A fresh commit disarms the poll: the timer is the timedout phase's
    // mechanism only.
    timer.disarm();
    assert!(!timer.due(5_000), "disarmed, the timer never fires");
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
    let cfg = phi::PhiConfig::default();
    let mut toggle = phi::TimeoutToggle::new();
    let mut table = phi::Table::new(cfg.clone());
    let mut t = 1_000u64;

    // Steady ~10 ms commits under the old leader.
    t += 10;
    let _ = table.observe(&key(1, 42), t);
    for _ in 0..50 {
        t += 10;
        let _ = table.observe(&key(1, 42), t);
    }
    let before = table.get(&key(1, 42)).expect("the old sketch lives");
    assert!((before.mean_interval_ms() - 10.0).abs() < 2.0);

    // The view change: the node times out on its leader. While timed
    // out, phi is never updated — the failover gap produces no
    // observation at all.
    assert!(toggle.on_suspicion(t).is_some());
    t += 400; // the failover: no commits, no observations

    // The new leader's first commit: the fresh-commit resume. The
    // toggle flips false and the stale sketch resets, so this arrival
    // only seeds — the 400 ms gap is never learned.
    let record = toggle.on_commit(t).expect("the commit toggles the resume");
    assert!(!record.timedout);
    table.reset();

    // Post-resume heartbeats under the new leader: a fresh sketch
    // seeds, then learns only real adjacent intervals.
    t += 10;
    let _ = table.observe(&key(1, 43), t);
    let resumed = table.get(&key(1, 43)).expect("the fresh sketch lives");
    assert_eq!(
        resumed.sample_count(),
        0,
        "the resume arrival seeds a fresh sketch — the gap is gone"
    );
    for _ in 0..30 {
        t += 10;
        let _ = table.observe(&key(1, 43), t);
    }
    let resumed = table.get(&key(1, 43)).expect("the fresh sketch lives");
    assert!(
        (resumed.mean_interval_ms() - 10.0).abs() < 2.0,
        "only post-resume intervals are learned: {}",
        resumed.mean_interval_ms()
    );

    // The same-leader failover: re-elect id 42 under the same era, and
    // the reset keeps the old->new gap out of the (unchanged) key's
    // sketch too.
    let mut table = phi::Table::new(cfg.clone());
    t = 1_000;
    t += 10;
    let _ = table.observe(&key(1, 42), t);
    for _ in 0..50 {
        t += 10;
        let _ = table.observe(&key(1, 42), t);
    }
    table.reset();
    t += 400; // the same-leader failover gap
    let _ = table.observe(&key(1, 42), t); // seeds a fresh sketch
    for _ in 0..30 {
        t += 10;
        let _ = table.observe(&key(1, 42), t);
    }
    let sketch = table.get(&key(1, 42)).expect("the sketch lives");
    assert_eq!(
        sketch.sample_count(),
        30,
        "exactly the post-resume intervals — never the 400 ms gap"
    );
}

// ------------------------------------------------------------------ ffi ----

#[cfg(feature = "experimental-phi")]
mod ffi {
    use lease_sequencer::phi::ffi::PhiHandle;

    #[test]
    fn c_abi_create_observe_query_free() {
        let handle: PhiHandle = lease_sequencer::phi::ffi::phi_detector_new(100);
        assert!(!handle.is_null(), "constructor returns a live handle");

        // Observe a steady 10 ms stream from t=1000: 1 + 50 arrivals, the
        // first seeds the clock and the rest learn 50 intervals.
        let mut t: i64 = 1_000;
        unsafe { lease_sequencer::phi::ffi::phi_observe(handle, t) };
        const JITTER: [i64; 8] = [0, 1, 3, 0, 2, 1, 0, 3];
        for i in 0..50 {
            t += 10 + JITTER[i % JITTER.len()];
            unsafe { lease_sequencer::phi::ffi::phi_observe(handle, t) };
        }

        let mut mean: f64 = 0.0;
        let mut samples: u32 = 0;
        unsafe { lease_sequencer::phi::ffi::phi_query(handle, &mut mean, &mut samples) };
        assert_eq!(samples, 50);
        assert!((mean - 10.0).abs() < 2.0, "ffi mean {mean} ~ 10");

        let mut phi_now: f64 = -1.0;
        unsafe { lease_sequencer::phi::ffi::phi_value(handle, t, &mut phi_now) };
        assert_eq!(phi_now, 0.0, "phi at the last arrival is 0");
        unsafe { lease_sequencer::phi::ffi::phi_value(handle, t + 100, &mut phi_now) };
        assert!(phi_now > 0.0, "phi rises with silence, got {phi_now}");

        unsafe { lease_sequencer::phi::ffi::phi_detector_free(handle) };
    }

    #[test]
    fn c_abi_null_safety() {
        let mut out: f64 = 0.0;
        unsafe {
            assert_eq!(
                lease_sequencer::phi::ffi::phi_value(std::ptr::null_mut(), 0, &mut out),
                1
            );
            assert_eq!(
                lease_sequencer::phi::ffi::phi_observe(std::ptr::null_mut(), 0),
                1
            );
        }
    }
}
