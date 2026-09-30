//! The host's timeout plane: red/green tests for the `timedout` toggle,
//! the cluster viewchange timeout, and the randomized delay law. The
//! leader timeout's own tests live in `leader_timeout_test.rs`.

use lease_sequencer::timeouts::{Rng, TimeoutToggle, ViewChangeTimer, random_wait_ms};

/// A fresh commit toggles `timedout` false; the next tick of the leader
/// timer resumes.
#[test]
fn a_fresh_commit_toggles_false_and_the_next_tick_resumes() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The toggle record carries the new state, the toggle's own ts, and the
/// ts of the LAST toggle.
#[test]
fn the_toggle_record_carries_state_ts_and_last_toggle_ts() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The viewchange timer refuses `min > max`: the randomized schedule
/// needs a real range.
#[test]
fn the_viewchange_timer_rejects_min_above_max() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The randomized delay law `min + unit * (max - min)` never escapes its
/// validated bounds, whatever unit sample it is handed.
#[test]
fn the_randomized_delay_stays_within_the_min_max_bounds() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The viewchange timer polls on its own schedule: it is a DIFFERENT
/// timer from the leader timeout, which stands down while `timedout`
/// holds.
#[test]
fn the_viewchange_timer_polls_on_its_own_schedule() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The `timedout` toggle and the cluster viewchange timeout
/// (`docs/src/failure-detection.md`): the leader timeout is a
/// steady-state leader-failure detector — it is neither re-armed nor
/// checked while the node is timed out on its leader, and the
/// viewchange timer polls on its own randomized schedule instead. The
/// naming law: "timeout" unqualified is the leader timeout; the
/// viewchange timeout is the distinct mechanism.
#[test]
fn suspicion_toggles_timedout_true_and_stays_true() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The pure timeout types against their contracts: the delay law's
/// bounds, the toggle's change-only reporting, the poll's arm/disarm,
/// and the RNG's unit interval.
mod pure {
    use super::*;

    /// The delay law clamps a hostile unit sample into `[min, max]` and
    /// treats a degenerate `max <= min` schedule as the fixed wait.
    #[test]
    fn random_wait_ms_clamps_and_degenerates() {
        assert_eq!(random_wait_ms(100, 200, 0.0), 100);
        assert_eq!(random_wait_ms(100, 200, 1.0), 200);
        assert_eq!(random_wait_ms(100, 200, -5.0), 100);
        assert_eq!(random_wait_ms(100, 200, 5.0), 200);
        assert_eq!(random_wait_ms(100, 200, 0.5), 150);
        assert_eq!(random_wait_ms(100, 100, 0.9), 100);
        assert_eq!(random_wait_ms(100, 50, 0.5), 100);
    }

    /// The toggle logs only on a state CHANGE: a steady `true` re-arming
    /// every tick logs nothing.
    #[test]
    fn the_toggle_reports_only_state_changes() {
        let mut toggle = TimeoutToggle::new();
        assert!(!toggle.timed_out());
        assert_eq!(toggle.last_toggle_ms(), None);

        let first = toggle.on_suspicion(1_000).expect("a change reports");
        assert!(first.timedout);
        assert_eq!(first.at_ms, 1_000);
        assert_eq!(first.previous_ms, None);

        assert_eq!(toggle.on_suspicion(1_100), None);
        assert!(toggle.timed_out());
        assert_eq!(toggle.last_toggle_ms(), Some(1_000));

        let second = toggle.on_commit(1_200).expect("a change reports");
        assert!(!second.timedout);
        assert_eq!(second.previous_ms, Some(1_000));
        assert_eq!(toggle.last_toggle_ms(), Some(1_200));
    }

    /// The viewchange poll arms, fires once due, and a fresh commit
    /// disarms it.
    #[test]
    fn the_viewchange_timer_arms_fires_and_disarms() {
        let mut timer = ViewChangeTimer::new(100, 200).expect("the bounds validate");
        assert!(!timer.armed());
        assert!(!timer.due(1_000));

        timer.arm(1_000, 0.0);
        assert!(timer.armed());
        assert_eq!(timer.deadline_ms(), 1_100);
        assert!(!timer.due(1_099));
        assert!(timer.due(1_100));

        timer.disarm();
        assert!(!timer.armed());
        assert!(!timer.due(u64::MAX));
    }

    /// `ViewChangeTimer::new` refuses `min > max`.
    #[test]
    fn the_viewchange_timer_refuses_an_inverted_range() {
        assert!(ViewChangeTimer::new(200, 100).is_err());
        assert!(ViewChangeTimer::new(100, 100).is_ok());
    }

    /// The RNG's unit sample lands in `[0, 1)` and its bound is at least
    /// one.
    #[test]
    fn the_rng_samples_inside_the_unit_interval() {
        let mut rng = Rng::new(7);
        for _ in 0..1_000 {
            let unit = rng.unit();
            assert!((0.0..1.0).contains(&unit), "unit {unit} escaped [0, 1)");
        }
        assert_eq!(rng.below(1), 0);
    }
}
