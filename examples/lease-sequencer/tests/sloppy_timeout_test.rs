//! The sloppy leader timeout (item25.18): the normal build's
//! leader-failure detector. Per watched (era, leader) key a deadline =
//! now + uniform_random(min, max) over the `--phi-timeout-min-ms/max`
//! knobs, re-armed on leader evidence (leader change, the fresh-commit
//! resume, each heartbeat Commit arriving from the current leader), and
//! firing the same downstream actuation the experimental build's phi
//! crossing drives.

use lease_sequencer::phi::{self, SloppyLeader};

#[test]
fn the_deadline_lands_within_the_min_max_window() {
    let mut sloppy = SloppyLeader::new(500, 1000);
    assert!(!sloppy.due(10_000), "an unwatched timeout never fires");
    sloppy.watch((1, 42), 10_000, 0.0);
    assert_eq!(sloppy.deadline_ms(), 10_500, "unit 0 lands on the floor");
    sloppy.watch((1, 42), 10_000, 1.0);
    assert_eq!(
        sloppy.deadline_ms(),
        10_500,
        "watching the same key is stable"
    );
    sloppy.watch((1, 43), 10_000, 1.0);
    assert_eq!(sloppy.deadline_ms(), 11_000, "unit 1 lands on the ceiling");
}

#[test]
fn due_fires_only_after_the_deadline() {
    let mut sloppy = SloppyLeader::new(500, 1000);
    sloppy.watch((1, 42), 1_000, 0.0);
    assert!(!sloppy.due(1_400), "before the deadline: not due");
    assert!(sloppy.due(1_500), "past the deadline: due");
    sloppy.rearm(1_600, 0.0);
    assert!(!sloppy.due(2_000), "re-armed, the old deadline is gone");
    assert!(sloppy.due(2_100));
}

#[test]
fn a_leader_change_re_arms_the_deadline() {
    let mut sloppy = SloppyLeader::new(500, 1000);
    sloppy.watch((1, 42), 1_000, 0.0);
    assert_eq!(sloppy.deadline_ms(), 1_500);
    sloppy.watch((1, 43), 1_200, 0.5);
    assert_eq!(sloppy.deadline_ms(), 1_950, "the new key re-arms");
}

#[test]
fn an_era_change_re_keys_the_watch() {
    let mut sloppy = SloppyLeader::new(500, 1000);
    sloppy.watch((1, 42), 1_000, 0.0);
    sloppy.watch((2, 42), 1_100, 0.0);
    assert_eq!(sloppy.deadline_ms(), 1_600, "era change re-arms");
}

#[test]
fn the_random_wait_spans_the_whole_window() {
    // The shared delay law: min + unit * (max - min) — the same
    // function the cluster viewchange schedule arms with.
    assert_eq!(phi::random_wait_ms(100, 200, 0.0), 100);
    assert_eq!(phi::random_wait_ms(100, 200, 0.5), 150);
    assert_eq!(phi::random_wait_ms(100, 200, 1.0), 200);
    assert_eq!(phi::random_wait_ms(100, 200, -3.0), 100, "clamped low");
    assert_eq!(phi::random_wait_ms(100, 200, 9.0), 200, "clamped high");
}
