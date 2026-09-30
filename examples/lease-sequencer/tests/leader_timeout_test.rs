//! The leader timeout: the host's leader-failure detector and the
//! service's only failure-detection mechanism. Per watched (era, leader)
//! key a deadline = now + uniform_random(min, max) over the
//! `--leader-timeout-min-ms/max` knobs, re-armed on leader evidence (a
//! leader change, the fresh-commit resume, each heartbeat Commit
//! arriving from the current leader), and firing the §14.2 host-forced
//! view change.

use lease_sequencer::timeouts::LeaderTimeout;

/// The deadline lands inside the schedule's `[min, max]` window and the
/// unit sample drives it.
#[test]
fn the_deadline_lands_within_the_min_max_window() {
    let mut leader = LeaderTimeout::new(500, 1_000);
    assert_eq!(leader.min_ms(), 500);
    assert_eq!(leader.max_ms(), 1_000);

    leader.watch((1, 7), 10_000, 0.0);
    assert_eq!(leader.watched(), Some((1, 7)));
    assert_eq!(leader.deadline_ms(), 10_500);

    let mut leader = LeaderTimeout::new(500, 1_000);
    leader.watch((1, 7), 10_000, 1.0);
    assert_eq!(leader.deadline_ms(), 11_000);

    let mut leader = LeaderTimeout::new(500, 1_000);
    leader.watch((1, 7), 10_000, 0.25);
    assert_eq!(leader.deadline_ms(), 10_625);
}

/// `due` fires only at or after the deadline, and only while a key is
/// watched.
#[test]
fn due_fires_only_after_the_deadline() {
    let mut leader = LeaderTimeout::new(500, 1_000);
    assert!(!leader.due(1_000), "an unwatched key never fires");

    leader.watch((1, 7), 10_000, 0.0);
    assert!(!leader.due(10_499));
    assert!(leader.due(10_500));
    assert!(leader.due(10_501));
}

/// A new (era, leader) key re-arms the deadline at its birth; the same
/// key is stable.
#[test]
fn a_leader_change_re_arms_the_deadline() {
    let mut leader = LeaderTimeout::new(500, 1_000);
    leader.watch((1, 7), 10_000, 0.0);
    let armed = leader.deadline_ms();

    leader.watch((1, 7), 20_000, 0.0);
    assert_eq!(
        leader.deadline_ms(),
        armed,
        "the same key must not re-arm: an evidence stream keeps it fresh"
    );

    leader.watch((1, 8), 30_000, 0.0);
    assert_eq!(leader.watched(), Some((1, 8)));
    assert_eq!(leader.deadline_ms(), 30_500);
    assert_eq!(leader.last_evidence_ms(), 30_000);
}

/// An era change re-keys the watch exactly as a leader change does.
#[test]
fn an_era_change_re_keys_the_watch() {
    let mut leader = LeaderTimeout::new(500, 1_000);
    leader.watch((1, 7), 10_000, 0.0);
    leader.watch((2, 7), 40_000, 0.0);
    assert_eq!(leader.watched(), Some((2, 7)));
    assert_eq!(leader.deadline_ms(), 40_500);
}

/// `rearm` on every leader-evidencing arrival walks the deadline forward
/// and never backwards.
#[test]
fn a_rearm_moves_the_deadline_and_the_evidence_stamp() {
    let mut leader = LeaderTimeout::new(500, 1_000);
    leader.watch((1, 7), 10_000, 0.0);
    leader.rearm(11_000, 1.0);
    assert_eq!(leader.last_evidence_ms(), 11_000);
    assert_eq!(leader.deadline_ms(), 12_000);
    assert_eq!(leader.watched(), Some((1, 7)));
}

/// The randomized wait spans the window: the observed deadlines stay
/// inside `[min, max]` and cover a wide band of it. The unit sample is
/// half-open `[0, 1)`, so the floor is drawable and the ceiling is not.
#[test]
fn the_random_wait_spans_the_whole_window() {
    let mut rng = lease_sequencer::timeouts::Rng::new(0x51EED);
    let (min_ms, max_ms) = (500_u64, 1_000_u64);
    let mut low = u64::MAX;
    let mut high = 0_u64;
    for _ in 0..10_000 {
        let mut leader = LeaderTimeout::new(min_ms, max_ms);
        leader.watch((1, 7), 0, rng.unit());
        let span = leader.deadline_ms();
        assert!(
            (min_ms..max_ms).contains(&span),
            "draw {span} escaped [{min_ms}, {max_ms}]"
        );
        low = low.min(span);
        high = high.max(span);
    }
    assert_eq!(low, min_ms, "the window's floor must be drawable (unit 0)");
    assert_eq!(
        high,
        max_ms - 1,
        "the largest drawable wait: the unit sample is half-open"
    );
}
