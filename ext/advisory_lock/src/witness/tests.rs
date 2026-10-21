use super::{Frontier, SyncOutcome, WitnessWatermarks};

fn frontier(era: u32, view: u32, slot: u64) -> Frontier {
    Frontier { era, view, slot }
}

#[test]
fn a_sync_point_the_flush_covers_is_acked_at_once() {
    let mut witness = WitnessWatermarks::new();
    witness.observe(frontier(1, 1, 10));
    assert_eq!(witness.flushed_through_buffer(), Vec::new());
    assert_eq!(witness.flushed(), Some(frontier(1, 1, 10)));

    assert_eq!(witness.sync_point(frontier(1, 1, 10)), SyncOutcome::Acked);
    assert_eq!(witness.sync_point(frontier(1, 1, 3)), SyncOutcome::Acked);
    assert_eq!(witness.sync_point(frontier(1, 0, 99)), SyncOutcome::Acked);
    assert!(witness.held().is_empty(), "nothing is held");
}

#[test]
fn a_sync_point_in_the_buffer_forces_the_flush() {
    let mut witness = WitnessWatermarks::new();
    witness.observe(frontier(1, 1, 7));
    assert_eq!(witness.flushed(), None, "nothing flushed yet");

    assert_eq!(
        witness.sync_point(frontier(1, 1, 7)),
        SyncOutcome::ForceFlush,
        "the batch in hand is flushed early"
    );

    assert_eq!(witness.flushed_through_buffer(), Vec::new());
    assert_eq!(witness.sync_point(frontier(1, 1, 7)), SyncOutcome::Acked);
}

#[test]
fn a_sync_point_ahead_of_the_stream_is_held_then_acked() {
    let mut witness = WitnessWatermarks::new();
    witness.observe(frontier(1, 1, 5));
    let _ = witness.flushed_through_buffer();

    assert_eq!(
        witness.sync_point(frontier(1, 1, 9)),
        SyncOutcome::Held,
        "the point is beyond the buffer: held open"
    );
    assert_eq!(witness.held(), &[frontier(1, 1, 9)]);

    witness.observe(frontier(1, 1, 9));
    witness.observe(frontier(1, 1, 12));
    assert_eq!(witness.held(), &[frontier(1, 1, 9)]);
    assert_eq!(
        witness.flushed_through_buffer(),
        vec![frontier(1, 1, 9)],
        "the flush the stream's coverage owed lands: the point acks"
    );
    assert!(witness.held().is_empty());
    assert_eq!(witness.sync_point(frontier(1, 1, 9)), SyncOutcome::Acked);
}

#[test]
fn a_held_point_holds_once() {
    let mut witness = WitnessWatermarks::new();
    assert_eq!(witness.sync_point(frontier(2, 1, 3)), SyncOutcome::Held);
    assert_eq!(witness.sync_point(frontier(2, 1, 3)), SyncOutcome::Held);
    assert_eq!(witness.held(), &[frontier(2, 1, 3)]);
}

#[test]
fn the_watermarks_never_move_backwards() {
    let mut witness = WitnessWatermarks::new();
    witness.observe(frontier(1, 1, 10));
    witness.observe(frontier(1, 1, 4));
    assert_eq!(witness.buffered(), Some(frontier(1, 1, 10)));

    witness.observe(frontier(2, 1, 1));
    assert_eq!(witness.buffered(), Some(frontier(2, 1, 1)));
    let _ = witness.flushed_through_buffer();
    assert_eq!(witness.flushed(), Some(frontier(2, 1, 1)));

    witness.observe(frontier(1, 1, 99));
    assert_eq!(witness.buffered(), Some(frontier(2, 1, 1)));

    assert_eq!(witness.flushed_through_buffer(), Vec::new());
    assert_eq!(witness.flushed(), Some(frontier(2, 1, 1)));
}

#[test]
fn the_held_points_resolve_in_arrival_order() {
    let mut witness = WitnessWatermarks::new();
    for point in [frontier(1, 1, 20), frontier(1, 1, 30), frontier(1, 1, 40)] {
        assert_eq!(witness.sync_point(point), SyncOutcome::Held);
    }
    witness.observe(frontier(1, 1, 30));
    assert_eq!(
        witness.flushed_through_buffer(),
        vec![frontier(1, 1, 20), frontier(1, 1, 30)],
        "the covered points ack in arrival order; the third stays held"
    );
    assert_eq!(witness.held(), &[frontier(1, 1, 40)]);
}
