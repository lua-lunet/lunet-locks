//! The watermarks and the three sync-point rules, red-green: each rule
//! pinned at its boundary, the monotonicity law pinned against every
//! mutation the rules can drive.

use super::{Frontier, SyncOutcome, WitnessWatermarks};

fn frontier(era: u32, view: u32, slot: u64) -> Frontier {
    Frontier { era, view, slot }
}

/// Rule 1: the flushed watermark already covers the frontier — the
/// witness acknowledges immediately, flushing nothing.
#[test]
fn a_sync_point_the_flush_covers_is_acked_at_once() {
    let mut witness = WitnessWatermarks::new();
    witness.observe(frontier(1, 1, 10));
    assert_eq!(witness.flushed_through_buffer(), Vec::new());
    assert_eq!(witness.flushed(), Some(frontier(1, 1, 10)));

    // The point at the flushed watermark, and any point behind it: acked.
    assert_eq!(witness.sync_point(frontier(1, 1, 10)), SyncOutcome::Acked);
    assert_eq!(witness.sync_point(frontier(1, 1, 3)), SyncOutcome::Acked);
    assert_eq!(witness.sync_point(frontier(1, 0, 99)), SyncOutcome::Acked);
    assert!(witness.held().is_empty(), "nothing is held");
}

/// Rule 2: the frontier is in the buffer — the witness forces the flush
/// of the batch in hand and the point acknowledges with it.
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

    // The driver runs the fsync and reports the flush: the point is now
    // covered, and a repeat of the same point acknowledges outright.
    assert_eq!(witness.flushed_through_buffer(), Vec::new());
    assert_eq!(witness.sync_point(frontier(1, 1, 7)), SyncOutcome::Acked);
}

/// Rule 3: the frontier is beyond the buffer — the point is held open,
/// and it acknowledges the moment the stream covers it AND the flush
/// lands.
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

    // The stream covers the point: the buffer advances...
    witness.observe(frontier(1, 1, 9));
    witness.observe(frontier(1, 1, 12));
    // ...but the held point is not acked until the flush lands.
    assert_eq!(witness.held(), &[frontier(1, 1, 9)]);
    assert_eq!(
        witness.flushed_through_buffer(),
        vec![frontier(1, 1, 9)],
        "the flush the stream's coverage owed lands: the point acks"
    );
    assert!(witness.held().is_empty());
    assert_eq!(witness.sync_point(frontier(1, 1, 9)), SyncOutcome::Acked);
}

/// A point held twice holds once: the hold set carries no duplicates.
#[test]
fn a_held_point_holds_once() {
    let mut witness = WitnessWatermarks::new();
    assert_eq!(witness.sync_point(frontier(2, 1, 3)), SyncOutcome::Held);
    assert_eq!(witness.sync_point(frontier(2, 1, 3)), SyncOutcome::Held);
    assert_eq!(witness.held(), &[frontier(2, 1, 3)]);
}

/// The monotonicity law: neither watermark ever moves backwards, and a
/// flush never acks below where it already stood.
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

    // An out-of-order late record below the watermark moves nothing.
    witness.observe(frontier(1, 1, 99));
    assert_eq!(witness.buffered(), Some(frontier(2, 1, 1)));

    // A flush with an empty buffer's advance acks nothing new.
    assert_eq!(witness.flushed_through_buffer(), Vec::new());
    assert_eq!(witness.flushed(), Some(frontier(2, 1, 1)));
}

/// The held points resolve in arrival order, and only the covered ones.
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
