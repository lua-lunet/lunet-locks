//! The shutdown-restart consistency check's acceptance (the snapshot
//! rule): a log claiming a clean flush must stand on a final marker
//! showing flushed/stopped at that identity, the reverse must hold, the
//! stop path's records must stay ordered, and the check reads a raw run
//! directory and a snapshot archive to the same verdict.
//!
//! The superblock fixtures here are REAL marker files — written by the
//! vendored Zig store through `lunet_locks_aof::marker::write` — so the
//! classification path the check exercises is the production one.

/// The canonical clean stop: the log ends on the flushed record and the
/// marker copies agree at flushed. The verdict is consistent.
#[test]
fn a_clean_stop_cross_checks_consistent() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The spec's planted inconsistency: a log carrying
/// "drained and flushed" as the node's final stop record, over a
/// superblock whose final state is NOT flushed — the marker copies sit
/// at unflushed. The finding names the log line, the record's
/// timestamp, and both copy states.
#[test]
fn the_planted_flush_claim_against_an_unflushed_marker_is_reported() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// A flushed single-file projection over an unflushed quorum is its own
/// inconsistency: the projection is written only after the quorum write
/// succeeded.
#[test]
fn the_projection_ahead_of_the_quorum_is_reported() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// A clean-flush claim with no superblock file at all is an
/// inconsistency: nothing on disk vouches for the flush.
#[test]
fn the_flush_claim_without_a_superblock_is_reported() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The reverse direction: the marker vouches for flushed with no stop
/// record in the node's logs.
#[test]
fn the_flushed_marker_without_a_stop_record_is_reported() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The stop path's records are ordered: a flushed record before the
/// drain record inverts the write order and is flagged.
#[test]
fn the_stop_path_out_of_order_is_flagged() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// Work the runtime logged after the persist order completed is an
/// inversion: the node kept working after the stop path said it was
/// done.
#[test]
fn the_work_after_the_persist_order_is_flagged() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// A stop cycle followed by a later life (a boot record, then a later
/// crashed existence) is superseded: the final marker belongs to the
/// later life and the earlier flushed record is not contradicted by it.
#[test]
fn a_later_life_supersedes_the_earlier_stop_cycle() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The two marker copies must name the same identity: a single-file
/// marker one incarnation ahead of the superblock is an inconsistency.
#[test]
fn the_identity_mismatch_between_marker_copies_is_reported() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// A rotted superblock is an inconsistency, never a silent pass.
#[test]
fn the_unreadable_superblock_is_reported() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// Reading from the snapshot archive equals reading from the raw run
/// directory: the same report text, the same verdict.
#[test]
fn the_archive_read_equals_the_raw_read() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// A snapshot_run.lua archive (the system tar's own bytes) reads exactly
/// like the raw directory: the check is transparent over the tool's
/// archive format.
#[test]
fn the_snapshot_tool_archive_reads_like_the_raw_dir() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// An input that is neither a directory nor a gzip archive is a usage
/// error, reported loudly.
#[test]
fn an_unreadable_input_is_a_loud_error() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
