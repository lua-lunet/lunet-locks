//! The shutdown-restart consistency check's acceptance (the snapshot
//! rule): a log claiming a clean flush must stand on a final marker
//! showing flushed/stopped at that identity, the reverse must hold, the
//! stop path's records must stay ordered, and the check reads a raw run
//! directory and a snapshot archive to the same verdict.
//!
//! The superblock fixtures here are REAL marker files — written by the
//! vendored Zig store through `lunet_locks_aof::marker::write` — so the
//! classification path the check exercises is the production one.

use lease_sequencer::shutdown_check::{Verdict, check_shutdown};
use lunet_locks_aof::marker::{self, MarkerState};
use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lease-sequencer-shutdown-check-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = fs::remove_dir_all(&dir);
    fs::create_dir_all(&dir).unwrap();
    dir
}

fn write_state(base: &Path, incarnation: u64, word: &str) {
    fs::write(base, format!("{incarnation} {word}\n")).unwrap();
}

fn write_superblock(base: &Path, incarnation: u64, state: MarkerState) {
    let superblock = PathBuf::from(format!("{}.superblock", base.display()));
    marker::write(&superblock, incarnation, state)
        .expect("the vendored store writes the marker copy");
}

fn write_log(path: &Path, lines: &[impl AsRef<str>]) {
    let mut file = fs::File::create(path).unwrap();
    for line in lines {
        writeln!(file, "{}", line.as_ref()).unwrap();
    }
}

/// One clean stop cycle's records, the run logs' own shape: the begin
/// record carries the wall timestamp, the drain and flushed records
/// follow in write order.
fn clean_cycle(begin_ts: u64, node: &str) -> Vec<String> {
    vec![
        format!(
            " INFO status state=1 leader=2 era=1 view=37 config_era=1 voting=1 sidecar_drops=0 ts={begin_ts}"
        ),
        format!(" INFO sigterm: clean stop ts={begin_ts}"),
        format!(" INFO stop: the wire is closed, the in-memory state is final node={node}"),
        format!(
            " INFO stop: drained and flushed; the next boot continues under the same incarnation node={node}"
        ),
    ]
}

/// The canonical clean stop: the log ends on the flushed record and the
/// marker copies agree at flushed. The verdict is consistent.
#[test]
fn a_clean_stop_cross_checks_consistent() {
    let dir = temp_dir("clean");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "flushed");
    write_superblock(&base, 3, MarkerState::Flushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Consistent, "{}", report.text);
    assert!(
        report.text.contains("OK [n1] consistent"),
        "the node reads consistent: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// The spec's planted inconsistency: a log carrying
/// "drained and flushed" as the node's final stop record, over a
/// superblock whose final state is NOT flushed — the marker copies sit
/// at unflushed. The finding names the log line, the record's
/// timestamp, and both copy states.
#[test]
fn the_planted_flush_claim_against_an_unflushed_marker_is_reported() {
    let dir = temp_dir("planted");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "unflushed");
    write_superblock(&base, 3, MarkerState::Unflushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report
            .text
            .contains("INCONSISTENCY [n1] log-claims-flush-marker-not-flushed"),
        "the flush claim is named: {}",
        report.text
    );
    assert!(
        report.text.contains("drained and flushed"),
        "the log line is quoted: {}",
        report.text
    );
    assert!(
        report.text.contains("unflushed at incarnation 3"),
        "the superblock copy state is named: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A flushed single-file projection over an unflushed quorum is its own
/// inconsistency: the projection is written only after the quorum write
/// succeeded.
#[test]
fn the_projection_ahead_of_the_quorum_is_reported() {
    let dir = temp_dir("projection");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "flushed");
    write_superblock(&base, 3, MarkerState::Unflushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report
            .text
            .contains("INCONSISTENCY [n1] projection-ahead-of-quorum"),
        "the projection-ahead rule fires: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A clean-flush claim with no superblock file at all is an
/// inconsistency: nothing on disk vouches for the flush.
#[test]
fn the_flush_claim_without_a_superblock_is_reported() {
    let dir = temp_dir("no-superblock");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "unflushed");

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report
            .text
            .contains("INCONSISTENCY [n1] log-claims-flush-no-superblock"),
        "the missing superblock is named: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// The reverse direction: the marker vouches for flushed with no stop
/// record in the node's logs.
#[test]
fn the_flushed_marker_without_a_stop_record_is_reported() {
    let dir = temp_dir("reverse");
    let log = dir.join("n1.2026-09-18.log");
    write_log(
        &log,
        &[
            " INFO status state=1 leader=2 era=1 view=37 config_era=1 voting=1 sidecar_drops=0 ts=1789603440714",
            " WARN peer input dropped with a named diagnostic diagnostic=SlotNotOutstanding",
        ],
    );
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "flushed");
    write_superblock(&base, 3, MarkerState::Flushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report
            .text
            .contains("INCONSISTENCY [n1] marker-without-stop-record"),
        "the reverse direction fires: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// The stop path's records are ordered: a flushed record before the
/// drain record inverts the write order and is flagged.
#[test]
fn the_stop_path_out_of_order_is_flagged() {
    let dir = temp_dir("ordering");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(
        &log,
        &[
            format!(" INFO sigterm: clean stop ts={begin_ts}"),
            " INFO stop: drained and flushed; the next boot continues under the same incarnation node=1".to_string(),
            " INFO stop: the wire is closed, the in-memory state is final node=1".to_string(),
        ],
    );
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "flushed");
    write_superblock(&base, 3, MarkerState::Flushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report
            .text
            .contains("INVERSION [n1] stop-path-out-of-order"),
        "the inversion is named: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Work the runtime logged after the persist order completed is an
/// inversion: the node kept working after the stop path said it was
/// done.
#[test]
fn the_work_after_the_persist_order_is_flagged() {
    let dir = temp_dir("work-after");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    let mut lines = clean_cycle(begin_ts, "1");
    lines.push(
        " INFO status state=2 leader=1 era=1 view=37 config_era=1 voting=1 sidecar_drops=0 ts=1789603449941".to_string(),
    );
    write_log(&log, &lines);
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "flushed");
    write_superblock(&base, 3, MarkerState::Flushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report.text.contains("INVERSION [n1] work-after-persist"),
        "the post-persist work is named: {}",
        report.text
    );
    assert!(
        report.text.contains("line 5"),
        "the offending line number is named: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A stop cycle followed by a later life (a boot record, then a later
/// crashed existence) is superseded: the final marker belongs to the
/// later life and the earlier flushed record is not contradicted by it.
#[test]
fn a_later_life_supersedes_the_earlier_stop_cycle() {
    let dir = temp_dir("later-life");
    let log = dir.join("n1.2026-09-18.log");
    let mut lines = clean_cycle(1789603443941u64, "1");
    lines.push(
        " INFO boot name=n1 descriptor-id=1 own=1 incarnation=0 ts=1789603149868".to_string(),
    );
    lines.push(" INFO status state=1 leader=2 era=1 view=37 config_era=1 voting=1 sidecar_drops=0 ts=1789603159868".to_string());
    write_log(&log, &lines);
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 4, "unflushed");
    write_superblock(&base, 4, MarkerState::Unflushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Consistent, "{}", report.text);
    let _ = fs::remove_dir_all(&dir);
}

/// The two marker copies must name the same identity: a single-file
/// marker one incarnation ahead of the superblock is an inconsistency.
#[test]
fn the_identity_mismatch_between_marker_copies_is_reported() {
    let dir = temp_dir("identity");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 4, "flushed");
    write_superblock(&base, 3, MarkerState::Flushed);

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report.text.contains("INCONSISTENCY [n1] identity-mismatch"),
        "the identity mismatch is named: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// A rotted superblock is an inconsistency, never a silent pass.
#[test]
fn the_unreadable_superblock_is_reported() {
    let dir = temp_dir("rotted");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "unflushed");
    fs::write(
        PathBuf::from(format!("{}.superblock", base.display())),
        vec![0u8; 4096],
    )
    .unwrap();

    let report = check_shutdown(&dir).expect("the check runs");
    assert_eq!(report.verdict, Verdict::Inconsistent, "{}", report.text);
    assert!(
        report
            .text
            .contains("INCONSISTENCY [n1] unreadable-superblock"),
        "the rotted copies are named: {}",
        report.text
    );
    let _ = fs::remove_dir_all(&dir);
}

/// Reading from the snapshot archive equals reading from the raw run
/// directory: the same report text, the same verdict.
#[test]
fn the_archive_read_equals_the_raw_read() {
    let dir = temp_dir("archive");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "flushed");
    write_superblock(&base, 3, MarkerState::Flushed);
    fs::write(
        dir.join("anchors.md"),
        "## teardown ts=2026-09-18T10:00:06Z\n",
    )
    .unwrap();

    let raw = check_shutdown(&dir).expect("the raw read runs");

    let archive = dir.join("run.tar.gz");
    let file = fs::File::create(&archive).unwrap();
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut builder = tar::Builder::new(encoder);
    builder
        .append_dir_all(".", &dir)
        .expect("the archive packs");
    builder.into_inner().unwrap().finish().unwrap();

    let archived = check_shutdown(&archive).expect("the archive read runs");

    assert_eq!(raw.verdict, archived.verdict);
    assert_eq!(raw.text, archived.text, "the reports are identical");
    let _ = fs::remove_dir_all(&dir);
}

/// A snapshot_run.sh archive (the system tar's own bytes) reads exactly
/// like the raw directory: the check is transparent over the tool's
/// archive format.
#[test]
fn the_snapshot_tool_archive_reads_like_the_raw_dir() {
    let dir = temp_dir("snapshot-archive");
    let begin_ts = 1789603443941u64;
    let log = dir.join("n1.2026-09-18.log");
    write_log(&log, &clean_cycle(begin_ts, "1"));
    let base = dir.join("state").join("n1.state");
    fs::create_dir_all(dir.join("state")).unwrap();
    write_state(&base, 3, "flushed");
    write_superblock(&base, 3, MarkerState::Flushed);

    let raw = check_shutdown(&dir).expect("the raw read runs");

    // The same gzip tar snapshot_run.sh produces: bsdtar -czf with the
    // run dir's contents at the archive root. The archive lands outside
    // the run dir, as snapshot_run.sh's own default does.
    let archive = std::env::temp_dir().join(format!(
        "skaffold-shutdown-check-snapshot-{}-{}.tar.gz",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos(),
    ));
    let _ = fs::remove_file(&archive);
    let status = std::process::Command::new("tar")
        .arg("-czf")
        .arg(&archive)
        .arg("-C")
        .arg(&dir)
        .arg(".")
        .status()
        .expect("the system tar runs");
    assert!(status.success(), "the system tar packed the run dir");

    let archived = check_shutdown(&archive).expect("the archive read runs");

    assert_eq!(raw.verdict, archived.verdict);
    assert_eq!(raw.text, archived.text);
    let _ = fs::remove_dir_all(&dir);
    let _ = fs::remove_file(&archive);
}

/// An input that is neither a directory nor a gzip archive is a usage
/// error, reported loudly.
#[test]
fn an_unreadable_input_is_a_loud_error() {
    let dir = temp_dir("bad-input");
    let plain = dir.join("not-an-archive.txt");
    fs::write(&plain, b"just text").unwrap();
    let error = check_shutdown(&plain).expect_err("a plain file is refused");
    assert!(
        error.contains("not a run directory and not a gzip archive"),
        "the refusal names the shape: {error}"
    );
    let _ = fs::remove_dir_all(&dir);
}
