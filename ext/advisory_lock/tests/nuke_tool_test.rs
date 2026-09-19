//! The `lunet_locks_nuke` admin tool's law, proven by driving the real
//! binary (via `CARGO_BIN_EXE_lunet_locks_nuke`) against real marker
//! stores in tempdirs — one test per law clause: it NEVER panics (even
//! on a corrupt store), shows corruption labelled with the verdict line,
//! defaults every write to NO, is non-interactive under
//! `--dangerously-skip-review` (+ `-q`), exits 66 on a missing path,
//! takes state NAMES (never numbers), reprints nothing after a write,
//! and reads the padded on-block state string. Real code, real
//! superblock files.

use std::fs;
use std::io::{Read, Seek, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};
use std::time::{SystemTime, UNIX_EPOCH};

use lunet_locks_aof::marker as marker_ffi;

fn workdir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after Unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "lunet-nuke-tool-{name}-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("tempdir root");
    dir
}

/// Drives the real binary. `stdin: None` closes stdin (EOF — the
/// prompt's default-NO path); `Some(bytes)` feeds an answer.
fn run_tool(args: &[&str], stdin: Option<&[u8]>) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_lunet_locks_nuke"));
    command
        .args(args)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    match stdin {
        Some(answer) => {
            command.stdin(Stdio::piped());
            let mut child = command.spawn().expect("the tool spawns");
            child
                .stdin
                .as_mut()
                .expect("the tool's stdin")
                .write_all(answer)
                .expect("the prompt's answer");
            child.wait_with_output().expect("the tool exits")
        }
        None => {
            command.stdin(Stdio::null());
            command.output().expect("the tool exits")
        }
    }
}

fn stdout(out: &Output) -> String {
    String::from_utf8_lossy(&out.stdout).to_string()
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).to_string()
}

/// A real store via the vendored Zig FFI: the four-copy superblock plus
/// the single-file projection.
fn seed_store(dir: &Path, incarnation: u64, state: marker_ffi::MarkerState) -> PathBuf {
    let superblock = dir.join("state.superblock");
    marker_ffi::write(&superblock, incarnation, state).expect("the store's first write");
    fs::write(
        dir.join("state"),
        format!("{} {}\n", incarnation, state.name()),
    )
    .expect("the projection");
    dir.join("state")
}

/// Rots one copy zone with 0xA5 bytes (the boot read's canonical
/// corruption).
fn corrupt_copy(superblock: &Path, slot: usize, geometry: marker_ffi::Geometry) {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(superblock)
        .expect("the copies file");
    file.seek(SeekFrom::Start((geometry.copy_size * slot) as u64))
        .expect("the zone seek");
    let rot = vec![0xA5u8; geometry.copy_size];
    file.write_all(&rot).expect("the corrupt bytes");
}

/// One copy zone's raw bytes as they stand on disk.
fn read_zone(superblock: &Path, slot: usize, geometry: marker_ffi::Geometry) -> Vec<u8> {
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(superblock)
        .expect("the copies file");
    let offset = (geometry.copy_size * slot) as u64;
    file.seek(SeekFrom::Start(offset)).expect("seek zone");
    let zone_len = usize::try_from(file.metadata().expect("stat").len() - offset)
        .unwrap_or(0)
        .min(geometry.copy_size);
    let mut zone = vec![0u8; zone_len];
    file.read_exact(&mut zone).expect("read zone");
    zone
}

fn write_zone(superblock: &Path, slot: usize, bytes: &[u8], geometry: marker_ffi::Geometry) {
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(superblock)
        .expect("the copies file");
    file.seek(SeekFrom::Start((geometry.copy_size * slot) as u64))
        .expect("seek zone");
    file.write_all(bytes).expect("write zone");
}

/// THE LAW: the tool NEVER panics — a store with corrupt bytes prints
/// (labelled) and exits 0: the investigation succeeded.
#[test]
fn a_corrupt_store_prints_and_never_panics() {
    let root = workdir("no-panic");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    let geometry = marker_ffi::geometry().expect("geometry");
    corrupt_copy(&dir.join("state.superblock"), 1, geometry);

    let out = run_tool(&[state.to_str().expect("path")], None);

    assert_eq!(
        out.status.code(),
        Some(0),
        "a corrupt store shown is a success"
    );
    let out_stdout = stdout(&out);
    assert!(
        !out_stdout.contains("panicked"),
        "no panic text: {out_stdout}"
    );
    assert!(
        !out_stdout.contains("thread "),
        "no panic text: {out_stdout}"
    );
    let out_stderr = stderr(&out);
    assert!(
        !out_stderr.contains("panicked"),
        "no panic text: {out_stderr}"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: the corruption is SHOWN and SAID — per-copy `BAD CHECKSUM`
/// rows plus the overall `verdict: CORRUPT` line, and the corrupt copy's
/// garbage never prints as fake decimal sequence/incarnation facts.
#[test]
fn a_corrupt_store_is_labelled_per_copy_and_in_the_verdict() {
    let root = workdir("corrupt-label");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    let geometry = marker_ffi::geometry().expect("geometry");
    corrupt_copy(&dir.join("state.superblock"), 1, geometry);

    let out = run_tool(&[state.to_str().expect("path")], None);
    let out_stdout = stdout(&out);

    assert!(
        out_stdout.contains("BAD CHECKSUM"),
        "the per-copy label: {out_stdout}"
    );
    assert!(
        out_stdout.contains("verdict: CORRUPT (copy 1: bad checksum)"),
        "the overall verdict: {out_stdout}"
    );
    let corrupt_row = out_stdout
        .lines()
        .find(|line| line.contains("BAD CHECKSUM"))
        .expect("the corrupt copy's row");
    assert!(
        corrupt_row.contains("raw=0x"),
        "the raw bytes as hex: {corrupt_row}"
    );
    assert!(
        !corrupt_row.contains("sequence="),
        "no fake decimal sequence fact: {corrupt_row}"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: the prompt defaults to NO — EOF (or anything but y/Y)
/// declines, exit 0, nothing written, and the prompt lives on STDERR
/// (stdout carries data only, POSIX `rm -i` style).
#[test]
fn the_write_prompt_defaults_to_no() {
    let root = workdir("default-no");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    let superblock = dir.join("state.superblock");
    let before = fs::read(&superblock).expect("the copies before");

    let out = run_tool(
        &[
            state.to_str().expect("path"),
            "--set-state",
            "flushed",
            "--set-incarnation",
            "9",
        ],
        None,
    );

    assert_eq!(
        out.status.code(),
        Some(0),
        "a declined prompt is not an error"
    );
    assert_eq!(
        fs::read(&superblock).expect("the copies after"),
        before,
        "nothing was written"
    );
    let out_stderr = stderr(&out);
    assert!(
        out_stderr.contains("proceed with the reset? [y/N]"),
        "the prompt on stderr: {out_stderr}"
    );
    assert!(
        out_stderr.contains("aborted: not confirmed"),
        "the abort notice on stderr: {out_stderr}"
    );
    assert!(
        !stdout(&out).contains("proceed with the reset?"),
        "no prompt text on stdout"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: `--dangerously-skip-review` + `-q` is fully non-interactive
/// (the flag IS the consent): stdin closed, write lands, stdout EMPTY,
/// exit 0. The tool's OWN stderr is silent on success — the only stderr
/// content is the Zig store's internal `debug(superblock_quorums)`
/// read-back logs (the store logs its own reads; that is the store's
/// voice, not the tool's output).
#[test]
fn skip_review_with_quiet_is_fully_non_interactive() {
    let root = workdir("skip-quiet");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);

    let out = run_tool(
        &[
            state.to_str().expect("path"),
            "--set-state",
            "stopped",
            "--set-incarnation",
            "4",
            "--dangerously-skip-review",
            "-q",
        ],
        None,
    );

    assert_eq!(out.status.code(), Some(0));
    assert!(out.stdout.is_empty(), "quiet writes nothing to stdout");
    let out_stderr = stderr(&out);
    assert!(
        !out_stderr.contains("lunet_locks_nuke:"),
        "the tool says nothing on stderr: {out_stderr}"
    );
    assert!(
        !out_stderr.contains("panicked"),
        "no panic text: {out_stderr}"
    );
    let classified = marker_ffi::classify(&dir.join("state.superblock")).expect("the store reads");
    assert_eq!(
        classified,
        marker_ffi::Classified {
            state: marker_ffi::MarkerState::Stopped,
            incarnation: 4,
        },
        "the write landed"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: a missing file/folder is a SPECIFIC nonzero exit (66,
/// `EX_NOINPUT`), a one-line stderr error, no stdout data.
#[test]
fn a_missing_path_is_a_specific_nonzero_exit() {
    let root = workdir("missing");
    let missing = root.join("nowhere").join("state");

    let out = run_tool(&[missing.to_str().expect("path")], None);

    assert_eq!(out.status.code(), Some(66), "EX_NOINPUT");
    assert!(out.stdout.is_empty(), "no stdout data on a not-found error");
    let out_stderr = stderr(&out);
    assert!(
        out_stderr.contains("no such file or directory"),
        "the one-line error: {out_stderr}"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: the write takes the stringified state NAME, never a number —
/// a numeric or unknown name is a usage error (exit 2) naming the
/// accepted names, the store untouched; `running` resolves the on-disk
/// running sentinel.
#[test]
fn the_write_argument_takes_names_not_numbers() {
    let root = workdir("names");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    let superblock = dir.join("state.superblock");

    for name in ["2", "bogus"] {
        let before = fs::read(&superblock).expect("the copies before");
        let out = run_tool(
            &[
                state.to_str().expect("path"),
                "--set-state",
                name,
                "--dangerously-skip-review",
            ],
            None,
        );
        assert_eq!(out.status.code(), Some(2), "a usage error for {name:?}");
        let out_stderr = stderr(&out);
        assert!(
            out_stderr.contains("running | unflushed | stopped | flushed"),
            "the accepted names: {out_stderr}"
        );
        assert_eq!(
            fs::read(&superblock).expect("the copies after"),
            before,
            "{name:?} wrote nothing"
        );
    }

    let out = run_tool(
        &[
            state.to_str().expect("path"),
            "--set-state",
            "running",
            "--dangerously-skip-review",
        ],
        None,
    );
    assert_eq!(out.status.code(), Some(0), "the documented alias works");
    let classified = marker_ffi::classify(&superblock).expect("the store reads");
    assert_eq!(
        classified,
        marker_ffi::Classified {
            state: marker_ffi::MarkerState::Unflushed,
            incarnation: 3,
        },
        "running maps to the on-disk running sentinel"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: after a write there is NO reprint of what it wrote — the
/// operator re-runs with no args to confirm. (The Zig store already
/// verified the write by its own 3/4 read-back; a reprint adds nothing.)
#[test]
fn a_write_reprints_nothing() {
    let root = workdir("no-reprint");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    let superblock = dir.join("state.superblock");
    let before = fs::read(&superblock).expect("the copies before");

    let out = run_tool(
        &[
            state.to_str().expect("path"),
            "--set-state",
            "flushed",
            "--set-incarnation",
            "9",
        ],
        Some(b"y\n"),
    );

    assert_eq!(out.status.code(), Some(0));
    assert!(!stdout(&out).contains("wrote:"), "no post-write reprint");
    assert_ne!(
        fs::read(&superblock).expect("the copies after"),
        before,
        "the write landed"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: the padded on-block state string reads in the print — each
/// copy row carries the state string as it stands ON the block (the
/// fixed-width, space-padded name a raw hexdump reads).
#[test]
fn the_on_block_state_string_reads_in_the_print() {
    let root = workdir("padded");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 7, marker_ffi::MarkerState::Flushed);

    let out = run_tool(&[state.to_str().expect("path")], None);
    let out_stdout = stdout(&out);

    assert_eq!(out.status.code(), Some(0));
    assert!(
        out_stdout.contains("state=flushed"),
        "the shared-table name: {out_stdout}"
    );
    assert!(
        out_stdout.contains("state_string=\"flushed\""),
        "the padded on-block string: {out_stdout}"
    );

    let _ = fs::remove_dir_all(&root);
}

/// The torn spread: checksum-valid copies at differing states print the
/// `verdict: TORN` line — the law's disagreement clause, said loudly.
#[test]
fn a_torn_spread_prints_the_torn_verdict() {
    let root = workdir("torn");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    let superblock = dir.join("state.superblock");
    let geometry = marker_ffi::geometry().expect("geometry");

    // The restore-zones trick: one copy advances past the others (a
    // death after exactly one copy of the next write), the rest stand at
    // the older generation — every copy checksum-valid, the spread torn.
    let older: Vec<Vec<u8>> = (1..geometry.copies)
        .map(|slot| read_zone(&superblock, slot, geometry))
        .collect();
    marker_ffi::write(&superblock, 3, marker_ffi::MarkerState::Unflushed)
        .expect("the next transition's write");
    for (index, snapshot) in older.iter().enumerate() {
        write_zone(&superblock, index + 1, snapshot, geometry);
    }

    let out = run_tool(&[state.to_str().expect("path")], None);
    let out_stdout = stdout(&out);

    assert_eq!(out.status.code(), Some(0));
    assert!(
        out_stdout.contains("verdict: TORN (working copies disagree)"),
        "the torn verdict: {out_stdout}"
    );

    let _ = fs::remove_dir_all(&root);
}

/// The overturned item25.20 call: a commanded write over a corrupt store
/// PROCEEDS (the tool is explicit operator intent, outside the
/// never-self-heal rule), and the re-run shows the fresh store — the
/// padded `flushed` string on the blocks, sequence 1, verdict OK.
#[test]
fn an_operator_write_proceeds_over_a_corrupt_store() {
    let root = workdir("over-corrupt");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    let geometry = marker_ffi::geometry().expect("geometry");
    corrupt_copy(&dir.join("state.superblock"), 1, geometry);

    let out = run_tool(
        &[
            state.to_str().expect("path"),
            "--set-state",
            "flushed",
            "--set-incarnation",
            "9",
            "--dangerously-skip-review",
        ],
        None,
    );
    assert_eq!(
        out.status.code(),
        Some(0),
        "the corrupt store does not refuse the operator: {}",
        stderr(&out)
    );

    // The operator re-runs with no args to confirm: a fresh store.
    let out = run_tool(&[state.to_str().expect("path")], None);
    let out_stdout = stdout(&out);
    assert_eq!(out.status.code(), Some(0));
    assert!(
        out_stdout.contains("sequence=1"),
        "fresh format: {out_stdout}"
    );
    assert!(
        out_stdout.contains("state=flushed"),
        "the named state: {out_stdout}"
    );
    assert!(
        out_stdout.contains("state_string=\"flushed\""),
        "the padded on-block string: {out_stdout}"
    );
    assert!(
        out_stdout.contains("verdict: OK"),
        "unanimous again: {out_stdout}"
    );
    assert_eq!(
        fs::read_to_string(&state).expect("the projection"),
        "9 flushed\n",
        "the projection reset to the same identity"
    );

    let _ = fs::remove_dir_all(&root);
}

/// THE LAW: it NEVER panics on a storage failure either — a vanished
/// parent dir is a one-line stderr message + exit 1, no panic text.
#[test]
fn a_storage_failure_is_a_one_liner_never_a_panic() {
    let root = workdir("storage");
    let dir = root.join("store");
    fs::create_dir_all(&dir).expect("store dir");
    let state = seed_store(&dir, 3, marker_ffi::MarkerState::Flushed);
    fs::remove_dir_all(&dir).expect("the store's parent dir vanishes");

    let out = run_tool(
        &[
            state.to_str().expect("path"),
            "--set-state",
            "flushed",
            "--set-incarnation",
            "1",
            "--dangerously-skip-review",
            "-q",
        ],
        None,
    );

    assert_eq!(out.status.code(), Some(1), "a real failure, not a panic");
    assert!(out.stdout.is_empty(), "quiet keeps stdout empty");
    let out_stderr = stderr(&out);
    assert!(!out_stderr.is_empty(), "the one-line stderr message");
    assert!(
        !out_stderr.contains("panicked"),
        "no panic text: {out_stderr}"
    );

    let _ = fs::remove_dir_all(&root);
}
