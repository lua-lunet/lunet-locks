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
/// the single-file projection. The identity is the pair (system 1, the
/// named crash counter).
fn seed_store(dir: &Path, crash: u16, state: marker_ffi::MarkerState) -> PathBuf {
    let superblock = dir.join("state.superblock");
    marker_ffi::write(
        &superblock,
        marker_ffi::NodeIdentity::new(1, crash).expect("lawful pair"),
        state,
    )
    .expect("the store's first write");
    fs::write(dir.join("state"), format!("1 {crash} {}\n", state.name())).expect("the projection");
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
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: the corruption is SHOWN and SAID — per-copy `BAD CHECKSUM`
/// rows plus the overall `verdict: CORRUPT` line, and the corrupt copy's
/// garbage never prints as fake decimal sequence/incarnation facts.
#[test]
fn a_corrupt_store_is_labelled_per_copy_and_in_the_verdict() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: the prompt defaults to NO — EOF (or anything but y/Y)
/// declines, exit 0, nothing written, and the prompt lives on STDERR
/// (stdout carries data only, POSIX `rm -i` style).
#[test]
fn the_write_prompt_defaults_to_no() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: `--dangerously-skip-review` + `-q` is fully non-interactive
/// (the flag IS the consent): stdin closed, write lands, stdout EMPTY,
/// exit 0. The tool's OWN stderr is silent on success — the only stderr
/// content is the Zig store's internal `debug(superblock_quorums)`
/// read-back logs (the store logs its own reads; that is the store's
/// voice, not the tool's output).
#[test]
fn skip_review_with_quiet_is_fully_non_interactive() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: a missing file/folder is a SPECIFIC nonzero exit (66,
/// `EX_NOINPUT`), a one-line stderr error, no stdout data.
#[test]
fn a_missing_path_is_a_specific_nonzero_exit() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: the write takes the stringified state NAME, never a number —
/// a numeric or unknown name is a usage error (exit 2) naming the
/// accepted names, the store untouched; `running` resolves the on-disk
/// running sentinel.
#[test]
fn the_write_argument_takes_names_not_numbers() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: after a write there is NO reprint of what it wrote — the
/// operator re-runs with no args to confirm. (The Zig store already
/// verified the write by its own 3/4 read-back; a reprint adds nothing.)
#[test]
fn a_write_reprints_nothing() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: the padded on-block state string reads in the print — each
/// copy row carries the state string as it stands ON the block (the
/// fixed-width, space-padded name a raw hexdump reads).
#[test]
fn the_on_block_state_string_reads_in_the_print() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The torn spread: checksum-valid copies at differing states print the
/// `verdict: TORN` line — the law's disagreement clause, said loudly.
#[test]
fn a_torn_spread_prints_the_torn_verdict() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The operator's law: a commanded write over a corrupt store
/// PROCEEDS (the tool is explicit operator intent, outside the
/// never-self-heal rule), and the re-run shows the fresh store — the
/// padded `flushed` string on the blocks, sequence 1, verdict OK.
#[test]
fn an_operator_write_proceeds_over_a_corrupt_store() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// THE LAW: it NEVER panics on a storage failure either — a vanished
/// parent dir is a one-line stderr message + exit 1, no panic text.
#[test]
fn a_storage_failure_is_a_one_liner_never_a_panic() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
