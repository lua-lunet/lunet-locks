//! The Flight Recorder (the `flight-recorder` feature): a per-node
//! INTERNAL trace of everything the node does — every inbound and
//! outbound message byte-exact, every drive outcome, and the internal
//! private events the lock telemetry capture file never sees (lock-state
//! journal flushes, and which message tripped which assert / maybe /
//! arrest as the node stops).
//!
//! # The two planes, named (item12)
//!
//! - The **lock telemetry capture file** is the regular path's AOF — the
//!   Zig TigerBeetle-format series on the non-voting telemetry nodes —
//!   recording the public, wire-visible events our VanillaJS console
//!   observer reads. It is NOT this module.
//! - The **Flight Recorder** is the per-node internal trace: a
//!   debug-level tool, compiled in/out by the feature flag, never part of
//!   a prod release. It logs everything, bounded by the operator's cap:
//!   one node's tape series never exceeds 200 MiB on disk (the rotation
//!   and sweep below). The node's crash plus its flight recording is the
//!   black box: no guessing is required to see what tripped what.
//!
//! # The timeout-toggle event kind
//!
//! The `timeout-toggle` kind records every toggle of the host's
//! `timedout` state (`docs/src/phi-and-timeouts.md`): the new state,
//! the toggle's ts, and the ts of the LAST toggle. The host drives it
//! through `Node::note_timeout_toggle` on every state change, so the
//! phi/timeout story — detection standing down at the view change,
//! resuming at the fresh commit — is on the same tape as the drives and
//! messages it explains.
//!
//! # The 200 MiB cap (rotation + sweep)
//!
//! The ACTIVE tape `flight-<node_id>.jsonl` rotates to an epoch-named
//! history file `flight-<node_id>-<epoch>.jsonl` (same-second starts add
//! a `-1`, `-2`, ... disambiguator) when it passes
//! [`FLIGHT_ROTATION_BYTES`] — half the cap, so the active tape and the
//! retained history each own half of [`FLIGHT_RETENTION_BYTES`].
//! Rotation happens between flushed lines: every line is flushed before
//! the rename, the fresh tape opens on a fresh header record, and the
//! event sequence continues unbroken across files — the crash-evidence
//! property is intact. After every rotation (and at every open) the
//! history is swept: the newest rotated file is NEVER deleted, older
//! files roll away oldest-first while the retained history sum exceeds
//! the history budget. History is preserved as complete files for the
//! admin up to that budget; nothing is truncated mid-line. A rotation
//! or sweep failure poisons THIS recorder only — the tape stops, the
//! node runs on.
//!
//! # The commit gate
//!
//! The FIRST record of every flight recording is the header: the git
//! commit hash the recording build was compiled from (the build script's
//! `FLIGHT_COMMIT`), the dirty flag, the node id, and the format version.
//! A recording is readable ONLY by code as-at that commit: the reader
//! path (`FlightHeader::check_commit`) refuses a recording whose commit
//! the reading code is not and annotates a dirty-stamped recording
//! loudly. This is a debug-level contract — no long-term readability is
//! promised across commits.
//!
//! # File shape
//!
//! One active JSONL tape per node (`flight-<node_id>.jsonl` in the
//! directory the `LUNET_FLIGHT_RECORDER_DIR` env names at node
//! construction), appended on restart; an open onto a tape already over
//! the rotation threshold rotates it to an epoch-named history file
//! first, so the fresh boot records into a fresh tape. Every append is
//! flushed: a crash must not lose the events that explain it.

use serde_json::{Value, json};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// The commit hash this build was compiled from (the build script's
/// stamp). A flight recording's header names this; the reader path
/// refuses anything else.
pub const FLIGHT_COMMIT: &str = env!("FLIGHT_COMMIT");

/// Whether this build came from a dirty tree (the build-time override
/// `FLIGHT_RECORDER_ALLOW_DIRTY=1` still stamps it).
pub fn flight_dirty() -> bool {
    option_env!("FLIGHT_DIRTY").is_some_and(|value| value == "1")
}

/// The recording format's version. Debug-level tool: a reader that does
/// not match refuses (no long-term readability promise).
pub const FLIGHT_FORMAT: u64 = 1;

/// The env var that names the flight directory at node construction.
pub const FLIGHT_DIR_ENV: &str = "LUNET_FLIGHT_RECORDER_DIR";

/// The operator's cap on one node's flight-tape series: 200 MiB on disk
/// (active tape + retained history, never more, but for the one
/// in-flight line that triggers the next rotation).
pub const FLIGHT_RETENTION_BYTES: u64 = 200 * 1024 * 1024;

/// The rotation threshold: half the cap. The active tape rotates at
/// 100 MiB so the active tape and the retained history each own half of
/// the cap — the sweep then holds the history sum under the other half.
pub const FLIGHT_ROTATION_BYTES: u64 = FLIGHT_RETENTION_BYTES / 2;

/// The active tape's name.
fn active_name(node_id: u32) -> String {
    format!("flight-{node_id}.jsonl")
}

/// The rotation threshold for a given cap: half of it (at least one
/// byte, so a degenerate threshold still rotates).
fn rotation_bytes(retention: u64) -> u64 {
    std::cmp::max(retention / 2, 1)
}

/// One rotated (epoch-named) flight history file: its numeric epoch key
/// (the unix seconds of the rotation, with any `-n` same-second
/// disambiguator) and its size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightHistoryFile {
    pub epoch: u64,
    pub seq: u64,
    pub name: String,
    pub size: u64,
}

/// The epoch-named history file's name for a rotation at unix-epoch
/// milliseconds `epoch`. Same-millisecond collisions (many rotations in
/// one millisecond) pick a sequence suffix `{epoch}-1`, `-2`, ... so
/// the create-NEW rule holds and the sort keys stay monotonic in
/// creation order.
pub fn flight_history_name(node_id: u32, epoch: u64, seq: u64) -> String {
    match seq {
        0 => format!("flight-{node_id}-{epoch}.jsonl"),
        suffix => format!("flight-{node_id}-{epoch}-{suffix}.jsonl"),
    }
}

/// Parses a history file name into its (epoch, sequence) sort key for
/// THIS node. `None` for anything that is not this node's rotated tape
/// — the active tape, another node's series, junk: the sweep never
/// touches a foreign file.
pub fn parse_flight_history_name(name: &str, node_id: u32) -> Option<(u64, u64)> {
    let stem = name.strip_suffix(".jsonl")?;
    let rest = stem.strip_prefix(&format!("flight-{node_id}-"))?;
    let (epoch_text, seq) = match rest.split_once('-') {
        Some((epoch_text, seq_text)) => (epoch_text, seq_text.parse::<u64>().ok()?),
        None => (rest, 0),
    };
    let epoch = epoch_text.parse::<u64>().ok()?;
    Some((epoch, seq))
}

/// The (epoch, sequence)-sorted listing of one node's rotated history
/// files in `dir`, oldest first.
pub fn list_flight_history(dir: &Path, node_id: u32) -> std::io::Result<Vec<FlightHistoryFile>> {
    let mut result = Vec::new();
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if let Some((epoch, seq)) = parse_flight_history_name(&name, node_id) {
            let size = entry.metadata()?.len();
            result.push(FlightHistoryFile {
                epoch,
                seq,
                name,
                size,
            });
        }
    }
    result.sort_by_key(|file| (file.epoch, file.seq));
    Ok(result)
}

/// The sweep decision, exposed for TDD: which history files (oldest
/// first) to delete so the retained history fits the budget, never
/// dropping below "one active + one rotated" (the newest rotated file is
/// the floor). Boundary semantics mirror the telemetry retention's:
/// sum == budget keeps everything; one byte over deletes the oldest.
pub fn flight_sweep_plan(
    files: &[FlightHistoryFile],
    active_size: u64,
    budget: u64,
) -> Vec<String> {
    // Newest first (higher epoch wins; on a same-second collision the
    // later sequence number is the newer file).
    let mut files = files.to_vec();
    files.sort_by_key(|file| std::cmp::Reverse((file.epoch, file.seq)));

    // The newest rotated file always survives — this is the floor.
    let Some(newest) = files.first() else {
        return Vec::new();
    };
    let mut sum = active_size + newest.size;
    sum += files[1..].iter().map(|file| file.size).sum::<u64>();

    // Oldest-to-newest walk over the remainder: delete while the
    // retained sum is over the budget. The newest rotated file is never
    // in the walk — it is the floor.
    let mut deletions: Vec<String> = Vec::new();
    for file in files[1..].iter().rev() {
        if sum <= budget {
            break;
        }
        deletions.push(file.name.clone());
        sum -= file.size;
    }
    deletions
}

/// One node's flight recorder: the append-only JSONL tape.
pub struct FlightRecorder {
    writer: Option<BufWriter<File>>,
    dir: PathBuf,
    node_id: u32,
    path: PathBuf,
    retention: u64,
    bytes_written: u64,
    seq: u64,
}

impl FlightRecorder {
    /// Opens the node's recorder from the `LUNET_FLIGHT_RECORDER_DIR` env
    /// var. `None` when the var is unset (the recording build runs
    /// unrecorded — the operator opts a node in by pointing the var at a
    /// directory) or when the open fails (said loudly, the node keeps
    /// serving: the recorder never touches the replication path).
    pub fn open_from_env(node_id: u32) -> Option<Self> {
        let dir = std::env::var_os(FLIGHT_DIR_ENV)?;
        match Self::open(Path::new(&dir), node_id) {
            Ok(recorder) => Some(recorder),
            Err(error) => {
                eprintln!(
                    "lunet-advisory-lock: flight recorder open failed ({error}); \
                     this node runs unrecorded"
                );
                None
            }
        }
    }

    /// Opens (or appends to) the node's flight tape
    /// `<dir>/flight-<node_id>.jsonl` under the operator's 200 MiB cap.
    /// The header rides FIRST in each file's life; an append on restart
    /// adds a fresh header line — each boot names its own commit.
    pub fn open(dir: &Path, node_id: u32) -> std::io::Result<Self> {
        Self::open_capped(dir, node_id, FLIGHT_RETENTION_BYTES)
    }

    /// [`FlightRecorder::open`] with an explicit cap (the tests' small
    /// tapes); production rides the [`FLIGHT_RETENTION_BYTES`] default.
    pub fn open_capped(dir: &Path, node_id: u32, retention: u64) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(active_name(node_id));
        // Rotate-on-open (the telemetry capture's restart rotation): a
        // tape already over the rotation threshold becomes an
        // epoch-named history file; the fresh boot records into a fresh
        // tape. Best effort — a failed rename just keeps appending.
        if std::fs::metadata(&path).is_ok_and(|meta| {
            meta.len() >= rotation_bytes(retention)
                && rename_to_history(dir, node_id, &path).is_some()
        }) {
            // The old tape moved; the fresh open below starts empty.
        }
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let mut recorder = Self {
            writer: Some(BufWriter::new(file)),
            dir: dir.to_path_buf(),
            node_id,
            path,
            retention,
            bytes_written: 0,
            seq: 0,
        };
        recorder.bytes_written = recorder
            .writer
            .as_ref()
            .and_then(|writer| writer.get_ref().metadata().ok())
            .map_or(0, |meta| meta.len());
        recorder.write_line(&Self::header(node_id));
        recorder.sweep();
        Ok(recorder)
    }

    /// The recording file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The first record: the commit hash the recording build was
    /// compiled from, the dirty flag, the node id, and the format version.
    pub fn header(node_id: u32) -> Value {
        json!({
            "kind": "flight-header",
            "format": FLIGHT_FORMAT,
            "commit": FLIGHT_COMMIT,
            "dirty": flight_dirty(),
            "node": node_id,
            "pid": std::process::id(),
            "ts_ms": unix_millis(),
        })
    }

    /// Records one event. Never fails into the node path: a write error
    /// poisons THIS recorder only (the tape stops for the process, the
    /// node runs on). Every line is flushed — a crash must not lose the
    /// events that explain it. When the line pushes the active tape past
    /// the rotation threshold, the tape rotates (below).
    pub fn event(&mut self, kind: &str, detail: Value) {
        let line = json!({
            "seq": self.seq + 1,
            "kind": kind,
            "ts_ms": unix_millis(),
            "detail": detail,
        });
        self.seq += 1;
        if self.write_line(&line) && self.bytes_written >= rotation_bytes(self.retention) {
            self.rotate();
        }
    }

    /// Rotates the active tape to an epoch-named history file and opens
    /// a fresh tape on its own header. Between flushed lines: the
    /// rename moves the tape whole, the fresh tape continues the
    /// sequence unbroken, and the sweep then holds the history sum under
    /// the cap. Any failure poisons THIS recorder only.
    fn rotate(&mut self) {
        self.writer = None; // per-line flush means the tape is complete on disk
        if rename_to_history(&self.dir, self.node_id, &self.path).is_none() {
            return;
        }
        match OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
        {
            Ok(file) => {
                self.bytes_written = 0;
                self.writer = Some(BufWriter::new(file));
                self.write_line(&Self::header(self.node_id));
                self.sweep();
            }
            Err(_) => {
                self.writer = None;
            }
        }
    }

    /// Sweeps the rotated history under the cap: the newest rotated file
    /// never rolls away; older files go oldest-first while the retained
    /// history sum exceeds the history budget. A failure here is silent:
    /// the next rotation tries again.
    fn sweep(&mut self) {
        let Ok(history) = list_flight_history(&self.dir, self.node_id) else {
            return;
        };
        let budget = self
            .retention
            .saturating_sub(rotation_bytes(self.retention));
        for name in flight_sweep_plan(&history, self.bytes_written, budget) {
            let _ = std::fs::remove_file(self.dir.join(name));
        }
    }

    /// Writes one line, flushed. `false` when the recorder is poisoned
    /// (or just got poisoned): the tape stopped, the node runs on.
    fn write_line(&mut self, value: &Value) -> bool {
        let Some(writer) = self.writer.as_mut() else {
            return false; // poisoned earlier: the tape stopped, the node runs on
        };
        // A JSON object serializes with no newline inside; one line is
        // one record.
        let text = value.to_string();
        let written = text.len() + 1;
        if writer
            .write_all(text.as_bytes())
            .and_then(|_| writer.write_all(b"\n"))
            .is_err()
            || writer.flush().is_err()
        {
            self.writer = None;
            return false;
        }
        self.bytes_written += written as u64;
        true
    }
}

/// Renames the active tape to the next epoch-named history name. The
/// key is (unix-epoch MILLISECONDS, suffix): the suffix always exceeds
/// the highest existing suffix at that epoch, so the (epoch, sequence)
/// sort keys are monotonic in creation order even when many rotations
/// land inside one millisecond — the sweep's "newest survives" floor
/// never protects stale content. `None` on failure: the caller keeps
/// the recorder poisoned, the node runs on.
fn rename_to_history(dir: &Path, node_id: u32, active: &Path) -> Option<PathBuf> {
    let epoch = unix_millis();
    let existing = list_flight_history(dir, node_id).ok()?;
    let suffix = existing
        .iter()
        .filter(|file| file.epoch == epoch)
        .map(|file| file.seq)
        .max()
        .map_or(0, |highest| highest + 1);
    let target = dir.join(flight_history_name(node_id, epoch, suffix));
    std::fs::rename(active, &target).ok()?;
    Some(target)
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

/// Byte-exact hex of a message (the tape's `frame_hex` encoding, the same
/// convention the telemetry tape uses).
pub fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

// ------------------------------------------------------------- reader ----

/// One recording's header, read back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightHeader {
    pub commit: String,
    pub dirty: bool,
    pub node: u32,
    pub format: u64,
}

/// The reader path's refusal reasons.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightReadError {
    Io(String),
    MissingHeader,
    UnknownFormat(u64),
    CommitMismatch { recorded: String, reader: String },
}

impl std::fmt::Display for FlightReadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlightReadError::Io(error) => write!(f, "flight recording read failed: {error}"),
            FlightReadError::MissingHeader => {
                write!(f, "flight recording opens on no flight-header line")
            }
            FlightReadError::UnknownFormat(version) => {
                write!(
                    f,
                    "flight recording format {version} is unknown to this reader"
                )
            }
            FlightReadError::CommitMismatch { recorded, reader } => write!(
                f,
                "flight recording was recorded by commit {recorded}; this reader is \
                 commit {reader} — a recording is readable ONLY by the code as-at \
                 its commit (check out {recorded} to read it)"
            ),
        }
    }
}

impl std::error::Error for FlightReadError {}

/// Reads the recording's header: its FIRST line. `MissingHeader` when the
/// first line is not a `flight-header` (an empty or mangled file).
pub fn read_header(path: &Path) -> Result<FlightHeader, FlightReadError> {
    let text =
        std::fs::read_to_string(path).map_err(|error| FlightReadError::Io(error.to_string()))?;
    let first = text.lines().next().ok_or(FlightReadError::MissingHeader)?;
    let value: Value = serde_json::from_str(first).map_err(|_| FlightReadError::MissingHeader)?;
    if value.get("kind").and_then(|kind| kind.as_str()) != Some("flight-header") {
        return Err(FlightReadError::MissingHeader);
    }
    let unknown = FlightReadError::UnknownFormat(value["format"].as_u64().unwrap_or(0));
    let format = value["format"].as_u64().ok_or(unknown)?;
    if format != FLIGHT_FORMAT {
        return Err(FlightReadError::UnknownFormat(format));
    }
    Ok(FlightHeader {
        commit: value["commit"]
            .as_str()
            .ok_or(FlightReadError::MissingHeader)?
            .to_string(),
        dirty: value["dirty"].as_bool().unwrap_or(false),
        node: value["node"].as_u64().unwrap_or(0) as u32,
        format,
    })
}

/// The commit gate: a recording is readable ONLY by code as-at its
/// commit. `Ok` names the recorded commit; `Err(CommitMismatch)` refuses.
pub fn check_commit(header: &FlightHeader, reader_commit: &str) -> Result<String, FlightReadError> {
    if header.commit != reader_commit {
        return Err(FlightReadError::CommitMismatch {
            recorded: header.commit.clone(),
            reader: reader_commit.to_string(),
        });
    }
    Ok(header.commit.clone())
}

/// The reader path's commit facts for THIS build: what a caller passes to
/// [`check_commit`] as the reader's own commit.
pub fn reader_commit() -> &'static str {
    FLIGHT_COMMIT
}

#[cfg(test)]
#[path = "flight_test.rs"]
mod flight_test;
