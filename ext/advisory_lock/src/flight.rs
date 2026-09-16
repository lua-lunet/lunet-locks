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
//!   a prod release. It logs EVERYTHING with no size-reduction mechanism
//!   — it may consume a LOT of disk; that is accepted, by design. The
//!   node's crash plus its flight recording is the black box: no
//!   guessing is required to see what tripped what.
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
//! One JSONL file per node (`flight-<node_id>.jsonl` in the directory the
//! `LUNET_FLIGHT_RECORDER_DIR` env names at node construction), appended
//! on restart so one node's tape spans its whole life in that directory.
//! Every append is flushed: a crash must not lose the events that
//! explain it.

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

/// One node's flight recorder: the append-only JSONL tape.
pub struct FlightRecorder {
    writer: Option<BufWriter<File>>,
    path: PathBuf,
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
    /// `<dir>/flight-<node_id>.jsonl` and writes the header record. The
    /// header rides FIRST in the file's life; an append on restart adds a
    /// fresh header line — each boot names its own commit.
    pub fn open(dir: &Path, node_id: u32) -> std::io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let path = dir.join(format!("flight-{node_id}.jsonl"));
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        let mut recorder = Self {
            writer: Some(BufWriter::new(file)),
            path,
            seq: 0,
        };
        recorder.write_line(&Self::header(node_id));
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
    /// events that explain it.
    pub fn event(&mut self, kind: &str, detail: Value) {
        let line = json!({
            "seq": self.seq + 1,
            "kind": kind,
            "ts_ms": unix_millis(),
            "detail": detail,
        });
        self.seq += 1;
        self.write_line(&line);
    }

    fn write_line(&mut self, value: &Value) {
        let Some(writer) = self.writer.as_mut() else {
            return; // poisoned earlier: the tape stopped, the node runs on
        };
        // A JSON object serializes with no newline inside; one line is
        // one record.
        if writeln!(writer, "{value}").is_err() || writer.flush().is_err() {
            self.writer = None;
        }
    }
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
