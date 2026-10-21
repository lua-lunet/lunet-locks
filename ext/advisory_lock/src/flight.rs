//! The Flight Recorder (the `flight-recorder` feature): a per-node
//! INTERNAL trace — every message byte-exact, every drive outcome, the
//! private events — under the operator's 200 MiB cap
//! ([`FLIGHT_RETENTION_BYTES`]). Debug-level: a recording is readable
//! only by code as-at its commit.

use serde_json::{Value, json};

use crate::disk::{Disk, DiskFile, std_disk};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// The commit hash this build was compiled from; the reader path refuses
/// anything else.
pub const FLIGHT_COMMIT: &str = env!("FLIGHT_COMMIT");

/// Whether this build came from a dirty tree.
pub fn flight_dirty() -> bool {
    option_env!("FLIGHT_DIRTY").is_some_and(|value| value == "1")
}

/// The recording format's version; a reader that does not match refuses.
pub const FLIGHT_FORMAT: u64 = 1;

/// The env var that names the flight directory at node construction.
pub const FLIGHT_DIR_ENV: &str = "LUNET_FLIGHT_RECORDER_DIR";

/// The operator's cap on one node's flight-tape series: 200 MiB on disk.
pub const FLIGHT_RETENTION_BYTES: u64 = 200 * 1024 * 1024;

/// The rotation threshold: half the cap.
pub const FLIGHT_ROTATION_BYTES: u64 = FLIGHT_RETENTION_BYTES / 2;

fn active_name(node_id: u32) -> String {
    format!("flight-{node_id}.jsonl")
}

fn rotation_bytes(retention: u64) -> u64 {
    std::cmp::max(retention / 2, 1)
}

/// One rotated (epoch-named) flight history file: its (epoch, sequence)
/// sort key and its size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightHistoryFile {
    pub epoch: u64,
    pub seq: u64,
    pub name: String,
    pub size: u64,
}

/// The history file's name for a rotation; same-millisecond collisions
/// pick a sequence suffix so the create-NEW rule holds.
pub fn flight_history_name(node_id: u32, epoch: u64, seq: u64) -> String {
    match seq {
        0 => format!("flight-{node_id}-{epoch}.jsonl"),
        suffix => format!("flight-{node_id}-{epoch}-{suffix}.jsonl"),
    }
}

/// Parses a history file name into its (epoch, sequence) sort key;
/// `None` for anything that is not this node's rotated tape.
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
/// files in `dir`, oldest first, over the default disk.
pub fn list_flight_history(dir: &Path, node_id: u32) -> std::io::Result<Vec<FlightHistoryFile>> {
    list_flight_history_on(&*std_disk(), dir, node_id)
}

/// [`list_flight_history`] against a named disk.
pub fn list_flight_history_on(
    disk: &dyn Disk,
    dir: &Path,
    node_id: u32,
) -> std::io::Result<Vec<FlightHistoryFile>> {
    let mut result = Vec::new();
    for entry in disk.read_dir(dir)? {
        let path = entry.path();
        let name = path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy()
            .to_string();
        if let Some((epoch, seq)) = parse_flight_history_name(&name, node_id) {
            // Only a matched entry is stat'ed: a vanishing neighbour
            // never fails this listing.
            let size = disk.file_len(&path)?;
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

/// The sweep decision: which history files (oldest first) to delete so
/// the retained history fits the budget; the newest rotated file is the
/// floor.
pub fn flight_sweep_plan(
    files: &[FlightHistoryFile],
    active_size: u64,
    budget: u64,
) -> Vec<String> {
    let mut files = files.to_vec();
    files.sort_by_key(|file| std::cmp::Reverse((file.epoch, file.seq)));

    let Some(newest) = files.first() else {
        return Vec::new();
    };
    let mut sum = active_size + newest.size;
    sum += files[1..].iter().map(|file| file.size).sum::<u64>();

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
    writer: Option<BufWriter<DiskFile>>,
    dir: PathBuf,
    node_id: u32,
    path: PathBuf,
    /// The seam every filesystem operation in this recorder crosses.
    disk: Arc<dyn Disk>,
    retention: u64,
    bytes_written: u64,
    seq: u64,
}

impl FlightRecorder {
    /// Opens the node's recorder from the env var; `None` when unset or
    /// when the open fails (said loudly, the node keeps serving).
    pub fn open_from_env(node_id: u32) -> Option<Self> {
        let disk = std_disk();
        Self::open_from_env_on(&disk, node_id)
    }

    /// [`FlightRecorder::open_from_env`] against a named disk.
    pub fn open_from_env_on(disk: &Arc<dyn Disk>, node_id: u32) -> Option<Self> {
        let dir = std::env::var_os(FLIGHT_DIR_ENV)?;
        match Self::open_on(disk, Path::new(&dir), node_id) {
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

    /// Opens (or appends to) the node's flight tape under the operator's
    /// cap; an append on restart adds a fresh header line.
    pub fn open(dir: &Path, node_id: u32) -> std::io::Result<Self> {
        Self::open_capped(dir, node_id, FLIGHT_RETENTION_BYTES)
    }

    /// [`FlightRecorder::open`] against a named disk.
    pub fn open_on(disk: &Arc<dyn Disk>, dir: &Path, node_id: u32) -> std::io::Result<Self> {
        Self::open_capped_on(disk, dir, node_id, FLIGHT_RETENTION_BYTES)
    }

    /// [`FlightRecorder::open`] with an explicit cap (the tests' small
    /// tapes).
    pub fn open_capped(dir: &Path, node_id: u32, retention: u64) -> std::io::Result<Self> {
        Self::open_capped_on(&std_disk(), dir, node_id, retention)
    }

    /// [`FlightRecorder::open_capped`] against a named disk.
    pub fn open_capped_on(
        disk: &Arc<dyn Disk>,
        dir: &Path,
        node_id: u32,
        retention: u64,
    ) -> std::io::Result<Self> {
        disk.create_dir_all(dir)?;
        let path = dir.join(active_name(node_id));
        // Rotate-on-open: a tape already over the rotation threshold
        // becomes an epoch-named history file. Best effort — a failed
        // rename just keeps appending.
        if disk.file_len(&path).is_ok_and(|len| {
            len >= rotation_bytes(retention)
                && rename_to_history(&**disk, dir, node_id, &path).is_some()
        }) {}
        let file = disk.open_or_create_append(&path)?;
        let mut recorder = Self {
            writer: Some(BufWriter::new(file)),
            dir: dir.to_path_buf(),
            node_id,
            path,
            disk: Arc::clone(disk),
            retention,
            bytes_written: 0,
            seq: 0,
        };
        recorder.bytes_written = recorder
            .writer
            .as_ref()
            .and_then(|writer| writer.get_ref().file_len().ok())
            .unwrap_or(0);
        recorder.write_line(&Self::header(node_id));
        recorder.sweep();
        Ok(recorder)
    }

    /// The recording file's path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The first record: the commit hash, the dirty flag, the node id,
    /// the format version.
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

    /// Records one event. A write error poisons THIS recorder only (the
    /// tape stops, the node runs on); every line is flushed.
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
    /// a fresh tape on its own header; any failure poisons THIS recorder
    /// only.
    fn rotate(&mut self) {
        self.writer = None; // per-line flush means the tape is complete on disk
        if rename_to_history(&*self.disk, &self.dir, self.node_id, &self.path).is_none() {
            return;
        }
        match self.disk.open_or_create_append(&self.path) {
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
    /// never rolls away. A failure here is silent: the next rotation
    /// tries again.
    fn sweep(&mut self) {
        let Ok(history) = list_flight_history_on(&*self.disk, &self.dir, self.node_id) else {
            return;
        };
        let budget = self
            .retention
            .saturating_sub(rotation_bytes(self.retention));
        for name in flight_sweep_plan(&history, self.bytes_written, budget) {
            let _ = self.disk.remove_file(&self.dir.join(name));
        }
    }

    /// Writes one line, flushed. `false` when the recorder is poisoned.
    fn write_line(&mut self, value: &Value) -> bool {
        let Some(writer) = self.writer.as_mut() else {
            return false;
        };
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

/// Renames the active tape to the next epoch-named history name; the
/// (epoch, sequence) sort keys stay monotonic in creation order. `None`
/// on failure: the caller keeps the recorder poisoned.
fn rename_to_history(disk: &dyn Disk, dir: &Path, node_id: u32, active: &Path) -> Option<PathBuf> {
    let epoch = unix_millis();
    let existing = list_flight_history_on(disk, dir, node_id).ok()?;
    let suffix = existing
        .iter()
        .filter(|file| file.epoch == epoch)
        .map(|file| file.seq)
        .max()
        .map_or(0, |highest| highest + 1);
    let target = dir.join(flight_history_name(node_id, epoch, suffix));
    disk.rename(active, &target).ok()?;
    Some(target)
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as u64)
}

/// Byte-exact hex of a message (the tape's `frame_hex` encoding).
pub fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

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
    read_header_on(&*std_disk(), path)
}

/// [`read_header`] against a named disk.
pub fn read_header_on(disk: &dyn Disk, path: &Path) -> Result<FlightHeader, FlightReadError> {
    let text = disk
        .read_to_string(path)
        .map_err(|error| FlightReadError::Io(error.to_string()))?;
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
