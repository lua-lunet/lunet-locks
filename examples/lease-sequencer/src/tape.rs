//! The rig AOF tape: the `from,to,jsonl` streaming layer that
//! turns any telemetry AOF directory into a replay tape — one CSV line
//! per record, `from,to,{json}`, in file order (epoch order = ns order).
//!
//! The operator's model, verbatim: "stream the AOF as CSV as
//! `from,to,${jsonl}` then filter on `^${from},${to}` to get the raw
//! jsonl of the message, fire up one node and force feed it that replay
//! tape". The trivial shell filter `... | grep "^44,55,"` MUST work on
//! the plain output: `from` and `to` are the first two CSV fields and
//! the JSON starts after the second comma.
//!
//! # The from/to derivation (honest, printed in the bin's --help)
//!
//! The telemetry envelope carries no sender: the wire header's 20-byte
//! prefix is `tag(4 BE) era(4) view(4) slot(8)` and uVRR's wire contract
//! names no sender on the datagram. The tape derives the endpoint labels
//! from what each record actually carries:
//!
//! - marker-1 `Wire` (a raw uVRR datagram the recorder RECEIVED):
//!   `to` = the recorder's node id; `from` = the sender the recorder's
//!   own `commit-in` JSON log line names for the frame's
//!   `(era, view, slot)`, when `--logs` supplies the recorder's JSON
//!   log series (the observability contract: a standby logs one line
//!   per Commit it accepts from its leader, the sender in its `from`
//!   member) — the join claims Commit frames only, and a frame with no
//!   matching line keeps `?`, never guessed.
//! - marker-2 `TelemetryTimeoutDecision`: `from` = the recorder (the
//!   deciding node), `to` = the recorder.
//! - marker-3 `TelemetryStateTransition`: `from` = the recorder, `to` =
//!   the recorder.
//! - marker-4 `TelemetryOutbound`: `from` = the recorder, `to` = the
//!   record's target (its JSON `to` field).
//!
//! The recorder's node id is `--recorder N` on the bin; with the flag
//! omitted, recorder-side endpoints render as `?`.
//!
//! # The jsonl payload
//!
//! The third CSV field is the full parsed record as one JSON line:
//! `ts_ms` (the envelope's ns clock truncated to ms), `ns`, and for wire
//! records `tag`/`era`/`view`/`slot`, the committed frontier when the
//! frame carries one, the lock fields when the payload is a committed
//! lock verb, and `frame_hex` — the raw wire bytes hex-encoded:
//! byte-exact playback needs the original datagram.

use lunet_locks_aof::envelope::{Marker, Record};
use lunet_locks_aof::retention;
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

/// The wire tag table (uVRR `wire.rs`), for the line's honest `tag`.
pub fn tag_name(tag: u32) -> &'static str {
    match tag {
        2 => "prepare",
        3 => "prepare_ok",
        4 => "commit",
        5 => "start_view_change",
        6 => "do_view_change",
        7 => "start_view",
        8 => "planned_view_change",
        9 => "get_state",
        10 => "new_state",
        13 => "reincarnation",
        _ => "unknown",
    }
}

/// One streamed tape line: the derived endpoints plus the record's JSON.
#[derive(Debug, Clone, PartialEq)]
pub struct TapeLine {
    pub from: String,
    pub to: String,
    pub json: Value,
}

impl TapeLine {
    /// The CSV form: `from,to,{json}`. The JSON contains no newline
    /// (control characters are escaped by the re-serialization), so one
    /// line is one record.
    pub fn render(&self) -> String {
        format!("{},{},{}", self.from, self.to, self.json)
    }
}

/// The streaming options (the bin's flags).
#[derive(Debug, Clone, Default)]
pub struct TapeOptions {
    /// The recorder's node id. `None` leaves recorder-side endpoints
    /// rendered as `?`.
    pub recorder: Option<u32>,
    /// Keep only lines whose derived `from` equals this. `?` lines are
    /// dropped unless `from_any`.
    pub from: Option<u32>,
    pub from_any: bool,
    /// Keep only lines whose derived `to` equals this. `?` lines are
    /// dropped unless `to_any`.
    pub to: Option<u32>,
    pub to_any: bool,
    /// The recorder's JSON log series (the repeatable `--logs` flag): a
    /// log file, or a directory contributing its sorted `*.log` entries.
    /// The wire rows' sender is derived from the `commit-in` lines.
    pub logs: Vec<PathBuf>,
    /// The marker kinds to keep; empty keeps every kind.
    pub kinds: Vec<Kind>,
}

/// The tape's kinds (the --kinds union).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Wire,
    Decision,
    Transition,
    Outbound,
}

impl Kind {
    pub fn parse(text: &str) -> Option<Kind> {
        match text {
            "wire" => Some(Kind::Wire),
            "decision" => Some(Kind::Decision),
            "transition" => Some(Kind::Transition),
            "outbound" => Some(Kind::Outbound),
            "all" => None, // callers expand; parsed per-token via all()
            _ => None,
        }
    }

    pub fn all() -> Vec<Kind> {
        vec![Kind::Wire, Kind::Decision, Kind::Transition, Kind::Outbound]
    }
}

/// One parsed uVRR datagram body: the fields the tape line carries.
struct WireBody {
    tag: u32,
    era: u32,
    view: u32,
    slot: u64,
    /// The committed frontier a Prepare or Commit carries.
    committed: Option<u64>,
    /// The lock fields when the payload is a lock verb's JSON.
    lock: Option<Map<String, Value>>,
}

/// Parses one uVRR frame (trailer already stripped) into its body
/// fields. `None` when the frame is too short to carry the 20-byte
/// header — the parser never guesses past what the bytes carry.
fn wire_body(front: &[u8]) -> Option<WireBody> {
    if front.len() < 20 {
        return None;
    }
    let tag = u32::from_be_bytes(front[0..4].try_into().expect("4 bytes"));
    let era = u32::from_be_bytes(front[4..8].try_into().expect("4 bytes"));
    let view = u32::from_be_bytes(front[8..12].try_into().expect("4 bytes"));
    let slot = u64::from_be_bytes(front[12..20].try_into().expect("8 bytes"));
    let mut body = WireBody {
        tag,
        era,
        view,
        slot,
        committed: None,
        lock: None,
    };
    // The discriminated body: byte 20 repeats the tag's low byte.
    if front.len() >= 21 && front[20] != (tag & 0xFF) as u8 {
        return Some(body); // a mangled body: the header fields still stand
    }
    let rest = &front[21.min(front.len())..];
    if tag == 2 && rest.len() > 8 + 4 {
        // Prepare { entry { slot, era, payload }, committed }
        let cursor = 12;
        let disc = rest[cursor];
        if disc == 1 && rest.len() >= cursor + 1 + 16 + 4 {
            // Operation { id(16), json(4 + bytes) }
            let after = cursor + 1 + 16;
            let jlen =
                u32::from_be_bytes(rest[after..after + 4].try_into().expect("4 bytes")) as usize;
            if rest.len() >= after + 4 + jlen && rest.len() >= after + 4 + jlen + 8 {
                let json = &rest[after + 4..after + 4 + jlen];
                body.lock = lock_fields(json);
                let committed = u64::from_be_bytes(
                    rest[after + 4 + jlen..after + 4 + jlen + 8]
                        .try_into()
                        .expect("8 bytes"),
                );
                body.committed = Some(committed);
            }
        }
    } else if tag == 4 && rest.len() >= 8 {
        // Commit { committed }
        body.committed = Some(u64::from_be_bytes(rest[0..8].try_into().expect("8 bytes")));
    }
    Some(body)
}

/// The lock fields of one committed lock verb's JSON, or `None` when the
/// payload is not a lock op (the reader never guesses).
fn lock_fields(json: &[u8]) -> Option<Map<String, Value>> {
    let value: Value = serde_json::from_slice(json).ok()?;
    let object = value.as_object()?;
    if !matches!(
        object.get("op").and_then(|v| v.as_str()),
        Some("get" | "set" | "release" | "break")
    ) {
        return None;
    }
    let mut fields = Map::new();
    for key in ["op", "lock_id", "message_id", "client_id", "request_num"] {
        if let Some(field) = object.get(key) {
            fields.insert(key.to_string(), field.clone());
        }
    }
    if let Some(lease) = object.get("lease").and_then(|v| v.as_object()) {
        for key in ["lease_id", "holder", "expiry", "lease_ms"] {
            if let Some(field) = lease.get(key) {
                fields.insert(key.to_string(), field.clone());
            }
        }
    }
    Some(fields)
}

/// The safe iterator wrapper over one AOF file (the same FFI the host
/// uses — never a shell-out to the python tool).
fn retention_iter(path: &Path) -> Result<lunet_locks_aof::ffi::RawIter, i32> {
    unsafe { lunet_locks_aof::ffi::RawIter::open(path.to_string_lossy().as_bytes()) }
}

/// The series' `.aof` files, oldest first.
fn series(dir: &Path) -> Vec<lunet_locks_aof::retention::ListedAofFile> {
    retention::list_aof_files(dir).unwrap_or_default()
}

/// The `--logs` surface expanded: a file contributes itself, a directory
/// its sorted `*.log` entries.
fn log_files(paths: &[PathBuf]) -> Vec<PathBuf> {
    let mut out = Vec::new();
    for path in paths {
        if path.is_dir() {
            let mut entries: Vec<PathBuf> = std::fs::read_dir(path)
                .into_iter()
                .flatten()
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| {
                    path.is_file()
                        && path
                            .file_name()
                            .is_some_and(|name| name.as_encoded_bytes().ends_with(b".log"))
                })
                .collect();
            entries.sort();
            out.extend(entries);
        } else {
            out.push(path.clone());
        }
    }
    out
}

/// The wire rows' senders, from the recorder's own JSON log series: one
/// `(era, view, slot) -> from` entry per `commit-in` line (the arrival
/// line a standby logs for every Commit it accepts from its leader, the
/// sender in its `from` member). A line that does not parse, or misses
/// a member, is skipped — never guessed.
fn senders_from_logs(paths: &[PathBuf]) -> HashMap<(u32, u32, u64), u32> {
    let mut senders = HashMap::new();
    for path in log_files(paths) {
        let Ok(text) = std::fs::read_to_string(&path) else {
            continue;
        };
        for line in text.lines() {
            let Ok(value) = serde_json::from_str::<Value>(line) else {
                continue;
            };
            if value.get("event").and_then(Value::as_str) != Some("commit-in") {
                continue;
            }
            let (Some(from), Some(era), Some(view), Some(slot)) = (
                value.get("from").and_then(Value::as_u64),
                value.get("era").and_then(Value::as_u64),
                value.get("view").and_then(Value::as_u64),
                value.get("slot").and_then(Value::as_u64),
            ) else {
                continue;
            };
            senders.insert((era as u32, view as u32, slot), from as u32);
        }
    }
    senders
}

/// Streams one telemetry AOF directory as the tape: one `from,to,{json}`
/// line per record, in file order, through `out`. Returns the counts.
pub fn stream_dir(
    dir: &Path,
    options: &TapeOptions,
    out: &mut dyn Write,
) -> std::io::Result<TapeCounts> {
    let recorder = options.recorder;
    let senders = senders_from_logs(&options.logs);
    let wanted: Option<Vec<Kind>> = if options.kinds.is_empty() {
        None
    } else {
        Some(options.kinds.clone())
    };
    let mut counts = TapeCounts::default();
    for (path, _, _) in series(dir) {
        let mut iter = match retention_iter(&path) {
            Ok(iter) => iter,
            Err(code) => {
                return Err(std::io::Error::other(format!(
                    "skaffold_aof_tape: iterator open {}: code {code}",
                    path.display()
                )));
            }
        };
        while let Ok(Some(entry)) = iter.next_entry() {
            counts.records += 1;
            let Some(record) = Record::decode(&entry.bytes) else {
                counts.undecodable += 1;
                continue;
            };
            let ns = record.ns;
            let kind = match record.marker {
                Marker::Wire => Kind::Wire,
                Marker::TelemetryTimeoutDecision => Kind::Decision,
                Marker::TelemetryStateTransition => Kind::Transition,
                Marker::TelemetryOutbound => Kind::Outbound,
            };
            if let Some(wanted) = &wanted
                && !wanted.contains(&kind)
            {
                continue;
            }
            let Some(line) = tape_line(kind, ns, &record.payload, recorder, &senders) else {
                counts.unnamed += 1;
                continue;
            };
            if !keeps(&line, options) {
                counts.filtered += 1;
                continue;
            }
            counts.lines += 1;
            writeln!(out, "{}", line.render())?;
        }
    }
    Ok(counts)
}

/// Per-record stream counters (the bin's stderr summary).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct TapeCounts {
    pub records: u64,
    pub undecodable: u64,
    /// Records whose endpoints could not be derived at all.
    pub unnamed: u64,
    pub filtered: u64,
    pub lines: u64,
}

/// Whether the line passes the numeric endpoint filters. A `?` endpoint
/// is kept only when the corresponding `--*-any` flag allows it.
fn keeps(line: &TapeLine, options: &TapeOptions) -> bool {
    if let Some(from) = options.from {
        if line.from == "?" {
            if !options.from_any {
                return false;
            }
        } else if line.from != from.to_string() {
            return false;
        }
    }
    if let Some(to) = options.to {
        if line.to == "?" {
            if !options.to_any {
                return false;
            }
        } else if line.to != to.to_string() {
            return false;
        }
    }
    true
}

/// One record's tape line from its kind, clock, payload bytes, and the
/// wire senders table (the `commit-in` join; empty when no `--logs`).
pub fn tape_line(
    kind: Kind,
    ns: u64,
    payload: &[u8],
    recorder: Option<u32>,
    senders: &HashMap<(u32, u32, u64), u32>,
) -> Option<TapeLine> {
    let recorder_text = recorder.map(|id| id.to_string());
    let ts_ms = ns / 1_000_000;
    let mut json = Map::new();
    json.insert("kind".into(), Value::from(kind_name(kind)));
    json.insert("ts_ms".into(), Value::from(ts_ms));
    json.insert("ns".into(), Value::from(ns));
    match kind {
        Kind::Wire => {
            let body = wire_body(payload)?;
            json.insert("tag".into(), Value::from(body.tag));
            json.insert("tag_name".into(), Value::from(tag_name(body.tag)));
            json.insert("era".into(), Value::from(body.era));
            json.insert("view".into(), Value::from(body.view));
            json.insert("slot".into(), Value::from(body.slot));
            if let Some(committed) = body.committed {
                json.insert("committed".into(), Value::from(committed));
            }
            if let Some(lock) = body.lock {
                json.insert("lock".into(), Value::Object(lock));
            }
            json.insert("frame_hex".into(), Value::from(hex(payload)));
            // The sender the recorder's own commit-in log line names for
            // this frame's (era, view, slot) — the join claims Commits
            // only (the arrival line is a Commit's), never a guessed id.
            let from = if body.tag == 4 {
                senders
                    .get(&(body.era, body.view, body.slot))
                    .map(|id| id.to_string())
            } else {
                None
            }
            .unwrap_or_else(|| "?".to_string());
            let to = recorder_text.clone()?;
            Some(TapeLine {
                from,
                to,
                json: Value::Object(json),
            })
        }
        Kind::Decision | Kind::Transition => {
            insert_parsed(&mut json, payload);
            let from = recorder_text.clone()?;
            let to = recorder_text?;
            Some(TapeLine {
                from,
                to,
                json: Value::Object(json),
            })
        }
        Kind::Outbound => {
            insert_parsed(&mut json, payload);
            let from = recorder_text.clone()?;
            let to = json
                .get("to")
                .and_then(|v| v.as_u64())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".to_string());
            Some(TapeLine {
                from,
                to,
                json: Value::Object(json),
            })
        }
    }
}

fn kind_name(kind: Kind) -> &'static str {
    match kind {
        Kind::Wire => "wire",
        Kind::Decision => "decision",
        Kind::Transition => "transition",
        Kind::Outbound => "outbound",
    }
}

/// Merges one telemetry record's JSON payload into the line's object;
/// an unparsable payload rides as an escaped `raw` string instead.
fn insert_parsed(json: &mut Map<String, Value>, payload: &[u8]) {
    match serde_json::from_slice::<Value>(payload) {
        Ok(value) => {
            if let Some(object) = value.as_object() {
                for (key, field) in object {
                    json.insert(key.clone(), field.clone());
                }
            }
        }
        Err(_) => {
            let raw = String::from_utf8_lossy(payload).replace(['\n', '\r'], " ");
            json.insert("raw".into(), Value::from(raw));
        }
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use lunet_locks_aof::{AofFile, Options};
    use std::path::Path;

    /// One uVRR Commit frame: tag(4 BE) | era | view | slot, the body
    /// discriminant repeating the tag's low byte, the committed frontier.
    fn commit_frame(era: u32, view: u32, slot: u64) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&4u32.to_be_bytes());
        frame.extend_from_slice(&era.to_be_bytes());
        frame.extend_from_slice(&view.to_be_bytes());
        frame.extend_from_slice(&slot.to_be_bytes());
        frame.push(4);
        frame.extend_from_slice(&slot.to_be_bytes());
        frame
    }

    /// One uVRR Prepare frame: the 20-byte header plus the tag's low byte.
    fn prepare_frame(era: u32, view: u32, slot: u64) -> Vec<u8> {
        let mut frame = Vec::new();
        frame.extend_from_slice(&2u32.to_be_bytes());
        frame.extend_from_slice(&era.to_be_bytes());
        frame.extend_from_slice(&view.to_be_bytes());
        frame.extend_from_slice(&slot.to_be_bytes());
        frame.push(2);
        frame
    }

    /// The wire rows' sender, restored from the recorder's own JSON log
    /// lines: the `commit-in` line names the sender (`from`) keyed by the
    /// `(era, view, slot)` the Commit wire record's header carries. A
    /// frame with no matching line keeps `?` — never guessed.
    #[test]
    fn wire_row_sender_comes_from_the_commit_in_log_lines() {
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/tape-sender-test");
        let _ = std::fs::remove_dir_all(&root);
        let dir = root.join("aof");
        std::fs::create_dir_all(&dir).unwrap();
        let mut file = AofFile::open_with(
            &dir,
            Options {
                force_flush: false,
                retention_bytes: u64::MAX / 2,
            },
        )
        .unwrap();
        // A Commit the recorder's logs name (era 4, view 1, slot 5, from
        // 66), a Commit they do not (slot 6), and a Prepare (slot 7) the
        // commit-in join never claims — only Commits carry the arrival.
        file.append(&Record::wire(1_000, &commit_frame(4, 1, 5)).encode())
            .unwrap();
        file.append(&Record::wire(2_000, &commit_frame(4, 1, 6)).encode())
            .unwrap();
        file.append(&Record::wire(3_000, &prepare_frame(4, 1, 7)).encode())
            .unwrap();
        file.close().unwrap();
        let logs = root.join("logs");
        std::fs::create_dir_all(&logs).unwrap();
        std::fs::write(
            logs.join("n99.log"),
            concat!(
                "{\"level\":\"INFO\",\"ts\":1789214915002,\"event\":\"commit-in\",",
                "\"node\":99,\"from\":66,\"era\":4,\"view\":1,\"slot\":5,",
                "\"leader\":66,\"state\":\"normal\"}\n",
                "not json at all\n",
            ),
        )
        .unwrap();
        let options = TapeOptions {
            recorder: Some(99),
            logs: vec![logs],
            ..Default::default()
        };
        let mut out = Vec::new();
        let counts = stream_dir(&dir, &options, &mut out).unwrap();
        let text = String::from_utf8(out).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(counts.lines, 3, "{counts:?}");
        assert_eq!(
            lines[0].split(',').take(2).collect::<Vec<_>>(),
            vec!["66", "99"],
            "the sender is the commit-in line's from: {}",
            lines[0]
        );
        assert!(
            lines[1].starts_with("?,99,"),
            "no matching commit-in line: never guessed: {}",
            lines[1]
        );
        assert!(
            lines[2].starts_with("?,99,"),
            "the join claims Commits only, never a Prepare: {}",
            lines[2]
        );
        std::fs::remove_dir_all(&root).ok();
    }
}
