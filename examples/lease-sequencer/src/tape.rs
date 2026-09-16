//! The rig AOF tape (item03): the `from,to,jsonl` streaming layer that
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
//! names no sender (the receiver derives it from the socket). The tape
//! derives the endpoint labels from what each record actually carries:
//!
//! - marker-1 `Wire` (a raw uVRR datagram the recorder RECEIVED):
//!   `to` = the recorder's node id; `from` = the phi trailer's `leader`
//!   when the frame carries the 22-byte trailer (magic `C0 0B`, little-
//!   endian fields) — the leader's Commit stream, else `?` (a Prepare,
//!   NewState, or any untrailed datagram names no sender on the wire).
//! - marker-2 `TelemetryTimeoutDecision`: `from` = the recorder (the
//!   deciding node), `to` = the recorder.
//! - marker-3 `TelemetryStateTransition`: `from` = the recorder, `to` =
//!   the recorder.
//! - marker-4 `TelemetryOutbound`: `from` = the recorder, `to` = the
//!   record's target (its JSON `to` field).
//! - marker-5 `TelemetryIntervalSample`: `from` = the sample's `node`
//!   field (the recording monitor's own id), `to` = the recorder.
//!
//! The recorder's node id is `--recorder N` on the bin, or derived from
//! the first marker-5 sample's `node` field when the flag is omitted
//! (the sample's `node` IS the recording node's own id).
//!
//! # The jsonl payload
//!
//! The third CSV field is the full parsed record as one JSON line:
//! `ts_ms` (the envelope's ns clock truncated to ms), `ns`, and for wire
//! records `tag`/`era`/`view`/`slot`, the committed frontier when the
//! frame carries one, the lock fields when the payload is a committed
//! lock verb, the phi fields when a trailer rides the frame, and
//! `frame_hex` — the raw wire bytes hex-encoded, trailer included:
//! byte-exact playback needs the original datagram.

use crate::phi::Trailer;
use lunet_locks_aof::envelope::{Marker, Record};
use lunet_locks_aof::retention;
use serde_json::{Map, Value};
use std::io::Write;
use std::path::Path;

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
    /// The recorder's node id. `None` derives it from the first
    /// marker-5 sample's `node` field; with neither, recorder-side
    /// endpoints render as `?`.
    pub recorder: Option<u32>,
    /// Keep only lines whose derived `from` equals this. `?` lines are
    /// dropped unless `from_any`.
    pub from: Option<u32>,
    pub from_any: bool,
    /// Keep only lines whose derived `to` equals this. `?` lines are
    /// dropped unless `to_any`.
    pub to: Option<u32>,
    pub to_any: bool,
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
    Sample,
}

impl Kind {
    pub fn parse(text: &str) -> Option<Kind> {
        match text {
            "wire" => Some(Kind::Wire),
            "decision" => Some(Kind::Decision),
            "transition" => Some(Kind::Transition),
            "outbound" => Some(Kind::Outbound),
            "sample" => Some(Kind::Sample),
            "all" => None, // callers expand; parsed per-token via all()
            _ => None,
        }
    }

    pub fn all() -> Vec<Kind> {
        vec![
            Kind::Wire,
            Kind::Decision,
            Kind::Transition,
            Kind::Outbound,
            Kind::Sample,
        ]
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
    if tag == 2 && rest.len() >= 8 + 4 + 1 {
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

/// The recorder's node id, derived from the series' first marker-5
/// sample's `node` field (the recording monitor's own id) when the flag
/// did not name it.
fn derive_recorder(dir: &Path) -> Option<u32> {
    for (path, _, _) in series(dir) {
        let mut iter = match unsafe { retention_iter(&path) } {
            Ok(iter) => iter,
            Err(_) => continue,
        };
        while let Ok(Some(entry)) = iter.next_entry() {
            let Some(record) = Record::decode(&entry.bytes) else {
                continue;
            };
            if record.marker != Marker::TelemetryIntervalSample {
                continue;
            }
            let value: Value = serde_json::from_slice(&record.payload).ok()?;
            // Borrow ends before the iterator's drop: build the id now.
            let node = value.get("node").and_then(|v| v.as_u64());
            if let Some(node) = node {
                return Some(node as u32);
            }
        }
    }
    None
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

/// Streams one telemetry AOF directory as the tape: one `from,to,{json}`
/// line per record, in file order, through `out`. Returns the counts.
pub fn stream_dir(
    dir: &Path,
    options: &TapeOptions,
    out: &mut dyn Write,
) -> std::io::Result<TapeCounts> {
    let recorder = match options.recorder {
        Some(id) => Some(id),
        None => derive_recorder(dir),
    };
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
                Marker::TelemetryIntervalSample => Kind::Sample,
            };
            if let Some(wanted) = &wanted {
                if !wanted.contains(&kind) {
                    continue;
                }
            }
            let Some(line) = tape_line(kind, ns, &record.payload, recorder) else {
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

/// One record's tape line from its kind, clock, and payload bytes.
pub fn tape_line(kind: Kind, ns: u64, payload: &[u8], recorder: Option<u32>) -> Option<TapeLine> {
    let recorder_text = recorder.map(|id| id.to_string());
    let ts_ms = ns / 1_000_000;
    let mut json = Map::new();
    json.insert("kind".into(), Value::from(kind_name(kind)));
    json.insert("ts_ms".into(), Value::from(ts_ms));
    json.insert("ns".into(), Value::from(ns));
    match kind {
        Kind::Wire => {
            let (front, trailer) = match Trailer::strip_from(payload) {
                Some((front, trailer)) => (front, Some(trailer)),
                None => (payload, None),
            };
            let body = wire_body(front)?;
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
            if let Some(trailer) = &trailer {
                let phi = serde_json::json!({
                    "era": trailer.era,
                    "leader": trailer.leader,
                    "seq": trailer.seq,
                    "sent_at_ms": trailer.sent_at_ms,
                });
                json.insert("phi".into(), phi);
            }
            json.insert("frame_hex".into(), Value::from(hex(payload)));
            let from = trailer
                .as_ref()
                .map(|t| t.leader.to_string())
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
        Kind::Sample => {
            insert_parsed(&mut json, payload);
            let from = json
                .get("node")
                .and_then(|v| v.as_u64())
                .map(|v| v.to_string())
                .unwrap_or_else(|| "?".to_string());
            let to = recorder_text?;
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
        Kind::Sample => "sample",
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
            let raw = String::from_utf8_lossy(payload)
                .replace('\n', " ")
                .replace('\r', " ");
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
