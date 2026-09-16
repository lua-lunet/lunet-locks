//! The Flight Recorder's tape layer (item12): the native-Rust
//! reader/extraction path for a node's internal trace — the
//! skaffold-class surface ad-hoc tools and the tests use to pull the
//! exact message/event sequence out of a flight recording and feed the
//! SAME playback engine the telemetry tape feeds
//! (`tests/scenario/mod.rs`).
//!
//! # The commit gate (the deep read's first duty)
//!
//! The FIRST record of every flight recording is the header naming the
//! git commit the recording build was compiled from. A recording is
//! readable ONLY by code as-at that commit: [`check_commit`] refuses
//! anything else — loudly, naming both sides — and a dirty-stamped
//! recording (the `FLIGHT_RECORDER_ALLOW_DIRTY=1` build-time override)
//! must be annotated loudly by the reader before any use. Debug-level
//! tool: NO long-term readability is promised across commits.
//!
//! The gate binds the DEEP read (`--deep`, or any kind beyond `wire`):
//! the full internal event log means nothing outside its commit. The
//! stable `from,to,jsonl` slice (the wire kinds) still streams from any
//! recording whose format this reader knows — cross-commit too,
//! best-effort: the CSV shape is the stable surface, the bytes under it
//! are not guaranteed.
//!
//! # The tape lines
//!
//! The recorder's `receive-in` / `request-in` / `emit` events carry the
//! raw bytes hex-encoded (`hex`), so the extraction renders the same
//! `from,to,{json}` CSV shape the telemetry tape renders — with BETTER
//! sender attribution: the node's own host named the sender of every
//! inbound datagram (`from` is exact, never the trailer-derived guess),
//! and every emission names its target. `feed_tape` in the scenario
//! module consumes these lines unchanged.
//!
//! The internal events (`drive-in`/`drive-out`/`fault`/`maybe`/
//! `journal`/`marker`/`timeout-toggle`/`stop-drain`) are the node's
//! private story — the predicted-vs-actual debugging material. They
//! pass through the filters by kind; the tape carries them only on
//! request (`--kinds internal`).

use crate::tape::TapeLine;
use serde_json::Map;
use std::path::Path;

/// The reader's own commit identity: the commit hash this build was
/// compiled from (the build script's stamp). Recordings whose header
/// commit differs are refused.
pub const READER_COMMIT: &str = env!("FLIGHT_COMMIT");

/// The recording format this reader understands.
pub const FLIGHT_FORMAT: u64 = 1;

/// The recording's kinds the tape extraction renders as wire frames.
pub const WIRE_KINDS: [&str; 3] = ["receive-in", "request-in", "emit"];

/// One parsed flight event: the recorder's JSONL line without the
/// header.
#[derive(Debug, Clone, PartialEq)]
pub struct FlightEvent {
    pub seq: u64,
    pub kind: String,
    pub ts_ms: u64,
    pub detail: serde_json::Value,
}

/// The reader path's failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FlightError {
    Io(String),
    MissingHeader,
    UnknownFormat(u64),
    CommitMismatch { recorded: String, reader: String },
}

impl std::fmt::Display for FlightError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FlightError::Io(error) => write!(f, "flight recording read failed: {error}"),
            FlightError::MissingHeader => {
                write!(f, "flight recording opens on no flight-header line")
            }
            FlightError::UnknownFormat(version) => {
                write!(
                    f,
                    "flight recording format {version} is unknown to this reader"
                )
            }
            FlightError::CommitMismatch { recorded, reader } => write!(
                f,
                "flight recording was recorded by commit {recorded}; this reader is \
                 commit {reader} — a recording is readable ONLY by the code as-at \
                 its commit (check out {recorded} to read it)"
            ),
        }
    }
}

impl std::error::Error for FlightError {}

/// One recording's header: the commit facts the reader gate runs on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FlightHeader {
    pub commit: String,
    pub dirty: bool,
    pub node: u32,
    pub format: u64,
}

/// Reads the recording's header: its FIRST line.
pub fn read_header(path: &Path) -> Result<FlightHeader, FlightError> {
    let text = std::fs::read_to_string(path).map_err(|error| FlightError::Io(error.to_string()))?;
    let first = text.lines().next().ok_or(FlightError::MissingHeader)?;
    let value: serde_json::Value =
        serde_json::from_str(first).map_err(|_| FlightError::MissingHeader)?;
    if value.get("kind").and_then(|kind| kind.as_str()) != Some("flight-header") {
        return Err(FlightError::MissingHeader);
    }
    let format = value["format"].as_u64().ok_or(FlightError::MissingHeader)?;
    if format != FLIGHT_FORMAT {
        return Err(FlightError::UnknownFormat(format));
    }
    Ok(FlightHeader {
        commit: value["commit"]
            .as_str()
            .ok_or(FlightError::MissingHeader)?
            .to_string(),
        dirty: value["dirty"].as_bool().unwrap_or(false),
        node: value["node"].as_u64().unwrap_or(0) as u32,
        format,
    })
}

/// The commit gate: the recorded commit must be THIS reader's commit.
pub fn check_commit(header: &FlightHeader, reader_commit: &str) -> Result<(), FlightError> {
    if header.commit != reader_commit {
        return Err(FlightError::CommitMismatch {
            recorded: header.commit.clone(),
            reader: reader_commit.to_string(),
        });
    }
    Ok(())
}

/// Parses every event line after the header. A mangled line is skipped
/// and counted — the honest reader never guesses.
pub fn read_events(path: &Path) -> Result<(Vec<FlightEvent>, u64), FlightError> {
    let text = std::fs::read_to_string(path).map_err(|error| FlightError::Io(error.to_string()))?;
    let mut events = Vec::new();
    let mut mangled = 0u64;
    for line in text.lines().skip(1) {
        let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
            mangled += 1;
            continue;
        };
        let Some(kind) = value["kind"].as_str() else {
            mangled += 1;
            continue;
        };
        events.push(FlightEvent {
            seq: value["seq"].as_u64().unwrap_or(0),
            kind: kind.to_string(),
            ts_ms: value["ts_ms"].as_u64().unwrap_or(0),
            detail: value["detail"].clone(),
        });
    }
    Ok((events, mangled))
}

/// The extraction options (the skaffold bin's flags).
#[derive(Debug, Clone, Default)]
pub struct FlightTapeOptions {
    /// The node id to require (`--node N`); `None` accepts any header.
    pub node: Option<u32>,
    /// Keep only lines whose derived `from` equals this. A `?` line is
    /// dropped unless `from_any` — the telemetry tape's filter
    /// semantics (item03's shape).
    pub from: Option<u32>,
    pub from_any: bool,
    /// Keep only lines whose derived `to` equals this. A `?` line is
    /// dropped unless `to_any`.
    pub to: Option<u32>,
    pub to_any: bool,
    /// The kinds to keep; empty keeps only the wire kinds (the playback
    /// surface). The literal kind `internal` adds every non-wire kind.
    /// Naming anything beyond `wire` makes the read a DEEP read (the
    /// commit gate applies).
    pub kinds: Vec<String>,
    /// The deep read: the full internal event log — every event, wire
    /// and internal, rendered with its `seq`, plus the header facts and
    /// per-kind counts on stderr. Same-commit only: the commit gate
    /// (`check_commit`) refuses a foreign commit loudly, before any
    /// extraction. `--deep` is not narrowed by `--kinds`.
    pub deep: bool,
}

/// Whether one event passes the kind and node filters. The deep read
/// streams every event; `--kinds` does not narrow it.
fn keeps(kind: &str, options: &FlightTapeOptions, own: &str) -> bool {
    let wire = WIRE_KINDS.contains(&kind);
    let internal = !wire;
    if options.deep {
        return options.node.is_none_or(|node| own == node.to_string());
    }
    if !options.kinds.is_empty() {
        let wanted = options.kinds.iter().any(|wanted| match wanted.as_str() {
            "wire" => wire,
            "internal" => internal,
            other => other == kind,
        });
        if !wanted {
            return false;
        }
    } else if internal {
        return false; // default: the playback surface only
    }
    if let Some(node) = options.node {
        if own != node.to_string() {
            return false;
        }
    }
    true
}

/// Whether the line passes the numeric endpoint filters — applied to the
/// RENDERED endpoints, so the trivial shell filter `grep "^66,44,"` keeps
/// exactly the lines the flags keep. A `?` endpoint is kept only when the
/// corresponding `--*-any` flag allows it.
fn keeps_endpoints(from: &str, to: &str, options: &FlightTapeOptions) -> bool {
    if let Some(from_want) = options.from {
        if from == "?" {
            if !options.from_any {
                return false;
            }
        } else if from != from_want.to_string() {
            return false;
        }
    }
    if let Some(to_want) = options.to {
        if to == "?" {
            if !options.to_any {
                return false;
            }
        } else if to != to_want.to_string() {
            return false;
        }
    }
    true
}

/// One event's tape line: `from,to,{json}`, the scenario engine's shape.
/// `None` when the event carries no frame bytes (the internal events with
/// `--kinds internal` still render — their JSON is the detail itself).
pub fn event_tape_line(event: &FlightEvent, own_node: u32) -> Option<TapeLine> {
    let own = own_node.to_string();
    let detail = event.detail.as_object()?.clone();
    let mut json = Map::new();
    for (key, field) in detail {
        // The recorder's `hex` rides the tape as `frame_hex` — the byte
        // key the scenario playback engine consumes; an emit's numeric
        // output kind rides as `out_kind` so the tape's `kind` stays the
        // event kind string.
        let key = match key.as_str() {
            "hex" => "frame_hex".to_string(),
            "kind" => "out_kind".to_string(),
            other => other.to_string(),
        };
        json.insert(key, field);
    }
    json.insert("kind".into(), serde_json::Value::from(event.kind.as_str()));
    json.insert("ts_ms".into(), serde_json::Value::from(event.ts_ms));
    let from = json
        .get("from")
        .and_then(|v| v.as_u64())
        .map(|v| v.to_string())
        .unwrap_or_else(|| "?".to_string());
    let to = json
        .get("to")
        .and_then(|v| v.as_u64())
        .map(|v| v.to_string())
        .unwrap_or_else(|| own.clone());
    // An inbound event's `from` is the sender the node's host named; the
    // recorder stamps the node's own id as the receiver in the tape's
    // `to` field. An emission's `to` is the recorded target and the
    // sender is the node itself. The internal kinds render the record's
    // own endpoints where the record carries them (the deep read's
    // honest attribution), else the node's own story: to = the recorder.
    match event.kind.as_str() {
        "emit" => Some(TapeLine {
            from: own,
            to,
            json: serde_json::Value::Object(json),
        }),
        "receive-in" | "request-in" => Some(TapeLine {
            from,
            to: own,
            json: serde_json::Value::Object(json),
        }),
        _ => Some(TapeLine {
            from,
            to,
            json: serde_json::Value::Object(json),
        }),
    }
}

/// The recording's tape: one `from,to,{json}` line per kept event, in
/// recording order. Returns the line count. The commit gate applies to
/// the DEEP read only (`--deep`, or any kind beyond `wire`): the stable
/// `from,to,jsonl` slice streams from any recording whose format this
/// reader knows — cross-commit too, best-effort (mangled lines are
/// counted, never guessed).
pub fn stream_recording(
    path: &Path,
    options: &FlightTapeOptions,
    out: &mut dyn std::io::Write,
) -> Result<(usize, u64), FlightError> {
    let header = read_header(path)?;
    let deep = options.deep
        || options
            .kinds
            .iter()
            .any(|kind| kind != "wire" && !WIRE_KINDS.contains(&kind.as_str()));
    if deep {
        // The deep read exposes the node's private story: readable ONLY
        // by the code as-at the recording's commit (item12's gate).
        check_commit(&header, READER_COMMIT)?;
    }
    if header.dirty {
        eprintln!(
            "skaffold_flight_tape: WARNING the recording carries dirty=true (an \
             overridden dirty-tree build, commit {}); read it, but never cite it \
             without naming that override",
            header.commit
        );
    }
    if let Some(node) = options.node
        && header.node != node
    {
        return Err(FlightError::Io(format!(
            "the recording is node {}'s, not node {node}'s",
            header.node
        )));
    }
    let (events, mangled) = read_events(path)?;
    let mut lines = 0usize;
    for event in &events {
        if !keeps(&event.kind, options, &header.node.to_string()) {
            continue;
        }
        let Some(mut line) = event_tape_line(event, header.node) else {
            continue;
        };
        if !keeps_endpoints(&line.from, &line.to, options) {
            continue;
        }
        if deep && let Some(map) = line.json.as_object_mut() {
            map.insert("seq".into(), serde_json::Value::from(event.seq));
        }
        lines += 1;
        writeln!(out, "{}", line.render()).map_err(|error| FlightError::Io(error.to_string()))?;
    }
    if deep {
        let mut tally: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
        for event in &events {
            *tally.entry(event.kind.clone()).or_insert(0) += 1;
        }
        let tally: Vec<String> = tally
            .into_iter()
            .map(|(kind, count)| format!("{kind}={count}"))
            .collect();
        eprintln!(
            "skaffold_flight_tape: DEEP read commit={} dirty={} node={} format={}: \
             events={} mangled={} kinds: {}",
            header.commit,
            header.dirty,
            header.node,
            header.format,
            events.len(),
            mangled,
            tally.join(" ")
        );
    }
    Ok((lines, mangled))
}
