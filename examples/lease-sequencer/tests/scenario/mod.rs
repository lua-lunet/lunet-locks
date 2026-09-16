//! The scenario + tape playback adapter (item03 3b/3c): the node initial
//! condition as serde structs, plus the plumbing that turns a scenario
//! JSON and a tape file (`from,to,{jsonl}` lines) into a driven node.
//!
//! The operator's model, verbatim: "stream the AOF as CSV as
//! `from,to,${jsonl}` then filter on `^${from},${to}` to get the raw
//! jsonl of the message, fire up one node and force feed it that replay
//! tape". The given-message machinery (`tests/uds_playback_test.rs`) is
//! the engine — this module is the plumbing: load scenario, load tape,
//! build the node, force-feed the `frame_hex` bytes in order, report the
//! deterministic outcome.
//!
//! # The two replay layers, and the structural finding that splits them
//!
//! A live `Node` fed the tape bytes is the literal reading, and the
//! `given_a_fresh_genesis_node_*` test in `tests/tape_playback_test.rs`
//! pins what it does: the run-4 window's datagrams are all era 4 while a
//! freshly opened node boots at the genesis configuration era, and the
//! core drops every datagram naming an era outside its table's retention
//! window (`uvrr-core` `src/replica/normal.rs`, the `EraUnevaluable`
//! gate) — the datagrams are digested with `OK` return codes and the
//! node's committed frontier never moves. Even past the era gate, a
//! mid-stream tape cannot advance the commit fold on a fresh node: the
//! core folds the contiguous journal prefix and the window carries no
//! slots before it. The faithful replay layer for a mid-stream window is
//! therefore the lock Service — the node's committed state machine the
//! core itself drives on every fold — fed the committed verbs extracted
//! byte-exactly from the tape's `frame_hex` Prepare payloads. Both
//! layers are implemented here; the acceptance pins both facts.

use lease_sequencer::phi::Trailer;
use lunet_advisory_lock::Node;
use lunet_advisory_lock::locks::{Request, Service, Transition};
use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;
use vrr::journal::Payload;
use vrr::message::{Body, Message};
use vrr::wire::Unpack;

/// One membership row: the descriptor's id, display name, the recorded
/// weight (0 for the joined standbys), and whether the member joined
/// after genesis (the adapter's `<id>:<name>:j` members grammar).
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct MemberRow {
    pub id: u32,
    pub name: String,
    #[serde(default)]
    pub weight: u32,
    #[serde(default)]
    pub joined: bool,
}

/// One lock record of the node's initial lease state: the lock and the
/// lease the replayed verbs must agree with.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct LeaseRecord {
    pub lock_id: u64,
    pub holder: String,
    pub lease_id: u64,
    pub expiry_ms: u64,
}

/// The client gate mode (the `client_gate` worker's Off/On switch), as
/// the scenario records it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GateMode {
    Off,
    On,
}

/// The node initial condition (item03 3b): the test suite IS the
/// scenario — the node's identity, membership, committed frontier,
/// lease state, gate mode, and display name — plus the
/// filtered-to-one-node, trimmed-to-the-bug-section tape.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Scenario {
    pub node_id: u32,
    #[serde(default)]
    pub name: Option<String>,
    pub membership: Vec<MemberRow>,
    /// The node's committed frontier at the window's start.
    pub era: u32,
    pub view: u32,
    #[serde(default)]
    pub committed_slot: u64,
    #[serde(default)]
    pub lease: Option<Vec<LeaseRecord>>,
    #[serde(default)]
    pub gate: Option<GateMode>,
}

impl Scenario {
    /// Parses the scenario JSON.
    pub fn parse(text: &str) -> Result<Scenario, serde_json::Error> {
        serde_json::from_str(text)
    }

    /// The scenario's own member row.
    pub fn own(&self) -> &MemberRow {
        self.membership
            .iter()
            .find(|row| row.id == self.node_id)
            .expect("the scenario's membership names its own node id")
    }

    /// The adapter's members buffer: `<id>:<name>` rows NUL-separated in
    /// descriptor order, post-genesis rows suffixed `:j`.
    pub fn members_buffer(&self) -> String {
        let rows: Vec<String> = self
            .membership
            .iter()
            .map(|row| {
                if row.joined {
                    format!("{}:{}:j", row.id, row.name)
                } else {
                    format!("{}:{}", row.id, row.name)
                }
            })
            .collect();
        rows.join("\0")
    }

    /// Builds the node: the scenario's identity and membership, a fresh
    /// scratch incarnation marker under `root`, no lock journal (the
    /// replay's evidence rides the return values and the status
    /// trajectory, not a journal).
    pub fn open_node(&self, root: &std::path::Path) -> Result<Node, i32> {
        let state = root.join("state");
        std::fs::create_dir_all(root).expect("scenario scratch root");
        Node::open(
            &self.members_buffer(),
            &self.own().name,
            state.to_str().expect("utf-8 scratch path"),
            None,
            0,
        )
    }
}

/// One tape frame: a wire record's derived endpoints, the parsed line
/// JSON, and the raw datagram with its phi trailer stripped (the front
/// is the intact VRR message the core evaluates; the trailer is the
/// heartbeat metadata that rides behind it).
#[derive(Debug, Clone)]
pub struct TapeFrame {
    pub from: String,
    pub json: Value,
    pub front: Vec<u8>,
    pub tag: u32,
    pub slot: u64,
}

/// Parses one `from,to,{jsonl}` tape line.
pub fn parse_tape_line(line: &str) -> Option<(String, String, Value)> {
    let mut parts = line.splitn(3, ',');
    let from = parts.next()?.to_string();
    let to = parts.next()?.to_string();
    let json: Value = serde_json::from_str(parts.next()?).ok()?;
    Some((from, to, json))
}

/// Decodes one wire record's `frame_hex` into a [`TapeFrame`]: the raw
/// bytes, the trailer stripped when one rides the frame, the header
/// fields unpacked with the core's own wire parser (never a guess).
pub fn tape_frame(from: String, json: Value) -> Option<TapeFrame> {
    let hex = json.get("frame_hex")?.as_str()?;
    let mut bytes = Vec::with_capacity(hex.len() / 2);
    for pair in hex.as_bytes().chunks(2) {
        let text = std::str::from_utf8(pair).ok()?;
        bytes.push(u8::from_str_radix(text, 16).ok()?);
    }
    let front = match Trailer::strip_from(&bytes) {
        Some((front, _trailer)) => front.to_vec(),
        None => bytes,
    };
    let message = Message::unpack_from(&front).ok()?;
    Some(TapeFrame {
        from,
        json,
        front,
        tag: message.header.tag as u32,
        slot: message.header.slot.0,
    })
}

/// The outcome of force-feeding one node a tape.
#[derive(Debug)]
pub struct FeedResult {
    pub fed: u64,
    /// Frames whose derived `from` is `?` with no trailed leader yet: the
    /// wire header names no sender, so the feed cannot attribute them.
    pub skipped_no_sender: u64,
    /// The receive return codes, keyed by code.
    pub codes: BTreeMap<i32, u64>,
    /// The node's status before the feed and after it.
    pub status_before: lunet_advisory_lock::NodeStatus,
    pub status_after: Option<lunet_advisory_lock::NodeStatus>,
}

/// Force-feeds the node the tape's wire frames in tape order. The sender
/// attribution follows the tape's own derivation: a trailed frame names
/// its leader; an untrailed frame rides the last named leader (the wire
/// header carries no sender); the first untrailed frames before any
/// trailer are unattributable and counted, never guessed.
pub fn feed_tape(node: &mut Node, frames: &[TapeFrame]) -> FeedResult {
    let mut result = FeedResult {
        status_before: node.status(),
        fed: 0,
        skipped_no_sender: 0,
        codes: BTreeMap::new(),
        status_after: None,
    };
    let mut last_leader: Option<u32> = None;
    for frame in frames {
        let sender = match frame.from.as_str() {
            "?" => match last_leader {
                Some(leader) => leader,
                None => {
                    result.skipped_no_sender += 1;
                    continue;
                }
            },
            other => other.parse::<u32>().expect("a numeric tape from"),
        };
        let code = node.receive(sender, &frame.front);
        *result.codes.entry(code).or_insert(0) += 1;
        result.fed += 1;
        // The next trailer's leader supersedes: the tape's own order. The
        // line's phi field carries the trailer the frame rode in with.
        if let Some(leader) = frame
            .json
            .get("phi")
            .and_then(|phi| phi.get("leader"))
            .and_then(|leader| leader.as_u64())
        {
            last_leader = Some(leader as u32);
        }
    }
    result.status_after = Some(node.status());
    result
}

/// One committed lock verb extracted byte-exactly from a tape frame.
#[derive(Debug, Clone)]
pub struct CommittedVerb {
    pub ns: u64,
    pub slot: u64,
    pub message_id: String,
    pub client_id: u64,
    pub request_num: u64,
    pub lock_id: u64,
    pub op: String,
    pub payload: Vec<u8>,
}

/// Extracts the committed lock verbs from the tape's Prepare frames: the
/// payload rides the frame's own bytes, so the replay input is the
/// recorded wire content itself.
pub fn committed_verbs(frames: &[TapeFrame]) -> Vec<CommittedVerb> {
    let mut verbs = Vec::new();
    for frame in frames {
        if frame.tag != 2 {
            continue;
        }
        let Ok(message) = Message::unpack_from(&frame.front) else {
            continue;
        };
        let Body::Prepare { entry, .. } = message.body else {
            continue;
        };
        let Payload::Operation { id: _, payload } = &entry.payload else {
            continue;
        };
        let Ok(request) = Service::decode(payload) else {
            continue;
        };
        let (message_id, client_id, request_num) = request.ids();
        let (op, lock_id) = match &request {
            Request::Get { lock_id, .. } => ("get", *lock_id),
            Request::Set { lock_id, .. } => ("set", *lock_id),
            Request::Release { lock_id, .. } => ("release", *lock_id),
            Request::Break { lock_id, .. } => ("break", *lock_id),
        };
        verbs.push(CommittedVerb {
            ns: frame.json.get("ns").and_then(|v| v.as_u64()).unwrap_or(0),
            slot: frame.slot,
            message_id: message_id.to_string(),
            client_id,
            request_num,
            lock_id,
            op: op.to_string(),
            payload: payload.to_vec(),
        });
    }
    verbs
}

/// One replayed verb's outcome: the reply's granted flag and the
/// transition the Service executed.
#[derive(Debug, Clone, PartialEq)]
pub struct ReplayedVerb {
    pub slot: u64,
    pub op: String,
    pub lock_id: u64,
    pub granted: Option<bool>,
    pub holder: Option<String>,
    pub transition: String,
}

/// Replays the committed verbs through the Service at their recorded
/// clocks (the given-message engine's execution rule): the deterministic
/// committed-state transitions the tape's bytes produce.
pub fn replay_verbs(verbs: &[CommittedVerb]) -> Vec<ReplayedVerb> {
    let mut service = Service::default();
    let mut replayed = Vec::new();
    for verb in verbs {
        let execution_time = verb.ns / 1_000_000;
        let Ok((bytes, transition)) = service.execute(
            verb.message_id.parse().expect("a uuid message id"),
            verb.client_id,
            verb.request_num,
            execution_time,
            &verb.payload,
        ) else {
            continue;
        };
        let reply: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
        replayed.push(ReplayedVerb {
            slot: verb.slot,
            op: verb.op.clone(),
            lock_id: verb.lock_id,
            granted: reply.get("granted").and_then(|v| v.as_bool()),
            holder: reply
                .get("lease")
                .and_then(|lease| lease.get("holder"))
                .and_then(|v| v.as_str())
                .map(|text| text.to_string()),
            transition: match transition {
                None => "none".to_string(),
                Some(Transition::Hold { .. }) => "hold".to_string(),
                Some(Transition::Renew { .. }) => "renew".to_string(),
                Some(_) => "other".to_string(),
            },
        });
    }
    replayed
}
