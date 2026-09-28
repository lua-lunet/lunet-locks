//! The tape acceptance: the corpus is RECORDED IN-TEST (record-once-
//! then-replay) — the former gitignored run pulls were dead for any
//! fresh clone — so the test records its own era-4 view-13 leader-66
//! telemetry AOF into the repository's `.tmp/` scratch through the
//! vendored record layer, streams it as the tape, and replays it in the
//! same run.
//!
//! Red (kept): a fresh genesis node force-fed the higher-era tape
//! digests every datagram with an `OK` return code yet never replays a
//! committed transition — the core drops datagrams naming an era outside
//! its configuration table's retention window (`uvrr-core`
//! `src/replica/normal.rs`, the `EraUnevaluable` gate), and a mid-stream
//! window carries no slots for a fresh node's commit fold to walk. This
//! pins WHY the replay layer for a mid-stream window is the lock
//! Service — the node's committed state machine — fed the verbs
//! extracted byte-exactly from the tape's `frame_hex` payloads.
//!
//! Green: the scenario + the leader-66 window replay the recorded
//! committed transitions byte-exactly — the renewal chain holds once per
//! holder run and renews the same holder on every later regrant, at the
//! recorded execution clocks, from the recorded wire bytes.

#[path = "recorder/mod.rs"]
mod recorder;

#[path = "scenario/mod.rs"]
mod scenario;

use lease_sequencer::phi::Trailer;
use lease_sequencer::tape::{TapeOptions, stream_dir};
use lunet_advisory_lock::locks::{LeaseCandidate, Request, Service, Transition};
use recorder::{commit_frame, prepare_frame, write_aof};
use scenario::{Scenario, TapeFrame, feed_tape, tape_frame};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::Path;
use uuid::Uuid;
use vrr::journal::Payload;
use vrr::message::{Body, Message};
use vrr::wire::Unpack;

const RECORDER: u32 = (99 << 16) | 1;
const ERA: u32 = 4;
const VIEW: u32 = 13;
const LEADER: u32 = (66 << 16) | 1;
/// The window's committed frontier at its start; the recorded verbs'
/// slots begin one past it.
const COMMITTED_SLOT: u64 = 41_001;
/// The polite lock's recorded holder runs: three SETs, then two, then
/// seven — three holder runs, the middle one a holder change.
const RUN_LENGTHS: [usize; 3] = [3, 2, 7];
const TOTAL_FRAMES: usize = {
    let mut total = 0;
    let mut index = 0;
    while index < RUN_LENGTHS.len() {
        total += 2 * RUN_LENGTHS[index];
        index += 1;
    }
    total
};
/// A fixed recording clock (ms `BASE_NS / 1_000_000`): the replay's
/// execution ticks are the recorded clocks.
const BASE_NS: u64 = 1_793_000_000_000_000_000;
const NS_STEP: u64 = 1_000_000;

/// The recorded holders, in run order: two distinct holders, the third
/// run a return to the first.
fn holders() -> [Uuid; 3] {
    [
        Uuid::from_u128(0x0000_DDBA_1101),
        Uuid::from_u128(0x0000_DDBA_1102),
        Uuid::from_u128(0x0000_DDBA_1101),
    ]
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
fn committed_verbs(frames: &[TapeFrame]) -> Vec<CommittedVerb> {
    let mut verbs = Vec::new();
    for frame in frames {
        if frame.tag != 2 {
            continue;
        }
        let Ok(message) = Message::unpack_from(&frame.front) else {
            continue;
        };
        let slot = message.header.slot.0;
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
            slot,
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
fn replay_verbs(verbs: &[CommittedVerb]) -> Vec<ReplayedVerb> {
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

/// Records the leader-66 window into a fresh telemetry AOF under `root`:
/// for each committed verb one untrailed Prepare (the wire header names
/// no sender) followed by one trailed Commit naming leader 66 — the
/// tape's own from-derivation shape. The window opens on the untrailed
/// Prepare, as the recorded corpora opened.
fn record_corpus(root: &Path) -> std::path::PathBuf {
    let dir = root.join("telemetry");
    let mut records = Vec::new();
    let holders = holders();
    let mut slot = COMMITTED_SLOT;
    let mut ns = BASE_NS;
    for (run_index, length) in RUN_LENGTHS.iter().enumerate() {
        // A holder change lands after the prior holder's lease window
        // (250 ms) has expired at the recorded clocks, so the new
        // holder's SET is grantable.
        if run_index > 0 {
            ns += 500 * NS_STEP;
        }
        for _ in 0..*length {
            slot += 1;
            ns += NS_STEP;
            let holder = holders[run_index];
            let message_id = Uuid::from_u128(slot as u128 | 0xDDBA_1200_0000_0000);
            let request = Request::Set {
                message_id,
                client_id: 7,
                request_num: slot,
                lock_id: 14_531_090,
                lease: LeaseCandidate {
                    lease_id: slot,
                    holder,
                    lease_ms: 250,
                },
                name: None,
                labels: None,
                sent_at_ms: None,
            };
            let payload = serde_json::to_vec(&request).expect("the verb serializes");
            records.push(lunet_locks_aof::envelope::Record::wire(
                ns,
                &prepare_frame(ERA, VIEW, slot, message_id, &payload, slot - 1),
            ));
            ns += NS_STEP;
            let trailer = Trailer {
                era: ERA,
                leader: LEADER,
                seq: (slot - COMMITTED_SLOT) as u32,
                sent_at_ms: ns / 1_000_000,
            };
            records.push(lunet_locks_aof::envelope::Record::wire(
                ns,
                &commit_frame(ERA, VIEW, slot, slot, &trailer),
            ));
        }
    }
    write_aof(&dir, 1_793_000_000, &records);
    dir
}

/// Streams the recorded corpus as the tape (all kinds), in file order.
fn stream_tape(dir: &Path) -> Vec<String> {
    let options = TapeOptions {
        recorder: Some(RECORDER),
        ..Default::default()
    };
    let mut capture: Vec<u8> = Vec::new();
    stream_dir(dir, &options, &mut capture).expect("the corpus streams");
    String::from_utf8_lossy(&capture)
        .lines()
        .map(|line| line.to_string())
        .collect()
}

/// The frames to the recorder: every wire record (all of them name the
/// recorder as their `to`).
fn frames_to_recorder(dir: &Path) -> Vec<TapeFrame> {
    stream_tape(dir)
        .iter()
        .filter_map(|line| {
            let (from, to, json) = scenario::parse_tape_line(line)?;
            if to != "6488065" {
                return None;
            }
            if json.get("kind").and_then(|v| v.as_str()) != Some("wire") {
                return None;
            }
            tape_frame(from, json)
        })
        .collect()
}

/// The scenario JSON: node 99's initial condition at the window's start —
/// the six-member descriptor (the joined standbys at weight 0), the
/// committed era-4 frontier, the silent client gate, and no lease state
/// carried across the window (the replayed verbs define it).
const SCENARIO_99: &str = r#"{
  "node_id": 6488065,
  "name": "node99",
  "membership": [
    {"id": 2883585, "name": "node44", "weight": 1},
    {"id": 3604481, "name": "node55", "weight": 1},
    {"id": 4325377, "name": "node66", "weight": 1},
    {"id": 5046273, "name": "node77", "weight": 0, "joined": true},
    {"id": 5767169, "name": "node88", "weight": 0, "joined": true},
    {"id": 6488065, "name": "node99", "weight": 0, "joined": true}
  ],
  "era": 4,
  "view": 13,
  "committed_slot": 41001,
  "gate": "off"
}"#;

/// RED, kept: the fresh node digests the whole tape with `OK` codes and
/// replays nothing — the era wall, pinned.
#[test]
fn given_a_fresh_genesis_node_the_higher_era_tape_never_replays() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// GREEN: the scenario + the era-4 view-13 leader-66 window: the
/// committed verbs extracted from the tape's own bytes replay the
/// recorded committed transitions byte-exactly — each holder run opens
/// with a Hold and renews the same holder thereafter, at the recorded
/// clocks.
#[test]
fn given_the_scenario_and_tape_the_committed_verbs_replay_the_recorded_transitions() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
