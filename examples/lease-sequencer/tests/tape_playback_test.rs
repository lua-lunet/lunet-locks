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
use lunet_advisory_lock::locks::{LeaseCandidate, Request};
use recorder::{commit_frame, prepare_frame, write_aof};
use scenario::{Scenario, TapeFrame, committed_verbs, feed_tape, replay_verbs, tape_frame};
use std::collections::BTreeMap;
use std::path::Path;
use uuid::Uuid;

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
        Uuid::from_u128(0x0DDB_A11_01),
        Uuid::from_u128(0x0DDB_A11_02),
        Uuid::from_u128(0x0DDB_A11_01),
    ]
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
                lock_id: 145_310_90,
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
    let mut options = TapeOptions::default();
    options.recorder = Some(RECORDER);
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
    let scenario = Scenario::parse(SCENARIO_99).expect("the scenario parses");
    let root = recorder::temp_root("tape-playback-red");
    let dir = record_corpus(&root);
    let mut node = scenario.open_node(&root).expect("the scenario node boots");

    let frames = frames_to_recorder(&dir);
    assert_eq!(
        frames.len(),
        TOTAL_FRAMES,
        "the tape to the recorder is the recorded stream, byte-count exact"
    );

    let result = feed_tape(&mut node, &frames);
    let _ = std::fs::remove_dir_all(&root);
    println!(
        "feed: fed={} skipped_no_sender={:?} codes={:?}",
        result.fed, result.skipped_no_sender, result.codes
    );
    println!(
        "status before: era={} view={} state={} leader={} config_era={}",
        result.status_before.era,
        result.status_before.view,
        result.status_before.state,
        result.status_before.leader,
        result.status_before.config_era
    );
    let after = result
        .status_after
        .expect("the feed reports the final status");
    println!(
        "status after:  era={} view={} state={} leader={} config_era={}",
        after.era, after.view, after.state, after.leader, after.config_era
    );

    // Every datagram was digested without a refusal code. The window
    // opens on one unattributable frame: the tape's first wire record is
    // an untrailed Prepare, and the wire header names no sender — the
    // derivation leaves it `?` until the first trailer names the leader.
    assert_eq!(
        result.skipped_no_sender, 1,
        "the window opens on exactly one unattributed Prepare"
    );
    assert!(
        result.codes.keys().all(|code| *code == 0),
        "the era wall must be silent (no refusal codes): {:?}",
        result.codes
    );
    // And the node replays nothing: the genesis era and view stand.
    assert_eq!(
        after.era, 1,
        "a fresh genesis node must never reach the tape's era"
    );
    assert_eq!(
        after.view, 0,
        "a fresh genesis node must never adopt the tape's view"
    );
}

/// GREEN: the scenario + the era-4 view-13 leader-66 window: the
/// committed verbs extracted from the tape's own bytes replay the
/// recorded committed transitions byte-exactly — each holder run opens
/// with a Hold and renews the same holder thereafter, at the recorded
/// clocks.
#[test]
fn given_the_scenario_and_tape_the_committed_verbs_replay_the_recorded_transitions() {
    let scenario = Scenario::parse(SCENARIO_99).expect("the scenario parses");
    // The scenario's frontier must agree with the tape it replays.
    assert_eq!(scenario.era, ERA);
    assert_eq!(scenario.view, VIEW);
    assert_eq!(scenario.committed_slot, COMMITTED_SLOT);

    let root = recorder::temp_root("tape-playback-green");
    let dir = record_corpus(&root);
    // The trimmed window: the leader-66 stream into the recorder — its
    // trailed Commits (the tape's `from` derivation names them 66) and
    // the untrailed Preparers whose committed slots those Commits cover.
    let frames: Vec<TapeFrame> = frames_to_recorder(&dir)
        .into_iter()
        .filter(|frame| {
            frame.json.get("era").and_then(|v| v.as_u64()) == Some(ERA as u64)
                && frame.json.get("view").and_then(|v| v.as_u64()) == Some(VIEW as u64)
        })
        .collect();
    let _ = std::fs::remove_dir_all(&root);
    assert_eq!(
        frames.len(),
        TOTAL_FRAMES,
        "the leader-66 window is the recorded stream, byte-count exact"
    );

    // The verbs and the transitions: the deterministic committed-state
    // replay of the window's own bytes.
    let verbs = committed_verbs(&frames);
    assert_eq!(
        verbs.len(),
        RUN_LENGTHS.iter().sum::<usize>(),
        "every recorded Prepare carries a decodable committed verb"
    );
    let replayed = replay_verbs(&verbs);
    let sets: Vec<&scenario::ReplayedVerb> =
        replayed.iter().filter(|verb| verb.op == "set").collect();
    assert!(
        sets.len() >= 2,
        "the window's committed SETs are unexpectedly thin: {}",
        sets.len()
    );

    // The recorded transition discipline, per lock, as holder runs: the
    // first SET of a run holds with the offered holder granted, every
    // later same-holder regrant on that run renews; a holder change
    // starts the next run.
    let mut by_lock: BTreeMap<u64, Vec<&scenario::ReplayedVerb>> = BTreeMap::new();
    for verb in &sets {
        by_lock.entry(verb.lock_id).or_default().push(verb);
    }
    assert!(
        by_lock.contains_key(&145_310_90),
        "the polite lock's committed chain must be in the window: {:?}",
        by_lock.keys()
    );
    for (lock_id, chain) in &by_lock {
        let mut runs: Vec<Vec<&scenario::ReplayedVerb>> = Vec::new();
        for verb in chain {
            let holder = verb.holder.clone();
            if runs
                .last()
                .and_then(|run| run.last())
                .is_some_and(|last| last.holder == holder)
            {
                runs.last_mut().expect("a run exists").push(verb);
            } else {
                runs.push(vec![verb]);
            }
        }
        for run in &runs {
            let first = run[0];
            assert_eq!(
                first.transition, "hold",
                "a run opens with a Hold on lock {lock_id}: {first:?}"
            );
            assert_eq!(first.granted, Some(true), "the Hold grants: {first:?}");
            let holder = first.holder.as_ref().expect("the Hold names its holder");
            for verb in run.iter().skip(1) {
                assert_eq!(
                    verb.transition, "renew",
                    "every later same-holder SET renews: {verb:?}"
                );
                assert_eq!(
                    verb.holder.as_deref(),
                    Some(holder.as_str()),
                    "the run holds one holder: {verb:?}"
                );
                assert_eq!(verb.granted, Some(true), "every renewal grants: {verb:?}");
            }
        }
        println!(
            "lock {lock_id}: {} runs of {:?} sets",
            runs.len(),
            runs.iter().map(Vec::len).collect::<Vec<_>>()
        );
    }
    // The window's recorded facts, pinned: the polite lock's chain is
    // three holder runs of the recorded lengths, the middle one a holder
    // change.
    let polite: Vec<&scenario::ReplayedVerb> = sets
        .iter()
        .copied()
        .filter(|v| v.lock_id == 145_310_90)
        .collect();
    let polite_runs = polite.chunk_by(|a, b| a.holder == b.holder);
    let run_lengths: Vec<usize> = polite_runs.map(<[&scenario::ReplayedVerb]>::len).collect();
    assert_eq!(
        run_lengths,
        RUN_LENGTHS,
        "the polite lock's holder runs are the recorded facts"
    );

    // The replayed bytes are the recorded bytes: the verbs' slots sit
    // inside the leader-66 commit frontier the tape records.
    for verb in &verbs {
        assert!(
            verb.slot >= scenario.committed_slot,
            "the window's verbs start at the recorded frontier: slot {}",
            verb.slot
        );
    }
}
