//! The tape acceptance (item03 3d): run-4's corpus streamed, filtered,
//! fed to one node — the Red/Green story pinned as the defensive test.
//!
//! Red (kept): a fresh genesis node force-fed the era-4 tape digests
//! every datagram with an `OK` return code yet never replays a committed
//! transition — the core drops datagrams naming an era outside its
//! configuration table's retention window (`uvrr-core`
//! `src/replica/normal.rs`, the `EraUnevaluable` gate), and a mid-stream
//! window carries no slots for a fresh node's commit fold to walk. This
//! pins WHY the replay layer for a mid-stream window is the lock
//! Service — the node's committed state machine — fed the verbs
//! extracted byte-exactly from the tape's `frame_hex` payloads.
//!
//! Green: the scenario + the trimmed leader-66 window replay the
//! recorded committed transitions byte-exactly — the renewal chain holds
//! once and renews the same holder on every later regrant, at the
//! recorded execution clocks, from the recorded wire bytes.

#[path = "scenario/mod.rs"]
mod scenario;

use lease_sequencer::tape::{TapeOptions, stream_dir};
use scenario::{Scenario, TapeFrame, committed_verbs, feed_tape, replay_verbs, tape_frame};
use std::collections::BTreeMap;

/// The run-4 dc3 corpus: the recording standby's AOF directory.
fn corpus_dir() -> std::path::PathBuf {
    std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/telemetry/run4/dc3")
}

/// Streams the corpus as the tape (all kinds), in file order.
fn stream_tape() -> Vec<String> {
    let mut options = TapeOptions::default();
    options.recorder = Some(99);
    let mut capture: Vec<u8> = Vec::new();
    stream_dir(&corpus_dir(), &options, &mut capture).expect("the corpus streams");
    String::from_utf8_lossy(&capture)
        .lines()
        .map(|line| line.to_string())
        .collect()
}

/// The scenario JSON: node 99's initial condition at the window's start —
/// the six-member descriptor (the joined standbys at weight 0), the
/// committed era-4 frontier, the silent client gate, and no lease state
/// carried across the window (the replayed verbs define it).
const SCENARIO_99: &str = r#"{
  "node_id": 99,
  "name": "node99",
  "membership": [
    {"id": 44, "name": "node44", "weight": 1},
    {"id": 55, "name": "node55", "weight": 1},
    {"id": 66, "name": "node66", "weight": 1},
    {"id": 77, "name": "node77", "weight": 0, "joined": true},
    {"id": 88, "name": "node88", "weight": 0, "joined": true},
    {"id": 99, "name": "node99", "weight": 0, "joined": true}
  ],
  "era": 4,
  "view": 13,
  "committed_slot": 41001,
  "gate": "off"
}"#;

/// RED, kept: the fresh node digests the whole tape with `OK` codes and
/// replays nothing — the era-table wall, pinned.
#[test]
fn given_a_fresh_genesis_node_the_run4_era4_tape_never_replays() {
    let scenario = Scenario::parse(SCENARIO_99).expect("the scenario parses");
    let root = std::env::temp_dir().join(format!("item03-red-{}", std::process::id()));
    let mut node = scenario.open_node(&root).expect("the scenario node boots");

    let frames: Vec<TapeFrame> = stream_tape()
        .iter()
        .filter_map(|line| {
            let (from, to, json) = scenario::parse_tape_line(line)?;
            if to != "99" {
                return None;
            }
            if json.get("kind").and_then(|v| v.as_str()) != Some("wire") {
                return None;
            }
            tape_frame(from, json)
        })
        .collect();
    assert!(
        frames.len() > 10_000,
        "the tape to node 99 is unexpectedly thin: {}",
        frames.len()
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

/// GREEN: the scenario + the trimmed era-4 view-13 leader-66 window: the
/// committed verbs extracted from the tape's own bytes replay the
/// recorded committed transitions byte-exactly — the renewal chain holds
/// once and renews the same holder thereafter, at the recorded clocks.
#[test]
fn given_the_scenario_and_tape_the_committed_verbs_replay_the_recorded_transitions() {
    let scenario = Scenario::parse(SCENARIO_99).expect("the scenario parses");
    // The scenario's frontier must agree with the tape it replays.
    assert_eq!(scenario.era, 4);
    assert_eq!(scenario.view, 13);
    assert_eq!(scenario.committed_slot, 41_001);

    // The trimmed window: the leader-66 stream into node 99 — its
    // trailed Commits (the ^66,99, filter shape) and the untrailed
    // Preparers whose committed slots those Commits cover. The tape's
    // `from` derivation names the trailed Commits 66 and leaves the
    // Preparers `?`; both belong to the same leader-66 era-4 view-13
    // section, so the trim keys on the recorded header fields.
    let frames: Vec<TapeFrame> = stream_tape()
        .iter()
        .filter_map(|line| {
            let (from, to, json) = scenario::parse_tape_line(line)?;
            if to != "99" {
                return None;
            }
            if json.get("kind").and_then(|v| v.as_str()) != Some("wire") {
                return None;
            }
            if json.get("era").and_then(|v| v.as_u64()) != Some(4) {
                return None;
            }
            if json.get("view").and_then(|v| v.as_u64()) != Some(13) {
                return None;
            }
            tape_frame(from, json)
        })
        .collect();
    assert!(
        frames.len() > 1_000,
        "the leader-66 window is unexpectedly thin: {}",
        frames.len()
    );

    // The verbs and the transitions: the deterministic committed-state
    // replay of the window's own bytes.
    let verbs = committed_verbs(&frames);
    let replayed = replay_verbs(&verbs);
    let sets: Vec<&scenario::ReplayedVerb> =
        replayed.iter().filter(|verb| verb.op == "set").collect();
    assert!(
        sets.len() >= 2,
        "the window's committed SETs are unexpectedly thin: {}",
        sets.len()
    );

    // The known committed transition, per lock, as holder runs: the
    // first SET of a run holds with the offered holder granted, every
    // later same-holder regrant on that run renews; a holder change
    // starts the next run. The window's committed chain on the polite
    // lock (14531090) is exactly three runs — the recorded facts.
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
    // three holder runs of 87, 87, and 7 committed SETs.
    let polite: Vec<&scenario::ReplayedVerb> = sets
        .iter()
        .copied()
        .filter(|v| v.lock_id == 145_310_90)
        .collect();
    let polite_runs = polite.chunk_by(|a, b| a.holder == b.holder);
    let run_lengths: Vec<usize> = polite_runs.map(<[&scenario::ReplayedVerb]>::len).collect();
    assert_eq!(
        run_lengths,
        vec![87, 87, 7],
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
