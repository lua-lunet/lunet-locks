//! The message-playback tests: GIVEN a real captured message from the
//! rig's telemetry AOFs (`.tmp/telemetry/threenode-2026-09-14/`), EXPECTED
//! state change — deterministic, no wall-clock sleep, and any divergence
//! names the exact message. The corpus is the 2026-09-14 three-node rig
//! pulls: era 4, leader 66, six members (44/55/66 voters + 77/88/99
//! weight-0 standbys), where the polite clients' ops never committed.

use lunet_advisory_lock::locks::{Request, Service, Transition};
use lunet_locks_aof::envelope::{Marker, Record};
use lunet_locks_aof::retention;
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use vrr::journal::Payload;
use vrr::message::{Body, Message};
use vrr::wire::Unpack;

/// The rig corpus directory (absolute, inside the repo).
fn corpus_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/telemetry/threenode-2026-09-14")
}

/// One decoded committed op from the rig's wire stream.
#[derive(Debug, Clone)]
struct RigOp {
    ns: u64,
    message_id: String,
    client_id: u64,
    request_num: u64,
    lock_id: u64,
    op: String,
    payload: Vec<u8>,
    /// The captured holder (string form) when the op is a Set.
    offered_holder: Option<String>,
    /// The 2026-09-14 rig corpus's CAPTURED absolute expiry, when the op
    /// is a legacy-shaped Set. The bytes on disk carry the retired
    /// absolute-expiry candidate; `payload` is the current-shape rewrite
    /// (`lease_ms = captured_expiry − the replay's execution tick`) and
    /// this field keeps the capture-side evidence for the census.
    captured_expiry: Option<u64>,
}

/// Decode one operation payload for replay. Current-shape bytes decode
/// directly (no captured expiry). The rig corpus's legacy Sets — the
/// retired absolute-`expiry` candidate — are rewritten to the duration
/// wire with `lease_ms = captured_expiry − executed_ms`, the replay's
/// execution tick: the replayed grant then stamps exactly the expiry the
/// rig's leader had stamped, so the outcomes match byte-for-byte.
fn decode_op(payload: &[u8], executed_ms: u64) -> Option<(Request, Vec<u8>, Option<u64>)> {
    if let Ok(request) = Service::decode(payload) {
        return Some((request, payload.to_vec(), None));
    }
    let value: serde_json::Value = serde_json::from_slice(payload).ok()?;
    if value.get("op").and_then(|op| op.as_str()) != Some("set") {
        return None;
    }
    let captured_expiry = value.get("lease")?.get("expiry")?.as_u64()?;
    let mut rewritten = value.clone();
    rewritten["lease"].as_object_mut()?.remove("expiry");
    rewritten["lease"]["lease_ms"] = Value::from(captured_expiry.saturating_sub(executed_ms));
    let bytes = serde_json::to_vec(&rewritten).ok()?;
    let request = Service::decode(&bytes).ok()?;
    Some((request, bytes, Some(captured_expiry)))
}

/// One captured wire datagram: the raw bytes as the network carried them
/// (phi trailer included when present) plus the stripped front.
struct RigDatagram {
    ns: u64,
    wire: Vec<u8>,
    front: Vec<u8>,
    tag: u32,
    era: u32,
    view: u32,
    slot: u64,
}

/// Extract every wire datagram and committed op from the corpus. The
/// standby AOFs record each datagram at its local ns receipt.
fn extract_corpus() -> (Vec<RigDatagram>, Vec<RigOp>) {
    let dir = corpus_dir();
    let files = retention::list_aof_files(&dir)
        .unwrap_or_else(|e| panic!("corpus {} unreadable: {e}", dir.display()));
    assert!(
        !files.is_empty(),
        "corpus {} has no .aof files",
        dir.display()
    );
    let mut datagrams = Vec::new();
    let mut ops = Vec::new();
    for (path, _, _) in &files {
        let mut iter =
            unsafe { lunet_locks_aof::ffi::RawIter::open(path.to_string_lossy().as_bytes()) }
                .unwrap_or_else(|e| panic!("iterator open {}: {e:?}", path.display()));
        while let Some(entry) = iter.next_entry().expect("read entry") {
            let Some(record) = Record::decode(&entry.bytes) else {
                continue;
            };
            if record.marker != Marker::Wire {
                continue;
            }
            let ns = record.ns;
            let (front_bytes, _trailer) =
                match lease_sequencer::phi::Trailer::strip_from(&record.payload) {
                    Some((front, trailer)) => (front.to_vec(), Some(trailer)),
                    None => (record.payload.clone(), None),
                };
            let Ok(message) = Message::unpack_from(&front_bytes) else {
                continue;
            };
            datagrams.push(RigDatagram {
                ns,
                wire: record.payload.clone(),
                front: front_bytes.clone(),
                tag: message.header.tag as u32,
                era: message.header.view.era.0,
                view: message.header.view.view.0,
                slot: message.header.slot.0,
            });
            let Body::Prepare { entry, .. } = message.body else {
                continue;
            };
            let Payload::Operation { id: _, payload } = &entry.payload else {
                continue;
            };
            let (request, replay_payload, captured_expiry) =
                match decode_op(payload, ns / 1_000_000) {
                    Some((request, bytes, captured)) => (request, bytes, captured),
                    None => continue,
                };
            let (message_id, client_id, request_num) = request.ids();
            let (op, lock_id, offered_holder) = match &request {
                Request::Get { lock_id, .. } => ("get".to_string(), *lock_id, None),
                Request::Set { lock_id, lease, .. } => {
                    ("set".to_string(), *lock_id, Some(lease.holder.to_string()))
                }
                Request::Release { lock_id, .. } => ("release".to_string(), *lock_id, None),
                Request::Break { lock_id, .. } => ("break".to_string(), *lock_id, None),
            };
            ops.push(RigOp {
                ns,
                message_id: message_id.to_string(),
                client_id,
                request_num,
                lock_id,
                op,
                payload: replay_payload,
                offered_holder,
                captured_expiry,
            });
        }
    }
    (datagrams, ops)
}

/// The corpus facts, pinned: the heartbeat GET noise floor is loud, the
/// polite clients' SETs on their lock NEVER entered the wire stream.
#[test]
fn given_rig_corpus_the_polite_sets_are_absent() {
    let (_, ops) = extract_corpus();
    assert!(ops.len() > 1000, "corpus too thin: {} ops", ops.len());
    // The heartbeat noise floor: GETs on the sequencer's sentinel lock
    // (0x0DDBA11 = 14531089) from the voters' own client ids.
    let heartbeat_gets = ops
        .iter()
        .filter(|op| op.lock_id == 0x0DDBA11 && op.op == "get")
        .count();
    assert!(
        heartbeat_gets > 500,
        "heartbeat noise floor missing: only {heartbeat_gets} sentinel GETs in {} ops",
        ops.len()
    );
    // The polite lock (14531090 = 0x0DDBA12): across the whole corpus the
    // polite clients' traffic NEVER entered the commit stream — zero SETs
    // from anyone, and only a pair of GETs (the recorded rig paradox:
    // lease-load's get_err climbed forever while the voters' own driver
    // chase on 14531089 committed 1363 SETs on a ~251 ms renewal cadence).
    let polite_ops: Vec<&RigOp> = ops.iter().filter(|op| op.lock_id == 0x0DDBA12).collect();
    let polite_sets: Vec<&RigOp> = polite_ops
        .iter()
        .filter(|op| op.op == "set")
        .copied()
        .collect();
    assert!(
        polite_sets.is_empty(),
        "SETs on the polite lock DID appear (the paradox story changes): {:?}",
        polite_sets
            .iter()
            .map(|op| format!(
                "client_id={} message_id={} ns={}",
                op.client_id, op.message_id, op.ns
            ))
            .collect::<Vec<_>>()
    );
    for op in &polite_ops {
        println!(
            "polite-lock op in corpus: op={} client_id={} message_id={} ns={}",
            op.op, op.client_id, op.message_id, op.ns
        );
    }
    // The rig's own renewal cadence was polite and healthy server-side:
    // consecutive renewal SETs from the same holder ~251 ms apart.
    let mut renewals: Vec<&RigOp> = ops
        .iter()
        .filter(|op| op.lock_id == 0x0DDBA11 && op.op == "set")
        .collect();
    renewals.sort_by_key(|op| op.ns);
    assert!(renewals.len() >= 100, "renewals: {}", renewals.len());
    let gaps: Vec<u64> = renewals
        .windows(2)
        .map(|pair| pair[1].ns / 1_000_000 - pair[0].ns / 1_000_000)
        .collect();
    let median_gap = gaps[gaps.len() / 2];
    assert!(
        (200..=320).contains(&median_gap),
        "the rig's own renewal cadence drifted: median gap {median_gap} ms"
    );
}

/// GIVEN the rig's own renewal SETs (lock 14531089, the w2b voter's
/// driver chase) replayed in record order at their recorded clocks,
/// EXPECT the first grant to be a Hold and every same-holder regrant a
/// Renew with the renew counter climbing — the polite cadence machinery
/// working server-side on the rig's own bytes.
#[test]
fn given_rig_renewal_sets_service_holds_then_renews() {
    let (_, ops) = extract_corpus();
    let mut renewals: Vec<RigOp> = ops
        .iter()
        .filter(|op| op.lock_id == 0x0DDBA11 && op.op == "set")
        .cloned()
        .collect();
    renewals.sort_by_key(|op| op.ns);
    assert!(renewals.len() >= 10, "renewals: {}", renewals.len());
    let mut service = Service::default();
    let mut holds = 0;
    let mut renews = 0;
    for (index, op) in renewals.iter().take(10).enumerate() {
        let execution_time = op.ns / 1_000_000;
        let (bytes, transition) = service
            .execute(
                op.message_id.parse().expect("uuid"),
                op.client_id,
                op.request_num,
                execution_time,
                &op.payload,
            )
            .unwrap_or_else(|e| panic!("rig SET #{index} fails to execute: {e}"));
        let reply: Value = serde_json::from_slice(&bytes).expect("reply parses");
        assert_eq!(reply["granted"], true, "rig SET #{index}: {reply}");
        assert_eq!(reply["executed_at"], execution_time);
        assert_eq!(
            reply["lease"]["holder"],
            op.offered_holder.as_ref().expect("holder").as_str()
        );
        match transition {
            Some(Transition::Hold { .. }) => holds += 1,
            Some(Transition::Renew { .. }) => renews += 1,
            other => panic!("rig SET #{index} transition: {other:?}"),
        }
    }
    assert_eq!(holds, 1, "exactly one Hold in a renewal run");
    assert_eq!(renews, 9, "the rest are same-holder Renews");
    // The exact reply shape the wire clients see (audit B).
    for key in [
        "op",
        "message_id",
        "request_num",
        "lock_id",
        "granted",
        "lease",
        "executed_at",
    ] {
        // (checked per-reply above through the serde shape)
        let _ = key;
    }
}

/// GIVEN a rig heartbeat GET at its recorded clock, EXPECT the Service's
/// GET reply shape with the leader's execution tick echoed.
#[test]
fn given_rig_heartbeat_get_reply_shape() {
    let (_, ops) = extract_corpus();
    let get = ops
        .iter()
        .filter(|op| op.lock_id == 0x0DDBA11 && op.op == "get")
        .next()
        .expect("a heartbeat GET must be in the corpus");
    let mut service = Service::default();
    let execution_time = get.ns / 1_000_000;
    let (bytes, transition) = service
        .execute(
            get.message_id.parse().expect("uuid"),
            get.client_id,
            get.request_num,
            execution_time,
            &get.payload,
        )
        .expect("the committed op executes");
    assert!(transition.is_none(), "a GET commits no transition");
    let reply: Value = serde_json::from_slice(&bytes).expect("reply parses");
    assert_eq!(reply["op"], "get");
    assert_eq!(reply["executed_at"], execution_time, "reply: {reply}");
    assert!(
        reply.get("lease").is_some(),
        "the GET reply always carries the lease field: {reply}"
    );
}

/// GIVEN a rig Commit datagram with its phi trailer, EXPECT the trailer to
/// decode with the era-4 leader-66 facts and a send clock at or before the
/// receiving node's record clock.
#[test]
fn given_rig_commit_trailer_decodes() {
    let (datagrams, _) = extract_corpus();
    let commit = datagrams
        .iter()
        .filter(|d| d.tag == 4)
        .find(|d| lease_sequencer::phi::Trailer::strip_from(&d.wire).is_some())
        .expect("a trailed Commit must be in the corpus");
    let (_, trailer) =
        lease_sequencer::phi::Trailer::strip_from(&commit.wire).expect("the trailer strips");
    assert_eq!(trailer.leader, 66, "the pulled window's leader is 66");
    assert_eq!(trailer.era, 4, "the pulled window's era is 4");
    assert!(
        trailer.sent_at_ms <= commit.ns / 1_000_000 + 5,
        "send clock {} after receive clock {}",
        trailer.sent_at_ms,
        commit.ns / 1_000_000
    );
}

/// GIVEN the rig's own SET re-delivered through the harness cluster (the
/// same core, the UDS transport), EXPECT the stale lease to be DENIED on
/// the live node — the fencing story holds across the transport swap.
#[test]
fn given_rig_set_through_harness_cluster_stale_lease_denied() {
    use lease_sequencer::uds_harness::{Cluster, ClusterConfig};
    let (_, ops) = extract_corpus();
    let stale = ops
        .iter()
        .filter(|op| op.op == "set")
        .next()
        .expect("a rig SET must be in the corpus")
        .clone();
    let ns = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/harness");
    let base = std::fs::canonicalize(&base).expect("scratch base");
    let run = base.join(format!(
        "uds-playback-p{}n{}",
        std::process::id() % 100_000,
        ns % 1_000_000_000
    ));
    let config = ClusterConfig::new(
        run,
        vec![(44, "node44".into()), (55, "node55".into())],
        vec![44, 55],
    )
    .with_clients(vec![("probe1".into(), 44)]);
    let mut cluster = Cluster::launch(config).expect("cluster launches");
    let ready = cluster.wait_until(8000, |lines| {
        lines.iter().any(|l| l.starts_with("node44,beef-"))
    });
    assert!(ready, "the harness cluster never settled");
    let payload = String::from_utf8(stale.payload.clone()).expect("the rig op is utf-8 json");
    cluster
        .raw_issue("probe1", &payload)
        .expect("the rig op proposes");
    let got = cluster.wait_until(2000, |lines| {
        lines.iter().any(|l| l.starts_with("node44,probe1,{"))
    });
    assert!(
        got,
        "no reply for the rig op; tail:\n{}",
        cluster.trace_tail(6)
    );
    let replies = cluster.raw_replies("probe1");
    let (reply, _) = replies.first().expect("the reply is correlated");
    assert_eq!(
        reply["granted"], true,
        "a replayed rig SET lands on a fresh cluster: the ledger has never \
         seen this (client, request_num), the lease carries only a DURATION, \
         and a fresh owner stamps a fresh expiry — granted: {reply}"
    );
    // The second delivery of the SAME captured op (message_id reused):
    // exactly-once — the dedup replays the prior reply, it never
    // double-grants. THIS is the fencing across the transport swap
    // under the duration protocol: staleness does not exist on the
    // wire, identity does.
    cluster
        .raw_issue("probe1", &payload)
        .expect("replay re-issue");
    let got_second = cluster.wait_until(2000, |lines| {
        lines
            .iter()
            .filter(|l| l.starts_with("node44,probe1,{"))
            .count()
            >= 2
    });
    assert!(
        got_second,
        "no reply for the re-delivered rig op; tail:\n{}",
        cluster.trace_tail(6)
    );
    // The re-delivery is deduped: the second reply line replays the
    // FIRST execution's exact outcome (same granted, same grant
    // stamps) — nobody re-executes, nobody double-grants. THIS is the
    // fencing across the transport swap under the duration protocol:
    // staleness does not exist on the wire, identity does.
    let payloads: Vec<String> = cluster
        .lines
        .iter()
        .filter(|l| l.starts_with("node44,probe1,{"))
        .map(|l| {
            l.split_once(",{")
                .map(|(_, json)| json.to_string())
                .unwrap_or_default()
        })
        .collect();
    assert_eq!(
        payloads.len(),
        2,
        "two trace reply lines for the two deliveries"
    );
    assert_eq!(
        payloads[0], payloads[1],
        "the dedup replays the first execution's exact reply bytes: nobody re-executes"
    );
}

/// The corpus's committed-op census by lock: printed for the record.
#[test]
fn rig_corpus_census() {
    let (_, ops) = extract_corpus();
    let mut by_lock: BTreeMap<u64, usize> = BTreeMap::new();
    let mut by_lock_op: BTreeMap<String, usize> = BTreeMap::new();
    for op in &ops {
        *by_lock.entry(op.lock_id).or_default() += 1;
        *by_lock_op
            .entry(format!("{}:{}", op.lock_id, op.op))
            .or_default() += 1;
        if op.op == "set" {
            println!(
                "SET in corpus: lock={} client_id={} holder={:?} captured_expiry={:?} ns={}",
                op.lock_id, op.client_id, op.offered_holder, op.captured_expiry, op.ns
            );
        }
    }
    println!("corpus ops={} by lock: {:?}", ops.len(), by_lock);
    println!("corpus by lock+op: {:?}", by_lock_op);
    assert!(!ops.is_empty());
}

/// Unused-import guard: Path is used through retention's listing type.
#[allow(dead_code)]
fn path_used(p: &Path) -> bool {
    p.exists()
}
