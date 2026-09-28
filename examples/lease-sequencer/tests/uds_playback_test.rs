//! The message-playback tests: the corpus is RECORDED IN-TEST (record-
//! once-then-replay) — the former gitignored rig pull
//! (`.tmp/telemetry/threenode-2026-09-14/`) was dead for any fresh clone
//! — so each test records its own wire stream into the repository's
//! `.tmp/` scratch through the vendored record layer and replays it in
//! the same run. The recorded stream's grammar mirrors the 2026-09-14
//! three-node rig pull's shape: era 4, leader 66, six members (44/55/66
//! voters + 77/88/99 weight-0 standbys), a loud heartbeat GET noise
//! floor on the sequencer's sentinel lock, the voters' renewal SETs on
//! that lock at the polite ~251 ms cadence, and the polite lock carrying
//! GETs only — the polite clients' SETs never entered the commit stream.

#[path = "recorder/mod.rs"]
mod recorder;

use lease_sequencer::phi::Trailer;
use lunet_advisory_lock::locks::{LeaseCandidate, Request, Service, Transition};
use lunet_locks_aof::envelope::Record;
use recorder::{commit_frame, prepare_frame, write_aof};
use serde_json::Value;
use std::collections::BTreeMap;
use std::path::PathBuf;
use uuid::Uuid;
use vrr::journal::Payload;
use vrr::message::{Body, Message};
use vrr::wire::Unpack;

/// The sentinel lock: the sequencer's own heartbeat GET noise floor.
const SENTINEL_LOCK: u64 = 0x0DDBA11;
/// The polite lock: the clients whose ops never committed on the rig.
const POLITE_LOCK: u64 = 0x0DDBA12;
const ERA: u32 = 4;
const VIEW: u32 = 13;
const LEADER: u32 = (66 << 16) | 1;
/// The heartbeat noise floor (the recorded loudness) and the renewal
/// run it carries.
const HEARTBEAT_GETS: usize = 900;
const RENEWALS: usize = 120;
/// The renewal cadence: consecutive same-holder renewals 251 ms apart.
const RENEWAL_GAP_MS: u64 = 251;
/// The heartbeat cadence.
const HEARTBEAT_GAP_MS: u64 = 250;
/// The recording's fixed base clock (ms), so the replay's execution
/// ticks are the recorded clocks.
const BASE_MS: u64 = 1_793_000_000_000;
/// The renewal SETs' holder: one holder across the whole run.
const RENEWAL_HOLDER: Uuid = Uuid::from_u128(0x00DD_BA11_0001);

/// One decoded committed op from the recorded wire stream.
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
}

/// One recorded wire datagram: the raw bytes as the network carried them
/// (phi trailer included when present).
struct RigDatagram {
    ns: u64,
    wire: Vec<u8>,
    tag: u32,
}

/// Records the wire stream into a fresh telemetry AOF under `root` and
/// returns the corpus directory: the heartbeat GET noise floor, the
/// renewal SET run, the polite lock's GET pair, and one trailed leader
/// Commit — every frame at era 4 view 13.
fn record_corpus(root: &std::path::Path) -> PathBuf {
    let dir = root.join("telemetry");
    let mut records: Vec<Record> = Vec::new();
    // The heartbeat GET noise floor.
    for index in 0..HEARTBEAT_GETS {
        let ns = (BASE_MS + index as u64 * HEARTBEAT_GAP_MS) * 1_000_000;
        let request = Request::Get {
            message_id: Uuid::from_u128(index as u128 + 1),
            client_id: 1,
            request_num: index as u64 + 1,
            lock_id: SENTINEL_LOCK,
        };
        records.push(Record::wire(
            ns,
            &prepare_frame(
                ERA,
                VIEW,
                10_000 + index as u64,
                Uuid::from_u128(index as u128 + 1),
                &serde_json::to_vec(&request).expect("the verb serializes"),
                10_000 + index as u64 - 1,
            ),
        ));
    }
    // The voters' renewal run on the sentinel lock.
    for index in 0..RENEWALS {
        let ns = (BASE_MS + 100 + index as u64 * RENEWAL_GAP_MS) * 1_000_000;
        let request = Request::Set {
            message_id: Uuid::from_u128(0x1_0000_0000 + index as u128),
            client_id: 2,
            request_num: index as u64 + 1,
            lock_id: SENTINEL_LOCK,
            lease: LeaseCandidate {
                lease_id: 1,
                holder: RENEWAL_HOLDER,
                // The lease window outlives the whole renewal run the
                // test replays: the 251 ms cadence renews a live lease,
                // never re-holds an expired one.
                lease_ms: 5_000,
            },
            name: None,
            labels: None,
            sent_at_ms: None,
        };
        records.push(Record::wire(
            ns,
            &prepare_frame(
                ERA,
                VIEW,
                20_000 + index as u64,
                Uuid::from_u128(0x1_0000_0000 + index as u128),
                &serde_json::to_vec(&request).expect("the verb serializes"),
                10_999,
            ),
        ));
    }
    // The polite lock's GET pair — the polite clients' SETs never enter
    // the recorded stream.
    for index in 0..2u64 {
        let ns = (BASE_MS + 1000 + index * 1000) * 1_000_000;
        let request = Request::Get {
            message_id: Uuid::from_u128(0x2_0000_0000 + index as u128),
            client_id: 3,
            request_num: index + 1,
            lock_id: POLITE_LOCK,
        };
        records.push(Record::wire(
            ns,
            &prepare_frame(
                ERA,
                VIEW,
                30_000 + index,
                Uuid::from_u128(0x2_0000_0000 + index as u128),
                &serde_json::to_vec(&request).expect("the verb serializes"),
                10_999,
            ),
        ));
    }
    // One trailed leader Commit closing the window.
    let sent_at_ms = BASE_MS + 2_500;
    let ns = sent_at_ms * 1_000_000;
    records.push(Record::wire(
        ns,
        &commit_frame(
            ERA,
            VIEW,
            11_000,
            11_000,
            &Trailer {
                era: ERA,
                leader: LEADER,
                seq: 7,
                sent_at_ms,
            },
        ),
    ));
    records.sort_by_key(|record| record.ns);
    write_aof(&dir, BASE_MS / 1000, &records);
    dir
}

/// Extracts every wire datagram and committed op from the freshly
/// recorded corpus. The recorder's AOFs carry each datagram at its local
/// ns receipt.
fn extract_corpus() -> (Vec<RigDatagram>, Vec<RigOp>) {
    let root = recorder::temp_root("uds-playback");
    let dir = record_corpus(&root);
    let mut datagrams = Vec::new();
    let mut ops = Vec::new();
    let files = lunet_locks_aof::retention::list_aof_files(&dir)
        .unwrap_or_else(|e| panic!("corpus {} unreadable: {e}", dir.display()));
    assert!(
        !files.is_empty(),
        "corpus {} has no .aof files",
        dir.display()
    );
    for (path, _, _) in &files {
        let mut iter =
            unsafe { lunet_locks_aof::ffi::RawIter::open(path.to_string_lossy().as_bytes()) }
                .unwrap_or_else(|e| panic!("iterator open {}: {e:?}", path.display()));
        while let Some(entry) = iter.next_entry().expect("read entry") {
            let Some(record) = Record::decode(&entry.bytes) else {
                continue;
            };
            if record.marker != lunet_locks_aof::envelope::Marker::Wire {
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
                tag: message.header.tag as u32,
            });
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
                payload: payload.to_vec(),
                offered_holder,
            });
        }
    }
    let _ = std::fs::remove_dir_all(&root);
    (datagrams, ops)
}

/// The recorded stream's facts, pinned: the heartbeat GET noise floor is
/// loud, and the polite lock carries GETs only — no polite SET ever
/// enters the commit stream.
#[test]
fn given_the_recorded_stream_the_polite_lock_carries_no_sets() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// GIVEN the recorded renewal SETs (the sentinel lock's driver chase)
/// replayed in record order at their recorded clocks, EXPECT the first
/// grant to be a Hold and every same-holder regrant a Renew with the
/// renew counter climbing — the polite cadence machinery working
/// server-side on the recorded bytes.
#[test]
fn given_the_recorded_renewal_sets_service_holds_then_renews() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// GIVEN a recorded heartbeat GET at its recorded clock, EXPECT the
/// Service's GET reply shape with the leader's execution tick echoed.
#[test]
fn given_the_recorded_heartbeat_get_reply_shape() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// GIVEN a recorded Commit datagram with its phi trailer, EXPECT the
/// trailer to decode with the era-4 leader-66 facts and a send clock at
/// or before the receiving node's record clock.
#[test]
fn given_the_recorded_trailed_commit_the_phi_trailer_decodes() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// GIVEN a recorded SET re-delivered through the harness cluster (the
/// same core, the UDS transport), EXPECT the fresh identity to be
/// granted on the live node, and the re-delivery of the SAME op
/// (message_id reused) to replay the first reply byte-exactly — nobody
/// re-executes, nobody double-grants. The fencing story across the
/// transport swap: staleness does not exist on the wire, identity does.
#[test]
fn given_a_recorded_set_through_the_harness_cluster_the_replay_is_deduped() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The recorded stream's committed-op census by lock: printed for the
/// record.
#[test]
fn the_recorded_corpus_census() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
