//! The AOF console bridge (item23): red/green tests for the decode and
//! replay surface. The fixture is a real AOF series written through the
//! vendored TigerBeetle writer (AofFile) with envelope records whose Wire
//! payloads are packed uVRR messages carrying Service request payloads —
//! the same bytes the standby learner records. The bridge must decode them
//! through the existing surfaces only: `Record::decode`,
//! `Message::unpack_from`, `Service::decode`, and the `Service::execute`
//! state machine (the exact committed-work path the adapter's
//! `Effect::Apply` arm runs).

use lease_sequencer::bridge::{self, replay_series};
use lunet_locks_aof::envelope::{Marker, Record};
use lunet_locks_aof::{AofFile, Options};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use uuid::Uuid;
use vrr::configuration::SystemOperation;
use vrr::ids::{Era, NodeId, OperationId, Slot, View, ViewId};
use vrr::journal::{LogEntry, Payload};
use vrr::message::{Body, Message};
use vrr::wire::Header;
use vrr::wire::{Pack, Tag};

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lease-sequencer-bridge-{name}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A message_id's operation identity, exactly as the adapter maps it
/// (ffi.rs `operation_id`): first 8 bytes big-endian msb, last 8 lsb.
fn operation_id(message_id: [u8; 16]) -> OperationId {
    OperationId {
        msb: u64::from_be_bytes(message_id[..8].try_into().unwrap()),
        lsb: u64::from_be_bytes(message_id[8..].try_into().unwrap()),
    }
}

/// One Prepare wire message carrying a Service request payload, packed with
/// the core's Pack impl — byte-identical to what the network carried.
fn prepare_wire(message_id: Uuid, json: &str, slot: u64) -> Vec<u8> {
    let entry = LogEntry {
        slot: Slot(slot),
        era: Era(0),
        payload: Payload::Operation {
            id: operation_id(*message_id.as_bytes()),
            payload: json.as_bytes().to_vec().into_boxed_slice(),
        },
    };
    let message = Message {
        header: Header {
            tag: Tag::Prepare,
            view: ViewId {
                era: Era(0),
                view: View(0),
            },
            slot: Slot(slot),
        },
        body: Body::Prepare {
            entry,
            committed: Slot(0),
        },
    };
    let mut buf = vec![0u8; message.packed_len()];
    let written = message.pack_into(&mut buf).unwrap();
    buf.truncate(written);
    buf
}

/// A non-lock-work wire message (a Commit frontier advance).
fn commit_wire() -> Vec<u8> {
    let message = Message {
        header: Header {
            tag: Tag::Commit,
            view: ViewId {
                era: Era(0),
                view: View(0),
            },
            slot: Slot(0),
        },
        body: Body::Commit { committed: Slot(4) },
    };
    let mut buf = vec![0u8; message.packed_len()];
    let written = message.pack_into(&mut buf).unwrap();
    buf.truncate(written);
    buf
}

fn request_json(op: &str, message_id: &Uuid, client_id: u64, request_num: u64, body: &str) -> String {
    format!(
        "{{\"op\":\"{op}\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{request_num}{body}}}"
    )
}

/// Append one Wire envelope record through the real AOF writer stack.
fn write_wire(aof: &mut AofFile, ns: u64, wire: &[u8]) {
    aof.append(&Record::wire(ns, wire).encode()).unwrap();
}

/// The fixture: a full committed lock story — acquire, renew, a competing
/// denied acquire, release, break — plus heartbeat Commit traffic and a
/// telemetry record. Returns the series dir.
fn fixture_aof(name: &str) -> std::path::PathBuf {
    let dir = temp_dir(name);
    let mut aof = AofFile::open(&dir).unwrap();

    let holder_a = Uuid::from_bytes([0xAA; 16]);
    let holder_b = Uuid::from_bytes([0xBB; 16]);

    // t = 1_700_000_000_123_456_789 ns → 1_700_000_000_123 ms exactly.
    let t0: u64 = 1_700_000_000_123_456_789;

    // 1. acquire: holder A sets lock 7 (no prior live record → Hold).
    let acquire_id = Uuid::new_v4();
    let acquire = request_json(
        "set",
        &acquire_id,
        1,
        1,
        &format!(
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":11,\"holder\":\"{holder_a}\",\"expiry\":1700000100000}},\"name\":\"/cluster/leader\",\"labels\":[\"smr\"]"
        ),
    );
    write_wire(&mut aof, t0, &prepare_wire(acquire_id, &acquire, 1));

    // 2. heartbeat Commit traffic (non-lock work).
    write_wire(&mut aof, t0 + 1_000_000, &commit_wire());

    // 3. renew: same holder extends (live prior → Renew).
    let renew_id = Uuid::new_v4();
    let renew = request_json(
        "set",
        &renew_id,
        1,
        2,
        &format!(
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":12,\"holder\":\"{holder_a}\",\"expiry\":1700000200000}},\"name\":\"/cluster/leader\""
        ),
    );
    write_wire(&mut aof, t0 + 2_000_000, &prepare_wire(renew_id, &renew, 2));

    // 4. denied competing acquire: holder B, lock still held by A.
    let deny_id = Uuid::new_v4();
    let deny = request_json(
        "set",
        &deny_id,
        2,
        1,
        &format!(
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":13,\"holder\":\"{holder_b}\",\"expiry\":1700000300000}},\"name\":\"/cluster/leader\""
        ),
    );
    write_wire(&mut aof, t0 + 3_000_000, &prepare_wire(deny_id, &deny, 3));

    // 5. release by holder A.
    let release_id = Uuid::new_v4();
    let release = request_json(
        "release",
        &release_id,
        1,
        3,
        &format!(",\"lock_id\":7,\"holder\":\"{holder_a}\",\"lease_id\":12"),
    );
    write_wire(&mut aof, t0 + 4_000_000, &prepare_wire(release_id, &release, 4));

    // 6. acquire again (lock free after release → Hold) so break has a record.
    let reacquire_id = Uuid::new_v4();
    let reacquire = request_json(
        "set",
        &reacquire_id,
        1,
        4,
        &format!(
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":14,\"holder\":\"{holder_a}\",\"expiry\":1700000400000}},\"name\":\"/cluster/leader\""
        ),
    );
    write_wire(&mut aof, t0 + 5_000_000, &prepare_wire(reacquire_id, &reacquire, 5));

    // 7. break: force-release whatever is stored.
    let break_id = Uuid::new_v4();
    let brk = request_json("break", &break_id, 3, 2, ",\"lock_id\":7");
    write_wire(&mut aof, t0 + 6_000_000, &prepare_wire(break_id, &brk, 6));

    // 8. one read-only Get (committed, but no lock event).
    let get_id = Uuid::new_v4();
    let get = request_json("get", &get_id, 4, 1, ",\"lock_id\":7");
    write_wire(&mut aof, t0 + 7_000_000, &prepare_wire(get_id, &get, 7));

    // 9. a telemetry record (marker 3): counted, never an event.
    aof.append(
        &Record::telemetry(
            Marker::TelemetryStateTransition,
            t0 + 8_000_000,
            b"{\"event\":\"teardown\"}",
        )
        .encode(),
    )
    .unwrap();

    aof.flush().unwrap();
    aof.close().unwrap();
    dir
}

// ------------------------------------------------------------ decode ----

/// The committed lock work decodes into the console's event kinds, in ns
/// order, with the record's ns clock converted to epoch ms at the boundary
/// (integer floor division by 1_000_000 — never widened back).
#[test]
fn decode_produces_events_with_ms_times() {
    let dir = fixture_aof("events");
    let (events, _state, metrics) = replay_series(&dir);

    let kinds: Vec<(&str, u64)> =
        events.iter().map(|e| (e.kind.as_str(), e.ts_ms)).collect();
    assert_eq!(
        kinds,
        vec![
            ("acquire", 1_700_000_000_123),
            ("renew", 1_700_000_000_125),
            ("deny", 1_700_000_000_126),
            ("release", 1_700_000_000_127),
            ("acquire", 1_700_000_000_128),
            ("break", 1_700_000_000_129),
        ]
    );

    // Event shape: the openapi Event fields are all present.
    let first = &events[0];
    assert_eq!(first.lock_id, 7);
    assert_eq!(first.name, "/cluster/leader");
    assert_eq!(first.seq, 1);
    assert!(first.ts_ms > 0);
    assert!(first.ns > 0);
    assert_eq!(first.ts_ms, first.ns / 1_000_000);

    // The acquire event carries the acquiring holder; the deny names the
    // refused one.
    assert!(first.holder.starts_with("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
    let deny = events.iter().find(|e| e.kind == "deny").unwrap();
    assert!(deny.holder.starts_with("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"));

    // Metrics: every message kind counted, lock events tallied, markers
    // tallied, first/last ns stamps.
    assert_eq!(metrics.lock_events.get("acquire"), Some(&2));
    assert_eq!(metrics.lock_events.get("renew"), Some(&1));
    assert_eq!(metrics.lock_events.get("deny"), Some(&1));
    assert_eq!(metrics.lock_events.get("release"), Some(&1));
    assert_eq!(metrics.lock_events.get("break"), Some(&1));
    assert_eq!(metrics.lock_events.get("get"), Some(&1));
    assert_eq!(metrics.messages.get("Prepare"), Some(&7));
    assert_eq!(metrics.messages.get("Commit"), Some(&1));
    assert_eq!(metrics.markers.get("wire"), Some(&8));
    assert_eq!(metrics.markers.get("state_transition"), Some(&1));
    assert_eq!(metrics.records, 9);
    assert_eq!(metrics.first_ns, Some(1_700_000_000_123_456_789));
    assert_eq!(metrics.last_ns, Some(1_700_000_000_131_456_789));
    std::fs::remove_dir_all(&dir).unwrap();
}

/// /locks state is the replayed Service state: the broken lock is free with
/// the bumped keeper lease_id, the name/labels are sticky.
#[test]
fn replayed_lock_state_matches() {
    let dir = fixture_aof("state");
    let (_events, state, _metrics) = replay_series(&dir);

    let lock = state.locks.get(&7).expect("lock 7 was touched");
    assert_eq!(lock.name.as_deref(), Some("/cluster/leader"));
    assert_eq!(lock.labels, vec!["smr".to_string()]);
    assert_eq!(lock.state, "free", "broken: keeper expiry 0 → free");
    assert_eq!(lock.fencing_token, 15, "keeper lease_id = broken 14 + 1");
    assert_eq!(lock.holder_changes, 2, "two Hold transitions");
    assert_eq!(lock.renew_count, 0, "break zeros the renew counter");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// An unknown envelope marker is rejected — never guessed. The record is
/// counted undecodable and produces no event.
#[test]
fn unknown_markers_are_rejected() {
    let dir = temp_dir("unknown-marker");
    let mut aof = AofFile::open(&dir).unwrap();
    let mut bytes = Record::wire(1, b"payload").encode();
    bytes[0] = 7; // unknown marker byte
    aof.append(&bytes).unwrap();
    aof.flush().unwrap();
    aof.close().unwrap();

    let (events, _state, metrics) = replay_series(&dir);
    assert!(events.is_empty());
    assert_eq!(metrics.undecodable, 1);
    assert_eq!(metrics.records, 0, "rejected records are not counted as read");
    std::fs::remove_dir_all(&dir).unwrap();
}

/// A Wire payload that is not a uVRR message (garbage) is counted as an
/// undecodable wire message, never an event.
#[test]
fn garbage_wire_payload_is_counted_not_decoded() {
    let dir = temp_dir("garbage-wire");
    let mut aof = AofFile::open(&dir).unwrap();
    write_wire(&mut aof, 1_700_000_000_000_000_000, &[0xFF; 40]);
    aof.flush().unwrap();
    aof.close().unwrap();

    let (events, _state, metrics) = replay_series(&dir);
    assert!(events.is_empty());
    assert_eq!(metrics.undecodable, 1);
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The series reader is read-only: replaying twice yields identical state
/// and never mutates the series (same file set, same sizes).
#[test]
fn replay_is_read_only_and_deterministic() {
    let dir = fixture_aof("readonly");
    let snapshot = || {
        let mut files: Vec<(String, u64)> = std::fs::read_dir(&dir)
            .unwrap()
            .map(|e| {
                let e = e.unwrap();
                let name = e.file_name().to_string_lossy().to_string();
                let size = e.metadata().unwrap().len();
                (name, size)
            })
            .collect();
        files.sort();
        files
    };
    let before = snapshot();

    let first = replay_series(&dir);
    let second = replay_series(&dir);
    assert_eq!(first.0, second.0);
    assert_eq!(first.2.records, second.2.records);

    assert_eq!(before, snapshot());
    std::fs::remove_dir_all(&dir).unwrap();
}

// -------------------------------------------------------------- HTTP ----

fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    write!(
        stream,
        "GET {path} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n"
    )
    .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, body)| body.to_string())
        .unwrap_or_default();
    (status, body)
}

/// The HTTP surface: the endpoints answer with the console's shapes.
#[test]
fn http_endpoints_serve_console_shapes() {
    let dir = fixture_aof("http");
    let server = bridge::Server::spawn(&dir, "127.0.0.1:0", false).unwrap();

    let (status, body) = http_get(server.port(), "/api/v1/health");
    assert_eq!(status, 200);
    let health: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(health["status"], "ok");
    assert!(health["nowMs"].as_u64().unwrap() > 0);

    let (status, body) = http_get(server.port(), "/api/v1/locks");
    assert_eq!(status, 200);
    let locks: Value = serde_json::from_str(&body).unwrap();
    assert!(locks["nowMs"].as_u64().unwrap() > 0);
    let lock = &locks["locks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|l| l["id"] == 7)
        .expect("lock 7 present");
    assert_eq!(lock["name"], "/cluster/leader");
    assert_eq!(lock["state"], "free");
    assert_eq!(lock["fencingToken"], 15);
    assert_eq!(lock["renewCount"], 0);
    assert_eq!(lock["holderChanges"], 2);

    let (status, body) = http_get(server.port(), "/api/v1/events");
    assert_eq!(status, 200);
    let events: Value = serde_json::from_str(&body).unwrap();
    let list = events["events"].as_array().unwrap();
    assert_eq!(list.len(), 6);
    assert_eq!(list[0]["kind"], "break", "newest first");
    assert!(list[0]["tsMs"].as_u64().unwrap() > 0);
    assert!(list[0]["seq"].as_u64().unwrap() > 0);

    let (status, body) = http_get(server.port(), "/api/v1/metrics");
    assert_eq!(status, 200);
    let metrics: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(metrics["records"], 9);
    assert_eq!(metrics["markers"]["wire"], 8);
    assert_eq!(metrics["messages"]["Prepare"], 7);
    assert_eq!(metrics["lockEvents"]["acquire"], 2);
    assert!(metrics["firstNs"].as_u64().unwrap() > 0);
    assert!(metrics["lastNs"].as_u64().unwrap() > 0);

    let (status, _) = http_get(server.port(), "/api/v1/nope");
    assert_eq!(status, 404);

    server.shutdown();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The live WebSocket push: with follow on, an appended record arrives as a
/// text frame on /api/v1/live.
#[test]
fn follow_pushes_new_events_over_websocket() {
    let dir = fixture_aof("ws");
    let server = bridge::Server::spawn(&dir, "127.0.0.1:0", true).unwrap();

    let mut stream = TcpStream::connect(("127.0.0.1", server.port())).unwrap();
    write!(
        stream,
        "GET /api/v1/live HTTP/1.1\r\nHost: 127.0.0.1\r\n\
         Upgrade: websocket\r\nConnection: Upgrade\r\n\
         Sec-WebSocket-Key: dGhlIHNhbXBsZSBub25jZQ==\r\n\
         Sec-WebSocket-Version: 13\r\n\r\n"
    )
    .unwrap();
    let mut buf = [0u8; 1024];
    let n = stream.read(&mut buf).unwrap();
    let handshake = String::from_utf8_lossy(&buf[..n]).into_owned();
    assert!(handshake.starts_with("HTTP/1.1 101"), "{handshake}");
    // RFC 6455 example vector: the accept-key derivation is correct.
    assert!(handshake.contains("s3pPLMBiTxaQ9kYGzzhZRbK+xOo="));

    // Grow the series the way a standby roll does: a NEW active file in the
    // same directory, appended through the real writer stack.
    let holder = Uuid::from_bytes([0xAA; 16]);
    let id = Uuid::new_v4();
    let json = request_json(
        "set",
        &id,
        9,
        1,
        &format!(
            ",\"lock_id\":8,\"lease\":{{\"lease_id\":1,\"holder\":\"{holder}\",\"expiry\":1700000500000}},\"name\":\"/jobs/compact/shard-00\""
        ),
    );
    let wire = prepare_wire(id, &json, 20);
    let mut next = AofFile::open_with(
        &dir,
        Options {
            force_flush: true,
            retention_bytes: u64::MAX,
        },
    )
    .unwrap();
    let record = Record::wire(1_700_000_000_140_000_000, &wire);
    next.append(&record.encode()).unwrap();
    next.flush().unwrap();
    next.close().unwrap();

    // Read one text frame: 2-byte header, optional extended length, payload.
    let mut header = [0u8; 2];
    stream.read_exact(&mut header).unwrap();
    let mut len = (header[1] & 0x7F) as usize;
    if len == 126 {
        let mut ext = [0u8; 2];
        stream.read_exact(&mut ext).unwrap();
        len = u16::from_be_bytes(ext) as usize;
    }
    assert_eq!(header[0] & 0x0F, 1, "text frame");
    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).unwrap();
    let text = String::from_utf8_lossy(&payload).into_owned();

    let msg: Value = serde_json::from_str(&text).unwrap();
    assert_eq!(msg["type"], "event");
    assert_eq!(msg["event"]["kind"], "acquire");
    assert_eq!(msg["event"]["lockId"], 8);
    assert_eq!(msg["event"]["tsMs"], serde_json::json!(1_700_000_000_140u64));

    server.shutdown();
    std::fs::remove_dir_all(&dir).unwrap();
}

/// The wire alphabet the bridge counts is total over the core's tags.
#[test]
fn wire_alphabet_is_total() {
    let kinds = [
        "Prepare",
        "PrepareOk",
        "Commit",
        "StartViewChange",
        "DoViewChange",
        "StartView",
        "PlannedViewChange",
        "GetState",
        "NewState",
        "Reincarnation",
    ];
    for kind in kinds {
        assert!(bridge::TAG_NAMES.contains(&kind));
    }
    let _ = SystemOperation::Join {
        node: NodeId(1),
        position: 0,
    };
}
