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
use vrr::ids::{Ballot, Era, NodeId, OperationId, Slot, View};
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
            view: Ballot {
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
            view: Ballot {
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

fn request_json(
    op: &str,
    message_id: &Uuid,
    client_id: u64,
    request_num: u64,
    body: &str,
) -> String {
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
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":11,\"holder\":\"{holder_a}\",\"lease_ms\":10000}},\"name\":\"/cluster/leader\",\"labels\":[\"smr\"]"
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
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":12,\"holder\":\"{holder_a}\",\"lease_ms\":10000}},\"name\":\"/cluster/leader\""
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
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":13,\"holder\":\"{holder_b}\",\"lease_ms\":10000}},\"name\":\"/cluster/leader\""
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
    write_wire(
        &mut aof,
        t0 + 4_000_000,
        &prepare_wire(release_id, &release, 4),
    );

    // 6. acquire again (lock free after release → Hold) so break has a record.
    let reacquire_id = Uuid::new_v4();
    let reacquire = request_json(
        "set",
        &reacquire_id,
        1,
        4,
        &format!(
            ",\"lock_id\":7,\"lease\":{{\"lease_id\":14,\"holder\":\"{holder_a}\",\"lease_ms\":10000}},\"name\":\"/cluster/leader\""
        ),
    );
    write_wire(
        &mut aof,
        t0 + 5_000_000,
        &prepare_wire(reacquire_id, &reacquire, 5),
    );

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
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// /locks state is the replayed Service state: the broken lock is free with
/// the bumped keeper lease_id, the name/labels are sticky.
#[test]
fn replayed_lock_state_matches() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// An unknown envelope marker is rejected — never guessed. The record is
/// counted undecodable and produces no event.
#[test]
fn unknown_markers_are_rejected() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// A Wire payload that is not a uVRR message (garbage) is counted as an
/// undecodable wire message, never an event.
#[test]
fn garbage_wire_payload_is_counted_not_decoded() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The series reader is read-only: replaying twice yields identical state
/// and never mutates the series (same file set, same sizes).
#[test]
fn replay_is_read_only_and_deterministic() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
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
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The same request written as three separate segments — exactly what
/// `write!` with an interpolation emits (one write per format piece) —
/// yielding between writes so the server observes each segment arrive
/// on its own. The response must come back through a clean close.
fn http_get_segmented(port: u16, path: &str) -> std::io::Result<(u16, String)> {
    let mut stream = TcpStream::connect(("127.0.0.1", port))?;
    stream.write_all(b"GET ")?;
    std::thread::yield_now();
    stream.write_all(path.as_bytes())?;
    std::thread::yield_now();
    stream.write_all(b" HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")?;
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf)?;
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
    Ok((status, body))
}

/// Segmented requests under parallel load: several servers, several
/// client threads, every request written one segment at a time. The
/// server drains the full request head before answering, so every
/// request must complete without a connection reset and parse to its
/// intended route — a reset or a mis-parsed path (the empty path of a
/// half-read request line) is a failure. Bounded: a few hundred
/// loopback connections.
#[test]
fn segmented_requests_under_parallel_load_never_reset() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The live WebSocket push: with follow on, an appended record arrives as a
/// text frame on /api/v1/live.
#[test]
fn follow_pushes_new_events_over_websocket() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The wire alphabet the bridge counts is total over the core's tags.
#[test]
fn wire_alphabet_is_total() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The bulk telemetry endpoint: the phi samples (marker 5) and the timeout
/// decisions (marker 2) as arrays, plus the span — the ECharts tab's data
/// source.
#[test]
fn telemetry_phi_endpoint_serves_samples_and_decisions() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
