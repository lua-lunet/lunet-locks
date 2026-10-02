//! The console telemetry panel's data source: the observability
//! contract's JSON log series (`docs/src/test-scaffold.md`) — the daily
//! rolling `<node>.<date>.log` appender files plus the telemetry tape's
//! slot-frontier records — served at `/api/v1/telemetry/log`. The
//! endpoint's life sign is a NON-EMPTY series with named events: a
//! permanently-empty panel is the failure this suite exists to prevent.

use lease_sequencer::bridge::Server;
use lunet_locks_aof::AofFile;
use lunet_locks_aof::envelope::{Marker, Record};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/telemetry-log");
    std::fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("the case directory creates");
    dir
}

/// One GET round against the bridge: `(status, body)`.
fn http_get(port: u16, path: &str) -> (u16, String) {
    let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("the bridge accepts");
    stream
        .write_all(
            format!("GET {path} HTTP/1.1\r\nhost: 127.0.0.1\r\nconnection: close\r\n\r\n")
                .as_bytes(),
        )
        .expect("the request writes");
    let mut text = String::new();
    stream.read_to_string(&mut text).expect("the reply reads");
    let mut parts = text.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or_default();
    let body = parts.next().unwrap_or_default().to_string();
    let status = head
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or_default();
    (status, body)
}

/// The fixture: one AOF series whose telemetry tape carries the
/// slot-frontier record (and one timeout-decision record the endpoint
/// does not serve), and a logs directory with two nodes' rolling logs in
/// the real line shapes — including one non-JSON neighbor line (the Lua
/// host's stdout captured beside the node's own log).
fn write_fixture(aof_dir: &Path, log_dir: &Path) {
    std::fs::create_dir_all(aof_dir).expect("the aof fixture dir creates");
    std::fs::create_dir_all(log_dir).expect("the logs fixture dir creates");

    let frontier_json =
        br#"{"event":"slot-frontier","era":4,"view":41,"leader":102,"state":"Normal","slot":5204}"#;
    let decision_json = br#"{"now_ms":1789725518100,"prev_wait_ms":900,"next_wait_ms":1000}"#;
    {
        let mut aof = AofFile::open(aof_dir).expect("the fixture aof opens");
        aof.append(
            &Record::telemetry(
                Marker::TelemetryStateTransition,
                1_789_725_518_123_456_789,
                frontier_json,
            )
            .encode(),
        )
        .expect("the frontier record appends");
        aof.append(
            &Record::telemetry(
                Marker::TelemetryTimeoutDecision,
                1_789_725_518_200_000_000,
                decision_json,
            )
            .encode(),
        )
        .expect("the decision record appends");
    }

    // n1 (follower 101): the heartbeat arrivals it observed, a grant, the
    // detection that fired on the stalled leader, and the first arrival
    // from the successor.
    std::fs::write(
        log_dir.join("n1.2026-10-01.log"),
        concat!(
            r#"{"ts":1789725518000,"level":"INFO","event":"commit-in","node":101,"from":102,"era":4,"view":41,"slot":5201,"leader":102,"state":"Normal","message":"commit-in"}"#, "\n",
            r#"{"ts":1789725518060,"level":"INFO","event":"commit-in","node":101,"from":102,"era":4,"view":41,"slot":5202,"leader":102,"state":"Normal","message":"commit-in"}"#, "\n",
            r#"{"ts":1789725518123,"level":"INFO","event":"lease-grant","node":101,"era":4,"view":41,"leader":102,"state":"Normal","op":"renew","expiry":1789725518623,"message":"lease-grant"}"#, "\n",
            r#"{"ts":1789725518200,"level":"INFO","event":"leader-timeout-detect","node":101,"era":4,"view":41,"config_era":4,"leader":102,"state":"Normal","silence_ms":720,"deadline_ms":1789725518150,"addr":"127.0.0.1:7002","message":"leader-timeout-detect"}"#, "\n",
            r#"{"ts":1789725518400,"level":"INFO","event":"commit-in","node":101,"from":103,"era":5,"view":42,"slot":5301,"leader":103,"state":"Normal","message":"commit-in"}"#, "\n",
            "advisory-lock membership fingerprint=84f2957f69449a2b encoding=lunet-advisory-lock/membership/v1\n",
        ),
    )
    .expect("n1's log writes");

    // n2 (leader 102): the heartbeat drive it logged, and n3 (follower
    // 103): the election wait that fired and the nomination it sent.
    std::fs::write(
        log_dir.join("n2.2026-10-01.log"),
        concat!(
            r#"{"ts":1789725518010,"level":"INFO","event":"heartbeat-commit","node":102,"era":4,"view":41,"leader":102,"state":"Normal","slot":5201,"request_num":61,"message":"heartbeat-commit"}"#, "\n",
            r#"{"ts":1789725518070,"level":"INFO","event":"heartbeat-commit","node":102,"era":4,"view":41,"leader":102,"state":"Normal","slot":5202,"request_num":62,"message":"heartbeat-commit"}"#, "\n",
        ),
    )
    .expect("n2's log writes");
    std::fs::write(
        log_dir.join("n3.2026-10-01.log"),
        concat!(
            r#"{"ts":1789725518190,"level":"INFO","event":"election-wait-fire","node":103,"era":4,"view":41,"leader":102,"state":"Normal","wait_ms":820,"stagger_ms":15,"fired_ms":835,"message":"election-wait-fire"}"#, "\n",
            r#"{"ts":1789725518195,"level":"INFO","event":"nominate-out","node":103,"to":102,"era":4,"view":42,"slot":5290,"tag":"StartViewChange","bytes":31}"#, "\n",
        ),
    )
    .expect("n3's log writes");
}

/// The endpoint serves a non-empty series with the named events, every
/// line carrying `ts` and `event`, every protocol line carrying both
/// `era` and `view`, the tape's slot-frontier record folded in with its
/// envelope ns floored to ms, the non-JSON neighbor counted not served,
/// and the fromMs window trimming the series.
#[test]
fn telemetry_log_endpoint_serves_a_nonempty_series_with_named_events() {
    let case = scratch("panel-source");
    let aof_dir = case.join("aof");
    let log_dir = case.join("logs");
    write_fixture(&aof_dir, &log_dir);

    let server =
        Server::spawn(&aof_dir, Some(&log_dir), "127.0.0.1:0", false).expect("the bridge spawns");
    let (status, body) = http_get(server.port(), "/api/v1/telemetry/log");

    assert_eq!(status, 200, "the endpoint answers; body: {body}");
    let value: Value = serde_json::from_str(&body).expect("the body is JSON");

    let lines = value["lines"]
        .as_array()
        .expect("the body carries a lines array");
    assert!(
        !lines.is_empty(),
        "a non-empty series: a permanently-empty panel is the failure this row exists to prevent"
    );

    let mut events: Vec<&str> = Vec::new();
    for line in lines {
        let event = line["event"].as_str().unwrap_or_default();
        assert!(!event.is_empty(), "every line names its event: {line}");
        assert!(
            line["ts"].as_u64().is_some(),
            "every line carries ts: {line}"
        );
        let (era, view) = (line.get("era"), line.get("view"));
        assert_eq!(
            era.is_some(),
            view.is_some(),
            "every protocol line carries both era and view: {line}"
        );
        events.push(event);
    }
    for expected in [
        "heartbeat-commit",
        "commit-in",
        "leader-timeout-detect",
        "election-wait-fire",
        "nominate-out",
        "lease-grant",
        "slot-frontier",
    ] {
        assert!(
            events.contains(&expected),
            "the series carries the named event {expected}; carried: {events:?}"
        );
    }

    // The tape record rides the series with its envelope ns floored to
    // ms; the timeout-decision record does not ride it.
    let frontier = lines
        .iter()
        .find(|line| line["event"] == "slot-frontier")
        .expect("the slot-frontier record is folded in");
    assert_eq!(
        frontier["ts"], 1_789_725_518_123u64,
        "the envelope ns floors to ms"
    );
    assert_eq!(frontier["slot"], 5204);

    // The Lua host's stdout line (no JSON object) is counted, not served.
    assert_eq!(value["unparsed"], 1, "the non-JSON neighbor is counted");

    // The span is the served series' own bounds.
    let first = lines.first().unwrap()["ts"].as_u64().unwrap();
    let last = lines.last().unwrap()["ts"].as_u64().unwrap();
    assert_eq!(value["span"]["first_ms"], first);
    assert_eq!(value["span"]["last_ms"], last);

    // The fromMs window trims: everything served is at or past it, and
    // the trimmed series is strictly smaller.
    let from_ms = first + 100;
    let (status, body) = http_get(
        server.port(),
        &format!("/api/v1/telemetry/log?fromMs={from_ms}"),
    );
    assert_eq!(status, 200, "the windowed endpoint answers; body: {body}");
    let windowed: Value = serde_json::from_str(&body).expect("the windowed body is JSON");
    let windowed_lines = windowed["lines"].as_array().expect("the windowed lines");
    assert!(
        windowed_lines.len() < lines.len(),
        "the window trims the series"
    );
    for line in windowed_lines {
        assert!(
            line["ts"].as_u64().unwrap() >= from_ms,
            "every windowed line is at or past fromMs: {line}"
        );
    }
    server.shutdown();
}
