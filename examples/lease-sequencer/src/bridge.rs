//! The AOF console bridge: the UI server's ui-tape replayed through the
//! real `Service` state machine and served as the console's HTTP surface.
//! The replay reads read-only — it never writes, renames, or takes the
//! writer's lock; the UI server owns the ui-tape.

use lunet_advisory_lock::locks::{Response, Service, Transition};
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{Shutdown, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tbio::envelope::{Marker, Record};
use tbio::retention;
use uuid::Uuid;
use vrr::journal::Payload;
use vrr::message::{Body, Message};
use vrr::wire::{Tag, Unpack};

pub const TAG_NAMES: [&str; 14] = [
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
    "Fuse",
    "FuseOk",
    "CommitBatch",
    "GossipRequest",
];

fn tag_name(tag: Tag) -> &'static str {
    match tag {
        Tag::Prepare => "Prepare",
        Tag::PrepareOk => "PrepareOk",
        Tag::Commit => "Commit",
        Tag::StartViewChange => "StartViewChange",
        Tag::DoViewChange => "DoViewChange",
        Tag::StartView => "StartView",
        Tag::PlannedViewChange => "PlannedViewChange",
        Tag::GetState => "GetState",
        Tag::NewState => "NewState",
        Tag::Reincarnation => "Reincarnation",
        Tag::Fuse => "Fuse",
        Tag::FuseOk => "FuseOk",
        Tag::CommitBatch => "CommitBatch",
        Tag::GossipRequest => "GossipRequest",
    }
}

pub fn ns_to_ms(ns: u64) -> u64 {
    ns / 1_000_000
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockEvent {
    pub kind: String,
    pub ns: u64,
    pub ts_ms: u64,
    pub lock_id: u64,
    pub name: String,
    pub holder: String,
    pub actor: String,
    pub detail: String,
    pub seq: u64,
}

impl LockEvent {
    fn to_json(&self) -> Value {
        json!({
            "seq": self.seq,
            "tsMs": self.ts_ms,
            "kind": self.kind,
            "lockId": self.lock_id,
            "name": self.name,
            "actor": self.actor,
            "detail": self.detail,
            "holder": self.holder,
            "ns": self.ns,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockView {
    pub id: u64,
    pub name: Option<String>,
    pub labels: Vec<String>,
    pub state: String,
    pub holder: Option<String>,
    pub fencing_token: u64,
    pub lease_ms: u64,
    pub expires_at_ms: Option<u64>,
    pub taken_at_ms: Option<u64>,
    pub last_holder_change_ms: Option<u64>,
    pub renew_count: u32,
    pub holder_changes: u64,
}

impl LockView {
    fn to_json(&self, now_ms: u64) -> Value {
        let held = self.holder.is_some()
            && self.holder.as_deref() != Some("00000000-0000-0000-0000-000000000000")
            && self.expires_at_ms.is_some_and(|expiry| expiry > now_ms);
        json!({
            "id": self.id,
            "name": self.name,
            "labels": self.labels,
            "state": if held { "held" } else { "free" },
            "holder": if held { self.holder.clone() } else { None::<String> },
            "session": null,
            "fencingToken": self.fencing_token,
            "leaseMs": self.lease_ms,
            "expiresAtMs": if held { self.expires_at_ms } else { None::<u64> },
            "takenAtMs": if held { self.taken_at_ms } else { None::<u64> },
            "lastHolderChangeMs": self.last_holder_change_ms,
            "renewCount": self.renew_count,
            "holderChanges": self.holder_changes,
        })
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockState {
    pub locks: BTreeMap<u64, LockView>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BridgeMetrics {
    pub records: u64,
    pub undecodable: u64,
    pub markers: BTreeMap<String, u64>,
    pub messages: BTreeMap<String, u64>,
    pub lock_events: BTreeMap<String, u64>,
    pub first_ns: Option<u64>,
    pub last_ns: Option<u64>,
    pub max_era: Option<u32>,
    pub max_view: Option<u32>,
}

impl BridgeMetrics {
    fn marker_name(marker: Marker) -> &'static str {
        match marker {
            Marker::Wire => "wire",
            Marker::TelemetryTimeoutDecision => "timeout_decision",
            Marker::TelemetryStateTransition => "state_transition",
            Marker::TelemetryOutbound => "outbound",
        }
    }

    fn to_json(&self) -> Value {
        json!({
            "records": self.records,
            "undecodable": self.undecodable,
            "markers": self.markers,
            "messages": self.messages,
            "lockEvents": self.lock_events,
            "firstNs": self.first_ns,
            "lastNs": self.last_ns,
            "maxEra": self.max_era,
            "maxView": self.max_view,
        })
    }
}

#[derive(Debug, Clone, Default)]
pub struct Replay {
    pub events: Vec<LockEvent>,
    pub state: LockState,
    pub metrics: BridgeMetrics,
    pub log: TelemetryLog,
}

#[derive(Debug, Clone, Default)]
pub struct TelemetryLog {
    pub lines: Vec<Value>,
    pub unparsed: u64,
}

impl TelemetryLog {
    pub fn to_json(&self, from_ms: Option<u64>, to_ms: Option<u64>) -> Value {
        let lines: Vec<&Value> = self
            .lines
            .iter()
            .filter(|line| {
                let Some(ts) = line.get("ts").and_then(Value::as_u64) else {
                    return false;
                };
                from_ms.is_none_or(|from| ts >= from) && to_ms.is_none_or(|to| ts <= to)
            })
            .collect();
        json!({
            "lines": lines,
            "unparsed": self.unparsed,
            "span": {
                "first_ms": lines.first().and_then(|line| line["ts"].as_u64()),
                "last_ms": lines.last().and_then(|line| line["ts"].as_u64()),
            },
        })
    }
}

struct CommittedOp {
    ns: u64,
    message_id: Uuid,
    client_id: u64,
    request_num: u64,
    payload: Vec<u8>,
    kind: String,
    lock_id: u64,
    offered: Option<Uuid>,
    lease_ms: Option<u64>,
}

fn request_kind(request: &lunet_advisory_lock::locks::Request) -> &'static str {
    match request {
        lunet_advisory_lock::locks::Request::Get { .. } => "get",
        lunet_advisory_lock::locks::Request::Set { .. } => "set",
        lunet_advisory_lock::locks::Request::Release { .. } => "release",
        lunet_advisory_lock::locks::Request::Break { .. } => "break",
    }
}

fn request_lock_id(request: &lunet_advisory_lock::locks::Request) -> u64 {
    match request {
        lunet_advisory_lock::locks::Request::Get { lock_id, .. }
        | lunet_advisory_lock::locks::Request::Set { lock_id, .. }
        | lunet_advisory_lock::locks::Request::Release { lock_id, .. }
        | lunet_advisory_lock::locks::Request::Break { lock_id, .. } => *lock_id,
    }
}

fn set_holder(request: &lunet_advisory_lock::locks::Request) -> Option<Uuid> {
    match request {
        lunet_advisory_lock::locks::Request::Set { lease, .. } => Some(lease.holder),
        _ => None,
    }
}

fn set_lease_ms(request: &lunet_advisory_lock::locks::Request) -> Option<u64> {
    match request {
        lunet_advisory_lock::locks::Request::Set { lease, .. } => Some(lease.lease_ms),
        _ => None,
    }
}

pub fn replay_series(dir: &Path) -> (Vec<LockEvent>, LockState, BridgeMetrics) {
    let mut metrics = BridgeMetrics::default();
    let mut committed: Vec<CommittedOp> = Vec::new();

    for file in series_files(dir) {
        for record in read_file_records(&file, &mut metrics) {
            metrics.records += 1;
            let ns = record.ns;
            if metrics.first_ns.is_none() {
                metrics.first_ns = Some(ns);
            }
            metrics.last_ns = Some(ns);
            *metrics
                .markers
                .entry(BridgeMetrics::marker_name(record.marker).to_string())
                .or_insert(0) += 1;

            if record.marker != Marker::Wire {
                continue;
            }

            let message = match Message::unpack_from(&record.payload) {
                Ok(message) => message,
                Err(_) => {
                    metrics.undecodable += 1;
                    continue;
                }
            };
            *metrics
                .messages
                .entry(tag_name(message.header.tag).to_string())
                .or_insert(0) += 1;
            let ballot = &message.header.view;
            let (era, view) = (ballot.era.0, ballot.view.0);
            if metrics.max_era.is_none_or(|seen| era > seen) {
                metrics.max_era = Some(era);
            }
            if metrics.max_view.is_none_or(|seen| view > seen) {
                metrics.max_view = Some(view);
            }

            let Body::Prepare { entry, .. } = message.body else {
                continue;
            };
            let Payload::Operation {
                id: operation_id,
                payload,
            } = &entry.payload
            else {
                continue;
            };

            let Ok(request) = Service::decode(payload) else {
                continue;
            };
            let (message_id, client_id, request_num) = request.ids();
            if message_id.as_bytes() != &operation_id_bytes(*operation_id) {
                continue;
            }
            committed.push(CommittedOp {
                ns,
                message_id,
                client_id,
                request_num,
                payload: payload.to_vec(),
                kind: request_kind(&request).to_string(),
                lock_id: request_lock_id(&request),
                offered: set_holder(&request),
                lease_ms: set_lease_ms(&request),
            });
        }
    }

    committed.sort_by_key(|op| op.ns);

    let mut service = Service::default();
    let mut events: Vec<LockEvent> = Vec::new();
    let mut seq: u64 = 0;
    let mut leases: BTreeMap<u64, lunet_advisory_lock::locks::Lease> = BTreeMap::new();
    let mut names: BTreeMap<u64, String> = BTreeMap::new();
    let mut labels_seen: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    let mut holder_changes: BTreeMap<u64, u64> = BTreeMap::new();
    let mut lease_ms_seen: BTreeMap<u64, u64> = BTreeMap::new();
    let mut last_holder_change: BTreeMap<u64, u64> = BTreeMap::new();

    for op in committed {
        let now_ms = ns_to_ms(op.ns);
        let executed = service.execute(
            op.message_id,
            op.client_id,
            op.request_num,
            now_ms,
            &op.payload,
        );
        let Ok((bytes, transition)) = executed else {
            metrics.undecodable += 1;
            continue;
        };
        let reply: Option<Response> = serde_json::from_slice(&bytes).ok();

        match &reply {
            Some(Response::Set {
                granted: true,
                lease: Some(lease),
                ..
            })
            | Some(Response::Break {
                broken: true,
                lease: Some(lease),
                ..
            }) => {
                leases.insert(op.lock_id, lease.clone());
            }
            Some(Response::Release { released: true, .. }) => {
                leases.remove(&op.lock_id);
            }
            _ => {}
        }
        if let Some(lease) = reply_lease(&reply) {
            if let Some(name) = &lease.name {
                names.insert(op.lock_id, name.clone());
            }
            if let Some(labels) = &lease.labels {
                labels_seen.insert(op.lock_id, labels.clone());
            }
            if let Some(lease_ms) = op.lease_ms
                && lease_ms > 0
            {
                lease_ms_seen.insert(op.lock_id, lease_ms);
            }
        }

        match transition {
            Some(transition) => {
                if matches!(transition, Transition::Hold { .. }) {
                    *holder_changes.entry(op.lock_id).or_insert(0) += 1;
                    last_holder_change.insert(op.lock_id, now_ms);
                }
                let name = names
                    .get(&op.lock_id)
                    .cloned()
                    .unwrap_or_else(|| format!("/lock/{}", op.lock_id));
                seq += 1;
                let event = event_from_transition(&transition, op.ns, seq, &name);
                *metrics.lock_events.entry(event.kind.clone()).or_insert(0) += 1;
                events.push(event);
            }
            None => {
                if op.kind == "set" {
                    let held_by = reply_lease(&reply)
                        .map(|lease| lease.holder.to_string())
                        .unwrap_or_else(|| "—".to_string());
                    let name = names
                        .get(&op.lock_id)
                        .cloned()
                        .unwrap_or_else(|| format!("/lock/{}", op.lock_id));
                    let holder = op
                        .offered
                        .map(|holder| holder.to_string())
                        .unwrap_or_else(|| "cluster".to_string());
                    seq += 1;
                    let event = LockEvent {
                        kind: "deny".to_string(),
                        ns: op.ns,
                        ts_ms: ns_to_ms(op.ns),
                        lock_id: op.lock_id,
                        name,
                        holder: holder.clone(),
                        actor: holder,
                        detail: format!("held by {held_by}"),
                        seq,
                    };
                    *metrics.lock_events.entry("deny".to_string()).or_insert(0) += 1;
                    events.push(event);
                } else {
                    *metrics.lock_events.entry(op.kind.clone()).or_insert(0) += 1;
                }
            }
        }
    }

    let mut locks = BTreeMap::new();
    let mut lock_ids: Vec<u64> = leases.keys().copied().collect();
    lock_ids.extend(names.keys().copied());
    lock_ids.sort_unstable();
    lock_ids.dedup();
    for lock_id in lock_ids {
        let lease = leases.get(&lock_id);
        let name = names
            .get(&lock_id)
            .cloned()
            .unwrap_or_else(|| format!("/lock/{lock_id}"));
        let view = LockView {
            id: lock_id,
            name: Some(name),
            labels: labels_seen.get(&lock_id).cloned().unwrap_or_default(),
            state: "free".to_string(),
            holder: lease.map(|lease| lease.holder.to_string()),
            fencing_token: lease.map(|lease| lease.lease_id).unwrap_or(0),
            lease_ms: lease_ms_seen.get(&lock_id).copied().unwrap_or(0),
            expires_at_ms: lease.map(|lease| lease.expiry),
            taken_at_ms: lease
                .map(|lease| (lease.taken_at_ms > 0).then_some(lease.taken_at_ms))
                .unwrap_or(None),
            last_holder_change_ms: last_holder_change.get(&lock_id).copied(),
            renew_count: lease.map(|lease| lease.renew_count).unwrap_or(0),
            holder_changes: holder_changes.get(&lock_id).copied().unwrap_or(0),
        };
        locks.insert(lock_id, view);
    }

    let state = LockState { locks };
    (events, state, metrics)
}

fn reply_lease(reply: &Option<Response>) -> Option<lunet_advisory_lock::locks::Lease> {
    match reply {
        Some(Response::Set { lease, .. }) | Some(Response::Get { lease, .. }) => lease.clone(),
        Some(Response::Release { lease, .. }) | Some(Response::Break { lease, .. }) => {
            lease.clone()
        }
        None => None,
    }
}

fn event_from_transition(transition: &Transition, ns: u64, seq: u64, name: &str) -> LockEvent {
    let (kind, lock_id, lease_id, holder, expiry) = match transition {
        Transition::Hold {
            lock_id,
            lease_id,
            holder,
            expiry,
        } => ("acquire", *lock_id, *lease_id, *holder, *expiry),
        Transition::Renew {
            lock_id,
            lease_id,
            holder,
            expiry,
        } => ("renew", *lock_id, *lease_id, *holder, *expiry),
        Transition::Release {
            lock_id,
            lease_id,
            holder,
            expiry,
        } => ("release", *lock_id, *lease_id, *holder, *expiry),
        Transition::Break {
            lock_id,
            lease_id,
            holder,
            expiry,
        } => ("break", *lock_id, *lease_id, *holder, *expiry),
    };
    let kind = kind.to_string();
    let holder = Uuid::from_bytes(holder).to_string();
    let actor = if kind == "break" {
        "admin@console".to_string()
    } else {
        holder.clone()
    };
    let detail = match kind.as_str() {
        "acquire" => format!("lease {lease_id}, expiry {expiry}"),
        "renew" => format!("fence {lease_id}, expiry {expiry}"),
        "release" => format!("clean release, expiry was {expiry}"),
        "break" => format!("admin force-release, broken lease {lease_id}"),
        _ => String::new(),
    };
    LockEvent {
        kind,
        ns,
        ts_ms: ns_to_ms(ns),
        lock_id,
        name: name.to_string(),
        holder: holder.clone(),
        actor,
        detail,
        seq,
    }
}

fn series_files(dir: &Path) -> Vec<PathBuf> {
    retention::list_aof_files(dir)
        .unwrap_or_default()
        .into_iter()
        .map(|(path, _, _)| path)
        .collect()
}

fn read_telemetry_log(log_dir: Option<&Path>, aof_dir: &Path) -> TelemetryLog {
    let mut log = TelemetryLog::default();

    if let Some(dir) = log_dir {
        let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
            .map(|entries| {
                entries
                    .filter_map(|entry| entry.ok())
                    .map(|entry| entry.path())
                    .filter(|path| {
                        path.is_file()
                            && path.extension().is_some_and(|extension| extension == "log")
                    })
                    .collect()
            })
            .unwrap_or_default();
        files.sort();
        for path in files {
            let Ok(text) = std::fs::read_to_string(&path) else {
                log.unparsed += 1;
                continue;
            };
            for line in text.lines() {
                let trimmed = line.trim();
                if trimmed.is_empty() {
                    continue;
                }
                match serde_json::from_str::<Value>(trimmed) {
                    Ok(value)
                        if value.is_object()
                            && value.get("ts").is_some_and(|ts| ts.as_u64().is_some()) =>
                    {
                        log.lines.push(value);
                    }
                    _ => log.unparsed += 1,
                }
            }
        }
    }

    let mut scan = BridgeMetrics::default();
    for file in series_files(aof_dir) {
        for record in read_file_records(&file, &mut scan) {
            if record.marker != Marker::TelemetryStateTransition {
                continue;
            }
            let Ok(mut value) = serde_json::from_slice::<Value>(&record.payload) else {
                continue;
            };
            let Some(map) = value.as_object_mut() else {
                continue;
            };
            map.entry("ts".to_string())
                .or_insert_with(|| json!(ns_to_ms(record.ns)));
            if map.get("ts").is_some_and(|ts| ts.as_u64().is_some()) {
                log.lines.push(value);
            }
        }
    }

    log.lines
        .sort_by_key(|line| line.get("ts").and_then(Value::as_u64));
    log
}

fn read_file_records(path: &Path, metrics: &mut BridgeMetrics) -> Vec<Record> {
    let mut records = Vec::new();
    let mut offset = 0u64;
    let mut index = 0u64;
    // SAFETY: the FFI reads only through the returned iterator and needs a
    // byte-encoded path to an existing file; `path` is an `.aof` entry just
    // listed from the series directory.
    let Ok(mut iterator) =
        (unsafe { tbio::ffi::RawIter::open(path.as_os_str().as_encoded_bytes()) })
    else {
        metrics.undecodable += 1;
        return records;
    };
    loop {
        match iterator.next_entry() {
            Ok(Some(entry)) => {
                match Record::decode(&entry.bytes) {
                    Some(record) => records.push(record),
                    None => {
                        metrics.undecodable += 1;
                    }
                }
                offset += entry.bytes.len() as u64;
                index += 1;
            }
            Ok(None) => break,
            Err(code) => {
                eprintln!(
                    "aof-console-bridge: the ui-tape file {} is corrupt: record {index} at \
                     byte offset {offset} does not read (FFI code {code}); the let-it-crash \
                     law aborts the process",
                    path.display()
                );
                std::process::abort();
            }
        }
    }
    records
}

/// The cassette inventory: every ui-tape file on disk, named with its
/// size — the finalized `{epoch}.aof` cassettes and the active
/// `ev-open-*.bin` file the writer has not finalized yet. The bridge
/// logs it at boot, on SIGUSR1, and never silently skips it.
pub fn series_inventory(dir: &Path) -> String {
    let files = series_files(dir);
    let mut lines = vec![format!(
        "ui-tape: {} finalized cassette{} on disk in {}",
        files.len(),
        if files.len() == 1 { "" } else { "s" },
        dir.display()
    )];
    for (position, path) in files.iter().enumerate() {
        let size = std::fs::metadata(path).map_or(0, |meta| meta.len());
        lines.push(format!(
            "  {}. {} ({size} bytes, finalized)",
            position + 1,
            path.display()
        ));
    }
    let active: Vec<PathBuf> = std::fs::read_dir(dir)
        .map(|entries| {
            entries
                .filter_map(|entry| entry.ok())
                .map(|entry| entry.path())
                .filter(|path| {
                    path.is_file()
                        && path
                            .file_name()
                            .and_then(|name| name.to_str())
                            .is_some_and(|name| name.starts_with("ev-open-"))
                })
                .collect()
        })
        .unwrap_or_default();
    for path in &active {
        let size = std::fs::metadata(path).map_or(0, |meta| meta.len());
        lines.push(format!(
            "  active file {} ({size} bytes, not yet finalized)",
            path.display()
        ));
    }
    if files.is_empty() && active.is_empty() {
        lines.push("  no cassettes on disk".to_string());
    }
    lines.join("\n")
}

/// Print the cassette inventory to the bridge's stdout.
pub fn log_series_inventory(dir: &Path) {
    println!("{}", series_inventory(dir));
}

fn operation_id_bytes(id: vrr::ids::OperationId) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&id.msb.to_be_bytes());
    bytes[8..].copy_from_slice(&id.lsb.to_be_bytes());
    bytes
}

pub struct Server {
    listener: TcpListener,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Server {
    pub fn spawn(
        dir: &Path,
        log_dir: Option<&Path>,
        bind: &str,
        follow: bool,
    ) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind)?;
        let snapshot = Arc::new(Mutex::new(replay_snapshot(dir, log_dir)));
        let live: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let server = Self {
            listener,
            running: Arc::clone(&running),
        };

        let listener = server.listener.try_clone()?;
        let snapshot_loop = Arc::clone(&snapshot);
        let live_loop = Arc::clone(&live);
        let running_loop = Arc::clone(&running);
        std::thread::spawn(move || {
            for stream in listener.incoming() {
                if !running_loop.load(std::sync::atomic::Ordering::Relaxed) {
                    break;
                }
                let Ok(stream) = stream else { continue };
                let snapshot = Arc::clone(&snapshot_loop);
                let live = Arc::clone(&live_loop);
                std::thread::spawn(move || {
                    handle_connection(stream, snapshot, live);
                });
            }
        });

        if follow {
            let dir = dir.to_path_buf();
            let log_dir = log_dir.map(Path::to_path_buf);
            let snapshot_follow = Arc::clone(&snapshot);
            let live_follow = Arc::clone(&live);
            let running_follow = Arc::clone(&running);
            std::thread::spawn(move || {
                while running_follow.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(200));
                    let fresh = replay_snapshot(&dir, log_dir.as_deref());
                    let mut guard = snapshot_follow.lock().unwrap();
                    let new_events: Vec<LockEvent> = fresh
                        .events
                        .iter()
                        .skip(guard.events.len().min(fresh.events.len()))
                        .cloned()
                        .collect();
                    for event in &new_events {
                        let frame = ws_text_frame(
                            json!({"type": "event", "event": event.to_json()})
                                .to_string()
                                .as_bytes(),
                        );
                        let mut sockets = live_follow.lock().unwrap();
                        sockets.retain(|mut socket| socket.write_all(&frame).is_ok());
                    }
                    if fresh.events.len() >= guard.events.len() {
                        *guard = fresh;
                    }
                }
            });
        }

        Ok(server)
    }

    pub fn port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }

    pub fn shutdown(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

fn replay_snapshot(dir: &Path, log_dir: Option<&Path>) -> Replay {
    let (events, state, metrics) = replay_series(dir);
    let log = read_telemetry_log(log_dir, dir);
    Replay {
        events,
        state,
        metrics,
        log,
    }
}

fn handle_connection(
    mut stream: TcpStream,
    snapshot: Arc<Mutex<Replay>>,
    live: Arc<Mutex<Vec<TcpStream>>>,
) {
    const MAX_HEAD_BYTES: usize = 64 * 1024;
    let mut head: Vec<u8> = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        let n = stream.read(&mut chunk).unwrap_or(0);
        if n == 0 {
            break;
        }
        head.extend_from_slice(&chunk[..n]);
        if head.windows(4).any(|window| window == b"\r\n\r\n") || head.len() >= MAX_HEAD_BYTES {
            break;
        }
    }
    if head.is_empty() {
        return;
    }
    let request = String::from_utf8_lossy(&head).into_owned();
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default().to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let (path, query) = match path.split_once('?') {
        Some((path, query)) => (path.to_string(), Some(query.to_string())),
        None => (path, None),
    };

    if path == "/api/v1/live" && request.contains("Upgrade: websocket") {
        let Some(key) = request
            .lines()
            .find_map(|line| line.strip_prefix("Sec-WebSocket-Key: "))
            .map(|key| key.trim().to_string())
        else {
            return;
        };
        let accept = ws_accept_key(&key);
        let handshake = format!(
            "HTTP/1.1 101 Switching Protocols\r\n\
             Upgrade: websocket\r\n\
             Connection: Upgrade\r\n\
             Sec-WebSocket-Accept: {accept}\r\n\r\n"
        );
        if stream.write_all(handshake.as_bytes()).is_err() {
            return;
        }
        live.lock().unwrap().push(stream);
        return;
    }

    let (status, body) = route(&method, &path, query.as_deref(), &snapshot);
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
    let _ = stream.shutdown(Shutdown::Write);
}

fn route(
    method: &str,
    path: &str,
    query: Option<&str>,
    snapshot: &Arc<Mutex<Replay>>,
) -> (u16, String) {
    if method != "GET" {
        return (
            405,
            json!({"error": "GET only; the bridge is read-only"}).to_string(),
        );
    }
    let replay = snapshot.lock().unwrap();
    match path {
        "/api/v1/health" => (
            200,
            json!({
                "status": "ok",
                "nowMs": unix_millis(),
                "source": "aof-console-bridge",
            })
            .to_string(),
        ),
        "/api/v1/locks" => {
            let now_ms = unix_millis();
            let locks: Vec<Value> = replay
                .state
                .locks
                .values()
                .map(|lock| lock.to_json(now_ms))
                .collect();
            (200, json!({"nowMs": now_ms, "locks": locks}).to_string())
        }
        "/api/v1/events" => {
            let events: Vec<Value> = replay.events.iter().rev().map(LockEvent::to_json).collect();
            (200, json!({"events": events}).to_string())
        }
        "/api/v1/metrics" => (200, replay.metrics.to_json().to_string()),
        "/api/v1/cluster" => (
            200,
            json!({
                "era": replay.metrics.max_era,
                "view": replay.metrics.max_view,
                "leader": "—",
                "nowMs": unix_millis(),
                "nodes": [],
            })
            .to_string(),
        ),
        "/api/v1/metrics/series" => (200, series_json(&replay, unix_millis()).to_string()),
        "/api/v1/telemetry/log" => (
            200,
            replay
                .log
                .to_json(query_ms(query, "fromMs"), query_ms(query, "toMs"))
                .to_string(),
        ),
        path if path.starts_with("/api/v1/locks/") => {
            let id: u64 = path
                .trim_start_matches("/api/v1/locks/")
                .parse()
                .unwrap_or(u64::MAX);
            match replay.state.locks.get(&id) {
                Some(lock) => {
                    let recent: Vec<Value> = replay
                        .events
                        .iter()
                        .filter(|event| event.lock_id == id)
                        .rev()
                        .take(8)
                        .map(LockEvent::to_json)
                        .collect();
                    (
                        200,
                        json!({"lock": lock.to_json(unix_millis()), "recentEvents": recent})
                            .to_string(),
                    )
                }
                None => (404, json!({"error": "no such lock"}).to_string()),
            }
        }
        _ => (404, json!({"error": "not found"}).to_string()),
    }
}

fn unix_millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

fn query_ms(query: Option<&str>, key: &str) -> Option<u64> {
    query?.split('&').find_map(|pair| {
        let (name, value) = pair.split_once('=')?;
        (name == key).then(|| value.parse().ok()).flatten()
    })
}

fn series_json(replay: &Replay, now_ms: u64) -> Value {
    const DEFAULT_BUCKET_MS: u64 = 5000;
    let from = replay
        .events
        .first()
        .map(|event| event.ts_ms)
        .unwrap_or(now_ms.saturating_sub(3_600_000));
    let to = replay
        .events
        .last()
        .map(|event| event.ts_ms + DEFAULT_BUCKET_MS)
        .unwrap_or(now_ms);
    let bucket_ms = DEFAULT_BUCKET_MS;
    let first_bucket = from - (from % bucket_ms);
    let mut buckets: Vec<Value> = Vec::new();
    let mut ts = first_bucket;
    while ts < to {
        buckets.push(json!({
            "tsMs": ts, "held": 0, "acquire": 0, "renew": 0, "release": 0,
            "cas": 0, "expire": 0, "break": 0, "deny": 0,
        }));
        ts += bucket_ms;
    }
    let mut active: Vec<(u64, u64)> = Vec::new();
    for event in &replay.events {
        fn bucket(
            ts_ms: u64,
            first_bucket: u64,
            bucket_ms: u64,
            buckets: &mut [Value],
        ) -> Option<&mut Value> {
            let index = ((ts_ms.saturating_sub(first_bucket)) / bucket_ms) as usize;
            buckets.get_mut(index)
        }
        match event.kind.as_str() {
            "acquire" => {
                if let Some(b) = bucket(event.ts_ms, first_bucket, bucket_ms, &mut buckets) {
                    b["acquire"] = json!(b["acquire"].as_u64().unwrap_or(0) + 1);
                }
                if let Some(expiry) = event
                    .detail
                    .split("expiry ")
                    .nth(1)
                    .and_then(|e| e.parse::<u64>().ok())
                {
                    active.push((event.ts_ms, expiry));
                }
            }
            "renew" => {
                if let Some(b) = bucket(event.ts_ms, first_bucket, bucket_ms, &mut buckets) {
                    b["renew"] = json!(b["renew"].as_u64().unwrap_or(0) + 1);
                }
                if let Some(expiry) = event
                    .detail
                    .split("expiry ")
                    .nth(1)
                    .and_then(|e| e.parse::<u64>().ok())
                    && let Some(interval) = active.last_mut()
                {
                    interval.1 = expiry;
                }
            }
            "release" | "break" => {
                let key = if event.kind == "release" {
                    "release"
                } else {
                    "break"
                };
                if let Some(b) = bucket(event.ts_ms, first_bucket, bucket_ms, &mut buckets) {
                    b[key] = json!(b[key].as_u64().unwrap_or(0) + 1);
                }
                active.clear();
            }
            "deny" => {
                if let Some(b) = bucket(event.ts_ms, first_bucket, bucket_ms, &mut buckets) {
                    b["deny"] = json!(b["deny"].as_u64().unwrap_or(0) + 1);
                }
            }
            _ => {}
        }
        for (taken_at, expiry) in &active {
            let sample_ts = event.ts_ms;
            if *taken_at <= sample_ts
                && *expiry > sample_ts
                && let Some(b) = bucket(sample_ts, first_bucket, bucket_ms, &mut buckets)
            {
                b["held"] = json!(active.len());
            }
        }
    }
    json!({"bucketMs": bucket_ms, "buckets": buckets})
}

fn ws_accept_key(key: &str) -> String {
    sha1_base64(format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes())
}

fn ws_text_frame(payload: &[u8]) -> Vec<u8> {
    let mut frame = vec![0x81];
    if payload.len() < 126 {
        frame.push(payload.len() as u8);
    } else {
        frame.push(126);
        frame.extend_from_slice(&(payload.len() as u16).to_be_bytes());
    }
    frame.extend_from_slice(payload);
    frame
}

fn sha1_base64(data: &[u8]) -> String {
    let mut h: [u32; 5] = [0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0];
    let mut message = data.to_vec();
    let bit_len = (data.len() as u64) * 8;
    message.push(0x80);
    while message.len() % 64 != 56 {
        message.push(0);
    }
    message.extend_from_slice(&bit_len.to_be_bytes());

    for chunk in message.chunks(64) {
        let mut w = [0u32; 80];
        for (i, word) in chunk.chunks(4).enumerate() {
            w[i] = u32::from_be_bytes(word.try_into().unwrap());
        }
        for i in 16..80 {
            w[i] = (w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16]).rotate_left(1);
        }
        let (mut a, mut b, mut c, mut d, mut e) = (h[0], h[1], h[2], h[3], h[4]);
        for (i, &word) in w.iter().enumerate() {
            let (f, k) = match i {
                0..=19 => ((b & c) | ((!b) & d), 0x5A827999u32),
                20..=39 => (b ^ c ^ d, 0x6ED9EBA1),
                40..=59 => ((b & c) | (b & d) | (c & d), 0x8F1BBCDC),
                _ => (b ^ c ^ d, 0xCA62C1D6),
            };
            let temp = a
                .rotate_left(5)
                .wrapping_add(f)
                .wrapping_add(e)
                .wrapping_add(k)
                .wrapping_add(word);
            e = d;
            d = c;
            c = b.rotate_left(30);
            b = a;
            a = temp;
        }
        h[0] = h[0].wrapping_add(a);
        h[1] = h[1].wrapping_add(b);
        h[2] = h[2].wrapping_add(c);
        h[3] = h[3].wrapping_add(d);
        h[4] = h[4].wrapping_add(e);
    }

    let digest: Vec<u8> = h.iter().flat_map(|word| word.to_be_bytes()).collect();
    base64(&digest)
}

fn base64(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [
            chunk[0],
            chunk.get(1).copied().unwrap_or(0),
            chunk.get(2).copied().unwrap_or(0),
        ];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | (b[2] as u32);
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            TABLE[(n >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            TABLE[n as usize & 63] as char
        } else {
            '='
        });
    }
    out
}
