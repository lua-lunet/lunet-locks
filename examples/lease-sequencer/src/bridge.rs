//! The AOF console bridge (item23): the vanilla JS console reads the trace.
//!
//! One binary surface, read-only on the standby's AOF series:
//!
//! 1. Read every `.aof` file of the series (epoch order) through the
//!    vendored checksum-validating iterator and decode each entry through
//!    the typed envelope layer (`Record::decode`).
//! 2. For Wire (marker 1) payloads — the raw uVRR wire messages,
//!    byte-identical to what the network carried — decode through the
//!    core's `Message::unpack_from` (the parser `Node::receive` itself
//!    runs). A Prepare whose entry payload decodes through
//!    `lunet_advisory_lock::locks::Service` is committed lock work; the
//!    payload replays through the real `Service::execute` state machine
//!    (the exact path the adapter's `Effect::Apply` arm runs at commit),
//!    yielding the same `Transition` the standby's AOF sink journaled:
//!    acquire / renew / release / break — plus the granted=false deny a
//!    losing competitor committed.
//! 3. Serve the console's OpenAPI shapes over loopback HTTP:
//!    `/api/v1/health`, `/locks` (state replayed from the committed
//!    stream), `/events` (the append-only log derived from the committed
//!    work), `/metrics` (every message kind counted), and a live
//!    WebSocket push at `/api/v1/live` while `--follow` re-scans the
//!    series as it grows.
//!
//! # Time at the boundary
//!
//! The envelope header carries the writing node's nanosecond clock. The
//! console speaks epoch milliseconds. The conversion is one-way integer
//! floor division `ms = ns / 1_000_000` at the boundary (the same
//! truncation direction the journal's ms stamps take); the bridge never
//! widens a millisecond back to nanoseconds, and `/metrics` reports the
//! raw `firstNs`/`lastNs` stamps so a post-mortem keeps full precision.
//!
//! # Read-only
//!
//! The bridge opens every file through the Zig iterator's read-only open;
//! it never takes the writer's exclusive lock and never writes, renames,
//! or deletes — the standby owns the series.

use lunet_advisory_lock::locks::{Response, Service, Transition};
use lunet_locks_aof::envelope::{Marker, Record};
use lunet_locks_aof::retention;
use serde_json::{Value, json};
use std::collections::BTreeMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;
use vrr::journal::Payload;
use vrr::message::{Body, Message};
use vrr::wire::{Tag, Unpack};

/// The wire alphabet's kind names, in tag order — the /metrics message
/// table's keys. Total over the core's tags: a message kind is counted,
/// never dropped.
pub const TAG_NAMES: [&str; 10] = [
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
    }
}

/// The ns → ms boundary conversion: one-way integer floor division. The
/// console speaks epoch milliseconds; full precision stays in the envelope
/// header and is reported raw in /metrics.
pub fn ns_to_ms(ns: u64) -> u64 {
    ns / 1_000_000
}

/// One derived lock event — the openapi `Event` shape plus the raw ns
/// stamp.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockEvent {
    /// The console's event kind: acquire | renew | release | break | deny
    /// (expire is reserved: expiry is not committed work).
    pub kind: String,
    /// The record's envelope ns clock (full precision, the source of truth).
    pub ns: u64,
    /// `ns` floored to epoch milliseconds — what the console renders.
    pub ts_ms: u64,
    pub lock_id: u64,
    /// The lock's display name (sticky, from the replayed state).
    pub name: String,
    /// The acting holder, as a uuid string; the console's holder.
    pub holder: String,
    /// The console's actor field.
    pub actor: String,
    /// The human detail line.
    pub detail: String,
    /// The append-only sequence, 1-based over the derived event log.
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

/// One replayed lock — the console's openapi `Lock` shape fields.
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
        // The live-lease rule is the Service's own: expiry > now and a
        // real holder. The keeper record a break installs (nil holder,
        // expiry 0) renders free.
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

/// The replayed lock table, keyed by lock id.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LockState {
    pub locks: BTreeMap<u64, LockView>,
}

/// Per-series counters: every record and message kind counted.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BridgeMetrics {
    /// Envelope records decoded (unknown-marker entries excluded).
    pub records: u64,
    /// Entries the readers refused (unknown marker, bad wire payload) —
    /// stated, never guessed.
    pub undecodable: u64,
    /// Per-marker record counts (wire / timeout_decision /
    /// state_transition / outbound).
    pub markers: BTreeMap<String, u64>,
    /// Per-wire-message-kind counts (every tag, not just lock work).
    pub messages: BTreeMap<String, u64>,
    /// Per-lock-event-kind counts (acquire/renew/release/break/deny/get).
    pub lock_events: BTreeMap<String, u64>,
    /// First record's ns stamp (full precision).
    pub first_ns: Option<u64>,
    /// Last record's ns stamp (full precision).
    pub last_ns: Option<u64>,
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
        })
    }
}

/// The replay snapshot: the derived event log (ns order), the lock state,
/// and the counters.
#[derive(Debug, Clone, Default)]
pub struct Replay {
    pub events: Vec<LockEvent>,
    pub state: LockState,
    pub metrics: BridgeMetrics,
}

/// One decoded committed op: everything the replay needs.
struct CommittedOp {
    ns: u64,
    message_id: Uuid,
    client_id: u64,
    request_num: u64,
    payload: Vec<u8>,
    kind: String,
    lock_id: u64,
    /// The Set's offered holder, when the op is a Set.
    offered: Option<Uuid>,
    /// The Set's offered expiry, when the op is a Set.
    lease_expiry: Option<u64>,
}

/// The request's wire kind name, from its decoded envelope.
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

/// The Set request's holder uuid, when the payload is a Set.
fn set_holder(request: &lunet_advisory_lock::locks::Request) -> Option<Uuid> {
    match request {
        lunet_advisory_lock::locks::Request::Set { lease, .. } => Some(lease.holder),
        _ => None,
    }
}

/// The Set request's offered expiry, when present.
fn set_expiry(request: &lunet_advisory_lock::locks::Request) -> Option<u64> {
    match request {
        lunet_advisory_lock::locks::Request::Set { lease, .. } => Some(lease.expiry),
        _ => None,
    }
}

/// Replay one series directory: every `.aof` file in epoch order, every
/// record decoded, every committed lock op executed through the real
/// Service state machine. Read-only on disk; deterministic across runs.
pub fn replay_series(dir: &Path) -> (Vec<LockEvent>, LockState, BridgeMetrics) {
    let mut metrics = BridgeMetrics::default();
    let mut committed: Vec<CommittedOp> = Vec::new();

    // Pass 1: decode every record; classify Wire payloads and collect the
    // committed lock work (Prepare entries whose payload passes the same
    // `Service::decode` gate the adapter applies at commit).
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

            // The wire message: the core's own parser, the one
            // `Node::receive` runs on every datagram.
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

            // The committed-payload gate: the same `Service::decode` the
            // adapter's `valid_message_payloads` applies inbound and its
            // `Effect::Apply` arm applies at commit.
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
                lease_expiry: set_expiry(&request),
            });
        }
    }

    // Pass 2: replay the committed stream in slot order (record order) —
    // slot order IS record order within the standby's trace, since the
    // standby appends each datagram at its local ns receipt and commits
    // follow the Prepare stream it records.
    committed.sort_by_key(|op| op.ns);

    let mut service = Service::default();
    let mut events: Vec<LockEvent> = Vec::new();
    let mut seq: u64 = 0;
    // The replay mirror: the executed replies' own shapes — the Service's
    // stored lease rides every Set/Get/Break reply, and a released lock's
    // absence IS the free state. Names/labels stick from the stored lease.
    let mut current: BTreeMap<u64, lunet_advisory_lock::locks::Lease> = BTreeMap::new();
    let mut names: BTreeMap<u64, String> = BTreeMap::new();
    let mut labels_seen: BTreeMap<u64, Vec<String>> = BTreeMap::new();
    let mut holder_changes: BTreeMap<u64, u64> = BTreeMap::new();
    let mut lease_ms_seen: BTreeMap<u64, u64> = BTreeMap::new();
    let mut last_holder_change: BTreeMap<u64, u64> = BTreeMap::new();

    for op in committed {
        let now_ms = ns_to_ms(op.ns);
        // The executed reply: the same serde shapes the state machine
        // returns at commit — the reply IS the observation surface.
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

        // Fold the reply into the state mirror: the stored lease rides
        // every Set/Get/Break reply; a released lock's absence IS the
        // free state. Names/labels stick from the stored lease.
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
                current.insert(op.lock_id, lease.clone());
            }
            Some(Response::Release {
                released: true, ..
            }) => {
                current.remove(&op.lock_id);
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
            if let Some(expiry) = op.lease_expiry {
                let offered = expiry.saturating_sub(now_ms);
                if offered > 0 {
                    lease_ms_seen.insert(op.lock_id, offered);
                }
            }
        }

        match transition {
            Some(transition) => {
                // Holder-changing Hold transitions bump the counter; a
                // Renew never does.
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
                *metrics
                    .lock_events
                    .entry(event.kind.clone())
                    .or_insert(0) += 1;
                events.push(event);
            }
            None => {
                if op.kind == "set" {
                    // A Set that decoded and executed but granted nothing:
                    // a denied acquire is committed lock work too.
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
                    // Get (and any other non-transitioning op): counted,
                    // never an event.
                    *metrics.lock_events.entry(op.kind.clone()).or_insert(0) += 1;
                }
            }
        }
    }

    // The replayed state: derived from the transitions and replies above —
    // the final Service table is private, so the bridge folds the observed
    // replies into its own mirror (the deterministic replay makes this
    // equivalent: same inputs, same state machine, same outcomes).
    let mut locks = BTreeMap::new();
    let mut lock_ids: Vec<u64> = current.keys().copied().collect();
    lock_ids.extend(names.keys().copied());
    lock_ids.sort_unstable();
    lock_ids.dedup();
    for lock_id in lock_ids {
        let lease = current.get(&lock_id);
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

/// The reply's stored lease, from the executed response's serde shape.
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

/// The `.aof` series files of one directory, oldest first. Read-only.
fn series_files(dir: &Path) -> Vec<PathBuf> {
    retention::list_aof_files(dir)
        .unwrap_or_default()
        .into_iter()
        .map(|(path, _, _)| path)
        .collect()
}

/// Every envelope record of one AOF file through the checksum-validating
/// iterator and the typed envelope decoder. Unknown markers and torn
/// entries are counted undecodable — stated, never guessed.
fn read_file_records(path: &Path, metrics: &mut BridgeMetrics) -> Vec<Record> {
    let mut records = Vec::new();
    let Ok(mut iterator) =
        (unsafe { lunet_locks_aof::ffi::RawIter::open(path.as_os_str().as_encoded_bytes()) })
    else {
        metrics.undecodable += 1;
        return records;
    };
    loop {
        match iterator.next_entry() {
            Ok(Some(entry)) => match Record::decode(&entry.bytes) {
                Some(record) => records.push(record),
                None => {
                    metrics.undecodable += 1;
                }
            },
            Ok(None) => break,
            Err(_) => {
                metrics.undecodable += 1;
                break;
            }
        }
    }
    records
}

fn operation_id_bytes(id: vrr::ids::OperationId) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&id.msb.to_be_bytes());
    bytes[8..].copy_from_slice(&id.lsb.to_be_bytes());
    bytes
}

// ---------------------------------------------------------------------------
// The HTTP + WebSocket server
// ---------------------------------------------------------------------------

/// The bridge server: one thread per connection, a shared replay snapshot
/// refreshed on demand (and re-scanned by the follow thread when `follow`
/// is on).
pub struct Server {
    listener: TcpListener,
    running: Arc<std::sync::atomic::AtomicBool>,
}

impl Server {
    /// Spawn the server bound to `bind` (host:port; port 0 picks a free
    /// ephemeral port). With `follow`, a background thread re-scans the
    /// series and pushes new events to the live sockets.
    pub fn spawn(dir: &Path, bind: &str, follow: bool) -> std::io::Result<Self> {
        let listener = TcpListener::bind(bind)?;
        let snapshot = Arc::new(Mutex::new(replay_snapshot(dir)));
        let live: Arc<Mutex<Vec<TcpStream>>> = Arc::new(Mutex::new(Vec::new()));
        let running = Arc::new(std::sync::atomic::AtomicBool::new(true));

        let server = Self {
            listener,
            running: Arc::clone(&running),
        };

        // The accept loop. Parked threads die with the process (the
        // binary's main returns); tests only need the sockets drained.
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
            // The follow thread: re-scan the series; new events push to
            // every live socket.
            let dir = dir.to_path_buf();
            let snapshot_follow = Arc::clone(&snapshot);
            let live_follow = Arc::clone(&live);
            let running_follow = Arc::clone(&running);
            std::thread::spawn(move || {
                while running_follow.load(std::sync::atomic::Ordering::Relaxed) {
                    std::thread::sleep(Duration::from_millis(200));
                    let fresh = replay_snapshot(&dir);
                    let mut guard = snapshot_follow.lock().unwrap();
                    // New events: everything past the previous snapshot's
                    // length (the replay is deterministic and append-only,
                    // so a longer event list means new committed work).
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

    /// The bound port (for tests and operators).
    pub fn port(&self) -> u16 {
        self.listener.local_addr().unwrap().port()
    }

    /// Stop the follow thread and accept loop (best-effort; parked
    /// threads exit with the process).
    pub fn shutdown(&self) {
        self.running
            .store(false, std::sync::atomic::Ordering::Relaxed);
    }
}

/// A fresh full replay of one series directory.
fn replay_snapshot(dir: &Path) -> Replay {
    let (events, state, metrics) = replay_series(dir);
    Replay {
        events,
        state,
        metrics,
    }
}

/// One connection's HTTP lifecycle. WebSocket upgrades park the socket in
/// the live set; everything else answers from the snapshot and closes.
fn handle_connection(mut stream: TcpStream, snapshot: Arc<Mutex<Replay>>, live: Arc<Mutex<Vec<TcpStream>>>) {
    let mut buf = [0u8; 8192];
    let n = stream.read(&mut buf).unwrap_or(0);
    if n == 0 {
        return;
    }
    let request = String::from_utf8_lossy(&buf[..n]).into_owned();
    let mut lines = request.lines();
    let request_line = lines.next().unwrap_or_default().to_string();
    let mut parts = request_line.split_whitespace();
    let method = parts.next().unwrap_or_default().to_string();
    let path = parts.next().unwrap_or_default().to_string();
    let path = path.split('?').next().unwrap_or(&path).to_string();

    // WebSocket upgrade on /api/v1/live.
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

    let (status, body) = route(&method, &path, &snapshot);
    let response = format!(
        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    let _ = stream.write_all(response.as_bytes());
    let _ = stream.flush();
}

/// The route table: the console's OpenAPI surface, read-only.
fn route(method: &str, path: &str, snapshot: &Arc<Mutex<Replay>>) -> (u16, String) {
    if method != "GET" {
        return (405, json!({"error": "GET only; the bridge is read-only"}).to_string());
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
            let events: Vec<Value> = replay
                .events
                .iter()
                .rev()
                .map(LockEvent::to_json)
                .collect();
            (200, json!({"events": events}).to_string())
        }
        "/api/v1/metrics" => (200, replay.metrics.to_json().to_string()),
        "/api/v1/metrics/series" => (200, series_json(&replay, unix_millis()).to_string()),
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

/// The openapi `/metrics/series` shape, derived from the event log: the
/// per-kind counts per bucket, and the held gauge replayed from the lease
/// intervals the events carry (an acquire opens one, a renew extends it, a
/// release/break closes it, expiry closes it at its stamp).
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
    // The held gauge: replay the lease intervals in ts order.
    let mut active: Vec<(u64, u64)> = Vec::new(); // (holder taken_at, expiry)
    for event in &replay.events {
        fn bucket<'a>(
            ts_ms: u64,
            first_bucket: u64,
            bucket_ms: u64,
            buckets: &'a mut [Value],
        ) -> Option<&'a mut Value> {
            let index = ((ts_ms.saturating_sub(first_bucket)) / bucket_ms) as usize;
            buckets.get_mut(index)
        }
        match event.kind.as_str() {
            "acquire" => {
                if let Some(b) = bucket(event.ts_ms, first_bucket, bucket_ms, &mut buckets) {
                    b["acquire"] = json!(b["acquire"].as_u64().unwrap_or(0) + 1);
                }
                if let Some(expiry) = event.detail.split("expiry ").nth(1).and_then(|e| e.parse::<u64>().ok()) {
                    active.push((event.ts_ms, expiry));
                }
            }
            "renew" => {
                if let Some(b) = bucket(event.ts_ms, first_bucket, bucket_ms, &mut buckets) {
                    b["renew"] = json!(b["renew"].as_u64().unwrap_or(0) + 1);
                }
                if let Some(expiry) = event.detail.split("expiry ").nth(1).and_then(|e| e.parse::<u64>().ok()) {
                    if let Some(interval) = active.last_mut() {
                        interval.1 = expiry;
                    }
                }
            }
            "release" | "break" => {
                let key = if event.kind == "release" { "release" } else { "break" };
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
        // Held samples: last write wins per bucket.
        for (taken_at, expiry) in &active {
            let sample_ts = event.ts_ms;
            if *taken_at <= sample_ts && *expiry > sample_ts {
                if let Some(b) = bucket(sample_ts, first_bucket, bucket_ms, &mut buckets) {
                    b["held"] = json!(active.len());
                }
            }
        }
    }
    json!({"bucketMs": bucket_ms, "buckets": buckets})
}

/// The RFC 6455 accept-key derivation: SHA-1 of key + GUID, base64.
fn ws_accept_key(key: &str) -> String {
    sha1_base64(format!("{key}258EAFA5-E914-47DA-95CA-C5AB0DC85B11").as_bytes())
}

/// One unmasked server text frame.
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

// SHA-1 (FIPS 180-1), the WebSocket handshake's only crypto — a spec-
// mandated obfuscation of the accept key, not a security boundary.
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

    let digest: Vec<u8> = h
        .iter()
        .flat_map(|word| word.to_be_bytes())
        .collect();
    base64(&digest)
}

/// Base64 (RFC 4648, standard alphabet, padded).
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
