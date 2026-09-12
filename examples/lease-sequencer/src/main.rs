//! The embedded-sequencer node: one storage node of the downstream
//! Corfu-style design draft. The process embeds `lunet_advisory_lock::Node`
//! (the same adapter the LuaJIT host drives through the C ABI) and owns the
//! host loop: heartbeat timer, election timer, fenced-boot recovery drive,
//! output drain, UDP receive pump, and the TCP client NDJSON port with the
//! same framing rules as `src/server.tl`.
//!
//! The lease driver runs the sequencer policy on every node: try to hold
//! the sentinel lock (SET, 500 ms lease), renew it 250 ms before the
//! deadline, and — while another node holds it — poll it as a GET and
//! schedule the next poll at the reported expiry plus `rand()*100 ms`.
//! Every attempt is logged as
//! `lease-attempt ts=<ms> node=<id> op=set|renew|get|steal expiry=<ms>`.

mod membership;
pub mod phi;
mod transport;

use lunet_advisory_lock::{
    NOT_LEADER, Node, OK, POSITION_APPEND, RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT,
    RECONFIGURE_JOIN, RECONFIGURE_LEAVE, RecoveryFlush, maybe_invariant,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::process::exit;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;

/// The sequencer lease's sentinel lock id.
const LOCK_ID: u64 = 0x0DDBA11;
/// The full lease the sequencer holds, in milliseconds.
const LEASE_MS: u64 = 500;
/// The renewal lead: the holder renews this long before its own deadline
/// (a 500 ms lease renewed at 250 ms after each grant).
const RENEW_MARGIN_MS: u64 = 250;
/// The poll jitter a non-holder adds beyond a reported expiry.
const POLL_JITTER_MS: u64 = 100;
/// One driver op's correlation deadline (direct or forwarded).
const OP_DEADLINE_MS: u64 = 1000;
/// The client-verb era-advance deadline (the admin ack discipline).
const ADMIN_DEADLINE_MS: u64 = 10000;
const TICK_MS: u64 = 5;
const LEADER_UNKNOWN: u32 = u32::MAX;
const MAX_CLIENT_LINE: usize = 65000;

const OUTPUT_SEND: u32 = 1;
const OUTPUT_REPLY: u32 = 2;
const STATE_NORMAL: u32 = 0;
const STATE_RECOVERING: u32 = 2;

/// Upstream `src/wire.rs` Tag::Commit; the adapter mirrors the tag the
/// same way it mirrors Tag::Reincarnation in `transport.rs`.
const VRR_COMMIT_TAG: u32 = 4;

/// Whether one VRR payload is a `Commit` datagram: the 20-byte header's
/// tag field is a big-endian `u32` at offset 0 (the same offset
/// `transport::reincarnation_pair` reads).
fn is_commit(payload: &[u8]) -> bool {
    payload.len() >= 21
        && u32::from_be_bytes(payload[0..4].try_into().expect("4 bytes")) == VRR_COMMIT_TAG
}

struct ClusterNode {
    id: u32,
    name: String,
    host: String,
    port: u16,
    endpoint: String,
    genesis: bool,
}

struct Driver {
    client_id: u64,
    request_num: u64,
    holder: uuid::Uuid,
    lease_id: u64,
    held_expiry: Option<u64>,
    last_get_foreign: bool,
    next_action_at: u64,
    pending: Option<Pending>,
}

enum Op {
    Get,
    Set,
    Steal,
    Renew,
}

impl Op {
    fn label(&self) -> &'static str {
        match self {
            Op::Get => "get",
            Op::Set => "set",
            Op::Steal => "steal",
            Op::Renew => "renew",
        }
    }
}

struct Pending {
    message_id: [u8; 16],
    op: Op,
    deadline: u64,
}

#[derive(Clone)]
enum TcpPending {
    Lock {
        message_id: [u8; 16],
        deadline: u64,
    },
    Admin {
        action: String,
        id: u32,
        endpoint: String,
        era0: u32,
        /// The establishing operation's choosing slot, captured from the
        /// reconfiguration drive's own sends: the slot at which the new
        /// configuration was chosen.
        slot: u64,
        deadline: u64,
    },
}

struct Conn {
    stream: TcpStream,
    buf: Vec<u8>,
    pending: Option<TcpPending>,
}

struct Host {
    node: Node,
    sock: UdpSocket,
    listener: TcpListener,
    peers: HashMap<u32, SocketAddr>,
    addr_to_id: HashMap<SocketAddr, u32>,
    fingerprint: String,
    own_id: u32,
    heartbeat_ms: u64,
    election_ms: u64,
    recovery_ms: u64,
    stagger_ms: u64,
    last_heartbeat: u64,
    leader_elapsed: u64,
    last_recovery: u64,
    last_status_note: u64,
    last_seen_leader: u32,
    /// This node booted a dirty restart (incarnation >= 1): the §8
    /// re-announce discipline drives `recover()` while the node is below
    /// voting weight, not only while it is fenced — a leader change
    /// mid-walk drops the armed forced-sequence machine (volatile leader
    /// state), and only a fresh announcement re-arms the new leader.
    reincarnated: bool,
    driver: Driver,
    forwarded_from: HashMap<[u8; 16], (SocketAddr, u64)>,
    conns: Vec<Conn>,
    /// The membership model: advisory evidence, never a consensus
    /// mechanism. It boots from the membership sidecar next to the
    /// incarnation marker, else from the descriptor, and moves only
    /// forward — a snapshot with a newer (era, slot) is adopted in memory,
    /// teaches the addressing rows its membership names, and writes behind
    /// on the lazy writer.
    model: membership::Model,
    sidecar: membership::SidecarWriter,
    discovery: Discovery,
    /// The phi-accrual monitor (item19): per-(era, leader, addr, monitor)
    /// sketches over the leader's heartbeat Commit arrivals. `None` when
    /// phi is disabled (`--phi-threshold 0`).
    phi_monitor: Option<phi::Table>,
    /// The phi policy (threshold, heartbeat, safety multiple, window).
    phi_cfg: phi::PhiConfig,
    /// The heartbeat sequence the leader stamps into trailers.
    heartbeat_seq: u32,
    /// The last Commit-send time this leader produced — "when otherwise
    /// idle" is measured against it.
    last_leader_commit_ms: u64,
    /// The keepalive proposer's own client identity and request counter.
    heartbeat_client_id: u64,
    heartbeat_request_num: u64,
    /// The last config era the phi monitor saw (era change = fresh
    /// sketch).
    phi_last_era: Option<u32>,
    /// The (config era, leader) the current detection is latched for. A
    /// fresh era or a new leader re-arms; an arriving heartbeat from the
    /// SAME leader does not — one detection per key.
    phi_detected_key: Option<(u32, u32)>,
    /// When each (era, leader) key was first watched: the bootstrap
    /// deadline for a sketch that never learns two intervals.
    phi_key_born: std::collections::HashMap<(u32, u32), u64>,
}

/// The boot-time era-qualified discovery state: request to every node the
/// process remembers, quorum within what it thought was the old cluster,
/// escalation on any newer (era, slot) — every escalation drops the
/// older-era responses and re-requests across that era's membership — and
/// a stop at a weighted quorum of agreeing snapshots. Bounded: on the
/// deadline the ordinary fenced boot proceeds regardless.
struct Discovery {
    era: u32,
    slot: u64,
    tallies: HashMap<String, membership::Tally>,
    deadline_ms: u64,
    next_request_ms: u64,
    active: bool,
}

fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

/// The phi monitor's construction: `None` when phi is disabled
/// (`--phi-threshold 0`), otherwise a table with the node's policy knobs.
fn phi_monitor(options: &Options) -> Option<phi::Table> {
    if options.phi_threshold <= 0.0 {
        return None;
    }
    Some(phi::Table::new(phi::PhiConfig {
        phi_threshold: options.phi_threshold,
        heartbeat_ms: options.heartbeat_ms,
        safety_multiple: options.phi_safety,
        window: 100,
    }))
}

/// The tracing subscriber stack. `--log` names the per-node file; its stem
/// becomes the daily-rolling file prefix under the same directory. The
/// returned `WorkerGuard` must live for the process lifetime.
fn init_tracing(log_path: &str) -> WorkerGuard {
    let path = std::path::Path::new(log_path);
    let dir = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| std::path::Path::new("."));
    let prefix = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("node");
    let appender = tracing_appender::rolling::RollingFileAppender::builder()
        .rotation(tracing_appender::rolling::Rotation::DAILY)
        .filename_prefix(prefix)
        .filename_suffix("log")
        .build(dir)
        .unwrap_or_else(|e| {
            eprintln!("lease-sequencer: cannot open rolling log {log_path}: {e}");
            exit(2);
        });
    let (writer, guard) = tracing_appender::non_blocking(appender);
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_target(false)
        .without_time()
        .with_writer(writer)
        .init();
    guard
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

fn parse_config(path: &str) -> Vec<ClusterNode> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("lease-sequencer: cannot read cluster descriptor {path}: {e}");
        exit(2);
    });
    let mut nodes = Vec::new();
    for line in text.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(trimmed).unwrap_or_else(|e| {
            eprintln!("lease-sequencer: bad descriptor line: {e}");
            exit(2);
        });
        let id = value["id"].as_u64().unwrap_or_else(|| {
            eprintln!("lease-sequencer: descriptor line without id");
            exit(2);
        }) as u32;
        let name = value["name"].as_str().unwrap_or_default().to_string();
        let host = value["host"].as_str().unwrap_or_default().to_string();
        let port = value["port"].as_u64().unwrap_or_default() as u16;
        let genesis =
            value["genesis"].as_str() == Some("true") || value["genesis"].as_bool() == Some(true);
        nodes.push(ClusterNode {
            id,
            endpoint: format!("{host}:{port}"),
            genesis,
            host,
            name,
            port,
        });
    }
    if nodes.is_empty() {
        eprintln!("lease-sequencer: empty cluster descriptor");
        exit(2);
    }
    nodes
}

struct Options {
    name: String,
    config: String,
    client: String,
    state: String,
    log: String,
    /// The AOF series directory. Non-empty turns the node into the standby
    /// telemetry host: the committed-transition hook feeds the async
    /// write-behind writer instead of the blocking journal, and the lease
    /// driver stays idle.
    aof_dir: String,
    /// The AOF's periodic-fsync knob (ms). 0 = only at roll and shutdown.
    aof_flush_ms: u64,
    /// The E2 recovery-boundary flush variant (diskless | single |
    /// double-ring). Non-diskless requires `recovery_scratch`.
    recovery_flush: RecoveryFlush,
    /// The scratch directory the recovery-boundary flush writes against.
    recovery_scratch: String,
    heartbeat_ms: u64,
    election_ms: u64,
    recovery_ms: u64,
    /// The phi threshold a leader-failure sketch must cross before its
    /// monitor acts (item19). 0 disables phi monitoring entirely.
    phi_threshold: f64,
    /// The hard safety multiple: no detection fires before
    /// `safety * heartbeat_ms` of leader silence, whatever phi says.
    phi_safety: f64,
}

fn parse_options() -> Options {
    let mut options = Options {
        name: String::new(),
        config: String::new(),
        client: String::new(),
        state: String::new(),
        log: String::new(),
        aof_dir: String::new(),
        aof_flush_ms: 1000,
        recovery_flush: RecoveryFlush::Diskless,
        recovery_scratch: String::new(),
        heartbeat_ms: 10,
        election_ms: 1000,
        recovery_ms: 1000,
        phi_threshold: 1.0,
        phi_safety: 2.0,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        let flag = &argv[index];
        let Some(value) = argv.get(index + 1) else {
            eprintln!("lease-sequencer: missing value for {flag}");
            exit(2);
        };
        match flag.as_str() {
            "--name" => options.name = value.clone(),
            "--config" => options.config = value.clone(),
            "--client" => options.client = value.clone(),
            "--state" => options.state = value.clone(),
            "--log" => options.log = value.clone(),
            "--aof-dir" => options.aof_dir = value.clone(),
            "--aof-flush-ms" => options.aof_flush_ms = value.parse().unwrap_or(1000),
            "--recovery-flush" => {
                options.recovery_flush = RecoveryFlush::parse(value).unwrap_or_else(|| {
                    eprintln!(
                        "lease-sequencer: --recovery-flush must be one of \
                         diskless|single|double-ring"
                    );
                    exit(2);
                })
            }
            "--recovery-scratch-dir" => options.recovery_scratch = value.clone(),
            "--heartbeat-ms" => options.heartbeat_ms = value.parse().unwrap_or(10),
            "--election-ms" => options.election_ms = value.parse().unwrap_or(1000),
            "--recovery-ms" => options.recovery_ms = value.parse().unwrap_or(1000),
            "--phi-threshold" => options.phi_threshold = value.parse().unwrap_or(1.0),
            "--phi-safety" => options.phi_safety = value.parse().unwrap_or(2.0),
            other => {
                eprintln!("lease-sequencer: unknown option {other}");
                exit(2);
            }
        }
        index += 2;
    }
    if options.name.is_empty()
        || options.config.is_empty()
        || options.client.is_empty()
        || options.state.is_empty()
        || options.log.is_empty()
    {
        eprintln!(
            "usage: lease-sequencer --name NAME --config PATH --client IPv4:PORT \
             --state PATH --log PATH [--aof-dir PATH] [--aof-flush-ms N] \
             [--recovery-flush diskless|single|double-ring] [--recovery-scratch-dir PATH] \
             [--heartbeat-ms N] [--election-ms N] [--recovery-ms N] \
             [--phi-threshold F] [--phi-safety F]"
        );
        exit(2);
    }
    options
}

impl Host {
    fn note(&self, body: &str) {
        info!("{} ts={}", body, millis());
    }

    /// One heartbeat arrival with a phi trailer: feed the (era, leader,
    /// addr, monitor) sketch, and lazily log the arrival-interval sample
    /// the normal-distribution chart plots.
    fn observe_heartbeat(
        &mut self,
        _sender: u32,
        addr: SocketAddr,
        trailer: &phi::Trailer,
        now: u64,
    ) {
        let Some(monitor) = &mut self.phi_monitor else {
            return;
        };
        let key = phi::SketchKey {
            era: trailer.era,
            leader: trailer.leader,
            leader_addr: phi::addr_text(addr),
            monitor: self.own_id,
        };
        let interval = monitor.observe(&key, now);
        self.phi_last_era = Some(trailer.era);
        if let Some(interval) = interval {
            self.note(&format!(
                "phi-interval node={} era={} leader={} addr={} dt={}",
                self.own_id,
                trailer.era,
                trailer.leader,
                phi::addr_text(addr),
                interval
            ));
        }
    }

    /// One monitor tick: evaluate the current leader's sketch against the
    /// threshold and the safety floor. On a crossing the host logs the
    /// detection and drives the existing view-change path
    /// (`leader_timeout`, the core's ordinary suspicion input); the core
    /// self-gates the actual fence on its own primary-timeout knob, so
    /// the drive is issued, not forced.
    fn phi_step(&mut self, now: u64, rng: &mut Rng) {
        if self.phi_monitor.is_none() {
            return;
        }
        let status = self.node.status();
        if self
            .phi_last_era
            .is_some_and(|era| era != status.config_era)
        {
            self.note(&format!(
                "phi-era-reset node={} era={}",
                self.own_id, status.config_era
            ));
            // Era/config change: the sketch table is fresh. Dropping the
            // live key forces the next observation to rebuild.
            self.phi_monitor = Some(phi::Table::new(self.phi_cfg.clone()));
            self.phi_detected_key = None;
        }
        self.phi_last_era = Some(status.config_era);
        // Detection stays armed in EVERY state: the §14.2 forced view
        // change can name a primary that never arrives, and the cluster
        // wedges in the pending state with the tick gate unable to
        // advance — the detector must keep watching the (dead) leader
        // from inside the limbo and force the next view itself.
        if status.leader == self.own_id {
            return;
        }
        if status.leader == LEADER_UNKNOWN {
            return;
        }
        let Some(&addr) = self.peers.get(&status.leader) else {
            return;
        };
        let key = phi::SketchKey {
            era: status.config_era,
            leader: status.leader,
            leader_addr: phi::addr_text(addr),
            monitor: self.own_id,
        };
        // Read the sketch's verdict first (immutable borrow ends), then
        // act on it — the drive borrows the node mutably. A sketch that
        // has never learned two intervals — INCLUDING one never observed
        // at all, the dead primary no heartbeat ever reaches — gets the
        // bootstrap verdict: the elected leader's first two heartbeats
        // are due within a few real intervals of election, so
        // `bootstrap_after_ms` of silence past the key's birth is a dead
        // primary no detector math can express yet. The floor stays
        // conservative (half a second, well past any live leader's
        // first-heartbeat lag) so a slow-start primary is never suspected.
        let first_seen = self.phi_key_born.entry((key.era, key.leader)).or_insert(now);
        let bootstrap_after = (6 * u64::from(self.phi_cfg.heartbeat_ms)).max(500);
        let sketch_ref = self
            .phi_monitor
            .as_ref()
            .and_then(|m| m.get(&key));
        let verdict = match sketch_ref {
            Some(sketch) if sketch.sample_count() >= 2 => (
                sketch.last_arrival(),
                sketch.phi(now),
                phi::decide(sketch, now, &self.phi_cfg),
            ),
            _ => {
                let bootstrapped = now.saturating_sub(*first_seen) > bootstrap_after;
                (
                    *first_seen,
                    if bootstrapped { f64::INFINITY } else { 0.0 },
                    bootstrapped,
                )
            }
        };
        let (last_arrival, phi_now, fires) = verdict;
        let silence = now.saturating_sub(last_arrival);
        let detected_key = (status.config_era, status.leader);
        if self.phi_detected_key == Some(detected_key) || !fires {
            return;
        }
        self.phi_detected_key = Some(detected_key);
        let floor = phi::floor_ms(
            self.phi_monitor
                .as_ref()
                .and_then(|m| m.get(&key))
                .expect("the verdict came from this sketch"),
            &self.phi_cfg,
        ) as u64;
        self.note(&format!(
            "phi-detect node={} era={} leader={} phi={:.3} silence={} floor={} addr={}",
            self.own_id,
            status.config_era,
            status.leader,
            phi_now,
            silence,
            floor,
            phi::addr_text(addr)
        ));
        // The phi-accrual actuation: the §14.2 host-forced view change —
        // no timed-tick suspicion gate, the detector's verdict drives it
        // directly. Falls back to the ordinary suspicion tick on refusal.
        let forced = self.node.force_view(status.era, status.view + 1);
        if forced != 0 {
            let _ = self.node.leader_timeout();
        }
        self.flush_outputs(now, rng);
    }

    /// The leader's idle heartbeat: when otherwise idle — no Commit left
    /// this node in the last interval — the leader proposes a read-only
    /// `get`, whose commit fan-out emits the heartbeat Commit every
    /// follower's phi sketch observes.
    fn heartbeat_op(&mut self, now: u64, rng: &mut Rng) {
        let status = self.node.status();
        if status.state != STATE_NORMAL
            || status.leader != self.own_id
            || now.saturating_sub(self.last_leader_commit_ms) < self.heartbeat_ms
        {
            return;
        }
        self.heartbeat_request_num += 1;
        let message_id = *uuid::Uuid::new_v4().as_bytes();
        let mid = uuid::Uuid::from_bytes(message_id).to_string();
        let json = format!(
            "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":{},\"request_num\":{},\"lock_id\":{LOCK_ID}}}",
            self.heartbeat_client_id, self.heartbeat_request_num
        );
        let _ = self.node.request(json.as_bytes());
        self.flush_outputs(now, rng);
        let _ = rng;
    }

    fn lease_attempt(&self, node_id: u32, op: &str, expiry: u64) {
        info!(
            "lease-attempt ts={} node={node_id} op={op} expiry={expiry}",
            millis()
        );
    }

    fn send_application(&mut self, addr: SocketAddr, payload: &[u8]) {
        let packet =
            transport::encode_peer(transport::PEER_APPLICATION, &self.fingerprint, payload);
        let _ = self.sock.send_to(&packet, addr);
    }

    fn send_forward_request(&mut self, addr: SocketAddr, message_id: &[u8; 16], json: &str) {
        let mut payload = Vec::with_capacity(1 + 16 + json.len());
        payload.push(transport::FORWARD_REQUEST);
        payload.extend_from_slice(message_id);
        payload.extend_from_slice(json.as_bytes());
        self.send_application(addr, &payload);
    }

    fn send_not_leader(&mut self, addr: SocketAddr, message_id: &[u8; 16], era: u32, view: u32) {
        let mut payload = Vec::with_capacity(1 + 16 + 8);
        payload.push(transport::FORWARD_NOT_LEADER);
        payload.extend_from_slice(message_id);
        payload.extend_from_slice(&era.to_be_bytes());
        payload.extend_from_slice(&view.to_be_bytes());
        self.send_application(addr, &payload);
    }

    fn send_forward_response(&mut self, addr: SocketAddr, message_id: &[u8; 16], bytes: &[u8]) {
        let mut payload = Vec::with_capacity(1 + 16 + bytes.len());
        payload.push(transport::FORWARD_RESPONSE);
        payload.extend_from_slice(message_id);
        payload.extend_from_slice(bytes);
        self.send_application(addr, &payload);
    }

    fn flush_outputs(&mut self, now: u64, rng: &mut Rng) -> u64 {
        let mut established_slot = 0u64;
        while let Some(out) = self.node.next_output() {
            if out.kind == OUTPUT_SEND {
                if out.slot > established_slot {
                    established_slot = out.slot;
                }
                let Some(&addr) = self.peers.get(&out.to) else {
                    // The maybe: a send to an unaddressable replica. Not
                    // provably impossible (the peer may be down mid-remap —
                    // the known "cannot address replica <old-id>" window
                    // after a reincarnation remap) and survivable: the
                    // datagram is dropped. Test/debug builds crash so the
                    // smokes surface it; release warns with full context.
                    maybe_invariant!(
                        "cannot address replica (to={} kind={} era={} view={} slot={} len={}); \
                         datagram dropped",
                        out.to,
                        out.kind,
                        out.era,
                        out.view,
                        out.slot,
                        out.bytes.len()
                    );
                    continue;
                };
                // The phi trailer (item19) rides only the leader's Commit
                // datagrams: the stream followers' sketches observe. The
                // trailer lives OUTSIDE the core's message bytes — the
                // receiving host strips it before node.receive() — so the
                // core's exact-length wire contract (W3) is untouched.
                let status = self.node.status();
                let payload = if status.state == STATE_NORMAL
                    && status.leader == self.own_id
                    && is_commit(&out.bytes)
                {
                    self.last_leader_commit_ms = now;
                    self.heartbeat_seq = self.heartbeat_seq.wrapping_add(1);
                    let trailer = phi::Trailer {
                        era: status.era,
                        leader: status.leader,
                        seq: self.heartbeat_seq,
                        sent_at_ms: now,
                    };
                    let mut payload = out.bytes.clone();
                    trailer.append_to(&mut payload);
                    payload
                } else {
                    out.bytes.clone()
                };
                let packet =
                    transport::encode_peer(transport::PEER_VRR, &self.fingerprint, &payload);
                let _ = self.sock.send_to(&packet, addr);
            } else if out.kind == OUTPUT_REPLY {
                if let Some((dest, _)) = self.forwarded_from.remove(&out.message_id) {
                    self.send_forward_response(dest, &out.message_id, &out.bytes);
                } else if self
                    .driver
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.message_id == out.message_id)
                {
                    let op = match self.driver.pending.take() {
                        Some(p) => p.op,
                        None => continue,
                    };
                    self.driver_complete(op, &out.bytes, now, rng);
                } else {
                    for conn in &mut self.conns {
                        let matches = matches!(
                            conn.pending,
                            Some(TcpPending::Lock { message_id, .. })
                                if message_id == out.message_id
                        );
                        if matches {
                            conn.pending = None;
                            let _ = conn.stream.write_all(&out.bytes);
                            let _ = conn.stream.write_all(b"\n");
                            let _ = conn.stream.flush();
                            break;
                        }
                    }
                }
            }
        }
        established_slot
    }

    /// Adds the addressing rows an adopted snapshot's membership names:
    /// rows grow additively and never regress (a departed member's rows
    /// leave through the leave verb's flow, never through a snapshot).
    fn add_member_rows(&mut self, members: &[membership::SnapshotMember]) {
        for member in members {
            if self.peers.contains_key(&member.id) {
                continue;
            }
            if let Ok(mut addrs) = member.endpoint.to_socket_addrs()
                && let Some(addr) = addrs.next()
            {
                self.peers.insert(member.id, addr);
                self.addr_to_id.insert(addr, member.id);
            }
        }
    }

    /// Adopts a newer (era, slot) snapshot into the model: rows, lazy
    /// write-behind, and the operator-visible record. An equal or older
    /// snapshot is a no-op.
    fn adopt(&mut self, snapshot: membership::Snapshot, source: &str) {
        if !self.model.adopt(snapshot.clone()) {
            return;
        }
        self.add_member_rows(&snapshot.members);
        self.sidecar.enqueue(&snapshot);
        self.note(&format!(
            "membership snapshot adopted era={} slot={} members={} source={source}",
            snapshot.era,
            snapshot.slot,
            snapshot.members.len()
        ));
    }

    /// The leader's post-commit dissemination: the as-at-new-generation
    /// snapshot goes to the (new) membership — the departed member's row
    /// is already gone for a leave, the joining member's row was added at
    /// proposal time — and the committed fact writes behind. Recipients
    /// only check up-to-dateness (era plus slot); a lower-era snapshot is
    /// ignored everywhere.
    fn disseminate(&mut self) {
        let snapshot = self.model.snapshot();
        let payload = membership::encode_response(&snapshot);
        let targets: Vec<SocketAddr> = self
            .peers
            .iter()
            .filter(|(id, _)| **id != self.own_id)
            .map(|(_, addr)| *addr)
            .collect();
        for addr in targets {
            let packet =
                transport::encode_peer(transport::PEER_SNAPSHOT, &self.fingerprint, &payload);
            let _ = self.sock.send_to(&packet, addr);
        }
        self.sidecar.enqueue(&snapshot);
        self.note(&format!(
            "membership snapshot disseminated era={} slot={} members={}",
            snapshot.era,
            snapshot.slot,
            snapshot.members.len()
        ));
    }

    /// The boot-time discovery round: one request per remembered peer, on
    /// the fixed cadence, until the deadline or the quorum.
    fn discovery_step(&mut self, now: u64) {
        if !self.discovery.active {
            return;
        }
        if now >= self.discovery.deadline_ms {
            self.discovery.active = false;
            self.note("discovery deadline reached; the ordinary fenced boot proceeds");
            return;
        }
        if now >= self.discovery.next_request_ms {
            self.discovery.next_request_ms = now + 100;
            let targets: Vec<SocketAddr> = self
                .peers
                .iter()
                .filter(|(id, _)| **id != self.own_id)
                .map(|(_, addr)| *addr)
                .collect();
            let payload = membership::encode_request();
            for addr in targets {
                let packet =
                    transport::encode_peer(transport::PEER_SNAPSHOT, &self.fingerprint, &payload);
                let _ = self.sock.send_to(&packet, addr);
            }
        }
    }

    /// One lock op's submission route: propose locally as the leader, or
    /// forward to the leader over the application channel; anything else is
    /// a backoff-and-retry for the policy loop.
    fn route_op(
        &mut self,
        rc: i32,
        op: Op,
        json: &str,
        message_id: [u8; 16],
        now: u64,
        rng: &mut Rng,
    ) {
        if rc == OK {
            self.driver.pending = Some(Pending {
                message_id,
                op,
                deadline: now + OP_DEADLINE_MS,
            });
            return;
        }
        self.flush_outputs(now, rng);
        if rc == NOT_LEADER {
            let status = self.node.status();
            if status.leader != LEADER_UNKNOWN
                && status.leader != self.own_id
                && let Some(&addr) = self.peers.get(&status.leader)
            {
                self.send_forward_request(addr, &message_id, json);
                self.driver.pending = Some(Pending {
                    message_id,
                    op,
                    deadline: now + OP_DEADLINE_MS,
                });
                return;
            }
        }
        self.driver.next_action_at = now + rng.below(100) + 50;
    }

    fn submit_get(&mut self, now: u64, rng: &mut Rng) {
        self.driver.request_num += 1;
        let message_id = *uuid::Uuid::new_v4().as_bytes();
        let mid = uuid::Uuid::from_bytes(message_id).to_string();
        let json = format!(
            "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":{},\"request_num\":{},\"lock_id\":{LOCK_ID}}}",
            self.driver.client_id, self.driver.request_num
        );
        let rc = self.node.request(json.as_bytes());
        self.route_op(rc, Op::Get, &json, message_id, now, rng);
    }

    fn submit_hold(&mut self, op: Op, now: u64, rng: &mut Rng) {
        self.driver.request_num += 1;
        self.driver.lease_id += 1;
        let message_id = *uuid::Uuid::new_v4().as_bytes();
        let mid = uuid::Uuid::from_bytes(message_id).to_string();
        let expiry = now + LEASE_MS;
        let json = format!(
            "{{\"op\":\"set\",\"message_id\":\"{mid}\",\"client_id\":{},\"request_num\":{},\"lock_id\":{LOCK_ID},\"lease\":{{\"lease_id\":{},\"holder\":\"{}\",\"expiry\":{expiry}}}}}",
            self.driver.client_id,
            self.driver.request_num,
            self.driver.lease_id,
            self.driver.holder
        );
        self.lease_attempt(self.own_id, op.label(), expiry);
        let rc = self.node.request(json.as_bytes());
        self.route_op(rc, op, &json, message_id, now, rng);
    }

    fn submit_renew(&mut self, now: u64, rng: &mut Rng) {
        self.driver.request_num += 1;
        self.driver.lease_id += 1;
        let message_id = *uuid::Uuid::new_v4().as_bytes();
        let mid = uuid::Uuid::from_bytes(message_id).to_string();
        let expiry = now + LEASE_MS;
        let json = format!(
            "{{\"op\":\"set\",\"message_id\":\"{mid}\",\"client_id\":{},\"request_num\":{},\"lock_id\":{LOCK_ID},\"lease\":{{\"lease_id\":{},\"holder\":\"{}\",\"expiry\":{expiry}}}}}",
            self.driver.client_id,
            self.driver.request_num,
            self.driver.lease_id,
            self.driver.holder
        );
        self.lease_attempt(self.own_id, "renew", expiry);
        let rc = self.node.request(json.as_bytes());
        self.route_op(rc, Op::Renew, &json, message_id, now, rng);
    }

    fn driver_complete(&mut self, op: Op, bytes: &[u8], now: u64, rng: &mut Rng) {
        let Ok(reply) = serde_json::from_slice::<serde_json::Value>(bytes) else {
            self.driver.next_action_at = now + rng.below(100) + 20;
            return;
        };
        match op {
            Op::Get => {
                let lease = reply["lease"].as_object();
                match lease {
                    None => {
                        self.lease_attempt(self.own_id, "get", 0);
                        let steal = self.driver.last_get_foreign;
                        self.driver.last_get_foreign = false;
                        self.submit_hold(if steal { Op::Steal } else { Op::Set }, now, rng);
                    }
                    Some(lease) => {
                        let expiry = lease["expiry"].as_u64().unwrap_or_default();
                        let holder = lease["holder"].as_str().unwrap_or_default();
                        self.lease_attempt(self.own_id, "get", expiry);
                        if holder == self.driver.holder.to_string() {
                            self.driver.held_expiry = Some(expiry);
                            self.driver.last_get_foreign = false;
                        } else {
                            self.driver.last_get_foreign = true;
                            self.driver.next_action_at = expiry + rng.below(POLL_JITTER_MS + 1);
                        }
                    }
                }
            }
            Op::Set | Op::Steal | Op::Renew => {
                let granted = reply["granted"].as_bool() == Some(true);
                if granted {
                    let expiry = reply["lease"]["expiry"].as_u64().unwrap_or(now + LEASE_MS);
                    self.driver.held_expiry = Some(expiry);
                    self.note(&format!(
                        "grant node={} op={} expiry={expiry}",
                        self.own_id,
                        op.label()
                    ));
                } else {
                    self.driver.held_expiry = None;
                    if let Some(lease) = reply["lease"].as_object() {
                        let expiry = lease["expiry"].as_u64().unwrap_or_default();
                        let holder = lease["holder"].as_str().unwrap_or_default();
                        if holder != self.driver.holder.to_string() {
                            self.driver.last_get_foreign = true;
                            self.driver.next_action_at = expiry + rng.below(POLL_JITTER_MS + 1);
                            return;
                        }
                    }
                    self.driver.next_action_at = now + rng.below(100) + 50;
                }
            }
        }
    }

    fn driver_step(&mut self, now: u64, rng: &mut Rng) {
        // Host policy: the client stream pauses unless this node is a
        // Normal member of the current view whose folded configuration era
        // matches it. A committed reconfiguration's establishing era
        // completes only through the §8.7.8 fence into the established
        // era, and the primary's client stream is the activity that keeps
        // that fence from arming — so every node holds its lease driver
        // (and its forwarded traffic) for the transition's bounded window;
        // the lease lapses and a fresh grant re-acquires it.
        let status = self.node.status();
        if status.config_era != status.era || status.state != STATE_NORMAL {
            self.driver.pending = None;
            self.driver.held_expiry = None;
            return;
        }
        if let Some(pending) = &self.driver.pending {
            if now < pending.deadline {
                return;
            }
            self.driver.pending = None;
            self.driver.next_action_at = now + rng.below(80) + 20;
        }
        if now < self.driver.next_action_at {
            return;
        }
        if let Some(expiry) = self.driver.held_expiry {
            if expiry <= now {
                self.driver.held_expiry = None;
            } else if now + RENEW_MARGIN_MS >= expiry {
                self.submit_renew(now, rng);
                return;
            } else {
                return;
            }
        }
        self.submit_get(now, rng);
    }
}

fn main() {
    let options = parse_options();
    let nodes = parse_config(&options.config);
    let Some(own) = nodes.iter().find(|node| node.name == options.name) else {
        eprintln!("lease-sequencer: --name not in descriptor");
        exit(2);
    };
    let own_desc_id = own.id;
    // The member buffer, in descriptor line order: plain entries are the
    // genesis succession sequence, `:j` entries are post-genesis joiners.
    let members = nodes
        .iter()
        .map(|node| {
            if node.genesis {
                format!("{}:{}", node.id, node.name)
            } else {
                format!("{}:{}:j", node.id, node.name)
            }
        })
        .collect::<Vec<_>>()
        .join("\0");
    let genesis: Vec<transport::GenesisMember<'_>> = nodes
        .iter()
        .filter(|node| node.genesis)
        .map(|node| transport::GenesisMember {
            id: node.id,
            name: &node.name,
            host: &node.host,
            port: node.port,
        })
        .collect();
    let fingerprint = transport::genesis_fingerprint(&genesis);

    // The subscriber stack (the binary owns it; the library stays
    // subscriber-free): `RUST_LOG` env-filter, ANSI off, no line timestamp
    // (events carry their own `ts=` fields), through `NonBlocking` over a
    // per-node daily rolling file. The guard is held for the process
    // lifetime and flushes on an orderly shutdown.
    let _worker_guard = init_tracing(&options.log);
    let standby = !options.aof_dir.is_empty();
    let node = if standby {
        // The standby telemetry host: the committed-transition hook feeds
        // the async AOF writer (the LKE1 journal producer path, deferred
        // durability target) and the lease driver stays idle.
        Node::open_aof(
            &members,
            &options.name,
            &options.state,
            &options.aof_dir,
            (options.aof_flush_ms > 0).then_some(options.aof_flush_ms),
        )
        .unwrap_or_else(|code| {
            eprintln!("lease-sequencer: standby node boot failed with code {code}");
            exit(2);
        })
    } else if options.recovery_flush == RecoveryFlush::Diskless {
        Node::open(&members, &options.name, &options.state, None, 0).unwrap_or_else(|code| {
            eprintln!("lease-sequencer: node boot failed with code {code}");
            exit(2);
        })
    } else {
        // The E2 variant boot: the recovery-boundary flush executes at the
        // dirty-boot classification inside the adapter.
        if options.recovery_scratch.is_empty() {
            eprintln!(
                "lease-sequencer: --recovery-flush {} requires --recovery-scratch-dir",
                options.recovery_flush.label()
            );
            exit(2);
        }
        Node::open_with_recovery_flush(
            &members,
            &options.name,
            &options.state,
            None,
            0,
            options.recovery_flush,
            &options.recovery_scratch,
        )
        .unwrap_or_else(|code| {
            eprintln!("lease-sequencer: node boot failed with code {code}");
            exit(2);
        })
    };
    let own_id = node.own_id();
    let incarnation = own_id.saturating_sub(own_desc_id) / (1 << 24);

    let udp_endpoint = own.endpoint.clone();
    let sock = UdpSocket::bind(udp_endpoint.as_str()).unwrap_or_else(|e| {
        eprintln!("lease-sequencer: UDP bind {udp_endpoint} failed: {e}");
        exit(2);
    });
    sock.set_nonblocking(true).expect("nonblocking udp");
    let listener = TcpListener::bind(options.client.as_str()).unwrap_or_else(|e| {
        eprintln!("lease-sequencer: TCP bind {} failed: {e}", options.client);
        exit(2);
    });
    listener.set_nonblocking(true).expect("nonblocking tcp");

    let mut peers: HashMap<u32, SocketAddr> = HashMap::new();
    let mut addr_to_id: HashMap<SocketAddr, u32> = HashMap::new();
    for descriptor in &nodes {
        if let Ok(mut addrs) = descriptor.endpoint.to_socket_addrs()
            && let Some(addr) = addrs.next()
        {
            peers.insert(descriptor.id, addr);
            addr_to_id.insert(addr, descriptor.id);
        }
    }

    let rank = nodes
        .iter()
        .position(|node| node.name == options.name)
        .unwrap_or_default() as u64;
    // The membership model boots from the membership sidecar next to the
    // incarnation marker (the node's last-known adopted facts), else from
    // the descriptor. A sidecar that does not parse is ignored entirely:
    // the descriptor is the fallback and discovery re-learns.
    let mut model_source = "descriptor";
    let (model_era, model_slot, model_rows) = match membership::load_sidecar(&options.state) {
        Some(snapshot) => {
            model_source = "sidecar";
            (snapshot.era, snapshot.slot, snapshot.members)
        }
        None => {
            let rows: Vec<(u32, String, u16, bool)> = nodes
                .iter()
                .map(|node| (node.id, node.host.clone(), node.port, node.genesis))
                .collect();
            (1, 0, membership::descriptor_model(&rows))
        }
    };
    let model = membership::Model {
        era: model_era,
        slot: model_slot,
        members: model_rows,
    };
    let sidecar = membership::SidecarWriter::open(&options.state).unwrap_or_else(|e| {
        eprintln!("lease-sequencer: membership sidecar open failed: {e}");
        exit(2);
    });
    let driver = Driver {
        client_id: own_desc_id as u64,
        request_num: 0,
        holder: uuid::Uuid::new_v4(),
        lease_id: 0,
        held_expiry: None,
        last_get_foreign: false,
        next_action_at: millis() + 300,
        pending: None,
    };
    let mut host = Host {
        node,
        sock,
        listener,
        peers,
        addr_to_id,
        fingerprint,
        own_id,
        heartbeat_ms: options.heartbeat_ms,
        election_ms: options.election_ms,
        recovery_ms: options.recovery_ms,
        stagger_ms: rank * 200,
        last_heartbeat: 0,
        leader_elapsed: 0,
        last_recovery: 0,
        last_status_note: 0,
        last_seen_leader: LEADER_UNKNOWN,
        reincarnated: incarnation > 0,
        driver,
        forwarded_from: HashMap::new(),
        conns: Vec::new(),
        model,
        sidecar,
        discovery: Discovery {
            era: model_era,
            slot: model_slot,
            tallies: HashMap::new(),
            deadline_ms: millis() + 15000,
            next_request_ms: 0,
            active: true,
        },
        phi_monitor: phi_monitor(&options),
        phi_cfg: phi::PhiConfig {
            phi_threshold: options.phi_threshold,
            heartbeat_ms: options.heartbeat_ms,
            safety_multiple: options.phi_safety,
            window: 100,
        },
        heartbeat_seq: 0,
        last_leader_commit_ms: 0,
        heartbeat_client_id: 0x0BEEF000 + own_desc_id as u64,
        heartbeat_request_num: 0,
        phi_last_era: None,
        phi_detected_key: None,
        phi_key_born: std::collections::HashMap::new(),
    };
    host.note(&format!(
        "boot name={} descriptor-id={own_desc_id} own={own_id} incarnation={incarnation}",
        options.name
    ));
    host.note(&format!("membership fingerprint={}", host.fingerprint));
    host.note(&format!(
        "membership model era={} slot={} members={} source={}",
        host.model.era,
        host.model.slot,
        host.model.members.len(),
        model_source
    ));
    let now = millis();
    let mut rng = Rng::new(millis() ^ (own_desc_id as u64) ^ (std::process::id() as u64));
    host.flush_outputs(now, &mut rng);

    loop {
        let now = millis();
        pump_udp(&mut host, now, &mut rng);
        pump_tcp(&mut host, now, &mut rng);
        timers(&mut host, now, &mut rng);
        host.discovery_step(now);
        host.driver_step(now, &mut rng);
        host.flush_outputs(now, &mut rng);
        std::thread::sleep(Duration::from_millis(TICK_MS));
    }
}

fn timers(host: &mut Host, now: u64, rng: &mut Rng) {
    let status = host.node.status();
    if status.leader != host.last_seen_leader {
        host.last_seen_leader = status.leader;
        host.note(&format!(
            "leader leader={} era={} view={}",
            status.leader, status.era, status.view
        ));
    }
    if now.saturating_sub(host.last_heartbeat) >= host.heartbeat_ms {
        host.last_heartbeat = now;
        let _ = host.node.idle();
        host.flush_outputs(now, rng);
        host.heartbeat_op(now, rng);
    }
    host.phi_step(now, rng);
    if status.state == STATE_NORMAL && status.leader == host.own_id {
        host.leader_elapsed = 0;
    } else {
        host.leader_elapsed += TICK_MS;
        if host.leader_elapsed >= host.election_ms + host.stagger_ms {
            host.leader_elapsed = 0;
            let _ = host.node.leader_timeout();
            host.flush_outputs(now, rng);
        }
    }
    if now.saturating_sub(host.last_status_note) >= 2000 {
        host.last_status_note = now;
        // `voting` is the E1 runner's rejoin-serving signal: the
        // reincarnated node is back at voting weight in the folded
        // configuration once the leader's forced reconfiguration walk
        // completes.
        let voting = u32::from(host.node.voting_weight().unwrap_or(0) > 0);
        host.note(&format!(
            "status state={} leader={} era={} view={} config_era={} voting={voting} \
             sidecar_drops={}",
            status.state,
            status.leader,
            status.era,
            status.view,
            status.config_era,
            host.sidecar.drops()
        ));
    }
    if (status.state == STATE_RECOVERING
        || (host.reincarnated && host.node.voting_weight().is_none_or(|weight| weight == 0)))
        && now.saturating_sub(host.last_recovery) >= host.recovery_ms
    {
        host.last_recovery = now;
        let _ = host.node.recover();
        host.flush_outputs(now, rng);
    }
}

fn pump_udp(host: &mut Host, now: u64, rng: &mut Rng) {
    let mut buf = [0u8; 65507];
    loop {
        let Ok((len, addr)) = host.sock.recv_from(&mut buf) else {
            return;
        };
        let Some(&replica) = host.addr_to_id.get(&addr) else {
            tracing::warn!(source = %addr, len, "datagram from an unregistered endpoint dropped");
            continue;
        };
        handle_packet(host, replica, addr, &buf[..len], now, rng);
    }
}

fn handle_packet(
    host: &mut Host,
    mut replica: u32,
    addr: SocketAddr,
    packet: &[u8],
    now: u64,
    rng: &mut Rng,
) {
    let Some((kind, fingerprint, payload)) = transport::decode_peer(packet) else {
        return;
    };
    if fingerprint != host.fingerprint {
        // Unexpected but survivable: a datagram from outside the
        // deployment's genesis fingerprint. Membership snapshots are
        // advisory evidence, so a foreign deployment's snapshot drops
        // without ceremony (here: for every packet kind this host
        // carries, and never a quarantine).
        host.note("dirty-fingerprint; datagram dropped");
        return;
    }
    if kind == transport::PEER_SNAPSHOT {
        handle_snapshot_packet(host, addr, payload);
        return;
    }
    if kind == transport::PEER_VRR {
        // The reincarnation remap: a `Reincarnation(old, new)` announcement
        // arriving from the socket the deployment attributes to `old` IS the
        // restarted process's entry ticket; the row for the bumped id is
        // ADDED at the source socket and the old id's row STAYS — the old
        // era's configuration still names it, its send targets the same
        // socket the new identity binds, and the row leaves only when the
        // reincarnation forced steps evict the old identity from the
        // serving configuration.
        //
        // Before anything else: the phi trailer (item19) rides at the BACK
        // of the leader's heartbeat Commits, entirely OUTSIDE the core's
        // message bytes. Strip it here so the core sees the exact-length
        // message its W3 contract demands, and feed the arrival to the
        // sketch when the sender is the leader this monitor watches.
        let (payload, trailer) = match phi::Trailer::strip_from(payload) {
            Some((front, trailer)) => (front, Some(trailer)),
            None => (payload, None),
        };
        if let Some(trailer) = &trailer {
            host.observe_heartbeat(replica, addr, trailer, now);
        }
        if let Some((old, new)) = transport::reincarnation_pair(payload)
            && old == replica
            && old != new
            && new >= (1 << 24)
            && !host.peers.contains_key(&new)
        {
            host.peers.insert(new, addr);
            host.addr_to_id.insert(addr, new);
            host.note(&format!("remap old={old} new={new}"));
            replica = new;
        }
        if host.node.status().leader == replica {
            host.leader_elapsed = 0;
        }
        let _ = host.node.receive(replica, payload);
        host.flush_outputs(now, rng);
        return;
    }
    if payload.is_empty() {
        return;
    }
    match payload[0] {
        0x01 => {
            // FORWARD_REQUEST: execute as the leader or redirect.
            if payload.len() > 1 + 16 {
                let mut message_id = [0u8; 16];
                message_id.copy_from_slice(&payload[1..17]);
                let json = &payload[17..];
                let status = host.node.status();
                if status.state == STATE_NORMAL && status.leader == host.own_id {
                    let deadline = now + 30000;
                    host.forwarded_from.insert(message_id, (addr, deadline));
                    let rc = host.node.request(json);
                    host.flush_outputs(now, rng);
                    if rc != OK {
                        host.forwarded_from.remove(&message_id);
                        host.send_not_leader(addr, &message_id, status.era, status.view);
                    }
                } else {
                    host.send_not_leader(addr, &message_id, status.era, status.view);
                }
            }
        }
        0x02 => {
            // FORWARD_RESPONSE: correlate to the driver's pending op. An
            // ack that correlates to nothing is a maybe: unexpected, not
            // provably impossible (the pending op may have timed out and
            // been retried in the window), and survivable.
            if payload.len() > 1 + 16
                && host
                    .driver
                    .pending
                    .as_ref()
                    .is_some_and(|p| p.message_id == payload[1..17])
            {
                if let Some(pending) = host.driver.pending.take() {
                    let op = pending.op;
                    host.driver_complete(op, &payload[17..], now, rng);
                }
            } else if payload.len() > 1 + 16 {
                let mut message_id = [0u8; 16];
                message_id.copy_from_slice(&payload[1..17]);
                maybe_invariant!(
                    "ack for an unclaimed verb (message_id={}, len={})",
                    uuid::Uuid::from_bytes(message_id),
                    payload.len()
                );
            }
        }
        // FORWARD_NOT_LEADER: drop the pending op; the policy retries.
        0x03 if payload.len() == 1 + 16 + 8
            && host
                .driver
                .pending
                .as_ref()
                .is_some_and(|p| p.message_id == payload[1..17]) =>
        {
            host.driver.pending = None;
            host.driver.next_action_at = now + rng.below(80) + 20;
        }
        _ => {}
    }
}

/// One PEER_SNAPSHOT packet: the request is answered from this node's
/// current membership model (no leader has to be known first), and a
/// response — from a discovery answer or from the leader's post-commit
/// dissemination, the same wire shape serving both — is fed to the
/// discovery state machine. While discovery is running a response only
/// escalates the era or tallies agreement within it: the model adopts at a
/// weighted quorum, never from one node's word. Afterwards the
/// dissemination semantics apply: only the up-to-dateness check, newer
/// (era, slot) adopted immediately, equal or older ignored everywhere.
fn handle_snapshot_packet(host: &mut Host, addr: SocketAddr, payload: &[u8]) {
    if membership::decode_request(payload) {
        let response = membership::encode_response(&host.model.snapshot());
        let packet = transport::encode_peer(transport::PEER_SNAPSHOT, &host.fingerprint, &response);
        let _ = host.sock.send_to(&packet, addr);
        return;
    }
    let Some(snap) = membership::decode_response(payload) else {
        return;
    };
    // The responder is attributed through the addressing tables; an
    // unattributed source contributes nothing (its weight is unknown).
    let Some(&responder) = host.addr_to_id.get(&addr) else {
        return;
    };
    if host.discovery.active {
        if membership::newer(snap.era, snap.slot, host.discovery.era, host.discovery.slot) {
            host.discovery.era = snap.era;
            host.discovery.slot = snap.slot;
            host.discovery.tallies.clear();
            host.add_member_rows(&snap.members);
            host.note(&format!(
                "discovery escalate era={} slot={}; older-era responses dropped",
                snap.era, snap.slot
            ));
        } else if snap.era == host.discovery.era && snap.slot == host.discovery.slot {
            let key = membership::agreement_key(&snap);
            let tally = host.discovery.tallies.entry(key).or_default();
            tally.record(
                responder,
                membership::member_weight(&snap.members, responder),
            );
            if tally.agrees_with(&snap.members) {
                host.discovery.active = false;
                host.adopt(snap, "discovery");
                host.note(&format!(
                    "discovery quorum reached at era={} slot={}",
                    host.model.era, host.model.slot
                ));
            }
        }
    } else {
        host.adopt(snap, "dissemination");
    }
}

fn pump_tcp(host: &mut Host, now: u64, rng: &mut Rng) {
    while let Ok((stream, _)) = host.listener.accept() {
        let _ = stream.set_nonblocking(true);
        host.conns.push(Conn {
            stream,
            buf: Vec::new(),
            pending: None,
        });
    }
    // Sweep the forward-correlation deadlines.
    let stale: Vec<[u8; 16]> = host
        .forwarded_from
        .iter()
        .filter(|(_, (_, deadline))| now >= *deadline)
        .map(|(id, _)| *id)
        .collect();
    for id in stale {
        host.forwarded_from.remove(&id);
    }
    for index in (0..host.conns.len()).rev() {
        let mut drop_conn = !read_conn(&mut host.conns[index]);
        while !drop_conn && host.conns[index].pending.is_none() {
            let Some(line) = pop_line(&mut host.conns[index]) else {
                if host.conns[index].buf.len() > MAX_CLIENT_LINE {
                    drop_conn = true;
                }
                break;
            };
            let Ok(text) = String::from_utf8(line) else {
                drop_conn = true;
                break;
            };
            drop_conn = !handle_client_line(host, index, &text, now, rng);
        }
        if !drop_conn {
            drop_conn = !pending_deadline(host, index, now);
        }
        if drop_conn {
            host.conns.remove(index);
        }
    }
}

/// One non-blocking read into a connection's buffer; false closes it.
fn read_conn(conn: &mut Conn) -> bool {
    let mut chunk = [0u8; 8192];
    loop {
        match conn.stream.read(&mut chunk) {
            Ok(0) => return false,
            Ok(n) => conn.buf.extend_from_slice(&chunk[..n]),
            Err(e)
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::Interrupted =>
            {
                return true;
            }
            Err(_) => return false,
        }
    }
}

/// Pop one NDJSON line (same framing rules as `transport.feed_ndjson`).
fn pop_line(conn: &mut Conn) -> Option<Vec<u8>> {
    let newline = conn.buf.iter().position(|byte| *byte == b'\n')?;
    let mut line: Vec<u8> = conn.buf.drain(..=newline).collect();
    line.pop();
    if line.last() == Some(&b'\r') {
        line.pop();
    }
    if line.is_empty() {
        return None;
    }
    Some(line)
}

/// The pending verb's deadline discipline (the admin ack's era-advance wait,
/// the lock reply's client deadline); false closes the connection.
fn pending_deadline(host: &mut Host, index: usize, now: u64) -> bool {
    let Some(pending) = host.conns[index].pending.clone() else {
        return true;
    };
    match pending {
        TcpPending::Admin {
            action,
            id,
            endpoint,
            era0,
            slot,
            deadline,
        } => {
            let status = host.node.status();
            let reply = if status.era > era0 {
                // Post-commit dissemination: the leader's membership model
                // takes the committed verb's change, moves to its next
                // generation at the establishing slot, and sends the
                // as-at-new-generation snapshot to the (new) membership.
                if host.model.apply_change(&action, id, &endpoint) {
                    host.model.advance(slot);
                    host.disseminate();
                }
                format!("\"action\":\"{action}\",\"id\":{id},\"accepted\":true")
            } else if now < deadline {
                return true;
            } else {
                format!(
                    "\"action\":\"{action}\",\"id\":{id},\"accepted\":false,\"reason\":\"deadline\""
                )
            };
            let line = format!("{{{reply}}}\n");
            let conn = &mut host.conns[index];
            conn.pending = None;
            let _ = conn.stream.write_all(line.as_bytes());
            let _ = conn.stream.flush();
            true
        }
        TcpPending::Lock { deadline, .. } => now < deadline,
    }
}

fn handle_client_line(host: &mut Host, index: usize, line: &str, now: u64, rng: &mut Rng) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        let _ = host.conns[index]
            .stream
            .write_all(b"{\"error\":\"bad_request\"}\n");
        return true;
    };
    let Some(action) = value["action"].as_str().map(|s| s.to_string()) else {
        // A lock verb: propose locally; a non-leader answers the error the
        // operator (or the run.sh driver) re-routes.
        let Some(message_id_hex) = value["message_id"].as_str() else {
            let _ = host.conns[index]
                .stream
                .write_all(b"{\"error\":\"bad_request\"}\n");
            return true;
        };
        let Some(message_id) = transport::uuid_bytes(message_id_hex) else {
            let _ = host.conns[index]
                .stream
                .write_all(b"{\"error\":\"bad_request\"}\n");
            return true;
        };
        let rc = host.node.request(line.as_bytes());
        host.flush_outputs(now, rng);
        if rc == OK {
            host.conns[index].pending = Some(TcpPending::Lock {
                message_id,
                deadline: now + 30000,
            });
            return true;
        }
        let reply = if rc == NOT_LEADER {
            "{\"error\":\"not_leader\"}".to_string()
        } else {
            format!("{{\"error\":\"rejected\",\"code\":{rc}}}")
        };
        let _ = host.conns[index].stream.write_all(reply.as_bytes());
        let _ = host.conns[index].stream.write_all(b"\n");
        let _ = host.conns[index].stream.flush();
        return true;
    };
    // An admin verb: leader-only in this host; the run.sh driver retries the
    // next replica until one accepts.
    let op = match action.as_str() {
        "join" => RECONFIGURE_JOIN,
        "increment" => RECONFIGURE_INCREMENT,
        "leave" => RECONFIGURE_LEAVE,
        "decrement" => RECONFIGURE_DECREMENT,
        _ => {
            let _ = host.conns[index]
                .stream
                .write_all(b"{\"error\":\"bad_request\"}\n");
            return true;
        }
    };
    let Some(id) = value["id"].as_u64() else {
        let _ = host.conns[index]
            .stream
            .write_all(b"{\"error\":\"bad_request\"}\n");
        return true;
    };
    let id = id as u32;
    let status = host.node.status();
    if status.state != STATE_NORMAL || status.leader != host.own_id {
        let _ = host.conns[index]
            .stream
            .write_all(b"{\"error\":\"not_leader\"}\n");
        return true;
    }
    let rc = host.node.reconfigure(op, id, POSITION_APPEND);
    // The reconfiguration drive's sends are the establishing Prepare(s),
    // all stamped with the entry slot at which the new configuration was
    // chosen; the max send slot is the establishing slot. The drive and
    // the drain run in the same loop slice, so the capture races nothing.
    let establishing_slot = host.flush_outputs(now, rng);
    if rc != OK {
        let reply = format!("{{\"action\":\"{action}\",\"id\":{id},\"accepted\":false}}\n");
        let _ = host.conns[index].stream.write_all(reply.as_bytes());
        let _ = host.conns[index].stream.flush();
        return true;
    }
    let endpoint = value["endpoint"].as_str().unwrap_or_default().to_string();
    host.conns[index].pending = Some(TcpPending::Admin {
        action,
        id,
        endpoint,
        era0: status.era,
        slot: establishing_slot,
        deadline: now + ADMIN_DEADLINE_MS,
    });
    true
}
