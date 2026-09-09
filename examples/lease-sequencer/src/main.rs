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

mod transport;

use lunet_advisory_lock::{
    NOT_LEADER, Node, OK, POSITION_APPEND, RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT,
    RECONFIGURE_JOIN, RECONFIGURE_LEAVE, maybe_invariant,
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

enum TcpPending {
    Lock {
        message_id: [u8; 16],
        deadline: u64,
    },
    Admin {
        action: String,
        id: u32,
        era0: u32,
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
    driver: Driver,
    forwarded_from: HashMap<[u8; 16], (SocketAddr, u64)>,
    conns: Vec<Conn>,
}

fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
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
    heartbeat_ms: u64,
    election_ms: u64,
    recovery_ms: u64,
}

fn parse_options() -> Options {
    let mut options = Options {
        name: String::new(),
        config: String::new(),
        client: String::new(),
        state: String::new(),
        log: String::new(),
        heartbeat_ms: 100,
        election_ms: 1000,
        recovery_ms: 1000,
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
            "--heartbeat-ms" => options.heartbeat_ms = value.parse().unwrap_or(100),
            "--election-ms" => options.election_ms = value.parse().unwrap_or(1000),
            "--recovery-ms" => options.recovery_ms = value.parse().unwrap_or(1000),
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
             --state PATH --log PATH [--heartbeat-ms N] [--election-ms N] [--recovery-ms N]"
        );
        exit(2);
    }
    options
}

impl Host {
    fn note(&self, body: &str) {
        info!("{} ts={}", body, millis());
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

    fn flush_outputs(&mut self, now: u64, rng: &mut Rng) {
        while let Some(out) = self.node.next_output() {
            if out.kind == OUTPUT_SEND {
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
                let packet =
                    transport::encode_peer(transport::PEER_VRR, &self.fingerprint, &out.bytes);
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
    let node =
        Node::open(&members, &options.name, &options.state, None, 0).unwrap_or_else(|code| {
            eprintln!("lease-sequencer: node boot failed with code {code}");
            exit(2);
        });
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
        driver,
        forwarded_from: HashMap::new(),
        conns: Vec::new(),
    };
    host.note(&format!(
        "boot name={} descriptor-id={own_desc_id} own={own_id} incarnation={incarnation}",
        options.name
    ));
    host.note(&format!("membership fingerprint={}", host.fingerprint));
    let now = millis();
    let mut rng = Rng::new(millis() ^ (own_desc_id as u64) ^ (std::process::id() as u64));
    host.flush_outputs(now, &mut rng);

    loop {
        let now = millis();
        pump_udp(&mut host, now, &mut rng);
        pump_tcp(&mut host, now, &mut rng);
        timers(&mut host, now, &mut rng);
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
    }
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
        host.note(&format!(
            "status state={} leader={} era={} view={}",
            status.state, status.leader, status.era, status.view
        ));
    }
    if status.state == STATE_RECOVERING
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
        // deployment's genesis fingerprint.
        host.note("dirty-fingerprint; datagram dropped");
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
    let Some(pending) = &host.conns[index].pending else {
        return true;
    };
    match pending {
        TcpPending::Admin {
            action,
            id,
            era0,
            deadline,
        } => {
            let status = host.node.status();
            let reply = if status.era > *era0 {
                format!("\"action\":\"{action}\",\"id\":{id},\"accepted\":true")
            } else if now < *deadline {
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
        TcpPending::Lock { deadline, .. } => now < *deadline,
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
    host.flush_outputs(now, rng);
    if rc != OK {
        let reply = format!("{{\"action\":\"{action}\",\"id\":{id},\"accepted\":false}}\n");
        let _ = host.conns[index].stream.write_all(reply.as_bytes());
        let _ = host.conns[index].stream.flush();
        return true;
    }
    host.conns[index].pending = Some(TcpPending::Admin {
        action,
        id,
        era0: status.era,
        deadline: now + ADMIN_DEADLINE_MS,
    });
    true
}
