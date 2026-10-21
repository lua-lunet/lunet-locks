mod membership;
use lease_sequencer::rejoin;
pub use lease_sequencer::timeouts;
use lease_sequencer::timeouts::Rng;
pub mod telemetry;
mod transport;

use lease_sequencer::embedded_client::{self, Action, Runner};
use lease_sequencer::relay;
use lunet_advisory_lock::{
    NOT_LEADER, Node, OK, POSITION_APPEND, RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT,
    RECONFIGURE_JOIN, RECONFIGURE_LEAVE, RecoveryFlush, invariant,
};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::process::exit;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tbio::envelope::{Marker, Record, local_ns};
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use vrr::ids::NodeId;

const LOCK_ID: u64 = 0x0DDBA11;
const EMBEDDED_LOCK_ID: u64 = 0x0DDBA12;
const EMBEDDED_CLIENT_ID_BASE: u64 = 800_000;
const LEASE_MS: u64 = 500;
const RENEW_MARGIN_MS: u64 = 250;
const POLL_JITTER_MS: u64 = 100;
const OP_DEADLINE_MS: u64 = 1000;
const ADMIN_DEADLINE_MS: u64 = 10000;
const TICK_MS: u64 = 5;
const JOURNAL_ROLL_BYTES: u32 = 64 * 1024 * 1024;
const LEADER_UNKNOWN: u32 = u32::MAX;
const MAX_CLIENT_LINE: usize = 65000;

const OUTPUT_SEND: u32 = 1;
const OUTPUT_REPLY: u32 = 2;
const STATE_NORMAL: u32 = 0;
const STATE_VIEW_CHANGE: u32 = 1;
const STATE_RECOVERING: u32 = 2;
const STATE_JOINING: u32 = 4;

const DETECTOR: &str = "sloppy-timeout";

#[derive(Clone, Debug)]
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
        proposal_era: u32,
        establishing_slot: u64,
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
    recovery_ms: u64,
    stagger_ms: u64,
    last_heartbeat: u64,
    leader_elapsed: u64,
    last_recovery: u64,
    last_gossip: u64,
    last_status_note: u64,
    last_seen_leader: u32,
    reincarnated: bool,
    driver: Driver,
    forwarded_from: HashMap<[u8; 16], (SocketAddr, u64)>,
    late_acks: u64,
    conns: Vec<Conn>,
    model: membership::Model,
    sidecar: membership::SidecarWriter,
    discovery: Discovery,
    last_leader_commit_ms: u64,
    frontier_slot: u64,
    relay: relay::Ledger,
    detected_key: Option<(u32, u32)>,
    timedout: timeouts::TimeoutToggle,
    viewchange: timeouts::ViewChangeTimer,
    leader_timeout: timeouts::LeaderTimeout,
    last_state: u32,
    last_weight: Option<u32>,
    election_wait_armed: u64,
    telemetry: Option<telemetry::TelemetryLog>,
    embedded: Option<Runner>,
}

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
        .json()
        .flatten_event(true)
        .with_current_span(false)
        .with_span_list(false)
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_ansi(false)
        .with_target(false)
        .without_time()
        .with_writer(writer)
        .init();
    guard
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

const VOTING_MINIMUM: usize = 3;

fn check_voting_minimum(nodes: &[ClusterNode]) -> Result<(), String> {
    let voting = nodes.iter().filter(|node| node.genesis).count();
    if voting < VOTING_MINIMUM {
        return Err(format!(
            "lease-sequencer: the descriptor names {voting} voting members; the minimum is {VOTING_MINIMUM} (a cluster with fewer cannot commit a quorum)"
        ));
    }
    Ok(())
}

fn boot_nodes(nodes: Vec<ClusterNode>, options: &Options) -> Result<Vec<ClusterNode>, String> {
    if nodes.iter().any(|node| node.name == options.name) {
        if options.join_id != 0 || !options.join_endpoint.is_empty() {
            return Err(format!(
                "lease-sequencer: --name {} is in the descriptor; --join-id/--join-endpoint are for a name it omits",
                options.name
            ));
        }
        check_voting_minimum(&nodes)?;
        return Ok(nodes);
    }
    if options.join_id == 0 || options.join_endpoint.is_empty() {
        return Err(format!(
            "lease-sequencer: --name {} is not in the descriptor; supply --join-id N and --join-endpoint HOST:PORT to boot as a joining member",
            options.name
        ));
    }
    let Some((host, port)) = options.join_endpoint.rsplit_once(':') else {
        return Err(
            "lease-sequencer: --join-endpoint must be HOST:PORT (bracket IPv6 host)".to_string(),
        );
    };
    let Ok(port) = port.parse::<u16>() else {
        return Err("lease-sequencer: --join-endpoint port must be u16".to_string());
    };
    if host.is_empty() {
        return Err("lease-sequencer: --join-endpoint host is empty".to_string());
    }
    let mut nodes = nodes;
    nodes.push(ClusterNode {
        id: options.join_id,
        endpoint: options.join_endpoint.clone(),
        genesis: false,
        host: host.to_string(),
        name: options.name.clone(),
        port,
    });
    Ok(nodes)
}

struct Options {
    name: String,
    config: String,
    client: String,
    state: String,
    log: String,
    aof_dir: String,
    aof_flush_ms: u64,
    aof_retention_mib: u64,
    telemetry_aof_dir: String,
    telemetry_rollover_mib: u64,
    leader_timeout_min_ms: u64,
    leader_timeout_max_ms: u64,
    viewchange_timeout_min_ms: u64,
    viewchange_timeout_max_ms: u64,
    recovery_flush: RecoveryFlush,
    recovery_scratch: String,
    heartbeat_ms: u64,
    election_ms: u64,
    recovery_ms: u64,
    embedded_clients: usize,
    embedded_lock_id: u64,
    embedded_client_ttl_ms: u64,
    embedded_renew_fraction: f64,
    join_id: u32,
    join_endpoint: String,
    bench_store: String,
    journal_dir: String,
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
        aof_retention_mib: 10,
        telemetry_aof_dir: String::new(),
        telemetry_rollover_mib: 4,
        leader_timeout_min_ms: 500,
        leader_timeout_max_ms: 1000,
        viewchange_timeout_min_ms: 100,
        viewchange_timeout_max_ms: 200,
        recovery_flush: RecoveryFlush::Diskless,
        recovery_scratch: String::new(),
        heartbeat_ms: 10,
        election_ms: 1000,
        recovery_ms: 1000,
        embedded_clients: 0,
        embedded_lock_id: EMBEDDED_LOCK_ID,
        embedded_client_ttl_ms: 500,
        embedded_renew_fraction: 0.5,
        join_id: 0,
        join_endpoint: String::new(),
        bench_store: String::new(),
        journal_dir: String::new(),
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
            "--aof-retention-mib" => options.aof_retention_mib = value.parse().unwrap_or(10),
            "--telemetry-aof-dir" => options.telemetry_aof_dir = value.clone(),
            "--telemetry-rollover-mib" => {
                options.telemetry_rollover_mib = value.parse().unwrap_or(4)
            }
            "--leader-timeout-min-ms" => {
                options.leader_timeout_min_ms = value.parse().unwrap_or(500)
            }
            "--leader-timeout-max-ms" => {
                options.leader_timeout_max_ms = value.parse().unwrap_or(1000)
            }
            "--viewchange-timeout-min-ms" => {
                options.viewchange_timeout_min_ms = value.parse().unwrap_or(100)
            }
            "--viewchange-timeout-max-ms" => {
                options.viewchange_timeout_max_ms = value.parse().unwrap_or(200)
            }
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
            "--embedded-client" => options.embedded_clients = value.parse().unwrap_or(0),
            "--lock" => options.embedded_lock_id = value.parse().unwrap_or(EMBEDDED_LOCK_ID),
            "--client-ttl-ms" => options.embedded_client_ttl_ms = value.parse().unwrap_or(500),
            "--renew-fraction" => options.embedded_renew_fraction = value.parse().unwrap_or(0.5),
            "--join-id" => options.join_id = value.parse().unwrap_or(0),
            "--join-endpoint" => options.join_endpoint = value.clone(),
            "--bench-store" => options.bench_store = value.clone(),
            "--journal-dir" => options.journal_dir = value.clone(),
            other => {
                eprintln!("lease-sequencer: unknown option {other}");
                exit(2);
            }
        }
        index += 2;
    }
    if options.viewchange_timeout_min_ms > options.viewchange_timeout_max_ms {
        eprintln!(
            "lease-sequencer: --viewchange-timeout-min-ms ({}) exceeds \
             --viewchange-timeout-max-ms ({})",
            options.viewchange_timeout_min_ms, options.viewchange_timeout_max_ms
        );
        exit(2);
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
         [--aof-retention-mib N] [--telemetry-aof-dir PATH] \
         [--telemetry-rollover-mib N] [--leader-timeout-min-ms N] \
         [--leader-timeout-max-ms N] \
         [--viewchange-timeout-min-ms N] [--viewchange-timeout-max-ms N] \
         [--recovery-flush diskless|single|double-ring] [--recovery-scratch-dir PATH] \
         [--heartbeat-ms N] [--election-ms N] [--recovery-ms N] \
         [--embedded-client N] [--lock N] [--client-ttl-ms N] [--renew-fraction F] \
         [--join-id N --join-endpoint HOST:PORT] \
         [--bench-store PATH --journal-dir PATH]"
        );
        exit(2);
    }
    if !options.bench_store.is_empty() {
        if !options.aof_dir.is_empty() || !options.telemetry_aof_dir.is_empty() {
            eprintln!(
                "lease-sequencer: --bench-store excludes --aof-dir and \
                 --telemetry-aof-dir (the journal, the logs, and the flight \
                 tape are the bench's evidence)"
            );
            exit(2);
        }
        if options.recovery_flush != RecoveryFlush::Diskless {
            eprintln!(
                "lease-sequencer: --bench-store excludes --recovery-flush \
                 (the bench store is the force-feed)"
            );
            exit(2);
        }
        if options.journal_dir.is_empty() {
            eprintln!(
                "lease-sequencer: --bench-store requires --journal-dir \
                 (the journal is the bench's CAS chain)"
            );
            exit(2);
        }
    }
    options
}

impl Host {
    fn note(&self, body: &str) {
        info!(ts = millis(), event = "note", text = body);
    }

    fn suspect(&mut self, now: u64, why: &str) {
        if self.node.voting_weight().is_none_or(|weight| weight == 0) {
            return;
        }
        if let Some(record) = self.timedout.on_suspicion(now) {
            self.log_timeout_toggle(&record, why);
        }
    }

    fn log_timeout_toggle(&mut self, record: &timeouts::ToggleRecord, why: &str) {
        let status = self.node.status();
        info!(
            ts = millis(),
            event = "timeout-toggle",
            node = self.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            timedout = record.timedout,
            toggle_ts = record.at_ms,
            last_toggle = record.previous_ms,
            why,
            "timeout toggle"
        );
        self.node
            .note_timeout_toggle(record.timedout, record.at_ms, record.previous_ms);
    }

    fn record_telemetry(&mut self, record: Record) {
        if let Some(log) = self.telemetry.as_mut() {
            log.record(record);
        }
    }

    fn record_state_transition(&mut self, fields: String) {
        let ns = local_ns();
        self.record_telemetry(Record::telemetry(
            Marker::TelemetryStateTransition,
            ns,
            fields.as_bytes(),
        ));
    }

    fn record_outbound(&mut self, out: &lunet_advisory_lock::NodeOutput) {
        let json = format!(
            "{{\"to\":{},\"era\":{},\"view\":{},\"slot\":{},\"bytes\":{}}}",
            out.to,
            out.era,
            out.view,
            out.slot,
            out.bytes.len()
        );
        let ns = local_ns();
        self.record_telemetry(Record::telemetry(
            Marker::TelemetryOutbound,
            ns,
            json.as_bytes(),
        ));
    }

    fn election_wait(&self) -> u64 {
        self.election_wait_armed
    }

    fn rearm_election_wait(&mut self, _now: u64, unit: f64) {
        let (min_ms, max_ms) = (self.leader_timeout.min_ms(), self.leader_timeout.max_ms());
        let wait = timeouts::random_wait_ms(min_ms, max_ms, unit);
        if wait != self.election_wait_armed {
            let previous = self.election_wait_armed;
            self.election_wait_armed = wait;
            let status = self.node.status();
            info!(
                ts = millis(),
                event = "timeout-wait",
                node = self.own_id,
                era = status.era,
                view = status.view,
                leader = status.leader,
                prev_wait_ms = previous,
                next_wait_ms = wait,
                min_ms = min_ms,
                max_ms = max_ms,
                "timeout-wait"
            );
            let json = format!(
                "{{\"leader\":{},\"era\":{},\"view\":{},\
                 \"prev_wait_ms\":{previous},\"next_wait_ms\":{wait},\
                 \"min_ms\":{min_ms},\"max_ms\":{max_ms}}}",
                status.leader, status.era, status.view
            );
            let ns = local_ns();
            self.record_telemetry(Record::telemetry(
                Marker::TelemetryTimeoutDecision,
                ns,
                json.as_bytes(),
            ));
        }
    }

    fn leader_timeout_step(&mut self, now: u64, rng: &mut Rng) {
        if self.timedout.timed_out() {
            return;
        }
        let status = self.node.status();
        if status.config_era != status.era {
            return;
        }
        if status.leader == self.own_id || status.leader == LEADER_UNKNOWN {
            return;
        }
        let Some(&addr) = self.peers.get(&status.leader) else {
            return;
        };
        let watched = (status.config_era, status.leader);
        if self.leader_timeout.watched() != Some(watched) {
            self.leader_timeout.watch(watched, now, rng.unit());
        }
        if !self.leader_timeout.due(now) {
            return;
        }
        let latched = self.detected_key == Some(watched) && status.state == STATE_NORMAL;
        if latched {
            return;
        }
        self.detected_key = Some(watched);
        let silence = now.saturating_sub(self.leader_timeout.last_evidence_ms());
        info!(
            ts = millis(),
            event = "leader-timeout-detect",
            node = self.own_id,
            era = status.era,
            view = status.view,
            config_era = status.config_era,
            leader = status.leader,
            state = status.state_name(),
            silence_ms = silence,
            deadline_ms = self.leader_timeout.deadline_ms(),
            addr = timeouts::addr_text(addr),
            "leader-timeout-detect"
        );
        let drives = self.node.voting_weight().is_some_and(|weight| weight > 0);
        if drives {
            let forced = self.node.force_view(status.era, status.view + 1);
            if forced != 0 {
                let _ = self.node.leader_timeout();
            }
            self.suspect(now, "leader-timeout");
        }
        self.flush_outputs(now, rng);
    }

    fn on_leader_commit(
        &mut self,
        replica: u32,
        header: Option<transport::VrrHeader>,
        now: u64,
        rng: &mut Rng,
    ) {
        let status = self.node.status();
        let Some(header) = header.filter(|h| h.tag == transport::COMMIT_TAG) else {
            return;
        };
        if status.leader != replica {
            return;
        }
        info!(
            ts = millis(),
            event = "commit-in",
            node = self.own_id,
            from = replica,
            era = header.era,
            view = header.view,
            slot = header.slot,
            leader = status.leader,
            state = status.state_name(),
            "commit-in"
        );
        if self.timedout.timed_out() {
            if let Some(record) = self.timedout.on_commit(now) {
                self.log_timeout_toggle(&record, "fresh-commit");
                self.detected_key = None;
                self.leader_timeout.rearm(now, rng.unit());
                self.rearm_election_wait(now, rng.unit());
            }
            return;
        }
        self.leader_timeout.rearm(now, rng.unit());
    }

    fn relay_out(&mut self, datagram: &relay::Datagram, why: &str) -> bool {
        let Some(&addr) = self.peers.get(&datagram.to) else {
            return false;
        };
        let packet =
            transport::encode_peer(transport::PEER_VRR, &self.fingerprint, &datagram.bytes);
        let _ = self.sock.send_to(&packet, addr);
        info!(
            ts = millis(),
            event = "relay-out",
            node = self.own_id,
            to = datagram.to,
            era = datagram.era,
            view = datagram.view,
            slot = datagram.slot,
            tag = datagram.tag.name(),
            bytes = datagram.bytes.len(),
            why,
            unacknowledged = self.relay.unacknowledged(),
            relay_dropped = self.relay.dropped(),
            "relay-out"
        );
        true
    }

    fn cluster_timeout(&mut self, why: &str) -> usize {
        let status = self.node.status();
        let sends = relay::cluster_timeout(status.state, &mut self.relay, status.era, status.view);
        let mut sent = 0usize;
        for datagram in &sends {
            if self.relay_out(datagram, why) {
                sent += 1;
            }
        }
        sent
    }

    fn heartbeat_op(&mut self, now: u64) {
        let status = self.node.status();
        if status.state != STATE_NORMAL
            || status.leader != self.own_id
            || status.config_era != status.era
            || now.saturating_sub(self.last_leader_commit_ms) < self.heartbeat_ms
        {
            return;
        }
        let beat = relay::heartbeat(self.relay.last_commit(status.era, status.view));
        let Some(beat) = beat else {
            return;
        };
        let slot = beat.first().map(|datagram| datagram.slot).unwrap_or(0);
        let era = beat.first().map(|datagram| datagram.era).unwrap_or(0);
        let view = beat.first().map(|datagram| datagram.view).unwrap_or(0);
        let mut peers = 0u32;
        for datagram in &beat {
            if self.relay_out(datagram, "heartbeat") {
                peers += 1;
            }
        }
        if peers == 0 {
            return;
        }
        info!(
            ts = millis(),
            event = "heartbeat-commit",
            node = self.own_id,
            era,
            view,
            leader = status.leader,
            state = status.state_name(),
            slot,
            peers,
            "heartbeat-commit"
        );
    }

    fn record_frontier(&mut self, status: &lunet_advisory_lock::NodeStatus) {
        let json = format!(
            "{{\"event\":\"slot-frontier\",\"era\":{},\"view\":{},\"leader\":{},\
             \"state\":\"{}\",\"slot\":{}}}",
            status.era,
            status.view,
            status.leader,
            status.state_name(),
            self.frontier_slot,
        );
        let ns = local_ns();
        if let Some(log) = self.telemetry.as_mut() {
            log.record_frontier(Record::telemetry(
                Marker::TelemetryStateTransition,
                ns,
                json.as_bytes(),
            ));
        }
    }

    fn lease_attempt(&self, node_id: u32, op: &str, expiry: u64) {
        let status = self.node.status();
        info!(
            ts = millis(),
            event = "lease-attempt",
            node = node_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            op,
            expiry,
            "lease-attempt"
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
        let status = self.node.status();
        info!(
            ts = millis(),
            event = "forward-request",
            node = self.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            peer = %addr,
            message_id = %uuid::Uuid::from_bytes(*message_id),
            bytes = json.len(),
            "forward-request"
        );
        self.send_application(addr, &payload);
    }

    fn send_not_leader(&mut self, addr: SocketAddr, message_id: &[u8; 16], era: u32, view: u32) {
        let mut payload = Vec::with_capacity(1 + 16 + 8);
        payload.push(transport::FORWARD_NOT_LEADER);
        payload.extend_from_slice(message_id);
        payload.extend_from_slice(&era.to_be_bytes());
        payload.extend_from_slice(&view.to_be_bytes());
        let status = self.node.status();
        info!(
            ts = millis(),
            event = "forward-not-leader",
            node = self.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            peer = %addr,
            message_id = %uuid::Uuid::from_bytes(*message_id),
            "forward-not-leader"
        );
        self.send_application(addr, &payload);
    }

    fn send_forward_response(&mut self, addr: SocketAddr, message_id: &[u8; 16], bytes: &[u8]) {
        let mut payload = Vec::with_capacity(1 + 16 + bytes.len());
        payload.push(transport::FORWARD_RESPONSE);
        payload.extend_from_slice(message_id);
        payload.extend_from_slice(bytes);
        let status = self.node.status();
        info!(
            ts = millis(),
            event = "forward-response",
            node = self.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            peer = %addr,
            message_id = %uuid::Uuid::from_bytes(*message_id),
            bytes = bytes.len(),
            "forward-response"
        );
        self.send_application(addr, &payload);
    }

    fn join_gossip(&mut self) {
        let status = self.node.status();
        let payload = rejoin::gossip_datagram(status.era, status.view);
        let packet = transport::encode_peer(transport::PEER_VRR, &self.fingerprint, &payload);
        let mut sent = 0u32;
        for (id, addr) in &self.peers {
            if *id == self.own_id {
                continue;
            }
            let _ = self.sock.send_to(&packet, *addr);
            sent += 1;
        }
        info!(
            ts = millis(),
            event = "join-gossip",
            node = self.own_id,
            era = status.era,
            view = status.view,
            state = status.state_name(),
            peers = sent,
            "join-gossip"
        );
    }

    fn flush_outputs(&mut self, now: u64, rng: &mut Rng) -> u64 {
        let mut established_slot = 0u64;
        while let Some(out) = self.node.next_output() {
            if out.kind == OUTPUT_SEND {
                if out.slot > established_slot {
                    established_slot = out.slot;
                }
                if out.slot > self.frontier_slot {
                    self.frontier_slot = out.slot;
                }
                invariant!(
                    self.peers.contains_key(&out.to),
                    "cannot address replica (to={} kind={} era={} view={} slot={} len={}); \
                     datagram dropped",
                    out.to,
                    out.kind,
                    out.era,
                    out.view,
                    out.slot,
                    out.bytes.len()
                );
                let &addr = self.peers.get(&out.to).unwrap();
                let status = self.node.status();
                let payload = if status.state == STATE_NORMAL
                    && status.leader == self.own_id
                    && transport::vrr_header(&out.bytes)
                        .is_some_and(|h| h.tag == transport::COMMIT_TAG)
                {
                    self.last_leader_commit_ms = now;
                    out.bytes.clone()
                } else {
                    out.bytes.clone()
                };
                let packet =
                    transport::encode_peer(transport::PEER_VRR, &self.fingerprint, &payload);
                let _ = self.sock.send_to(&packet, addr);
                if let Some(header) = transport::vrr_header(&out.bytes)
                    && let Some(tag) = vrr::wire::Tag::from_u32(header.tag)
                {
                    self.relay.record(relay::Datagram {
                        to: out.to,
                        era: header.era,
                        view: header.view,
                        slot: header.slot,
                        tag,
                        bytes: out.bytes.clone(),
                    });
                }
                self.record_outbound(&out);
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
                } else if self.embedded_reply(now, rng, &out.message_id, &out.bytes) {
                } else {
                    let mut claimant = None;
                    for (index, conn) in self.conns.iter_mut().enumerate() {
                        let matches = matches!(
                            conn.pending,
                            Some(TcpPending::Lock { message_id, .. })
                                if message_id == out.message_id
                        );
                        if matches {
                            conn.pending = None;
                            claimant = Some(index);
                            break;
                        }
                    }
                    if let Some(index) = claimant {
                        client_reply(self, index, "lock", &out.bytes);
                    } else {
                        self.late_acks += 1;
                        let status = self.node.status();
                        tracing::warn!(
                            ts = millis(),
                            event = "late-ack",
                            node = self.own_id,
                            era = status.era,
                            view = status.view,
                            leader = status.leader,
                            state = status.state_name(),
                            late_acks = self.late_acks,
                            message_id = %uuid::Uuid::from_bytes(out.message_id),
                            bytes = out.bytes.len(),
                            "committed reply drained with no live conn claimant"
                        );
                    }
                }
            }
        }
        established_slot
    }

    fn learn_member_row(&mut self, addr: SocketAddr) -> bool {
        for member in self.model.members.iter() {
            if let Ok(mut addrs) = member.endpoint.to_socket_addrs()
                && let Some(member_addr) = addrs.next()
                && member_addr == addr
                && !self.peers.contains_key(&member.id)
            {
                self.peers.insert(member.id, addr);
                self.addr_to_id.insert(addr, member.id);
                let status = self.node.status();
                info!(
                    ts = millis(),
                    event = "membership-row-learned",
                    node = self.own_id,
                    era = status.era,
                    view = status.view,
                    id = member.id,
                    endpoint = %member.endpoint,
                    "membership-row-learned"
                );
                return true;
            }
        }
        false
    }

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

    fn adopt(&mut self, snapshot: membership::Snapshot, source: &str) {
        if !self.model.adopt(snapshot.clone()) {
            return;
        }
        self.add_member_rows(&snapshot.members);
        self.sidecar.enqueue(&snapshot);
        let status = self.node.status();
        info!(
            ts = millis(),
            event = "membership-snapshot-adopted",
            node = self.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            model_era = snapshot.era,
            model_slot = snapshot.slot,
            members = snapshot.members.len(),
            source,
            "membership-snapshot-adopted"
        );
    }

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
        let status = self.node.status();
        info!(
            ts = millis(),
            event = "membership-snapshot-disseminated",
            node = self.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            model_era = snapshot.era,
            model_slot = snapshot.slot,
            members = snapshot.members.len(),
            peers = self.peers.len(),
            "membership-snapshot-disseminated"
        );
    }

    fn discovery_step(&mut self, now: u64) {
        if !self.discovery.active {
            return;
        }
        if now >= self.discovery.deadline_ms {
            self.discovery.active = false;
            let status = self.node.status();
            info!(
                ts = millis(),
                event = "discovery-deadline",
                node = self.own_id,
                era = status.era,
                view = status.view,
                model_era = self.discovery.era,
                model_slot = self.discovery.slot,
                "discovery deadline reached; the ordinary fenced boot proceeds"
            );
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
            for addr in &targets {
                let packet =
                    transport::encode_peer(transport::PEER_SNAPSHOT, &self.fingerprint, &payload);
                let _ = self.sock.send_to(&packet, addr);
            }
            let status = self.node.status();
            info!(
                ts = millis(),
                event = "discovery-request",
                node = self.own_id,
                era = status.era,
                view = status.view,
                model_era = self.discovery.era,
                model_slot = self.discovery.slot,
                peers = targets.len(),
                bytes = payload.len(),
                "discovery-request"
            );
        }
    }

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
        let json = lease_sequencer::chase_set_payload(
            &mid,
            LOCK_ID,
            self.driver.client_id,
            self.driver.request_num,
            self.driver.lease_id,
            &self.driver.holder.to_string(),
            LEASE_MS,
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
        let json = lease_sequencer::chase_set_payload(
            &mid,
            LOCK_ID,
            self.driver.client_id,
            self.driver.request_num,
            self.driver.lease_id,
            &self.driver.holder.to_string(),
            LEASE_MS,
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
                    let status = self.node.status();
                    info!(
                        ts = millis(),
                        event = "lease-grant",
                        node = self.own_id,
                        era = status.era,
                        view = status.view,
                        leader = status.leader,
                        state = status.state_name(),
                        op = op.label(),
                        expiry,
                        "lease-grant"
                    );
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
            if self.cluster_timeout("op-deadline") > 0 {
                if let Some(pending) = &mut self.driver.pending {
                    pending.deadline = now + OP_DEADLINE_MS;
                }
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

    fn embedded_reply(
        &mut self,
        now: u64,
        rng: &mut Rng,
        message_id: &[u8; 16],
        bytes: &[u8],
    ) -> bool {
        let Some(mut runner) = self.embedded.take() else {
            return false;
        };
        let absorbed = runner.absorb(now, message_id, bytes, &mut |action| {
            self.submit_embedded(now, rng, action)
        });
        self.embedded = Some(runner);
        absorbed
    }

    fn embedded_not_leader(&mut self, now: u64, message_id: &[u8; 16]) -> bool {
        let Some(mut runner) = self.embedded.take() else {
            return false;
        };
        let dropped = runner.not_leader(now, message_id);
        self.embedded = Some(runner);
        dropped
    }

    fn submit_embedded(&mut self, now: u64, rng: &mut Rng, action: &Action) -> bool {
        let rc = self.node.request(action.request.as_bytes());
        if rc == OK {
            return true;
        }
        self.flush_outputs(now, rng);
        if rc == NOT_LEADER {
            let status = self.node.status();
            if status.leader != LEADER_UNKNOWN
                && status.leader != self.own_id
                && let Some(&addr) = self.peers.get(&status.leader)
            {
                self.send_forward_request(addr, &action.message_id, &action.request);
                return true;
            }
        }
        false
    }
}

struct Lifecycle {
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    caught_term: std::sync::Arc<std::sync::atomic::AtomicBool>,
    caught_int: std::sync::Arc<std::sync::atomic::AtomicBool>,
    caught_quit: std::sync::Arc<std::sync::atomic::AtomicBool>,
    hup: std::sync::Arc<std::sync::atomic::AtomicBool>,
    clean_cycle: std::sync::Arc<std::sync::atomic::AtomicBool>,
    dirty_cycle: std::sync::Arc<std::sync::atomic::AtomicBool>,
}

impl Lifecycle {
    fn register(bench: bool) -> Lifecycle {
        let stopped = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let caught_term = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let caught_int = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let caught_quit = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        for (signal, caught) in [
            (
                signal_hook::consts::SIGTERM,
                std::sync::Arc::clone(&caught_term),
            ),
            (
                signal_hook::consts::SIGINT,
                std::sync::Arc::clone(&caught_int),
            ),
            (
                signal_hook::consts::SIGQUIT,
                std::sync::Arc::clone(&caught_quit),
            ),
        ] {
            signal_hook::flag::register(signal, stopped.clone()).expect("signal flag registration");
            signal_hook::flag::register(signal, caught).expect("signal name registration");
        }
        let hup = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        signal_hook::flag::register(signal_hook::consts::SIGHUP, hup.clone())
            .expect("SIGHUP flag registration");
        let clean_cycle = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let dirty_cycle = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        if bench {
            signal_hook::flag::register(signal_hook::consts::SIGUSR1, clean_cycle.clone())
                .expect("SIGUSR1 flag registration");
            signal_hook::flag::register(signal_hook::consts::SIGUSR2, dirty_cycle.clone())
                .expect("SIGUSR2 flag registration");
        }
        Lifecycle {
            stopped,
            caught_term,
            caught_int,
            caught_quit,
            hup,
            clean_cycle,
            dirty_cycle,
        }
    }
}

enum Serve {
    Exit(i32),
    CleanCycle,
    DirtyCycle,
}

fn main() {
    let options = parse_options();
    let nodes = boot_nodes(parse_config(&options.config), &options).unwrap_or_else(|message| {
        eprintln!("{message}");
        exit(2);
    });

    let mut worker_guard = Some(init_tracing(&options.log));
    let lifecycle = Lifecycle::register(!options.bench_store.is_empty());
    loop {
        match serve(&options, &nodes, &lifecycle) {
            Serve::Exit(code) => {
                drop(worker_guard.take());
                exit(code);
            }
            Serve::CleanCycle => eprintln!(
                "lease-sequencer: bench clean cycle: the stop path ran; re-booting in-process"
            ),
            Serve::DirtyCycle => eprintln!(
                "lease-sequencer: bench dirty cycle: no stop path ran; re-booting in-process"
            ),
        }
    }
}

fn serve(options: &Options, nodes: &[ClusterNode], lifecycle: &Lifecycle) -> Serve {
    let Some(own) = nodes.iter().find(|node| node.name == options.name) else {
        unreachable!("boot_nodes guarantees the own row");
    };
    let own_desc_id = own.id;
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

    let standby = !options.aof_dir.is_empty();
    let telemetry_dir = if options.telemetry_aof_dir.is_empty() {
        options.aof_dir.clone()
    } else {
        options.telemetry_aof_dir.clone()
    };
    let telemetry = if telemetry_dir.is_empty() {
        None
    } else {
        match telemetry::TelemetryLog::open(
            std::path::Path::new(&telemetry_dir),
            options.aof_flush_ms,
            options.telemetry_rollover_mib * 1024 * 1024,
            options.aof_retention_mib * 1024 * 1024,
        ) {
            Ok(log) => {
                eprintln!(
                    "lease-sequencer: telemetry aof active file {}",
                    log.active_path().display()
                );
                Some(log)
            }
            Err(error) => {
                eprintln!(
                    "lease-sequencer: telemetry aof open failed ({error}); \
                     the telemetry stream is disabled for this process"
                );
                None
            }
        }
    };
    let journal_dir = (!options.journal_dir.is_empty()).then_some(options.journal_dir.as_str());
    let node = if !options.bench_store.is_empty() {
        Node::open_bench(
            &members,
            &options.name,
            journal_dir,
            JOURNAL_ROLL_BYTES,
            &options.bench_store,
        )
        .unwrap_or_else(|code| {
            eprintln!("lease-sequencer: bench node boot failed with code {code}");
            exit(2);
        })
    } else if standby {
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
        Node::open(
            &members,
            &options.name,
            &options.state,
            journal_dir,
            JOURNAL_ROLL_BYTES,
            lunet_advisory_lock::PRIMARY_TIMEOUT_MS,
        )
        .unwrap_or_else(|code| {
            eprintln!("lease-sequencer: node boot failed with code {code}");
            exit(2);
        })
    } else {
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
    let incarnation = u64::from(
        NodeId::from(own_id)
            .crash_counter()
            .expect("the announced identity is lawful")
            .get(),
    );

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
    for descriptor in nodes {
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
    let embedded = (options.embedded_clients > 0).then(|| {
        let signals = if options.bench_store.is_empty() {
            embedded_client::Signals::register()
        } else {
            embedded_client::Signals::always_on()
        };
        Runner::new(
            options.embedded_clients,
            embedded_client::Config {
                lock_id: options.embedded_lock_id,
                client_id: EMBEDDED_CLIENT_ID_BASE,
                lease_ms: options.embedded_client_ttl_ms,
                renew_fraction: options.embedded_renew_fraction,
                probe_floor_ms: 0,
            },
            signals,
            OP_DEADLINE_MS,
            millis() ^ (std::process::id() as u64),
        )
    });
    let mut host = Host {
        node,
        sock,
        listener,
        peers,
        addr_to_id,
        fingerprint,
        own_id,
        heartbeat_ms: options.heartbeat_ms,
        recovery_ms: options.recovery_ms,
        stagger_ms: rank * 200,
        last_heartbeat: 0,
        leader_elapsed: 0,
        last_recovery: 0,
        last_gossip: 0,
        last_status_note: 0,
        last_seen_leader: LEADER_UNKNOWN,
        reincarnated: incarnation > 1,
        driver,
        forwarded_from: HashMap::new(),
        late_acks: 0,
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
        last_leader_commit_ms: 0,
        frontier_slot: 0,
        relay: relay::Ledger::new(),
        detected_key: None,
        timedout: timeouts::TimeoutToggle::new(),
        viewchange: timeouts::ViewChangeTimer::new(
            options.viewchange_timeout_min_ms,
            options.viewchange_timeout_max_ms,
        )
        .expect("viewchange bounds validated at parse"),
        leader_timeout: timeouts::LeaderTimeout::new(
            options.leader_timeout_min_ms,
            options.leader_timeout_max_ms,
        ),
        last_state: STATE_RECOVERING,
        last_weight: None,
        election_wait_armed: options.election_ms,
        telemetry,
        embedded,
    };
    {
        let status = host.node.status();
        info!(
            ts = millis(),
            event = "boot",
            node = host.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            name = %options.name,
            descriptor_id = own_desc_id,
            own = own_id,
            incarnation,
            detector = DETECTOR,
            "boot"
        );
        info!(
            ts = millis(),
            event = "membership-fingerprint",
            node = host.own_id,
            era = status.era,
            view = status.view,
            fingerprint = %host.fingerprint,
            "membership-fingerprint"
        );
        info!(
            ts = millis(),
            event = "membership-model",
            node = host.own_id,
            era = status.era,
            view = status.view,
            model_era = host.model.era,
            model_slot = host.model.slot,
            members = host.model.members.len(),
            source = model_source,
            "membership-model"
        );
    }
    {
        let status = host.node.status();
        let decision = if incarnation > 1 {
            "restarting"
        } else if status.state == STATE_RECOVERING {
            "recovering"
        } else {
            "joining"
        };
        host.record_state_transition(format!(
            "{{\"event\":\"boot\",\"decision\":\"{decision}\",\"detector\":\"{DETECTOR}\",\
             \"incarnation\":{incarnation},\
             \"state\":\"{}\",\"leader\":{},\"era\":{},\"view\":{},\"config_era\":{}}}",
            lunet_advisory_lock::replication_state_name(status.state),
            status.leader,
            status.era,
            status.view,
            status.config_era
        ));
    }
    let now = millis();
    let mut rng = Rng::new(millis() ^ (own_desc_id as u64) ^ (std::process::id() as u64));
    host.flush_outputs(now, &mut rng);

    loop {
        if lifecycle
            .hup
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            host.note("sighup: config reload not wired; noop");
        }
        if lifecycle.stopped.load(std::sync::atomic::Ordering::Relaxed) {
            let name = if lifecycle
                .caught_term
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                "sigterm"
            } else if lifecycle
                .caught_int
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                "sigint"
            } else if lifecycle
                .caught_quit
                .swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                "sigquit"
            } else {
                "sigterm"
            };
            host.note(&format!("{name}: clean stop"));
            break;
        }
        if lifecycle
            .clean_cycle
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            host.note("sigusr1: bench clean cycle");
            let code = host.node.stop();
            if code != OK {
                eprintln!("lease-sequencer: bench clean cycle stop failed with code {code}");
            }
            return Serve::CleanCycle;
        }
        if lifecycle
            .dirty_cycle
            .swap(false, std::sync::atomic::Ordering::Relaxed)
        {
            host.note("sigusr2: bench dirty cycle");
            return Serve::DirtyCycle;
        }
        let now = millis();
        pump_udp(&mut host, now, &mut rng);
        pump_tcp(&mut host, now, &mut rng);
        timers(&mut host, now, &mut rng);
        host.discovery_step(now);
        host.driver_step(now, &mut rng);
        embedded_step(&mut host, now, &mut rng);
        host.flush_outputs(now, &mut rng);
        if let Some(log) = host.telemetry.as_mut() {
            log.on_weight(host.node.voting_weight(), now);
            log.tick(now);
        }
        std::thread::sleep(Duration::from_millis(TICK_MS));
    }
    let code = host.node.stop();
    if code != OK {
        eprintln!("lease-sequencer: node stop failed with code {code}");
    }
    if let Some(log) = host.telemetry.as_mut()
        && let Err(error) = log.teardown()
    {
        eprintln!("lease-sequencer: telemetry aof teardown failed ({error})");
    }
    Serve::Exit(0)
}

fn timers(host: &mut Host, now: u64, rng: &mut Rng) {
    let status = host.node.status();
    if status.leader != host.last_seen_leader {
        host.last_seen_leader = status.leader;
        info!(
            ts = millis(),
            event = "leader",
            node = host.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            frontier_slot = host.frontier_slot,
            "leader"
        );
        host.rearm_election_wait(now, rng.unit());
    }
    if now.saturating_sub(host.last_heartbeat) >= host.heartbeat_ms {
        host.last_heartbeat = now;
        let _ = host.node.idle();
        host.flush_outputs(now, rng);
        host.heartbeat_op(now);
    }
    if status.state == STATE_VIEW_CHANGE {
        host.suspect(now, "view-change");
    }
    host.leader_timeout_step(now, rng);
    if host.timedout.timed_out() {
        if !host.viewchange.armed() {
            host.viewchange.arm(now, rng.unit());
        } else if host.viewchange.due(now) {
            let fired_ms = host.viewchange.deadline_ms();
            host.viewchange.arm(now, rng.unit());
            let opinion = relay::poll_opinion(status.state);
            let relayed = match status.state {
                STATE_VIEW_CHANGE => {
                    let sends = relay::cluster_timeout(
                        status.state,
                        &mut host.relay,
                        status.era,
                        status.view,
                    );
                    let mut sent = 0u32;
                    for datagram in &sends {
                        if host.relay_out(datagram, "viewchange-poll") {
                            sent += 1;
                        }
                    }
                    sent
                }
                _ => 0,
            };
            if status.state != STATE_VIEW_CHANGE
                || !matches!(opinion, vrr::timeout::Opinion::Retransmit)
            {
                let _ = host.node.leader_timeout();
            }
            info!(
                ts = millis(),
                event = "timeout-poll",
                node = host.own_id,
                era = status.era,
                view = status.view,
                leader = status.leader,
                state = status.state_name(),
                opinion = opinion.name(),
                relayed,
                fired_ms,
                next_deadline_ms = host.viewchange.deadline_ms(),
                "timeout-poll"
            );
            host.flush_outputs(now, rng);
        }
    } else {
        host.viewchange.disarm();
    }
    if status.state == STATE_NORMAL && status.leader == host.own_id {
        host.leader_elapsed = 0;
    } else {
        host.leader_elapsed += TICK_MS;
        if !host.timedout.timed_out() {
            let wait = host.election_wait();
            if host.leader_elapsed >= wait + host.stagger_ms {
                let fired_ms = host.leader_elapsed;
                host.leader_elapsed = 0;
                let _ = host.node.leader_timeout();
                host.flush_outputs(now, rng);
                host.suspect(now, "election-wait");
                info!(
                    ts = millis(),
                    event = "election-wait-fire",
                    node = host.own_id,
                    era = status.era,
                    view = status.view,
                    leader = status.leader,
                    state = status.state_name(),
                    wait_ms = wait,
                    stagger_ms = host.stagger_ms,
                    fired_ms,
                    "election-wait-fire"
                );
            }
        }
    }
    if status.state != host.last_state {
        host.last_state = status.state;
        host.record_state_transition(format!(
            "{{\"event\":\"state\",\"state\":\"{}\",\"leader\":{},\"era\":{},\"view\":{},\
             \"config_era\":{}}}",
            lunet_advisory_lock::replication_state_name(status.state),
            status.leader,
            status.era,
            status.view,
            status.config_era
        ));
    }
    let weight_now = host.node.voting_weight();
    if weight_now != host.last_weight {
        host.last_weight = weight_now;
        host.record_state_transition(format!(
            "{{\"event\":\"weight\",\"weight\":{},\"era\":{},\"view\":{}}}",
            weight_now
                .map(|w| w.to_string())
                .unwrap_or_else(|| "null".into()),
            status.era,
            status.view
        ));
    }
    if now.saturating_sub(host.last_status_note) >= 2000 {
        host.last_status_note = now;
        let voting = u32::from(host.node.voting_weight().unwrap_or(0) > 0);
        info!(
            ts = millis(),
            event = "status",
            node = host.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            config_era = status.config_era,
            frontier_slot = host.frontier_slot,
            voting,
            sidecar_drops = host.sidecar.drops(),
            "status"
        );
        host.record_frontier(&status);
    }
    if (status.state == STATE_RECOVERING
        || (host.reincarnated && host.node.voting_weight().is_none_or(|weight| weight == 0)))
        && now.saturating_sub(host.last_recovery) >= host.recovery_ms
    {
        host.last_recovery = now;
        let _ = host.node.recover();
        host.flush_outputs(now, rng);
    }
    let unseated_reincarnation =
        host.reincarnated && host.node.voting_weight().is_none_or(|weight| weight == 0);
    if (status.state == STATE_JOINING || unseated_reincarnation)
        && now.saturating_sub(host.last_gossip) >= rejoin::GOSSIP_RESEND_MS
    {
        host.last_gossip = now;
        host.join_gossip();
    }
}

fn embedded_step(host: &mut Host, now: u64, rng: &mut Rng) {
    let Some(mut runner) = host.embedded.take() else {
        return;
    };
    let expired = runner.tick(now, &mut |action| host.submit_embedded(now, rng, action));
    host.embedded = Some(runner);
    if expired > 0 {
        host.cluster_timeout("embedded-deadline");
    }
}

fn pump_udp(host: &mut Host, now: u64, rng: &mut Rng) {
    let mut buf = [0u8; 65507];
    loop {
        let Ok((len, addr)) = host.sock.recv_from(&mut buf) else {
            return;
        };
        let Some(&replica) = host.addr_to_id.get(&addr) else {
            if !host.learn_member_row(addr) {
                tracing::warn!(source = %addr, len, "datagram from an unregistered endpoint dropped");
                continue;
            }
            let Some(&replica) = host.addr_to_id.get(&addr) else {
                tracing::warn!(source = %addr, len, "datagram from an unregistered endpoint dropped");
                continue;
            };
            handle_packet(host, replica, addr, &buf[..len], now, rng);
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
        host.note("dirty-fingerprint; datagram dropped");
        return;
    }
    if kind == transport::PEER_SNAPSHOT {
        handle_snapshot_packet(host, addr, payload);
        return;
    }
    if kind == transport::PEER_VRR {
        if let Some(header) = transport::vrr_header(payload)
            && let Some(tag) = vrr::wire::Tag::from_u32(header.tag)
        {
            host.relay
                .answered(replica, header.era, header.view, header.slot, tag);
        }
        host.on_leader_commit(replica, transport::vrr_header(payload), now, rng);
        if host.telemetry.is_some() {
            host.record_telemetry(Record::wire(local_ns(), payload));
        }
        if let Some((old, new)) = transport::reincarnation_pair(payload)
            && old == replica
            && NodeId::from(old).is_lawful()
            && NodeId::from(old).next_life() == Some(NodeId::from(new))
            && !host.peers.contains_key(&new)
        {
            host.peers.insert(new, addr);
            host.addr_to_id.insert(addr, new);
            let status = host.node.status();
            info!(
                ts = millis(),
                event = "remap",
                node = host.own_id,
                era = status.era,
                view = status.view,
                leader = status.leader,
                state = status.state_name(),
                old,
                new,
                "remap"
            );
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
            if payload.len() > 1 + 16 {
                let mut message_id = [0u8; 16];
                message_id.copy_from_slice(&payload[1..17]);
                let json = &payload[17..];
                let status = host.node.status();
                info!(
                    ts = millis(),
                    event = "forward-request-in",
                    node = host.own_id,
                    era = status.era,
                    view = status.view,
                    leader = status.leader,
                    state = status.state_name(),
                    peer = %addr,
                    message_id = %uuid::Uuid::from_bytes(message_id),
                    bytes = json.len(),
                    "forward-request-in"
                );
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
                let status = host.node.status();
                info!(
                    ts = millis(),
                    event = "forward-response-in",
                    node = host.own_id,
                    era = status.era,
                    view = status.view,
                    leader = status.leader,
                    state = status.state_name(),
                    peer = %addr,
                    message_id = %uuid::Uuid::from_bytes(message_id),
                    bytes = payload.len() - 17,
                    "forward-response-in"
                );
                if host.embedded_reply(now, rng, &message_id, &payload[17..]) {
                } else if let Some(index) = host.conns.iter().position(|conn| {
                    matches!(
                        conn.pending,
                        Some(TcpPending::Lock { message_id: pending_id, .. })
                            if pending_id == message_id
                    )
                }) {
                    host.conns[index].pending = None;
                    client_reply(host, index, "lock", &payload[17..]);
                } else {
                    host.late_acks += 1;
                    let status = host.node.status();
                    tracing::warn!(
                        ts = millis(),
                        event = "late-ack-forward",
                        node = host.own_id,
                        era = status.era,
                        view = status.view,
                        leader = status.leader,
                        state = status.state_name(),
                        late_acks = host.late_acks,
                        message_id = %uuid::Uuid::from_bytes(message_id),
                        len = payload.len(),
                        "late ack for an unclaimed verb drained"
                    );
                }
            }
        }
        0x03 if payload.len() == 1 + 16 + 8 => {
            let mut message_id = [0u8; 16];
            message_id.copy_from_slice(&payload[1..17]);
            let status = host.node.status();
            info!(
                ts = millis(),
                event = "forward-not-leader-in",
                node = host.own_id,
                era = status.era,
                view = status.view,
                leader = status.leader,
                state = status.state_name(),
                peer = %addr,
                message_id = %uuid::Uuid::from_bytes(message_id),
                "forward-not-leader-in"
            );
            if host
                .driver
                .pending
                .as_ref()
                .is_some_and(|p| p.message_id == message_id)
            {
                host.driver.pending = None;
                host.driver.next_action_at = now + rng.below(80) + 20;
            } else if host.embedded_not_leader(now, &message_id) {
            } else if let Some(index) = host.conns.iter().position(|conn| {
                matches!(
                    conn.pending,
                    Some(TcpPending::Lock { message_id: pending_id, .. })
                        if pending_id == message_id
                )
            }) {
                host.conns[index].pending = None;
                client_reply(host, index, "not_leader", b"{\"error\":\"not_leader\"}");
            }
        }
        _ => {}
    }
}

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
    let Some(&responder) = host.addr_to_id.get(&addr) else {
        return;
    };
    if host.discovery.active {
        if membership::newer(snap.era, snap.slot, host.discovery.era, host.discovery.slot) {
            host.discovery.era = snap.era;
            host.discovery.slot = snap.slot;
            host.discovery.tallies.clear();
            host.add_member_rows(&snap.members);
            let status = host.node.status();
            info!(
                ts = millis(),
                event = "discovery-escalate",
                node = host.own_id,
                era = status.era,
                view = status.view,
                responder,
                model_era = snap.era,
                model_slot = snap.slot,
                "discovery escalate: the older-era responses dropped"
            );
        } else if snap.era == host.discovery.era && snap.slot == host.discovery.slot {
            let key = membership::agreement_key(&snap);
            let tally = host.discovery.tallies.entry(key).or_default();
            tally.record(
                responder,
                membership::member_weight(&snap.members, responder),
            );
            let status = host.node.status();
            info!(
                ts = millis(),
                event = "discovery-tally",
                node = host.own_id,
                era = status.era,
                view = status.view,
                responder,
                weight = membership::member_weight(&snap.members, responder),
                model_era = snap.era,
                model_slot = snap.slot,
                members = snap.members.len(),
                "discovery-tally"
            );
            if tally.agrees_with(&snap.members) {
                host.discovery.active = false;
                host.adopt(snap, "discovery");
                info!(
                    ts = millis(),
                    event = "discovery-quorum",
                    node = host.own_id,
                    era = status.era,
                    view = status.view,
                    model_era = host.model.era,
                    model_slot = host.model.slot,
                    "discovery quorum reached"
                );
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

fn client_reply(host: &mut Host, index: usize, outcome: &str, bytes: &[u8]) {
    let status = host.node.status();
    info!(
        ts = millis(),
        event = "client-reply",
        node = host.own_id,
        era = status.era,
        view = status.view,
        leader = status.leader,
        state = status.state_name(),
        outcome,
        bytes = bytes.len(),
        "client-reply"
    );
    let _ = host.conns[index].stream.write_all(bytes);
    let _ = host.conns[index].stream.write_all(b"\n");
    let _ = host.conns[index].stream.flush();
}

fn pending_deadline(host: &mut Host, index: usize, now: u64) -> bool {
    let Some(pending) = host.conns[index].pending.clone() else {
        return true;
    };
    match pending {
        TcpPending::Admin {
            action,
            id,
            endpoint,
            proposal_era,
            establishing_slot,
            deadline,
        } => {
            let status = host.node.status();
            let reply = if status.era > proposal_era {
                if host.model.apply_change(&action, id, &endpoint) {
                    host.model.advance(establishing_slot);
                    host.disseminate();
                }
                format!("\"action\":\"{action}\",\"id\":{id},\"accepted\":true")
            } else if now < deadline {
                return true;
            } else {
                host.cluster_timeout("admin-deadline");
                format!(
                    "\"action\":\"{action}\",\"id\":{id},\"accepted\":false,\"reason\":\"deadline\""
                )
            };
            host.conns[index].pending = None;
            client_reply(host, index, &action, format!("{{{reply}}}").as_bytes());
            true
        }
        TcpPending::Lock { deadline, .. } => {
            if now < deadline {
                return true;
            }
            host.cluster_timeout("lock-deadline");
            false
        }
    }
}

fn handle_client_line(host: &mut Host, index: usize, line: &str, now: u64, rng: &mut Rng) -> bool {
    let Ok(value) = serde_json::from_str::<serde_json::Value>(line) else {
        client_reply(host, index, "bad_request", b"{\"error\":\"bad_request\"}");
        return true;
    };
    let Some(action) = value["action"].as_str().map(|s| s.to_string()) else {
        let Some(message_id_hex) = value["message_id"].as_str() else {
            client_reply(host, index, "bad_request", b"{\"error\":\"bad_request\"}");
            return true;
        };
        let Some(message_id) = transport::uuid_bytes(message_id_hex) else {
            client_reply(host, index, "bad_request", b"{\"error\":\"bad_request\"}");
            return true;
        };
        let status = host.node.status();
        info!(
            ts = millis(),
            event = "client-request",
            node = host.own_id,
            era = status.era,
            view = status.view,
            leader = status.leader,
            state = status.state_name(),
            message_id = %uuid::Uuid::from_bytes(message_id),
            bytes = line.len(),
            verb = value["op"].as_str().unwrap_or("lock"),
            "client-request"
        );
        let rc = host.node.request(line.as_bytes());
        host.flush_outputs(now, rng);
        if rc == OK {
            host.conns[index].pending = Some(TcpPending::Lock {
                message_id,
                deadline: now + 30000,
            });
            return true;
        }
        if rc == NOT_LEADER
            && status.leader != LEADER_UNKNOWN
            && status.leader != host.own_id
            && let Some(&addr) = host.peers.get(&status.leader)
        {
            host.send_forward_request(addr, &message_id, line);
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
        client_reply(host, index, "rejected", reply.as_bytes());
        return true;
    };
    let op = match action.as_str() {
        "join" => RECONFIGURE_JOIN,
        "increment" => RECONFIGURE_INCREMENT,
        "leave" => RECONFIGURE_LEAVE,
        "decrement" => RECONFIGURE_DECREMENT,
        _ => {
            client_reply(host, index, "bad_request", b"{\"error\":\"bad_request\"}");
            return true;
        }
    };
    let Some(id) = value["id"].as_u64() else {
        client_reply(host, index, "bad_request", b"{\"error\":\"bad_request\"}");
        return true;
    };
    let id = id as u32;
    let status = host.node.status();
    info!(
        ts = millis(),
        event = "client-request",
        node = host.own_id,
        era = status.era,
        view = status.view,
        leader = status.leader,
        state = status.state_name(),
        action = %action,
        id,
        bytes = line.len(),
        "client-request"
    );
    if status.state != STATE_NORMAL || status.leader != host.own_id {
        client_reply(host, index, "not_leader", b"{\"error\":\"not_leader\"}");
        return true;
    }
    let rc = host.node.reconfigure(op, id, POSITION_APPEND);
    let establishing_slot = host.flush_outputs(now, rng);
    if rc != OK {
        let reply = format!("{{\"action\":\"{action}\",\"id\":{id},\"accepted\":false}}");
        client_reply(host, index, "rejected", reply.as_bytes());
        return true;
    }
    let endpoint = value["endpoint"].as_str().unwrap_or_default().to_string();
    if action == "join"
        && let Ok(mut addrs) = endpoint.to_socket_addrs()
        && let Some(addr) = addrs.next()
    {
        host.peers.insert(id, addr);
        host.addr_to_id.insert(addr, id);
    }
    host.conns[index].pending = Some(TcpPending::Admin {
        action,
        id,
        endpoint,
        proposal_era: status.era,
        establishing_slot,
        deadline: now + ADMIN_DEADLINE_MS,
    });
    true
}

#[cfg(test)]
mod voting_minimum_tests {

    use super::{ClusterNode, check_voting_minimum};

    fn row(id: u32, name: &str, genesis: bool) -> ClusterNode {
        let port = u16::try_from(7000 + id).expect("the test port fits u16");
        ClusterNode {
            id,
            endpoint: format!("127.0.0.1:{port}"),
            genesis,
            host: "127.0.0.1".to_string(),
            name: name.to_string(),
            port,
        }
    }

    #[test]
    fn two_voting_rows_are_refused_with_the_named_minimum() {
        let nodes = vec![row(1, "n1", true), row(2, "n2", true)];
        let error = check_voting_minimum(&nodes).expect_err("two voters cannot commit");
        assert!(
            error.contains("2 voting members") && error.contains("minimum is 3"),
            "the error names the count and the minimum: {error}"
        );
    }

    #[test]
    fn a_single_voting_row_is_refused() {
        let nodes = vec![row(1, "n1", true)];
        assert!(check_voting_minimum(&nodes).is_err());
    }

    #[test]
    fn three_voting_rows_are_accepted() {
        let nodes = vec![row(1, "n1", true), row(2, "n2", true), row(3, "n3", true)];
        assert_eq!(check_voting_minimum(&nodes), Ok(()));
    }

    #[test]
    fn learners_do_not_count_toward_the_minimum() {
        let nodes = vec![
            row(1, "n1", true),
            row(2, "n2", true),
            row(3, "n3", false),
            row(4, "n4", false),
        ];
        let error =
            check_voting_minimum(&nodes).expect_err("two voters plus learners is two voters");
        assert!(
            error.contains("2 voting members"),
            "the count excludes the weight-0 learners: {error}"
        );
    }

    #[test]
    fn a_singleton_descriptor_is_refused() {
        assert!(check_voting_minimum(&[]).is_err());
    }
}
