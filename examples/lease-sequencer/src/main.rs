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
//!
//! The embedded lock client: launched with `--embedded-client N`, the
//! host runs N contender loops against its own `Node` in-process (the
//! shared `embedded_client` module — the same chase machine the
//! `lease-load` binary drives over the wire, no client→cluster TCP).
//! Every op is submitted through the node's own request path — proposed
//! locally when this node leads, forwarded to the leader over the peer
//! application channel otherwise — so the committed lock transitions
//! are the same Service calls the wire clients' verbs exercise and the
//! AOF records identical evidence. The loops share the host's client
//! gate: SIGUSR1 silences every embedded client (holdership forgotten,
//! in-flight ops abandoned), SIGUSR2 starts them, boot is OFF, and a
//! restarted client re-enters as a NON-holder — its first action is a
//! GET probe, never a blind BUMP.

mod membership;
// The lib crate owns the phi module (its #[no_mangle] C-ABI surface must
// exist in exactly one compilation unit — the lib rlib the bin links).
pub use lease_sequencer::phi;
use lease_sequencer::phi::Rng;
use lease_sequencer::rejoin;
pub mod telemetry;
mod transport;

use lease_sequencer::embedded_client::{self, Action, Runner};
use lunet_advisory_lock::{
    NOT_LEADER, Node, OK, POSITION_APPEND, RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT,
    RECONFIGURE_JOIN, RECONFIGURE_LEAVE, RecoveryFlush, maybe_invariant,
};
use lunet_locks_aof::envelope::{Marker, Record, local_ns};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs, UdpSocket};
use std::process::exit;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::info;
use tracing_appender::non_blocking::WorkerGuard;
use vrr::ids::NodeId;

/// The sequencer lease's sentinel lock id.
const LOCK_ID: u64 = 0x0DDBA11;
/// The embedded lock clients' default chase target (the lease-load
/// binary's default lock).
const EMBEDDED_LOCK_ID: u64 = 0x0DDBA12;
/// The embedded lock clients' client-id base (the lease-load default;
/// client `i` uses `800_000 + i`).
const EMBEDDED_CLIENT_ID_BASE: u64 = 800_000;
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
/// The lock-event journal's roll threshold: a run never rolls (the CAS
/// chain stays one file per node per process lifetime; a bench cycle
/// resumes the same open file).
const JOURNAL_ROLL_BYTES: u32 = 64 * 1024 * 1024;
const LEADER_UNKNOWN: u32 = u32::MAX;
const MAX_CLIENT_LINE: usize = 65000;

const OUTPUT_SEND: u32 = 1;
const OUTPUT_REPLY: u32 = 2;
const STATE_NORMAL: u32 = 0;
/// Inside a view change: the node has issued or joined a view change —
/// the `timedout` toggle holds (`docs/src/phi-and-timeouts.md`).
const STATE_VIEW_CHANGE: u32 = 1;
const STATE_RECOVERING: u32 = 2;
/// The boot fence of a fresh provisioned member (`vrr::progress`
/// `Status::Joining`'s snapshot word): the node has adopted nothing and
/// its own tick emits nothing — the rejoin gossip is the host's drive.
const STATE_JOINING: u32 = 4;

const VRR_COMMIT_TAG: u32 = 4;

/// The compiled-in leader-failure detector, named on the boot line and
/// in the boot trace: the sloppy randomised timeout (the normal build)
/// or the phi-accrual detector (`experimental-phi`).
#[cfg(feature = "experimental-phi")]
const DETECTOR: &str = "experimental-phi";
#[cfg(not(feature = "experimental-phi"))]
const DETECTOR: &str = "sloppy-timeout";

fn is_commit(payload: &[u8]) -> bool {
    payload.len() >= 21
        && u32::from_be_bytes(payload[0..4].try_into().expect("4 bytes")) == VRR_COMMIT_TAG
}

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
    /// The last join-gossip resend (the rejoin gossip's own timer,
    /// `rejoin::GOSSIP_RESEND_MS`): a fenced `Joining` boot gossips its
    /// entry ticket to every peer until the cluster's answer installs.
    last_gossip: u64,
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
    /// Forwarded verbs whose committed ack arrived after this node had
    /// already dropped the claim — by design on the driver's churn gate
    /// (the transition pause drops the pending), by discipline on the op
    /// deadline or a FORWARD_NOT_LEADER refusal, or on the TCP conn's
    /// own deadline. Each late ack is drained and counted here: the
    /// lease lapses and a fresh grant re-acquires it, so the ack's
    /// result is dead on arrival and never a fault.
    late_acks: u64,
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
    /// sketches over the leader's heartbeat Commit arrivals. Compiles
    /// only into the `experimental-phi` build; `None` when phi is
    /// disabled (`--phi-threshold 0`).
    #[cfg(feature = "experimental-phi")]
    phi_monitor: Option<phi::Table>,
    /// The phi policy (threshold, heartbeat, safety multiple, window).
    #[cfg(feature = "experimental-phi")]
    phi_cfg: phi::PhiConfig,
    /// The heartbeat sequence the leader stamps into the experimental
    /// build's trailers.
    #[cfg(feature = "experimental-phi")]
    heartbeat_seq: u32,
    /// The last Commit-send time this leader produced — "when otherwise
    /// idle" is measured against it.
    last_leader_commit_ms: u64,
    /// The keepalive proposer's own client identity and request counter.
    heartbeat_client_id: u64,
    heartbeat_request_num: u64,
    /// The last config era the phi monitor saw (era change = fresh
    /// sketch).
    #[cfg(feature = "experimental-phi")]
    phi_last_era: Option<u32>,
    /// The (config era, leader) the current detection is latched for. A
    /// fresh era or a new leader re-arms; an arriving heartbeat from the
    /// SAME leader does not — one detection per key.
    phi_detected_key: Option<(u32, u32)>,
    /// The currently watched (config era, leader) key and when it became
    /// the watched one: the bootstrap deadline for a sketch that never
    /// learns two intervals. Re-stamped on every leader change.
    #[cfg(feature = "experimental-phi")]
    phi_watch: Option<((u32, u32), u64)>,
    /// The `timedout` toggle (`docs/src/phi-and-timeouts.md`): true from
    /// the moment this node times out on its leader and issues or joins
    /// a view change, until a fresh commit arrives. While it holds, phi
    /// is neither updated nor checked and the cluster viewchange timeout
    /// polls instead.
    timedout: phi::TimeoutToggle,
    /// The cluster viewchange timeout (`docs/src/phi-and-timeouts.md`):
    /// a DIFFERENT timer from the phi timer. While `timedout` holds the
    /// node polls on `min + rand * (max - min)`; a fresh commit disarms
    /// the poll. Validated `min <= max` at parse.
    viewchange: phi::ViewChangeTimer,
    /// The sloppy leader timeout — the NORMAL build's leader-failure
    /// detector (item25.18): a uniform random wait in
    /// `[min, max]` per watched (era, leader) key, re-armed on leader
    /// evidence, firing the §14.2 forced view when due. The
    /// `experimental-phi` build compiles it out (the detector stands in).
    #[cfg(not(feature = "experimental-phi"))]
    sloppy: phi::SloppyLeader,
    /// The last replication state the transition recorder saw; a change
    /// emits a `TelemetryStateTransition` record.
    last_state: u32,
    /// The last voting weight the transition recorder saw; a change emits
    /// a `TelemetryStateTransition` record (the AOF on/off story).
    last_weight: Option<u32>,
    /// The election wait the tick loop currently runs (ms): the
    /// phi-informed estimate (M3), re-derived each tick and re-armed when
    /// it changes.
    election_wait_armed: u64,
    /// The phi-timeout clamp knobs (M3).
    timeout_knobs: telemetry::TimeoutKnobs,
    /// The telemetry AOF (item22): the envelope record layer over the
    /// vendored TigerBeetle AOF, gated by the node's voting weight —
    /// weight 0 (or the boot Recovering/Joining phase) = ON, weight > 0 =
    /// OFF. Carries the boot trace (state transitions + outbound), every
    /// received VRR message as a Wire record while active, and the
    /// phi-informed timeout decisions. `None` when no `--telemetry-aof-dir`
    /// (or `--aof-dir` fallback) is configured, or after an append failure
    /// disabled the stream for the process (telemetry contract: never
    /// poison the replication path).
    telemetry: Option<telemetry::TelemetryLog>,
    /// The embedded lock client runner (item04): N contender loops against
    /// this node's own service, ticked from the host loop behind the
    /// host's SIGUSR1/SIGUSR2 client gate. `None` when launched without
    /// `--embedded-client`.
    embedded: Option<Runner>,
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

/// The phi monitor's construction (`experimental-phi` only): `None` when
/// phi is disabled (`--phi-threshold 0`), otherwise a table with the
/// node's policy knobs.
#[cfg(feature = "experimental-phi")]
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

/// The descriptor is a hint list of where the cluster is, not membership
/// law. A name the descriptor carries boots exactly as before; a name it
/// omits boots anyway as a weight-0 joining member whose identity comes
/// from `--join-id` and `--join-endpoint`, appended after the hint rows.
/// The node stays fenced until the leader's committed configuration
/// carries its row (the join verb); a future PSK, not this file, is the
/// cross-environment boundary.
fn boot_nodes(nodes: Vec<ClusterNode>, options: &Options) -> Result<Vec<ClusterNode>, String> {
    if nodes.iter().any(|node| node.name == options.name) {
        if options.join_id != 0 || !options.join_endpoint.is_empty() {
            return Err(format!(
                "lease-sequencer: --name {} is in the descriptor; --join-id/--join-endpoint are for a name it omits",
                options.name
            ));
        }
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
    /// The AOF series directory. Non-empty turns the node into the standby
    /// telemetry host: the committed-transition hook feeds the async
    /// write-behind writer instead of the blocking journal, and the lease
    /// driver stays idle.
    aof_dir: String,
    /// The AOF's periodic-fsync knob (ms). 0 = only at roll and shutdown.
    aof_flush_ms: u64,
    /// The vendored TigerBeetle AOF's retention threshold, in MiB of
    /// `{epoch}.aof` series on disk (item21). Default 10.
    aof_retention_mib: u64,
    /// The telemetry AOF's series directory (item22). Empty = fall back to
    /// `aof_dir`. Every node can carry one: voting nodes log the boot
    /// trace, then the gate disarms at weight > 0; standbys stay ON.
    telemetry_aof_dir: String,
    /// The telemetry active file's rollover threshold, in MiB (item22).
    /// Default 4; the series keeps exactly the current file + one closed
    /// old.
    telemetry_rollover_mib: u64,
    /// The phi-informed election-wait clamp's floor, in ms (item22 M3).
    phi_timeout_min_ms: u64,
    /// The phi-informed election-wait clamp's ceiling, in ms (item22 M3).
    phi_timeout_max_ms: u64,
    /// The cluster viewchange timeout's floor
    /// (`docs/src/phi-and-timeouts.md`): while a node is timed out on
    /// its leader it polls on `min + rand * (max - min)`. The minimum
    /// must stay above 4x RTT (RTT 20 ms under load -> the 100/200
    /// default); validated `min <= max`.
    viewchange_timeout_min_ms: u64,
    /// The cluster viewchange timeout's ceiling (see the min).
    viewchange_timeout_max_ms: u64,
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
    /// Compiles only into the `experimental-phi` build; the sloppy
    /// timeout needs no threshold.
    #[cfg(feature = "experimental-phi")]
    phi_threshold: f64,
    /// The hard safety multiple: no detection fires before
    /// `safety * heartbeat_ms` of leader silence, whatever phi says.
    #[cfg(feature = "experimental-phi")]
    phi_safety: f64,
    /// The embedded lock client count (item04): N contender loops run
    /// in-process against the node's own service, behind the host's
    /// SIGUSR1/SIGUSR2 client gate. 0 = none (no signal registration).
    embedded_clients: usize,
    /// The lock the embedded clients chase.
    embedded_lock_id: u64,
    /// The embedded clients' lease window (the lease-load default).
    embedded_client_ttl_ms: u64,
    /// The embedded clients' renewal point as a fraction of the window
    /// (the lease-load default).
    embedded_renew_fraction: f64,
    /// The id of a name the descriptor omits: the descriptor is a hint
    /// list of where the cluster is, not membership law, so an absent
    /// name boots as a weight-0 joining member with this identity.
    join_id: u32,
    /// The joining member's endpoint (`[host]:port`), the UDP bind.
    join_endpoint: String,
    /// The bench harness's store control socket
    /// (`docs/src/bench-harness.md`). Non-empty puts the node on the
    /// bench: the lifecycle marker store rides the driver's socket, the
    /// SIGUSR1/SIGUSR2 pair carries the clean/dirty in-process cycles
    /// (the client gate never registers), the build must be the
    /// `experimental-phi` shape (the sloppy timeout is not compiled
    /// in), and the AOF/telemetry options are refused — the journal
    /// (`--journal-dir`), the logs, and the flight tape are the
    /// evidence.
    bench_store: String,
    /// The blocking lock-event journal's directory (the committed-
    /// transition CAS chain). Empty = journaling off, as before.
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
        phi_timeout_min_ms: 500,
        phi_timeout_max_ms: 1000,
        viewchange_timeout_min_ms: 100,
        viewchange_timeout_max_ms: 200,
        recovery_flush: RecoveryFlush::Diskless,
        recovery_scratch: String::new(),
        heartbeat_ms: 10,
        election_ms: 1000,
        recovery_ms: 1000,
        #[cfg(feature = "experimental-phi")]
        phi_threshold: 1.0,
        #[cfg(feature = "experimental-phi")]
        phi_safety: 2.0,
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
            "--phi-timeout-min-ms" => options.phi_timeout_min_ms = value.parse().unwrap_or(500),
            "--phi-timeout-max-ms" => options.phi_timeout_max_ms = value.parse().unwrap_or(1000),
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
            #[cfg(feature = "experimental-phi")]
            "--phi-threshold" => options.phi_threshold = value.parse().unwrap_or(1.0),
            #[cfg(feature = "experimental-phi")]
            "--phi-safety" => options.phi_safety = value.parse().unwrap_or(2.0),
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
    // The cluster viewchange timeout's validation
    // (`docs/src/phi-and-timeouts.md`): the randomized schedule needs a
    // real range, `min <= max`.
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
        #[cfg(feature = "experimental-phi")]
        const USAGE_PHI: &str = "             [--phi-threshold F] [--phi-safety F] \\\n";
        #[cfg(not(feature = "experimental-phi"))]
        const USAGE_PHI: &str = "";
        eprintln!(
            "usage: lease-sequencer --name NAME --config PATH --client IPv4:PORT \
         --state PATH --log PATH [--aof-dir PATH] [--aof-flush-ms N] \
         [--aof-retention-mib N] [--telemetry-aof-dir PATH] \
         [--telemetry-rollover-mib N] [--phi-timeout-min-ms N] [--phi-timeout-max-ms N] \
         [--viewchange-timeout-min-ms N] [--viewchange-timeout-max-ms N] \
         [--recovery-flush diskless|single|double-ring] [--recovery-scratch-dir PATH] \
         [--heartbeat-ms N] [--election-ms N] [--recovery-ms N] \
{USAGE_PHI}\
         [--embedded-client N] [--lock N] [--client-ttl-ms N] [--renew-fraction F] \
         [--join-id N --join-endpoint HOST:PORT] \
         [--bench-store PATH --journal-dir PATH]"
        );
        exit(2);
    }
    if !options.bench_store.is_empty() {
        #[cfg(not(feature = "experimental-phi"))]
        {
            eprintln!(
                "lease-sequencer: --bench-store requires the experimental-phi build \
                 (the bench compiles the sloppy timeout out): \
                 cargo build --features experimental-phi"
            );
            exit(2);
        }
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

/// The marker-5 interval-sample JSON: the arrival-interval sample the
/// exported phi-samples kind plots (one record per learned interval).
/// Rides the `experimental-phi` feature (the trailer-carried evidence).
#[cfg(feature = "experimental-phi")]
fn interval_sample_json(
    own_id: u32,
    trailer: &phi::Trailer,
    addr_text: &str,
    dt_ms: u64,
    now_ms: u64,
    phi: f64,
) -> String {
    format!(
        "{{\"node\":{own_id},\"era\":{},\"leader\":{},\"addr\":\"{addr_text}\",\
         \"dt_ms\":{dt_ms},\"ts_ms\":{now_ms},\"phi\":{phi:.3},\
         \"sent_at_ms\":{}}}",
        trailer.era, trailer.leader, trailer.sent_at_ms
    )
}

impl Host {
    fn note(&self, body: &str) {
        info!("{} ts={}", body, millis());
    }

    /// The node timed out on its leader (`docs/src/phi-and-timeouts.md`):
    /// toggle `timedout=true`. Voters only — a lagging learner's
    /// suspicion is noise, and its polling would wedge its own catch-up.
    /// The toggle record rides BOTH the regular log AND the Flight
    /// Recorder (`timeout-toggle` event, `Node::note_timeout_toggle`).
    fn suspect(&mut self, now: u64, why: &str) {
        if self.node.voting_weight().is_none_or(|weight| weight == 0) {
            return;
        }
        if let Some(record) = self.timedout.on_suspicion(now) {
            self.log_timeout_toggle(&record, why);
        }
    }

    /// One toggle record's logging: the new state, the current ts, and
    /// the ts of the LAST toggle — in the regular log AND the Flight
    /// Recorder (`docs/src/phi-and-timeouts.md`).
    fn log_timeout_toggle(&mut self, record: &phi::ToggleRecord, why: &str) {
        self.note(&format!(
            "timedout={} ts={} last_toggle={} why={why}",
            record.timedout,
            record.at_ms,
            record
                .previous_ms
                .map(|ts| ts.to_string())
                .unwrap_or_else(|| "none".into())
        ));
        self.node
            .note_timeout_toggle(record.timedout, record.at_ms, record.previous_ms);
    }

    /// One telemetry record into the gated AOF (a no-op without a series
    /// or with the gate disarmed — telemetry never touches the
    /// replication path).
    fn record_telemetry(&mut self, record: Record) {
        if let Some(log) = self.telemetry.as_mut() {
            log.record(record);
        }
    }

    /// One `TelemetryStateTransition` record: the boot decisions
    /// (Recovering/Restarting/Joining), the replication state changes,
    /// and the voting-weight moves — the node's own story, with the local
    /// nanosecond clock in the envelope header.
    fn record_state_transition(&mut self, fields: String) {
        let ns = local_ns();
        self.record_telemetry(Record::telemetry(
            Marker::TelemetryStateTransition,
            ns,
            fields.as_bytes(),
        ));
    }

    /// One `TelemetryOutbound` record: what this node decided to send
    /// (peer, era/view/slot, byte length), so the trace shows what the
    /// node decided it was.
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

    /// The phi-informed election wait (M3, `experimental-phi`): the
    /// leader's sketch's learned mean drives `safety * max(heartbeat,
    /// mean)`, clamped to the [min, max] knobs; an unsettled sketch (<2
    /// intervals) falls back to the clamped fixed gate. Every CHANGED
    /// armed wait emits one `TelemetryTimeoutDecision` record (phi, now,
    /// previous wait, next wait) while the AOF gate is active, plus the
    /// tracing note.
    #[cfg(feature = "experimental-phi")]
    fn election_wait(&mut self, now: u64) -> u64 {
        let status = self.node.status();
        let watchable = status.leader != LEADER_UNKNOWN && status.leader != self.own_id;
        let (mean_ms, phi_now) = if watchable {
            // The live sketch, matched on the leader id (see phi_step:
            // the folded configuration era can trail the leader's trailer
            // era on a lagging node — demanding era equality would make
            // every learned sketch look never-observed).
            let sketch = self
                .phi_monitor
                .as_ref()
                .and_then(|m| m.live())
                .filter(|(key, _)| key.leader == status.leader)
                .map(|(_, sketch)| sketch)
                .filter(|sketch| sketch.sample_count() >= 2);
            let mean = sketch.map(|sketch| sketch.mean_interval_ms());
            let phi = sketch.map(|sketch| sketch.phi(now)).unwrap_or(0.0);
            (mean, phi)
        } else {
            (None, 0.0)
        };
        let wait = telemetry::phi_wait_ms(
            mean_ms,
            self.heartbeat_ms,
            self.phi_cfg.safety_multiple,
            &self.timeout_knobs,
        )
        .unwrap_or(self.election_ms);
        if wait != self.election_wait_armed {
            let previous = self.election_wait_armed;
            self.election_wait_armed = wait;
            self.note(&format!(
                "phi-wait leader={} phi={phi_now:.3} prev_wait={previous} next_wait={wait}",
                status.leader
            ));
            let json = format!(
                "{{\"leader\":{},\"era\":{},\"view\":{},\"phi\":{phi_now:.3},\
                 \"mean_interval_ms\":{},\"prev_wait_ms\":{previous},\
                 \"next_wait_ms\":{wait},\"min_ms\":{},\"max_ms\":{}}}",
                status.leader,
                status.era,
                status.view,
                mean_ms
                    .map(|m| format!("{m:.1}"))
                    .unwrap_or_else(|| "null".into()),
                self.timeout_knobs.min_ms,
                self.timeout_knobs.max_ms
            );
            let ns = local_ns();
            self.record_telemetry(Record::telemetry(
                Marker::TelemetryTimeoutDecision,
                ns,
                json.as_bytes(),
            ));
        }
        wait
    }

    /// The sloppy election wait (the normal build): the armed uniform
    /// random in `[min, max]` — re-armed on a leader change and the
    /// fresh-commit resume only (`rearm_election_wait`), stable
    /// otherwise. One `TelemetryTimeoutDecision` record per CHANGED
    /// armed wait (existing behaviour, unchanged); the record's phi
    /// column is 0 — the sloppy timeout computes no phi.
    #[cfg(not(feature = "experimental-phi"))]
    fn election_wait(&mut self, _now: u64) -> u64 {
        self.election_wait_armed
    }

    /// Re-arms the election wait on leader change / fresh-commit (the
    /// normal build's arm points): one uniform random in
    /// `[min, max]`, logged with the note and the timeout-decision
    /// record when the armed wait changed.
    #[cfg(not(feature = "experimental-phi"))]
    fn rearm_election_wait(&mut self, now: u64, unit: f64) {
        let wait = phi::random_wait_ms(self.timeout_knobs.min_ms, self.timeout_knobs.max_ms, unit);
        if wait != self.election_wait_armed {
            let previous = self.election_wait_armed;
            self.election_wait_armed = wait;
            let status = self.node.status();
            self.note(&format!(
                "phi-wait leader={} phi=0.000 prev_wait={previous} next_wait={wait}",
                status.leader
            ));
            let json = format!(
                "{{\"leader\":{},\"era\":{},\"view\":{},\"phi\":0.000,\
                 \"mean_interval_ms\":null,\"prev_wait_ms\":{previous},\
                 \"next_wait_ms\":{wait},\"min_ms\":{},\"max_ms\":{}}}",
                status.leader,
                status.era,
                status.view,
                self.timeout_knobs.min_ms,
                self.timeout_knobs.max_ms
            );
            let ns = local_ns();
            self.record_telemetry(Record::telemetry(
                Marker::TelemetryTimeoutDecision,
                ns,
                json.as_bytes(),
            ));
        }
    }

    /// One heartbeat arrival with a phi trailer (`experimental-phi`):
    /// feed the (era, leader, addr, monitor) sketch, and lazily log the
    /// arrival-interval sample the normal-distribution chart plots.
    #[cfg(feature = "experimental-phi")]
    fn observe_heartbeat(
        &mut self,
        _sender: u32,
        addr: SocketAddr,
        trailer: &phi::Trailer,
        now: u64,
    ) {
        // `docs/src/phi-and-timeouts.md`: while timed out, phi is never
        // updated — and a commit arriving IS the fresh-commit resume.
        // The toggle flips false, the stale sketch resets (the old
        // leader's last commit and this one are not adjacent heartbeats
        // under the same leader — the gap must not enter the window),
        // the bootstrap watch re-stamps for the fresh sketch, and this
        // arrival seeds it.
        if self.timedout.timed_out() {
            if let Some(record) = self.timedout.on_commit(now) {
                self.log_timeout_toggle(&record, "fresh-commit");
                if let Some(monitor) = self.phi_monitor.as_mut() {
                    monitor.reset();
                }
                self.phi_watch = None;
                self.phi_detected_key = None;
            } else {
                return;
            }
        }
        let Some(monitor) = &mut self.phi_monitor else {
            return;
        };
        let addr_text = phi::addr_text(addr);
        let key = phi::SketchKey {
            era: trailer.era,
            leader: trailer.leader,
            leader_addr: addr_text.clone(),
            monitor: self.own_id,
        };
        // The pre-arrival phi: how suspect the leader had become just
        // before this heartbeat proved it alive — the scatter's y over
        // time. Reset by the observe below, so read it first.
        let pre_phi = monitor
            .live()
            .filter(|(live_key, _)| **live_key == key)
            .map(|(_, sketch)| sketch.phi(now))
            .unwrap_or(0.0);
        let interval = monitor.observe(&key, now);
        self.phi_last_era = Some(trailer.era);
        if let Some(interval) = interval {
            self.note(&format!(
                "phi-interval node={} era={} leader={} addr={} dt={}",
                self.own_id, trailer.era, trailer.leader, addr_text, interval
            ));
            // The sampled-estimate evidence (marker 5): the arrival's
            // learned interval AND when it was sampled — the exported
            // phi-samples kind. One record per learned interval.
            self.record_telemetry(Record::telemetry(
                Marker::TelemetryIntervalSample,
                local_ns(),
                interval_sample_json(self.own_id, trailer, &addr_text, interval, now, pre_phi)
                    .as_bytes(),
            ));
        }
    }

    /// One monitor tick (`experimental-phi`): evaluate the current
    /// leader's sketch against the threshold and the safety floor. On a
    /// crossing the host logs the detection and drives the existing
    /// view-change path (`leader_timeout`, the core's ordinary suspicion
    /// input); the core self-gates the actual fence on its own
    /// primary-timeout knob, so the drive is issued, not forced.
    #[cfg(feature = "experimental-phi")]
    fn phi_step(&mut self, now: u64, rng: &mut Rng) {
        if self.phi_monitor.is_none() {
            return;
        }
        // `docs/src/phi-and-timeouts.md`: the phi timer checks `if not
        // timedout` before doing anything — while the toggle holds, phi
        // is neither updated nor checked (phi is never updated for
        // leader-election costs); the cluster viewchange timeout polls
        // instead, and the next tick resumes on the fresh commit.
        if self.timedout.timed_out() {
            return;
        }
        let status = self.node.status();
        // Detection stands down in the establishing-era window: a
        // committed reconfiguration's new era awaits the view that enters
        // it (§8.7.8), and inside that window a forced view restarts the
        // fence instead of letting it close. The driver already holds the
        // client stream for this bounded window; the detector stands down
        // with it and re-arms when the era folds.
        if status.config_era != status.era {
            return;
        }
        if status.leader == self.own_id {
            return;
        }
        if status.leader == LEADER_UNKNOWN {
            return;
        }
        let Some(&addr) = self.peers.get(&status.leader) else {
            return;
        };
        // The live sketch is matched on the LEADER id, whatever era its
        // trailers carry: a lagging node's folded configuration era can
        // trail the leader's view era, and demanding era equality would
        // make every learned sketch look never-observed (the churn the
        // nine-process run surfaced). The live key's (era, leader) pair
        // still watches — a leader change re-keys the table and re-stamps
        // the watch.
        let watched_sketch = self
            .phi_monitor
            .as_ref()
            .and_then(|m| m.live())
            .filter(|(key, _)| key.leader == status.leader)
            .map(|(_, sketch)| sketch);
        let watched_era = self
            .phi_monitor
            .as_ref()
            .and_then(|m| m.live())
            .filter(|(key, _)| key.leader == status.leader)
            .map(|(key, _)| key.era)
            .unwrap_or(status.config_era);
        // Read the sketch's verdict first (immutable borrow ends), then
        // act on it — the drive borrows the node mutably. A sketch that
        // has never learned two intervals — INCLUDING one never observed
        // at all, the dead primary no heartbeat ever reaches — gets the
        // bootstrap verdict: the elected leader's first two heartbeats
        // are due within a few real intervals of election, so
        // `bootstrap_after_ms` of silence past the key's BIRTH is a dead
        // primary no detector math can express yet. The birth stamp is
        // the CURRENT watched key's: a leader change re-stamps it, so a
        // re-elected id can never inherit an old deadline and fire in a
        // loop. The floor stays conservative (half a second, well past
        // any live leader's first-heartbeat lag) so a slow-start primary
        // is never suspected.
        let bootstrap_after = (6 * self.phi_cfg.heartbeat_ms).max(500);
        let watched = (watched_era, status.leader);
        let born = match self.phi_watch {
            Some((held, born)) if held == watched => born,
            _ => {
                self.phi_watch = Some((watched, now));
                now
            }
        };
        let verdict = match watched_sketch.filter(|sketch| sketch.sample_count() >= 2) {
            Some(sketch) => (
                sketch.last_arrival(),
                sketch.phi(now),
                phi::decide(sketch, now, &self.phi_cfg),
            ),
            None => {
                let bootstrapped = now.saturating_sub(born) > bootstrap_after;
                (
                    born,
                    if bootstrapped { f64::INFINITY } else { 0.0 },
                    bootstrapped,
                )
            }
        };
        let (last_arrival, phi_now, fires) = verdict;
        let silence = now.saturating_sub(last_arrival);
        let detected_key = (status.config_era, status.leader);
        // One detection per (era, leader) — but only while THIS node is
        // Normal: inside the limbo (the §14.2 forced view can name a
        // primary that never arrives) the latch must not hold, or the
        // wedged cluster has no driver left and sits in view_change
        // forever. In the limbo the detection re-fires on each bootstrap
        // window until a live primary takes the view.
        let latched = self.phi_detected_key == Some(detected_key) && status.state == STATE_NORMAL;
        if latched || !fires {
            return;
        }
        self.phi_detected_key = Some(detected_key);
        let floor = match watched_sketch {
            Some(sketch) => phi::floor_ms(sketch, &self.phi_cfg) as u64,
            None => (self.phi_cfg.safety_multiple * self.phi_cfg.heartbeat_ms as f64) as u64,
        };
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
        // A node below voting weight (or below its folded configuration's
        // era) does not drive: a lagging learner's verdict is noise (its
        // sketches are degenerate, its leader stream is UnevaluableEra-
        // truncated), and its local view churn wedges its own catch-up —
        // the promotion walk then never completes. The detection note
        // above still lands; only the drive is gated.
        let drives = self.node.voting_weight().is_some_and(|weight| weight > 0);
        if drives {
            let forced = self.node.force_view(status.era, status.view + 1);
            if forced != 0 {
                let _ = self.node.leader_timeout();
            }
            // The phi-actuated view change IS the node timing out on its
            // leader: the toggle flips true, phi stands down, and the
            // cluster viewchange timeout takes the polling
            // (`docs/src/phi-and-timeouts.md`).
            self.suspect(now, "phi-detect");
        }
        self.flush_outputs(now, rng);
    }

    /// One monitor tick — the normal build's sloppy timeout. The
    /// watched (config era, leader) key arms a uniform random deadline
    /// at its birth; every heartbeat Commit arriving from the current
    /// leader re-arms it (`on_leader_commit`), as does the fresh-commit
    /// resume. A due deadline fires the detection: the phi-detect note
    /// (the silence and the armed deadline, no phi value), the §14.2
    /// host-forced view, the `timedout` toggle, and the output flush —
    /// the same actuation the experimental build's phi crossing drives,
    /// a different verdict source.
    #[cfg(not(feature = "experimental-phi"))]
    fn phi_step(&mut self, now: u64, rng: &mut Rng) {
        // `docs/src/phi-and-timeouts.md`: the detector timer checks `if
        // not timedout` before doing anything — while the toggle holds
        // the cluster viewchange timeout polls instead, and the next
        // tick resumes on the fresh commit.
        if self.timedout.timed_out() {
            return;
        }
        let status = self.node.status();
        // Detection stands down in the establishing-era window: a
        // committed reconfiguration's new era awaits the view that enters
        // it (§8.7.8), and inside that window a forced view restarts the
        // fence instead of letting it close. The driver already holds the
        // client stream for this bounded window; the detector stands down
        // with it and re-arms when the era folds.
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
        if self.sloppy.watched() != Some(watched) {
            // The key's birth: a leader change (or a re-keying era)
            // arms a fresh uniform random wait — a re-elected id can
            // never inherit an old deadline and fire in a loop.
            self.sloppy.watch(watched, now, rng.unit());
        }
        if !self.sloppy.due(now) {
            return;
        }
        // One detection per (era, leader) — but only while THIS node is
        // Normal: inside the limbo (the §14.2 forced view can name a
        // primary that never arrives) the latch must not hold, or the
        // wedged cluster has no driver left and sits in view_change
        // forever. In the limbo the detection re-fires on each bootstrap
        // window until a live primary takes the view.
        let latched = self.phi_detected_key == Some(watched) && status.state == STATE_NORMAL;
        if latched {
            return;
        }
        self.phi_detected_key = Some(watched);
        let silence = now.saturating_sub(self.sloppy.last_evidence_ms());
        self.note(&format!(
            "phi-detect node={} era={} leader={} silence={} deadline={} addr={}",
            self.own_id,
            status.config_era,
            status.leader,
            silence,
            self.sloppy.deadline_ms(),
            phi::addr_text(addr)
        ));
        // The sloppy actuation: the §14.2 host-forced view change — the
        // detector's verdict drives it directly, falling back to the
        // ordinary suspicion tick on refusal. A node below voting weight
        // does not drive: a lagging learner's verdict is noise, and its
        // local view churn wedges its own catch-up — the promotion walk
        // then never completes. The detection note above still lands;
        // only the drive is gated.
        let drives = self.node.voting_weight().is_some_and(|weight| weight > 0);
        if drives {
            let forced = self.node.force_view(status.era, status.view + 1);
            if forced != 0 {
                let _ = self.node.leader_timeout();
            }
            // The detection IS the node timing out on its leader: the
            // toggle flips true, the detector stands down, and the
            // cluster viewchange timeout takes the polling
            // (`docs/src/phi-and-timeouts.md`).
            self.suspect(now, "phi-detect");
        }
        self.flush_outputs(now, rng);
    }

    /// One Commit datagram arrived from the replica the host currently
    /// believes is its leader — the NORMAL build's heartbeat evidence
    /// (no trailer rides the wire; the Commit tag at the header's head
    /// is the evidence). Re-arms the sloppy deadline; while the node is
    /// timed out the arrival IS the fresh-commit resume: the toggle
    /// flips false, the detection latch clears, and both waits re-arm.
    #[cfg(not(feature = "experimental-phi"))]
    fn on_leader_commit(&mut self, replica: u32, is_commit: bool, now: u64, rng: &mut Rng) {
        let status = self.node.status();
        if !is_commit || status.leader != replica {
            return;
        }
        if self.timedout.timed_out() {
            if let Some(record) = self.timedout.on_commit(now) {
                self.log_timeout_toggle(&record, "fresh-commit");
                self.phi_detected_key = None;
                self.sloppy.rearm(now, rng.unit());
                self.rearm_election_wait(now, rng.unit());
            }
            return;
        }
        self.sloppy.rearm(now, rng.unit());
    }

    /// The leader's idle heartbeat: when otherwise idle — no Commit left
    /// this node in the last interval — the leader proposes a read-only
    /// `get`, whose commit fan-out emits the heartbeat Commit every
    /// follower's phi sketch observes.
    fn heartbeat_op(&mut self, now: u64, rng: &mut Rng) {
        let status = self.node.status();
        if status.state != STATE_NORMAL
            || status.leader != self.own_id
            || status.config_era != status.era
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

    /// The join gossip's resend (the rejoin gossip's joiner half,
    /// `rejoin`): the entry ticket at the node's current view to every
    /// peer it knows. The datagram rides the ordinary VRR peer channel;
    /// every peer that hears it records this node as a gossip-witness and
    /// the leader answers with the missed-range push the boot fence
    /// qualifies.
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
        self.note(&format!(
            "join-gossip era={} view={} peers={sent}",
            status.era, status.view
        ));
    }

    fn flush_outputs(&mut self, now: u64, rng: &mut Rng) -> u64 {
        let mut established_slot = 0u64;
        while let Some(out) = self.node.next_output() {
            if out.kind == OUTPUT_SEND {
                if out.slot > established_slot {
                    established_slot = out.slot;
                }
                let Some(&addr) = self.peers.get(&out.to) else {
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
                // The phi trailer (item19, `experimental-phi` only) rides
                // only the leader's Commit datagrams: the stream
                // followers' sketches observe. The trailer lives OUTSIDE
                // the core's message bytes — the receiving host strips it
                // before node.receive() — so the core's exact-length wire
                // contract (W3) is untouched. A normal build sends bare
                // core datagrams: no trailer, nothing appended.
                let status = self.node.status();
                let payload = if status.state == STATE_NORMAL
                    && status.leader == self.own_id
                    && is_commit(&out.bytes)
                {
                    self.last_leader_commit_ms = now;
                    #[cfg(feature = "experimental-phi")]
                    {
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
                    }
                    #[cfg(not(feature = "experimental-phi"))]
                    {
                        out.bytes.clone()
                    }
                } else {
                    out.bytes.clone()
                };
                let packet =
                    transport::encode_peer(transport::PEER_VRR, &self.fingerprint, &payload);
                let _ = self.sock.send_to(&packet, addr);
                // The outbound trace (item22): what this node decided it
                // was — one TelemetryOutbound record per sent datagram
                // while the AOF gate is active.
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
                    // An embedded contender's op completed in-process.
                } else {
                    let mut matched = false;
                    for conn in &mut self.conns {
                        let matches = matches!(
                            conn.pending,
                            Some(TcpPending::Lock { message_id, .. })
                                if message_id == out.message_id
                        );
                        if matches {
                            matched = true;
                            conn.pending = None;
                            let _ = conn.stream.write_all(&out.bytes);
                            let _ = conn.stream.write_all(b"\n");
                            let _ = conn.stream.flush();
                            break;
                        }
                    }
                    if !matched {
                        // The maybe: an operation's committed reply reached
                        // the drain with no live claimant (the conn's 30 s
                        // pending expired and its client retried with a
                        // fresh id in the window). Unexpected, not provably
                        // impossible, survivable — reported with full
                        // context, never silent.
                        self.late_acks += 1;
                        tracing::warn!(
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

    /// Learns the addressing row for a member the model already names
    /// when that member's socket speaks for the first time (a changed
    /// leader re-learns a joined standby from the standby's own first
    /// datagram; the standby's identity comes from the model's endpoint
    /// match, never guessed from the wire). `true` when a row was added.
    fn learn_member_row(&mut self, addr: SocketAddr) -> bool {
        for member in self.model.members.iter() {
            if let Ok(mut addrs) = member.endpoint.to_socket_addrs()
                && let Some(member_addr) = addrs.next()
                && member_addr == addr
                && !self.peers.contains_key(&member.id)
            {
                self.peers.insert(member.id, addr);
                self.addr_to_id.insert(addr, member.id);
                self.note(&format!(
                    "member row learned id={} endpoint={}",
                    member.id, member.endpoint
                ));
                return true;
            }
        }
        false
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

    /// Feed one kind-2 reply to the embedded runner: the client whose
    /// in-flight op carries this message id absorbs it (and submits the
    /// free-probe SET race its absorption decides on). `true` when the
    /// runner claimed the reply.
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

    /// A forwarded embedded op's not-leader refusal: the pending op is
    /// dropped and the chase backs off. `true` when the runner owned the
    /// message id.
    fn embedded_not_leader(&mut self, now: u64, message_id: &[u8; 16]) -> bool {
        let Some(mut runner) = self.embedded.take() else {
            return false;
        };
        let dropped = runner.not_leader(now, message_id);
        self.embedded = Some(runner);
        dropped
    }

    /// One embedded action's submission route — the same route the
    /// lease driver's ops take: propose locally as the leader, forward
    /// to the leader over the application channel; anything else is a
    /// refusal the runner absorbs as a backoff-and-reprobe.
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

/// The process's signal flags, registered once and shared across the
/// bench's in-process cycles: SIGTERM/SIGINT/SIGQUIT form the clean-stop
/// catch set — all three flip the one `stopped` flag and process exit
/// through the same drain point — and the per-signal latches record
/// WHICH member fired so the stop-begin record names it. SIGHUP latches
/// its own flag: the serve loop logs it as a no-op and never stops on
/// it. On the bench (`--bench-store`) SIGUSR1 requests the clean
/// shutdown+startup cycle and SIGUSR2 the dirty restart cycle
/// (`docs/src/bench-harness.md`). Off the bench the cycle flags are
/// never registered and the embedded clients' gate owns SIGUSR1/SIGUSR2.
struct Lifecycle {
    stopped: std::sync::Arc<std::sync::atomic::AtomicBool>,
    caught_term: std::sync::Arc<std::sync::atomic::AtomicBool>,
    caught_int: std::sync::Arc<std::sync::atomic::AtomicBool>,
    caught_quit: std::sync::Arc<std::sync::atomic::AtomicBool>,
    /// SIGHUP: the serve loop consumes this flag and carries on. It will
    /// reload the runtime config — the passive witness list (never-voting,
    /// out-of-region, loaded at startup, outside the cluster config) and
    /// the cluster jsonl — but that wiring is a scheduled item; until it
    /// lands every HUP is the logged no-op.
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

/// One `serve` run's ending: the process exits (the SIGTERM/SIGINT/SIGQUIT
/// clean stop), or the bench re-boots in-process — clean (the stop path
/// ran, the store shows the drain-proven marker) or dirty (no stop path,
/// the running sentinel stands).
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

    // The subscriber stack (the binary owns it; the library stays
    // subscriber-free): `RUST_LOG` env-filter, ANSI off, no line timestamp
    // (events carry their own `ts=` fields), through `NonBlocking` over a
    // per-node daily rolling file. The guard is held for the process
    // lifetime and flushes on an orderly shutdown — the Exit arm drops it
    // explicitly, because `exit()` skips the drop and would lose the
    // stop path's final records still sitting in the writer's buffer.
    let mut worker_guard = Some(init_tracing(&options.log));
    let lifecycle = Lifecycle::register(!options.bench_store.is_empty());
    // The bench cycle loop: a SIGUSR1/SIGUSR2 cycle returns from `serve`
    // and the node re-boots in-process — the adapter, its session, and
    // every volatile host state are rebuilt exactly as a process restart
    // rebuilds them; the tracing stack and the signal registrations are
    // the process's and survive. Off the bench `serve` runs once.
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

    let standby = !options.aof_dir.is_empty();
    // The telemetry AOF (item22): the envelope record layer over the
    // vendored TigerBeetle AOF, gated by the node's voting weight. The
    // series directory is `--telemetry-aof-dir`, falling back to
    // `--aof-dir` (the standby's). EVERY node can carry one: the boot
    // Recovering/Restarting/Joining trace always logs; once the node's
    // voting weight exceeds 0 the gate disarms (the flusher stops, the
    // trace pauses), and 1→0 re-arms it. An open failure logs and
    // disables the stream for the process; the node keeps serving (the
    // telemetry contract).
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
        // The bench boot: the lifecycle marker store rides the driver's
        // control socket; the journal is real (the run's CAS chain).
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
        Node::open(
            &members,
            &options.name,
            &options.state,
            journal_dir,
            JOURNAL_ROLL_BYTES,
        )
        .unwrap_or_else(|code| {
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
    // The marker pair vouches for the life: the announced id is the
    // packed identity (MSB system, LSB crash counter), the marker's next
    // life when the boot bumped. The crash counter is the life's number.
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
    // The embedded lock client (item04): N contender loops against this
    // node's own service. Off the bench they run behind the host's client
    // gate and the SIGUSR1/SIGUSR2 flags register only when clients run —
    // a gateless host ignores them. On the bench the SIGUSR pair carries
    // the node's lifecycle, so the clients run ungated (`always_on`) and
    // never listen for it.
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
        election_ms: options.election_ms,
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
        #[cfg(feature = "experimental-phi")]
        phi_monitor: phi_monitor(&options),
        #[cfg(feature = "experimental-phi")]
        phi_cfg: phi::PhiConfig {
            phi_threshold: options.phi_threshold,
            heartbeat_ms: options.heartbeat_ms,
            safety_multiple: options.phi_safety,
            window: 100,
        },
        #[cfg(feature = "experimental-phi")]
        heartbeat_seq: 0,
        last_leader_commit_ms: 0,
        heartbeat_client_id: 0x0BEEF000 + own_desc_id as u64,
        heartbeat_request_num: 0,
        #[cfg(feature = "experimental-phi")]
        phi_last_era: None,
        phi_detected_key: None,
        #[cfg(feature = "experimental-phi")]
        phi_watch: None,
        timedout: phi::TimeoutToggle::new(),
        viewchange: phi::ViewChangeTimer::new(
            options.viewchange_timeout_min_ms,
            options.viewchange_timeout_max_ms,
        )
        .expect("viewchange bounds validated at parse"),
        #[cfg(not(feature = "experimental-phi"))]
        sloppy: phi::SloppyLeader::new(options.phi_timeout_min_ms, options.phi_timeout_max_ms),
        last_state: STATE_RECOVERING,
        last_weight: None,
        election_wait_armed: options.election_ms,
        timeout_knobs: telemetry::TimeoutKnobs {
            min_ms: options.phi_timeout_min_ms,
            max_ms: options.phi_timeout_max_ms,
            fixed_ms: options.election_ms,
        },
        telemetry,
        embedded,
    };
    host.note(&format!(
        "boot name={} descriptor-id={own_desc_id} own={own_id} incarnation={incarnation} \
         detector={DETECTOR}",
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
    // The boot trace (item22): every node's startup decision —
    // Restarting on a dirty restart (incarnation bump), Recovering when
    // the core boots into the recovering state, Joining for a fresh
    // member already Normal — lands in its AOF with the outbound messages
    // that follow while the gate is ON, whatever the node's future
    // weight.
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

    // SIGTERM/SIGINT/SIGQUIT (the clean-stop catch set): the flag flips,
    // the loop exits through the teardown discipline — the
    // boot-marker/teardown record LAST, then the unconditional flush.
    // SIGHUP is a consumed no-op: the flag is read and cleared, the loop
    // carries on. SIGKILL skips all of it and loses the last unflushed
    // window (documented). The bench's SIGUSR1/SIGUSR2 cycle flags ride
    // the same loop top.
    loop {
        if lifecycle.hup.swap(false, std::sync::atomic::Ordering::Relaxed) {
            host.note("sighup: config reload not wired; noop");
        }
        if lifecycle.stopped.load(std::sync::atomic::Ordering::Relaxed) {
            let name = if lifecycle.caught_term.swap(false, std::sync::atomic::Ordering::Relaxed)
            {
                "sigterm"
            } else if lifecycle.caught_int.swap(false, std::sync::atomic::Ordering::Relaxed) {
                "sigint"
            } else if lifecycle.caught_quit.swap(false, std::sync::atomic::Ordering::Relaxed)
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
            // The bench clean cycle: the loop return IS the drain point,
            // exactly as the SIGTERM break — the stop path runs below and
            // the process re-boots in-process from the flushed marker.
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
            // The bench dirty cycle: NO stop path — the adapter is
            // dropped with the running sentinel standing, and the
            // in-process re-boot reincarnates off it (the crash shape,
            // without the process death).
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
        // The AOF lifecycle gate + the 1000 ms forced flusher + the
        // rollover (item22 M2): the gate follows the node's voting
        // weight, the flusher runs only while the AOF is ON.
        if let Some(log) = host.telemetry.as_mut() {
            log.on_weight(host.node.voting_weight(), now);
            log.tick(now);
        }
        std::thread::sleep(Duration::from_millis(TICK_MS));
    }
    // Clean stop (the uVRR termination obligations): the loop break IS
    // the drain point — no inbound pumping happens after it. The node
    // stop closes the wire, writes the `stopped` marker, drains the
    // committed-transition sink to quiescence, and writes `flushed`; the
    // next boot continues under the SAME incarnation (no resurrection).
    // SIGKILL skips all of it: the running sentinel stays behind and the
    // next boot reincarnates — the documented crash shape.
    let code = host.node.stop();
    if code != OK {
        eprintln!("lease-sequencer: node stop failed with code {code}");
    }
    // The telemetry teardown record LAST and the unconditional flush.
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
        host.note(&format!(
            "leader leader={} era={} view={}",
            status.leader, status.era, status.view
        ));
        // The normal build re-arms the election wait on a leader change
        // (the fresh-commit resume re-arms it at `on_leader_commit`).
        #[cfg(not(feature = "experimental-phi"))]
        host.rearm_election_wait(now, rng.unit());
    }
    if now.saturating_sub(host.last_heartbeat) >= host.heartbeat_ms {
        host.last_heartbeat = now;
        let _ = host.node.idle();
        host.flush_outputs(now, rng);
        host.heartbeat_op(now, rng);
    }
    // `docs/src/phi-and-timeouts.md`: a node INSIDE a view change has,
    // by definition, issued or joined one (its own fence, a peer's
    // StartViewChange, or its evidence vote) — the toggle holds until a
    // fresh commit arrives, whatever the entry path was.
    if status.state == STATE_VIEW_CHANGE {
        host.suspect(now, "view-change");
    }
    host.phi_step(now, rng);
    // The cluster viewchange timeout (`docs/src/phi-and-timeouts.md`):
    // a DIFFERENT timer from the phi timer. While the node is timed out
    // on its leader it polls on the randomized schedule
    // `min + rand * (max - min)` — it may be pleasantly surprised when
    // the partition heals and the SAME leader returns, in which case a
    // fresh commit disarms the poll and phi resumes. The poll's drive
    // is `phi::poll_actuation`'s decision: inside the view-change
    // limbo a bare tick cannot advance the attempt (the core's tick
    // suspicion gate admits only `Normal` nodes), so the poll carries
    // the §14.2 host-forced view — a NEW attempt re-broadcasts its
    // fence, the peers join and vote, and a live primary installs.
    if host.timedout.timed_out() {
        if !host.viewchange.armed() {
            host.viewchange.arm(now, rng.unit());
        } else if host.viewchange.due(now) {
            host.viewchange.arm(now, rng.unit());
            match phi::poll_actuation(true, status.state, true) {
                phi::PollActuation::ForceView => {
                    let forced = host.node.force_view(status.era, status.view + 1);
                    if forced != 0 {
                        let _ = host.node.leader_timeout();
                    }
                }
                phi::PollActuation::LeaderTimeout | phi::PollActuation::None => {
                    let _ = host.node.leader_timeout();
                }
            }
            host.flush_outputs(now, rng);
        }
    } else {
        host.viewchange.disarm();
    }
    if status.state == STATE_NORMAL && status.leader == host.own_id {
        host.leader_elapsed = 0;
    } else {
        host.leader_elapsed += TICK_MS;
        // The phi-informed election wait (item22 M3): the host tick loop
        // owns the timeout. It consults the current leader's phi sketch,
        // clamps the derived wait to [min, max] (never earlier than a
        // settled phi allows, never later than the old fixed gate), logs
        // one TelemetryTimeoutDecision record per changed wait, and the
        // drive still happens: leader_timeout, the core's ordinary
        // suspicion input. While the node is timed out the wait stands
        // down: the cluster viewchange timeout polls instead
        // (`docs/src/phi-and-timeouts.md`).
        if !host.timedout.timed_out() {
            let wait = host.election_wait(now);
            if host.leader_elapsed >= wait + host.stagger_ms {
                host.leader_elapsed = 0;
                let _ = host.node.leader_timeout();
                host.flush_outputs(now, rng);
                host.suspect(now, "election-wait");
            }
        }
    }
    // The state-transition trace (item22): Recovering/Restarting/Joining
    // decisions and the voting-weight moves land in the AOF while its
    // gate is active.
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
        // `voting` is the E1 runner's rejoin-serving signal: the
        // reincarnated node is back at voting weight in the folded
        // configuration once the leader's forced reconfiguration walk
        // completes.
        let voting = u32::from(host.node.voting_weight().unwrap_or(0) > 0);
        host.note(&format!(
            "status state={} leader={} era={} view={} config_era={} voting={voting} \
             sidecar_drops={}",
            lunet_advisory_lock::replication_state_name(status.state),
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
    // The rejoin gossip's joiner half (`rejoin`): a fenced `Joining` boot
    // — a fresh provisioned voter whose engine sits at the boot fence —
    // never assumes the cluster will come to it. A bumped boot awaiting
    // its first seat is the same fence shape: the engine boots unseated
    // (voting weight 0) and the cluster is views ahead. Both gosssip the
    // entry ticket to every peer on the resend timer; the leader's
    // answering push is the evidence the boot fence qualifies, so the
    // node catches up and stays a streamed witness until a view change
    // seats it.
    let unseated_reincarnation =
        host.reincarnated && host.node.voting_weight().is_none_or(|weight| weight == 0);
    if (status.state == STATE_JOINING || unseated_reincarnation)
        && now.saturating_sub(host.last_gossip) >= rejoin::GOSSIP_RESEND_MS
    {
        host.last_gossip = now;
        host.join_gossip();
    }
}

/// One host-loop tick of the embedded lock client runner (item04): drain
/// the process signal flags into every embedded client's gate and step
/// each chase — one op in flight at a time, submitted through the node's
/// own request path.
fn embedded_step(host: &mut Host, now: u64, rng: &mut Rng) {
    let Some(mut runner) = host.embedded.take() else {
        return;
    };
    runner.tick(now, &mut |action| host.submit_embedded(now, rng, action));
    host.embedded = Some(runner);
}

fn pump_udp(host: &mut Host, now: u64, rng: &mut Rng) {
    let mut buf = [0u8; 65507];
    loop {
        let Ok((len, addr)) = host.sock.recv_from(&mut buf) else {
            return;
        };
        let Some(&replica) = host.addr_to_id.get(&addr) else {
            // An unregistered endpoint whose socket matches a member the
            // model already names IS that member: learn the row (the join
            // verb's proposal-time row only lives on the leader that
            // accepted the verb — a changed leader re-learns it here from
            // the member's own first datagram, whose fingerprint the
            // transport already validated). The AOF standby never speaks
            // until it is spoken to, so the first learn rides whatever it
            // sent.
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
        // Before anything else: the phi trailer (item19, `experimental-phi`
        // only) rides at the BACK of the leader's heartbeat Commits,
        // entirely OUTSIDE the core's message bytes. Strip it here so the
        // core sees the exact-length message its W3 contract demands, and
        // feed the arrival to the sketch when the sender is the leader
        // this monitor watches. The wire bytes AS THE NETWORK CARRIED
        // THEM, trailer and all: the telemetry record's evidence for the
        // leader's send clock. A NORMAL build strips nothing: bare core
        // datagrams arrive, and the Commit-from-leader arrival is the
        // heartbeat evidence `on_leader_commit` consumes.
        let wire_bytes = payload;
        #[cfg(not(feature = "experimental-phi"))]
        let commit_datagram = is_commit(payload);
        #[cfg(feature = "experimental-phi")]
        let (payload, trailer) = match phi::Trailer::strip_from(payload) {
            Some((front, trailer)) => (front, Some(trailer)),
            None => (payload, None),
        };
        #[cfg(feature = "experimental-phi")]
        if let Some(trailer) = &trailer {
            host.observe_heartbeat(replica, addr, trailer, now);
        }
        #[cfg(not(feature = "experimental-phi"))]
        host.on_leader_commit(replica, commit_datagram, now, rng);
        // The telemetry AOF stream (item22): EVERY VRR datagram this node
        // sees is one `Wire` envelope record — the datagram's VRR payload
        // byte-identical to what the network carried (the phi trailer's
        // sent_at_ms stays in the record: the export's leader-timestamp
        // evidence), with the local nanosecond clock in the envelope
        // header — appended while the lifecycle gate is active (weight 0 /
        // boot phase). An append failure disables the stream for the
        // process and logs once — telemetry must never poison the
        // replication path. The STRIPPED bytes are what the core receives.
        if host.telemetry.is_some() {
            host.record_telemetry(Record::wire(local_ns(), wire_bytes));
        }
        // The reincarnation remap: a `Reincarnation(old, new)` announcement
        // arriving from the socket the learned map attributes to `old` IS
        // the restarted process's entry ticket. The row for the bumped id
        // is ADDED at the source socket and the socket is re-attributed to
        // the bumped id, so THIS datagram and every later one from the
        // same socket are delivered as the new identity — the core's own
        // anti-spoof guard then sees `from == new` and the leader drives
        // the two-era resurrection. The old id's row STAYS: the serving
        // configuration still names it, its sends target the same socket
        // the new identity binds, and the row leaves only when the forced
        // steps evict the old identity.
        if let Some((old, new)) = transport::reincarnation_pair(payload)
            && old == replica
            && NodeId::from(old).is_lawful()
            && NodeId::from(old).next_life() == Some(NodeId::from(new))
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
            // FORWARD_RESPONSE: correlate to the driver's pending op, an
            // embedded contender's, or a forwarded TCP client's. An ack
            // that correlates to nothing is a maybe: unexpected, not
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
                if host.embedded_reply(now, rng, &message_id, &payload[17..]) {
                    // The embedded contender's forwarded op completed.
                } else if let Some(conn) = host.conns.iter_mut().find(|conn| {
                    matches!(
                        conn.pending,
                        Some(TcpPending::Lock { message_id: pending_id, .. })
                            if pending_id == message_id
                    )
                }) {
                    // The forwarded TCP client's op committed on the
                    // leader: one reply line, then the conn is idle.
                    conn.pending = None;
                    let _ = conn.stream.write_all(&payload[17..]);
                    let _ = conn.stream.write_all(b"\n");
                    let _ = conn.stream.flush();
                } else {
                    // The late ack: the verb's claim was already gone —
                    // by design on the driver's churn gate (the
                    // transition pause drops the pending long before the
                    // leader's committed reply can arrive), by
                    // discipline on the op deadline or a
                    // FORWARD_NOT_LEADER refusal, or on the TCP conn's
                    // own deadline. The lease lapses and a fresh grant
                    // re-acquires it, so the ack's result is dead on
                    // arrival: drained and counted, never a fault.
                    host.late_acks += 1;
                    tracing::warn!(
                        message_id = %uuid::Uuid::from_bytes(message_id),
                        len = payload.len(),
                        "late ack for an unclaimed verb drained"
                    );
                }
            }
        }
        // FORWARD_NOT_LEADER: drop the pending op — the driver's or an
        // embedded contender's; the policy retries.
        0x03 if payload.len() == 1 + 16 + 8 => {
            let mut message_id = [0u8; 16];
            message_id.copy_from_slice(&payload[1..17]);
            if host
                .driver
                .pending
                .as_ref()
                .is_some_and(|p| p.message_id == message_id)
            {
                host.driver.pending = None;
                host.driver.next_action_at = now + rng.below(80) + 20;
            } else if host.embedded_not_leader(now, &message_id) {
                // The embedded contender's forwarded op was refused; the
                // chase backs off and re-probes.
            } else if let Some(conn) = host.conns.iter_mut().find(|conn| {
                matches!(
                    conn.pending,
                    Some(TcpPending::Lock { message_id: pending_id, .. })
                        if pending_id == message_id
                )
            }) {
                // The forwarded TCP client's op was refused (the leader
                // stood down mid-flight); the conn answers not_leader and
                // its client retries or rotates.
                conn.pending = None;
                let _ = conn.stream.write_all(b"{\"error\":\"not_leader\"}\n");
                let _ = conn.stream.flush();
            }
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
        // The non-leader route: the op is FORWARDED to the leader over the
        // peer application channel (the same wire the lease driver and the
        // embedded clients use). The leader's committed reply rides back
        // the FORWARD_RESPONSE datagram and this conn answers once. When
        // no leader is known yet (or the forwarding route is unlearned),
        // the error reply stands so the client retries or rotates.
        if rc == NOT_LEADER {
            let status = host.node.status();
            if status.leader != LEADER_UNKNOWN
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
    // The abdication is not a reconfiguration: nothing enters the log and
    // no era advances. The leader drives the abdication through the
    // adapter — the standard view-change emission for v+1 arms and the
    // leader steps down in the same synchronous drive — and answers
    // immediately: the emission is flushed before the ack, and the
    // failover itself is the ordinary view change the successor completes.
    // A non-leader answers not_leader; the driver rotates to the next
    // replica (the same shape every admin verb here has).
    if action == "abdicate" {
        let status = host.node.status();
        if status.state != STATE_NORMAL || status.leader != host.own_id {
            let _ = host.conns[index]
                .stream
                .write_all(b"{\"error\":\"not_leader\"}\n");
            return true;
        }
        let rc = host.node.abdicate();
        host.flush_outputs(now, rng);
        let reply = if rc == OK {
            "{\"action\":\"abdicate\",\"accepted\":true}\n"
        } else {
            "{\"action\":\"abdicate\",\"accepted\":false}\n"
        };
        let _ = host.conns[index].stream.write_all(reply.as_bytes());
        let _ = host.conns[index].stream.flush();
        return true;
    }
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
    // The joining member's addressing row is learned AT PROPOSAL TIME
    // from the verb's own endpoint: without it the leader cannot fan out
    // to the new member at all, and a standby never folds (its datagrams
    // die as unregistered-endpoint drops). Rows grow additively and the
    // snapshot path's dedupe keeps a double-learn harmless.
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
        era0: status.era,
        slot: establishing_slot,
        deadline: now + ADMIN_DEADLINE_MS,
    });
    true
}

#[cfg(test)]
#[cfg(feature = "experimental-phi")]
mod interval_sample_tests {
    use super::*;

    #[test]
    fn sample_json_carries_the_leader_send_clock() {
        let trailer = phi::Trailer {
            era: 4,
            leader: 33,
            seq: 9,
            sent_at_ms: 1_789_214_915_000,
        };
        let json = interval_sample_json(88, &trailer, "127.0.0.1:1", 22, 1_789_214_915_022, 0.5);
        for key in [
            "node",
            "era",
            "leader",
            "addr",
            "dt_ms",
            "ts_ms",
            "phi",
            "sent_at_ms",
        ] {
            assert!(
                json.contains(&format!("\"{key}\"")),
                "missing {key}: {json}"
            );
        }
        assert!(
            json.contains("\"sent_at_ms\":1789214915000"),
            "leader send clock missing: {json}"
        );
    }
}

#[cfg(test)]
#[cfg(test)]
mod forward_tests {
    //! The external TCP client channel's forwarding route: a lock verb
    //! addressed to a NON-LEADER voter's client port is forwarded to the
    //! leader over the peer application channel, and the leader's
    //! committed reply rides FORWARD_RESPONSE back to the same conn. The
    //! regression is the rig's takeover failure: the non-leader answered
    //! `{"error":"not_leader"}` and never forwarded, so external
    //! contenders whose local voter was not the leader never committed a
    //! get or a set — and, when the holder died, no contender could ever
    //! see the lease expire and race for the takeover.

    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

    const LOCK_ID: u64 = 0x0DDBA12;

    static FORWARD_SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_root() -> PathBuf {
        // Test scratch stays inside the repository's `.tmp/` directory;
        // this crate sits two levels below the repository root, and the
        // crate directory is baked in at compile time.
        let repo_tmp = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join(".tmp");
        let seq = FORWARD_SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = repo_tmp.join(format!(
            "lease-sequencer-forward-{}-{}-{seq}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp root");
        dir
    }

    struct NodeHost {
        host: Host,
        udp: SocketAddr,
        client: u16,
    }

    fn boot_host(name: &str, root: &PathBuf) -> NodeHost {
        let state = root.join(format!("{name}.state"));
        // A parallel-test boot race (same-process marker churn) is
        // tolerated by one fresh-root retry; the boot CONFIG error otherwise.
        let node = match Node::open("65537:a\x00131073:b", name, state.to_str().expect("path"), None, 0) {
            Ok(node) => node,
            Err(_) => {
                let root = temp_root();
                let state = root.join(format!("{name}.state"));
                Node::open("65537:a\x00131073:b", name, state.to_str().expect("path"), None, 0)
                    .expect("node boots")
            }
        };
        let own_id = node.own_id();
        let sock = UdpSocket::bind("127.0.0.1:0").expect("udp bind");
        sock.set_nonblocking(true).expect("nonblocking udp");
        let listener = TcpListener::bind("127.0.0.1:0").expect("tcp bind");
        listener.set_nonblocking(true).expect("nonblocking tcp");
        let client_port = listener.local_addr().expect("local").port();
        let udp_addr = sock.local_addr().expect("udp local");
        let rows = vec![
            (65537u32, "127.0.0.1".to_string(), 42901u16, true),
            (131073u32, "127.0.0.1".to_string(), 42902u16, true),
            (196609u32, "127.0.0.1".to_string(), 42903u16, true),
        ];
        let model = membership::Model {
            era: 1,
            slot: 0,
            members: membership::descriptor_model(&rows),
        };
        let sidecar =
            membership::SidecarWriter::open(state.to_str().expect("path")).expect("sidecar opens");
        // Both hosts compute the same genesis fingerprint — the same three
        // facts the byte-identical deployment carries.
        let fingerprint = transport::genesis_fingerprint(&[transport::GenesisMember {
            id: 1,
            name: "a",
            host: "127.0.0.1",
            port: 42901,
        }]);
        let host = Host {
            node,
            sock,
            listener,
            peers: HashMap::new(),
            addr_to_id: HashMap::new(),
            fingerprint,
            own_id,
            heartbeat_ms: 100,
            election_ms: 200,
            recovery_ms: 200,
            stagger_ms: 200,
            last_heartbeat: 0,
            leader_elapsed: 0,
            last_recovery: 0,
            last_gossip: 0,
            last_status_note: 0,
            last_seen_leader: LEADER_UNKNOWN,
            reincarnated: false,
            driver: Driver {
                client_id: 800_000,
                request_num: 0,
                holder: uuid::Uuid::new_v4(),
                lease_id: 0,
                held_expiry: None,
                last_get_foreign: false,
                next_action_at: millis() + 300,
                pending: None,
            },
            forwarded_from: HashMap::new(),
            late_acks: 0,
            conns: Vec::new(),
            model,
            sidecar,
            discovery: Discovery {
                era: 1,
                slot: 0,
                tallies: HashMap::new(),
                deadline_ms: millis() + 15000,
                next_request_ms: 0,
                active: true,
            },
            #[cfg(feature = "experimental-phi")]
            phi_monitor: None,
            #[cfg(feature = "experimental-phi")]
            phi_cfg: phi::PhiConfig {
                phi_threshold: 0.0,
                heartbeat_ms: 100,
                safety_multiple: 2.0,
                window: 100,
            },
            #[cfg(feature = "experimental-phi")]
            heartbeat_seq: 0,
            last_leader_commit_ms: 0,
            heartbeat_client_id: 0x0BEEF000,
            heartbeat_request_num: 0,
            #[cfg(feature = "experimental-phi")]
            phi_last_era: None,
            phi_detected_key: None,
            #[cfg(feature = "experimental-phi")]
            phi_watch: None,
            timedout: phi::TimeoutToggle::new(),
            viewchange: phi::ViewChangeTimer::new(100, 200)
                .expect("the harness's viewchange bounds are valid"),
            #[cfg(not(feature = "experimental-phi"))]
            sloppy: phi::SloppyLeader::new(100, 300),
            last_state: STATE_RECOVERING,
            last_weight: None,
            election_wait_armed: 200,
            timeout_knobs: telemetry::TimeoutKnobs {
                min_ms: 100,
                max_ms: 300,
                fixed_ms: 200,
            },
            telemetry: None,
            embedded: None,
        };
        NodeHost {
            host,
            udp: udp_addr,
            client: client_port,
        }
    }

    /// The harness's scratch root: removed when the guard drops, so a
    /// test's scratch never outlives the test (panic paths included).
    struct ScratchRoot(PathBuf);

    impl Drop for ScratchRoot {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// A settled two-node localhost harness: `a` leads genesis's primary,
    /// `b` follows; both peer rows know each other's real sockets.
    fn harness() -> (NodeHost, NodeHost, Rng, ScratchRoot) {
        let root = temp_root();
        let scratch = ScratchRoot(root.clone());
        let mut a = boot_host("a", &root);
        let mut b = boot_host("b", &root);
        a.host.peers.insert(131073, b.udp);
        a.host.addr_to_id.insert(b.udp, 131073);
        b.host.peers.insert(65537, a.udp);
        b.host.addr_to_id.insert(a.udp, 65537);
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis() as u64;
        let mut rng = Rng::new(seed);
        let deadline = Instant::now() + Duration::from_secs(10);
        while a.host.node.status().state != STATE_NORMAL
            || b.host.node.status().state != STATE_NORMAL
        {
            assert!(
                Instant::now() < deadline,
                "the two-node forward harness never settled"
            );
            tick(&mut a, &mut b, &mut rng);
        }
        (a, b, rng, scratch)
    }

    fn tick(a: &mut NodeHost, b: &mut NodeHost, rng: &mut Rng) {
        let now = millis();
        pump_udp(&mut a.host, now, rng);
        pump_tcp(&mut a.host, now, rng);
        a.host.discovery_step(now);
        a.host.driver_step(now, rng);
        let _ = a.host.node.idle();
        a.host.flush_outputs(now, rng);
        let now = millis();
        pump_udp(&mut b.host, now, rng);
        pump_tcp(&mut b.host, now, rng);
        b.host.discovery_step(now);
        b.host.driver_step(now, rng);
        let _ = b.host.node.idle();
        b.host.flush_outputs(now, rng);
        std::thread::sleep(Duration::from_millis(1));
    }

    /// One TCP round trip from the test's client to the named host (true =
    /// to the follower `b`): the request line out, the first reply line
    /// back, the host loop driven alongside.
    fn round_trip(
        a: &mut NodeHost,
        b: &mut NodeHost,
        rng: &mut Rng,
        to_follower: bool,
        request: &str,
    ) -> String {
        let port = if to_follower { b.client } else { a.client };
        let mut stream = TcpStream::connect(("127.0.0.1", port)).expect("client connects");
        stream
            .set_read_timeout(Some(Duration::from_millis(10)))
            .expect("read timeout");
        let _ = stream.write_all(request.as_bytes());
        let _ = stream.write_all(b"\n");
        let deadline = Instant::now() + Duration::from_secs(30);
        let mut buf: Vec<u8> = Vec::new();
        loop {
            assert!(
                Instant::now() < deadline,
                "no reply line arrived in time (buf={buf:?})"
            );
            tick(a, b, rng);
            let mut chunk = [0u8; 4096];
            match stream.read(&mut chunk) {
                Ok(0) => panic!("the server closed the conn before replying"),
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut
                        || e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => panic!("{e}"),
            }
            if buf.iter().any(|byte| *byte == b'\n') {
                break;
            }
        }
        let end = buf
            .iter()
            .position(|byte| *byte == b'\n')
            .unwrap_or(buf.len());
        String::from_utf8_lossy(&buf[..end]).to_string()
    }

    fn get_request(client_id: u64, request_num: u64) -> String {
        let mid = uuid::Uuid::new_v4();
        format!(
            "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":{client_id},\
             \"request_num\":{request_num},\"lock_id\":{LOCK_ID}}}"
        )
    }

    fn set_request(client_id: u64, request_num: u64) -> String {
        let mid = uuid::Uuid::new_v4();
        let holder = uuid::Uuid::new_v4();
        format!(
            "{{\"op\":\"set\",\"message_id\":\"{mid}\",\"client_id\":{client_id},\
             \"request_num\":{request_num},\"lock_id\":{LOCK_ID},\
             \"lease\":{{\"lease_id\":1,\"holder\":\"{holder}\",\"lease_ms\":500}}}}"
        )
    }

    fn ok_reply(line: &str) -> serde_json::Value {
        let value: serde_json::Value = serde_json::from_str(line.trim_end())
            .unwrap_or_else(|e| panic!("the reply line is JSON ({e}): {line}"));
        assert!(
            value.get("error").is_none(),
            "the reply must not be an error: {line}"
        );
        value
    }

    /// One sequential scenario: the four forward-path cases run against
    /// one fresh harness, in order — the parallel-test interference this
    /// module's harnesses saw as boot/addressing invariants is the reason
    /// the cases do not run as separate concurrent #[test]s.
    #[test]
    fn the_forward_path_end_to_end() {
        one_follower_get_scenario();
        one_follower_set_scenario();
        one_leader_local_scenario();
        one_refusal_scenario();
        one_churn_late_ack_scenario();
    }

    fn one_follower_get_scenario() {
        let (mut a, mut b, mut rng, _scratch) = harness();
        let follower_to_b = b.host.node.status().leader != b.host.own_id;
        assert!(
            follower_to_b,
            "harness shape: b follows (the forward regression's shape)"
        );
        let line = round_trip(&mut a, &mut b, &mut rng, true, &get_request(800_001, 1));
        let reply = ok_reply(&line);
        assert_eq!(reply["op"], "get");
        assert!(
            reply.get("executed_at").is_some(),
            "the committed reply carries the leader's execution tick: {line}"
        );
    }

    fn one_follower_set_scenario() {
        let (mut a, mut b, mut rng, _scratch) = harness();
        let line = round_trip(&mut a, &mut b, &mut rng, true, &set_request(800_006, 1));
        let reply = ok_reply(&line);
        assert_eq!(reply["granted"], true, "the forwarded set grants: {line}");
        assert!(
            reply["lease"]["expiry"].as_u64().is_some(),
            "the grant carries the lease expiry: {line}"
        );
    }

    fn one_leader_local_scenario() {
        let (mut a, mut b, mut rng, _scratch) = harness();
        let line = round_trip(&mut a, &mut b, &mut rng, false, &get_request(800_002, 1));
        ok_reply(&line);
    }

    /// The four-panic regression: a follower's forwarded driver op is
    /// pending when era churn takes the node out of NORMAL and the
    /// driver's churn gate drops the pending (the host's design pause,
    /// `driver_step`) — and the leader's committed FORWARD_RESPONSE
    /// arrives afterwards. The late ack must not abort the node: two
    /// dead voters kill the cluster.
    fn one_churn_late_ack_scenario() {
        let (mut a, mut b, mut rng, _scratch) = harness();
        assert!(
            b.host.node.status().leader != b.host.own_id,
            "harness shape: b follows"
        );
        // One driver op on b: propose (refused NOT_LEADER), forward to
        // the leader, pending armed (the route_op forward path).
        let mid_uuid = uuid::Uuid::new_v4();
        let mid = *mid_uuid.as_bytes();
        let json = format!(
            "{{\"op\":\"get\",\"message_id\":\"{mid_uuid}\",\"client_id\":{},\
             \"request_num\":1,\"lock_id\":{LOCK_ID}}}",
            b.host.driver.client_id
        );
        let rc = b.host.node.request(json.as_bytes());
        assert_ne!(rc, OK, "b's proposal is refused (it is not the leader)");
        b.host.route_op(rc, Op::Get, &json, mid, millis(), &mut rng);
        assert!(
            b.host.driver.pending.is_some(),
            "the forward route armed the pending"
        );

        // Drive the commit forward with the churn wedged into the
        // op's flight: pump a (it accepts the FORWARD_REQUEST and
        // proposes), pump b (its PrepareOk routes back), then the
        // churn takes b out of NORMAL and the churn gate drops the
        // pending — then pump a again: the commit completes and the
        // FORWARD_RESPONSE datagram lands in b's socket buffer, and
        // only then does b pump the ack.
        let now = millis();
        pump_udp(&mut a.host, now, &mut rng);
        pump_tcp(&mut a.host, now, &mut rng);
        a.host.discovery_step(now);
        let _ = a.host.node.idle();
        a.host.flush_outputs(now, &mut rng);
        let now = millis();
        pump_udp(&mut b.host, now, &mut rng);
        pump_tcp(&mut b.host, now, &mut rng);
        b.host.discovery_step(now);
        let _ = b.host.node.idle();
        b.host.flush_outputs(now, &mut rng);

        // The churn: the forced view takes b out of NORMAL, and the
        // driver's churn gate drops the pending (the host's design).
        let status = b.host.node.status();
        assert_eq!(
            b.host.node.force_view(status.era, status.view + 1),
            OK,
            "the forced view takes b into the view-change window"
        );
        b.host.driver_step(millis(), &mut rng);
        assert!(
            b.host.driver.pending.is_none(),
            "the churn gate dropped the pending (the host's design)"
        );

        // The leader completes the forwarded op's commit; the ack lands
        // in b's socket buffer. Event-driven: pump a until its
        // forwarded table is drained (the reply emit removes the row).
        let ack_deadline = std::time::Instant::now() + Duration::from_secs(10);
        while !a.host.forwarded_from.is_empty() {
            assert!(
                std::time::Instant::now() < ack_deadline,
                "the leader never emitted the committed reply"
            );
            let now = millis();
            pump_udp(&mut a.host, now, &mut rng);
            pump_tcp(&mut a.host, now, &mut rng);
            a.host.discovery_step(now);
            let _ = a.host.node.idle();
            a.host.flush_outputs(now, &mut rng);
            std::thread::sleep(Duration::from_millis(1));
        }

        // The late ack arrives. The node must NOT abort: the ack drains
        // into the counter.
        pump_udp(&mut b.host, millis(), &mut rng);
        assert_eq!(
            b.host.late_acks, 1,
            "the late ack was drained and counted, not aborted on"
        );
    }

    fn one_refusal_scenario() {
        let (mut a, mut b, mut rng, _scratch) = harness();
        // A conn with a pending forward: a real socket pair installed
        // straight into the host (the accept path is covered above), with
        // one op in flight whose leader-side refusal is what we feed next.
        let pair = std::net::TcpListener::bind("127.0.0.1:0").expect("pair listener");
        let client = TcpStream::connect(("127.0.0.1", pair.local_addr().expect("local").port()))
            .expect("client connects");
        let (conn_stream, _) = pair.accept().expect("pair accepted");
        client
            .set_read_timeout(Some(Duration::from_millis(10)))
            .ok();
        let mid = *uuid::Uuid::new_v4().as_bytes();
        b.host.conns.push(Conn {
            stream: conn_stream,
            buf: Vec::new(),
            pending: Some(TcpPending::Lock {
                message_id: mid,
                deadline: millis() + 30000,
            }),
        });
        let mut payload = vec![transport::FORWARD_NOT_LEADER];
        payload.extend_from_slice(&mid);
        payload.extend_from_slice(&1u32.to_be_bytes());
        payload.extend_from_slice(&0u32.to_be_bytes());
        let packet =
            transport::encode_peer(transport::PEER_APPLICATION, &b.host.fingerprint, &payload);
        handle_packet(&mut b.host, 1, a.udp, &packet, millis(), &mut rng);
        assert!(
            !b.host.conns.iter().any(|conn| conn.pending.is_some()),
            "the refusal releases the conn's pending"
        );
        // The refusal line lands on the client end of the same conn.
        let mut buf: Vec<u8> = Vec::new();
        let read_deadline = Instant::now() + Duration::from_secs(2);
        let mut chunk = [0u8; 256];
        let mut client = client;
        loop {
            assert!(
                Instant::now() < read_deadline,
                "the refusal line never reached the client (buf={buf:?})"
            );
            match client.read(&mut chunk) {
                Ok(0) => panic!("the conn was closed without a refusal line"),
                Ok(n) => buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::TimedOut
                        || e.kind() == std::io::ErrorKind::Interrupted => {}
                Err(e) => panic!("{e}"),
            }
            if buf.iter().any(|byte| *byte == b'\n') {
                break;
            }
        }
        let text = String::from_utf8_lossy(&buf).to_string();
        assert!(
            text.contains("not_leader"),
            "the conn reads back the refusal line, got {text:?}"
        );
    }
}

#[cfg(test)]
mod boot_hint_tests {
    //! The descriptor-as-hint regression: run 1's replacement refused to
    //! boot with `--name not in descriptor` and needed a hand-edited
    //! descriptor listing the joining node. A name the descriptor omits
    //! must boot as a weight-0 joiner from `--join-id`/`--join-endpoint`
    //! (the file says where the cluster is, never who may exist), and a
    //! listed name must not take join flags.

    use super::*;

    fn hint_nodes() -> Vec<ClusterNode> {
        vec![
            ClusterNode {
                id: 44,
                endpoint: "[2001:db8::1]:9101".to_string(),
                genesis: true,
                host: "[2001:db8::1]".to_string(),
                name: "w1b".to_string(),
                port: 9101,
            },
            ClusterNode {
                id: 55,
                endpoint: "[2001:db8::2]:9101".to_string(),
                genesis: true,
                host: "[2001:db8::2]".to_string(),
                name: "w2b".to_string(),
                port: 9101,
            },
        ]
    }

    fn options(name: &str, join_id: u32, join_endpoint: &str) -> Options {
        Options {
            name: name.to_string(),
            config: "cluster.jsonl".to_string(),
            client: "[::]:19301".to_string(),
            state: "state/node.state".to_string(),
            log: "node.log".to_string(),
            aof_dir: String::new(),
            aof_flush_ms: 0,
            aof_retention_mib: 10,
            telemetry_aof_dir: String::new(),
            telemetry_rollover_mib: 4,
            phi_timeout_min_ms: 10,
            phi_timeout_max_ms: 200,
            viewchange_timeout_min_ms: 100,
            viewchange_timeout_max_ms: 200,
            recovery_flush: RecoveryFlush::Diskless,
            recovery_scratch: String::new(),
            heartbeat_ms: 5,
            election_ms: 1000,
            recovery_ms: 1000,
            #[cfg(feature = "experimental-phi")]
            phi_threshold: 1.0,
            #[cfg(feature = "experimental-phi")]
            phi_safety: 2.0,
            embedded_clients: 0,
            embedded_lock_id: EMBEDDED_LOCK_ID,
            embedded_client_ttl_ms: 500,
            embedded_renew_fraction: 0.5,
            join_id,
            join_endpoint: join_endpoint.to_string(),
            bench_store: String::new(),
            journal_dir: String::new(),
        }
    }

    #[test]
    fn absent_name_boots_as_a_joiner_with_the_cli_identity() {
        let nodes = boot_nodes(hint_nodes(), &options("w1b-r1", 45, "[2001:db8::1]:9103"))
            .expect("the omitted name boots");
        assert_eq!(nodes.len(), 3, "the joiner row is appended");
        let own = nodes
            .iter()
            .find(|node| node.name == "w1b-r1")
            .expect("the own row exists");
        assert_eq!(own.id, 45);
        assert_eq!(own.genesis, false);
        assert_eq!(own.endpoint, "[2001:db8::1]:9103");
        assert_eq!(own.host, "[2001:db8::1]");
        assert_eq!(own.port, 9103);
    }

    #[test]
    fn absent_name_without_join_flags_is_a_usage_error_not_a_refusal() {
        let error = boot_nodes(hint_nodes(), &options("w1b-r1", 0, ""))
            .expect_err("the flags are named in the error");
        assert!(
            error.contains("--join-id") && error.contains("--join-endpoint"),
            "the error names both flags: {error}"
        );
    }

    #[test]
    fn listed_name_rejects_join_flags() {
        let error = boot_nodes(hint_nodes(), &options("w1b", 45, "[2001:db8::1]:9103"))
            .expect_err("a listed name does not take join flags");
        assert!(
            error.contains("is in the descriptor"),
            "the conflict is named: {error}"
        );
    }

    #[test]
    fn listed_name_boot_is_unchanged() {
        let hint = hint_nodes();
        let nodes = boot_nodes(hint.clone(), &options("w2b", 0, "")).expect("boots as before");
        assert_eq!(nodes.len(), hint.len(), "no row is appended");
    }

    #[test]
    fn bad_join_endpoint_is_rejected() {
        for bad in ["no-port", "[2001:db8::1]:notaport", ":9103"] {
            assert!(
                boot_nodes(hint_nodes(), &options("w1b-r1", 45, bad)).is_err(),
                "{bad} must be rejected"
            );
        }
    }
}

#[cfg(test)]
mod reincarnation_remap_tests {
    //! The transport attribution obligation: a `Reincarnation(old, new)`
    //! announcement arriving from the socket the learned map attributes to
    //! `old` is the restarted process's entry ticket — the row for the
    //! bumped id is added at the source socket, the source socket is
    //! re-attributed to the bumped id (so THIS and every later datagram
    //! from it are delivered as the new identity, where the core's own
    //! anti-spoof gates apply), and the old id's row stays (the serving
    //! configuration still names it; it leaves only when the forced steps
    //! evict the identity). An announcement whose body's `old` is not the
    //! socket's current attribution is delivered under the current
    //! attribution and the core refuses it by name — zero reconfiguration,
    //! the lawful outcome for a mis-attributed sender.

    use super::*;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
    use vrr::ids::{Era, NodeId, Slot, View, ViewId};
    use vrr::message::{Body, Message};
    use vrr::wire::{Header, Pack, Tag};

    struct NodeHost {
        host: Host,
        udp: SocketAddr,
    }

    /// Distinct scratch roots per harness instance: the wall clock alone
    /// does not separate parallel test threads, and two harnesses sharing
    /// one root would reopen each other's in-flight state files (a torn
    /// reopen classifies as a crashed restart and boots a bumped
    /// identity), so every root carries a process-global sequence.
    static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_root() -> PathBuf {
        // Test scratch stays inside the repository's `.tmp/` directory;
        // this crate sits two levels below the repository root, and the
        // crate directory is baked in at compile time.
        let repo_tmp = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join(".tmp");
        let seq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = repo_tmp.join(format!(
            "lease-sequencer-remap-{}-{}-{seq}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp root");
        dir
    }

    fn boot_host(name: &str, root: &PathBuf) -> NodeHost {
        let state = root.join(format!("{name}.state"));
        let node = match Node::open(
            "65537:a\x00131073:b\x00196609:c",
            name,
            state.to_str().expect("path"),
            None,
            0,
        ) {
            Ok(node) => node,
            Err(_) => {
                let root = temp_root();
                let state = root.join(format!("{name}.state"));
                Node::open(
                    "65537:a\x00131073:b\x00196609:c",
                    name,
                    state.to_str().expect("path"),
                    None,
                    0,
                )
                .expect("node boots")
            }
        };
        let own_id = node.own_id();
        let sock = UdpSocket::bind("127.0.0.1:0").expect("udp bind");
        sock.set_nonblocking(true).expect("nonblocking udp");
        let listener = TcpListener::bind("127.0.0.1:0").expect("tcp bind");
        listener.set_nonblocking(true).expect("nonblocking tcp");
        let udp_addr = sock.local_addr().expect("udp local");
        let rows = vec![
            (1u32, "127.0.0.1".to_string(), 42901u16, true),
            (2u32, "127.0.0.1".to_string(), 42902u16, true),
            (3u32, "127.0.0.1".to_string(), 42903u16, true),
        ];
        let model = membership::Model {
            era: 1,
            slot: 0,
            members: membership::descriptor_model(&rows),
        };
        let sidecar =
            membership::SidecarWriter::open(state.to_str().expect("path")).expect("sidecar opens");
        let fingerprint = transport::genesis_fingerprint(&[transport::GenesisMember {
            id: 1,
            name: "a",
            host: "127.0.0.1",
            port: 42901,
        }]);
        let host = Host {
            node,
            sock,
            listener,
            peers: HashMap::new(),
            addr_to_id: HashMap::new(),
            fingerprint,
            own_id,
            heartbeat_ms: 100,
            election_ms: 200,
            recovery_ms: 200,
            stagger_ms: 200,
            last_heartbeat: 0,
            leader_elapsed: 0,
            last_recovery: 0,
            last_gossip: 0,
            last_status_note: 0,
            last_seen_leader: LEADER_UNKNOWN,
            reincarnated: false,
            driver: Driver {
                client_id: 900_000,
                request_num: 0,
                holder: uuid::Uuid::new_v4(),
                lease_id: 0,
                held_expiry: None,
                last_get_foreign: false,
                next_action_at: millis() + 300,
                pending: None,
            },
            forwarded_from: HashMap::new(),
            late_acks: 0,
            conns: Vec::new(),
            model,
            sidecar,
            discovery: Discovery {
                era: 1,
                slot: 0,
                tallies: HashMap::new(),
                deadline_ms: millis() + 15000,
                next_request_ms: 0,
                active: true,
            },
            #[cfg(feature = "experimental-phi")]
            phi_monitor: None,
            #[cfg(feature = "experimental-phi")]
            phi_cfg: phi::PhiConfig {
                phi_threshold: 1.0,
                heartbeat_ms: 100,
                safety_multiple: 2.0,
                window: 100,
            },
            #[cfg(feature = "experimental-phi")]
            heartbeat_seq: 0,
            last_leader_commit_ms: 0,
            heartbeat_client_id: 0x0BEEF000,
            heartbeat_request_num: 0,
            #[cfg(feature = "experimental-phi")]
            phi_last_era: None,
            phi_detected_key: None,
            #[cfg(feature = "experimental-phi")]
            phi_watch: None,
            timedout: phi::TimeoutToggle::new(),
            viewchange: phi::ViewChangeTimer::new(100, 200)
                .expect("the harness's viewchange bounds are valid"),
            #[cfg(not(feature = "experimental-phi"))]
            sloppy: phi::SloppyLeader::new(100, 300),
            last_state: STATE_RECOVERING,
            last_weight: None,
            election_wait_armed: 200,
            timeout_knobs: telemetry::TimeoutKnobs {
                min_ms: 100,
                max_ms: 300,
                fixed_ms: 200,
            },
            telemetry: None,
            embedded: None,
        };
        NodeHost {
            host,
            udp: udp_addr,
        }
    }

    /// Three hosts — the three-voter genesis the forced weight sequence
    /// needs (a `Decrement` that would leave fewer voters than the
    /// quorum names is refused by the fold) — each peer row wired to the
    /// others' real sockets.
    fn wire() -> (NodeHost, NodeHost, NodeHost, Rng) {
        let root = temp_root();
        let mut a = boot_host("a", &root);
        let mut b = boot_host("b", &root);
        let mut c = boot_host("c", &root);
        let a_udp = a.udp;
        let b_udp = b.udp;
        let c_udp = c.udp;
        for (host, others) in [
            (&mut a, [(131073u32, b_udp), (196609u32, c_udp)]),
            (&mut b, [(65537u32, a_udp), (196609u32, c_udp)]),
            (&mut c, [(65537u32, a_udp), (131073u32, b_udp)]),
        ] {
            for (id, addr) in others {
                host.host.peers.insert(id, addr);
                host.host.addr_to_id.insert(addr, id);
            }
        }
        let seed = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_millis() as u64;
        (a, b, c, Rng::new(seed))
    }

    /// The settled three-node harness: every node Normal under the
    /// genesis primary `a` (id 1).
    fn settled() -> (NodeHost, NodeHost, NodeHost, Rng) {
        let (mut a, mut b, mut c, mut rng) = wire();
        let deadline = Instant::now() + Duration::from_secs(10);
        while a.host.node.status().state != STATE_NORMAL
            || b.host.node.status().state != STATE_NORMAL
            || c.host.node.status().state != STATE_NORMAL
        {
            assert!(
                Instant::now() < deadline,
                "the three-node remap harness never settled"
            );
            tick(&mut a, &mut b, &mut c, &mut rng);
        }
        (a, b, c, rng)
    }

    fn tick(a: &mut NodeHost, b: &mut NodeHost, c: &mut NodeHost, rng: &mut Rng) {
        for host in [&mut *a, &mut *b, &mut *c] {
            let now = millis();
            pump_udp(&mut host.host, now, rng);
            pump_tcp(&mut host.host, now, rng);
            host.host.discovery_step(now);
            host.host.driver_step(now, rng);
            let _ = host.host.node.idle();
            host.host.flush_outputs(now, rng);
        }
        std::thread::sleep(Duration::from_millis(1));
    }

    /// The core's own packed announcement frame for `(old, new)`.
    fn announcement(view: ViewId, old: u32, new: u32) -> Vec<u8> {
        let message = Message {
            header: Header {
                tag: Tag::Reincarnation,
                view,
                slot: Slot(0),
            },
            body: Body::Reincarnation {
                old: NodeId(old),
                new: NodeId(new),
                committed: Slot(2),
                prepared: Slot(2),
            },
        };
        let mut frame = vec![0u8; message.packed_len()];
        let written = message
            .pack_into(&mut frame)
            .expect("the core packs its own body");
        assert_eq!(written, frame.len());
        frame
    }

    fn send(from: &NodeHost, to_addr: SocketAddr, to_fingerprint: &str, frame: &[u8]) {
        let packet = transport::encode_peer(transport::PEER_VRR, to_fingerprint, frame);
        from.host
            .sock
            .send_to(&packet, to_addr)
            .expect("the announcement datagram leaves the wire");
    }

    /// One announcement's full delivery: the datagram leaves the source
    /// socket, then a brief pause lets loopback delivery land it in the
    /// recipient's non-blocking socket buffer before the pump reads.
    fn deliver(
        from: &NodeHost,
        to: &mut NodeHost,
        to_fingerprint: &str,
        frame: &[u8],
        rng: &mut Rng,
    ) {
        send(from, to.udp, to_fingerprint, frame);
        std::thread::sleep(Duration::from_millis(20));
        pump_udp(&mut to.host, millis(), rng);
    }

    #[test]
    fn the_remap_rebinds_the_source_socket_and_keeps_the_old_row() {
        let (mut a, b, _c, mut rng) = wire();
        let fingerprint = a.host.fingerprint.clone();
        let new = (2 << 16) | 2;
        let frame = announcement(
            ViewId {
                era: Era(1),
                view: View(0),
            },
            (2 << 16) | 1,
            new,
        );
        deliver(&b, &mut a, &fingerprint, &frame, &mut rng);
        assert_eq!(
            a.host.peers.get(&new),
            Some(&b.udp),
            "the bumped id's row is added at the source socket"
        );
        assert_eq!(
            a.host.addr_to_id.get(&b.udp),
            Some(&new),
            "the source socket is re-attributed to the bumped id"
        );
        assert_eq!(
            a.host.peers.get(&((2 << 16) | 1)),
            Some(&b.udp),
            "the old id's row stays: the serving configuration still names it"
        );
        // The same announcement a second time changes nothing: the socket
        // is now attributed to the bumped id, so `old` no longer names
        // the source, and the bumped id already has its row. The map
        // still carries the wired rows for ids 2 and 3 plus the bumped
        // id's row: three rows, none added by the repeat.
        deliver(&b, &mut a, &fingerprint, &frame, &mut rng);
        assert_eq!(a.host.peers.len(), 3, "no second row is learned");
        assert_eq!(
            a.host.addr_to_id.get(&b.udp),
            Some(&new),
            "the attribution is not rewritten"
        );
    }

    #[test]
    fn the_remap_arms_only_on_a_lawful_next_life_pair() {
        let (mut a, b, _c, mut rng) = wire();
        // A `new` that is not `old`'s next life (a skipped counter) arms
        // nothing: no row, no rebind.
        let fingerprint = a.host.fingerprint.clone();
        let skipped_life = announcement(
            ViewId {
                era: Era(1),
                view: View(0),
            },
            (2 << 16) | 1,
            (2 << 16) | 3,
        );
        deliver(&b, &mut a, &fingerprint, &skipped_life, &mut rng);
        assert_eq!(
            a.host.peers.get(&((2 << 16) | 3)),
            None,
            "a skipped life's id gains no row"
        );
        assert_eq!(
            a.host.addr_to_id.get(&b.udp),
            Some(&((2 << 16) | 1)),
            "a skipped life's pair does not re-attribute the socket"
        );
        // An unlawful `new` (a zero system half) is no identity: no row,
        // no rebind.
        let unlawful = announcement(
            ViewId {
                era: Era(1),
                view: View(0),
            },
            (2 << 16) | 1,
            5,
        );
        deliver(&b, &mut a, &fingerprint, &unlawful, &mut rng);
        assert_eq!(a.host.peers.get(&5), None, "an unlawful id gains no row");
        // A degenerate pair (`old == new`) is not an announcement of a
        // bumped identity: no row, no rebind.
        let degenerate = announcement(
            ViewId {
                era: Era(1),
                view: View(0),
            },
            (2 << 16) | 1,
            (2 << 16) | 1,
        );
        deliver(&b, &mut a, &fingerprint, &degenerate, &mut rng);
        assert_eq!(
            a.host.addr_to_id.get(&b.udp),
            Some(&((2 << 16) | 1)),
            "a degenerate pair does not re-attribute the socket"
        );
        // No row was learned: the map still carries exactly the two rows
        // the three-voter wiring gave this host (ids 131073 and 196609).
        assert_eq!(a.host.peers.len(), 2, "no row was learned");
    }

    #[test]
    fn correct_attribution_drives_the_one_pass_fused_batch() {
        let (mut a, mut b, mut c, mut rng) = settled();
        let status = a.host.node.status();
        assert_eq!(
            status.leader,
            65537,
            "the genesis primary leads the harness"
        );
        let view = ViewId {
            era: Era(status.era),
            view: View(status.view),
        };
        let new = (2 << 16) | 2;
        let frame = announcement(view, (2 << 16) | 1, new);
        let fingerprint = a.host.fingerprint.clone();
        deliver(&b, &mut a, &fingerprint, &frame, &mut rng);
        let deadline = Instant::now() + Duration::from_secs(10);
        while a.host.node.status().config_era == status.config_era {
            assert!(
                Instant::now() < deadline,
                "the fused batch never committed: the announcement was not attributed to the bumped identity"
            );
            tick(&mut a, &mut b, &mut c, &mut rng);
        }
        assert_eq!(
            a.host.node.status().config_era,
            status.config_era + 1,
            "the establishing Batch([Decrement, Join]) committed in one pass"
        );
    }

    #[test]
    fn a_mis_attributed_announcement_leaves_zero_reconfiguration() {
        let (mut a, mut b, mut c, mut rng) = settled();
        let status = a.host.node.status();
        let view = ViewId {
            era: Era(status.era),
            view: View(status.view),
        };
        // The announcement body names an `old` that is NOT the socket's
        // current attribution: the remap lawfully refuses, the datagram is
        // delivered under the current attribution, and the core's
        // anti-spoof guard refuses it by name — no reconfiguration.
        let forged_old = 999u32 << 16 | 1;
        let new = forged_old + 1;
        let frame = announcement(view, forged_old, new);
        let fingerprint = a.host.fingerprint.clone();
        deliver(&b, &mut a, &fingerprint, &frame, &mut rng);
        let deadline = Instant::now() + Duration::from_secs(2);
        while Instant::now() < deadline {
            tick(&mut a, &mut b, &mut c, &mut rng);
        }
        assert_eq!(
            a.host.node.status().config_era,
            status.config_era,
            "a mis-attributed announcement drives no reconfiguration"
        );
        assert_eq!(
            a.host.peers.get(&new),
            None,
            "a mis-attributed announcement learns no row"
        );
        assert_eq!(
            a.host.addr_to_id.get(&b.udp),
            Some(&131073),
            "the source socket keeps its current attribution"
        );
    }

    /// The remap's state machine, exhausted: the socket attribution
    /// (matches old / mismatches old / no row) crossed with the announced
    /// pair (a lawful next life / a skipped life / degenerate / unlawful
    /// old) crossed with whether a row for `new` already exists. One cell
    /// arms the remap; every other cell is a refusal that mutates nothing.
    /// The table is the requirement; the exhaustive match refuses to
    /// compile if a cell is added without a route.
    #[test]
    fn the_remap_transition_table_is_exhaustive() {
        /// The socket's attribution against the `old` the pair names.
        #[derive(Clone, Copy)]
        enum Attribution {
            Matches,
            Mismatches,
            NoRow,
        }
        /// The announced pair's shape against the identity law.
        #[derive(Clone, Copy)]
        enum PairShape {
            NextLife,
            SkippedLife,
            Degenerate,
            UnlawfulOld,
        }
        /// Whether a `peers` row for the announced `new` already exists.
        #[derive(Clone, Copy)]
        enum NewRow {
            Absent,
            Present,
        }
        /// What the cell must do.
        #[derive(Clone, Copy, PartialEq, Eq, Debug)]
        enum Route {
            Remap,
            Refuse,
        }
        fn route(attribution: Attribution, pair: PairShape, new_row: NewRow) -> Route {
            match attribution {
                Attribution::Mismatches | Attribution::NoRow => Route::Refuse,
                Attribution::Matches => match pair {
                    PairShape::SkippedLife
                    | PairShape::Degenerate
                    | PairShape::UnlawfulOld => Route::Refuse,
                    PairShape::NextLife => match new_row {
                        NewRow::Present => Route::Refuse,
                        NewRow::Absent => Route::Remap,
                    },
                },
            }
        }

        const OLD: u32 = (2 << 16) | 1; // node 2, the genesis life
        let cells = [
            Attribution::Matches,
            Attribution::Mismatches,
            Attribution::NoRow,
        ]
        .into_iter()
        .flat_map(|attribution| {
            [
                PairShape::NextLife,
                PairShape::SkippedLife,
                PairShape::Degenerate,
                PairShape::UnlawfulOld,
            ]
            .into_iter()
            .flat_map(move |pair| {
                [NewRow::Absent, NewRow::Present]
                    .into_iter()
                    .map(move |new_row| (attribution, pair, new_row))
            })
        });
        let mut armed = 0usize;
        let mut refused = 0usize;
        for (cell, (attribution, pair, new_row)) in cells.enumerate() {
            let (mut a, b, _c, mut rng) = wire();
            let fingerprint = a.host.fingerprint.clone();
            // The announced pair for the cell's shape.
            let new = match pair {
                PairShape::NextLife => (2 << 16) | 2,
                PairShape::SkippedLife => (2 << 16) | 3,
                PairShape::Degenerate => OLD,
                PairShape::UnlawfulOld => 5,
            };
            let old = match pair {
                PairShape::UnlawfulOld => 5, // a zero system half is no identity
                _ => OLD,
            };
            let frame = announcement(
                ViewId {
                    era: Era(1),
                    view: View(0),
                },
                old,
                new,
            );
            // The cell's attribution state.
            match attribution {
                Attribution::Matches => {} // the wire() rows already name OLD at b.udp
                Attribution::Mismatches => {
                    // Attribute b's socket to ANOTHER MEMBER (node 3's
                    // genesis id): a stale row after a socket churn. The
                    // pair names old = node 2, the attribution says node
                    // 3, so G1 refuses; the adapter still knows the id
                    // (a member), so its unknown-sender guard stays quiet.
                    a.host.addr_to_id.insert(b.udp, 196609);
                }
                Attribution::NoRow => {
                    a.host.addr_to_id.remove(&b.udp);
                    a.host.peers.remove(&OLD);
                }
            }
            // The cell's row-for-new state.
            if let NewRow::Present = new_row {
                a.host.peers.insert(new, _c.udp);
            }
            let (peers_before, addr_before) = (a.host.peers.clone(), a.host.addr_to_id.clone());
            deliver(&b, &mut a, &fingerprint, &frame, &mut rng);
            match route(attribution, pair, new_row) {
                Route::Remap => {
                    armed += 1;
                    assert_eq!(
                        a.host.peers.get(&new),
                        Some(&b.udp),
                        "cell {cell}: the bumped id's row is added at the source socket"
                    );
                    assert_eq!(
                        a.host.addr_to_id.get(&b.udp),
                        Some(&new),
                        "cell {cell}: the socket is re-attributed to the bumped id"
                    );
                    assert_eq!(
                        a.host.peers.get(&OLD),
                        Some(&b.udp),
                        "cell {cell}: the old id's row stays"
                    );
                }
                Route::Refuse => {
                    refused += 1;
                    assert_eq!(
                        a.host.peers, peers_before,
                        "cell {cell}: a refusal changes no peers row"
                    );
                    assert_eq!(
                        a.host.addr_to_id, addr_before,
                        "cell {cell}: a refusal changes no attribution"
                    );
                }
            }
            // The invariants in every cell: the maps stay consistent — a
            // socket names at most one current id, and every attributed id
            // has a peers row.
            for (addr, id) in &a.host.addr_to_id {
                assert!(
                    a.host.peers.contains_key(id),
                    "cell {cell}: the attributed id {id} at {addr} has a peers row"
                );
            }
        }
        assert_eq!(armed, 1, "exactly one cell arms the remap");
        assert_eq!(refused, 23, "every other cell refuses");
    }
}
