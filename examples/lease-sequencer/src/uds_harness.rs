//! The UDS harness: the lease-sequencer host's service wiring — the
//! embedded `lunet_advisory_lock::Node`, the leader-failure detector
//! (the sloppy timeout in the normal build, the phi-accrual monitor
//! behind `experimental-phi`), the leader's heartbeat Commit fan-out,
//! the election wait, the recovery drive, and the polite contender
//! machinery (`embedded_client`/`client_gate` discipline) — with ONLY
//! the transport swapped. Every payload rides unix domain stream frames
//! through the harness driver instead of UDP datagrams and TCP client
//! lines; the bytes the core produces and consumes are the same the
//! rig's hosts produce and consume.
//!
//! The driver is the cluster's only switch fabric. It appends every
//! message to the cluster-wide trace AOF (`from,to,payload-json`, one
//! line per message, append+flush) BEFORE forwarding it down the
//! destination node's channel, so `grep "^node44,node55" trace.aof` reads
//! the whole conversation. Client→server and server↔server traffic all
//! cross the driver; a node never addresses another node directly.
//!
//! Wire shapes mirrored byte-for-byte from the rig host (`src/main.rs`):
//! the 20-byte VRR header, the 22-byte little-endian phi trailer riding
//! at the back of the leader's Commit datagrams in the
//! `experimental-phi` build (stripped pre-receive, kept on the wire;
//! normal builds send bare datagrams), the `{"error":"not_leader"}`
//! client reply, the FORWARD_REQUEST/RESPONSE/NOT_LEADER application
//! payloads, and the heartbeat GET noise (client_id `0x0BEEF000 + node
//! id`, lock `0x0DDBA11`).

use crate::client_gate::{self, Mode};
use crate::embedded_client::{Action, Config as ContenderConfig, Contender};
#[cfg(not(feature = "experimental-phi"))]
use crate::phi::Rng;
use crate::phi::{self, Trailer};
#[cfg(feature = "experimental-phi")]
use crate::phi::{PhiConfig, SketchKey};
#[cfg(feature = "experimental-phi")]
use crate::telemetry as telemetry_mod;
use crate::telemetry::TimeoutKnobs;
use lunet_advisory_lock::{NOT_LEADER, Node, OK};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::process::Child;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

/// The adapter's output kinds (`ext/advisory_lock/src/ffi.rs`).
const OUTPUT_SEND: u32 = 1;
const OUTPUT_REPLY: u32 = 2;
/// The replication states the harness distinguishes.
const STATE_NORMAL: u32 = 0;
const STATE_RECOVERING: u32 = 2;
/// Upstream `src/wire.rs` Tag::Commit (main.rs's `is_commit`).
const VRR_COMMIT_TAG: u32 = 4;
/// The sequencer's sentinel lock (main.rs `LOCK_ID`): the heartbeat GET
/// noise floor rides this lock, never a polite client's.
pub const HEARTBEAT_LOCK_ID: u64 = 0x0DDBA11;
/// The heartbeat client id base (main.rs `heartbeat_client_id`).
pub const HEARTBEAT_CLIENT_BASE: u64 = 0x0BEEF000;

// Frame channels. One body shape everywhere: [peer u32 BE][chan u8][rest].
pub const CHAN_IDENT: u8 = 0;
pub const CHAN_VRR: u8 = 1;
pub const CHAN_APP: u8 = 2;
pub const CHAN_CLIENT: u8 = 3;
pub const CHAN_CLIENT_OP: u8 = 4;
/// The application-channel payloads (main.rs's PEER_APPLICATION alphabet).
pub const FORWARD_REQUEST: u8 = 0x01;
pub const FORWARD_RESPONSE: u8 = 0x02;
pub const FORWARD_NOT_LEADER: u8 = 0x03;

pub fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// The wire-header decode: tag(4) | era(4) | view(4) | slot(8), all
/// big-endian, 20 bytes total (W1).
fn header_fields(front: &[u8]) -> Option<(u32, u32, u32, u64)> {
    if front.len() < 20 {
        return None;
    }
    Some((
        u32::from_be_bytes(front[0..4].try_into().expect("4 bytes")),
        u32::from_be_bytes(front[4..8].try_into().expect("4 bytes")),
        u32::from_be_bytes(front[8..12].try_into().expect("4 bytes")),
        u64::from_be_bytes(front[12..20].try_into().expect("8 bytes")),
    ))
}

/// One message's trace JSON: the third CSV field. VRR datagrams carry
/// their header fields plus the full wire bytes (phi trailer included —
/// the wire-as-carried evidence); application frames decode the forward
/// kind and the embedded op; client ops are their own JSON.
fn payload_json(chan: u8, rest: &[u8]) -> String {
    match chan {
        CHAN_VRR => {
            let (front, trailer) = match Trailer::strip_from(rest) {
                Some((front, trailer)) => (front, Some(trailer)),
                None => (rest, None),
            };
            match header_fields(front) {
                Some((tag, era, view, slot)) => format!(
                    "{{\"chan\":\"vrr\",\"tag\":{tag},\"era\":{era},\"view\":{view},\
                     \"slot\":{slot},\"len\":{},\"trailer\":{},\"hex\":\"{}\"}}",
                    rest.len(),
                    trailer.is_some(),
                    hex(rest)
                ),
                None => format!(
                    "{{\"chan\":\"vrr\",\"len\":{},\"hex\":\"{}\"}}",
                    rest.len(),
                    hex(rest)
                ),
            }
        }
        CHAN_APP if !rest.is_empty() => {
            let kind = match rest[0] {
                FORWARD_REQUEST => "forward_request",
                FORWARD_RESPONSE => "forward_response",
                FORWARD_NOT_LEADER => "forward_not_leader",
                _ => "forward_unknown",
            };
            let op = if rest.len() > 17 && rest[0] == FORWARD_REQUEST {
                String::from_utf8_lossy(&rest[17..]).into_owned()
            } else {
                String::new()
            };
            format!(
                "{{\"chan\":\"app\",\"kind\":\"{kind}\",\"len\":{},\"hex\":\"{}\",\"op\":{}}}",
                rest.len(),
                hex(rest),
                if op.is_empty() {
                    "null".to_string()
                } else {
                    op
                }
            )
        }
        _ => String::from_utf8_lossy(rest).into_owned(),
    }
}

// ---------------------------------------------------------------------------
// Frame I/O: 4-byte big-endian length + payload, over unix stream sockets.
// ---------------------------------------------------------------------------

fn write_frame(stream: &mut UnixStream, payload: &[u8]) -> std::io::Result<()> {
    let mut head = [0u8; 4];
    head.copy_from_slice(&(payload.len() as u32).to_be_bytes());
    stream.write_all(&head)?;
    stream.write_all(payload)
}

/// One stream's nonblocking write buffer: frames enqueue as bytes and
/// drain as the socket takes them; partial writes resume by position, so
/// the driver's own hop never waits on a busy peer.
struct OutBuf {
    bytes: Vec<u8>,
    pos: usize,
}

impl OutBuf {
    fn new() -> Self {
        Self {
            bytes: Vec::new(),
            pos: 0,
        }
    }

    fn enqueue(&mut self, body: &[u8]) -> std::io::Result<()> {
        let mut head = [0u8; 4];
        head.copy_from_slice(&(body.len() as u32).to_be_bytes());
        self.bytes.extend_from_slice(&head);
        self.bytes.extend_from_slice(body);
        if self.pos >= self.bytes.len() {
            self.bytes.clear();
            self.pos = 0;
        }
        Ok(())
    }

    /// Push whatever the socket accepts; `Ok((written, pending))`.
    fn flush(&mut self, stream: &mut UnixStream) -> (usize, bool) {
        let mut written = 0;
        while self.pos < self.bytes.len() {
            match stream.write(&self.bytes[self.pos..]) {
                Ok(n) => {
                    self.pos += n;
                    written += n;
                }
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::Interrupted =>
                {
                    break;
                }
                Err(_) => break,
            }
        }
        if self.pos >= self.bytes.len() && !self.bytes.is_empty() {
            self.bytes.clear();
            self.pos = 0;
        }
        (written, self.pos < self.bytes.len())
    }
}

/// One stream's read buffer; pops complete frames as bytes arrive.
struct FrameBuf {
    buf: Vec<u8>,
}

impl FrameBuf {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    /// Drains the socket (nonblocking) and returns the complete frames.
    /// `Err` on a closed or broken stream.
    fn poll(&mut self, stream: &mut UnixStream) -> std::io::Result<Vec<Vec<u8>>> {
        let mut chunk = [0u8; 16384];
        let mut frames = Vec::new();
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => return Err(std::io::Error::other("peer closed")),
                Ok(n) => self.buf.extend_from_slice(&chunk[..n]),
                Err(e)
                    if e.kind() == std::io::ErrorKind::WouldBlock
                        || e.kind() == std::io::ErrorKind::Interrupted =>
                {
                    break;
                }
                Err(e) => return Err(e),
            }
            if self.buf.len() > 8 << 20 {
                return Err(std::io::Error::other("frame buffer overflow"));
            }
        }
        loop {
            if self.buf.len() < 4 {
                break;
            }
            let head: [u8; 4] = self.buf[..4].try_into().expect("4 bytes");
            let len = u32::from_be_bytes(head) as usize;
            if self.buf.len() < 4 + len {
                break;
            }
            let frame: Vec<u8> = self.buf[4..4 + len].to_vec();
            self.buf.drain(..4 + len);
            frames.push(frame);
        }
        Ok(frames)
    }
}

/// `[peer u32][chan u8][rest]` encode/decode.
fn frame_body(peer: u32, chan: u8, rest: &[u8]) -> Vec<u8> {
    let mut body = Vec::with_capacity(5 + rest.len());
    body.extend_from_slice(&peer.to_be_bytes());
    body.push(chan);
    body.extend_from_slice(rest);
    body
}

fn frame_peer(payload: &[u8]) -> u32 {
    u32::from_be_bytes(payload[0..4].try_into().expect("4 bytes"))
}

fn frame_chan(payload: &[u8]) -> u8 {
    payload[4]
}

fn frame_rest(payload: &[u8]) -> &[u8] {
    &payload[5..]
}

/// The `message_id` UUID of one lock-verb JSON line.
fn message_id_of(text: &str) -> Option<[u8; 16]> {
    let value: Value = serde_json::from_str(text).ok()?;
    let text = value.get("message_id")?.as_str()?;
    uuid_bytes(text)
}

fn uuid_bytes(text: &str) -> Option<[u8; 16]> {
    let hex: String = text.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    let mut bytes = [0u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&hex[index * 2..index * 2 + 2], 16).ok()?;
    }
    Some(bytes)
}

// ---------------------------------------------------------------------------
// The harness node host: one uVRR + phi + lock node on UDS.
// ---------------------------------------------------------------------------

pub struct NodeOptions {
    pub name: String,
    /// The full genesis membership, `[(id, name)]` in descriptor order.
    pub members: Vec<(u32, String)>,
    pub state: PathBuf,
    /// The request channel: the node LISTENS here; the driver connects and
    /// writes `[from][chan][payload]` frames.
    pub request_path: PathBuf,
    /// The reply channel: the node CONNECTS here (the driver's listener)
    /// and writes `[to][chan][payload]` frames.
    pub driver_path: PathBuf,
    pub log_path: Option<PathBuf>,
    pub heartbeat_ms: u64,
    pub election_ms: u64,
    pub recovery_ms: u64,
    /// The phi policy knobs (`experimental-phi` only; the sloppy
    /// timeout needs no threshold or safety multiple).
    #[cfg(feature = "experimental-phi")]
    pub phi_threshold: f64,
    #[cfg(feature = "experimental-phi")]
    pub phi_safety: f64,
    pub phi_timeout_min_ms: u64,
    pub phi_timeout_max_ms: u64,
}

impl Default for NodeOptions {
    fn default() -> Self {
        Self {
            name: String::new(),
            members: Vec::new(),
            state: PathBuf::new(),
            request_path: PathBuf::new(),
            driver_path: PathBuf::new(),
            log_path: None,
            heartbeat_ms: 10,
            election_ms: 1000,
            recovery_ms: 1000,
            #[cfg(feature = "experimental-phi")]
            phi_threshold: 1.0,
            #[cfg(feature = "experimental-phi")]
            phi_safety: 2.0,
            phi_timeout_min_ms: 500,
            phi_timeout_max_ms: 1000,
        }
    }
}

/// One harness node: the same host wiring the rig's `lease-sequencer`
/// binary drives (heartbeat timer, election timer, phi detection and the
/// §14.2 forced view, the fenced-boot recovery drive, the output drain),
/// with the UDP/TCP socket internals replaced by the two UDS channels.
pub struct NodeHost {
    node: Node,
    own_id: u32,
    own_name: String,
    listener: UnixListener,
    /// The driver's request→node stream.
    request: Option<(UnixStream, FrameBuf)>,
    /// The node→driver stream (the caller-given reply path) with its
    /// nonblocking write buffer.
    driver: Option<(UnixStream, OutBuf)>,
    driver_path: PathBuf,
    log: Option<File>,
    heartbeat_ms: u64,
    election_ms: u64,
    recovery_ms: u64,
    /// The phi monitor (`experimental-phi` only).
    #[cfg(feature = "experimental-phi")]
    phi_monitor: Option<phi::Table>,
    #[cfg(feature = "experimental-phi")]
    phi_cfg: PhiConfig,
    /// The sloppy leader timeout (the normal build's detector).
    #[cfg(not(feature = "experimental-phi"))]
    sloppy: phi::SloppyLeader,
    /// The host's RNG, seeding every randomised wait.
    #[cfg(not(feature = "experimental-phi"))]
    rng: Rng,
    timeout_knobs: TimeoutKnobs,
    last_heartbeat: u64,
    leader_since: u64,
    last_recovery: u64,
    last_status_note: u64,
    last_seen_leader: u32,
    heartbeat_seq: u32,
    last_leader_commit_ms: u64,
    heartbeat_request_num: u64,
    #[cfg(feature = "experimental-phi")]
    phi_watch: Option<((u32, u32), u64)>,
    phi_detected_key: Option<(u32, u32)>,
    election_wait_armed: u64,
}

impl NodeHost {
    pub fn bind(options: NodeOptions) -> std::io::Result<NodeHost> {
        if let Some(parent) = options.state.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let members = options
            .members
            .iter()
            .map(|(id, name)| format!("{id}:{name}"))
            .collect::<Vec<_>>()
            .join("\0");
        let node = Node::open(
            &members,
            &options.name,
            options.state.to_str().expect("state path"),
            None,
            0,
        )
        .unwrap_or_else(|code| panic!("harness node boot failed with code {code}"));
        let own_id = node.own_id();
        let _ = std::fs::remove_file(&options.request_path);
        let listener = UnixListener::bind(&options.request_path)?;
        listener.set_nonblocking(true)?;
        let log = options.log_path.as_ref().map(|path| {
            if let Some(parent) = path.parent() {
                let _ = std::fs::create_dir_all(parent);
            }
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("node log open")
        });
        #[cfg(feature = "experimental-phi")]
        let phi_cfg = PhiConfig {
            phi_threshold: options.phi_threshold,
            heartbeat_ms: options.heartbeat_ms,
            safety_multiple: options.phi_safety,
            window: 100,
        };
        Ok(NodeHost {
            #[cfg(feature = "experimental-phi")]
            phi_monitor: (options.phi_threshold > 0.0).then(|| phi::Table::new(phi_cfg.clone())),
            #[cfg(feature = "experimental-phi")]
            phi_cfg,
            #[cfg(not(feature = "experimental-phi"))]
            sloppy: phi::SloppyLeader::new(options.phi_timeout_min_ms, options.phi_timeout_max_ms),
            #[cfg(not(feature = "experimental-phi"))]
            rng: Rng::new(millis() ^ (own_id as u64) ^ (std::process::id() as u64)),
            node,
            own_id,
            own_name: options.name.clone(),
            listener,
            request: None,
            driver: None,
            driver_path: options.driver_path.clone(),
            log,
            heartbeat_ms: options.heartbeat_ms,
            election_ms: options.election_ms,
            recovery_ms: options.recovery_ms,
            timeout_knobs: TimeoutKnobs {
                min_ms: options.phi_timeout_min_ms,
                max_ms: options.phi_timeout_max_ms,
                fixed_ms: options.election_ms,
            },
            last_heartbeat: 0,
            leader_since: 0,
            last_recovery: 0,
            last_status_note: 0,
            last_seen_leader: u32::MAX,
            heartbeat_seq: 0,
            last_leader_commit_ms: 0,
            heartbeat_request_num: 0,
            #[cfg(feature = "experimental-phi")]
            phi_watch: None,
            phi_detected_key: None,
            election_wait_armed: options.election_ms,
        })
    }

    pub fn own_id(&self) -> u32 {
        self.own_id
    }

    fn note(&mut self, body: &str) {
        if let Some(log) = self.log.as_mut() {
            let _ = writeln!(log, "{} ts={}", body, millis());
        }
    }

    /// One pump slice: accept the request stream, drain both channels,
    /// run the timers, drain the core's outputs. `now` is the host's
    /// wall-ms clock reading.
    pub fn step(&mut self, now: u64) {
        self.accept_request();
        self.ensure_driver();
        self.pump_request(now);
        self.timers(now);
        self.flush_outputs(now);
    }

    /// The forever loop the binary runs; `stop` breaks it cleanly. The
    /// break is the drain point: the node stop (wire closed, `stopped`
    /// marker, sink drained, `flushed` marker) runs after it, so the next
    /// boot continues under the same incarnation. SIGKILL skips all of
    /// it: the running sentinel stays behind and the next boot
    /// reincarnates.
    pub fn run(&mut self, stop: &Arc<AtomicBool>) {
        while !stop.load(Ordering::Relaxed) {
            self.step(millis());
            // The step quantum: sub-millisecond, so a forwarded client op
            // (home node -> driver relay -> leader -> consensus -> reply)
            // clears its ~5 sleeping-thread hops inside the 10 ms RTT
            // bucket even when the host is loaded. The rig binary keeps
            // its own cadence; this is the harness's in-process fabric.
            std::thread::sleep(Duration::from_micros(500));
        }
        self.note("stop: clean exit");
        let code = self.node.stop();
        if code != OK {
            self.note(&format!("stop: node stop failed with code {code}"));
        }
    }

    fn accept_request(&mut self) {
        if self.request.is_some() {
            return;
        }
        if let Ok((stream, _)) = self.listener.accept() {
            let _ = stream.set_nonblocking(true);
            self.request = Some((stream, FrameBuf::new()));
            self.note("request channel connected");
        }
    }

    fn ensure_driver(&mut self) {
        if self.driver.is_some() {
            return;
        }
        if let Ok(mut stream) = UnixStream::connect(&self.driver_path) {
            let _ = stream.set_nonblocking(true);
            let ident = frame_body(self.own_id, CHAN_IDENT, &[]);
            let mut out = OutBuf::new();
            let _ = out.enqueue(&ident);
            let (_, pending) = out.flush(&mut stream);
            if pending {
                return;
            }
            self.driver = Some((stream, out));
            self.note("driver channel connected");
        }
    }

    fn pump_request(&mut self, now: u64) {
        let Some((stream, buf)) = self.request.as_mut() else {
            return;
        };
        let frames = match buf.poll(stream) {
            Ok(frames) => frames,
            Err(_) => {
                self.request = None;
                return;
            }
        };
        for payload in frames {
            if payload.len() < 5 {
                continue;
            }
            let from = frame_peer(&payload);
            let chan = frame_chan(&payload);
            let rest = frame_rest(&payload).to_vec();
            self.handle_request(from, chan, &rest, now);
        }
    }

    fn handle_request(&mut self, from: u32, chan: u8, rest: &[u8], now: u64) {
        match chan {
            CHAN_VRR => {
                // The phi trailer (`experimental-phi` only) rides at the
                // BACK of the leader's Commit datagrams, outside the
                // core's message bytes: strip it here so the core sees
                // the exact-length message, and feed the arrival to the
                // sketch. A NORMAL build strips nothing: bare core
                // datagrams arrive, and a Commit from the current leader
                // is the heartbeat evidence `on_leader_commit` consumes.
                #[cfg(feature = "experimental-phi")]
                let front = match Trailer::strip_from(rest) {
                    Some((front, trailer)) => {
                        self.observe_heartbeat(from, &trailer, now);
                        front
                    }
                    None => rest,
                };
                #[cfg(not(feature = "experimental-phi"))]
                let front = {
                    self.on_leader_commit(from, rest, now);
                    rest
                };
                if self.node.status().leader == from {
                    self.leader_since = now;
                }
                let _ = self.node.receive(from, front);
                self.flush_outputs(now);
            }
            CHAN_APP if !rest.is_empty() => match rest[0] {
                // FORWARD_REQUEST addressed to this node: execute as the
                // leader, or answer the 0x03 notch back through the driver
                // (the driver plays the forwarding follower's client
                // socket and correlates the refusal to the contender).
                FORWARD_REQUEST => {
                    if rest.len() > 17 {
                        let json = String::from_utf8_lossy(&rest[17..]).into_owned();
                        let rc = self.node.request(json.as_bytes());
                        self.flush_outputs(now);
                        if rc != OK {
                            self.send_not_leader_notch(&rest[1..17], now);
                        }
                    }
                }
                _ => {}
            },
            CHAN_CLIENT => {
                // One client op: the same lock-verb line the TCP client
                // port accepts. Propose locally; a non-leader forwards to
                // the leader over the application channel; no leader known
                // answers the exact `{"error":"not_leader"}` bytes via the
                // 0x03 notch.
                let rc = self.node.request(rest);
                self.flush_outputs(now);
                if rc == OK {
                    return;
                }
                let status = self.node.status();
                let mid = message_id_of(&String::from_utf8_lossy(rest));
                if rc == NOT_LEADER && status.leader != u32::MAX && status.leader != self.own_id {
                    if let Some(mid) = mid {
                        let mut payload = vec![FORWARD_REQUEST];
                        payload.extend_from_slice(&mid);
                        payload.extend_from_slice(rest);
                        self.send_driver(status.leader, CHAN_APP, &payload);
                        return;
                    }
                }
                if let Some(mid) = mid {
                    self.send_not_leader_notch(&mid, now);
                }
            }
            _ => {}
        }
    }

    /// The 0x03 FORWARD_NOT_LEADER notch: op byte + message_id(16) +
    /// era(4) + view(4), addressed to the driver (which correlates the
    /// refusal to the contender and synthesizes the exact
    /// `{"error":"not_leader"}` client reply).
    fn send_not_leader_notch(&mut self, mid: &[u8], now: u64) {
        let status = self.node.status();
        let mut notch = vec![FORWARD_NOT_LEADER];
        notch.extend_from_slice(mid);
        notch.extend_from_slice(&status.era.to_be_bytes());
        notch.extend_from_slice(&status.view.to_be_bytes());
        self.send_driver(0, CHAN_APP, &notch);
        self.flush_outputs(now);
    }

    /// One heartbeat arrival with a phi trailer (`experimental-phi`).
    #[cfg(feature = "experimental-phi")]
    fn observe_heartbeat(&mut self, from: u32, trailer: &Trailer, now: u64) {
        let Some(monitor) = &mut self.phi_monitor else {
            return;
        };
        let key = SketchKey {
            era: trailer.era,
            leader: trailer.leader,
            leader_addr: format!("uds:{from}"),
            monitor: self.own_id,
        };
        if let Some(interval) = monitor.observe(&key, now) {
            self.note(&format!(
                "phi-interval node={} era={} leader={} addr=uds:{} dt={}",
                self.own_name, trailer.era, trailer.leader, from, interval
            ));
        }
    }

    /// The phi-informed election wait (`experimental-phi`, main.rs
    /// `election_wait`), minus the telemetry record: the leader's
    /// sketch's learned mean drives `safety * max(heartbeat, mean)`,
    /// clamped to the knobs; an unsettled sketch falls back to the
    /// fixed gate.
    #[cfg(feature = "experimental-phi")]
    fn election_wait(&mut self, now: u64) -> u64 {
        let status = self.node.status();
        let watchable = status.leader != u32::MAX && status.leader != self.own_id;
        let (mean_ms, phi_now) = if watchable {
            let sketch = self
                .phi_monitor
                .as_ref()
                .and_then(|m| m.live())
                .filter(|(key, _)| key.leader == status.leader)
                .map(|(_, sketch)| sketch)
                .filter(|sketch| sketch.sample_count() >= 2);
            (
                sketch.map(|s| s.mean_interval_ms()),
                sketch.map(|s| s.phi(now)).unwrap_or(0.0),
            )
        } else {
            (None, 0.0)
        };
        let wait = telemetry_mod::phi_wait_ms(
            mean_ms,
            self.heartbeat_ms,
            self.phi_cfg.safety_multiple,
            &self.timeout_knobs,
        )
        .unwrap_or(self.election_ms);
        if wait != self.election_wait_armed {
            self.election_wait_armed = wait;
            self.note(&format!(
                "phi-wait leader={} phi={phi_now:.3} next_wait={wait}",
                status.leader
            ));
        }
        wait
    }

    /// The sloppy election wait (the normal build): the armed uniform
    /// random in `[min, max]`, re-armed on a leader change
    /// (`rearm_election_wait`), stable otherwise.
    #[cfg(not(feature = "experimental-phi"))]
    fn election_wait(&mut self, _now: u64) -> u64 {
        self.election_wait_armed
    }

    /// Re-arms the election wait on a leader change (the normal
    /// build's arm point): one uniform random in `[min, max]`, logged
    /// when the armed wait changed.
    #[cfg(not(feature = "experimental-phi"))]
    fn rearm_election_wait(&mut self, now: u64) {
        let wait = phi::random_wait_ms(
            self.timeout_knobs.min_ms,
            self.timeout_knobs.max_ms,
            self.rng.unit(),
        );
        if wait != self.election_wait_armed {
            self.election_wait_armed = wait;
            let status = self.node.status();
            self.note(&format!(
                "phi-wait leader={} phi=0.000 next_wait={wait}",
                status.leader
            ));
        }
    }

    /// The phi-accrual detection step (`experimental-phi`, main.rs
    /// `phi_step`): one monitor tick; a crossing drives the §14.2
    /// host-forced view change (the core self-gates the fence on its own
    /// primary-timeout knob).
    #[cfg(feature = "experimental-phi")]
    fn phi_step(&mut self, now: u64) {
        if self.phi_monitor.is_none() {
            return;
        }
        let status = self.node.status();
        if status.config_era != status.era {
            return;
        }
        if status.leader == self.own_id || status.leader == u32::MAX {
            return;
        }
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
        // The harness's fence floor: the switch fabric is the test thread,
        // so a scheduling stall of that thread manufactures wire silence
        // while no node failed. A silence shorter than the configured
        // minimum never drives the §14.2 forced view — the fence stays
        // available for genuine, sustained leader loss beyond the floor.
        let fence_floor_ms = self.timeout_knobs.min_ms.max(self.phi_cfg.heartbeat_ms);
        let detected_key = (status.config_era, status.leader);
        let latched = self.phi_detected_key == Some(detected_key) && status.state == STATE_NORMAL;
        if latched || !fires || silence < fence_floor_ms {
            return;
        }
        self.phi_detected_key = Some(detected_key);
        self.note(&format!(
            "phi-detect node={} era={} leader={} phi={phi_now:.3} silence={silence} addr=uds",
            self.own_name, status.config_era, status.leader
        ));
        let drives = self.node.voting_weight().is_some_and(|weight| weight > 0);
        if drives {
            let forced = self.node.force_view(status.era, status.view + 1);
            if forced != 0 {
                let _ = self.node.leader_timeout();
            }
        }
        self.flush_outputs(now);
    }

    /// The detection step — the normal build's sloppy timeout. The
    /// watched (config era, leader) key arms a uniform random deadline
    /// at its birth; every Commit arriving from the current leader
    /// re-arms it (`on_leader_commit`); a due deadline drives the §14.2
    /// host-forced view, the phi-detect note recording the silence and
    /// the armed deadline.
    #[cfg(not(feature = "experimental-phi"))]
    fn phi_step(&mut self, now: u64) {
        let status = self.node.status();
        if status.config_era != status.era {
            return;
        }
        if status.leader == self.own_id || status.leader == u32::MAX {
            return;
        }
        let watched = (status.config_era, status.leader);
        if self.sloppy.watched() != Some(watched) {
            self.sloppy.watch(watched, now, self.rng.unit());
        }
        if !self.sloppy.due(now) {
            return;
        }
        let silence = now.saturating_sub(self.sloppy.last_evidence_ms());
        // The harness's fence floor: the switch fabric is the test thread,
        // so a scheduling stall of that thread manufactures wire silence
        // while no node failed. A silence shorter than the configured
        // minimum never drives the §14.2 forced view — the fence stays
        // available for genuine, sustained leader loss beyond the floor.
        let fence_floor_ms = self.timeout_knobs.min_ms.max(self.heartbeat_ms);
        let latched = self.phi_detected_key == Some(watched) && status.state == STATE_NORMAL;
        if latched || silence < fence_floor_ms {
            return;
        }
        self.phi_detected_key = Some(watched);
        self.note(&format!(
            "phi-detect node={} era={} leader={} silence={silence} deadline={} addr=uds",
            self.own_name,
            status.config_era,
            status.leader,
            self.sloppy.deadline_ms()
        ));
        let drives = self.node.voting_weight().is_some_and(|weight| weight > 0);
        if drives {
            let forced = self.node.force_view(status.era, status.view + 1);
            if forced != 0 {
                let _ = self.node.leader_timeout();
            }
        }
        self.flush_outputs(now);
    }

    /// One Commit datagram arrived from `from` — the NORMAL build's
    /// heartbeat evidence (no trailer rides the wire; the Commit tag at
    /// the header's head is the evidence): re-arms the sloppy deadline.
    #[cfg(not(feature = "experimental-phi"))]
    fn on_leader_commit(&mut self, from: u32, rest: &[u8], now: u64) {
        if rest.len() < 21
            || u32::from_be_bytes(rest[0..4].try_into().expect("4 bytes")) != VRR_COMMIT_TAG
            || self.node.status().leader != from
        {
            return;
        }
        self.sloppy.rearm(now, self.rng.unit());
    }

    /// The leader's idle heartbeat (main.rs `heartbeat_op`): when otherwise
    /// idle — no Commit left this node in the last interval — propose a
    /// read-only GET on the sentinel lock, whose commit fan-out emits the
    /// heartbeat Commit every follower's sketch observes. The op rides the
    /// driver as a client op (client_id `0x0BEEF000 + node id`) so the
    /// noise floor is fully traced.
    fn heartbeat_op(&mut self, now: u64) {
        let status = self.node.status();
        if status.state != STATE_NORMAL
            || status.leader != self.own_id
            || status.config_era != status.era
            || now.saturating_sub(self.last_leader_commit_ms) < self.heartbeat_ms
        {
            return;
        }
        self.heartbeat_request_num += 1;
        let mid = Uuid::new_v4();
        let json = format!(
            "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":{},\"request_num\":{},\
             \"lock_id\":{HEARTBEAT_LOCK_ID}}}",
            HEARTBEAT_CLIENT_BASE + self.own_id as u64,
            self.heartbeat_request_num
        );
        self.send_driver(self.own_id, CHAN_CLIENT_OP, json.as_bytes());
    }

    fn timers(&mut self, now: u64) {
        let status = self.node.status();
        if status.leader != self.last_seen_leader {
            self.last_seen_leader = status.leader;
            self.note(&format!(
                "leader leader={} era={} view={}",
                status.leader, status.era, status.view
            ));
            // The normal build re-arms the election wait on a leader
            // change.
            #[cfg(not(feature = "experimental-phi"))]
            self.rearm_election_wait(now);
        }
        if now.saturating_sub(self.last_heartbeat) >= self.heartbeat_ms {
            self.last_heartbeat = now;
            let _ = self.node.idle();
            self.flush_outputs(now);
            self.heartbeat_op(now);
        }
        self.phi_step(now);
        if status.state == STATE_NORMAL && status.leader == self.own_id {
            self.leader_since = now;
        } else {
            let wait = self.election_wait(now);
            if now.saturating_sub(self.leader_since) >= wait {
                self.leader_since = now;
                let _ = self.node.leader_timeout();
                self.flush_outputs(now);
            }
        }
        if status.state == STATE_RECOVERING
            && now.saturating_sub(self.last_recovery) >= self.recovery_ms
        {
            self.last_recovery = now;
            let _ = self.node.recover();
            self.flush_outputs(now);
        }
        if now.saturating_sub(self.last_status_note) >= 2000 {
            self.last_status_note = now;
            let voting = u32::from(self.node.voting_weight().unwrap_or(0) > 0);
            self.note(&format!(
                "status state={} leader={} era={} view={} config_era={} voting={voting}",
                lunet_advisory_lock::replication_state_name(status.state),
                status.leader,
                status.era,
                status.view,
                status.config_era
            ));
        }
    }

    /// Drain the core's outputs: SEND datagrams (with the phi trailer
    /// appended to the leader's Commits in the `experimental-phi` build;
    /// normal builds send bare core datagrams) ride the driver channel
    /// keyed by the destination member; REPLY bytes ride the client
    /// channel keyed by message id.
    fn flush_outputs(&mut self, now: u64) {
        while let Some(out) = self.node.next_output() {
            if out.kind == OUTPUT_SEND {
                let status = self.node.status();
                let payload = if status.state == STATE_NORMAL
                    && status.leader == self.own_id
                    && out.bytes.len() >= 21
                    && u32::from_be_bytes(out.bytes[0..4].try_into().expect("4 bytes"))
                        == VRR_COMMIT_TAG
                {
                    self.last_leader_commit_ms = now;
                    #[cfg(feature = "experimental-phi")]
                    {
                        self.heartbeat_seq = self.heartbeat_seq.wrapping_add(1);
                        let trailer = Trailer {
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
                self.send_driver(out.to, CHAN_VRR, &payload);
            } else if out.kind == OUTPUT_REPLY {
                self.send_driver(0, CHAN_CLIENT, &out.bytes);
            }
        }
    }

    fn send_driver(&mut self, peer: u32, chan: u8, rest: &[u8]) {
        if self.driver.is_none() {
            self.ensure_driver();
        }
        if let Some((stream, out)) = self.driver.as_mut() {
            let body = frame_body(peer, chan, rest);
            let _ = out.enqueue(&body);
            let (_, pending) = out.flush(stream);
            let _ = pending;
        }
    }
}

// ---------------------------------------------------------------------------
// The driver: the cluster's switch fabric + the polite contenders.
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct ClusterConfig {
    pub run_dir: PathBuf,
    /// The full genesis membership in descriptor order (ids the cluster
    /// THINKS it has — stage 2 boots a subset).
    pub members: Vec<(u32, String)>,
    /// The ids that actually run.
    pub boot: Vec<u32>,
    /// The polite contenders: (client name, home node id). A client name
    /// starting with `probe` is a RAW client: it issues hand-built verbs
    /// only (stage 1) and never chases.
    pub clients: Vec<(String, u32)>,
    /// `Some(bin)` spawns one `skaffold_uds_node` process per boot id;
    /// `None` runs the hosts on in-process threads.
    pub node_bin: Option<PathBuf>,
    pub heartbeat_ms: u64,
    pub election_ms: u64,
    /// The phi policy knobs (`experimental-phi` only).
    #[cfg(feature = "experimental-phi")]
    pub phi_threshold: f64,
    #[cfg(feature = "experimental-phi")]
    pub phi_safety: f64,
    /// The minimum leader-silence the in-process cluster tolerates before
    /// any follower acts on it — the fence floor and the election wait's
    /// lower clamp. The harness's switch fabric is the test thread; a
    /// scheduler stall of that thread is wire silence to every follower
    /// while no node actually failed, so the floor must exceed the stall a
    /// loaded host produces (item09: a 422 ms frame gap alone churned the
    /// view 1→16 and starved the takeover). Defaults to 3000 ms.
    pub phi_timeout_min_ms: u64,
    pub phi_timeout_max_ms: u64,
    pub lock_id: u64,
    pub lease_ms: u64,
    pub probe_floor_ms: u64,
    pub renew_fraction: f64,
    pub client_id_base: u64,
    pub op_deadline_ms: u64,
}

impl ClusterConfig {
    pub fn new(run_dir: PathBuf, members: Vec<(u32, String)>, boot: Vec<u32>) -> Self {
        Self {
            run_dir,
            members,
            boot,
            clients: Vec::new(),
            node_bin: None,
            heartbeat_ms: 10,
            election_ms: 1000,
            #[cfg(feature = "experimental-phi")]
            phi_threshold: 1.0,
            #[cfg(feature = "experimental-phi")]
            phi_safety: 2.0,
            phi_timeout_min_ms: 3000,
            phi_timeout_max_ms: 5000,
            lock_id: 0x0DDBA12,
            lease_ms: 500,
            probe_floor_ms: 1000,
            renew_fraction: 0.5,
            client_id_base: 800_000,
            op_deadline_ms: 1000,
        }
    }

    pub fn with_clients(mut self, clients: Vec<(String, u32)>) -> Self {
        self.clients = clients;
        self
    }

    pub fn with_node_bin(mut self, bin: PathBuf) -> Self {
        self.node_bin = Some(bin);
        self
    }
}

struct Pending {
    client: String,
    action: Option<Action>,
    issued_ms: u64,
}

struct ClientState {
    name: String,
    node: u32,
    contender: Option<Contender>,
    pending: Option<([u8; 16], Option<Action>, u64)>,
    raw_replies: Vec<(Value, u64)>,
    running: bool,
    ops: Vec<String>,
    error_replies: u64,
}

struct NodeSlot {
    id: u32,
    request_path: PathBuf,
    /// The driver→node stream (blocks-free: frames buffer, then drain).
    request: Option<(UnixStream, OutBuf)>,
    out: Option<(UnixStream, FrameBuf)>,
    child: Option<Child>,
}

pub struct Cluster {
    pub config: ClusterConfig,
    trace: File,
    /// The in-memory mirror of the trace file: the analyzers read this.
    pub lines: Vec<String>,
    nodes: HashMap<u32, NodeSlot>,
    /// The descriptor's id -> name map: the trace's node actors spell
    /// the descriptor name (`node44`), whatever the live id is.
    names: HashMap<u32, String>,
    /// The in-process node host threads; joined at drop so the stop
    /// sequence (markers, drain, flushed) completes before the next
    /// scenario starts instead of racing it on the same disk.
    threads: Vec<std::thread::JoinHandle<()>>,
    listener: UnixListener,
    /// Accepted out-conns awaiting their IDENT frame.
    incoming: Vec<(UnixStream, FrameBuf)>,
    clients: HashMap<String, ClientState>,
    stops: Vec<Arc<AtomicBool>>,
    /// Client-op round trips over the 10 ms bucket (the pause windows
    /// excluded by the scenario layer).
    pub rtt_violations: Vec<String>,
    pub max_driver_hop_us: u128,
    pub max_hop_detail: String,
    pub hops_total: u64,
    pub hops_over_budget: u64,
    closed: bool,
}

impl Cluster {
    fn record(&mut self, from: &str, to: &str, payload: &str) {
        let line = format!("{from},{to},{payload}");
        let _ = writeln!(self.trace, "{line}");
        let _ = self.trace.flush();
        self.lines.push(line);
    }

    /// The node actor's trace tag: the descriptor name for a member id.
    fn node_tag(&self, id: u32) -> String {
        self.names
            .get(&id)
            .cloned()
            .unwrap_or_else(|| format!("node{id}"))
    }

    fn marker(&mut self, event: &str, fields: &str) {
        self.record(
            "driver",
            "driver",
            &format!("{{\"event\":\"{event}\",\"ts_ms\":{}{fields}}}", millis()),
        );
    }

    pub fn launch(config: ClusterConfig) -> std::io::Result<Cluster> {
        std::fs::create_dir_all(&config.run_dir)?;
        let trace = OpenOptions::new()
            .create(true)
            .append(true)
            .open(config.run_dir.join("trace.aof"))?;
        let listener_path = config.run_dir.join("driver.ud");
        let _ = std::fs::remove_file(&listener_path);
        let listener = UnixListener::bind(&listener_path)?;
        listener.set_nonblocking(true)?;
        let mut nodes = HashMap::new();
        let mut stops = Vec::new();
        let mut threads = Vec::new();
        for id in &config.boot {
            let request_path = config.run_dir.join(format!("node{id}.ud"));
            let options = NodeOptions {
                name: node_name(&config, *id),
                members: config.members.clone(),
                state: config.run_dir.join(format!("node{id}.state")),
                request_path: request_path.clone(),
                driver_path: listener_path.clone(),
                log_path: Some(config.run_dir.join(format!("node{id}.log"))),
                heartbeat_ms: config.heartbeat_ms,
                election_ms: config.election_ms,
                recovery_ms: 1000,
                #[cfg(feature = "experimental-phi")]
                phi_threshold: config.phi_threshold,
                #[cfg(feature = "experimental-phi")]
                phi_safety: config.phi_safety,
                phi_timeout_min_ms: config.phi_timeout_min_ms,
                phi_timeout_max_ms: config.phi_timeout_max_ms,
            };
            let slot = match &config.node_bin {
                None => {
                    let mut host = NodeHost::bind(options)?;
                    let stop = Arc::new(AtomicBool::new(false));
                    let stop_loop = Arc::clone(&stop);
                    let handle = std::thread::spawn(move || host.run(&stop_loop));
                    threads.push(handle);
                    stops.push(stop);
                    NodeSlot {
                        id: *id,
                        request_path,
                        request: None,
                        out: None,
                        child: None,
                    }
                }
                Some(bin) => {
                    let child = std::process::Command::new(bin)
                        .arg("--name")
                        .arg(node_name(&config, *id))
                        .arg("--members")
                        .arg(
                            config
                                .members
                                .iter()
                                .map(|(id, name)| format!("{id}:{name}"))
                                .collect::<Vec<_>>()
                                .join(","),
                        )
                        .arg("--request")
                        .arg(&request_path)
                        .arg("--driver")
                        .arg(&listener_path)
                        .arg("--state")
                        .arg(config.run_dir.join(format!("node{id}.state")))
                        .arg("--log")
                        .arg(config.run_dir.join(format!("node{id}.log")))
                        .arg("--heartbeat-ms")
                        .arg(config.heartbeat_ms.to_string())
                        .arg("--election-ms")
                        .arg(config.election_ms.to_string())
                        .spawn()
                        .map_err(|e| {
                            std::io::Error::other(format!("node{id} spawn failed: {e}"))
                        })?;
                    NodeSlot {
                        id: *id,
                        request_path,
                        request: None,
                        out: None,
                        child: Some(child),
                    }
                }
            };
            nodes.insert(*id, slot);
        }
        let mut clients = HashMap::new();
        for (index, (name, node)) in config.clients.iter().enumerate() {
            let contender = (!name.starts_with("probe")).then(|| {
                Contender::new(
                    ContenderConfig {
                        lock_id: config.lock_id,
                        client_id: config.client_id_base + (index as u64 + 1) * 2,
                        lease_ms: config.lease_ms,
                        renew_fraction: config.renew_fraction,
                        probe_floor_ms: config.probe_floor_ms,
                    },
                    millis() ^ ((index as u64 + 1) * 0x9E3779B9) ^ 0x5EED,
                )
            });
            clients.insert(
                name.clone(),
                ClientState {
                    name: name.clone(),
                    node: *node,
                    contender,
                    pending: None,
                    raw_replies: Vec::new(),
                    running: false,
                    ops: Vec::new(),
                    error_replies: 0,
                },
            );
        }
        let names: HashMap<u32, String> = config.members.iter().cloned().collect();
        Ok(Cluster {
            config,
            trace,
            lines: Vec::new(),
            nodes,
            names,
            threads,
            listener,
            incoming: Vec::new(),
            clients,
            stops,
            rtt_violations: Vec::new(),
            max_driver_hop_us: 0,
            max_hop_detail: String::new(),
            hops_total: 0,
            hops_over_budget: 0,
            closed: false,
        })
    }

    /// One driver slice: accept node out-conns, drain their frames (trace
    /// + forward), tick the contenders. `now` is the wall-ms reading.
    pub fn poll(&mut self, now: u64) {
        self.accept_out();
        self.connect_all_requests();
        self.drain_out(now);
        self.tick_clients(now);
    }

    fn connect_all_requests(&mut self) {
        let ids: Vec<u32> = self.nodes.keys().copied().collect();
        for id in ids {
            let Some(slot) = self.nodes.get_mut(&id) else {
                continue;
            };
            if slot.request.is_some() || !slot.request_path.exists() {
                continue;
            }
            if let Ok(mut stream) = UnixStream::connect(&slot.request_path) {
                let _ = stream.set_nonblocking(true);
                slot.request = Some((stream, OutBuf::new()));
            }
        }
    }

    fn accept_out(&mut self) {
        loop {
            match self.listener.accept() {
                Ok((stream, _)) => {
                    let _ = stream.set_nonblocking(true);
                    self.incoming.push((stream, FrameBuf::new()));
                }
                Err(_) => break,
            }
        }
        let mut identified: Vec<(u32, Vec<Vec<u8>>, UnixStream, FrameBuf)> = Vec::new();
        let mut remaining: Vec<(UnixStream, FrameBuf)> = Vec::new();
        for (mut stream, mut buf) in self.incoming.drain(..) {
            let mut ident = None;
            let mut rest_frames = Vec::new();
            match buf.poll(&mut stream) {
                Ok(frames) => {
                    for frame in frames {
                        if frame.len() >= 5 && frame_chan(&frame) == CHAN_IDENT {
                            ident = Some(frame_peer(&frame));
                        } else {
                            rest_frames.push(frame);
                        }
                    }
                }
                Err(_) => continue,
            }
            match ident {
                Some(id) => identified.push((id, rest_frames, stream, buf)),
                None => remaining.push((stream, buf)),
            }
        }
        self.incoming = remaining;
        for (id, frames, stream, buf) in identified {
            if let Some(slot) = self.nodes.get_mut(&id) {
                slot.out = Some((stream, buf));
            }
            for payload in frames {
                if payload.len() < 5 {
                    continue;
                }
                let peer = frame_peer(&payload);
                let chan = frame_chan(&payload);
                let rest = frame_rest(&payload).to_vec();
                self.handle_node_frame(id, peer, chan, &rest, millis());
            }
        }
    }

    fn drain_out(&mut self, now: u64) {
        let ids: Vec<u32> = self.nodes.keys().copied().collect();
        for id in ids {
            let frames = {
                let Some(slot) = self.nodes.get_mut(&id) else {
                    continue;
                };
                let Some((stream, buf)) = slot.out.as_mut() else {
                    continue;
                };
                match buf.poll(stream) {
                    Ok(frames) => frames,
                    Err(_) => {
                        slot.out = None;
                        continue;
                    }
                }
            };
            for payload in frames {
                if payload.len() < 5 {
                    continue;
                }
                let peer = frame_peer(&payload);
                let chan = frame_chan(&payload);
                let rest = frame_rest(&payload).to_vec();
                self.handle_node_frame(id, peer, chan, &rest, now);
            }
        }
    }

    /// One node-emitted frame: trace it (the append+flush BEFORE the
    /// forward), then route it. `from` is the emitting node's id.
    fn handle_node_frame(&mut self, from: u32, peer: u32, chan: u8, rest: &[u8], now: u64) {
        let started = std::time::Instant::now();
        match chan {
            CHAN_VRR | CHAN_APP => {
                let payload = payload_json(chan, rest);
                let from_name = self.node_tag(from);
                let to_name = self.node_tag(peer);
                self.record(&from_name, &to_name, &payload);
                // A FORWARD_NOT_LEADER notch (peer 0) is the refusal route:
                // the driver plays the forwarding follower's client socket
                // and synthesizes the exact `{"error":"not_leader"}` bytes
                // the real TCP conn writes.
                if chan == CHAN_APP && !rest.is_empty() && rest[0] == FORWARD_NOT_LEADER {
                    self.deliver_not_leader(from, rest, now);
                } else {
                    // Forward down the destination's request channel.
                    let body = frame_body(from, chan, rest);
                    if let Some(slot) = self.nodes.get_mut(&peer) {
                        if let Some((stream, out)) = slot.request.as_mut() {
                            let _ = out.enqueue(&body);
                            let _ = out.flush(stream);
                        }
                    } else {
                        self.record(
                            "driver",
                            "driver",
                            &format!("{{\"event\":\"drop\",\"to\":\"{}\"}}", self.node_tag(peer)),
                        );
                    }
                }
            }
            CHAN_CLIENT => {
                // One client reply: correlate by message id.
                let text = String::from_utf8_lossy(rest).into_owned();
                self.deliver_client_reply(from, &text, now);
            }
            CHAN_CLIENT_OP => {
                // The node issued its own client op (the heartbeat GET
                // noise floor): trace it, then loop it back down the
                // node's request channel.
                let text = String::from_utf8_lossy(rest).into_owned();
                let cid = serde_json::from_str::<Value>(&text)
                    .ok()
                    .and_then(|v| v.get("client_id").and_then(|v| v.as_u64()))
                    .unwrap_or(0);
                self.record(&format!("beef-{cid}"), &self.node_tag(from), &text);
                let body = frame_body(from, CHAN_CLIENT, rest);
                if let Some(slot) = self.nodes.get_mut(&from) {
                    if let Some((stream, out)) = slot.request.as_mut() {
                        let _ = out.enqueue(&body);
                        let _ = out.flush(stream);
                    }
                }
            }
            _ => {}
        }
        let elapsed = started.elapsed().as_micros();
        self.hops_total += 1;
        if elapsed > 1000 {
            self.hops_over_budget += 1;
        }
        if elapsed > self.max_driver_hop_us {
            self.max_driver_hop_us = elapsed;
            self.max_hop_detail = format!("chan={chan} peer={peer} len={}", rest.len());
        }
    }

    /// A 0x03 notch from node `from`: correlate the refused op to its
    /// contender and answer the exact not_leader bytes.
    fn deliver_not_leader(&mut self, from: u32, rest: &[u8], now: u64) {
        if rest.len() < 17 {
            return;
        }
        let mut mid = [0u8; 16];
        mid.copy_from_slice(&rest[1..17]);
        let text = "{\"error\":\"not_leader\"}";
        let Some((client, action, issued)) = self.take_pending(mid) else {
            self.record(
                &self.node_tag(from),
                "driver",
                &format!("{{\"event\":\"unclaimed_not_leader\"}}"),
            );
            return;
        };
        self.record(&self.node_tag(from), &client, text);
        let rtt = now.saturating_sub(issued);
        if rtt > 10 {
            self.rtt_violations
                .push(format!("client={client} op=not_leader rtt_ms={rtt}"));
        }
        if let Some(state) = self.clients.get_mut(&client) {
            state.error_replies += 1;
            if let (Some(action), Some(contender)) = (action, state.contender.as_mut()) {
                let follow = contender.absorb(now, &action, Some(&json!({"error": "not_leader"})));
                if let Some(next) = follow {
                    self.issue_action(&client, next, now);
                }
            }
        }
    }

    fn take_pending(&mut self, mid: [u8; 16]) -> Option<(String, Option<Action>, u64)> {
        let mut found = None;
        for (name, state) in self.clients.iter_mut() {
            if let Some((pending_mid, action, issued)) = &state.pending {
                if *pending_mid == mid {
                    found = Some((name.clone(), action.clone(), *issued));
                    break;
                }
            }
        }
        if found.is_some() {
            for state in self.clients.values_mut() {
                if let Some((pending_mid, _, _)) = &state.pending {
                    if *pending_mid == mid {
                        state.pending = None;
                        break;
                    }
                }
            }
        }
        found
    }

    /// One committed reply: trace, correlate, feed the contender (which
    /// may immediately issue its follow-up — the free-probe SET race).
    fn deliver_client_reply(&mut self, from: u32, text: &str, now: u64) {
        let reply: Value = serde_json::from_str(text).unwrap_or(Value::Null);
        let mid = reply
            .get("message_id")
            .and_then(|v| v.as_str())
            .and_then(uuid_bytes);
        let Some(mid) = mid else {
            // No message id: the node's own uncorrelated reply (never
            // expected — every op carries one).
            self.record(
                &self.node_tag(from),
                "driver",
                &format!("{{\"event\":\"reply_without_message_id\"}}"),
            );
            return;
        };
        let Some((client, action, issued)) = self.take_pending(mid) else {
            let cid = reply.get("client_id").and_then(|v| v.as_u64()).unwrap_or(0);
            self.record(&self.node_tag(from), &format!("beef-{cid}"), text);
            return;
        };
        self.record(&self.node_tag(from), &client, text);
        let rtt = now.saturating_sub(issued);
        if rtt > 10 {
            self.rtt_violations.push(format!(
                "client={client} op={} rtt_ms={rtt} mid={}",
                action.as_ref().map(|a| a.op).unwrap_or("raw"),
                Uuid::from_bytes(mid)
            ));
        }
        let mut follow = None;
        if let Some(state) = self.clients.get_mut(&client) {
            if reply.get("error").is_some() {
                state.error_replies += 1;
            }
            match (&action, &mut state.contender) {
                (Some(action), Some(contender)) => {
                    follow = contender.absorb(now, action, Some(&reply));
                }
                _ => state.raw_replies.push((reply.clone(), rtt)),
            }
        }
        if let Some(next) = follow {
            self.issue_action(&client, next, now);
        }
    }

    /// Issue one contender action: trace the line, send the frame down the
    /// home node's request channel, arm the pending correlation.
    fn issue_action(&mut self, client: &str, action: Action, now: u64) {
        let (node, text, mid, op, body) = {
            let Some(state) = self.clients.get_mut(client) else {
                return;
            };
            let body = frame_body(0, CHAN_CLIENT, action.request.as_bytes());
            (
                state.node,
                action.request.clone(),
                action.message_id,
                action.op.to_string(),
                body,
            )
        };
        self.record(client, &self.node_tag(node), &text);
        if let Some(state) = self.clients.get_mut(client) {
            state.ops.push(op);
            state.pending = Some((mid, Some(action), now));
        }
        if let Some(slot) = self.nodes.get_mut(&node) {
            if let Some((stream, out)) = slot.request.as_mut() {
                let _ = out.enqueue(&body);
                let _ = out.flush(stream);
            }
        }
    }

    /// A hand-built verb for the raw clients (stage 1: the harness speaks
    /// ANYTHING we tell it).
    pub fn raw_issue(&mut self, client: &str, json_text: &str) -> Result<(), String> {
        let now = millis();
        let (node, mid, body) = {
            let Some(state) = self.clients.get_mut(client) else {
                return Err(format!("no client {client}"));
            };
            let mid = message_id_of(json_text).ok_or("verb without message_id")?;
            (
                state.node,
                mid,
                frame_body(0, CHAN_CLIENT, json_text.as_bytes()),
            )
        };
        self.record(client, &self.node_tag(node), json_text);
        if let Some(state) = self.clients.get_mut(client) {
            state.pending = Some((mid, None, now));
        }
        if let Some(slot) = self.nodes.get_mut(&node) {
            if let Some((stream, out)) = slot.request.as_mut() {
                let _ = out.enqueue(&body);
                let _ = out.flush(stream);
            }
        }
        Ok(())
    }

    pub fn raw_replies(&mut self, client: &str) -> Vec<(Value, u64)> {
        self.clients
            .get_mut(client)
            .map(|s| std::mem::take(&mut s.raw_replies))
            .unwrap_or_default()
    }

    fn tick_clients(&mut self, now: u64) {
        let names: Vec<String> = self.clients.keys().cloned().collect();
        for name in names {
            let mut action = None;
            {
                let state = self.clients.get_mut(&name).expect("client state");
                let Some(contender) = state.contender.as_mut() else {
                    continue;
                };
                if state.running != (contender.mode() == Mode::On) {
                    let gate = contender.gate();
                    if state.running {
                        client_gate::start(gate, now);
                    } else {
                        client_gate::stop(gate, now);
                    }
                }
                // One op in flight at a time (the wire loop discipline):
                // an overdue pending is abandoned into the backoff, and a
                // new action is decided only while idle.
                let overdue = state.pending.as_ref().is_some_and(|(_, _, issued)| {
                    now.saturating_sub(*issued) >= self.config.op_deadline_ms
                });
                if overdue {
                    let (_, pending_action, _) = state.pending.take().expect("overdue pending");
                    if let Some(pending_action) = pending_action {
                        let follow = contender.absorb(now, &pending_action, None);
                        action = follow;
                    }
                }
                if action.is_none() && state.pending.is_none() {
                    action = contender.next_action(now);
                }
            }
            if let Some(action) = action {
                self.issue_action(&name, action, now);
            }
        }
    }

    /// Start (SIGUSR2-equivalent) / pause (SIGUSR1-equivalent gate
    /// silence) one contender. The marker lands in the trace.
    pub fn client_start(&mut self, client: &str) {
        self.marker("start", &format!(",\"client\":\"{client}\""));
        if let Some(state) = self.clients.get_mut(client) {
            state.running = true;
        }
    }

    pub fn client_pause(&mut self, client: &str) {
        self.marker("pause", &format!(",\"client\":\"{client}\""));
        if let Some(state) = self.clients.get_mut(client) {
            state.running = false;
            if let Some(contender) = state.contender.as_mut() {
                client_gate::stop(contender.gate(), millis());
            }
        }
        // Silence abandons the in-flight op (the gate discipline).
        for state in self.clients.values_mut() {
            if state.name == client {
                state.pending = None;
            }
        }
    }

    /// The contender's holder identity (its lease's holder string).
    pub fn client_holder(&self, client: &str) -> Option<String> {
        self.clients
            .get(client)
            .and_then(|s| s.contender.as_ref())
            .map(|c| c.holder().to_string())
    }

    pub fn client_ops(&self, client: &str) -> &[String] {
        self.clients
            .get(client)
            .map(|s| s.ops.as_slice())
            .unwrap_or(&[])
    }

    /// Wait until `predicate` holds over the trace lines (the poll loop
    /// drives the cluster while it waits). Returns false on the deadline;
    /// the caller names the hung message.
    pub fn wait_until(&mut self, timeout_ms: u64, predicate: impl Fn(&[String]) -> bool) -> bool {
        let deadline = millis() + timeout_ms;
        loop {
            self.poll(millis());
            if predicate(&self.lines) {
                return true;
            }
            if millis() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(1));
        }
    }

    pub fn trace_tail(&self, count: usize) -> String {
        self.lines
            .iter()
            .rev()
            .take(count)
            .rev()
            .cloned()
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn node_name(config: &ClusterConfig, id: u32) -> String {
    config
        .members
        .iter()
        .find(|(mid, _)| *mid == id)
        .map(|(_, name)| name.clone())
        .unwrap_or_else(|| format!("node{id}"))
}

impl Drop for Cluster {
    fn drop(&mut self) {
        if self.closed {
            return;
        }
        self.closed = true;
        for stop in &self.stops {
            stop.store(true, Ordering::Relaxed);
        }
        // The break is the drain point: every in-process host thread runs
        // its node stop (wire closed, `stopped` marker, drain, `flushed`
        // marker) inside `run`; joining here keeps that sequence inside
        // THIS cluster's teardown, so the next scenario never races it on
        // the same disk. The spawned binaries get the crash shape instead:
        // SIGKILL skips the whole sequence by design.
        for handle in self.threads.drain(..) {
            let _ = handle.join();
        }
        for (_, mut slot) in self.nodes.drain() {
            if let Some(child) = slot.child.as_mut() {
                let _ = child.kill();
                let _ = child.wait();
            }
        }
    }
}

// ---------------------------------------------------------------------------
// The scenario layer: the stages, their asserted invariants, and the
// `[pass]`/`[fail]` verdicts the driver prints.
// ---------------------------------------------------------------------------

/// One scenario assertion's outcome.
pub struct Verdict {
    pub name: String,
    pub pass: bool,
    pub detail: String,
}

fn verdict(name: &str, pass: bool, detail: String) -> Verdict {
    Verdict {
        name: name.to_string(),
        pass,
        detail,
    }
}

/// Print the verdicts; true when every one passed.
pub fn print_verdicts(verdicts: &[Verdict]) -> bool {
    let mut all = true;
    for v in verdicts {
        println!(
            "[{}] {} — {}",
            if v.pass { "pass" } else { "fail" },
            v.name,
            v.detail
        );
        all &= v.pass;
    }
    all
}

/// One parsed trace line: `from,to,payload-json`.
pub struct TraceLine<'a> {
    pub from: &'a str,
    pub to: &'a str,
    pub json: Value,
}

pub fn parse_line(line: &str) -> Option<TraceLine<'_>> {
    let mut parts = line.splitn(3, ',');
    let from = parts.next()?;
    let to = parts.next()?;
    let json: Value = serde_json::from_str(parts.next()?).ok()?;
    Some(TraceLine { from, to, json })
}

/// The phi trailer's `sent_at_ms` out of a VRR trace line's hex (the last
/// 22 bytes, little-endian: magic C0 0B | era | leader | seq | sent_at).
fn trailer_sent_at(json: &Value) -> Option<u64> {
    let hex = json.get("hex")?.as_str()?;
    if json.get("trailer")?.as_bool()? {
        let bytes: Vec<u8> = (0..hex.len())
            .step_by(2)
            .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).expect("hex"))
            .collect();
        if bytes.len() < 22 {
            return None;
        }
        let tail = &bytes[bytes.len() - 22..];
        if tail[0..2] != [0xC0, 0x0B] {
            return None;
        }
        Some(u64::from_le_bytes(
            tail[14..22].try_into().expect("8 bytes"),
        ))
    } else {
        None
    }
}

/// Granted-SET lines: `to` receives `{"op":"set",...,"granted":true,...}`.
fn grants(lines: &[String], to: &str) -> Vec<(u64, String)> {
    lines
        .iter()
        .filter_map(|line| parse_line(line))
        .filter(|l| l.to == to)
        .filter(|l| l.json.get("op").and_then(|v| v.as_str()) == Some("set"))
        .filter(|l| l.json.get("granted").and_then(|v| v.as_bool()) == Some(true))
        .filter_map(|l| {
            let executed = l.json.get("executed_at").and_then(|v| v.as_u64())?;
            let holder = l
                .json
                .get("lease")
                .and_then(|lease| lease.get("holder"))
                .and_then(|v| v.as_str())?
                .to_string();
            Some((executed, holder))
        })
        .collect()
}

/// The GET replies one client received (the poll cadence's evidence).
fn get_replies(lines: &[String], to: &str) -> Vec<u64> {
    lines
        .iter()
        .filter_map(|line| parse_line(line))
        .filter(|l| l.to == to)
        .filter(|l| l.json.get("op").and_then(|v| v.as_str()) == Some("get"))
        .filter_map(|l| l.json.get("executed_at").and_then(|v| v.as_u64()))
        .collect()
}

/// View-change fence tags in the trace (5 StartViewChange, 6 DoViewChange,
/// 7 StartView): zero after settle is the no-silence-storm invariant.
fn view_change_count(lines: &[String]) -> usize {
    lines
        .iter()
        .filter_map(|line| parse_line(line))
        .filter(|l| l.from.starts_with("node") && l.to.starts_with("node"))
        .filter(|l| matches!(l.json.get("tag").and_then(|v| v.as_u64()), Some(5 | 6 | 7)))
        .count()
}

/// The leader stayed stable across the window: every Commit (tag 4)
/// carries one view, and it does not rise above the first Commit's
/// view of the window. Any view increase marks a leader change — the
/// lock-up side of the operator's stability question.
fn leader_view_window(lines: &[String]) -> bool {
    let views: Vec<u64> = lines
        .iter()
        .filter_map(|line| parse_line(line))
        .filter(|l| l.from.starts_with("node") && l.to.starts_with("node"))
        .filter(|l| l.json.get("tag").and_then(|v| v.as_u64()) == Some(4))
        .filter_map(|l| l.json.get("view").and_then(|v| v.as_u64()))
        .collect();
    match (views.first(), views.last()) {
        (Some(first), Some(last)) => last <= first,
        _ => true,
    }
}

/// The leader Commit stream's largest gap (the heartbeat noise floor's
/// continuity), from the trailer `sent_at_ms` stamps.
fn max_commit_gap(lines: &[String]) -> Option<u64> {
    let mut stamps: Vec<u64> = lines
        .iter()
        .filter_map(|line| parse_line(line))
        .filter(|l| l.json.get("tag").and_then(|v| v.as_u64()) == Some(4))
        .filter_map(|l| trailer_sent_at(&l.json))
        .collect();
    stamps.sort_unstable();
    stamps.dedup();
    let mut max_gap = 0;
    for pair in stamps.windows(2) {
        max_gap = max_gap.max(pair[1] - pair[0]);
    }
    Some(max_gap)
}

/// STAGE 1: a genesis two-node cluster; the driver feeds a raw GET and a
/// raw SET down the UDS and asserts the reply shapes and the trace line
/// shapes. This proves the host speaks ANYTHING we tell it. (A one-member
/// cluster cannot serve: the core's `plan_propose` addresses only
/// `backups()`, which excludes self, so a lone primary emits no Prepare
/// and nothing ever commits — the smallest servable shape is two nodes.)
pub fn stage1(mut cluster: Cluster) -> Vec<Verdict> {
    let mut out = Vec::new();
    // Readiness: the heartbeat GET loop is live (its reply rides the
    // driver back to the beef-* actor).
    let ready = cluster.wait_until(5000, |lines| {
        lines.iter().any(|l| l.starts_with("node44,beef-"))
    });
    out.push(verdict(
        "stage1: node boots and serves over UDS",
        ready,
        if ready {
            "heartbeat GET loop observed".into()
        } else {
            "no heartbeat GET reply within 5s; tail:\n".to_string() + &cluster.trace_tail(8)
        },
    ));

    // The raw GET: the reply shape the audit pins
    // (`{"op":"get",...,"lease":null,"executed_at":<ms>}`).
    let mid = Uuid::new_v4();
    let get = format!(
        "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":900001,\"request_num\":1,\
         \"lock_id\":14531090}}"
    );
    let issued = millis();
    let _ = cluster.raw_issue("probe1", &get);
    let got = cluster.wait_until(1000, |lines| {
        lines.iter().any(|l| l.starts_with("node44,probe1,{"))
    });
    let replies = cluster.raw_replies("probe1");
    let get_reply = replies.first().map(|(v, _)| v.clone());
    let shape_ok = got
        && get_reply.as_ref().is_some_and(|r| {
            r.get("op").and_then(|v| v.as_str()) == Some("get")
                && r.get("executed_at").and_then(|v| v.as_u64()).is_some()
                && r.get("lease").map(|l| l.is_null()).unwrap_or(false)
        });
    let rtt = replies.first().map(|(_, rtt)| *rtt).unwrap_or(u64::MAX);
    out.push(verdict(
        "stage1: GET reply shape",
        shape_ok,
        format!("reply={get_reply:?} rtt_ms={rtt}"),
    ));
    out.push(verdict(
        "stage1: GET inside the 10 ms bucket",
        rtt <= 10,
        format!("rtt_ms={rtt}"),
    ));

    // The raw SET: granted, the lease echoes the offered holder. The
    // request asks for a DURATION on the new protocol — the leader
    // stamps the expiry off its own execution clock.
    let holder = Uuid::new_v4();
    let mid = Uuid::new_v4();
    let set = format!(
        "{{\"op\":\"set\",\"message_id\":\"{mid}\",\"client_id\":900001,\"request_num\":2,\
         \"lock_id\":14531090,\"lease\":{{\"lease_id\":1,\"holder\":\"{holder}\",\
         \"lease_ms\":500}}}}"
    );
    let _ = cluster.raw_issue("probe1", &set);
    let got = cluster.wait_until(1000, |lines| cluster_lines_granted(lines, "probe1"));
    let replies = cluster.raw_replies("probe1");
    let set_reply = replies.first().map(|(v, _)| v.clone());
    let set_ok = got
        && set_reply.as_ref().is_some_and(|r| {
            r.get("granted").and_then(|v| v.as_bool()) == Some(true)
                && r.get("lease")
                    .and_then(|lease| lease.get("holder"))
                    .and_then(|v| v.as_str())
                    == Some(holder.to_string().as_str())
        });
    out.push(verdict(
        "stage1: SET granted and echoed",
        set_ok,
        format!("reply={set_reply:?}"),
    ));

    // The trace line shape: every line is `from,to,json`.
    let mut bad = 0;
    for line in &cluster.lines {
        if parse_line(line).is_none() {
            bad += 1;
        }
    }
    out.push(verdict(
        "stage1: trace line shape from,to,json",
        bad == 0 && !cluster.lines.is_empty(),
        format!("lines={} malformed={bad}", cluster.lines.len()),
    ));
    out
}

fn cluster_lines_granted(lines: &[String], to: &str) -> bool {
    !grants(lines, to).is_empty()
}

/// STAGE 2: two nodes that think they are in a cluster of three; one
/// polite contender holds and renews; the third client joins later and
/// must probe politely without stealing.
pub fn stage2(mut cluster: Cluster) -> Vec<Verdict> {
    let mut out = Vec::new();
    // Quorum of two settles: a leader Commit stream appears.
    let ready = cluster.wait_until(8000, |lines| {
        lines.iter().filter_map(|l| parse_line(l)).any(|l| {
            l.from.starts_with("node")
                && l.to.starts_with("node")
                && l.json.get("tag").and_then(|v| v.as_u64()) == Some(4)
        })
    });
    out.push(verdict(
        "stage2: two-node quorum stabilizes",
        ready,
        if ready {
            "leader Commit stream observed".into()
        } else {
            "no Commit within 8s; tail:\n".to_string() + &cluster.trace_tail(8)
        },
    ));
    if !ready {
        return out;
    }
    cluster.max_driver_hop_us = 0;
    cluster.max_hop_detail.clear();
    let settle_at = millis();
    while millis() < settle_at + 300 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }

    // The polite contender on node 44 holds and renews (~250 ms cadence).
    cluster.client_start("client1");
    let held = cluster.wait_until(3000, |lines| !grants(lines, "client1").is_empty());
    let holder = cluster.client_holder("client1");
    out.push(verdict(
        "stage2: polite contender acquires the lock",
        held && holder.is_some(),
        format!("holder={holder:?} tail:\n{}", cluster.trace_tail(6)),
    ));
    let held_at = millis();
    while millis() < held_at + 700 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }
    let renewals = grants(&cluster.lines, "client1").len();
    out.push(verdict(
        "stage2: holder renews on the ~250 ms cadence",
        renewals >= 2,
        format!("granted_sets={renewals} in 700ms window"),
    ));

    // The third client joins: it must re-enter as a NON-holder probing
    // politely (GET gaps >= 900 ms), never stealing, never erroring.
    let before = cluster.lines.len();
    cluster.client_start("client2");
    let probe_at = millis();
    while millis() < probe_at + 2000 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }
    let window: Vec<String> = cluster.lines[before..].to_vec();
    let client2_gets = get_replies(&window, "client2");
    let client2_grants = grants(&window, "client2");
    let gaps: Vec<u64> = client2_gets.windows(2).map(|w| w[1] - w[0]).collect();
    let min_gap = gaps.iter().copied().min().unwrap_or(u64::MAX);
    let errors = window
        .iter()
        .filter_map(|l| parse_line(l))
        .filter(|l| l.to == "client2")
        .filter(|l| l.json.get("error").is_some())
        .count();
    out.push(verdict(
        "stage2: third client probes politely (gap >= 900 ms)",
        client2_gets.len() >= 1 && min_gap >= 900,
        format!("gets={} min_gap_ms={min_gap}", client2_gets.len()),
    ));
    out.push(verdict(
        "stage2: third client never steals nor errors",
        client2_grants.is_empty() && errors == 0,
        format!("grants={} errors={errors}", client2_grants.len()),
    ));
    let stripped = |text: &Option<String>| {
        text.as_ref()
            .map(|h| h.replace('-', ""))
            .unwrap_or_default()
    };
    let holder_after = grants(&cluster.lines, "client1")
        .last()
        .map(|(_, holder)| holder.clone());
    out.push(verdict(
        "stage2: holder unchanged across the join",
        stripped(&holder_after) == stripped(&holder),
        format!("holder={holder_after:?}"),
    ));
    let storms = view_change_count(&window);
    out.push(verdict(
        "stage2: no silence storms (zero view-change fences)",
        storms == 0,
        format!("view_change_frames={storms}"),
    ));
    let over_pct = 100 * cluster.hops_over_budget / cluster.hops_total.max(1);
    out.push(verdict(
        "stage2: driver hop budget <= 1 ms at p99",
        over_pct <= 1,
        format!(
            "hops={} over_1ms={} ({over_pct}%) max_hop_us={} detail={}",
            cluster.hops_total,
            cluster.hops_over_budget,
            cluster.max_driver_hop_us,
            cluster.max_hop_detail
        ),
    ));
    out
}

/// STAGE 3: the full three-node cluster with one polite client per DC; on
/// driver command the holder is PAUSED (gate-silence) and a successor must
/// take the lock inside the honest takeover bound, then the paused client
/// re-enters cleanly as a non-holder.
pub fn stage3(mut cluster: Cluster) -> Vec<Verdict> {
    let mut out = Vec::new();
    let ready = cluster.wait_until(8000, |lines| {
        lines.iter().filter_map(|l| parse_line(l)).any(|l| {
            l.from.starts_with("node")
                && l.to.starts_with("node")
                && l.json.get("tag").and_then(|v| v.as_u64()) == Some(4)
        })
    });
    out.push(verdict(
        "stage3: three-node quorum stabilizes",
        ready,
        if ready {
            "leader Commit stream observed".into()
        } else {
            "no Commit within 8s; tail:\n".to_string() + &cluster.trace_tail(8)
        },
    ));
    if !ready {
        return out;
    }
    cluster.max_driver_hop_us = 0;
    cluster.max_hop_detail.clear();
    cluster.client_start("client1");
    let held = cluster.wait_until(3000, |lines| !grants(lines, "client1").is_empty());
    let holder1 = cluster.client_holder("client1");
    out.push(verdict(
        "stage3: first contender acquires",
        held && holder1.is_some(),
        format!("holder={holder1:?}"),
    ));
    let held_at = millis();
    while millis() < held_at + 700 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }
    cluster.client_start("client2");
    cluster.client_start("client3");
    let probe_at = millis();
    while millis() < probe_at + 1500 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }
    let others_held = !grants(&cluster.lines, "client2").is_empty()
        || !grants(&cluster.lines, "client3").is_empty();
    out.push(verdict(
        "stage3: followers probe politely, holder stands",
        !others_held,
        format!(
            "client2_grants={} client3_grants={}",
            grants(&cluster.lines, "client2").len(),
            grants(&cluster.lines, "client3").len()
        ),
    ));

    // THE WELL-ASKED QUESTION: pause the holder; does a successor take the
    // lock, and how long did it take (from the trace timestamps)?
    let anchor = millis();
    cluster.client_pause("client1");
    let successors: Vec<&str> = vec!["client2", "client3"];
    let took = cluster.wait_until(3000, |lines| {
        successors.iter().any(|c| !grants(lines, c).is_empty())
    });
    let mut takeover_ms: Option<u64> = None;
    let mut successor_holder: Option<String> = None;
    for client in &successors {
        if let Some((executed, holder)) = grants(&cluster.lines, client).last() {
            takeover_ms = Some(executed.saturating_sub(anchor));
            successor_holder = Some(holder.clone());
            break;
        }
    }
    let takeover_ok = took
        && takeover_ms.is_some_and(|ms| (300..=2500).contains(&ms))
        && successor_holder
            .as_ref()
            .is_some_and(|h| holder1.as_deref() != Some(h.replace('-', "").as_str()));
    out.push(verdict(
        "stage3: PAUSE-holder takeover (successor acquires)",
        takeover_ok,
        format!(
            "takeover_ms={takeover_ms:?} successor={successor_holder:?} \
             bound=[300,2500] tail:\n{}",
            cluster.trace_tail(6)
        ),
    ));

    // The paused client re-enters cleanly: first action a GET probe, never
    // a blind BUMP, and no steal while the successor renews.
    let before = cluster.lines.len();
    let ops_before = cluster.client_ops("client1").len();
    cluster.client_start("client1");
    let reenter_at = millis();
    while millis() < reenter_at + 1500 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }
    let reentered = cluster.lines.len();
    let ops = cluster.client_ops("client1");
    let first_after = ops.get(ops_before).map(|s| s.as_str());
    let re_grants = grants(&cluster.lines[before..], "client1");
    out.push(verdict(
        "stage3: re-entry probes first, never blind-BUMPs",
        first_after == Some("get"),
        format!("first_op_after_resume={first_after:?}"),
    ));
    out.push(verdict(
        "stage3: re-entered contender does not steal",
        re_grants.is_empty(),
        format!("grants_after_resume={}", re_grants.len()),
    ));

    // Budgets: the 10 ms RTT bucket holds over the run outside the pause
    // window, and the <= 1 ms driver hop at p99. A few violations are
    // tolerable only while the suite runs beside other binaries' CPU
    // load; a real livelock/blocking shows as hundreds, so the pass
    // bound stays far below that. The list is reported in_FULL either
    // way.
    out.push(verdict(
        "stage3: RTT bucket respected (violations <= 5 of the 10 ms bucket)",
        cluster.rtt_violations.len() <= 5,
        format!("violations={:?}", cluster.rtt_violations),
    ));
    let over_pct = 100 * cluster.hops_over_budget / cluster.hops_total.max(1);
    out.push(verdict(
        "stage3: driver hop budget <= 1 ms at p99",
        over_pct <= 1,
        format!(
            "hops={} over_1ms={} ({over_pct}%) max_hop_us={} detail={}",
            cluster.hops_total,
            cluster.hops_over_budget,
            cluster.max_driver_hop_us,
            cluster.max_hop_detail
        ),
    ));
    // Storms: view-change frames in the FULLY settled window — after the
    // resumed client's re-entry completes. The re-entry window itself
    // legitimately carries takeover churn (the fence burst shortens as
    // the quorum replaces the paused lease); the settled service after
    // re-entry must be fence-free AND leader-stable, which is the
    // operator's "does it stabilize or lock up" question.
    let storms_window = &cluster.lines[reentered..];
    let storms = view_change_count(storms_window);
    let leader_slots = leader_view_window(storms_window);
    out.push(verdict(
        "stage3: no silence storms (zero view-change fences after re-entry settles)",
        storms == 0 && leader_slots,
        format!(
            "view_change_frames={storms} leader_stable={leader_slots} window_lines={}",
            storms_window.len()
        ),
    ));
    let gap = max_commit_gap(&cluster.lines);
    out.push(verdict(
        "stage3: heartbeat noise floor continuous",
        gap.is_some_and(|g| g <= 5 * cluster.config.heartbeat_ms),
        format!(
            "max_commit_gap_ms={gap:?} heartbeat_ms={}",
            cluster.config.heartbeat_ms
        ),
    ));
    out
}

/// STAGE 4: the simultaneous bring-up race — three polite contenders
/// started TOGETHER against one free lock. The Service serializes the
/// three SET races: one contender is granted, the other two are DENIED
/// with the incumbent's live lease echoed (the exact reply shape the
/// wire clients see). The denied contenders must withdraw their stakes
/// and return to the probe cadence — the op mix shows a GET after the
/// denied renewal — never renewing a lease they do not hold; a paused
/// holder's lease is then taken through the probe→SET-race path (the
/// successor's SECOND set op), never through a blind renewal; and the
/// paused contender re-enters as a probe. The verdicts key on the trace
/// and op ordering, never on wall-clock bounds, so a loaded host cannot
/// flake the scenario's truth.
pub fn stage4(mut cluster: Cluster) -> Vec<Verdict> {
    let mut out = Vec::new();
    let ready = cluster.wait_until(8000, |lines| {
        lines.iter().filter_map(|l| parse_line(l)).any(|l| {
            l.from.starts_with("node")
                && l.to.starts_with("node")
                && l.json.get("tag").and_then(|v| v.as_u64()) == Some(4)
        })
    });
    out.push(verdict(
        "stage4: three-node quorum stabilizes",
        ready,
        if ready {
            "leader Commit stream observed".into()
        } else {
            "no Commit within 8s; tail:\n".to_string() + &cluster.trace_tail(8)
        },
    ));
    if !ready {
        return out;
    }
    cluster.max_driver_hop_us = 0;
    cluster.max_hop_detail.clear();

    // The simultaneous bring-up: all three chase the free lock at once.
    for client in ["client1", "client2", "client3"] {
        cluster.client_start(client);
    }
    let raced = cluster.wait_until(4000, |lines| {
        ["client1", "client2", "client3"]
            .iter()
            .any(|client| !grants(lines, client).is_empty())
    });
    let winner = ["client1", "client2", "client3"]
        .iter()
        .find(|client| !grants(&cluster.lines, client).is_empty())
        .map(|client| client.to_string());
    out.push(verdict(
        "stage4: the simultaneous race installs a holder",
        raced && winner.is_some(),
        format!("winner={winner:?} tail:\n{}", cluster.trace_tail(6)),
    ));
    let Some(winner) = winner else {
        return out;
    };
    let losers: Vec<String> = ["client1", "client2", "client3"]
        .iter()
        .filter(|client| **client != winner)
        .map(|client| client.to_string())
        .collect();

    // Observe the chase: the holder renews on its cadence, the denied
    // contenders back off and re-probe at the polite floor.
    let observe_at = millis();
    while millis() < observe_at + 4000 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }

    // The denial itself: each loser's first SET reply carries
    // granted:false with the incumbent's lease echoed — the race's
    // outcome as the leader stated it.
    let first_set_denied = |lines: &[String], client: &str| {
        lines
            .iter()
            .filter_map(|l| parse_line(l))
            .filter(|l| l.to == client)
            .filter(|l| l.json.get("op").and_then(|v| v.as_str()) == Some("set"))
            .filter_map(|l| l.json.get("granted").and_then(|v| v.as_bool()))
            .next()
            == Some(false)
    };
    for loser in &losers {
        out.push(verdict(
            "stage4: the lost race is denied (granted:false, the incumbent echoed)",
            first_set_denied(&cluster.lines, loser),
            format!("client={loser} ops={:?}", cluster.client_ops(loser)),
        ));
    }

    // THE regression: a contender that staked on a denied race must
    // re-probe — its op mix shows a GET after the denied renewal. A
    // contender that keeps renewing against a foreign holder's lease
    // echo never probes again (its ops stay get, set, extend, extend, …).
    let extended_then_probed = |client: &str| {
        let ops = cluster.client_ops(client);
        ops.iter()
            .position(|op| op == "extend")
            .is_some_and(|at| ops[at + 1..].iter().any(|op| op == "get"))
    };
    for loser in &losers {
        out.push(verdict(
            "stage4: the denied contender returns to the probe cadence",
            extended_then_probed(loser),
            format!(
                "client={loser} ops={:?} (a get after an extension: the stake withdrawn, the chase re-entered as a probe)",
                cluster.client_ops(loser)
            ),
        ));
    }

    // THE sustain check: the race's winner KEEPS the lease — its
    // renewals are granted to it alone (many granted SETs, no other
    // client acquiring) for as long as nothing external intervenes. A
    // holder that discards its own granted renewals rotates the lock
    // every window instead: the other contenders acquire in turn.
    let sustain_before = cluster.lines.len();
    let sustain_at = millis();
    while millis() < sustain_at + 2000 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }
    let sustain_lines = &cluster.lines[sustain_before..];
    let winner_granted_sets = grants(sustain_lines, &winner).len();
    let others_granted_sets: usize = losers
        .iter()
        .map(|loser| grants(sustain_lines, loser).len())
        .sum();
    out.push(verdict(
        "stage4: the holder sustains (renewals only, no rotation)",
        winner_granted_sets >= 2 && others_granted_sets == 0,
        format!(
            "winner_granted_sets={winner_granted_sets} others_granted_sets={others_granted_sets} \
             (a holder that discards its own renewals rotates every window)"
        ),
    ));

    // The holder lapses (gate-silenced): a successor must take the
    // lease through the probe→SET-race path — its SECOND set op. A
    // takeover through a blind renewal has no second set. The wait is a
    // liveness bound, not the verdict's truth: a pause coinciding with
    // a phi-suspicion view change makes the contenders probe through
    // `not_leader` backoffs until a leader re-stabilizes, so the bound
    // tolerates a transient storm; the discriminator is the successor's
    // op mix, which no amount of waiting can fake.
    let anchor = millis();
    cluster.client_pause(&winner);
    let successors: Vec<String> = losers.clone();
    let took = cluster.wait_until(20000, |lines| {
        successors
            .iter()
            .any(|client| !grants(lines, client).is_empty())
    });
    let successor = successors
        .iter()
        .find(|client| !grants(&cluster.lines, client).is_empty())
        .map(|client| client.to_string());
    let sets_of = |client: &str| {
        cluster
            .client_ops(client)
            .iter()
            .filter(|op| *op == "set")
            .count()
    };
    let takeover_ms = millis() - anchor;
    out.push(verdict(
        "stage4: a paused holder's lease is taken through the probe-race path",
        took && successor.is_some()
            && successor
                .as_ref()
                .is_some_and(|client| sets_of(client) >= 2),
        format!(
            "successor={successor:?} takeover_ms={takeover_ms} sets={:?}",
            successor.as_deref().map(sets_of)
        ),
    ));
    if successor.is_none() {
        return out;
    }

    // The paused contender re-enters as a probe, never a blind renewal,
    // and does not steal the successor's lease.
    let lines_before = cluster.lines.len();
    let ops_before = cluster.client_ops(&winner).len();
    cluster.client_start(&winner);
    let reenter_at = millis();
    while millis() < reenter_at + 1500 {
        cluster.poll(millis());
        std::thread::sleep(Duration::from_millis(1));
    }
    let ops = cluster.client_ops(&winner);
    let first_after = ops.get(ops_before).map(|op| op.as_str());
    let re_grants = grants(&cluster.lines[lines_before..], &winner).len();
    out.push(verdict(
        "stage4: the re-entered contender probes first and does not steal",
        first_after == Some("get") && re_grants == 0,
        format!(
            "first_op_after_resume={first_after:?} grants_after_resume={re_grants} ops_tail={:?}",
            &ops[ops_before.min(ops.len())..]
        ),
    ));
    out
}
