//! The application-protocol gateway (`docs/src/rfc-application-protocol.md`):
//! the role a node holds when it terminates alien application traffic over
//! uVRR. The gateway holds sessions, assigns the command identity
//! (`pack_128(session_id, counter)`), keeps the nexus — the map from a
//! command's uuid to the opaque socket handle the command arrived on,
//! together with the command in flight — and answers each committed
//! command by taking the nexus entry and writing the response to the
//! handle it names.
//!
//! The vocabulary is the RFC's: application, gateway, session,
//! session_id, counter, uuid, bearer, nexus. The far side is never a
//! client.

use hmac::{Hmac, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use std::collections::BTreeMap;
use std::net::TcpStream;

/// The opaque socket handle a command arrived on: the RFC's "may be TCP
/// or a more exotic thing". The nexus holds the trait object; nothing
/// downstream knows which transport it names.
pub trait AppSink {
    /// Write the command's response bytes.
    fn write_response(&mut self, response: &[u8]);
}

/// The loopback TCP stream is one such handle: responses ride the
/// stream, framed the way the abstract socket's messages are
/// (newline-delimited JSON), and the drop of the handle closes the
/// socket.
impl AppSink for TcpStream {
    fn write_response(&mut self, response: &[u8]) {
        use std::io::Write;
        let _ = self.write_all(response);
        let _ = self.write_all(b"\n");
        let _ = self.flush();
    }
}

/// The command identity: a 16-byte value whose high 64 bits are the
/// `session_id` and whose low 64 bits the `counter`. The bytes cross the
/// core as the operation identity, first 8 big-endian, last 8
/// big-endian — the same channel the committed entry carries back.
pub fn pack_128(session_id: u64, counter: u64) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&session_id.to_be_bytes());
    bytes[8..].copy_from_slice(&counter.to_be_bytes());
    bytes
}

/// The identity's halves, back out of the 16 bytes: `(session_id,
/// counter)`.
pub fn unpack_128(bytes: [u8; 16]) -> (u64, u64) {
    let high = u64::from_be_bytes(bytes[..8].try_into().expect("8 bytes"));
    let low = u64::from_be_bytes(bytes[8..].try_into().expect("8 bytes"));
    (high, low)
}

/// The outcome name a timeout is surfaced under. A timeout is not
/// evidence that the command did not happen, only that the outcome was
/// not learned.
pub const OUTCOME_UNKNOWN: &str = "unknown";

/// The refusal name for a second, different command on a session that
/// already holds one outstanding.
pub const REFUSAL_OUTSTANDING: &str = "outstanding";

/// The refusal name for a bearer the gateway holds no session for.
pub const REFUSAL_BEARER_UNKNOWN: &str = "bearer_unknown";

/// The forward step's outcome: accepted for proposal, or refused by
/// name (the replication protocol reports the refusal, never silence).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForwardOutcome {
    Accepted,
    Refused(&'static str),
}

/// The replication protocol side of the loop: the forward step hands a
/// command's uuid and payload to the leader.
pub trait Forwarder {
    fn forward(&mut self, uuid: [u8; 16], payload: &[u8]) -> ForwardOutcome;
}

/// What a read from the abstract socket became.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AcceptOutcome {
    Accepted,
    Attached,
    Refused(&'static str),
}

/// The gateway's knobs: the bearer key, the in-flight command deadline
/// and the idle-session lifetime, in the driver's own logical ticks.
#[derive(Clone)]
pub struct GatewayConfig {
    pub key: [u8; 32],
    pub command_deadline: u64,
    pub session_ttl: u64,
}

/// What a session's bookkeeping reads back as, through `inspect`: the
/// counter and the command in flight (its uuid and the application's
/// own request number).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionInspect {
    pub counter: u64,
    pub outstanding: Option<([u8; 16], u64)>,
}

/// What the entry in the nexus is for: the session-establishing join,
/// or a command riding one session.
enum Kind {
    Join,
    Command { session_id: u64 },
}

/// One nexus entry: the opaque handle to answer on, what the command
/// was, and the deadline its outcome is learned by.
struct NexusEntry {
    sink: Box<dyn AppSink>,
    kind: Kind,
    deadline: u64,
}

/// One session: the counter, the clock the sweeper reads, and the
/// command in flight.
struct Session {
    counter: u64,
    last_active: u64,
    outstanding: Option<([u8; 16], u64)>,
}

pub struct Gateway {
    config: GatewayConfig,
    sessions: BTreeMap<u64, Session>,
    by_bearer: BTreeMap<String, u64>,
    nexus: BTreeMap<[u8; 16], NexusEntry>,
    join_seq: u64,
}

impl Gateway {
    pub fn new(config: GatewayConfig) -> Gateway {
        Gateway {
            config,
            sessions: BTreeMap::new(),
            by_bearer: BTreeMap::new(),
            nexus: BTreeMap::new(),
            join_seq: 0,
        }
    }

    /// The bearer a session_id answers as: the keyed hash of the
    /// session_id under the gateway's key. A session_id never travels
    /// to an application in the clear; the bearer cannot be inverted to
    /// enumerate adjacent sessions.
    pub fn bearer(&self, session_id: u64) -> String {
        bearer_hex(&self.config.key, session_id)
    }

    /// The join command: the application supplies its audit metadata,
    /// the gateway proposes it, and the reply — the bearer — is written
    /// to the handle when the join commits.
    pub fn join(
        &mut self,
        audit: &Value,
        sink: Box<dyn AppSink>,
        now: u64,
        forwarder: &mut dyn Forwarder,
    ) -> AcceptOutcome {
        // The join precedes the session: its identity rides the
        // no-session prefix, unique within the gateway by the join
        // sequence. The committed slot makes the session_id.
        let uuid = pack_128(0, self.join_seq);
        self.join_seq += 1;
        let payload = json!({"op": "join", "audit": audit});
        match forwarder.forward(uuid, payload.to_string().as_bytes()) {
            ForwardOutcome::Accepted => {
                self.nexus.insert(
                    uuid,
                    NexusEntry {
                        sink,
                        kind: Kind::Join,
                        deadline: now + self.config.command_deadline,
                    },
                );
                AcceptOutcome::Accepted
            }
            ForwardOutcome::Refused(name) => AcceptOutcome::Refused(name),
        }
    }

    /// One command on a session: read from the abstract socket, the
    /// counter increments once, the nexus entry goes in, the command is
    /// forwarded to the leader. A session that already holds one
    /// command refuses the next by name; the re-send of the command in
    /// flight attaches.
    pub fn command(
        &mut self,
        bearer: &str,
        request_num: u64,
        body: &Value,
        mut sink: Box<dyn AppSink>,
        now: u64,
        forwarder: &mut dyn Forwarder,
    ) -> AcceptOutcome {
        let Some(&session_id) = self.by_bearer.get(bearer) else {
            sink.write_response(refusal_bytes(REFUSAL_BEARER_UNKNOWN).as_bytes());
            return AcceptOutcome::Refused(REFUSAL_BEARER_UNKNOWN);
        };
        let outstanding = self
            .sessions
            .get(&session_id)
            .and_then(|session| session.outstanding);
        if let Some((held_uuid, held_num)) = outstanding {
            if held_num == request_num {
                // The re-send of the command in flight: not a second
                // outstanding command — it attaches to the entry
                // already in the nexus and answers on this handle.
                if let Some(entry) = self.nexus.get_mut(&held_uuid) {
                    entry.sink = sink;
                }
                let session = self
                    .sessions
                    .get_mut(&session_id)
                    .expect("the map holds it");
                session.last_active = now;
                return AcceptOutcome::Attached;
            }
            // The refusal by name: never queued behind the command in
            // flight, never a second counter step.
            sink.write_response(refusal_bytes(REFUSAL_OUTSTANDING).as_bytes());
            return AcceptOutcome::Refused(REFUSAL_OUTSTANDING);
        }
        let uuid = {
            let session = self
                .sessions
                .get_mut(&session_id)
                .expect("the map holds it");
            session.last_active = now;
            session.counter += 1;
            pack_128(session_id, session.counter)
        };
        let payload = json!({"request_num": request_num, "body": body});
        match forwarder.forward(uuid, payload.to_string().as_bytes()) {
            ForwardOutcome::Accepted => {
                self.nexus.insert(
                    uuid,
                    NexusEntry {
                        sink,
                        kind: Kind::Command { session_id },
                        deadline: now + self.config.command_deadline,
                    },
                );
                let session = self
                    .sessions
                    .get_mut(&session_id)
                    .expect("the map holds it");
                session.outstanding = Some((uuid, request_num));
                AcceptOutcome::Accepted
            }
            ForwardOutcome::Refused(name) => {
                // A forward that refused is a command that was never
                // accepted: the counter unsteps with it.
                let session = self
                    .sessions
                    .get_mut(&session_id)
                    .expect("the map holds it");
                session.counter -= 1;
                AcceptOutcome::Refused(name)
            }
        }
    }

    /// The committed upcall: a committed message carries the uuid of
    /// the command and the command result as committed at the node. The
    /// gateway-nexus is asked whether it holds the uuid; the entry is
    /// TAKEN from the map before anything is written — a second upcall
    /// for the same uuid finds the map without it, and a crash between
    /// the take and the write leaves nothing to write the result from.
    pub fn committed(&mut self, now: u64, slot: u64, uuid: [u8; 16], result: &[u8]) {
        let Some(entry) = self.nexus.remove(&uuid) else {
            return;
        };
        let NexusEntry { mut sink, kind, .. } = entry;
        match kind {
            Kind::Join => {
                // The committed slot IS the session_id. The session is
                // born here; the application learns the bearer, never
                // the slot.
                let bearer = bearer_hex(&self.config.key, slot);
                self.by_bearer.insert(bearer.clone(), slot);
                self.sessions.insert(
                    slot,
                    Session {
                        counter: 0,
                        last_active: now,
                        outstanding: None,
                    },
                );
                sink.write_response(json!({"bearer": bearer}).to_string().as_bytes());
            }
            Kind::Command { session_id } => {
                if let Some(session) = self.sessions.get_mut(&session_id) {
                    session.last_active = now;
                    if session.outstanding.is_some_and(|(held, _)| held == uuid) {
                        session.outstanding = None;
                    }
                }
                let committed: Value = serde_json::from_slice(result).unwrap_or(Value::Null);
                sink.write_response(json!({"committed": committed}).to_string().as_bytes());
            }
        }
    }

    /// The driver's clock sweep: every command in flight past its
    /// deadline is surfaced as UNKNOWN on its socket — the outcome was
    /// not learned, never that the command did not happen — and every
    /// session idle past its lifetime is dropped, its nexus entry
    /// released and its handle closed.
    pub fn pump(&mut self, now: u64) {
        let expired: Vec<([u8; 16], Kind)> = self
            .nexus
            .iter()
            .filter(|(_, entry)| entry.deadline <= now)
            .map(|(uuid, entry)| {
                (
                    *uuid,
                    match entry.kind {
                        Kind::Join => Kind::Join,
                        Kind::Command { session_id } => Kind::Command { session_id },
                    },
                )
            })
            .collect();
        for (uuid, kind) in expired {
            let Some(mut entry) = self.nexus.remove(&uuid) else {
                continue;
            };
            entry
                .sink
                .write_response(json!({"outcome": OUTCOME_UNKNOWN}).to_string().as_bytes());
            if let Kind::Command { session_id } = kind
                && let Some(session) = self.sessions.get_mut(&session_id)
                && session.outstanding.is_some_and(|(held, _)| held == uuid)
            {
                session.outstanding = None;
            }
        }
        let swept: Vec<u64> = self
            .sessions
            .iter()
            .filter(|(_, session)| session.last_active + self.config.session_ttl < now)
            .map(|(session_id, _)| *session_id)
            .collect();
        for session_id in swept {
            self.sessions.remove(&session_id);
            let bearer = bearer_hex(&self.config.key, session_id);
            self.by_bearer.remove(&bearer);
            // The entry the session still held goes with it: the handle
            // closes, nothing is written — the application re-establishes
            // its session by presenting nothing.
            let owned: Vec<[u8; 16]> = self
                .nexus
                .iter()
                .filter(|(_, entry)| {
                    matches!(entry.kind, Kind::Command { session_id: held } if held == session_id)
                })
                .map(|(uuid, _)| *uuid)
                .collect();
            for uuid in owned {
                self.nexus.remove(&uuid);
            }
        }
    }

    /// Shutdown: release every nexus entry, close every handle, hold no
    /// session.
    pub fn shutdown(&mut self) {
        self.nexus.clear();
        self.sessions.clear();
        self.by_bearer.clear();
    }

    /// A session's bookkeeping, by the bearer the application presents.
    pub fn inspect(&self, bearer: &str) -> Option<SessionInspect> {
        let session_id = *self.by_bearer.get(bearer)?;
        let session = self.sessions.get(&session_id)?;
        Some(SessionInspect {
            counter: session.counter,
            outstanding: session.outstanding,
        })
    }

    /// Whether the nexus still holds the uuid — the loop's own "see if
    /// it has it in the map" step, observable to the driver.
    pub fn nexus_holds(&self, uuid: [u8; 16]) -> bool {
        self.nexus.contains_key(&uuid)
    }
}

/// The bearer's hex rendering: HMAC-SHA-256 of the session_id's
/// big-endian bytes under the gateway's key.
fn bearer_hex(key: &[u8; 32], session_id: u64) -> String {
    let digest = hmac_digest(key, &session_id.to_be_bytes());
    let mut text = String::with_capacity(digest.len() * 2);
    for byte in digest {
        text.push_str(&format!("{byte:02x}"));
    }
    text
}

fn hmac_digest(key: &[u8; 32], message: &[u8]) -> [u8; 32] {
    let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(key).expect("a key of any length");
    mac.update(message);
    mac.finalize().into_bytes().into()
}

/// A refusal's bytes: the refusal by name, never a bare boolean.
fn refusal_bytes(name: &str) -> String {
    json!({"refused": name}).to_string()
}
