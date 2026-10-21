//! The lock service: our gateway envelope over the core's machinery
//! (`vrr::locks`) — the executing node hands its own wallclock as the
//! core's clock, so `expiry = clock + window` is the leader-stamped
//! absolute expiry the clients see.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;
use vrr::locks::{
    ClientId, Lease as CoreLease, Request as CoreRequest, Response as CoreResponse,
    Service as CoreService, StateSnapshot as CoreSnapshot,
};

/// The wire's UUID identity discipline: every id on the wire must be the
/// canonical hyphenated lowercase rendering; decode refuses every other
/// form.
mod canonical_uuid {
    use super::Uuid;

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Uuid, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let text = <String as serde::Deserialize>::deserialize(deserializer)?;
        let parsed = Uuid::parse_str(&text)
            .map_err(|_| <D::Error as serde::de::Error>::custom("uuid: invalid syntax"))?;
        if parsed.to_string() != text {
            return Err(<D::Error as serde::de::Error>::custom(
                "uuid: not the wire's canonical hyphenated lowercase form",
            ));
        }
        Ok(parsed)
    }
}

/// Classification of a committed lock transition, returned by
/// `Service::execute` so the adapter can append a journal event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Transition {
    Hold {
        lock_id: u64,
        lease_id: u64,
        holder: [u8; 16],
        expiry: u64,
    },
    Renew {
        lock_id: u64,
        lease_id: u64,
        holder: [u8; 16],
        expiry: u64,
    },
    Release {
        lock_id: u64,
        lease_id: u64,
        holder: [u8; 16],
        expiry: u64,
    },
    /// The privileged holder-break: the stored record was force-replaced
    /// by the bumped, cleared keeper record; the fields are the broken
    /// lease's values as they were.
    Break {
        lock_id: u64,
        lease_id: u64,
        holder: [u8; 16],
        expiry: u64,
    },
}

impl Transition {
    pub fn lock_id(&self) -> u64 {
        match self {
            Transition::Hold { lock_id, .. }
            | Transition::Renew { lock_id, .. }
            | Transition::Release { lock_id, .. }
            | Transition::Break { lock_id, .. } => *lock_id,
        }
    }
}

/// The state machine's stored lease record and the reply shape.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub lease_id: u64,
    pub holder: Uuid,
    /// The leader-stamped absolute expiry: `execution_time + lease_ms` on
    /// the executing host's wallclock.
    pub expiry: u64,
    /// The granted window in milliseconds.
    pub lease_ms: u64,
    pub name: Option<String>,
    pub labels: Option<Vec<String>>,
    /// The leader's execution tick at the holder-changing SET that
    /// installed the current holder.
    #[serde(default)]
    pub taken_at_ms: u64,
    /// Incremented on a same-holder renewal; zeroed on a holder change,
    /// a release, an expiry, and a break.
    #[serde(default)]
    pub renew_count: u32,
}

/// The SET request's lease candidate: the client names a duration, never
/// an absolute expiry — the leader stamps it. The `lease_id` field is the
/// envelope's vestige; the core's watermark mints every id.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseCandidate {
    #[serde(default)]
    pub lease_id: u64,
    #[serde(deserialize_with = "canonical_uuid::deserialize")]
    pub holder: Uuid,
    /// The requested lease window in milliseconds; the grant condition is
    /// `lease_ms > 0`.
    pub lease_ms: u64,
}

/// Whether `name` is a valid lock display name: at most 128 bytes, a
/// zookeeper-style absolute path
/// `^/(?:[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)$`.
fn valid_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    if bytes.len() > 128 || bytes.first() != Some(&b'/') {
        return false;
    }
    let mut segment_len = 0;
    for &byte in &bytes[1..] {
        match byte {
            b'/' => {
                if segment_len == 0 {
                    return false;
                }
                segment_len = 0;
            }
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'.' | b'_' | b'-' => {
                segment_len += 1;
            }
            _ => return false,
        }
    }
    segment_len > 0
}

/// Whether `label` is a valid operator label: 1–32 bytes matching
/// `^[a-z0-9](?:[-a-z0-9]{0,30}[a-z0-9])?$`.
fn valid_label(label: &str) -> bool {
    let bytes = label.as_bytes();
    if bytes.is_empty() || bytes.len() > 32 {
        return false;
    }
    let alnum = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    if !alnum(bytes[0]) {
        return false;
    }
    if bytes.len() == 1 {
        return true;
    }
    if !alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    bytes[1..bytes.len() - 1]
        .iter()
        .all(|&byte| alnum(byte) || byte == b'-')
}

/// Validates a SET's optional lock identity at decode time — the only point
/// where a request can be refused before it enters the replication log.
fn validate_identity(
    name: &Option<String>,
    labels: &Option<Vec<String>>,
) -> Result<(), serde_json::Error> {
    if let Some(name) = name
        && !valid_name(name)
    {
        return Err(<serde_json::Error as serde::de::Error>::custom(
            "invalid lock name",
        ));
    }
    if let Some(labels) = labels {
        if labels.len() > 8 {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "too many labels",
            ));
        }
        for label in labels {
            if !valid_label(label) {
                return Err(<serde_json::Error as serde::de::Error>::custom(
                    "invalid label",
                ));
            }
        }
    }
    Ok(())
}

/// The `taken_at_ms` a holder-changing SET stores: the execution tick,
/// bumped by 1 ms when it collides with the replaced record's value.
pub fn taken_at_ms_on_holder_change(prior_taken_at_ms: u64, execution_time: u64) -> u64 {
    if prior_taken_at_ms == execution_time {
        execution_time.saturating_add(1)
    } else {
        execution_time
    }
}

/// The client request, as the gateway's envelope carries it: the
/// replication identity (`message_id`/`client_id`/`request_num`) beside
/// the lock operation.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
pub enum Request {
    Get {
        #[serde(deserialize_with = "canonical_uuid::deserialize")]
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
    },
    Set {
        #[serde(deserialize_with = "canonical_uuid::deserialize")]
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
        /// The candidate: the duration the client asks for, never an
        /// absolute expiry.
        lease: LeaseCandidate,
        /// Optional lock display name; absent leaves the stored name
        /// unchanged.
        name: Option<String>,
        /// Optional operator labels; absent leaves the stored set
        /// unchanged.
        labels: Option<Vec<String>>,
        /// The client's send timestamp, purely observational; no protocol
        /// decision may read it.
        #[serde(default)]
        sent_at_ms: Option<u64>,
    },
    Release {
        #[serde(deserialize_with = "canonical_uuid::deserialize")]
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
        #[serde(deserialize_with = "canonical_uuid::deserialize")]
        holder: Uuid,
        lease_id: u64,
    },
    /// The privileged holder-break: an unconditional force-release that
    /// names no holder.
    Break {
        #[serde(deserialize_with = "canonical_uuid::deserialize")]
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
    },
}

impl Request {
    pub fn ids(&self) -> (Uuid, u64, u64) {
        match self {
            Request::Get {
                message_id,
                client_id,
                request_num,
                ..
            }
            | Request::Set {
                message_id,
                client_id,
                request_num,
                ..
            }
            | Request::Release {
                message_id,
                client_id,
                request_num,
                ..
            }
            | Request::Break {
                message_id,
                client_id,
                request_num,
                ..
            } => (*message_id, *client_id, *request_num),
        }
    }
}

/// What the leader answered. Every field is carried in every reply
/// (`lease` as `null` where no record stands), so replies are
/// self-describing.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Response {
    Get {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        lease: Option<Lease>,
        /// The leader's execution tick.
        executed_at: u64,
    },
    Set {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        granted: bool,
        lease: Option<Lease>,
        /// The leader's execution tick.
        executed_at: u64,
    },
    Release {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        released: bool,
        lease: Option<Lease>,
        /// The leader's execution tick.
        executed_at: u64,
    },
    Break {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        /// Whether a stored record was broken; `false` when the lock was
        /// already free.
        broken: bool,
        /// On a successful break, the post-break keeper record: the bumped
        /// `lease_id`, the retained identity, the nil holder, `expiry: 0`.
        lease: Option<Lease>,
        /// The leader's execution tick.
        executed_at: u64,
    },
}

/// The lock table's whole durable state as one serialisable value — the
/// state seam's flush and load payload. The watermark is deliberately not
/// defaulted: a snapshot without it is a decode refusal, never an empty
/// one.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct StateSnapshot {
    /// The table itself: lock id -> stored record, ordered so the encoded
    /// bytes are a function of the state.
    pub locks: BTreeMap<u64, Lease>,
    /// Every lock's generation watermark: the highest generation the lock
    /// has ever handed out, which outlives the record a release removes.
    pub generations: BTreeMap<u64, u64>,
}

/// The display identity and counters kept beside the core's machinery;
/// never a protocol decision's input.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DurableEntry {
    name: Option<String>,
    labels: Option<Vec<String>>,
    taken_at_ms: u64,
    renew_count: u32,
}

/// The lock service: the core's machinery wrapped in the gateway's
/// envelope.
#[derive(Clone, Default)]
pub struct Service {
    core: CoreService,
    durable: BTreeMap<u64, DurableEntry>,
}

impl Service {
    pub fn decode(bytes: &[u8]) -> Result<Request, serde_json::Error> {
        let mut request: Request = serde_json::from_slice(bytes)?;
        if let Request::Set { name, labels, .. } = &mut request {
            validate_identity(name, labels)?;
            if let Some(list) = labels {
                list.sort();
                list.dedup();
            }
            if labels.as_ref().is_some_and(|list| list.is_empty()) {
                *labels = None;
            }
        }
        Ok(request)
    }

    pub fn validate(message_id: Uuid, client_id: u64, request_num: u64, payload: &[u8]) -> bool {
        Self::decode(payload)
            .is_ok_and(|request| request.ids() == (message_id, client_id, request_num))
    }

    /// Executes one committed operation against the lock table.
    // The (reply bytes, transition) pair is the shape; a named type would
    // not carry more than the tuple does.
    #[allow(clippy::type_complexity)]
    pub fn execute(
        &mut self,
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        execution_time: u64,
        payload: &[u8],
    ) -> Result<(Vec<u8>, Option<Transition>), serde_json::Error> {
        let request = Self::decode(payload)?;
        if request.ids() != (message_id, client_id, request_num) {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "replicated envelope does not match lock payload",
            ));
        }
        let lock_id = match &request {
            Request::Get { lock_id, .. }
            | Request::Set { lock_id, .. }
            | Request::Release { lock_id, .. }
            | Request::Break { lock_id, .. } => *lock_id,
        };
        let stored = self.core.snapshot().locks.get(&lock_id).cloned();
        let incumbent = stored
            .as_ref()
            .filter(|lease| lease.expiry > execution_time)
            .cloned();
        let prior_entry = self.durable.get(&lock_id).cloned().unwrap_or_default();
        let (set_name, set_labels) = match &request {
            Request::Set { name, labels, .. } => (name.clone(), labels.clone()),
            _ => (None, None),
        };

        let core_request = match request {
            Request::Get { lock_id, .. } => CoreRequest::Get { lock_id },
            Request::Set { lock_id, lease, .. } => CoreRequest::Set {
                client: client_of(lease.holder)?,
                lock_id,
                window: lease.lease_ms,
            },
            Request::Release {
                lock_id,
                holder,
                lease_id,
                ..
            } => CoreRequest::Release {
                client: client_of(holder)?,
                lock_id,
                lease_id,
            },
            Request::Break { lock_id, .. } => CoreRequest::Break { lock_id },
        };
        let core_payload = serde_json::to_vec(&core_request)?;
        let reply: CoreResponse =
            serde_json::from_slice(&self.core.execute(execution_time, &core_payload)?)?;

        let (response, transition) = match reply {
            CoreResponse::Get {
                lock_id,
                lease,
                executed_at,
            } => (
                Response::Get {
                    message_id,
                    request_num,
                    lock_id,
                    lease: lease.map(|lease| self.compose(lock_id, &lease)),
                    executed_at,
                },
                None,
            ),
            CoreResponse::Set {
                lock_id,
                granted,
                lease,
                executed_at,
            } => {
                if !granted {
                    (
                        Response::Set {
                            message_id,
                            request_num,
                            lock_id,
                            granted: false,
                            lease: lease.map(|lease| self.compose(lock_id, &lease)),
                            executed_at,
                        },
                        None,
                    )
                } else {
                    let granted_lease = lease.expect("a granted Set carries the stored lease");
                    let entry = if incumbent.is_some() {
                        DurableEntry {
                            name: set_name.or(prior_entry.name),
                            labels: set_labels.or(prior_entry.labels),
                            taken_at_ms: prior_entry.taken_at_ms,
                            renew_count: prior_entry.renew_count.saturating_add(1),
                        }
                    } else {
                        DurableEntry {
                            name: set_name.or(prior_entry.name),
                            labels: set_labels.or(prior_entry.labels),
                            taken_at_ms: match self.durable.get(&lock_id) {
                                Some(prior) => {
                                    taken_at_ms_on_holder_change(prior.taken_at_ms, execution_time)
                                }
                                None => execution_time,
                            },
                            renew_count: 0,
                        }
                    };
                    self.durable.insert(lock_id, entry);
                    let transition = if incumbent.is_some() {
                        Transition::Renew {
                            lock_id,
                            lease_id: granted_lease.lease_id,
                            holder: client_bytes(&granted_lease.holder),
                            expiry: granted_lease.expiry,
                        }
                    } else {
                        Transition::Hold {
                            lock_id,
                            lease_id: granted_lease.lease_id,
                            holder: client_bytes(&granted_lease.holder),
                            expiry: granted_lease.expiry,
                        }
                    };
                    (
                        Response::Set {
                            message_id,
                            request_num,
                            lock_id,
                            granted: true,
                            lease: Some(self.compose(lock_id, &granted_lease)),
                            executed_at,
                        },
                        Some(transition),
                    )
                }
            }
            CoreResponse::Release {
                lock_id,
                released,
                lease,
                executed_at,
            } => {
                if released {
                    let transition = incumbent.map(|lease| Transition::Release {
                        lock_id,
                        lease_id: lease.lease_id,
                        holder: client_bytes(&lease.holder),
                        expiry: lease.expiry,
                    });
                    if let Some(entry) = self.durable.get_mut(&lock_id) {
                        entry.taken_at_ms = 0;
                        entry.renew_count = 0;
                    }
                    (
                        Response::Release {
                            message_id,
                            request_num,
                            lock_id,
                            released: true,
                            lease: None,
                            executed_at,
                        },
                        transition,
                    )
                } else {
                    (
                        Response::Release {
                            message_id,
                            request_num,
                            lock_id,
                            released: false,
                            lease: lease.map(|lease| self.compose(lock_id, &lease)),
                            executed_at,
                        },
                        None,
                    )
                }
            }
            CoreResponse::Break {
                lock_id,
                broken,
                lease,
                executed_at,
            } => {
                if broken {
                    let transition = stored.map(|lease| Transition::Break {
                        lock_id,
                        lease_id: lease.lease_id,
                        holder: client_bytes(&lease.holder),
                        expiry: lease.expiry,
                    });
                    if let Some(entry) = self.durable.get_mut(&lock_id) {
                        entry.taken_at_ms = 0;
                        entry.renew_count = 0;
                    }
                    (
                        Response::Break {
                            message_id,
                            request_num,
                            lock_id,
                            broken: true,
                            lease: lease.map(|lease| self.compose(lock_id, &lease)),
                            executed_at,
                        },
                        transition,
                    )
                } else {
                    (
                        Response::Break {
                            message_id,
                            request_num,
                            lock_id,
                            broken: false,
                            lease: lease.map(|lease| self.compose(lock_id, &lease)),
                            executed_at,
                        },
                        None,
                    )
                }
            }
        };
        let bytes = serde_json::to_vec(&response)?;
        Ok((bytes, transition))
    }

    /// The envelope's reply identity: the message id a reply echoes, for
    /// the deduplication the leader keeps at its own edge.
    pub fn response_message_id(bytes: &[u8]) -> Option<Uuid> {
        #[derive(Deserialize)]
        struct ReplyId {
            #[serde(deserialize_with = "canonical_uuid::deserialize")]
            message_id: Uuid,
        }
        serde_json::from_slice::<ReplyId>(bytes)
            .ok()
            .map(|reply| reply.message_id)
    }

    /// Whether the table holds nothing at all: the core's records and
    /// watermarks (the side table's sticky identity does not hold it up).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.is_empty()
    }

    /// The table's whole state as one serialisable value: the state
    /// seam's flush payload.
    #[must_use]
    pub fn snapshot(&self) -> StateSnapshot {
        let core = self.core.snapshot();
        StateSnapshot {
            locks: core
                .locks
                .iter()
                .map(|(&lock_id, lease)| (lock_id, self.compose(lock_id, lease)))
                .collect(),
            generations: core.generations,
        }
    }

    /// Install a loaded snapshot as the table's whole contents —
    /// wholesale, not a merge; the watermarks install with the records.
    pub fn restore(&mut self, snapshot: StateSnapshot) {
        let mut locks = BTreeMap::new();
        let mut durable = BTreeMap::new();
        for (lock_id, lease) in &snapshot.locks {
            locks.insert(
                *lock_id,
                CoreLease {
                    lease_id: lease.lease_id,
                    holder: client_of(lease.holder).unwrap_or_else(|_| {
                        // The keeper record's nil holder: the one identity the
                        // mint refuses, lawful on restore.
                        core_nil_client()
                    }),
                    expiry: lease.expiry,
                    window: lease.lease_ms,
                },
            );
            durable.insert(
                *lock_id,
                DurableEntry {
                    name: lease.name.clone(),
                    labels: lease.labels.clone(),
                    taken_at_ms: lease.taken_at_ms,
                    renew_count: lease.renew_count,
                },
            );
        }
        self.core.restore(CoreSnapshot {
            locks,
            generations: snapshot.generations,
        });
        self.durable = durable;
    }

    /// The envelope's stored record: the core's lease with the side
    /// table's identity and counters composed in.
    fn compose(&self, lock_id: u64, lease: &CoreLease) -> Lease {
        let entry = self.durable.get(&lock_id);
        Lease {
            lease_id: lease.lease_id,
            holder: holder_uuid(lease.holder),
            expiry: lease.expiry,
            lease_ms: lease.window,
            name: entry.and_then(|entry| entry.name.clone()),
            labels: entry.and_then(|entry| entry.labels.clone()),
            taken_at_ms: entry.map_or(0, |entry| entry.taken_at_ms),
            renew_count: entry.map_or(0, |entry| entry.renew_count),
        }
    }
}

/// The holder's 128 bits as the core's client identity; the nil pair is
/// refused here, at the envelope's own door, exactly as the core refuses
/// it at its own.
fn client_of(holder: Uuid) -> Result<ClientId, serde_json::Error> {
    let bytes = holder.as_bytes();
    let msb = u64::from_be_bytes(bytes[0..8].try_into().expect("the holder's first half"));
    let lsb = u64::from_be_bytes(bytes[8..16].try_into().expect("the holder's second half"));
    ClientId::new(msb, lsb).ok_or_else(|| {
        <serde_json::Error as serde::de::Error>::custom("the nil holder is not a client")
    })
}

/// The nil client identity, by the serde door: the derived deserialiser
/// constructs the private halves the public mint refuses. Lawful only for
/// the keeper record's restore.
fn core_nil_client() -> ClientId {
    serde_json::from_value(serde_json::json!({"msb": 0, "lsb": 0}))
        .expect("the core's deserialiser constructs the nil identity")
}

/// The core's client identity as the envelope's holder UUID.
fn holder_uuid(client: ClientId) -> Uuid {
    Uuid::from_bytes(client_bytes(&client))
}

/// The identity's 128 bits, big-endian, as the UUID wire form; the serde
/// shape is the door the core's private halves cross by.
fn client_bytes(client: &ClientId) -> [u8; 16] {
    let bits: ClientIdBits = serde_json::from_value(
        serde_json::to_value(client).expect("the client identity serialises"),
    )
    .expect("the client identity's serde shape is its two halves");
    let mut bytes = [0u8; 16];
    bytes[0..8].copy_from_slice(&bits.msb.to_be_bytes());
    bytes[8..16].copy_from_slice(&bits.lsb.to_be_bytes());
    bytes
}

#[derive(Deserialize)]
struct ClientIdBits {
    msb: u64,
    lsb: u64,
}
