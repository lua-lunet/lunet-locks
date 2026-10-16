//! The lock service: our gateway envelope over the core's machinery.
//!
//! The machinery is upstream's (`vrr::locks`): the four verbs, the
//! CAS grant, the generation watermark that never hands a generation out
//! twice, the truthful release, and the nil holder no request can name.
//! This module owns the envelope — the gateway wire the clients and the
//! console speak — and adapts between the two, keeping the lease
//! semantics the deployment already runs: the executing node hands its
//! own wallclock as the core's clock, so `expiry = clock + window` is
//! the leader-stamped absolute expiry the clients have always seen.
//!
//! Ours, beside the envelope: the display identity (`name`/`labels`, the
//! sticky, validated, canonically sorted identity a SET installs and a
//! release keeps), the counters (`taken_at_ms`/`renew_count`), and the
//! [`Transition`] classification the journal and the changed-set notes
//! ride. The core's generation watermark mints every `lease_id`: the
//! candidate's `lease_id` field stays on the wire for the envelope's
//! compatibility and crosses nowhere — a client can never name a
//! generation.

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;
use vrr::locks::{
    ClientId, Lease as CoreLease, Request as CoreRequest, Response as CoreResponse,
    Service as CoreService, StateSnapshot as CoreSnapshot,
};

/// The wire's UUID identity discipline: every id a client puts on the wire
/// must be the canonical hyphenated lowercase rendering (`Uuid::to_string`)
/// — exactly the form the leader echoes back. The Contender regression
/// (a bare 32-hex draw) parsed silently and only missed at the client's
/// string comparison, so decode now refuses every non-canonical id loudly,
/// before the request can enter the replication log.
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
    /// The privileged holder-break: the stored record (a live lease or an
    /// expired one) was force-released and replaced by the bumped, cleared
    /// keeper record. The fields are the broken lease's values as they were,
    /// so the journal record is self-describing exactly like a release's.
    Break {
        lock_id: u64,
        lease_id: u64,
        holder: [u8; 16],
        expiry: u64,
    },
}

impl Transition {
    /// The transition's lock: the address every committed lock
    /// transition is noted under (the tick path's changed set).
    pub fn lock_id(&self) -> u64 {
        match self {
            Transition::Hold { lock_id, .. }
            | Transition::Renew { lock_id, .. }
            | Transition::Release { lock_id, .. }
            | Transition::Break { lock_id, .. } => *lock_id,
        }
    }
}

/// The state machine's stored lease record and the reply shape. Every field
/// is serialized in every reply (`name`/`labels` as `null` when the lock
/// carries none), so observers classify events without diffing successive
/// states.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub lease_id: u64,
    pub holder: Uuid,
    /// The leader-stamped absolute expiry: `execution_time + lease_ms` on
    /// the executing host's wallclock. Requests never carry it — the
    /// client names a duration and the leader stamps the instant.
    pub expiry: u64,
    /// The granted window in milliseconds: the duration the SET asked
    /// for, echoed so the reply is self-describing (`expiry - executed_at`
    /// is the remaining life the leader measured).
    pub lease_ms: u64,
    pub name: Option<String>,
    pub labels: Option<Vec<String>>,
    /// The leader's execution tick at the holder-changing SET that installed
    /// the current holder; cleared on release, expiry, and break. Requests
    /// never carry it: the state machine sets it.
    #[serde(default)]
    pub taken_at_ms: u64,
    /// Incremented on a same-holder renewal. A holder change, a release, an
    /// expiry, and a break zero it. The state machine sets it, never the
    /// client.
    #[serde(default)]
    pub renew_count: u32,
}

/// The SET request's lease candidate: what the client asks for. The client
/// names a DURATION and can never name an absolute expiry — the leader
/// stamps `expiry = execution_time + lease_ms` on its own wallclock, so no
/// client clock enters the lease's semantics. The shape decodes strictly:
/// a candidate that still carries the retired absolute-`expiry` field is
/// refused at decode, before the request can enter the replication log.
///
/// The `lease_id` field is the envelope's vestige: the core's generation
/// watermark mints every id, so the value a client sends crosses nowhere.
/// It stays on the wire because the clients already send it, and the reply
/// carries the minted id.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct LeaseCandidate {
    #[serde(default)]
    pub lease_id: u64,
    #[serde(deserialize_with = "canonical_uuid::deserialize")]
    pub holder: Uuid,
    /// The requested lease window in milliseconds. The grant condition is
    /// `lease_ms > 0`; a zero (or absent) window is refused.
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
/// bumped by 1 ms when it collides with the replaced record's value so
/// consecutive takes remain distinguishable.
pub fn taken_at_ms_on_holder_change(prior_taken_at_ms: u64, execution_time: u64) -> u64 {
    if prior_taken_at_ms == execution_time {
        execution_time.saturating_add(1)
    } else {
        execution_time
    }
}

/// The client request, as the gateway's envelope carries it: the
/// replication identity (`message_id`/`client_id`/`request_num`) beside
/// the lock operation. The state machine reads none of the envelope —
/// the leader stamps its own clock and the holder names itself inside the
/// operation's payload.
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
        /// The candidate: the duration the client asks for. Never an
        /// absolute expiry — the leader stamps `expiry` at execution.
        lease: LeaseCandidate,
        /// Optional lock display name; absent leaves the stored name
        /// unchanged. Validated and stored canonically at decode.
        name: Option<String>,
        /// Optional operator labels; absent leaves the stored set
        /// unchanged. Validated and canonically sorted and deduplicated on
        /// receipt.
        labels: Option<Vec<String>>,
        /// The client's send timestamp, purely observational (latency
        /// measurement). NO protocol decision may read it: the grant
        /// condition, the expiry stamp, and the counters never touch it.
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
    /// names no holder. Authorization is owned by the admin edge, never by
    /// the state machine.
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
        /// The leader's execution tick (§7): the client interprets the
        /// lease's expiry against the leader's timeline without assuming
        /// synchronized clocks.
        executed_at: u64,
    },
    Set {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        granted: bool,
        lease: Option<Lease>,
        /// The leader's execution tick (see `Get`).
        executed_at: u64,
    },
    Release {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        released: bool,
        lease: Option<Lease>,
        /// The leader's execution tick (see `Get`).
        executed_at: u64,
    },
    Break {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        /// Whether a stored record was broken. `false` reports that nothing
        /// was stored under the lock: the lock was already free, with no
        /// record to break and no journal event.
        broken: bool,
        /// On a successful break, the post-break keeper record: the bumped
        /// `lease_id`, the retained `name` and `labels`, the nil holder,
        /// and `expiry: 0` — never live.
        lease: Option<Lease>,
        /// The leader's execution tick (see `Get`).
        executed_at: u64,
    },
}

/// The lock table's whole durable state as one serialisable value: every
/// lock id's stored record — its holder, its lease id, its expiry, its
/// window, its display identity and its counters — and every lock id's
/// generation watermark.
///
/// This is the payload the state seam flushes and loads (see
/// [`crate::state`]): the table is the only state the lock service keeps,
/// so its serialisable view IS its state. The watermark is deliberately
/// not defaulted: a snapshot without it is a decode refusal, never an
/// empty one, because a resumed table that read it as absent would hand
/// out a generation the lock had already spent.
#[derive(Serialize, Deserialize, Debug, Clone, Default, PartialEq, Eq)]
pub struct StateSnapshot {
    /// The table itself: lock id -> stored record, ordered so the encoded
    /// bytes are a function of the state and never of the map's insertion
    /// history.
    pub locks: BTreeMap<u64, Lease>,
    /// Every lock's generation watermark: the highest generation the lock
    /// has ever handed out or reserved, which outlives the record a
    /// release removes.
    pub generations: BTreeMap<u64, u64>,
}

/// The display identity and the counters the envelope keeps beside the
/// core's machinery: the sticky identity a SET installs and a release
/// keeps (the durable class's kept frame carries it), and the counters
/// the console's audit reads. Never a protocol decision's input.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct DurableEntry {
    name: Option<String>,
    labels: Option<Vec<String>>,
    taken_at_ms: u64,
    renew_count: u32,
}

/// The lock service: the core's machinery wrapped in the gateway's
/// envelope. Clone and default because a node constructs an empty table
/// and installs a loaded snapshot wholesale.
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
                // Canonical form on receipt: sorted, deduplicated; an empty
                // list is no labels.
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
    ///
    /// The machinery is the core's: the payload crosses into the core's
    /// request shape (the holder's 128 bits become the core's client
    /// identity, the nil pair refused at this door exactly as the core
    /// refuses it at its own), the core executes against the leader's
    /// own wallclock, and the reply crosses back into the envelope's
    /// shape with the display identity and the counters the side table
    /// holds. The [`Transition`] classification — the journal's and the
    /// changed set's feed — derives from the reply and the
    /// pre-execution record.
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
        // The pre-execution facts, read off the core's own state at the
        // moment: the stored record (live or expired — the break's prior
        // and the counters' prior), the live incumbent (the grant's
        // classification prior), and the sticky identity the side table
        // holds.
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

        // The translation: the envelope's request into the core's payload.
        // The candidate's lease_id crosses nowhere — the core's watermark
        // mints every id.
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
                    // The counters: a renewal over the live incumbent
                    // extends the take; a fresh grant installs one.
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
                    // The retired lease's fields, as they were: the journal
                    // record is self-describing exactly like the core's.
                    let transition = incumbent.map(|lease| Transition::Release {
                        lock_id,
                        lease_id: lease.lease_id,
                        holder: client_bytes(&lease.holder),
                        expiry: lease.expiry,
                    });
                    // The counters clear; the identity sticks (the kept
                    // frame the durable class carries).
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
                    // The broken record's fields as they were: the stored
                    // record (a live lease or an expired one), read before
                    // the machinery moved.
                    let transition = stored.map(|lease| Transition::Break {
                        lock_id,
                        lease_id: lease.lease_id,
                        holder: client_bytes(&lease.holder),
                        expiry: lease.expiry,
                    });
                    // The counters clear; the identity sticks — the
                    // keeper the reply carries composes from the side
                    // table after the clear.
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
    /// watermarks. The sticky identity a released lock leaves in the side
    /// table does not hold the table up — a load replaces it wholesale,
    /// exactly as the load always replaced the records.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.core.is_empty()
    }

    /// The table's whole state as one serialisable value, captured at the
    /// moment the caller asks. The state seam's flush payload.
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

    /// Install a loaded snapshot as the table's whole contents.
    ///
    /// Wholesale, not a merge: the loaded value is the pre-stop table, so
    /// a merge against whatever the node holds would fabricate records
    /// the stop never wrote. The seam calls this on the lazy-load path
    /// only, against a table it has just established is empty. The
    /// watermarks install with the records: a resumed table that dropped
    /// them would hand out a generation the lock had already spent.
    pub fn restore(&mut self, snapshot: StateSnapshot) {
        let mut locks = BTreeMap::new();
        let mut durable = BTreeMap::new();
        for (lock_id, lease) in &snapshot.locks {
            locks.insert(
                *lock_id,
                CoreLease {
                    lease_id: lease.lease_id,
                    holder: client_of(lease.holder).unwrap_or_else(|_| {
                        // The keeper record's nil holder: the one identity
                        // the mint refuses and a restored table may lawfully
                        // carry. It re-enters by the serde door — the core's
                        // derived deserialiser constructs the private halves
                        // the public mint refuses.
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

/// The holder's 128 bits as the core's client identity. The nil pair is
/// refused here, at the envelope's own door, exactly as the core refuses
/// it at its own: a request that names no holder names nothing.
fn client_of(holder: Uuid) -> Result<ClientId, serde_json::Error> {
    let bytes = holder.as_bytes();
    let msb = u64::from_be_bytes(bytes[0..8].try_into().expect("the holder's first half"));
    let lsb = u64::from_be_bytes(bytes[8..16].try_into().expect("the holder's second half"));
    ClientId::new(msb, lsb).ok_or_else(|| {
        <serde_json::Error as serde::de::Error>::custom("the nil holder is not a client")
    })
}

/// The nil client identity, by the serde door: the core's derived
/// deserialiser constructs the private halves the public mint refuses.
/// The one lawful use is the keeper record's restore — the record a
/// break writes and a snapshot may carry back.
fn core_nil_client() -> ClientId {
    serde_json::from_value(serde_json::json!({"msb": 0, "lsb": 0}))
        .expect("the core's deserialiser constructs the nil identity")
}

/// The core's client identity as the envelope's holder UUID.
fn holder_uuid(client: ClientId) -> Uuid {
    Uuid::from_bytes(client_bytes(&client))
}

/// The identity's 128 bits, big-endian, as the UUID wire form and the
/// journal's holder field carry them. The halves are the core's private;
/// the serde shape is the one door they cross by — the same door the nil
/// identity's restore uses, in the other direction.
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

/// The holder pair as the serde wire carries it, the door the private
/// halves cross by.
#[derive(Deserialize)]
struct ClientIdBits {
    msb: u64,
    lsb: u64,
}
