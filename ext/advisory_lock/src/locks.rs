use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

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

/// The state machine's stored lease record and the reply shape. Every field
/// is serialized in every reply (`name`/`labels` as `null` when the lock
/// carries none), so observers classify events without diffing successive
/// states.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct Lease {
    pub lease_id: u64,
    pub holder: Uuid,
    pub expiry: u64,
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
    if !alnum(bytes[0]) || !alnum(bytes[bytes.len() - 1]) {
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
    if let Some(name) = name {
        if !valid_name(name) {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "invalid lock name",
            ));
        }
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

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Request {
    Get {
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
    },
    Set {
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
        lease: Lease,
        /// Optional lock display name; absent leaves the stored name
        /// unchanged. Validated and stored canonically at decode.
        name: Option<String>,
        /// Optional operator labels; absent leaves the stored set
        /// unchanged. Validated and canonically sorted and deduplicated on
        /// receipt.
        labels: Option<Vec<String>>,
    },
    Release {
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
        holder: Uuid,
        lease_id: u64,
    },
    /// The privileged holder-break: an unconditional force-release that
    /// names no holder. Authorization is owned by the admin edge, never by
    /// the state machine.
    Break {
        message_id: Uuid,
        client_id: u64,
        request_num: u64,
        lock_id: u64,
    },
}

impl Request {
    pub fn ids(&self) -> (Uuid, u64, u64) {
        match self {
            Self::Get {
                message_id,
                client_id,
                request_num,
                ..
            }
            | Self::Set {
                message_id,
                client_id,
                request_num,
                ..
            }
            | Self::Release {
                message_id,
                client_id,
                request_num,
                ..
            }
            | Self::Break {
                message_id,
                client_id,
                request_num,
                ..
            } => (*message_id, *client_id, *request_num),
        }
    }
}

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

#[derive(Clone, Default)]
pub struct Service {
    locks: BTreeMap<u64, Lease>,
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
        let (response, transition) = match request {
            Request::Get {
                message_id,
                request_num,
                lock_id,
                ..
            } => (
                Response::Get {
                    message_id,
                    request_num,
                    lock_id,
                    lease: self.live(execution_time, lock_id),
                    executed_at: execution_time,
                },
                None,
            ),
            Request::Set {
                message_id,
                request_num,
                lock_id,
                lease,
                name,
                labels,
                ..
            } => {
                let held = self.live(execution_time, lock_id);
                let granted = held
                    .as_ref()
                    .is_none_or(|current| current.holder == lease.holder)
                    && lease.expiry > execution_time;
                if granted {
                    // The replaced record, live or not: it donates the
                    // counters' prior values and the sticky identity.
                    let prior = self.locks.get(&lock_id).cloned();
                    let name = name.or_else(|| prior.as_ref().and_then(|p| p.name.clone()));
                    let labels = labels.or_else(|| prior.as_ref().and_then(|p| p.labels.clone()));
                    let (taken_at_ms, renew_count) = if let Some(incumbent) = &held {
                        // Same-holder renewal over a live lease.
                        (
                            incumbent.taken_at_ms,
                            incumbent.renew_count.saturating_add(1),
                        )
                    } else {
                        let taken_at_ms = match prior.as_ref() {
                            // A same-ms holder change over a replaced record
                            // bumps by 1 ms so consecutive takes stay
                            // distinguishable.
                            Some(prior) => {
                                taken_at_ms_on_holder_change(prior.taken_at_ms, execution_time)
                            }
                            None => execution_time,
                        };
                        (taken_at_ms, 0)
                    };
                    let transition = if held.is_some() {
                        // Same-holder regrant with a prior live lease → Renew.
                        Some(Transition::Renew {
                            lock_id,
                            lease_id: lease.lease_id,
                            holder: *lease.holder.as_bytes(),
                            expiry: lease.expiry,
                        })
                    } else {
                        // No prior live lease → Hold.
                        Some(Transition::Hold {
                            lock_id,
                            lease_id: lease.lease_id,
                            holder: *lease.holder.as_bytes(),
                            expiry: lease.expiry,
                        })
                    };
                    let stored = Lease {
                        lease_id: lease.lease_id,
                        holder: lease.holder,
                        expiry: lease.expiry,
                        name,
                        labels,
                        taken_at_ms,
                        renew_count,
                    };
                    self.locks.insert(lock_id, stored.clone());
                    (
                        Response::Set {
                            message_id,
                            request_num,
                            lock_id,
                            granted: true,
                            lease: Some(stored),
                            executed_at: execution_time,
                        },
                        transition,
                    )
                } else {
                    (
                        Response::Set {
                            message_id,
                            request_num,
                            lock_id,
                            granted: false,
                            lease: held,
                            executed_at: execution_time,
                        },
                        None,
                    )
                }
            }
            Request::Release {
                message_id,
                request_num,
                lock_id,
                holder,
                lease_id,
                ..
            } => {
                let held = self.live(execution_time, lock_id);
                let released = held
                    .as_ref()
                    .is_none_or(|lease| lease.holder == holder && lease.lease_id == lease_id);
                let transition = if released {
                    // Carry the removed lease's fields (tombstone must be
                    // self-describing).
                    held.clone().map(|lease| Transition::Release {
                        lock_id,
                        lease_id: lease.lease_id,
                        holder: *lease.holder.as_bytes(),
                        expiry: lease.expiry,
                    })
                } else {
                    None
                };
                if released {
                    self.locks.remove(&lock_id);
                }
                (
                    Response::Release {
                        message_id,
                        request_num,
                        lock_id,
                        released,
                        lease: if released { None } else { held },
                        executed_at: execution_time,
                    },
                    transition,
                )
            }
            Request::Break {
                message_id,
                request_num,
                lock_id,
                ..
            } => {
                // Unconditional force-release: whatever record the lock
                // table stores is replaced by the bumped, cleared keeper.
                match self.locks.remove(&lock_id) {
                    Some(broken) => {
                        let keeper = Lease {
                            lease_id: broken.lease_id.saturating_add(1),
                            holder: Uuid::nil(),
                            expiry: 0,
                            name: broken.name.clone(),
                            labels: broken.labels.clone(),
                            taken_at_ms: 0,
                            renew_count: 0,
                        };
                        self.locks.insert(lock_id, keeper.clone());
                        (
                            Response::Break {
                                message_id,
                                request_num,
                                lock_id,
                                broken: true,
                                lease: Some(keeper),
                                executed_at: execution_time,
                            },
                            Some(Transition::Break {
                                lock_id,
                                lease_id: broken.lease_id,
                                holder: *broken.holder.as_bytes(),
                                expiry: broken.expiry,
                            }),
                        )
                    }
                    None => (
                        Response::Break {
                            message_id,
                            request_num,
                            lock_id,
                            broken: false,
                            lease: None,
                            executed_at: execution_time,
                        },
                        None,
                    ),
                }
            }
        };
        let bytes = serde_json::to_vec(&response)?;
        Ok((bytes, transition))
    }

    pub fn response_message_id(bytes: &[u8]) -> Option<Uuid> {
        match serde_json::from_slice(bytes).ok()? {
            Response::Get { message_id, .. }
            | Response::Set { message_id, .. }
            | Response::Release { message_id, .. }
            | Response::Break { message_id, .. } => Some(message_id),
        }
    }

    fn live(&self, execution_time: u64, lock_id: u64) -> Option<Lease> {
        self.locks
            .get(&lock_id)
            .cloned()
            .filter(|lease| lease.expiry > execution_time)
    }
}
