use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use uuid::Uuid;

const MAX_NAME_BYTES: usize = 128;
const MAX_LABELS: usize = 8;
const MAX_LABEL_BYTES: usize = 32;

/// The lease triple a client proposes in a SET.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub struct Lease {
    pub lease_id: u64,
    pub holder: Uuid,
    pub expiry: u64,
}

/// The lock-table view returned in GET/SET/BREAK replies: the lease triple
/// plus the replicated metadata (name, labels, lease-age counters).
/// `holder`/`expiry` are null only in the cleared state a BREAK reply echoes.
#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct ExtendedLease {
    pub lease_id: u64,
    pub holder: Option<Uuid>,
    pub expiry: Option<u64>,
    pub name: Option<String>,
    pub labels: Vec<String>,
    pub taken_at_ms: Option<u64>,
    pub renew_count: u32,
}

/// State-machine event classification so telemetry never diffs replies.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Event {
    /// Granted SET over a lock with no recorded holder (missing, released,
    /// or broken).
    Acquire,
    /// Granted SET whose holder equals the recorded holder.
    Renew,
    /// Granted SET that changes the recorded holder (over an expired
    /// incumbent; a live incumbent rejects a foreign holder outright).
    Cas,
    /// Rejected SET (expired candidate, live foreign incumbent, or invalid
    /// name/labels).
    Deny,
    /// RELEASE that actually removed a live lease.
    Release,
    /// BREAK op (always emitted, even when idempotent).
    Break,
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
        #[serde(default)]
        name: Option<String>,
        #[serde(default)]
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
        lease: Option<ExtendedLease>,
    },
    Set {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        granted: bool,
        event: Event,
        lease: Option<ExtendedLease>,
    },
    Release {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        released: bool,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        event: Option<Event>,
        lease: Option<Lease>,
    },
    Break {
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        broken: bool,
        event: Event,
        lease: Option<ExtendedLease>,
    },
}

/// A lock-table row. `holder`/`expiry` are the recorded lease; liveness is
/// `expiry > now`. The row (and its name/labels) survives release, expiry,
/// and break so display metadata and the fencing `lease_id` persist.
/// `taken_at_ms` is the last-taken watermark: it is only *reported* while a
/// holder is recorded, but the stored value is never cleared so that a
/// holder change landing in the same millisecond as the previous one (e.g.
/// release + re-acquire inside one wall-clock ms) still yields a strictly
/// increasing `taken_at_ms`.
#[derive(Clone, Default)]
struct Entry {
    lease_id: u64,
    holder: Option<Uuid>,
    expiry: Option<u64>,
    name: Option<String>,
    labels: Vec<String>,
    taken_at_ms: Option<u64>,
    renew_count: u32,
}

impl Entry {
    fn live(&self, now: u64) -> bool {
        self.holder.is_some() && self.expiry.is_some_and(|expiry| expiry > now)
    }

    fn extended(&self) -> ExtendedLease {
        ExtendedLease {
            lease_id: self.lease_id,
            holder: self.holder,
            expiry: self.expiry,
            name: self.name.clone(),
            labels: self.labels.clone(),
            taken_at_ms: if self.holder.is_some() {
                self.taken_at_ms
            } else {
                None
            },
            renew_count: self.renew_count,
        }
    }
}

/// `^/(?:[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)$`, at most 128 bytes.
fn valid_name(name: &str) -> bool {
    if name.len() > MAX_NAME_BYTES {
        return false;
    }
    let Some(path) = name.strip_prefix('/') else {
        return false;
    };
    !path.is_empty()
        && path.split('/').all(|segment| {
            !segment.is_empty()
                && segment
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
        })
}

/// `^[a-z0-9](?:[-a-z0-9]{0,30}[a-z0-9])?$`, 1–32 bytes.
fn valid_label(label: &str) -> bool {
    let bytes = label.as_bytes();
    if bytes.is_empty() || bytes.len() > MAX_LABEL_BYTES {
        return false;
    }
    let alnum = |byte: u8| byte.is_ascii_lowercase() || byte.is_ascii_digit();
    alnum(bytes[0])
        && alnum(bytes[bytes.len() - 1])
        && bytes.iter().all(|&byte| alnum(byte) || byte == b'-')
}

/// Validate, deduplicate, and canonically sort labels; at most 8 unique.
fn canonical_labels(labels: &[String]) -> Option<Vec<String>> {
    if labels.iter().any(|label| !valid_label(label)) {
        return None;
    }
    let mut unique = labels.to_vec();
    unique.sort();
    unique.dedup();
    if unique.len() > MAX_LABELS {
        return None;
    }
    Some(unique)
}

#[derive(Clone, Default)]
pub struct Service {
    locks: BTreeMap<u64, Entry>,
}

impl Service {
    pub fn decode(bytes: &[u8]) -> Result<Request, serde_json::Error> {
        serde_json::from_slice(bytes)
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
    ) -> Result<Vec<u8>, serde_json::Error> {
        let request = Self::decode(payload)?;
        if request.ids() != (message_id, client_id, request_num) {
            return Err(<serde_json::Error as serde::de::Error>::custom(
                "replicated envelope does not match lock payload",
            ));
        }
        let response = match request {
            Request::Get {
                message_id,
                request_num,
                lock_id,
                ..
            } => {
                let entry = self.observed(lock_id, execution_time);
                Response::Get {
                    message_id,
                    request_num,
                    lock_id,
                    lease: entry
                        .filter(|entry| entry.live(execution_time))
                        .map(Entry::extended),
                }
            }
            Request::Set {
                message_id,
                request_num,
                lock_id,
                lease,
                name,
                labels,
                ..
            } => self.set(
                message_id,
                request_num,
                lock_id,
                lease,
                name,
                labels,
                execution_time,
            ),
            Request::Release {
                message_id,
                request_num,
                lock_id,
                holder,
                lease_id,
                ..
            } => {
                let entry = self.observed(lock_id, execution_time);
                let live = entry.filter(|entry| entry.live(execution_time));
                let removed = live.is_some_and(|entry| {
                    entry.holder == Some(holder) && entry.lease_id == lease_id
                });
                let released = removed || live.is_none();
                let incumbent = live.map(|entry| Lease {
                    lease_id: entry.lease_id,
                    holder: entry.holder.unwrap_or_default(),
                    expiry: entry.expiry.unwrap_or_default(),
                });
                if removed {
                    if let Some(entry) = self.locks.get_mut(&lock_id) {
                        entry.holder = None;
                        entry.expiry = None;
                        entry.renew_count = 0;
                    }
                }
                Response::Release {
                    message_id,
                    request_num,
                    lock_id,
                    released,
                    event: removed.then_some(Event::Release),
                    lease: if removed { None } else { incumbent },
                }
            }
            Request::Break {
                message_id,
                request_num,
                lock_id,
                ..
            } => {
                let entry = self.observed(lock_id, execution_time);
                let held = entry.is_some_and(|entry| entry.live(execution_time));
                let lease = if held {
                    let entry = self.locks.get_mut(&lock_id).expect("held entry exists");
                    entry.lease_id = entry.lease_id.wrapping_add(1);
                    entry.holder = None;
                    entry.expiry = None;
                    entry.renew_count = 0;
                    Some(entry.extended())
                } else {
                    None
                };
                Response::Break {
                    message_id,
                    request_num,
                    lock_id,
                    broken: held,
                    event: Event::Break,
                    lease,
                }
            }
        };
        let bytes = serde_json::to_vec(&response)?;
        Ok(bytes)
    }

    pub fn response_message_id(bytes: &[u8]) -> Option<Uuid> {
        match serde_json::from_slice(bytes).ok()? {
            Response::Get { message_id, .. }
            | Response::Set { message_id, .. }
            | Response::Release { message_id, .. }
            | Response::Break { message_id, .. } => Some(message_id),
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn set(
        &mut self,
        message_id: Uuid,
        request_num: u64,
        lock_id: u64,
        lease: Lease,
        name: Option<String>,
        labels: Option<Vec<String>>,
        execution_time: u64,
    ) -> Response {
        // Metadata is validated before any state read so an invalid SET never
        // changes the lock table (the expiry observation below is a no-op for
        // locks this SET cannot touch anyway, but validate first regardless).
        let canonical = match (&name, &labels) {
            (Some(name), _) if !valid_name(name) => None,
            (_, Some(labels)) => canonical_labels(labels).map(Some),
            _ => Some(None),
        };
        let entry = self.observed(lock_id, execution_time);
        let live = entry.filter(|entry| entry.live(execution_time));
        let granted = canonical.is_some()
            && live.is_none_or(|entry| entry.holder == Some(lease.holder))
            && lease.expiry > execution_time;
        let event = if !granted {
            Event::Deny
        } else {
            match entry.and_then(|entry| entry.holder) {
                None => Event::Acquire,
                Some(holder) if holder == lease.holder => Event::Renew,
                Some(_) => Event::Cas,
            }
        };
        let reply_lease = if granted {
            let entry = self.locks.entry(lock_id).or_default();
            if let Some(name) = name {
                entry.name = Some(name);
            }
            if let Some(Some(labels)) = canonical {
                entry.labels = labels;
            }
            match event {
                Event::Renew => {
                    entry.renew_count = entry.renew_count.saturating_add(1);
                }
                _ => {
                    entry.taken_at_ms = Some(match entry.taken_at_ms {
                        // Holder change in the same millisecond as the previous
                        // one still yields a strictly increasing timestamp.
                        Some(previous) if previous == execution_time => execution_time + 1,
                        _ => execution_time,
                    });
                    entry.renew_count = 0;
                }
            }
            entry.lease_id = lease.lease_id;
            entry.holder = Some(lease.holder);
            entry.expiry = Some(lease.expiry);
            Some(entry.extended())
        } else {
            live.map(Entry::extended)
        };
        Response::Set {
            message_id,
            request_num,
            lock_id,
            granted,
            event,
            lease: reply_lease,
        }
    }

    /// Fetch an entry, applying the deterministic expiry observation: an
    /// expired lease frees the lock and zeroes `renew_count` while keeping
    /// the recorded holder (for renew/cas classification), the name/labels,
    /// and the last-taken watermark.
    fn observed(&mut self, lock_id: u64, now: u64) -> Option<&Entry> {
        if let Some(entry) = self.locks.get_mut(&lock_id) {
            if entry.holder.is_some() && entry.expiry.is_some_and(|expiry| expiry <= now) {
                entry.renew_count = 0;
            }
        }
        self.locks.get(&lock_id)
    }
}
