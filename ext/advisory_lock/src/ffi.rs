//! Host-side FFI adapter between the LuaJIT host and the uVRR core
//! (vrr-core @ 0fc6380).
//!
//! Concrete core: `Replica<SegmentedLog, WeightedMajority>` running
//! `Stability::Volatile` — nothing is persisted but the boot nonce file, so
//! restart is `Replica::provision` over an empty journal. The core has no
//! amnesia-recovery protocol: a fenced boot starts clean and the ordinary
//! bootstrap machinery (tick self-promotion of the genesis primary, view
//! adoption on the primary's messages) brings the node into the protocol.
//! `lunet_lock_node_recover` therefore drives that bootstrap tick while the
//! node is fenced; the full restart story (identity bump +
//! `Input::Reincarnate`) is future work and the adapter does not pretend
//! otherwise.
//!
//! Adapter policies the core deliberately does not own:
//!
//! - **Tick clock.** The adapter owns the tick clock: a monotonic
//!   nondecreasing milliseconds-since-Unix-epoch value, clamped per node so
//!   a clock regression never reaches the core. ABI functions take no `at`
//!   parameter. Fenced-boot drives take their tick from the durable nonce
//!   file (`next_nonce`, fsync+rename+dir-sync) — the nonce no longer
//!   authorizes a recovery protocol (the core has none); it remains the
//!   per-node durable tick source so per-node monotonicity holds across
//!   the two sources (S4 discipline carried over).
//! - **Identity.** Membership is the admin-assigned deployment descriptor:
//!   the member buffer carries NUL-separated `<u32-id>:<name>` entries, in
//!   the descriptor's line order. Each id is the member's live `NodeId`
//!   (sparse, admin-assigned, never recycled), and the buffer order is the
//!   genesis succession sequence (`primary(v) = order[v mod N]`). Member ids
//!   are the ABI's peer addresses: `receive`'s `from`, send outputs' `to`,
//!   and the leader outs all carry member ids; the host maps id -> endpoint
//!   through the descriptor. `own` is matched by name. A post-genesis
//!   (joined-later) entry carries a `:j` suffix: `<u32-id>:<name>:j`. Such
//!   entries are outside the genesis order — the core's `provision` refuses
//!   an `own` that is not a founding member — so a node whose `own` names a
//!   `:j` entry boots as a JOINER: `Replica::reopen` over the deployment's
//!   genesis (the entries `provision` installs, mirrored byte-for-byte, plus
//!   the era table folded from them), fenced `Recovering`, addressed but
//!   outside every configuration until a committed `Join` admits it. Upstream
//!   has no fresh-node catch-up at this commit: the establishing operation's
//!   fan-out reaches configuration members only and the `GetState` serving
//!   gate serves configuration members only, so the joiner evaluates only
//!   the era-1 traffic its genesis table covers and drops everything past it
//!   by name — the same boundary upstream's own learner corpus states
//!   (§10 acquisition, future work). The joiner fabricates nothing.
//! - **Reconfiguration.** `lunet_lock_node_reconfigure` drives
//!   `Input::Reconfigure { op, pivot }` (Join at weight 0 / Increment /
//!   Leave at weight 0) on the current primary through the ordinary
//!   plan/publish pipeline. The host derives the non-stop overlap pivot with
//!   the core's own `construct_pivot` (`config(e+1)` probed exactly the way
//!   the planner probes it, at `accepted().next()`); a `None` result — or a
//!   failed probe — drives the stop-the-world fallback with `pivot: None`,
//!   which upstream defines as a latency outcome, never an error. The era
//!   advances exactly at the establishing operation's commit; refusals never
//!   enter the log. Error mapping: `NotPrimary` is the one actionable code
//!   (NOT_LEADER, re-forward to the named primary); every other refusal
//!   (transition-outstanding gates, fold and closed-intersection gates,
//!   view-exhaustion) is internal and reports SERVICE.
//! - **Operation identity.** `OperationId` is derived from the client
//!   request's 16-byte message_id: `msb` = first 8 bytes, `lsb` = last 8,
//!   both big-endian (the core's wire order). The operation payload IS the
//!   client JSON bytes; `Service::decode` parses the ids out of it.
//! - **Exactly-once (B2).** The core never deduplicates and never answers a
//!   proposal. The adapter caches each executed reply by message_id and
//!   replays the cached bytes for a duplicate request or a duplicated
//!   committed operation, without re-executing the Service. A reply output
//!   is queued only for an operation this node proposed (locally pending).
//! - **Timers.** The tag has a single liveness input, `Input::Tick`; both
//!   `node_idle` (heartbeat) and `node_leader_timeout` (election) drive it.
//!   `ViewChangeKnobs::primary_timeout` is `PRIMARY_TIMEOUT_MS` below;
//!   `view_change_budget` is `MAX_DATAGRAM` so a core-built suffix never
//!   exceeds one datagram.
//! - **Peer payload gate.** The core carries operation payloads opaque and
//!   validates none of them (B2), so the adapter re-checks every peer-carried
//!   operation entry (Prepare / DoViewChange / StartView / NewState) with
//!   `Service` before the message reaches the core — the same gate the old
//!   adapter called `valid_message_payload`.
//! - **Lock-event journal.** An optional append-only binary journal records
//!   every committed lock transition (Hold/Renew/Release) to rolling files
//!   under a per-replica directory. Enabled when `lunet_lock_node_new`
//!   receives a non-empty `journal_dir`; disabled otherwise. Journal errors
//!   log to stderr and disable journaling for the process; the node keeps
//!   serving. See `journal.rs` for the record and metafile formats.
//!
//! `node_next` output contract (kinds): 1 = send (unicast to `to`; era, view
//! and slot report the encoded message's wire header), 2 = reply (message_id
//! and response bytes; to/era/view/slot are zero). Return 1 when an output
//! was produced, 0 when the queue is empty, negative on error. When the next
//! output's bytes exceed `capacity`, the call reports the needed size in
//! `out_len`, returns TOO_LARGE and does NOT pop the queue.
//!
//! # ABI surface
//!
//! ```text
//! lunet_lock_node_new(
//!     members_len: usize, members_data: *const u8,
//!     own_len: usize, own_data: *const u8,
//!     state_len: usize, state_data: *const u8,
//!     journal_dir_len: usize, journal_dir_data: *const u8,
//!     roll_bytes: u32,
//!     out: *mut *mut c_void,
//! ) -> i32
//! ```
//!
//! Empty `journal_dir` (len=0 or data points to empty string) disables the
//! journal. `roll_bytes` is the byte threshold at which the current event
//! file rolls to a final name with an atomic metafile. `members_data` holds
//! NUL-separated `<u32-id>:<name>` entries in descriptor (genesis succession)
//! order, with post-genesis entries suffixed `:j` (see Identity above);
//! `own_data` is the local member's name.

use crate::journal::{self, Journal as LockJournal, JournalEvent};
use crate::locks::{Service, Transition};
use std::collections::{HashMap, VecDeque};
use std::ffi::{OsString, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use vrr::configuration::{EraTable, INIT_SLOT, MAX_MEMBERS, SystemOperation, VOID_SLOT};
use vrr::effects::{Effect, Stability};
use vrr::ids::{Era, NodeId, Operation, OperationId, Slot, Tick, View, ViewId};
use vrr::journal::{Journal, LogEntry, Payload, SegmentedLog};
use vrr::message::{Body, Message};
use vrr::progress::Status;
use vrr::quorum::{WeightedMajority, construct_pivot};
use vrr::replica::{
    Input, PersistedProgress, Pivot, PlanRefusal, PublishOutcome, Replica, TimedInput,
    ViewChangeKnobs,
};
use vrr::wire::{Pack, Unpack, UnpackError};

const OK: i32 = 0;
const INVALID: i32 = -1;
const CONFIG: i32 = -2;
const CLIENT_JSON: i32 = -4;
const VRR_MESSAGE: i32 = -5;
const TOO_LARGE: i32 = -6;
const SERVICE: i32 = -7;
const NOT_LEADER: i32 = -8;
const FAULTED: i32 = -9;
const PANIC: i32 = -127;

const OUTPUT_SEND: u32 = 1;
const OUTPUT_REPLY: u32 = 2;

/// Host packetization bound (W5: the core owns no size limit). One IPv4/IPv6
/// UDP datagram, matching `transport.tl`.
const MAX_DATAGRAM: usize = 65507;

/// Ticks (milliseconds) of primary silence before a backup fences into the
/// next view. Host policy; correctness never depends on it.
const PRIMARY_TIMEOUT_MS: u64 = 5000;

/// Leader/primary unknown (era outside the core's three-era retention
/// window, or the void configuration): the value status and
/// leader-for-view report in that case.
const LEADER_UNKNOWN: u32 = u32::MAX;

struct Queued {
    kind: u32,
    to: u32,
    era: u32,
    view: u32,
    slot: u64,
    message_id: [u8; 16],
    bytes: Vec<u8>,
}

type Core = Replica<SegmentedLog, WeightedMajority>;

pub struct Node {
    replica: Core,
    outputs: VecDeque<Queued>,
    service: Service,
    /// message_id bytes -> cached response bytes, for duplicate replay (B2).
    replies: HashMap<[u8; 16], Vec<u8>>,
    /// Operations proposed through this node's `request` entry and not yet
    /// applied: `OperationId` -> message_id bytes. Only these get replies.
    pending: HashMap<OperationId, [u8; 16]>,
    nonce_path: PathBuf,
    last_tick: u64,
    poisoned: bool,
    /// Append-only lock-event journal. `None` when journaling is disabled
    /// (empty journal_dir at construction) or after a journal error.
    journal: Option<LockJournal>,
}

impl Node {
    /// The next monotonic tick from the adapter-owned ms clock (never
    /// decreasing per node, even across a wall-clock regression).
    fn tick(&mut self) -> Result<u64, i32> {
        let now = unix_millis()?;
        self.last_tick = self.last_tick.max(now);
        Ok(self.last_tick)
    }

    /// The next durable fenced-boot tick (S4 discipline: the nonce is the
    /// tick source), clamped into the same monotone sequence as the ms
    /// clock.
    fn recovery_tick(&mut self) -> Result<u64, i32> {
        let nonce = next_nonce(&self.nonce_path).map_err(|_| CONFIG)?;
        self.last_tick = self.last_tick.max(nonce);
        Ok(self.last_tick)
    }

    /// One plan/publish/effects cycle, looping on the host acknowledgements
    /// (`Input::Applied`) the effects require, until the core goes quiet.
    /// Under `Stability::Volatile` every publish is `Published`; a `Parked`
    /// outcome is an invariant/API mismatch, so the node is poisoned rather
    /// than allowed to fabricate a `StabilityResult::Stable`.
    fn drive(&mut self, event: Input) -> i32 {
        let at = match self.tick() {
            Ok(at) => at,
            Err(error) => return error,
        };
        self.drive_at(at, event)
    }

    /// A drive whose outer input's tick the caller already chose (fenced
    /// boot nonce ticks); feedback inputs inside the loop still sample the
    /// clock.
    fn drive_at(&mut self, at: u64, event: Input) -> i32 {
        if self.poisoned {
            return SERVICE;
        }
        let mut pending = vec![TimedInput {
            at: Tick(at),
            event,
        }];
        while let Some(input) = pending.pop() {
            if self.poisoned {
                return SERVICE;
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                let planned = self
                    .replica
                    .plan(&input, &self.replica.journal().view())
                    .map_err(plan_error)?;
                match self.replica.publish(planned).map_err(|_| SERVICE)? {
                    PublishOutcome::Published { effects, .. } => Ok(effects),
                    PublishOutcome::Parked { .. } => Err(FAULTED),
                }
            }));
            let effects = match result {
                Ok(Ok(effects)) => effects,
                Ok(Err(error)) => {
                    if error == FAULTED {
                        // Parked under Volatile: the durability handshake the
                        // core expects does not exist in this host. Poison.
                        self.poisoned = true;
                        self.outputs.clear();
                        return SERVICE;
                    }
                    return error;
                }
                Err(_) => {
                    self.poisoned = true;
                    self.outputs.clear();
                    return PANIC;
                }
            };
            for effect in effects {
                match self.apply_effect(effect, &mut pending) {
                    Ok(()) => {}
                    Err(error) => {
                        if error == PANIC || error == FAULTED {
                            self.poisoned = true;
                            self.outputs.clear();
                            return if error == PANIC { PANIC } else { SERVICE };
                        }
                        return error;
                    }
                }
            }
        }
        OK
    }

    fn apply_effect(&mut self, effect: Effect, pending: &mut Vec<TimedInput>) -> Result<(), i32> {
        match effect {
            Effect::Send { to, message, .. } => {
                let size = message.packed_len();
                if size > MAX_DATAGRAM {
                    return Err(TOO_LARGE);
                }
                let mut bytes = vec![0u8; size];
                let written = message.pack_into(&mut bytes).map_err(|_| VRR_MESSAGE)?;
                bytes.truncate(written);
                let header = message.header;
                self.outputs.push_back(Queued {
                    kind: OUTPUT_SEND,
                    to: to.0,
                    era: header.view.era.0,
                    view: header.view.view.0,
                    slot: header.slot.0,
                    message_id: [0; 16],
                    bytes,
                });
                Ok(())
            }
            Effect::Apply {
                slot,
                operation_id,
                payload,
            } => {
                let message_id = operation_id_bytes(operation_id);
                let (response, transition) = if let Some(cached) = self.replies.get(&message_id) {
                    // Duplicate committed operation: replay the cached reply,
                    // never re-execute (B2). No journal append for duplicates.
                    (cached.clone(), None)
                } else {
                    // The committed entry carries only the operation identity
                    // and the opaque payload; the client ids ride inside the
                    // JSON and the execution time is the host's clock — the
                    // old core's entry-carried fields have no equivalent.
                    let request = Service::decode(&payload).map_err(|_| SERVICE)?;
                    let (id, client_id, request_num) = request.ids();
                    if id.as_bytes() != &message_id {
                        return Err(SERVICE);
                    }
                    let execution_time = unix_millis()?;
                    let (bytes, transition) = self
                        .service
                        .execute(id, client_id, request_num, execution_time, &payload)
                        .map_err(|_| SERVICE)?;
                    self.replies.insert(message_id, bytes.clone());
                    (bytes, transition)
                };
                // Append a journal event for first-execution transitions
                // only (never on cached duplicate replay). Journal errors
                // disable journaling for the process; the node keeps serving.
                if let Some(transition) = transition {
                    if let Some(journal) = self.journal.as_mut() {
                        let ts = unix_millis().unwrap_or(0);
                        let event = match &transition {
                            Transition::Hold {
                                lock_id,
                                lease_id,
                                holder,
                                expiry,
                            } => JournalEvent {
                                kind: journal::KIND_HOLD,
                                ts,
                                lock_id: *lock_id,
                                lease_id: *lease_id,
                                holder: *holder,
                                expiry: *expiry,
                            },
                            Transition::Renew {
                                lock_id,
                                lease_id,
                                holder,
                                expiry,
                            } => JournalEvent {
                                kind: journal::KIND_RENEW,
                                ts,
                                lock_id: *lock_id,
                                lease_id: *lease_id,
                                holder: *holder,
                                expiry: *expiry,
                            },
                            Transition::Release {
                                lock_id,
                                lease_id,
                                holder,
                                expiry,
                            } => JournalEvent {
                                kind: journal::KIND_RELEASE,
                                ts,
                                lock_id: *lock_id,
                                lease_id: *lease_id,
                                holder: *holder,
                                expiry: *expiry,
                            },
                        };
                        if let Err(e) = journal.append(&event) {
                            eprintln!(
                                "lunet-advisory-lock: journal append failed ({e}); \
                                 journaling disabled for this process"
                            );
                            self.journal = None;
                        }
                    }
                }
                if let Some(message_id) = self.pending.remove(&operation_id) {
                    if response.len() > MAX_DATAGRAM {
                        return Err(TOO_LARGE);
                    }
                    self.outputs.push_back(Queued {
                        kind: OUTPUT_REPLY,
                        to: 0,
                        era: 0,
                        view: 0,
                        slot: 0,
                        message_id,
                        bytes: response,
                    });
                }
                let at = self.tick()?;
                pending.push(TimedInput {
                    at: Tick(at),
                    event: Input::Applied { slot },
                });
                Ok(())
            }
            Effect::Persist(_) => {
                // Unreachable under Volatile (publish never releases Persist
                // without parking); treated as an invariant breach.
                Err(FAULTED)
            }
        }
    }

    /// The fenced-boot drive. The core has no recovery protocol (the
    /// classic `Input::Recover` exchange was removed upstream): a fenced
    /// node starts clean, and the only protocol lever is `Input::Tick` —
    /// the genesis primary self-promotes on it, and the primary's messages
    /// adopt the fenced backups. The durable nonce remains the tick source
    /// so the drive stays monotone across restarts. The full restart story
    /// (identity bump + `Input::Reincarnate`) is future work; this drive
    /// never fabricates recovered state.
    fn recover(&mut self) -> i32 {
        if self.poisoned {
            return SERVICE;
        }
        let at = match self.recovery_tick() {
            Ok(at) => at,
            Err(error) => return error,
        };
        self.drive_at(at, Input::Tick)
    }

    /// The current view's primary as a positional index, via the public
    /// route: progress config -> era record -> configuration primary.
    fn primary_index(&self) -> u32 {
        let current = self.replica.progress().current();
        self.leader_for(current.era.0, current.view.0)
    }

    fn leader_for(&self, era: u32, view: u32) -> u32 {
        let Some(record) = self.replica.progress().config().record(vrr::ids::Era(era)) else {
            return LEADER_UNKNOWN;
        };
        match record.config.primary(vrr::ids::View(view)) {
            Some(node) => node.0,
            None => LEADER_UNKNOWN,
        }
    }
}

/// Map a plan refusal onto the ABI error codes. `NotPrimary` is the one a
/// caller can act on (re-forward to the named primary); the rest — the
/// fault, the reconfiguration gates, the outstanding-transition bookkeeping
/// — are internal states the host cannot repair in place.
fn plan_error(rejection: PlanRefusal) -> i32 {
    match rejection {
        PlanRefusal::NotPrimary { .. } => NOT_LEADER,
        PlanRefusal::Faulted(_) => FAULTED,
        _ => SERVICE,
    }
}

/// `OperationId` from the request's 16-byte message_id: first 8 bytes are
/// `msb`, last 8 `lsb`, both big-endian (the core's wire order).
fn operation_id(message_id: [u8; 16]) -> OperationId {
    OperationId {
        msb: u64::from_be_bytes(message_id[..8].try_into().expect("8 bytes")),
        lsb: u64::from_be_bytes(message_id[8..].try_into().expect("8 bytes")),
    }
}

fn operation_id_bytes(id: OperationId) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&id.msb.to_be_bytes());
    bytes[8..].copy_from_slice(&id.lsb.to_be_bytes());
    bytes
}

/// The peer-carried payload gate: every operation entry in an incoming
/// message must decode as a Service request whose message_id matches the
/// operation identity the entry claims. The core carries payloads opaque
/// (B2), so this host-side check is the only validation they get.
fn valid_message_payloads(message: &Message) -> bool {
    fn valid_entry(entry: &vrr::journal::LogEntry) -> bool {
        match &entry.payload {
            Payload::Operation { id, payload } => {
                let Ok(request) = Service::decode(payload) else {
                    return false;
                };
                let (message_id, client_id, request_num) = request.ids();
                message_id.as_bytes() == &operation_id_bytes(*id)
                    && Service::validate(message_id, client_id, request_num, payload)
            }
            Payload::System(_) => true,
        }
    }
    fn valid_entries(entries: &[vrr::journal::LogEntry]) -> bool {
        entries.iter().all(valid_entry)
    }
    match &message.body {
        Body::Prepare { entry, .. } => valid_entry(entry),
        Body::DoViewChange { suffix, .. } => valid_entries(suffix),
        Body::StartView { suffix, .. } => valid_entries(suffix),
        Body::NewState { entries, .. } => valid_entries(entries),
        _ => true,
    }
}

/// One parsed member-buffer entry: the admin-assigned id, the name, and
/// whether the member joined after genesis (the `<id>:<name>:j` grammar).
/// A `:j` entry is outside the genesis succession order; it registers the
/// id->name mapping and, when it is `own`, selects the joiner boot.
struct MemberEntry {
    id: u32,
    name: String,
    joined: bool,
}

/// Reconfiguration operation codes for `lunet_lock_node_reconfigure`.
const RECONFIGURE_JOIN: u32 = 1;
const RECONFIGURE_INCREMENT: u32 = 2;
const RECONFIGURE_LEAVE: u32 = 3;

/// `lunet_lock_node_reconfigure`'s Join position sentinel: append at the
/// core's current succession end (resolved against the folded configuration).
const POSITION_APPEND: u32 = u32::MAX;

fn parse_member_entry(entry: &[u8]) -> Option<MemberEntry> {
    let text = std::str::from_utf8(entry).ok()?;
    let (id_text, rest) = text.split_once(':')?;
    let id = id_text.parse::<u32>().ok()?;
    let (name, joined) = match rest.split_once(':') {
        Some((name, "j")) => (name, true),
        Some(_) => return None,
        None => (rest, false),
    };
    Some(MemberEntry {
        id,
        name: name.to_owned(),
        joined,
    })
}

/// The joiner boot (see the module's Identity note): a later life over the
/// deployment's genesis. The journal and the era table hold exactly the
/// entries `provision` installs — mirrored byte-for-byte so the joiner's
/// slot-2 entry equals the cluster's committed one — and `reopen` fences the
/// node to `Recovering` regardless. Nothing about a restart is pretended: the
/// node holds the shared committed root and nothing else.
fn joiner_replica(
    own: NodeId,
    genesis_order: Vec<NodeId>,
    knobs: ViewChangeKnobs,
) -> Result<Core, i32> {
    let table = EraTable::genesis()
        .extend(&SystemOperation::Void, VOID_SLOT)
        .and_then(|table| {
            table.extend(
                &SystemOperation::Init {
                    order: genesis_order.clone(),
                },
                INIT_SLOT,
            )
        })
        .map_err(|_| CONFIG)?;
    let era = table.current().era;
    let genesis = [
        LogEntry {
            slot: VOID_SLOT,
            era: Era::INITIAL,
            payload: Payload::System(SystemOperation::Void),
        },
        LogEntry {
            slot: INIT_SLOT,
            era,
            payload: Payload::System(SystemOperation::Init {
                order: genesis_order,
            }),
        },
    ];
    let mut journal = SegmentedLog::new();
    if journal.install_suffix(VOID_SLOT, &genesis).is_err() {
        return Err(CONFIG);
    }
    let view = ViewId {
        era,
        view: View::INITIAL,
    };
    let persisted = PersistedProgress {
        current: view,
        retained: view,
        status: Status::Recovering,
        accepted: INIT_SLOT,
        committed: INIT_SLOT,
        applied: INIT_SLOT,
        checkpoint: Slot::NONE,
        revision: 0,
        fault: None,
    };
    Replica::reopen(
        own,
        WeightedMajority,
        journal,
        persisted,
        Arc::new(table),
        Stability::Volatile,
        knobs,
    )
    .map_err(|_| CONFIG)
}

unsafe fn bytes<'a>(len: usize, data: *const u8) -> Result<&'a [u8], i32> {
    if data.is_null() {
        return if len == 0 { Ok(&[]) } else { Err(INVALID) };
    }
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

fn unix_millis() -> Result<u64, i32> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SERVICE)
        .and_then(|duration| u64::try_from(duration.as_millis()).map_err(|_| SERVICE))
}

fn initialize_nonce(path: &Path) -> std::io::Result<bool> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(b"0\n")?;
            file.sync_all()?;
            sync_parent(path)?;
            Ok(false)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_nonce(path)?;
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

fn read_nonce(path: &Path) -> std::io::Result<u64> {
    fs::read_to_string(path)?
        .trim()
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid nonce"))
}

fn next_nonce(path: &Path) -> std::io::Result<u64> {
    let nonce = read_nonce(path)?
        .checked_add(1)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "nonce overflow"))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut temporary = OsString::from(".");
    temporary.push(base);
    temporary.push(format!(".tmp-{}-{unique}", std::process::id()));
    let temporary = parent.join(temporary);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| {
        writeln!(file, "{nonce}")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map(|_| nonce)
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    sync_dir(parent)
}

// Fsyncing the containing directory after create/rename is a POSIX crash-
// consistency idiom (persist the new directory entry, not just the file's
// data). Windows has no equivalent: `File::open` on a directory fails with
// ERROR_ACCESS_DENIED (std does not set FILE_FLAG_BACKUP_SEMANTICS), and NTFS
// does not require or support an explicit directory fsync for this guarantee
// the way POSIX filesystems do. Other Rust crates with the same durability
// pattern (e.g. `atomicwrites`) no-op this step on Windows for the same
// reason; do the same here rather than fail every nonce write on Windows.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(windows)]
fn sync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

fn guarded(run: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(run)).unwrap_or(PANIC)
}

/// Conservative worst-case size of the Prepare datagram a proposal of
/// `payload_len` bytes produces: 20-byte header + 1-byte body discriminant +
/// entry (8 slot + 4 era + 1 payload discriminant + 16 operation id + 4
/// opaque length prefix + payload) + 8 piggybacked committed slot. Exact per
/// the wire module's fixed-width big-endian layout (W3/W4); stated as a
/// margin over the payload so admission can refuse before proposing.
const PREPARE_OVERHEAD: usize = 20 + 1 + 8 + 4 + 1 + 16 + 4 + 8;

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_new(
    members_len: usize,
    members_data: *const u8,
    own_len: usize,
    own_data: *const u8,
    state_len: usize,
    state_data: *const u8,
    journal_dir_len: usize,
    journal_dir_data: *const u8,
    roll_bytes: u32,
    out: *mut *mut c_void,
) -> i32 {
    guarded(|| {
        if out.is_null() {
            return INVALID;
        }
        let Ok(members_data) = (unsafe { bytes(members_len, members_data) }) else {
            return INVALID;
        };
        let Ok(own_data) = (unsafe { bytes(own_len, own_data) }) else {
            return INVALID;
        };
        let Ok(state_data) = (unsafe { bytes(state_len, state_data) }) else {
            return INVALID;
        };
        let Ok(journal_dir_data) = (unsafe { bytes(journal_dir_len, journal_dir_data) }) else {
            return INVALID;
        };
        // Member entries are "<u32-id>:<name>"; a post-genesis (joined)
        // entry is "<u32-id>:<name>:j". The plain-entry order is the
        // descriptor's genesis succession sequence and each id is the
        // member's live NodeId.
        let Some(members) = members_data
            .split(|byte| *byte == 0)
            .map(parse_member_entry)
            .collect::<Option<Vec<_>>>()
        else {
            return CONFIG;
        };
        let Ok(own) = std::str::from_utf8(own_data) else {
            return CONFIG;
        };
        let Ok(state) = std::str::from_utf8(state_data) else {
            return CONFIG;
        };
        let journal_dir = if journal_dir_data.is_empty() {
            None
        } else {
            match std::str::from_utf8(journal_dir_data) {
                Ok(s) if !s.is_empty() => Some(s),
                _ => None,
            }
        };
        if state.is_empty()
            || members.is_empty()
            || members.len() > MAX_MEMBERS as usize
            || members.iter().any(|member| member.name.is_empty())
        {
            return CONFIG;
        }
        let mut unique_ids = members.iter().map(|member| member.id).collect::<Vec<_>>();
        unique_ids.sort_unstable();
        unique_ids.dedup();
        let mut unique_names = members
            .iter()
            .map(|member| member.name.clone())
            .collect::<Vec<_>>();
        unique_names.sort();
        unique_names.dedup();
        if unique_ids.len() != members.len() || unique_names.len() != members.len() {
            return CONFIG;
        }
        let Some(own_member) = members.iter().find(|member| member.name == own) else {
            return CONFIG;
        };
        // Explicit admin-assigned identity: the descriptor's genesis (plain)
        // ids in buffer order are both the live NodeIds of the founding
        // membership and the genesis succession sequence.
        let genesis_order: Vec<NodeId> = members
            .iter()
            .filter(|member| !member.joined)
            .map(|member| NodeId(member.id))
            .collect();
        let knobs = ViewChangeKnobs {
            primary_timeout: PRIMARY_TIMEOUT_MS,
            view_change_budget: MAX_DATAGRAM,
        };
        let own_id = NodeId(own_member.id);
        let replica = if own_member.joined {
            // A post-genesis member boots as a joiner: a later life over the
            // deployment's genesis, fenced until the stream proves currency.
            match joiner_replica(own_id, genesis_order, knobs) {
                Ok(replica) => replica,
                Err(_) => return CONFIG,
            }
        } else {
            match Replica::provision(
                own_id,
                genesis_order,
                WeightedMajority,
                SegmentedLog::new(),
                Stability::Volatile,
                knobs,
            ) {
                Ok(replica) => replica,
                Err(_) => return CONFIG,
            }
        };
        let nonce_path = PathBuf::from(state);
        if initialize_nonce(&nonce_path).is_err() {
            return CONFIG;
        }
        let journal = if let Some(dir) = journal_dir {
            match LockJournal::open(Path::new(dir), roll_bytes as u64) {
                Ok(j) => Some(j),
                Err(e) => {
                    eprintln!(
                        "lunet-advisory-lock: journal open failed ({e}); \
                         journaling disabled for this process"
                    );
                    None
                }
            }
        } else {
            None
        };
        let node = Node {
            replica,
            outputs: VecDeque::new(),
            service: Service::default(),
            replies: HashMap::new(),
            pending: HashMap::new(),
            nonce_path,
            last_tick: 0,
            poisoned: false,
            journal,
        };
        // Clean start: provision leaves the node fenced `Recovering` with an
        // empty output queue. There is no boot recovery handshake any more;
        // the host's fenced-boot drive (`lunet_lock_node_recover`, a tick)
        // and the primary's messages bring the node into the protocol.
        unsafe { *out = Box::into_raw(Box::new(node)).cast() };
        OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_free(node: *mut c_void) {
    if !node.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
            drop(Box::from_raw(node.cast::<Node>()));
        }));
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_request(
    node: *mut c_void,
    json_len: usize,
    json: *const u8,
) -> i32 {
    guarded(|| {
        let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
            return INVALID;
        };
        let Ok(json) = (unsafe { bytes(json_len, json) }) else {
            return INVALID;
        };
        if json.len() > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        let Ok(request) = Service::decode(json) else {
            return CLIENT_JSON;
        };
        let (message_id, _, _) = request.ids();
        let message_id = *message_id.as_bytes();
        // Duplicate suppression (B2): a request whose reply is already cached
        // replays the cached bytes without re-proposing or re-executing.
        if let Some(cached) = node.replies.get(&message_id) {
            node.outputs.push_back(Queued {
                kind: OUTPUT_REPLY,
                to: 0,
                era: 0,
                view: 0,
                slot: 0,
                message_id,
                bytes: cached.clone(),
            });
            return OK;
        }
        if json.len() + PREPARE_OVERHEAD > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        let id = operation_id(message_id);
        node.pending.insert(id, message_id);
        let result = node.drive(Input::Propose {
            operation: Operation {
                id,
                payload: json.to_vec().into_boxed_slice(),
            },
        });
        if result != OK {
            node.pending.remove(&id);
        }
        result
    })
}

impl Node {
    /// The non-stop overlap pivot for a reconfiguration, derived the
    /// upstream way: the host builds it with the core's own `construct_pivot`
    /// against the current configuration and the configuration the
    /// establishing operation folds at the slot it would occupy — the same
    /// slot the planner's gate 5 probes. `None` (no legal pivot for this
    /// leader, or the fold probe failed) drives the stop-the-world
    /// fallback, which upstream defines as a latency outcome, never an
    /// error; the core's `validate_pivot` gate still judges whatever the
    /// host passes.
    fn derived_pivot(&self, op: &SystemOperation) -> Option<Pivot> {
        let next_slot = self.replica.progress().accepted().next()?;
        let next_table = self
            .replica
            .progress()
            .config()
            .extend(op, next_slot)
            .ok()?;
        let current = &self.replica.progress().config().current().config;
        construct_pivot(
            &WeightedMajority,
            current,
            &next_table.current().config,
            self.replica.own(),
        )
    }
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_reconfigure(
    node: *mut c_void,
    op: u32,
    member: u32,
    position: u32,
) -> i32 {
    guarded(|| {
        let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
            return INVALID;
        };
        let position = if op == RECONFIGURE_JOIN && position == POSITION_APPEND {
            let len = node
                .replica
                .progress()
                .config()
                .current()
                .config
                .order()
                .len();
            match u32::try_from(len) {
                Ok(len) => len,
                Err(_) => return SERVICE,
            }
        } else {
            position
        };
        let system = match op {
            RECONFIGURE_JOIN => SystemOperation::Join {
                node: NodeId(member),
                position,
            },
            RECONFIGURE_INCREMENT => SystemOperation::Increment(NodeId(member)),
            RECONFIGURE_LEAVE => SystemOperation::Leave(NodeId(member)),
            _ => return INVALID,
        };
        let pivot = node.derived_pivot(&system);
        node.drive(Input::Reconfigure { op: system, pivot })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_receive(
    node: *mut c_void,
    from: u32,
    len: usize,
    data: *const u8,
) -> i32 {
    guarded(|| {
        let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
            return INVALID;
        };
        let Ok(data) = (unsafe { bytes(len, data) }) else {
            return INVALID;
        };
        if data.len() > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        // W5: `Incomplete` means "more bytes could make this a message" and
        // `Malformed` means none could; over datagram transport there is no
        // reassembly, so both are a bad datagram from this host's view.
        let message = match Message::unpack_from(data) {
            Ok(message) => message,
            Err(UnpackError::Incomplete { .. } | UnpackError::Malformed(_)) => {
                return VRR_MESSAGE;
            }
        };
        if !valid_message_payloads(&message) {
            return VRR_MESSAGE;
        }
        node.drive(Input::Peer {
            from: NodeId(from),
            message,
        })
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_idle(node: *mut c_void) -> i32 {
    // Heartbeat tick. The tag has a single liveness input, Input::Tick;
    // there is no separate idle input.
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.drive(Input::Tick))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_leader_timeout(node: *mut c_void) -> i32 {
    // Election tick: same Input::Tick — tick-driven suspicion is the tag's
    // only view-change trigger (ViewChangeKnobs::primary_timeout).
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.drive(Input::Tick))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_recover(node: *mut c_void) -> i32 {
    guarded(|| unsafe { node.cast::<Node>().as_mut().map_or(INVALID, Node::recover) })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_status(
    node: *mut c_void,
    out_status: *mut u32,
    out_leader: *mut u32,
    out_era: *mut u32,
    out_view: *mut u32,
) -> i32 {
    guarded(|| {
        if node.is_null()
            || out_status.is_null()
            || out_leader.is_null()
            || out_era.is_null()
            || out_view.is_null()
        {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        let snapshot = node.replica.observer().read();
        unsafe {
            *out_status = snapshot.status;
            *out_era = snapshot.era;
            *out_view = snapshot.view;
            *out_leader = node.primary_index();
        }
        OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_leader_for_view(
    node: *mut c_void,
    era: u32,
    view: u32,
    out_leader: *mut u32,
) -> i32 {
    guarded(|| {
        if node.is_null() || out_leader.is_null() {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        unsafe { *out_leader = node.leader_for(era, view) };
        OK
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_next(
    node: *mut c_void,
    out_kind: *mut u32,
    out_to: *mut u32,
    out_era: *mut u32,
    out_view: *mut u32,
    out_slot_hi: *mut u32,
    out_slot_lo: *mut u32,
    out_message_id: *mut u8,
    capacity: usize,
    out_len: *mut usize,
    out_data: *mut u8,
) -> i32 {
    guarded(|| {
        if node.is_null()
            || out_kind.is_null()
            || out_to.is_null()
            || out_era.is_null()
            || out_view.is_null()
            || out_slot_hi.is_null()
            || out_slot_lo.is_null()
            || out_message_id.is_null()
            || out_len.is_null()
        {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        let Some(output) = node.outputs.front() else {
            return 0;
        };
        unsafe { *out_len = output.bytes.len() };
        if output.bytes.len() > capacity || (out_data.is_null() && !output.bytes.is_empty()) {
            return TOO_LARGE;
        }
        unsafe {
            *out_kind = output.kind;
            *out_to = output.to;
            *out_era = output.era;
            *out_view = output.view;
            *out_slot_hi = (output.slot >> 32) as u32;
            *out_slot_lo = output.slot as u32;
            ptr::copy_nonoverlapping(output.message_id.as_ptr(), out_message_id, 16);
            if !output.bytes.is_empty() {
                ptr::copy_nonoverlapping(output.bytes.as_ptr(), out_data, output.bytes.len());
            }
        }
        node.outputs.pop_front();
        1
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;
    use vrr::ids::Slot;

    fn state_path(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQUENCE: AtomicU64 = AtomicU64::new(0);
        std::env::temp_dir().join(format!(
            "lunet-advisory-lock-{name}-{}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after Unix epoch")
                .as_nanos(),
            SEQUENCE.fetch_add(1, Ordering::Relaxed),
        ))
    }

    #[test]
    fn recovery_nonces_are_created_then_durably_incremented() {
        let path = state_path("nonce");
        assert!(!initialize_nonce(&path).expect("first boot creates nonce"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "0\n");
        assert_eq!(next_nonce(&path).unwrap(), 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), "1\n");
        assert_eq!(next_nonce(&path).unwrap(), 2);
        assert_eq!(fs::read_to_string(&path).unwrap(), "2\n");
        fs::remove_file(path).unwrap();
    }

    /// The sparse admin-assigned member ids every test cluster uses, in
    /// deployment-descriptor (genesis succession) order.
    const TEST_IDS: [u32; 3] = [10, 20, 30];

    fn provision(name: &str, own: u32, members: u32) -> Node {
        let nonce_path = state_path(name);
        initialize_nonce(&nonce_path).expect("nonce file");
        let replica = Replica::provision(
            NodeId(own),
            TEST_IDS[..members as usize]
                .iter()
                .map(|id| NodeId(*id))
                .collect(),
            WeightedMajority,
            SegmentedLog::new(),
            Stability::Volatile,
            ViewChangeKnobs {
                primary_timeout: PRIMARY_TIMEOUT_MS,
                view_change_budget: MAX_DATAGRAM,
            },
        )
        .expect("provision");
        Node {
            replica,
            outputs: VecDeque::new(),
            service: Service::default(),
            replies: HashMap::new(),
            pending: HashMap::new(),
            nonce_path,
            last_tick: 0,
            poisoned: false,
            journal: None,
        }
    }

    fn request_json(message_id: Uuid) -> Vec<u8> {
        serde_json::to_vec(&crate::locks::Request::Get {
            message_id,
            client_id: 11,
            request_num: 13,
            lock_id: 17,
        })
        .unwrap()
    }

    /// Deliver every queued send on every node to its destination,
    /// recursively draining whatever the destination emits in answer, until
    /// no node holds a send. Replies stay queued on their node. `ids` are
    /// the member ids of `nodes`, in the same order.
    fn route_until_quiet(nodes: &mut [Node], ids: &[u32]) {
        loop {
            let mut moved = false;
            for source in 0..nodes.len() {
                let drained: VecDeque<Queued> = std::mem::take(&mut nodes[source].outputs);
                let (sends, kept): (Vec<Queued>, Vec<Queued>) = drained
                    .into_iter()
                    .partition(|output| output.kind == OUTPUT_SEND);
                nodes[source].outputs = kept.into_iter().collect();
                for send in sends {
                    moved = true;
                    let message = Message::unpack_from(&send.bytes).expect("wire round trip");
                    let to = ids
                        .iter()
                        .position(|id| *id == send.to)
                        .expect("known destination");
                    assert_eq!(
                        nodes[to].drive(Input::Peer {
                            from: NodeId(ids[source]),
                            message,
                        }),
                        OK
                    );
                }
            }
            if !moved {
                return;
            }
        }
    }

    /// A fresh three-node cluster brought to Normal: every node provisions
    /// fenced `Recovering` (the boot rule); the genesis primary (member id
    /// 10, first in the descriptor order) self-promotes on a tick and
    /// broadcasts Commit; the Recovering backups adopt the view under the
    /// §4 bootstrap rule when the primary's messages reach them.
    fn boot_cluster() -> [Node; 3] {
        let mut nodes = [
            provision("cluster-one", TEST_IDS[0], 3),
            provision("cluster-two", TEST_IDS[1], 3),
            provision("cluster-three", TEST_IDS[2], 3),
        ];
        for (index, node) in nodes.iter_mut().enumerate() {
            assert_eq!(
                node.replica.progress().status(),
                vrr::progress::Status::Recovering,
                "node {index} boots fenced"
            );
            node.outputs.clear();
        }
        assert_eq!(nodes[0].drive(Input::Tick), OK);
        assert_eq!(
            nodes[0].replica.observer().read().status,
            0,
            "genesis primary promotes to Normal"
        );
        route_until_quiet(&mut nodes, &TEST_IDS);
        for (index, node) in nodes.iter_mut().enumerate() {
            assert_eq!(
                node.replica.observer().read().status,
                0,
                "node {index} is Normal"
            );
            node.outputs.clear();
        }
        nodes
    }

    fn request(node: &mut Node, json: &[u8]) -> i32 {
        unsafe { lunet_lock_node_request((&raw mut *node).cast(), json.len(), json.as_ptr()) }
    }

    fn receive(node: &mut Node, from: u32, data: &[u8]) -> i32 {
        unsafe { lunet_lock_node_receive((&raw mut *node).cast(), from, data.len(), data.as_ptr()) }
    }

    fn reconfigure(node: &mut Node, op: u32, member: u32, position: u32) -> i32 {
        unsafe { lunet_lock_node_reconfigure((&raw mut *node).cast(), op, member, position) }
    }

    fn pop_send(node: &mut Node, to: u32, tag: vrr::wire::Tag) -> Option<Queued> {
        let drained: VecDeque<Queued> = std::mem::take(&mut node.outputs);
        let mut found = None;
        let mut kept = VecDeque::new();
        for output in drained {
            if found.is_none()
                && output.kind == OUTPUT_SEND
                && output.to == to
                && Message::unpack_from(&output.bytes)
                    .ok()
                    .is_some_and(|message| message.header.tag == tag)
            {
                found = Some(output);
            } else {
                kept.push_back(output);
            }
        }
        node.outputs = kept;
        found
    }

    /// Feeds one queued send to its destination and drains the destination's
    /// answers the same way, one hop at a time. `nodes`/`ids` are parallel
    /// arrays (the `ids[i]` member runs `nodes[i]`).
    fn deliver_hop(nodes: &mut [Node], ids: &[u32], source: usize, send: Queued) {
        let message = Message::unpack_from(&send.bytes).expect("wire round trip");
        let to = ids
            .iter()
            .position(|id| *id == send.to)
            .expect("known destination");
        assert_eq!(
            nodes[to].drive(Input::Peer {
                from: NodeId(ids[source]),
                message,
            }),
            OK
        );
    }

    /// Drives a suspicion fence on `driver`: one `Input::Tick` stamped past
    /// the primary-timeout window (the adapter clamps ticks to the wall
    /// clock, so the test stamps a synthetic monotone value), then the
    /// ordinary fence choreography routes to quiescence.
    fn drive_fence(nodes: &mut [Node], ids: &[u32], driver: usize) {
        let at = nodes[driver].last_tick + PRIMARY_TIMEOUT_MS + 1;
        assert_eq!(nodes[driver].drive_at(at, Input::Tick), OK);
        route_until_quiet(nodes, ids);
    }

    /// The joiner member of the four-node tests: id 40, booted the joiner
    /// way — a later life over the deployment's genesis, fenced
    /// `Recovering`, addressed, and outside every configuration until a
    /// committed `Join` admits it.
    fn provision_joiner(name: &str, own: u32, genesis: &[u32]) -> Node {
        let nonce_path = state_path(name);
        initialize_nonce(&nonce_path).expect("nonce file");
        let replica = match joiner_replica(
            NodeId(own),
            genesis.iter().map(|id| NodeId(*id)).collect(),
            ViewChangeKnobs {
                primary_timeout: PRIMARY_TIMEOUT_MS,
                view_change_budget: MAX_DATAGRAM,
            },
        ) {
            Ok(replica) => replica,
            Err(_) => panic!("joiner boot"),
        };
        Node {
            replica,
            outputs: VecDeque::new(),
            service: Service::default(),
            replies: HashMap::new(),
            pending: HashMap::new(),
            nonce_path,
            last_tick: 0,
            poisoned: false,
            journal: None,
        }
    }

    /// The four-node cluster at "the join committed": the three genesis
    /// incumbents bootstrapped with member id 40 as primary, the join
    /// established through the ABI, era 2 folded at the commit, and the
    /// ordinary view change into era 2 not yet run. The joiner holds the
    /// deployment's genesis and nothing else: it adopts the announced
    /// era-1 view through the bootstrap rule, its frontier stands at the
    /// genesis slots, and its era table covers era 1 only (see the
    /// module's Identity note).
    fn boot_four_and_join() -> ([Node; 4], [u32; 4]) {
        let [one, two, three] = boot_cluster();
        let joiner = provision_joiner("cluster-joiner", 40, &TEST_IDS);
        assert_eq!(joiner.replica.progress().status(), Status::Recovering);
        let mut nodes = [one, two, three, joiner];
        let ids = [TEST_IDS[0], TEST_IDS[1], TEST_IDS[2], 40];

        // The join: `construct_pivot` cannot place a non-member in either
        // vote set (the cardinality rule's union coverage), so the adapter
        // drives the stop-the-world fallback: the establishing Prepare goes
        // to every backup, and the era awaits the ordinary view change.
        assert_eq!(
            reconfigure(&mut nodes[0], RECONFIGURE_JOIN, 40, POSITION_APPEND),
            OK
        );
        let prepare = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::Prepare)
            .expect("the establishing Prepare reaches every backup");
        let prepare_two = pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the establishing Prepare reaches every backup");
        assert!(
            pop_send(&mut nodes[0], 40, vrr::wire::Tag::Prepare).is_none(),
            "a non-member is never in the establishing fan-out"
        );
        assert_eq!(nodes[0].replica.progress().config().current().era, Era(1));
        deliver_hop(&mut nodes, &ids, 0, prepare);
        deliver_hop(&mut nodes, &ids, 0, prepare_two);
        let ok = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the backup acknowledges");
        let ok_two = pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the backup acknowledges");
        deliver_hop(&mut nodes, &ids, 1, ok);
        deliver_hop(&mut nodes, &ids, 2, ok_two);

        // The commit folds era 2 (the joiner at weight 0), the commit
        // cascade announces the frontier to every folded-configuration
        // member, and the stop-the-world path arms no planned machine.
        // Route the announcements: the backups' commit advance folds era 2
        // too, which is what their later fence targets read (§8.7.8).
        route_until_quiet(&mut nodes, &ids);
        assert_eq!(
            nodes[0].replica.progress().config().current().era,
            Era(2),
            "the era advances exactly at the establishing commit"
        );
        for node in nodes[..3].iter_mut() {
            assert_eq!(
                node.replica.progress().config().current().era,
                Era(2),
                "every incumbent folds the era through the commit cascade"
            );
        }
        assert!(
            pop_send(
                &mut nodes[0],
                TEST_IDS[2],
                vrr::wire::Tag::PlannedViewChange
            )
            .is_none(),
            "the stop-the-world path solicits nothing"
        );
        (nodes, ids)
    }

    /// Mirrors upstream's
    /// `tests/nonstop_overlap_protocol.rs::overlap_transition_runs_the_seven_steps_without_stopping_the_stream`
    /// through the adapter ABI: a three-node genesis cluster, the
    /// incrementing reconfiguration with the adapter-derived pivot, the
    /// seven steps in order, and the client stream uninterrupted.
    #[test]
    fn reconfigure_abi_runs_the_nonstop_overlap_transition() {
        let mut nodes = boot_cluster();
        let ids = TEST_IDS;

        // Step 1: the establishing proposal goes to `qII - {L}` and nowhere
        // else, routed under the era its entry is stamped with; acceptance
        // establishes nothing.
        assert_eq!(
            reconfigure(&mut nodes[0], RECONFIGURE_INCREMENT, TEST_IDS[1], 0),
            OK
        );
        let prepare = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::Prepare)
            .expect("the pivot routes the Prepare to qII - {L}");
        assert!(
            pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::Prepare).is_none(),
            "never outside qII"
        );
        assert_eq!((prepare.era, prepare.view, prepare.slot >> 32), (1, 0, 0));
        let message = Message::unpack_from(&prepare.bytes).expect("wire round trip");
        let Body::Prepare { entry, .. } = &message.body else {
            panic!("the send is the establishing Prepare")
        };
        assert_eq!(entry.slot, Slot(3));
        assert_eq!(entry.era, Era(1));
        assert_eq!(
            nodes[0].replica.progress().config().current().era,
            Era(1),
            "acceptance establishes nothing"
        );

        // Step 2: acceptance through qII, then the commit — the era folds,
        // the pivot lands on the era record, and the solicitation goes to
        // `qI - {L}` (never `qII - {L}`) while the leader stays in (1, 0).
        deliver_hop(&mut nodes, &ids, 0, prepare);
        let ok = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the qII member acknowledges");
        deliver_hop(&mut nodes, &ids, 1, ok);
        let solicitation = pop_send(
            &mut nodes[0],
            TEST_IDS[2],
            vrr::wire::Tag::PlannedViewChange,
        )
        .expect("exactly one solicitation, to qI - {L}");
        assert!(
            pop_send(
                &mut nodes[0],
                TEST_IDS[1],
                vrr::wire::Tag::PlannedViewChange
            )
            .is_none(),
            "qII - {{L}} never sees the solicitation"
        );
        assert_eq!(
            (solicitation.era, solicitation.view),
            (2, 3),
            "the header names v' = (2, 3); the core routes the copy under the current era (the \
             adapter reports encoded headers, not effect route eras)"
        );
        let record = nodes[0]
            .replica
            .progress()
            .config()
            .record(Era(2))
            .expect("the fold recorded era 2");
        assert_eq!(record.established_by, Slot(3));
        assert_eq!(
            record.pivot,
            Some(Pivot {
                q_i: vec![NodeId(TEST_IDS[0]), NodeId(TEST_IDS[2])],
                q_ii: vec![NodeId(TEST_IDS[0]), NodeId(TEST_IDS[1])],
            }),
            "the pivot is threaded onto the era record at the fold"
        );
        assert_eq!(
            nodes[0].replica.observer().read().view,
            0,
            "the leader has not switched"
        );

        // Step 3: the qI recipient answers with planned evidence and
        // RETAINS its view — no fence, still Normal in (1, 0) — and opens
        // the suffix fallback fetch.
        deliver_hop(&mut nodes, &ids, 0, solicitation);
        let evidence = pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::DoViewChange)
            .expect("the planned answer");
        let fetch = pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::GetState)
            .expect("the suffix fallback fetch opened");
        let answer = Message::unpack_from(&evidence.bytes).expect("wire round trip");
        let Body::DoViewChange { evidence: kind, .. } = &answer.body else {
            panic!("the answer is a DoViewChange")
        };
        assert!(matches!(kind, vrr::message::EvidenceKind::Planned));
        assert_eq!((evidence.era, evidence.view), (2, 3), "the answer names v'");
        let snapshot = nodes[2].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 1, 0));

        // Step 4 (interleaved): the client stream continues — an ordinary
        // era-2 prepare commits through qII while the planned exchange runs.
        assert_eq!(
            request(&mut nodes[0], &request_json(Uuid::from_bytes([21; 16]))),
            OK
        );
        let stream_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::Prepare)
            .expect("the stream fans out to the era-1 backups");
        let stream_two = pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the stream fans out to the era-1 backups");
        assert_eq!(
            (stream_one.era, stream_one.view),
            (1, 0),
            "the encoded header stays in the current view; the core routes the copy under the \
             entry's era (the adapter reports headers, not effect route eras)"
        );
        let Body::Prepare { entry, .. } = &Message::unpack_from(&stream_one.bytes)
            .expect("wire round trip")
            .body
        else {
            panic!("the send is a Prepare")
        };
        assert_eq!(entry.era, Era(2), "stamped with the authorizing era");
        let _ = stream_two;
        // Only the qII copy is delivered (upstream's script does the
        // same): own vote plus the heavy member's weight clears the era-2
        // threshold mid-transition.
        deliver_hop(&mut nodes, &ids, 0, stream_one);
        assert!(
            pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::GetState).is_none(),
            "the light member's copy was never delivered: its step-3 fetch stands"
        );
        let ok_one = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the qII member acknowledges the stream");
        deliver_hop(&mut nodes, &ids, 1, ok_one);
        assert!(
            pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::PrepareOk).is_none(),
            "the light member's copy was never delivered"
        );
        assert_eq!(
            nodes[0].replica.progress().committed(),
            Slot(4),
            "the client operation committed through qII mid-transition"
        );
        assert_eq!(nodes[0].replica.observer().read().view, 0, "still in v");

        // Step 5: the responder's fetch folds the era through the ordinary
        // state-transfer path — still no view change anywhere.
        deliver_hop(&mut nodes, &ids, 2, fetch);
        let chunk = pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::NewState)
            .expect("the leader serves the fetch");
        assert_eq!(chunk.era, 1, "served under the requested era's record");
        deliver_hop(&mut nodes, &ids, 0, chunk);
        let snapshot = nodes[2].replica.observer().read();
        assert_eq!(
            nodes[2].replica.progress().config().current().era,
            Era(2),
            "the fetched commit folded the era"
        );
        assert_eq!(snapshot.committed, 4);
        assert_eq!(snapshot.status, 0, "retention survives the fetch");
        assert_eq!(snapshot.view, 0);

        // Step 6: the planned answer completes the quorum — the casting
        // vote and the switch are ONE published transition, and StartView
        // goes to every member of config(e+1) under the successor era.
        deliver_hop(&mut nodes, &ids, 2, evidence);
        let snapshot = nodes[0].replica.observer().read();
        assert_eq!(snapshot.status, 0, "the leader resumed normal operation");
        assert_eq!((snapshot.era, snapshot.view), (2, 3), "the single switch");
        let start_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::StartView)
            .expect("StartView to every member of config(e+1)");
        let start_two = pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::StartView)
            .expect("StartView to every member of config(e+1)");
        assert_eq!((start_one.era, start_one.view), (2, 3));
        let Body::StartView { committed, .. } = &Message::unpack_from(&start_one.bytes)
            .expect("wire round trip")
            .body
        else {
            panic!("the announcement is a StartView")
        };
        assert_eq!(*committed, Slot(4), "the frontier the vote certified");

        // Step 7: the members install and the stream continues under the
        // NEW configuration's arithmetic (weights [1, 2, 1], threshold 3):
        // the light member's answer alone cannot commit, the heavy one's
        // does.
        deliver_hop(&mut nodes, &ids, 0, start_one);
        deliver_hop(&mut nodes, &ids, 0, start_two);
        for node in nodes[1..3].iter_mut() {
            let snapshot = node.replica.observer().read();
            assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 2, 3));
        }
        assert_eq!(
            request(&mut nodes[0], &request_json(Uuid::from_bytes([22; 16]))),
            OK
        );
        let p_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::Prepare)
            .expect("the post-transition stream");
        let p_two = pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the post-transition stream");
        deliver_hop(&mut nodes, &ids, 0, p_one);
        deliver_hop(&mut nodes, &ids, 0, p_two);
        let light = pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the light member answers");
        deliver_hop(&mut nodes, &ids, 2, light);
        assert_eq!(
            nodes[0].replica.progress().committed(),
            Slot(4),
            "qI alone cannot commit"
        );
        let heavy = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the heavy member answers");
        deliver_hop(&mut nodes, &ids, 1, heavy);
        assert_eq!(
            nodes[0].replica.progress().committed(),
            Slot(5),
            "the heavy qII member commits it"
        );
    }

    /// Joins a fourth member at weight 0 through the ABI, observes the era-2
    /// commit and the quorum arithmetic under the new configuration, then
    /// leaves the zero-weight member and commits era 3. The joiner is the
    /// upstream learner (the corpus's class F, through the adapter): the
    /// leader streams TO it once the view enters the era that admitted it,
    /// and the learner drops the stream by name — its genesis table covers
    /// era 1 only, and the missing range is unservable to a non-member of
    /// the era a fetch can name. The named drop is the proof of arrival.
    #[test]
    fn reconfigure_abi_joins_a_learner_then_leaves_it_at_zero() {
        let (mut nodes, ids) = boot_four_and_join();
        drive_fence(&mut nodes, &ids, 2);
        let snapshot = nodes[1].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 2, 1));

        // The era-2 stream: the fan-out follows the view's configuration
        // and reaches the weight-0 learner. The learner RECEIVES its copy
        // and drops it by name: nothing queued, frontier and table
        // unchanged at the genesis.
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([31; 16]))),
            OK
        );
        let to_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the learner is in the view configuration's fan-out");
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.accepted),
            (2, 1, 2),
            "the joiner stays fenced: the commit cascade targets the view's era only, and the \
             era-2 stream and StartView are unevaluable at its genesis table (its fetch is \
             dropped at the serving gate)"
        );
        assert!(nodes[3].outputs.is_empty(), "the drop emits nothing");

        // Quorum arithmetic under the new configuration: weights [1,1,1,0],
        // total 3, threshold 2 — one voter's acknowledgment alongside the
        // primary's own vote commits.
        let p_one = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the voter's copy");
        let p_undelivered = pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the second voter's copy");
        drop(p_undelivered);
        deliver_hop(&mut nodes, &ids, 1, p_one);
        let ok_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok_one);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(4),
            "the learner's weight is not needed"
        );

        // Leave the zero-weight member: the adapter's derived pivot places
        // the departing learner inside qI, so the establishing Prepare goes
        // only to `qII - {L}` = {id 10}; the commit advances the era to 3
        // and the departed identity is gone from the folded configuration.
        assert_eq!(reconfigure(&mut nodes[1], RECONFIGURE_LEAVE, 40, 0), OK);
        let prepare = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the pivot routes the establishing Prepare");
        assert!(
            pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare).is_none()
                && pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare).is_none(),
            "the pivot routes to qII - {{L}} only"
        );
        deliver_hop(&mut nodes, &ids, 1, prepare);
        let ok = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the qII member acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok);
        // The commit cascade announces the frontier; route to quiescence so
        // every incumbent folds era 3. The planned machine stalls on the
        // unserved learner (its vote never arrives), so the leader's view
        // stands still.
        route_until_quiet(&mut nodes, &ids);
        assert_eq!(
            nodes[1].replica.progress().config().current().era,
            Era(3),
            "the leave commits era 3"
        );
        assert_eq!(
            nodes[1].replica.observer().read().view,
            1,
            "the planned quorum waits on the unserved learner"
        );
        assert!(
            nodes[1]
                .replica
                .progress()
                .config()
                .current()
                .config
                .weight_of(NodeId(40))
                .is_none(),
            "the departed identity is out of the configuration"
        );
        // The era the leave established awaits the ordinary view change
        // (§8.7.8): the fence completes the entry — the latency outcome the
        // unserved catch-up costs.
        drive_fence(&mut nodes, &ids, 0);
        let snapshot = nodes[2].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 3, 2));
    }

    /// Joins the fourth member, enters era 2, then promotes it with a
    /// committed Increment through the non-stop overlap: the adapter's
    /// derived pivot puts the (weight-0, not-yet-caught-up) learner inside
    /// `qII` — its zero weight contributes nothing to either commit
    /// threshold — and the planned quorum over `qI = {L, id 30}` completes
    /// without stopping the stream. The promoted arithmetic is four voters
    /// (threshold 3): a single acknowledgment no longer commits.
    #[test]
    fn reconfigure_abi_promotes_the_learner_and_moves_the_quorum_arithmetic() {
        let (mut nodes, ids) = boot_four_and_join();
        drive_fence(&mut nodes, &ids, 2);
        let snapshot = nodes[1].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 2, 1));

        // The promotion on the era-2 primary: the establishing Prepare goes
        // only to `qII - {L}` = {id 10, id 40}.
        assert_eq!(reconfigure(&mut nodes[1], RECONFIGURE_INCREMENT, 40, 0), OK);
        let to_voter = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the pivot routes the establishing Prepare");
        let to_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the learner is inside qII: it receives the copy, weight or no weight");
        assert!(
            pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare).is_none(),
            "never outside qII"
        );
        assert_eq!(
            nodes[1].replica.progress().config().current().era,
            Era(2),
            "acceptance establishes nothing"
        );

        // The learner cannot evaluate the establishing copy (its table
        // covers era 1 only): the named drop is the proof of arrival.
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        assert!(
            pop_send(&mut nodes[3], TEST_IDS[1], vrr::wire::Tag::PrepareOk).is_none(),
            "the unserved learner cannot acknowledge"
        );

        // The commit through qII folds era 3 (weights [1,1,1,1]) and
        // solicits planned evidence from `qI - {L}` = {id 30}.
        deliver_hop(&mut nodes, &ids, 1, to_voter);
        let ok = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the qII member acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok);
        let solicitation = pop_send(
            &mut nodes[1],
            TEST_IDS[2],
            vrr::wire::Tag::PlannedViewChange,
        )
        .expect("the solicitation reaches qI - {L}");
        assert_eq!(nodes[1].replica.progress().config().current().era, Era(3));
        assert_eq!(
            nodes[1].replica.observer().read().view,
            1,
            "the leader has not switched"
        );

        // The planned answer completes the quorum — ONE published
        // transition — and StartView goes to every member of config(e+1)
        // under the successor era. (The mid-transition stream mirror lives
        // in the three-node overlap test; here the lagging incumbents first
        // install the promoted era through the StartView suffixes.)
        deliver_hop(&mut nodes, &ids, 1, solicitation);
        let evidence = pop_send(&mut nodes[2], TEST_IDS[1], vrr::wire::Tag::DoViewChange)
            .expect("the qI member answers planned evidence");
        deliver_hop(&mut nodes, &ids, 2, evidence);
        let snapshot = nodes[1].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (0, 3, 5),
            "the single switch: v' = (3, 5) selects the primary under the new order"
        );
        let start_one = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::StartView)
            .expect("StartView to every member of config(e+1)");
        let start_two = pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::StartView)
            .expect("StartView to every member of config(e+1)");
        let start_three = pop_send(&mut nodes[1], 40, vrr::wire::Tag::StartView)
            .expect("StartView to every member of config(e+1)");
        assert_eq!((start_one.era, start_one.view), (3, 5));
        deliver_hop(&mut nodes, &ids, 1, start_one);
        deliver_hop(&mut nodes, &ids, 1, start_two);
        deliver_hop(&mut nodes, &ids, 1, start_three);
        assert!(
            pop_send(&mut nodes[3], TEST_IDS[1], vrr::wire::Tag::PrepareOk).is_none(),
            "the unserved learner installs nothing and acknowledges nothing"
        );
        // Route the commit cascade and the installed suffixes, then let the
        // ordinary tick re-run the retained offers: the StartView arrived
        // before the fetched suffix made it constructible (§13.1 step 5's
        // ruling), so the install completes on the tick.
        route_until_quiet(&mut nodes, &ids);
        for node in nodes[..3].iter_mut() {
            assert_eq!(node.drive(Input::Tick), OK);
        }
        route_until_quiet(&mut nodes, &ids);
        for node in nodes[..3].iter_mut() {
            let snapshot = node.replica.observer().read();
            assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 3, 5));
        }

        // The stream continues under the promoted arithmetic: threshold 3
        // under weights [1,1,1,1]. One acknowledgment does not commit; the
        // second does. The promoted member's copy arrives and is dropped by
        // name — it cannot acknowledge what it cannot evaluate.
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([41; 16]))),
            OK
        );
        let p_one = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the voter's copy");
        let p_two = pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the voter's copy");
        let p_three = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the promoted member is in the fan-out");
        assert_eq!((p_one.era, p_one.view), (3, 5), "the header names v'");
        deliver_hop(&mut nodes, &ids, 1, p_one);
        let ok_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok_one);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(4),
            "one of the two needed votes does not commit"
        );
        deliver_hop(&mut nodes, &ids, 1, p_two);
        let ok_two = pop_send(&mut nodes[2], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        deliver_hop(&mut nodes, &ids, 2, ok_two);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(5),
            "the second voter's acknowledgment clears threshold 3"
        );
        deliver_hop(&mut nodes, &ids, 1, p_three);
        assert!(
            pop_send(&mut nodes[3], TEST_IDS[1], vrr::wire::Tag::PrepareOk).is_none(),
            "the promoted member still cannot evaluate the stream: no acknowledgment, by name"
        );
    }

    /// Refusals keep the log untouched: a non-primary is NOT_LEADER (the one
    /// actionable code), a reconfigure while a transition is outstanding is
    /// SERVICE, a fold-refused operation is SERVICE, and a bad op code is
    /// INVALID.
    #[test]
    fn reconfigure_abi_refusals_never_touch_the_log() {
        let (mut nodes, _ids) = boot_four_and_join();

        // Non-primary: the actionable refusal, with nothing proposed.
        let frontier = nodes[1].replica.observer().read().accepted;
        assert_eq!(
            reconfigure(&mut nodes[1], RECONFIGURE_INCREMENT, 40, 0),
            NOT_LEADER
        );
        assert!(nodes[1].outputs.is_empty());
        assert_eq!(nodes[1].replica.observer().read().accepted, frontier);

        // The join already committed era 2 and the ordinary view change has
        // not run: a second reconfigure is refused by the
        // transition-outstanding gate — internal, SERVICE.
        let frontier = nodes[0].replica.observer().read().accepted;
        assert_eq!(
            reconfigure(&mut nodes[0], RECONFIGURE_INCREMENT, TEST_IDS[0], 0),
            SERVICE
        );
        assert!(nodes[0].outputs.is_empty());
        assert_eq!(nodes[0].replica.observer().read().accepted, frontier);

        // Fold-refused operations (a member already in the configuration)
        // never enter the log either.
        assert_eq!(
            reconfigure(
                &mut nodes[0],
                RECONFIGURE_JOIN,
                TEST_IDS[0],
                POSITION_APPEND
            ),
            SERVICE
        );
        assert!(nodes[0].outputs.is_empty());
        assert_eq!(nodes[0].replica.observer().read().accepted, frontier);

        // Bad op codes are invalid arguments.
        assert_eq!(reconfigure(&mut nodes[0], 0, 40, 0), INVALID);
        assert_eq!(reconfigure(&mut nodes[0], 4, 40, 0), INVALID);
    }

    #[test]
    fn committed_request_produces_a_correlated_reply_and_duplicate_replay() {
        let mut nodes = boot_cluster();
        let message_id = Uuid::from_bytes([7; 16]);
        let payload = request_json(message_id);

        // Propose on the primary (member id 10, first in the era-1 genesis
        // succession order).
        assert_eq!(request(&mut nodes[0], &payload), OK);
        let prepares: Vec<&Queued> = nodes[0]
            .outputs
            .iter()
            .filter(|output| output.kind == OUTPUT_SEND)
            .collect();
        assert_eq!(prepares.len(), 2, "primary fans out one Prepare per backup");
        for prepare in &prepares {
            assert!(prepare.bytes.len() <= MAX_DATAGRAM);
            let message = Message::unpack_from(&prepare.bytes).unwrap();
            assert!(matches!(message.body, Body::Prepare { .. }));
            assert_eq!(prepare.era, 1);
            assert_eq!(prepare.view, 0);
            assert_eq!(prepare.slot, 3, "first client slot after genesis");
        }

        // Prepares out, PrepareOks back, Commit out: route everything.
        route_until_quiet(&mut nodes, &TEST_IDS);

        // The quorum committed the slot: exactly one reply, on the proposer
        // only, correlated by the request's message_id.
        let reply = nodes[0]
            .outputs
            .iter()
            .find(|output| output.kind == OUTPUT_REPLY)
            .expect("quorum commits and the proposer replies");
        assert_eq!(reply.message_id, *message_id.as_bytes());
        let first_reply = reply.bytes.clone();
        for (index, node) in nodes.iter().enumerate() {
            assert_eq!(
                node.replica.observer().read().applied,
                3,
                "node {index} applied the client slot"
            );
            let replies = node
                .outputs
                .iter()
                .filter(|output| output.kind == OUTPUT_REPLY)
                .count();
            assert_eq!(replies, usize::from(index == 0), "node {index} reply count");
        }

        // A duplicate request replays the cached reply without re-executing
        // and without proposing anything to the cluster (B2, host-side).
        assert_eq!(request(&mut nodes[0], &payload), OK);
        let sends = nodes[0]
            .outputs
            .iter()
            .filter(|output| output.kind == OUTPUT_SEND)
            .count();
        assert_eq!(sends, 0, "duplicate is never proposed");
        let replay = nodes[0]
            .outputs
            .iter()
            .find(|output| {
                output.kind == OUTPUT_REPLY && output.message_id == *message_id.as_bytes()
            })
            .expect("duplicate reply replayed");
        assert_eq!(replay.bytes, first_reply);
        assert_eq!(
            nodes[0].replica.observer().read().accepted,
            3,
            "duplicate never reaches the journal"
        );
    }

    #[test]
    fn non_primary_propose_is_refused_not_leader() {
        let mut nodes = boot_cluster();
        let payload = request_json(Uuid::from_bytes([9; 16]));
        // Member 20 is second in the genesis succession: not the primary.
        assert_eq!(request(&mut nodes[1], &payload), NOT_LEADER);
        assert!(nodes[1].outputs.is_empty());
        assert_eq!(nodes[1].replica.observer().read().accepted, 2);
    }

    #[test]
    fn malformed_and_oversize_ingress_are_refused() {
        let mut nodes = boot_cluster();
        let garbage = [0xFFu8; 64];
        assert_eq!(receive(&mut nodes[0], TEST_IDS[1], &garbage), VRR_MESSAGE);
        let truncated = [0u8; 4];
        assert_eq!(receive(&mut nodes[0], TEST_IDS[1], &truncated), VRR_MESSAGE);
        let oversize = vec![0u8; MAX_DATAGRAM + 1];
        assert_eq!(receive(&mut nodes[0], TEST_IDS[1], &oversize), TOO_LARGE);
        // A well-formed message whose peer-carried operation payload is not
        // a valid Service request is refused before it reaches the core.
        let forged = Message {
            header: vrr::wire::Header {
                tag: vrr::wire::Tag::Prepare,
                view: nodes[0].replica.progress().current(),
                slot: Slot(3),
            },
            body: Body::Prepare {
                entry: vrr::journal::LogEntry {
                    slot: Slot(3),
                    era: vrr::ids::Era(1),
                    payload: Payload::Operation {
                        id: OperationId { msb: 1, lsb: 2 },
                        payload: b"not json".as_slice().into(),
                    },
                },
                committed: Slot(2),
            },
        };
        let mut buf = vec![0u8; forged.packed_len()];
        forged.pack_into(&mut buf).unwrap();
        assert_eq!(receive(&mut nodes[1], TEST_IDS[0], &buf), VRR_MESSAGE);
    }

    #[test]
    fn invalid_client_json_and_oversize_requests_are_refused() {
        let mut nodes = boot_cluster();
        assert_eq!(request(&mut nodes[0], b"not json"), CLIENT_JSON);
        let oversize = vec![b'x'; MAX_DATAGRAM + 1];
        assert_eq!(request(&mut nodes[0], &oversize), TOO_LARGE);
    }

    #[test]
    fn panic_guard_reports_and_poison_sticks() {
        assert_eq!(guarded(|| panic!("boom")), PANIC);

        let mut nodes = boot_cluster();
        nodes[0].poisoned = true;
        let payload = request_json(Uuid::from_bytes([11; 16]));
        assert_eq!(request(&mut nodes[0], &payload), SERVICE);
        assert_eq!(
            unsafe { lunet_lock_node_idle((&raw mut nodes[0]).cast()) },
            SERVICE
        );
        assert_eq!(
            unsafe { lunet_lock_node_recover((&raw mut nodes[0]).cast()) },
            SERVICE
        );
        assert!(nodes[0].outputs.is_empty());
    }

    #[test]
    fn status_and_leader_report_the_published_view() {
        let mut nodes = boot_cluster();
        let (mut status, mut leader, mut era, mut view) = (0u32, 0u32, 0u32, 0u32);
        assert_eq!(
            unsafe {
                lunet_lock_node_status(
                    (&raw mut nodes[1]).cast(),
                    &mut status,
                    &mut leader,
                    &mut era,
                    &mut view,
                )
            },
            OK
        );
        assert_eq!(status, 0, "normal");
        assert_eq!((era, view), (1, 0), "era-1 genesis view");
        assert_eq!(leader, TEST_IDS[0], "genesis primary is member id 10");

        let mut for_view = u32::MAX;
        assert_eq!(
            unsafe {
                lunet_lock_node_leader_for_view((&raw mut nodes[1]).cast(), 1, 1, &mut for_view)
            },
            OK
        );
        assert_eq!(for_view, TEST_IDS[1], "view 1's primary is member id 20");
        assert_eq!(
            unsafe {
                lunet_lock_node_leader_for_view((&raw mut nodes[1]).cast(), 42, 0, &mut for_view)
            },
            OK
        );
        assert_eq!(for_view, LEADER_UNKNOWN, "era outside the retention window");
    }

    #[test]
    fn abi_new_status_next_and_free_round_trip() {
        // The full C surface: members are NUL-separated "<id>:<name>"
        // entries in descriptor (genesis succession) order with sparse
        // admin-assigned ids, state is the nonce file path.
        let members = b"10:n1\x0020:n2\x0030:n3";
        let own = b"n1";
        let state = state_path("abi-node");
        let state_bytes = state.as_os_str().as_encoded_bytes();
        let mut handle: *mut c_void = ptr::null_mut();
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    members.len(),
                    members.as_ptr(),
                    own.len(),
                    own.as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    0,
                    ptr::null(),
                    0,
                    &mut handle,
                )
            },
            OK
        );
        assert!(!handle.is_null());

        // Clean start: the node boots fenced and quiet — no fabricated
        // recovery handshake, the output queue is empty.
        let (mut kind, mut to, mut era, mut view) = (0u32, 0u32, 0u32, 0u32);
        let (mut slot_hi, mut slot_lo) = (0u32, 0u32);
        let mut message_id = [0u8; 16];
        let mut len = 0usize;
        let mut buffer = [0u8; MAX_DATAGRAM];
        assert_eq!(
            unsafe {
                lunet_lock_node_next(
                    handle,
                    &mut kind,
                    &mut to,
                    &mut era,
                    &mut view,
                    &mut slot_hi,
                    &mut slot_lo,
                    message_id.as_mut_ptr(),
                    buffer.len(),
                    &mut len,
                    buffer.as_mut_ptr(),
                )
            },
            0,
            "a fenced boot fabricates no outputs"
        );

        // Status reports the fenced boot state over the ABI.
        let (mut status, mut leader, mut era, mut view) = (0u32, 0u32, 0u32, 0u32);
        assert_eq!(
            unsafe {
                lunet_lock_node_status(handle, &mut status, &mut leader, &mut era, &mut view)
            },
            OK
        );
        assert_eq!(status, 2, "recovering");
        assert_eq!((era, view), (1, 0));
        assert_eq!(leader, 10);

        // The fenced-boot drive is a tick: the genesis primary self-promotes
        // and announces its committed frontier to every backup, drained
        // through node_next as kind-1 sends.
        assert_eq!(unsafe { lunet_lock_node_recover(handle) }, OK);
        let mut destinations = Vec::new();
        loop {
            let rc = unsafe {
                lunet_lock_node_next(
                    handle,
                    &mut kind,
                    &mut to,
                    &mut era,
                    &mut view,
                    &mut slot_hi,
                    &mut slot_lo,
                    message_id.as_mut_ptr(),
                    buffer.len(),
                    &mut len,
                    buffer.as_mut_ptr(),
                )
            };
            if rc == 0 {
                break;
            }
            assert_eq!(rc, 1);
            assert_eq!(kind, OUTPUT_SEND);
            assert!(len > 0 && len <= MAX_DATAGRAM);
            let message = Message::unpack_from(&buffer[..len]).expect("wire decodable");
            assert_eq!(
                (era, view),
                (message.header.view.era.0, message.header.view.view.0)
            );
            assert_eq!(
                u64::from(slot_hi) << 32 | u64::from(slot_lo),
                message.header.slot.0
            );
            assert!(matches!(message.body, Body::Commit { .. }));
            destinations.push(to);
        }
        assert_eq!(
            destinations,
            vec![20, 30],
            "the promotion fans out to the backups, by member id"
        );

        // Status reports the promoted state over the ABI.
        assert_eq!(
            unsafe {
                lunet_lock_node_status(handle, &mut status, &mut leader, &mut era, &mut view)
            },
            OK
        );
        assert_eq!(status, 0, "normal after the bootstrap tick");
        assert_eq!((era, view), (1, 0));
        assert_eq!(leader, 10);

        unsafe { lunet_lock_node_free(handle) };
        fs::remove_file(state).unwrap();
    }

    #[test]
    fn node_new_refuses_bad_membership() {
        let state = state_path("abi-config");
        let state_bytes = state.as_os_str().as_encoded_bytes();
        let mut handle: *mut c_void = ptr::null_mut();
        // own not a member.
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    b"10:n1\x0020:n2".len(),
                    b"10:n1\x0020:n2".as_ptr(),
                    b"n9".len(),
                    b"n9".as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    0,
                    ptr::null(),
                    0,
                    &mut handle,
                )
            },
            CONFIG
        );
        // duplicate member id.
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    b"10:n1\x0010:n2".len(),
                    b"10:n1\x0010:n2".as_ptr(),
                    b"n1".len(),
                    b"n1".as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    0,
                    ptr::null(),
                    0,
                    &mut handle,
                )
            },
            CONFIG
        );
        // duplicate member name.
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    b"10:n1\x0020:n1".len(),
                    b"10:n1\x0020:n1".as_ptr(),
                    b"n1".len(),
                    b"n1".as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    0,
                    ptr::null(),
                    0,
                    &mut handle,
                )
            },
            CONFIG
        );
        // malformed entry: no id separator.
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    b"10:n1\x0020n2\x0030:n3".len(),
                    b"10:n1\x0020n2\x0030:n3".as_ptr(),
                    b"n1".len(),
                    b"n1".as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    0,
                    ptr::null(),
                    0,
                    &mut handle,
                )
            },
            CONFIG
        );
        // malformed entry: non-numeric id.
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    b"ab:n1\x0020:n2\x0030:n3".len(),
                    b"ab:n1\x0020:n2\x0030:n3".as_ptr(),
                    b"n1".len(),
                    b"n1".as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    0,
                    ptr::null(),
                    0,
                    &mut handle,
                )
            },
            CONFIG
        );
        assert!(handle.is_null());
    }

    #[test]
    fn journal_records_committed_transitions_with_roll_and_meta() {
        use crate::journal::{self, Meta, parse_file};
        use crate::locks::{Lease, Request};

        let journal_dir = state_path("journal-integration");
        let _ = fs::remove_dir_all(&journal_dir);
        let members = b"10:n1\x0020:n2\x0030:n3";
        let own = b"n1";
        let state = state_path("journal-node");
        let state_bytes = state.as_os_str().as_encoded_bytes();
        let journal_dir_bytes = journal_dir.as_os_str().as_encoded_bytes();
        // Tiny roll threshold: 3 records = 183 bytes triggers a roll.
        let roll_bytes: u32 = (journal::RECORD_SIZE * 3) as u32;
        let mut handle: *mut c_void = ptr::null_mut();
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    members.len(),
                    members.as_ptr(),
                    own.len(),
                    own.as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    journal_dir_bytes.len(),
                    journal_dir_bytes.as_ptr(),
                    roll_bytes,
                    &mut handle,
                )
            },
            OK
        );
        assert!(!handle.is_null());

        // Build two backup nodes (no journal) to form a quorum.
        let mut backups = [
            provision("journal-backup-1", TEST_IDS[1], 3),
            provision("journal-backup-2", TEST_IDS[2], 3),
        ];

        // Drive the bootstrap: nodes boot fenced, the primary's tick
        // promotes it, and the route adopts the backups.
        let primary = unsafe { &mut *handle.cast::<Node>() };
        assert_eq!(primary.drive(Input::Tick), OK);

        // Route until quiet across all three nodes, by member id.
        let mut all_nodes = vec![primary as *mut Node];
        for b in &mut backups {
            all_nodes.push(b as *mut Node);
        }
        loop {
            let mut moved = false;
            for source in 0..all_nodes.len() {
                let drained: VecDeque<Queued> =
                    std::mem::take(&mut unsafe { &mut *all_nodes[source] }.outputs);
                let (sends, kept): (Vec<Queued>, Vec<Queued>) =
                    drained.into_iter().partition(|o| o.kind == OUTPUT_SEND);
                unsafe { &mut *all_nodes[source] }.outputs = kept.into_iter().collect();
                for send in sends {
                    moved = true;
                    let message = Message::unpack_from(&send.bytes).expect("wire round trip");
                    let to = TEST_IDS
                        .iter()
                        .position(|id| *id == send.to)
                        .expect("known destination");
                    assert_eq!(
                        unsafe { &mut *all_nodes[to] }.drive(Input::Peer {
                            from: NodeId(TEST_IDS[source]),
                            message,
                        }),
                        OK
                    );
                }
            }
            if !moved {
                break;
            }
        }
        // Clear all outputs.
        for ptr in &all_nodes {
            unsafe { &mut **ptr }.outputs.clear();
        }

        // Helper to propose a Set request on the primary and route until
        // committed.
        let propose_set = |primary: &mut Node,
                           backups: &mut [Node; 2],
                           msg_id: Uuid,
                           lock_id: u64,
                           lease_id: u64| {
            let holder = Uuid::from_bytes([0xBB; 16]);
            let payload = serde_json::to_vec(&Request::Set {
                message_id: msg_id,
                client_id: 1,
                request_num: 1,
                lock_id,
                lease: Lease {
                    lease_id,
                    holder,
                    expiry: unix_millis().unwrap() + 60_000,
                },
            })
            .unwrap();
            assert_eq!(
                unsafe {
                    lunet_lock_node_request(
                        (&raw mut *primary).cast(),
                        payload.len(),
                        payload.as_ptr(),
                    )
                },
                OK
            );
            // Route until quiet across all nodes, by member id.
            let mut all: Vec<&mut Node> = vec![primary];
            for b in backups.iter_mut() {
                all.push(b);
            }
            loop {
                let mut moved = false;
                for source in 0..all.len() {
                    let drained: VecDeque<Queued> = std::mem::take(&mut all[source].outputs);
                    let (sends, kept): (Vec<Queued>, Vec<Queued>) =
                        drained.into_iter().partition(|o| o.kind == OUTPUT_SEND);
                    all[source].outputs = kept.into_iter().collect();
                    for send in sends {
                        moved = true;
                        let message = Message::unpack_from(&send.bytes).expect("wire round trip");
                        let to = TEST_IDS
                            .iter()
                            .position(|id| *id == send.to)
                            .expect("known destination");
                        assert_eq!(
                            all[to].drive(Input::Peer {
                                from: NodeId(TEST_IDS[source]),
                                message,
                            }),
                            OK
                        );
                    }
                }
                if !moved {
                    break;
                }
            }
            for n in &mut all {
                n.outputs.clear();
            }
        };

        let primary = unsafe { &mut *handle.cast::<Node>() };
        // Propose 3 Set operations to trigger a roll (roll_bytes = 3 records).
        propose_set(primary, &mut backups, Uuid::from_bytes([1; 16]), 100, 1001);
        propose_set(primary, &mut backups, Uuid::from_bytes([2; 16]), 200, 2002);
        propose_set(primary, &mut backups, Uuid::from_bytes([3; 16]), 300, 3003);

        // After 3 records the journal should have rolled: one final .bin +
        // one .meta, plus a fresh ev-open-*.bin.
        let entries: Vec<_> = fs::read_dir(&journal_dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        let bin_files: Vec<&str> = entries
            .iter()
            .filter(|n| n.starts_with("ev-") && n.ends_with(".bin") && !n.contains("open"))
            .map(|s| s.as_str())
            .collect();
        assert_eq!(bin_files.len(), 1, "one rolled file after 3 records");
        let meta_files: Vec<&str> = entries
            .iter()
            .filter(|n| n.ends_with(".meta"))
            .map(|s| s.as_str())
            .collect();
        assert_eq!(meta_files.len(), 1, "one meta file after roll");

        // Verify the meta matches the op/expiry windows.
        let meta_path = journal_dir.join(meta_files[0]);
        let meta_bytes = fs::read(&meta_path).unwrap();
        let meta = Meta::decode(&meta_bytes).expect("valid meta");
        assert_eq!(meta.count, 3);
        assert!(meta.op_min <= meta.op_max);
        assert!(meta.expiry_min <= meta.expiry_max);

        // Parse back the records and verify they are Hold events.
        let bin_path = journal_dir.join(bin_files[0]);
        let bin_data = fs::read(&bin_path).unwrap();
        let events = parse_file(&bin_data);
        assert_eq!(events.len(), 3);
        for event in &events {
            assert_eq!(event.kind, journal::KIND_HOLD);
            assert_eq!(event.holder, [0xBB; 16]);
        }
        assert_eq!(events[0].lock_id, 100);
        assert_eq!(events[1].lock_id, 200);
        assert_eq!(events[2].lock_id, 300);

        unsafe { lunet_lock_node_free(handle) };
        let _ = fs::remove_file(state);
        let _ = fs::remove_dir_all(&journal_dir);
    }
}
