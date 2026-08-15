//! Host-side FFI adapter between the LuaJIT host and the uVRR v0.2.0 core.
//!
//! Concrete core: `Replica<SegmentedLog, WeightedMajority>` running
//! `Stability::Volatile` — nothing is persisted but the recovery nonce file,
//! so restart is `Replica::provision` over an empty journal followed by
//! `Input::Recover` (the tag's Volatile restart path; `reopen` is not used).
//!
//! Adapter policies the core deliberately does not own:
//!
//! - **Tick clock.** The adapter owns the tick clock: a monotonic
//!   nondecreasing milliseconds-since-Unix-epoch value, clamped per node so
//!   a clock regression never reaches the core. ABI functions take no `at`
//!   parameter. Recovery ticks come from the durable nonce file
//!   (`next_nonce`, fsync+rename+dir-sync), never from the ms clock, and are
//!   clamped the same way so per-node monotonicity holds across the two
//!   sources (S4: the recovery nonce IS the recovery input's tick).
//! - **Identity.** Membership is positional: the member at index `i` in the
//!   host-supplied order is `NodeId(i)`, and the same order is the genesis
//!   succession sequence (`primary(v) = order[v mod N]`). Peer indices on
//!   the ABI (`from`, `to`, leader outs) are these positional indices.
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
//!   operation entry (Prepare / DoViewChange / StartView / RecoveryResponse /
//!   NewState) with `Service` before the message reaches the core — the same
//!   gate the old adapter called `valid_message_payloads`.
//!
//! `node_next` output contract (kinds): 1 = send (unicast to `to`; era, view
//! and slot report the encoded message's wire header), 2 = reply (message_id
//! and response bytes; to/era/view/slot are zero). Return 1 when an output
//! was produced, 0 when the queue is empty, negative on error. When the next
//! output's bytes exceed `capacity`, the call reports the needed size in
//! `out_len`, returns TOO_LARGE and does NOT pop the queue.

use crate::locks::Service;
use std::collections::{HashMap, VecDeque};
use std::ffi::{OsString, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::time::{SystemTime, UNIX_EPOCH};
use vrr::configuration::MAX_MEMBERS;
use vrr::effects::{Effect, Stability};
use vrr::ids::{NodeId, Operation, OperationId, Tick};
use vrr::journal::{Journal, Payload, SegmentedLog};
use vrr::message::{Body, Message};
use vrr::quorum::WeightedMajority;
use vrr::replica::{Input, PlanRejection, PublishOutcome, Replica, TimedInput, ViewChangeKnobs};
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
}

impl Node {
    /// The next monotonic tick from the adapter-owned ms clock (never
    /// decreasing per node, even across a wall-clock regression).
    fn tick(&mut self) -> Result<u64, i32> {
        let now = unix_millis()?;
        self.last_tick = self.last_tick.max(now);
        Ok(self.last_tick)
    }

    /// The next durable recovery tick (S4: the nonce IS the tick), clamped
    /// into the same monotone sequence as the ms clock.
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

    /// A drive whose outer input's tick the caller already chose (recovery
    /// nonce ticks); feedback inputs inside the loop still sample the clock.
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
                let response = if let Some(cached) = self.replies.get(&message_id) {
                    // Duplicate committed operation: replay the cached reply,
                    // never re-execute (B2).
                    cached.clone()
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
                    let bytes = self
                        .service
                        .execute(id, client_id, request_num, execution_time, &payload)
                        .map_err(|_| SERVICE)?;
                    self.replies.insert(message_id, bytes.clone());
                    bytes
                };
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
            Effect::RequestApplicationState { through } => {
                // No checkpoint transfer facility exists in this host; the
                // core surfaced a shortfall it cannot repair. Log, poison,
                // and report on subsequent calls — never fabricate state.
                eprintln!(
                    "lunet-advisory-lock: application state shortfall through slot {}; \
                     node poisoned (no application-state transfer facility)",
                    through.0
                );
                Err(FAULTED)
            }
        }
    }

    fn recover(&mut self) -> i32 {
        if self.poisoned {
            return SERVICE;
        }
        let at = match self.recovery_tick() {
            Ok(at) => at,
            Err(error) => return error,
        };
        self.drive_at(at, Input::Recover)
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
/// caller can act on (re-forward to the named primary); the rest are
/// internal states the host cannot repair in place.
fn plan_error(rejection: PlanRejection) -> i32 {
    match rejection {
        PlanRejection::NotPrimary { .. } => NOT_LEADER,
        PlanRejection::Faulted(_) => FAULTED,
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
        Body::RecoveryResponse {
            suffix: Some(suffix),
            ..
        } => valid_entries(suffix),
        Body::NewState { entries, .. } => valid_entries(entries),
        _ => true,
    }
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
        let Some(members) = members_data
            .split(|byte| *byte == 0)
            .map(|member| std::str::from_utf8(member).ok().map(str::to_owned))
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
        if state.is_empty()
            || members.is_empty()
            || members.len() > MAX_MEMBERS as usize
            || members.iter().any(|member| member.is_empty())
        {
            return CONFIG;
        }
        let mut unique = members.clone();
        unique.sort();
        unique.dedup();
        if unique.len() != members.len() {
            return CONFIG;
        }
        let Some(own_index) = members.iter().position(|member| member == own) else {
            return CONFIG;
        };
        // Positional identity: member index i is NodeId(i), and the same
        // order is the genesis succession sequence.
        let genesis_order: Vec<NodeId> = (0..members.len() as u32).map(NodeId).collect();
        let knobs = ViewChangeKnobs {
            primary_timeout: PRIMARY_TIMEOUT_MS,
            view_change_budget: MAX_DATAGRAM,
        };
        let Ok(replica) = Replica::provision(
            NodeId(own_index as u32),
            genesis_order,
            WeightedMajority,
            SegmentedLog::new(),
            Stability::Volatile,
            knobs,
        ) else {
            return CONFIG;
        };
        let nonce_path = PathBuf::from(state);
        if initialize_nonce(&nonce_path).is_err() {
            return CONFIG;
        }
        let mut node = Node {
            replica,
            outputs: VecDeque::new(),
            service: Service::default(),
            replies: HashMap::new(),
            pending: HashMap::new(),
            nonce_path,
            last_tick: 0,
            poisoned: false,
        };
        // Every boot enters fenced Recovering (the tag's boot rule); drive a
        // recovery attempt immediately with a durable nonce tick.
        if node.recover() != OK {
            return CONFIG;
        }
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

    fn provision(name: &str, own: u32, members: u32) -> Node {
        let nonce_path = state_path(name);
        initialize_nonce(&nonce_path).expect("nonce file");
        let replica = Replica::provision(
            NodeId(own),
            (0..members).map(NodeId).collect(),
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
    /// no node holds a send. Replies stay queued on their node.
    fn route_until_quiet(nodes: &mut [Node]) {
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
                    let to = send.to as usize;
                    assert_eq!(
                        nodes[to].drive(Input::Peer {
                            from: NodeId(source as u32),
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

    /// A fresh three-node cluster brought to Normal: every node boots fenced
    /// and starts a recovery attempt; the genesis primary (index 0)
    /// self-promotes on a tick and broadcasts Commit; the Recovering backups
    /// adopt the view under the §4 bootstrap rule.
    fn boot_cluster() -> [Node; 3] {
        let mut nodes = [
            provision("cluster-one", 0, 3),
            provision("cluster-two", 1, 3),
            provision("cluster-three", 2, 3),
        ];
        for (index, node) in nodes.iter_mut().enumerate() {
            assert_eq!(node.recover(), OK, "node {index} starts recovery");
            assert_eq!(
                node.replica.progress().status(),
                vrr::progress::Status::Recovering
            );
            node.outputs.clear();
        }
        assert_eq!(nodes[0].drive(Input::Tick), OK);
        assert_eq!(
            nodes[0].replica.observer().read().status,
            0,
            "genesis primary promotes to Normal"
        );
        route_until_quiet(&mut nodes);
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

    #[test]
    fn committed_request_produces_a_correlated_reply_and_duplicate_replay() {
        let mut nodes = boot_cluster();
        let message_id = Uuid::from_bytes([7; 16]);
        let payload = request_json(message_id);

        // Propose on the primary (member 0 in the era-1 genesis view).
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
        route_until_quiet(&mut nodes);

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
        assert_eq!(request(&mut nodes[1], &payload), NOT_LEADER);
        assert!(nodes[1].outputs.is_empty());
        assert_eq!(nodes[1].replica.observer().read().accepted, 2);
    }

    #[test]
    fn malformed_and_oversize_ingress_are_refused() {
        let mut nodes = boot_cluster();
        let garbage = [0xFFu8; 64];
        assert_eq!(receive(&mut nodes[0], 1, &garbage), VRR_MESSAGE);
        let truncated = [0u8; 4];
        assert_eq!(receive(&mut nodes[0], 1, &truncated), VRR_MESSAGE);
        let oversize = vec![0u8; MAX_DATAGRAM + 1];
        assert_eq!(receive(&mut nodes[0], 1, &oversize), TOO_LARGE);
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
        assert_eq!(receive(&mut nodes[1], 0, &buf), VRR_MESSAGE);
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
        assert_eq!(leader, 0, "genesis primary is member 0");

        let mut for_view = u32::MAX;
        assert_eq!(
            unsafe {
                lunet_lock_node_leader_for_view((&raw mut nodes[1]).cast(), 1, 1, &mut for_view)
            },
            OK
        );
        assert_eq!(for_view, 1, "view 1's primary is member 1");
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
        // The full C surface: members are NUL-separated names, positional
        // index is the identity, state is the nonce file path.
        let members = b"n1\0n2\0n3";
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
                    &mut handle,
                )
            },
            OK
        );
        assert!(!handle.is_null());

        // Boot drove one recovery attempt: a Recovery solicitation per
        // backup, drained through node_next as kind-1 sends.
        let (mut kind, mut to, mut era, mut view) = (0u32, 0u32, 0u32, 0u32);
        let (mut slot_hi, mut slot_lo) = (0u32, 0u32);
        let mut message_id = [0u8; 16];
        let mut len = 0usize;
        let mut buffer = [0u8; MAX_DATAGRAM];
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
            assert!(matches!(message.body, Body::Recovery { .. }));
            destinations.push(to);
        }
        assert_eq!(destinations, vec![1, 2], "recovery fans out to the backups");

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
        assert_eq!(leader, 0);

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
                    b"n1\0n2".len(),
                    b"n1\0n2".as_ptr(),
                    b"n9".len(),
                    b"n9".as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    &mut handle,
                )
            },
            CONFIG
        );
        // duplicate member.
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    b"n1\0n1".len(),
                    b"n1\0n1".as_ptr(),
                    b"n1".len(),
                    b"n1".as_ptr(),
                    state_bytes.len(),
                    state_bytes.as_ptr(),
                    &mut handle,
                )
            },
            CONFIG
        );
        assert!(handle.is_null());
    }
}
