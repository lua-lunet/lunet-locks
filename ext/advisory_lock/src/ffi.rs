//! Host-side FFI adapter between the LuaJIT host and the uVRR core
//!
//! Concrete core: `Replica<SegmentedLog, WeightedMajority>` running
//! `Stability::Volatile` — nothing is persisted but the boot gate's
//! markers, so a process that died while operating has no same-identity
//! clean restart in this embedder: its restart is CRASHED. A graceful
//! stop is the one same-identity restart (the boot gate's stopped
//! quorum, below). The restart story is upstream's Crash-Stop-Self-Evict
//! protocol (`src/replica/reincarnation.rs`): the durable markers carry
//! the incarnation (the quorum-of-copies construction, `marker_store`),
//! a crashed boot decides the bumped pair, the bumped node drives
//! `Input::Reincarnate { old }` — the `Reincarnation(old, new)` entry
//! ticket — and the stable leader computes `forced_steps` idempotently from
//! the committed configuration, proposing each remaining era's batch
//! through the ordinary reconfiguration pipeline, continued tick-driven,
//! until the new identity sits at weight 1 in the old succession position
//! and the old identity is evicted. The reincarnated node reopens over the
//! deployment's genesis (the only true shared history a Volatile process
//! has) and acquires nothing beyond its genesis — the §10 learner
//! acquisition streams it the committed history through its boot fence —
//! so it replays no lock state it did not commit, exactly like the joiner.
//!
//! Adapter policies the core deliberately does not own:
//!
//! - **Tick clock.** The adapter owns the tick clock: a monotonic
//!   nondecreasing milliseconds-since-Unix-epoch value, clamped per node so
//!   a clock regression never reaches the core. ABI functions take no `at`
//!   parameter. Ticks come from the ms clock only; the durable state file
//!   is the incarnation marker, not a tick source (the old nonce-as-tick
//!   role is retired with the amnesia protocol it served).
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
//!   `:j` entry boots as a JOINER: `Replica::join` over the deployment's
//!   genesis (the entries `provision` installs, mirrored byte-for-byte, plus
//!   the era table folded from them), fenced `Restarting`, addressed but
//!   outside every configuration until a committed `Join` admits it. Upstream
//!   has no fresh-node catch-up at this commit: the establishing operation's
//!   fan-out reaches configuration members only and the `GetState` serving
//!   gate serves configuration members only, so the joiner evaluates only
//!   the era-1 traffic its genesis table covers and drops everything past it
//!   by name — the same boundary upstream's own learner corpus states
//!   (§10 acquisition, future work). The joiner fabricates nothing.
//! - **Identity (the restart story).** The boot classification is the
//!   engine's (`lifecycle::boot` over the superblock quorum store,
//!   `vrr::lifecycle`): no marker ever written is the FIRST life — the
//!   first latch anchors the genesis life (crash counter 1); a stopped
//!   quorum is a CLEAN continue under the SAME identity
//!   (`Replica::resume` behind the engine's `Vouched` token); no
//!   stopped quorum is a CRASH — the identity is dead, the replacement
//!   pair is decided at boot (`Crashed::pair`), and THE EMISSION GATE
//!   lands the bump's one durable marker round — the next life at the
//!   running sentinel — before the driver releases the first
//!   announcement, unconditional, seated or not. The engine's session
//!   is held so its typestate can latch the same round again once the
//!   seated observation mints the witness (`Replica::rejoined`): the
//!   same identity and state, idempotent on the marker.
//!   Membership is the admin-assigned deployment descriptor: the member
//!   buffer carries NUL-separated `<u32-id>:<name>` entries, in the
//!   descriptor's line order. Each id is the member's provisioned
//!   identity — the packed pair (system half, crash counter 1); the
//!   marker's crash counter carries the life from there, so a crashed
//!   boot's announced id is the strict next life of the same system.
//!   The buffer order is the genesis succession sequence
//!   (`primary(v) = order[v mod N]`). Member ids are the ABI's peer
//!   addresses: `receive`'s `from`, send outputs' `to`, and the leader
//!   outs carry the live packed ids; the host maps id -> endpoint
//!   through the descriptor, and a later life of a member rides the
//!   transport's remap. `own` is matched by name. A post-genesis
//!   (joined-later) entry carries a `:j` suffix: `<u32-id>:<name>:j`. Such
//!   entries are outside the genesis order — the core's `provision` refuses
//!   an `own` that is not a founding member — so a node whose `own` names a
//!   `:j` entry boots as a JOINER: `Replica::join` over the deployment's
//!   genesis (the entries `provision` installs, mirrored byte-for-byte, plus
//!   the era table folded from them), fenced `Restarting`, addressed but
//!   outside every configuration until a committed `Join` admits it. Upstream
//!   has no fresh-node catch-up at this commit: the establishing operation's
//!   fan-out reaches configuration members only and the `GetState` serving
//!   gate serves configuration members only, so the joiner evaluates only
//!   the era-1 traffic its genesis table covers and drops everything past it
//!   by name — the same boundary upstream's own learner corpus states
//!   (§10 acquisition, future work). The joiner fabricates nothing.
//!   A crashed boot reopens the
//!   reincarnation way — a later life over the deployment's genesis
//!   (`Replica::reincarnate` under the bumped identity; there is no
//!   durable journal to carry forward) — and announces
//!   `Input::Reincarnate { old }` on the host's fenced-boot drives
//!   (`recover` — the §8 re-announce; the core self-gates: a member
//!   already voting at weight ≥ 1 has nothing to announce). The boot
//!   itself emits nothing: a reopened node answers only what it is
//!   driven with, and the announcement is the first fenced-boot
//!   drive's work.
//!   `lunet_lock_node_own_id` reports the live identity so the host can
//!   compare leaders against it after a bump.
//! - **Termination (the stop story).** The ENGINE owns the stop
//!   schedule: the Running session's
//!   `begin_stop` writes the halt's first round (`Stopping` — it
//!   vouches for nothing), the host drain forces the committed-
//!   transition sink to quiescence strictly between the rounds, and
//!   `finish_stop` writes the drain-proven second round (`Stopped` — a
//!   stopped quorum at the next boot proves the clean stop). The host
//!   obligations stay ours: the wire closes FIRST (the `stopped` flag
//!   refuses every further inbound entry — `request`, `receive`,
//!   `idle`, `leader_timeout`, `force_view`, `recover`, `reconfigure` —
//!   before any marker write and before any task processing, making the
//!   in-memory state final), and the durable write is the existing
//!   sink drain. A node still inside the crashed path's unseated window
//!   (its engine session not yet latched) cannot run the halt's rounds —
//!   the stop closes the wire, drains the sink, and leaves the markers
//!   at the emission gate's round (the new life's running sentinel); the
//!   next boot re-classifies crashed and derives the strictly next
//!   life. Marker storage: the lifecycle rides the
//!   vendored Zig store's quorum-of-copies superblock construction
//!   (four fixed sector-aligned Aegis-checksummed copies, hash-chained
//!   sequence/parent, quorum write with forced I/O verified at the 3/4
//!   threshold, quorum read resolving by highest sequence at the 2/4
//!   threshold, through the AOF C ABI's marker exports and linked
//!   statically so this cdylib stays self-contained); the single
//!   fsynced flag file remains as the compatibility projection — written
//!   after every quorum write, never a classification input. An
//!   unreadable marker quorum refuses the boot
//!   (`BootError::QuorumLost` shape): no projection fallback, no
//!   guessed identity.
//! - **Reconfiguration.** `lunet_lock_node_reconfigure` drives
//!   `Input::Reconfigure { op, pivot }` (Join at weight 0 / Increment /
//!   Decrement (voter to learner) / Leave at weight 0) on the current primary
//!   through the ordinary
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
//!   `view_change_budget` is `EVIDENCE_BUDGET` — the largest core-built
//!   suffix or chunk this host puts on one datagram, sized to what a
//!   default UDP socket actually delivers (see the constant's note).
//! - **Peer payload gate.** The core carries operation payloads opaque and
//!   validates none of them (B2), so the adapter re-checks every peer-carried
//!   operation entry (Prepare / DoViewChange / StartView / NewState) with
//!   `Service` before the message reaches the core — the same gate the old
//!   adapter called `valid_message_payload`.
//! - **Self-arrest reporting.** The core's never-repair contract makes a
//!   fault sticky: a legality-gate breach, a journal refusal, or a boundary
//!   panic self-arrests the node — poison is permanent and every further
//!   entry reports SERVICE, by design (the node never continues past a
//!   violated invariant). What the contract never allowed was SILENCE: the
//!   first fault observation records its reason, says it once on stderr,
//!   and exports it ([`NodeStatus`] and `lunet_lock_node_fault`) so the
//!   runbook can tell a breached node from a wedged one without
//!   restarting anything. The arrest itself is unchanged.
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

use crate::aof::{AofConfig, AofWriter};
use crate::disk::{Disk, StdDisk, std_disk};
use crate::journal::{self, Journal as LockJournal, JournalEvent};
use crate::locks::{Service, Transition};
use crate::recovery_flush::{self, FlushOutcome, RecoveryFlush};
#[cfg(any(test, debug_assertions))]
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsString, c_void};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, trace, warn};
use vrr::configuration::{EraTable, INIT_SLOT, MAX_MEMBERS, SystemOperation, VOID_SLOT};
use vrr::effects::{Effect, Stability};
use vrr::ids::{
    Ballot, CrashCounter, Era, NodeId, Operation, OperationId, Slot, SystemId, Tick, View,
};
use vrr::journal::{Journal, LogEntry, Payload, SegmentedLog};
use vrr::lifecycle::{self, BootError, BootOutcome, Bumped, Crashed, Marker, Running, Vouched};
use vrr::message::{Body, Message};
use vrr::observe::Diagnostic;
use vrr::progress::Status;
use vrr::quorum::{WeightedMajority, construct_pivot};
use vrr::replica::{
    Input, PersistedProgress, Pivot, PlanRefusal, PublishOutcome, PublishRefusal, Replica,
    TimedInput, ViewChangeKnobs,
};
use vrr::timeout::UNFLUSHED_RUNBOOK;
use vrr::wire::{Pack, Tag, Unpack, UnpackError};

use crate::marker_store::{GateStore, SinkDoor, drain_sink, sink_guard};

/// An unexpected-but-not-provably-impossible condition — a "maybe" in the
/// TigerBeetle model — reported where the path today logs nothing.
///
/// The two-tier convention this adapter follows:
///
/// - **Invariants** are conditions whose impossibility the surrounding code
///   establishes (a reply cache hit must never re-execute the Service, ticks
///   are nondecreasing, a reincarnated identity never reuses the old id, the
///   output queue carries only kinds 1/2, a poisoned node executes nothing).
///   They are `assert!`ed: a violation is a bug and the node stops. Through
///   the Rust API (`Node`) the panic propagates to the embedder's process;
///   through the C ABI the boundary's `catch_unwind` poisons the node and
///   reports `PANIC` — an unwound panic must never cross into C. Either way
///   the node never continues past a violated invariant, in tests AND
///   release.
/// - **Maybes** are conditions that are not provably impossible and are
///   often adversary- or environment-adjacent (a datagram attributed to an
///   unknown peer id, a send to an unaddressable replica, an ack for an
///   unclaimed verb, a folded-era regression). In test/debug builds
///   (`cfg!(test)` or `cfg!(debug_assertions)`) the macro panics, so the
///   test suite and any smoke run surface it; in release it logs `warn!`
///   with full context and continues — mirroring upstream's totality stance
///   (`wire.rs`: "a decoder that aborts the host process on a hostile
///   datagram is a denial-of-service vector"), which never lets hostile or
///   merely unexpected input crash a release build.
///
/// Upstream (`uvrr-core`) has no equivalent helper — its
/// `src/invariant.rs` names drops through the `Diagnostic` observation and
/// faults impossible local transitions, but neither asserts nor warns; the
/// convention is proposed upstream and this macro is
/// the reference implementation. It is exported so downstream embedders
/// (e.g. the `lease-sequencer` example) report their host-side maybes under
/// the same convention.
#[macro_export]
macro_rules! maybe_invariant {
    ($($arg:tt)*) => {
        if cfg!(test) || cfg!(debug_assertions) {
            panic!("maybe-invariant violation: {}", format_args!($($arg)*));
        } else {
            ::tracing::warn!(
                ts = $crate::log_millis(),
                event = "maybe-invariant",
                $($arg)*
            );
        }
    };
}

/// The wall-clock millisecond every log line this crate emits carries:
/// the same host clock the marker layer's `unix_millis` reads and the
/// same one the embedding host stamps its own lines with, so the log's
/// order is the host's order. A clock reading before the UNIX epoch
/// carries no millisecond; the line then reads `0` and nothing else
/// decides on it — no protocol path reads a log field.
#[doc(hidden)]
#[must_use]
pub fn log_millis() -> u64 {
    unix_millis().unwrap_or(0)
}

/// The view-change message family (`vrr::wire::Tag`): the NOMINATE
/// computation's own traffic — the fence, the evidence, the selected
/// history. Ordinary replication traffic stays at `trace` (it is the
/// heartbeat's subject, and the host logs every heartbeat commit with
/// its slot, view and era), so the whole fence is visible in the log at
/// the default `RUST_LOG=info` without the replication flood.
fn is_view_change_family(tag: Tag) -> bool {
    matches!(
        tag,
        Tag::StartViewChange
            | Tag::DoViewChange
            | Tag::PlannedViewChange
            | Tag::StartView
            | Tag::GetState
            | Tag::NewState
    )
}

/// One lifecycle path line: the named census entry that a boot or stop path
/// emits as it takes it. The observability contract's third clause — "the
/// zero-cost `maybe!`/trace discipline logs every path the node takes
/// through boot, drain, flush, and stop — a path that cannot show itself in
/// the trace is a defect" — is carried here, alongside the
/// [`maybe_invariant!`](crate::maybe_invariant) discipline it extends.
///
/// The convention, stated once and applied everywhere:
///
/// - A path's name is its stable identity: `<subject>.<verb>[.<qualifier>]`,
///   lowercase, hyphenated. `boot.crashed`, `marker.read.corrupt`,
///   `stop.drain-window.open`. The name never changes shape when the code
///   around it moves, because the proving test asserts on it.
/// - EVERY path through boot and through stop names itself, refusals
///   included, with its own line. A refusal that shares another refusal's
///   line is a path that cannot show itself.
/// - The line costs nothing when the tape is not wanted. The expansion is a
///   `#[cfg]`-gated call on a `&'static str`: a release build carries no
///   tape, no push, and no formatting — the call site compiles to nothing
///   rather than formatting and discarding.
/// - The census tape is THREAD-LOCAL, not per node: a boot that REFUSES
///   hands back no [`Node`], so a node-scoped tape could not carry the
///   refusal paths at all. Thread-local is the granularity that works for
///   both the node-bearing and the node-refusing paths, and it keeps
///   concurrent callers' lines apart. [`crate::census_paths`] is the reader
///   that drains it.
///
/// Unlike [`maybe_invariant!`] this is not a convention for downstream
/// hosts: it names this adapter's own boot and stop paths.
#[macro_export]
macro_rules! trace_line {
    ($path:expr) => {{
        #[cfg(any(test, debug_assertions))]
        $crate::census_push($path);
    }};
}

// The lifecycle census tape: every named path line the calling thread
// took, in order. Present in the test and dev profiles (the builds that
// run the proving test and the cloud rigs); absent from a release build,
// where `trace_line!` expands to nothing.
#[cfg(any(test, debug_assertions))]
std::thread_local! {
    static LIFECYCLE_CENSUS: RefCell<Vec<&'static str>> = const { RefCell::new(Vec::new()) };
}

/// Records one path line on the calling thread's census tape
/// ([`trace_line!`]).
#[cfg(any(test, debug_assertions))]
#[doc(hidden)]
pub fn census_push(path: &'static str) {
    LIFECYCLE_CENSUS.with_borrow_mut(|tape| tape.push(path));
}

/// The calling thread's census tape, drained: the lines it emitted since
/// the last read, in emission order.
#[cfg(any(test, debug_assertions))]
#[doc(hidden)]
pub fn census_paths() -> Vec<&'static str> {
    LIFECYCLE_CENSUS.with_borrow_mut(std::mem::take)
}

pub const OK: i32 = 0;
pub const INVALID: i32 = -1;
pub const CONFIG: i32 = -2;
pub const CLIENT_JSON: i32 = -4;
pub const VRR_MESSAGE: i32 = -5;
pub const TOO_LARGE: i32 = -6;
pub const SERVICE: i32 = -7;
pub const NOT_LEADER: i32 = -8;
pub const FAULTED: i32 = -9;
/// The drain point's refusal code: the node has stopped and the wire is
/// closed — every inbound entry (request, receive, ticks, admin drives)
/// reports STOPPED and processes nothing.
pub const STOPPED: i32 = -10;
pub const PANIC: i32 = -127;

/// The queued output's kinds (`NodeOutput.kind`): a unicast peer datagram
/// or a client reply. The names the human surfaces spell for them ride
/// `output_kind_name`.
pub const OUTPUT_SEND: u32 = 1;
pub const OUTPUT_REPLY: u32 = 2;

/// The output kind's name, for every surface a human reads: `send`,
/// `reply`, or `unknown(N)` for anything else — never a bare integer for
/// a human to memorise.
pub fn output_kind_name(kind: u32) -> &'static str {
    match kind {
        OUTPUT_SEND => "send",
        OUTPUT_REPLY => "reply",
        _ => "unknown",
    }
}

/// Host packetization bound (W5: the core owns no size limit). One IPv4/IPv6
/// UDP datagram, matching `transport.tl`.
const MAX_DATAGRAM: usize = 65507;

/// Ticks (milliseconds) of primary silence before a backup fences into the
/// next view. Host policy; correctness never depends on it. The default
/// every [`Node::open`] caller passes; a caller that pins its own policy
/// passes its own value.
pub const PRIMARY_TIMEOUT_MS: u64 = 5000;

/// The host's evidence-and-transfer datagram budget (W5): the largest
/// core-built suffix or chunk this host will put on one datagram. The
/// core's own cap is `MAX_DATAGRAM` (one UDP datagram), but the wire path
/// here is the LAL peer envelope over a host UDP socket, and a default
/// socket's send buffer caps a single send well below that limit — on
/// macOS the default is 9216 bytes, and a send past it fails with
/// `EMSGSIZE` and the datagram silently never arrives. A view-change
/// evidence or `StartView` suffix sized past that budget would then
/// strand the very view change it carries: the designated primary waits
/// forever on evidence that was sent and lost. 8192 leaves headroom for
/// the envelope under every default UDP send buffer this stack runs on;
/// the transfer machinery resumes whatever does not fit through the
/// fetch cursor (§13.1 step 5), so correctness never depends on the
/// number — only liveness does.
const EVIDENCE_BUDGET: usize = 8192;

/// Leader/primary unknown (era outside the core's three-era retention
/// window, or the void configuration): the value status and
/// leader-for-view report in that case.
const LEADER_UNKNOWN: u32 = u32::MAX;

/// The lawful packing's halves: the system identifier is the high
/// sixteen bits of a packed `NodeId`, the crash counter the low sixteen.
const SYSTEM_HALF_SHIFT: u32 = 16;

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

/// The node construction's runtime policy: the view-change timeout the
/// cluster plays by.
struct Construction {
    primary_timeout: u64,
}

pub struct Node {
    replica: Core,
    outputs: VecDeque<Queued>,
    service: Service,
    /// message_id bytes -> cached response bytes, for duplicate replay (B2).
    replies: HashMap<[u8; 16], Vec<u8>>,
    /// Operations proposed through this node's `request` entry and not yet
    /// applied: `OperationId` -> message_id bytes. Only these get replies.
    pending: HashMap<OperationId, [u8; 16]>,
    last_tick: u64,
    poisoned: bool,
    /// The recorded reason of the FIRST self-arrest observation (the
    /// core's never-repair fault, or a boundary panic): `None` until the
    /// node arrests. The sticky fault would otherwise repeat the
    /// observation on every drive, so only the first is recorded and
    /// said — loudly, on stderr, where the runbook reads.
    fault_note: Option<String>,
    /// The superseded identity when this node booted a DIRTY restart: the
    /// bumped node re-announces `Reincarnation(old, new)` on every
    /// fenced-boot drive (§8). `None` for an incarnation-0 boot.
    reincarnate_from: Option<NodeId>,
    /// The descriptor's member ids (the low band, `:j` entries included):
    /// the address space `receive` accepts. A message attributed to a
    /// low-band id outside this set is a maybe (an unknown peer id).
    known_ids: HashSet<u32>,
    /// The last (era, view) reported for lifecycle `debug!` events.
    last_view: Option<(u32, u32)>,
    /// The last leader reported for lifecycle `info!` events.
    last_leader: Option<u32>,
    /// The last folded configuration era, for the folded-era-regression
    /// maybe.
    last_config_era: Option<u32>,
    /// The lock-event sink. `None` when journaling is disabled (empty
    /// journal_dir at construction) or after a journal error.
    ///
    /// - `Blocking` writes the classic per-replica journal: blocking
    ///   buffered appends on the apply path; an append error disables it.
    /// - `Aof` enqueues to the async write-behind writer: producers never
    ///   wait on disk, overflow drops (the writer's drop counter tracks
    ///   it), and fsync happens only on the timer, at roll, at checkpoint,
    ///   and at shutdown.
    ///
    /// The door is shared with the boot gate's store: the engine's halt
    /// schedule forces it through `LifecycleStore::drain`, strictly
    /// between the two marker rounds.
    sink: SinkDoor,
    /// The durable incarnation-marker path: the boot gate's store rides
    /// it; the single-file projection the operators read mirrors it.
    state_path: PathBuf,
    /// The seam the node's own disk work crosses — the drain window's
    /// view-record write, above all. Dynamic, never generic: the C ABI
    /// surface stays monomorph-free.
    disk: Arc<dyn Disk>,
    /// The boot gate's latched session: the Running typestate whose
    /// schedule the stop path drives. `None` while the crashed
    /// classification's engine session is unlatched (the seated witness
    /// has not minted) and after the halt consumed it.
    session: Option<Running<GateStore>>,
    /// The crashed classification's engine session: held until the
    /// engine's seated observation (`Replica::rejoined`) mints the
    /// witness, then latched once — the same marker round the emission
    /// gate already made durable at boot, idempotent.
    deferred: Option<Crashed<GateStore>>,
    /// The drain point's wire-closed flag: set BEFORE any marker write at
    /// stop, and refusing every further inbound entry while set — the
    /// mandatory obligation that makes the in-memory state final.
    stopped: bool,
    /// The Flight Recorder's tape (the `flight-recorder` feature): the
    /// per-node internal trace. `None` without the feature (the field
    /// itself is compiled out) and whenever the env did not name a
    /// flight directory — the prod path carries nothing.
    #[cfg(feature = "flight-recorder")]
    flight: Option<crate::flight::FlightRecorder>,
}

/// The committed-transition sink behind `Node`'s journal hook.
pub(crate) enum JournalSink {
    Blocking(LockJournal),
    Aof(AofWriter),
}

impl Node {
    /// The next monotonic tick from the adapter-owned ms clock (never
    /// decreasing per node, even across a wall-clock regression). Ticks
    /// come from the clock only — the durable state file is the incarnation
    /// marker, not a tick source.
    fn tick(&mut self) -> Result<u64, i32> {
        let now = unix_millis()?;
        let next = self.last_tick.max(now);
        // Invariant (asserted, always): ticks are nondecreasing — the clamp
        // holds even across a wall-clock regression.
        assert!(
            next >= self.last_tick,
            "tick regression: last={} next={}",
            self.last_tick,
            next
        );
        self.last_tick = next;
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
    /// clock. The Flight Recorder wraps every drive's outcome (the
    /// `flight-recorder` feature).
    fn drive_at(&mut self, at: u64, event: Input) -> i32 {
        #[cfg(feature = "flight-recorder")]
        self.flight_log("drive-in", Self::flight_input_summary(&event));
        let code = self.drive_at_inner(at, event);
        #[cfg(feature = "flight-recorder")]
        self.flight_log("drive-out", serde_json::json!({ "code": code }));
        self.settle_deferred_latch();
        code
    }

    /// The engine session's latch settle: the engine's seated
    /// observation (`Replica::rejoined` — `Normal` at voting weight)
    /// mints the witness, and the engine latches the crashed
    /// classification — `(new, Joining)` written 4x through the machine,
    /// the same round the emission gate made durable at boot. Checked
    /// after every drive. A failed latch write
    /// keeps the session — the markers already hold the emission gate's
    /// round, and the next drive retries.
    fn settle_deferred_latch(&mut self) {
        if self.poisoned || self.stopped {
            return;
        }
        let Some(crashed) = self.deferred.take() else {
            return;
        };
        let Some(witness) = self.replica.rejoined() else {
            self.deferred = Some(crashed);
            return;
        };
        match crashed.latch(witness) {
            Ok(running) => {
                trace_line!("boot.deferred-latch");
                let snapshot = self.replica.observer().read();
                info!(
                    ts = log_millis(),
                    event = "deferred-latch",
                    node = self.replica.own().0,
                    identity = running.identity().0,
                    era = snapshot.era,
                    view = snapshot.view,
                    "the deferred latch landed: the bumped identity is durable"
                );
                self.session = Some(running);
            }
            Err((crashed, error)) => {
                trace_line!("boot.deferred-latch-refused");
                eprintln!(
                    "lunet-advisory-lock: the deferred latch write failed ({error:?}); \
                     the marker stays at the crashed identity and the latch retries"
                );
                self.deferred = Some(crashed);
            }
        }
    }

    /// One plan/publish/effects cycle, looping on the host acknowledgements
    /// (`Input::Applied`) the effects require, until the core goes quiet.
    /// Under `Stability::Volatile` every publish is `Published`; a `Parked`
    /// outcome is an invariant/API mismatch, so the node is poisoned rather
    /// than allowed to fabricate a `StabilityResult::Stable`.
    ///
    /// The feedback queue drains FIRST-IN-FIRST-OUT: the core's §11.1
    /// acknowledgement refuses any `Input::Applied` report but the next
    /// expected slot, and a publish whose effects apply several operations
    /// (a gap-served chunk, a view-change install — the live shapes any
    /// client stream produces) must report its completions in the slot
    /// order the effects were emitted in. A last-in-first-out drain
    /// reports the newest slot first, refuses against its own publish, and
    /// abandons the drive after it — with the install already published
    /// and the applied frontier stranded behind it.
    fn drive_at_inner(&mut self, at: u64, event: Input) -> i32 {
        if self.poisoned {
            return SERVICE;
        }
        let mut pending: VecDeque<TimedInput> = VecDeque::new();
        pending.push_back(TimedInput {
            at: Tick(at),
            event,
        });
        while let Some(input) = pending.pop_front() {
            if self.poisoned {
                return SERVICE;
            }
            let result = catch_unwind(AssertUnwindSafe(|| {
                // Invariant (asserted, always): poison means poisoned — no
                // post-poison execution. The loop-top guard reports SERVICE
                // for a poisoned node (the host may poll it); the execution
                // point itself must never be reached while poisoned.
                assert!(!self.poisoned, "post-poison execution");
                let planned = match self.replica.plan(&input, &self.replica.journal().view()) {
                    Ok(planned) => planned,
                    Err(PlanRefusal::Faulted(fault)) => {
                        // The core's never-repair contract: the fault is
                        // sticky and the node self-arrests. Record WHY —
                        // this observation is the operator's only chance
                        // to see the reason.
                        self.record_fault(format!("sticky fault: {fault:?}"));
                        return Err(FAULTED);
                    }
                    Err(refusal) => return Err(plan_error(refusal)),
                };
                match self.replica.publish(planned) {
                    Ok(PublishOutcome::Published { effects, .. }) => Ok(effects),
                    Ok(PublishOutcome::Parked { revision, .. }) => {
                        // Parked under Volatile: the durability handshake
                        // the core expects does not exist in this host.
                        self.record_fault(format!(
                            "publish parked under Stability::Volatile (revision {revision})"
                        ));
                        Err(FAULTED)
                    }
                    Err(PublishRefusal::IllegalCandidate(fault)) => {
                        // The closed legality gate (or the planner-declared
                        // breach) refused the candidate: the core faulted the
                        // node and the next drive observes it. The return
                        // code is unchanged (SERVICE, as every publish
                        // refusal reports); the reason is the fix.
                        self.record_fault(format!(
                            "the legality gate refused the candidate: {fault:?}"
                        ));
                        Err(SERVICE)
                    }
                    Err(PublishRefusal::JournalRefused(error)) => {
                        self.record_fault(format!(
                            "the journal refused the planned mutation: {error:?}"
                        ));
                        Err(SERVICE)
                    }
                    Err(_) => Err(SERVICE),
                }
            }));
            let effects = match result {
                Ok(Ok(effects)) => effects,
                Ok(Err(error)) => {
                    if error == FAULTED {
                        // The self-arrest: poison is sticky and every
                        // further entry reports SERVICE — but never
                        // silently. Say the arrest once, with the
                        // recorded reason when the core named one.
                        eprintln!(
                            "lunet-advisory-lock: node self-arrested ({}); \
                             every further entry reports SERVICE",
                            self.fault_note
                                .as_deref()
                                .unwrap_or("the core refused a transition")
                        );
                        self.poisoned = true;
                        self.outputs.clear();
                        return SERVICE;
                    }
                    return error;
                }
                Err(payload) => {
                    // A panic unwound at the boundary: the default hook has
                    // already printed it. Name the arrest with the payload
                    // so the fault report carries it, then poison — an
                    // unwound panic must never cross into C.
                    let message = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                        .unwrap_or_else(|| "an unprintable panic".to_string());
                    self.record_fault(format!("panic: {message}"));
                    eprintln!(
                        "lunet-advisory-lock: node self-arrested (panic); \
                         every further entry reports SERVICE"
                    );
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
        self.report();
        OK
    }

    /// The self-arrest bookkeeping: record the reason of the FIRST fault
    /// observation and say it once, loudly. The core's never-repair
    /// contract makes the fault sticky — every later drive repeats the
    /// observation — so the first is the operator's only chance to see
    /// WHY a node (and then, cascading, a cluster) stopped serving. The
    /// note rides [`NodeStatus`] and the `lunet_lock_node_fault` ABI for
    /// the runbook; the arrest itself is unchanged: poison is sticky and
    /// every further entry reports SERVICE.
    fn record_fault(&mut self, note: String) {
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "fault",
            serde_json::json!({
                "what": note,
                "arrest": self.fault_note.is_none(),
            }),
        );
        if self.fault_note.is_none() {
            eprintln!("lunet-advisory-lock: the node self-arrests — {note}");
            self.fault_note = Some(note);
        }
    }

    /// One Flight Recorder event (the `flight-recorder` feature): a no-op
    /// with the recorder absent. Every call site is `#[cfg]`-gated, so
    /// the flag-OFF build compiles nothing here — the prod path carries
    /// zero recorder code.
    #[cfg(feature = "flight-recorder")]
    fn flight_log(&mut self, kind: &str, detail: serde_json::Value) {
        if let Some(recorder) = self.flight.as_mut() {
            recorder.event(kind, detail);
        }
    }

    /// One inbound Flight Recorder event carrying the raw bytes (the
    /// flight recording's `frame_hex`, byte-exact for the playback
    /// engine). The call sites are `#[cfg]`-gated.
    #[cfg(feature = "flight-recorder")]
    fn flight_bytes_in(&mut self, kind: &str, from: u32, bytes: &[u8]) {
        self.flight_log(
            kind,
            serde_json::json!({
                "from": from,
                "len": bytes.len(),
                "hex": crate::flight::hex(bytes),
            }),
        );
    }

    /// The Flight Recorder's summary of one core input (the
    /// `flight-recorder` feature). Raw bytes ride the entry-level events
    /// (`receive-in`, `request-in`); this names the internal inputs the
    /// entries never see (the Applied feedback the drive loop feeds
    /// itself, the boot's Reincarnate announcement).
    #[cfg(feature = "flight-recorder")]
    fn flight_input_summary(input: &Input) -> serde_json::Value {
        match input {
            Input::Peer { from, message } => serde_json::json!({
                "input": "peer",
                "from": from.0,
                "era": message.header.view.era.0,
                "view": message.header.view.view.0,
                "slot": message.header.slot.0,
                "tag": message.header.tag.name(),
            }),
            Input::Propose { operation } => serde_json::json!({
                "input": "propose",
                "len": operation.payload.len(),
            }),
            Input::Tick => serde_json::json!({"input": "tick"}),
            Input::Applied { slot } => {
                serde_json::json!({"input": "applied", "slot": slot.0})
            }
            Input::Checkpointed { through } => {
                serde_json::json!({"input": "checkpointed", "through": through.0})
            }
            Input::StabilityConfirmation { revision, result } => serde_json::json!({
                "input": "stability-confirmation",
                "revision": revision,
                "result": format!("{result:?}"),
            }),
            Input::Reconfigure { op, pivot } => serde_json::json!({
                "input": "reconfigure",
                "op": format!("{op:?}"),
                "pivot": pivot.is_some(),
            }),
            Input::AdminForceView { target } => serde_json::json!({
                "input": "force-view",
                "era": target.era.0,
                "view": target.view.0,
            }),
            Input::Reincarnate { old } => {
                serde_json::json!({"input": "reincarnate", "old": old.0})
            }
            Input::SubmitPlan { .. } => {
                serde_json::json!({"input": "submit-plan"})
            }
        }
    }

    /// The lifecycle/observability read after a quiet drive: the named
    /// drop diagnostic (the core publishes every transition's drop outcome
    /// and nobody read it before), the view-change `debug!`, the
    /// leader-change `info!`, and the folded-era-regression maybe. One
    /// seqlock read per drive, level-gated formatting.
    fn report(&mut self) {
        let diagnostic = self.replica.observer().read_diagnostic();
        if diagnostic != Diagnostic::None {
            let snapshot = self.replica.observer().read();
            warn!(
                ts = log_millis(),
                event = "peer-input-dropped",
                node = self.replica.own().0,
                era = snapshot.era,
                view = snapshot.view,
                ?diagnostic,
                "peer input dropped with a named diagnostic"
            );
        }
        let snapshot = self.replica.observer().read();
        if self.last_view != Some((snapshot.era, snapshot.view)) {
            debug!(
                ts = log_millis(),
                event = "view-change",
                node = self.replica.own().0,
                era = snapshot.era,
                view = snapshot.view,
                "view changed"
            );
            self.last_view = Some((snapshot.era, snapshot.view));
        }
        let leader = self.primary_index();
        if self.last_leader != Some(leader) {
            info!(
                ts = log_millis(),
                event = "leader-change",
                node = self.replica.own().0,
                era = snapshot.era,
                view = snapshot.view,
                leader,
                "leader change"
            );
            self.last_leader = Some(leader);
        }
        let config_era = self.replica.progress().config().current().era.0;
        if folded_era_regressed(self.last_config_era, config_era) {
            #[cfg(feature = "flight-recorder")]
            self.flight_log(
                "maybe",
                serde_json::json!({
                    "where": "report",
                    "what": "folded configuration era regressed",
                    "previous": self.last_config_era,
                    "current": config_era,
                }),
            );
            maybe_invariant!(
                "folded configuration era regressed (previous={}, current={})",
                self.last_config_era.unwrap_or_default(),
                config_era
            );
        }
        self.last_config_era = Some(config_era);
    }

    fn apply_effect(
        &mut self,
        effect: Effect,
        pending: &mut VecDeque<TimedInput>,
    ) -> Result<(), i32> {
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
                // The NOMINATE computation's outbound half: every fence,
                // every piece of evidence and every selected history this
                // node sends is a line, with the ballot and the slot it
                // speaks at. Ordinary replication traffic stays at `trace`
                // (the host logs each heartbeat commit).
                if is_view_change_family(header.tag) {
                    info!(
                        ts = log_millis(),
                        event = "nominate-out",
                        node = self.replica.own().0,
                        to = to.0,
                        era = header.view.era.0,
                        view = header.view.view.0,
                        slot = header.slot.0,
                        tag = header.tag.name(),
                        bytes = size,
                    );
                }
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
                    // Invariant (asserted, always): exactly-once reply
                    // correlation — a Service executes exactly once per
                    // message_id; the cached path above replays without
                    // re-executing. If the cache were populated for this id
                    // between the check and here, the duplicate would have
                    // taken the cached path and this branch is a bug.
                    assert!(
                        !self.replies.contains_key(&message_id),
                        "exactly-once: a cached reply must never re-execute the Service \
                         (message_id={message_id:?})"
                    );
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
                // only (never on cached duplicate replay). A blocking-journal
                // error disables journaling for the process; the node keeps
                // serving. The AOF sink enqueues and never fails: overflow
                // drops into the writer's drop counter.
                if let Some(transition) = transition {
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
                        Transition::Break {
                            lock_id,
                            lease_id,
                            holder,
                            expiry,
                        } => JournalEvent {
                            kind: journal::KIND_BREAK,
                            ts,
                            lock_id: *lock_id,
                            lease_id: *lease_id,
                            holder: *holder,
                            expiry: *expiry,
                        },
                    };
                    #[cfg(feature = "flight-recorder")]
                    self.flight_log(
                        "journal",
                        serde_json::json!({
                            "what": "internal lock-state flush",
                            "kind": event.kind,
                            "ts": event.ts,
                            "lock_id": event.lock_id,
                            "lease_id": event.lease_id,
                            "holder_hex": crate::flight::hex(&event.holder),
                            "expiry": event.expiry,
                        }),
                    );
                    let mut sink = sink_guard(&self.sink);
                    match sink.as_mut() {
                        Some(JournalSink::Blocking(journal)) => {
                            if let Err(e) = journal.append(&event) {
                                eprintln!(
                                    "lunet-advisory-lock: journal append failed ({e}); \
                                     journaling disabled for this process"
                                );
                                *sink = None;
                            }
                        }
                        Some(JournalSink::Aof(writer)) => writer.enqueue(event),
                        None => {}
                    }
                    drop(sink);
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
                pending.push_back(TimedInput {
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
            Effect::AdminResponse { .. } => {
                // Unreachable: the host never submits plans over the admin
                // ingress, so no verdict can come back; treated as an
                // invariant breach.
                Err(FAULTED)
            }
        }
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

/// One queued output for an embedded host (`Node::next_output`): a unicast
/// peer datagram (`kind == 1`, deliver `bytes` to member id `to` over the
/// host's own peer transport) or a client reply (`kind == 2`, `bytes`
/// answers the request carrying `message_id`).
pub struct NodeOutput {
    pub kind: u32,
    pub to: u32,
    pub era: u32,
    pub view: u32,
    pub slot: u64,
    pub message_id: [u8; 16],
    pub bytes: Vec<u8>,
}

/// One status snapshot for an embedded host (`Node::status`): the
/// replication state (`0` normal, `1` view_change, `2` recovering,
/// `3` replaying), the current view's primary as a member id (`u32::MAX`
/// when unknown or void), the current view's era and view, the folded
/// configuration table's current era — the two eras differ exactly while a
/// committed reconfiguration's establishing era awaits the view that
/// enters it — whether the node has self-arrested, and the recorded
/// reason when it has.
#[derive(Debug)]
pub struct NodeStatus {
    pub state: u32,
    pub leader: u32,
    pub era: u32,
    pub view: u32,
    pub config_era: u32,
    /// Whether the node has self-arrested (the core's never-repair
    /// fault, or a boundary panic): every further entry reports SERVICE.
    pub poisoned: bool,
    /// The self-arrest's recorded reason, when the arrest named one.
    pub fault_note: Option<String>,
}

/// The replication state word's name, for every surface a human reads (a
/// log line, a status note, a trace): `state=joining`, never `state=4`.
/// The word and every comparison stay numeric; the name comes from the
/// core's own const table (`vrr::progress::Status::name` — the snapshot
/// word's numbering and its names are stated together there), and a word
/// no status encodes renders `invalid`, never a bare integer.
pub fn replication_state_name(word: u32) -> &'static str {
    match vrr::progress::Status::from_word(word) {
        Some(status) => status.name(),
        None => "invalid",
    }
}

impl NodeStatus {
    /// The status's replication state spelled for humans (`state=normal`,
    /// never `state=0`).
    pub fn state_name(&self) -> &'static str {
        replication_state_name(self.state)
    }
}

impl Node {
    /// Safe constructor over the same grammar the C ABI takes: `members` is
    /// the NUL-separated `<u32-id>:<name>` member buffer in descriptor
    /// (genesis succession) order, post-genesis entries suffixed `:j`;
    /// `own` is the local member's name; `state` is the durable
    /// incarnation-marker path; `journal_dir` enables the lock-event
    /// journal (an empty string disables it, matching the ABI's
    /// empty-buffer rule) with `roll_bytes` as its roll threshold.
    pub fn open(
        members: &str,
        own: &str,
        state: &str,
        journal_dir: Option<&str>,
        roll_bytes: u32,
        primary_timeout: u64,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            node_from_parts(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                journal_dir.filter(|dir| !dir.is_empty()),
                roll_bytes,
                primary_timeout,
                std_disk(),
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// The standby telemetry variant: the committed-transition hook enqueues
    /// to the AOF write-behind writer instead of the blocking journal. Same
    /// member/own/state grammar as [`Node::open`]; `aof_dir` is the AOF
    /// series directory (created or resumed); `flush_ms` is the periodic
    /// fsync knob (`None` fsyncs only at roll and shutdown). The roll
    /// threshold is fixed at the 2 MiB erasure block.
    ///
    /// The C ABI never takes this path; the ABI's journal surface is
    /// unchanged.
    pub fn open_aof(
        members: &str,
        own: &str,
        state: &str,
        aof_dir: &str,
        flush_ms: Option<u64>,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            node_from_aof(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                aof_dir,
                flush_ms.map(Duration::from_millis),
                std_disk(),
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// The E2 experiment variant: same grammar as [`Node::open`], plus the
    /// recovery-boundary flush variant and its scratch directory. At the
    /// dirty-boot classification point the variant's forced flush executes
    /// against `scratch_dir` (variant 0 — [`RecoveryFlush::Diskless`] —
    /// writes nothing); the measured latency lands in the node's log as a
    /// `recovery-boundary flush executed` event. The C ABI never takes this
    /// path: the ABI's boot stays variant 0.
    pub fn open_with_recovery_flush(
        members: &str,
        own: &str,
        state: &str,
        journal_dir: Option<&str>,
        roll_bytes: u32,
        variant: RecoveryFlush,
        scratch_dir: &str,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            let disk = std_disk();
            let journal = open_journal(
                Arc::clone(&disk),
                journal_dir.filter(|dir| !dir.is_empty()),
                roll_bytes,
            );
            node_from_sink(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                journal,
                Some((variant, PathBuf::from(scratch_dir))),
                None,
                Construction {
                    primary_timeout: PRIMARY_TIMEOUT_MS,
                },
                disk,
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// The bench-harness variant (`docs/src/bench-harness.md`, unix
    /// only): the lifecycle marker store rides the harness driver's
    /// control socket — every boot-read, commit, and drain is an RPC the
    /// driver answers from memory under the scenario's signalled
    /// discipline — while the blocking journal stays real (it is the
    /// run's CAS-chain evidence). The C ABI never takes this path.
    #[cfg(unix)]
    pub fn open_bench(
        members: &str,
        own: &str,
        journal_dir: Option<&str>,
        roll_bytes: u32,
        store_ctl: &str,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            let disk = std_disk();
            let journal = open_journal(Arc::clone(&disk), journal_dir, roll_bytes);
            node_from_sink(
                members.as_bytes(),
                own.as_bytes(),
                // The marker store is the driver's; the state path is
                // never touched on this path. The grammar still wants a
                // non-empty value.
                b"bench",
                journal,
                None,
                Some(store_ctl),
                Construction {
                    primary_timeout: PRIMARY_TIMEOUT_MS,
                },
                disk,
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// Propose a client request (the lock-verb JSON). `0` on acceptance —
    /// the correlated reply arrives later as a kind-2 `NodeOutput` keyed by
    /// the request's message_id; the ABI's negative codes otherwise.
    pub fn request(&mut self, json: &[u8]) -> i32 {
        trace!(len = json.len(), "node request entry");
        #[cfg(feature = "flight-recorder")]
        self.flight_bytes_in("request-in", 0, json);
        if self.stopped {
            // The drain point closed the wire: no further task processing.
            return STOPPED;
        }
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
        if let Some(cached) = self.replies.get(&message_id) {
            self.outputs.push_back(Queued {
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
        self.pending.insert(id, message_id);
        let result = self.drive(Input::Propose {
            operation: Operation {
                id,
                payload: json.to_vec().into_boxed_slice(),
            },
        });
        if result != OK {
            self.pending.remove(&id);
        }
        result
    }

    /// Deliver one peer datagram payload attributed to member id `from`
    /// (the host has already authenticated the source endpoint).
    pub fn receive(&mut self, from: u32, data: &[u8]) -> i32 {
        trace!(from, len = data.len(), "node receive entry");
        #[cfg(feature = "flight-recorder")]
        self.flight_bytes_in("receive-in", from, data);
        if self.stopped {
            // The drain point closed the wire: no further inbound reads.
            return STOPPED;
        }
        if data.len() > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        // A message attributed to an id outside the descriptor's
        // address space whose system half names no descriptor member is
        // a maybe (an unknown peer id). A later life of a member
        // (the crash counter advanced under the same system half — the
        // identity law's packing) is the reincarnation story's
        // legitimate caller and exempt.
        if !self.known_ids.contains(&from)
            && !self
                .known_ids
                .iter()
                .any(|known| (known >> SYSTEM_HALF_SHIFT) == (from >> SYSTEM_HALF_SHIFT))
        {
            #[cfg(feature = "flight-recorder")]
            self.flight_log(
                "maybe",
                serde_json::json!({
                    "where": "receive",
                    "what": "message from an unknown peer id",
                    "from": from,
                    "len": data.len(),
                }),
            );
            maybe_invariant!(
                "message from an unknown peer id (from={from}, len={})",
                data.len()
            );
        }
        // W5: `Incomplete` means "more bytes could make this a message" and
        // `Malformed` means none could; over datagram transport there is no
        // reassembly, so both are a bad datagram from this host's view.
        let message = match Message::unpack_from(data) {
            Ok(message) => message,
            Err(UnpackError::Incomplete { .. } | UnpackError::Malformed(_)) => {
                let snapshot = self.replica.observer().read();
                warn!(
                    ts = log_millis(),
                    event = "datagram-undecodable",
                    node = self.replica.own().0,
                    era = snapshot.era,
                    view = snapshot.view,
                    from,
                    len = data.len(),
                    "undecodable peer datagram discarded"
                );
                return VRR_MESSAGE;
            }
        };
        // The NOMINATE computation's inbound half, with the same ballot
        // discipline as the outbound half: a fence this node heard, the
        // evidence it holds, the history it was handed.
        if is_view_change_family(message.header.tag) {
            info!(
                ts = log_millis(),
                event = "nominate-in",
                node = self.replica.own().0,
                from,
                era = message.header.view.era.0,
                view = message.header.view.view.0,
                slot = message.header.slot.0,
                tag = message.header.tag.name(),
                len = data.len(),
            );
        }
        trace!(
            from,
            len = data.len(),
            era = message.header.view.era.0,
            view = message.header.view.view.0,
            slot = message.header.slot.0,
            tag = ?message.header.tag,
            "datagram in"
        );
        // The peer payload gate: the host re-checks every peer-carried
        // operation entry with `Service` before the message reaches the
        // core.
        if !valid_message_payloads(&message) {
            warn!(
                ts = log_millis(),
                event = "payload-gate-refused",
                node = self.replica.own().0,
                from,
                era = message.header.view.era.0,
                view = message.header.view.view.0,
                slot = message.header.slot.0,
                "peer-carried payload failed the Service gate; datagram discarded"
            );
            return VRR_MESSAGE;
        }
        self.drive(Input::Peer {
            from: NodeId(from),
            message,
        })
    }

    /// Heartbeat tick.
    pub fn idle(&mut self) -> i32 {
        trace!("node idle entry");
        if self.stopped {
            // The drain point closed the wire: no further task processing.
            return STOPPED;
        }
        self.drive(Input::Tick)
    }

    /// Election tick: same liveness input — tick-driven suspicion is the
    /// tag's only view-change trigger (`ViewChangeKnobs::primary_timeout`).
    pub fn leader_timeout(&mut self) -> i32 {
        trace!("node leader timeout entry");
        if self.stopped {
            // The drain point closed the wire: no further task processing.
            return STOPPED;
        }
        self.drive(Input::Tick)
    }

    /// Host-forced view change (§14.2): the leader timeout's
    /// conclusion that the primary is dead. `era` must be the node's
    /// current era and `view` must strictly advance the view number — the
    /// core refuses anything sideways or backwards.
    pub fn force_view(&mut self, era: u32, view: u32) -> i32 {
        trace!(era, view, "node force view entry");
        if self.stopped {
            // The drain point closed the wire: no further task processing.
            return STOPPED;
        }
        self.drive(Input::AdminForceView {
            target: Ballot {
                era: Era(era),
                view: View(view),
            },
        })
    }

    /// One timeout toggle's event capture (`docs/src/failure-detection.md`):
    /// the host's timeout plane records EVERY toggle of its
    /// `timedout` state — the new state, the toggle's local-clock ts, and
    /// the ts of the LAST toggle (kept in memory in the host's toggle) —
    /// in BOTH the regular log (the `info!` here) AND the Flight Recorder
    /// as one `timeout-toggle` event, alongside the other internal
    /// events. A no-op on the replication path: capture only.
    pub fn note_timeout_toggle(&mut self, timedout: bool, at_ms: u64, previous_ms: Option<u64>) {
        let snapshot = self.replica.observer().read();
        info!(
            ts = log_millis(),
            event = "timeout-toggle",
            node = self.replica.own().0,
            era = snapshot.era,
            view = snapshot.view,
            timedout,
            toggle_ts = at_ms,
            last_toggle = previous_ms.unwrap_or(at_ms),
            previous_known = previous_ms.is_some(),
            "timeout toggle"
        );
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "timeout-toggle",
            serde_json::json!({
                "timedout": timedout,
                "ts_ms": at_ms,
                "last_toggle_ms": previous_ms,
            }),
        );
    }

    /// One fenced-boot drive. The core has no recovery protocol: a fenced
    /// node starts clean, and the only protocol lever is `Input::Tick` —
    /// the genesis primary self-promotes on it, and the primary's messages
    /// adopt the fenced backups. A bumped node (a dirty restart) re-drives
    /// its `Input::Reincarnate { old }` announcement first, on every fenced
    /// drive, until it stops being fenced — upstream's §8 re-announce; the
    /// core self-gates the announcement (a member already voting at
    /// weight >= 1 has nothing to announce). The drive never fabricates
    /// recovered state.
    pub fn recover(&mut self) -> i32 {
        if self.poisoned {
            return SERVICE;
        }
        if self.stopped {
            // The drain point closed the wire: no further task processing.
            return STOPPED;
        }
        if let Some(old) = self.reincarnate_from {
            debug!(
                ts = log_millis(),
                event = "fenced-boot-reincarnate",
                node = self.replica.own().0,
                old = old.0,
                "fenced-boot drive: re-announcing the reincarnation"
            );
            let result = self.drive(Input::Reincarnate { old });
            if result != OK {
                return result;
            }
        }
        debug!(
            ts = log_millis(),
            event = "fenced-boot-tick",
            node = self.replica.own().0,
            "fenced-boot drive: tick"
        );
        self.drive(Input::Tick)
    }

    /// Drive a reconfiguration operation on this node: `op` is one of the
    /// `RECONFIGURE_*` codes, `member` the target member id, `position` the
    /// join succession position (`POSITION_APPEND` appends at the current
    /// succession end).
    pub fn reconfigure(&mut self, op: u32, member: u32, position: u32) -> i32 {
        if self.stopped {
            // The drain point closed the wire: no further task processing.
            return STOPPED;
        }
        let position = if op == RECONFIGURE_JOIN && position == POSITION_APPEND {
            let len = self
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
            RECONFIGURE_DECREMENT => SystemOperation::Decrement(NodeId(member)),
            RECONFIGURE_LEAVE => SystemOperation::Leave(NodeId(member)),
            _ => return INVALID,
        };
        // The deployment ruling (docs/src/decisions.md): a cluster never
        // drops below three VOTING members. A departure (Leave) or a
        // demotion (Decrement) that would is refused before it drives.
        if matches!(op, RECONFIGURE_DECREMENT | RECONFIGURE_LEAVE)
            && let Some((ids, weights)) = self.membership()
        {
            let voting = weights.iter().filter(|weight| **weight > 0).count();
            let member_voting = ids
                .iter()
                .zip(&weights)
                .any(|(id, weight)| id.0 == member && *weight > 0);
            if member_voting && voting <= 3 {
                return CONFIG;
            }
        }
        // The host's pivot policy (W5 is sizing; this is its transition
        // sibling): the non-stop overlap path is probed only for the
        // weight-moves it has been proven through. A membership change
        // — Join or Leave — drives the stop-the-world fallback with
        // `pivot: None`, a latency outcome, never an error: the
        // establishing era then completes through the ordinary fence the
        // stop-the-world exemption arms (the same proven path every
        // stop-the-world transition takes). The probed non-stop machine
        // for a weight-0 departure is the one liveness hole the
        // upstream-issue draft records: its solicited evidence races the
        // proposer's own establishing commit, the answer that arrives
        // first is dropped UnevaluableEra (the proposer's table has not
        // folded the establishing era yet), the one-shot solicitation
        // never re-fires, and with the machine armed the
        // stop-the-world exemption does not gate the primary's baseline —
        // the stream keeps the fence from arming and the transition sits
        // half-committed forever. The stop-the-world path has no such
        // window.
        let pivot = match op {
            RECONFIGURE_JOIN | RECONFIGURE_LEAVE => None,
            _ => self.derived_pivot(&system),
        };
        self.drive(Input::Reconfigure { op: system, pivot })
    }

    /// The graceful stop (the engine's halt schedule, driven through
    /// the Running typestate):
    ///
    /// 1. **The wire closes first** — the mandatory drain point, and the
    ///    host obligation the engine assumes: the `stopped` flag is set
    ///    BEFORE any marker write, so every further inbound entry
    ///    (`request`, `receive`, ticks, admin drives) refuses and
    ///    processes nothing: the in-memory state becomes final and
    ///    nothing arriving later can contradict it. The caller may still
    ///    drain already-queued outputs (outbound flush is the desirable
    ///    obligation and must never delay this one).
    /// 2. **`begin_stop`** — the engine writes the halt's first round
    ///    (`Stopping`, 4x): termination has begun, it vouches for
    ///    nothing.
    /// 3. **The drain** — the host's, through the engine's schedule: the
    ///    committed-transition sink drains to quiescence (the AOF
    ///    writer's queue appended and fsynced; the blocking journal
    ///    fsynced), strictly between the two rounds.
    /// 4. **`finish_stop`** — the engine writes the drain-proven second
    ///    round (`Stopped`, 4x): the next boot's stopped quorum proves
    ///    the clean stop.
    ///
    /// A node still inside the crashed path's unseated window (its
    /// engine session not yet latched) has no Running session: the stop
    /// closes the wire and drains the sink, and no marker round is
    /// lawful — the markers hold the emission gate's round (the new
    /// life's running sentinel), the next boot re-classifies crashed
    /// and derives the strictly next life.
    ///
    /// Idempotent: a second stop reports OK without writing anything. A
    /// failed round or drain reports SERVICE and the markers stand at
    /// the last completed transition (a death past the first round reads
    /// crashed — the purge's law: nothing vouches before the drain).
    /// SIGKILL takes none of this path: the running sentinel stays
    /// behind and the next boot classifies crashed.
    ///
    /// Every path below names itself on the lifecycle census tape
    /// ([`trace_line!`]): the idempotent re-entry, the drain point, both
    /// marker rounds, the drain window opening and closing, the unseated
    /// window's drain, and every refusal arm.
    pub fn stop(&mut self) -> i32 {
        if self.stopped {
            trace_line!("stop.idempotent");
            return OK;
        }
        // The drain point: the wire closes BEFORE any marker write.
        self.stopped = true;
        trace_line!("stop.wire-closed");
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "stop",
            serde_json::json!({
                "path": "stop.wire-closed",
                "node": self.replica.own().0,
            }),
        );
        let ballot = self.replica.observer().read();
        info!(
            ts = log_millis(),
            event = "stop-wire-closed",
            node = self.replica.own().0,
            era = ballot.era,
            view = ballot.view,
            "stop: the wire is closed, the in-memory state is final"
        );
        let Some(session) = self.session.take() else {
            // The deferred window: no latched identity, no marker round.
            // The host still owes the durable sink drain.
            trace_line!("stop.unseated");
            #[cfg(feature = "flight-recorder")]
            self.flight_log(
                "stop",
                serde_json::json!({
                    "path": "stop.unseated",
                    "node": self.replica.own().0,
                }),
            );
            let drained = drain_sink(&mut sink_guard(&self.sink));
            if let Err(error) = drained {
                trace_line!("stop.refuse.unseated-drain");
                eprintln!(
                    "lunet-advisory-lock: the stop drain failed ({error}); \
                           the markers hold the crash's evidence; {UNFLUSHED_RUNBOOK}"
                );
                #[cfg(feature = "flight-recorder")]
                self.flight_log(
                    "stop-drain",
                    serde_json::json!({ "what": "the stop drain failed", "error": error.to_string() }),
                );
                return SERVICE;
            }
            info!(
                ts = log_millis(),
                event = "stop-unseated-drain",
                node = self.replica.own().0,
                era = ballot.era,
                view = ballot.view,
                "stop: the unseated window drains and exits; the next boot derives the next life"
            );
            return OK;
        };
        trace_line!("stop.round.begin");
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "marker",
            serde_json::json!({
                "what": "the halt's first round (Stopping) begins",
                "identity": session.identity().0,
                "state": lunet_locks_aof::marker::MarkerState::Stopped.code(),
            }),
        );
        #[cfg(feature = "flight-recorder")]
        let identity = session.identity();
        let halting = match session.begin_stop() {
            Ok(halting) => halting,
            Err((_, error)) => {
                trace_line!("stop.refuse.first-round");
                eprintln!(
                    "lunet-advisory-lock: the stop's first marker round failed ({error:?}); \
                     {UNFLUSHED_RUNBOOK}"
                );
                return SERVICE;
            }
        };
        // The drain window's own durable write: the view record — the
        // ballot the node held — strictly between the two rounds, so
        // the next clean boot resumes at the view it stopped at. A
        // failed write leaves the markers at the first round (the next
        // boot reads crashed) and reports SERVICE, the same shape as a
        // failed round.
        let snapshot = self.replica.observer().read();
        let record = ViewRecord {
            era: snapshot.era,
            view: snapshot.view,
        };
        if let Err(error) = write_view_record(&*self.disk, &self.state_path, &record) {
            trace_line!("stop.refuse.drain-window");
            eprintln!(
                "lunet-advisory-lock: the stop's view-record write failed ({error}); \
                        the markers hold the halt's first round; {UNFLUSHED_RUNBOOK}"
            );
            return SERVICE;
        }
        trace_line!("stop.drain-window.open");
        let draining = match halting.drain() {
            Ok(draining) => draining,
            Err((_, error)) => {
                trace_line!("stop.refuse.drain");
                eprintln!(
                    "lunet-advisory-lock: the stop drain failed ({error}); \
                        the marker stays at the first round; {UNFLUSHED_RUNBOOK}"
                );
                #[cfg(feature = "flight-recorder")]
                self.flight_log(
                    "stop-drain",
                    serde_json::json!({ "what": "the stop drain failed", "error": error.to_string() }),
                );
                return SERVICE;
            }
        };
        trace_line!("stop.drain-window.close");
        trace_line!("stop.round.finish");
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "marker",
            serde_json::json!({
                "what": "the drain-proven second round (Stopped) begins",
                "identity": identity.0,
                "state": lunet_locks_aof::marker::MarkerState::Flushed.code(),
            }),
        );
        if let Err((_, error)) = draining.finish_stop() {
            trace_line!("stop.refuse.second-round");
            eprintln!(
                "lunet-advisory-lock: the stop's second marker round failed ({error:?}); \
                 {UNFLUSHED_RUNBOOK}"
            );
            return SERVICE;
        }
        trace_line!("stop.complete");
        info!(
            ts = log_millis(),
            event = "stop-drained",
            node = self.replica.own().0,
            era = snapshot.era,
            view = snapshot.view,
            state = %self.state_path.display(),
            "stop: drained and the drain proven; the next boot continues under the same identity"
        );
        OK
    }

    /// The live identity: the descriptor id at incarnation 0, the bumped
    /// high-band id after a dirty restart.
    pub fn own_id(&self) -> u32 {
        self.replica.own().0
    }

    /// The status snapshot (see `NodeStatus`).
    pub fn status(&self) -> NodeStatus {
        let snapshot = self.replica.observer().read();
        NodeStatus {
            state: snapshot.status,
            leader: self.primary_index(),
            era: snapshot.era,
            view: snapshot.view,
            config_era: self.replica.progress().config().current().era.0,
            poisoned: self.poisoned,
            fault_note: self.fault_note.clone(),
        }
    }

    /// This node's voting weight in the current folded configuration
    /// (`None` when it is not a member of it): the E1 runner's
    /// "voting and serving" signal — a reincarnated node is walked back to
    /// weight 1 by the leader's forced reconfiguration sequence, and the
    /// host reports the weight in its status notes.
    pub fn voting_weight(&self) -> Option<u32> {
        let current = self.replica.progress().config().current();
        current
            .config
            .weight_of(self.replica.own())
            .map(|weight| weight.0)
    }

    /// Pop the next queued output, if any.
    pub fn next_output(&mut self) -> Option<NodeOutput> {
        let queued = self.outputs.pop_front()?;
        // Invariant (asserted, always): the output queue carries only the
        // send (1) and reply (2) kinds — any other kind is an adapter bug
        // the host cannot interpret.
        assert!(
            queued.kind == OUTPUT_SEND || queued.kind == OUTPUT_REPLY,
            "output queue carries an unknown kind {}",
            queued.kind
        );
        trace!(
            kind = queued.kind,
            to = queued.to,
            era = queued.era,
            view = queued.view,
            slot = queued.slot,
            len = queued.bytes.len(),
            "datagram out"
        );
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "emit",
            serde_json::json!({
                "kind": queued.kind,
                "to": queued.to,
                "era": queued.era,
                "view": queued.view,
                "slot": queued.slot,
                "len": queued.bytes.len(),
                "hex": crate::flight::hex(&queued.bytes),
            }),
        );
        Some(NodeOutput {
            kind: queued.kind,
            to: queued.to,
            era: queued.era,
            view: queued.view,
            slot: queued.slot,
            message_id: queued.message_id,
            bytes: queued.bytes,
        })
    }
}

impl Node {
    /// One opaque proposal: the payload is raw bytes the core carries
    /// opaque (B2) — no Service decode, no reply correlation.
    ///
    /// # Panics
    ///
    /// Never deliberately; a drive's boundary panic poisons the node and
    /// reports `PANIC` through the usual path.
    pub fn propose_opaque(&mut self, id: OperationId, payload: &[u8]) -> i32 {
        self.drive(Input::Propose {
            operation: Operation {
                id,
                payload: payload.to_vec().into_boxed_slice(),
            },
        })
    }

    /// One typed cluster operation over the ordinary consensus pipeline,
    /// at the reference host's pivot policy: `pivot: None`, the
    /// stop-the-world fallback — a latency outcome, never an error.
    pub fn reconfigure_opaque(&mut self, op: SystemOperation) -> i32 {
        self.drive(Input::Reconfigure { op, pivot: None })
    }

    /// The published frontiers: `(accepted, committed, applied)` as the
    /// observation carries them.
    #[must_use]
    pub fn frontiers(&self) -> (u64, u64, u64) {
        let snapshot = self.replica.observer().read();
        (snapshot.accepted, snapshot.committed, snapshot.applied)
    }

    /// The folded configuration's current record: the succession order
    /// with each member's weight in the same order. `None` when the
    /// node holds no era record (never in a provisioned cluster).
    #[must_use]
    pub fn membership(&self) -> Option<(Vec<NodeId>, Vec<u64>)> {
        let current = self.replica.progress().config().current();
        let order = current.config.order();
        Some((
            order.iter().map(|member| member.node).collect(),
            order
                .iter()
                .map(|member| u64::from(member.weight.0))
                .collect(),
        ))
    }

    /// The gossip-witness list, in list order.
    #[must_use]
    pub fn witnesses(&self) -> Vec<NodeId> {
        self.replica.witnesses().to_vec()
    }
}

/// Whether the folded configuration era moved backwards across two
/// observations. The folded era advances exactly at a committed
/// reconfiguration's establishing operation; it never regresses. A
/// regression is a maybe (not provably impossible for a host juggling era
/// snapshots, and survivable), not an assert.
fn folded_era_regressed(previous: Option<u32>, current: u32) -> bool {
    matches!(previous, Some(previous) if current < previous)
}

/// Map a plan refusal onto the ABI error codes. `NotPrimary` is the one a
/// caller can act on (re-forward to the named primary); the rest —
/// the fault, the reconfiguration gates, the outstanding-transition
/// bookkeeping — are internal states the host cannot repair in place.
fn plan_error(rejection: PlanRefusal) -> i32 {
    debug!(?rejection, "plan refused");
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

/// The descriptor's provisioned-identity law: the id is the packed pair
/// (system, crash counter 1) — a lawful identity whose system half is
/// the sysadmin-assigned system identifier and whose crash half names
/// the genesis life. The marker's crash counter carries the life from
/// there.
fn provisioned_identity(id: u32) -> bool {
    NodeId::from(id).system_id().is_some() && (id & 0xffff) == 1
}

/// Reconfiguration operation codes for `lunet_lock_node_reconfigure`.
/// The departure route is `Decrement` to weight 0, then `Leave` (the core's
/// one departure route); a weight-0 member cannot be decremented further.
pub const RECONFIGURE_JOIN: u32 = 1;
pub const RECONFIGURE_INCREMENT: u32 = 2;
pub const RECONFIGURE_LEAVE: u32 = 3;
pub const RECONFIGURE_DECREMENT: u32 = 4;

/// `lunet_lock_node_reconfigure`'s Join position sentinel: append at the
/// core's current succession end (resolved against the folded configuration).
pub const POSITION_APPEND: u32 = u32::MAX;

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

/// The later-life knowledge every non-first constructor reopens over (see
/// the module's Identity note): the deployment's genesis, and nothing
/// else. The journal and the era table hold exactly the entries
/// `provision` installs — mirrored byte-for-byte so the joiner's slot-2
/// entry equals the cluster's committed one — and the constructors
/// (`join`, `resume`, `reincarnate`) fence the node to `Restarting`
/// regardless. Nothing about a restart is pretended: the node holds the
/// shared committed root and nothing else. The persisted record carries
/// the genesis frontiers; the constructor reconstitutes the boot status
/// (`Restarting`), so the record's own status is the Joining ruling's
/// spelling and is overridden.
fn joiner_parts(
    genesis_order: Vec<NodeId>,
) -> Result<(SegmentedLog, PersistedProgress, Arc<EraTable>), i32> {
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
    let view = Ballot {
        era,
        view: View::INITIAL,
    };
    let persisted = PersistedProgress {
        current: view,
        retained: view,
        status: Status::Joining,
        accepted: INIT_SLOT,
        committed: INIT_SLOT,
        applied: INIT_SLOT,
        checkpoint: Slot::NONE,
        revision: 0,
        fault: None,
    };
    Ok((journal, persisted, Arc::new(table)))
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

/// The marker file's on-disk line: `<system> <crash>
/// <unflushed|stopped|flushed>` — the identity pair spelled as its halves
/// (the projection's spelling of the engine marker,
/// `marker_store::projection_word`).
fn marker_line(system: u16, crash: u16, marker: Marker) -> String {
    format!(
        "{system} {crash} {}\n",
        crate::marker_store::projection_word(marker)
    )
}

/// The clean restart's view record: the view ballot the stop contract
/// writes into its drain window, strictly between the halt's two marker
/// rounds. The clean classification reads it back; the node resumes at
/// the view it stopped at instead of re-fencing at the genesis view —
/// a stale Prepare from below its own view then drops by name. The journal is not carried (the
/// adapter's durable shape: a reopened node holds the deployment's
/// genesis and catches up through the stream — its frontiers stay the
/// genesis ones the journal covers), so only the ballot is restored:
/// the one field the stream cannot hand back before the first answer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ViewRecord {
    pub era: u32,
    pub view: u32,
}

impl ViewRecord {
    /// The record's on-disk line: the two fields, space-separated.
    fn line(&self) -> String {
        format!("{} {}\n", self.era, self.view)
    }

    /// Parses the record's line. Anything else is unreadable.
    pub(crate) fn parse(text: &str) -> Option<ViewRecord> {
        let line = text.trim();
        let mut fields = line.split(' ');
        let era = fields.next()?.parse().ok()?;
        let view = fields.next()?.parse().ok()?;
        if fields.next().is_some() {
            return None;
        }
        Some(ViewRecord { era, view })
    }
}

/// The view record's path for a state path.
fn view_record_path(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".view");
    PathBuf::from(os)
}

/// Writes the view record durably (fsync+rename+dir-sync, the marker
/// projection's crash-consistency idiom).
pub(crate) fn write_view_record(
    disk: &dyn Disk,
    path: &Path,
    record: &ViewRecord,
) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut temporary = OsString::from(".");
    temporary.push(base);
    temporary.push(format!(".view-tmp-{}-{unique}", std::process::id()));
    let temporary = parent.join(temporary);
    let result = (|| {
        let mut file = disk.create_new_write(&temporary)?;
        file.write_all(record.line().as_bytes())?;
        file.sync_all()?;
        disk.rename(&temporary, &view_record_path(path))?;
        sync_parent(disk, path)
    })();
    if result.is_err() {
        let _ = disk.remove_file(&temporary);
    }
    result
}

/// Reads the view record. `None` when the file does not exist (no stop
/// ever wrote one); any existing-but-unreadable record is an error.
///
/// The single-file projection's reader, over the default disk: its only
/// caller is the marker store, whose own disk path rides the vendored
/// store's own boundary, so no disk is threaded in here.
pub(crate) fn read_view_record(path: &Path) -> std::io::Result<Option<ViewRecord>> {
    let path = view_record_path(path);
    if !StdDisk.exists(&path) {
        return Ok(None);
    }
    let text = StdDisk.read_to_string(&path)?;
    ViewRecord::parse(&text).map(Some).ok_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid view record: the line is not two fields",
        )
    })
}

/// Parses the marker line. Anything else is an unreadable marker: the boot
/// refuses rather than guessing an identity. The file's words are the
/// identity pair's halves (a zero half is refused — no identity) and the
/// engine's markers: `unflushed` is the running sentinel (`Joining` — an
/// operating or freshly-latched process), `stopped` is the halt's first
/// round (`Stopping` — it vouches for nothing), `flushed` is the
/// drain-proven second round (`Stopped`).
pub(crate) fn parse_marker(text: &str) -> Option<(u16, u16, Marker)> {
    let line = text.trim();
    let (system_text, rest) = line.split_once(' ')?;
    let (crash_text, marker_text) = rest.split_once(' ')?;
    let system = system_text.parse::<u16>().ok()?;
    let crash = crash_text.parse::<u16>().ok()?;
    if system == 0 || crash == 0 {
        return None;
    }
    let marker = match marker_text {
        "flushed" => Marker::Stopped,
        "stopped" => Marker::Stopping,
        "unflushed" => Marker::Joining,
        _ => return None,
    };
    Some((system, crash, marker))
}

/// The marker's single-file projection, read over the default disk: the
/// marker store is this reader's only caller, and it rides the vendored
/// store's own boundary for its durable path.
pub(crate) fn read_marker(path: &Path) -> std::io::Result<(u16, u16, Marker)> {
    parse_marker(&StdDisk.read_to_string(path)?)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid marker"))
}

/// One durable marker write: fsync+rename+dir-sync (POSIX crash
/// consistency — persist the new directory entry, not just the file's
/// data; Windows no-ops the directory sync, see `sync_dir`).
///
/// The single-file write is the COMPATIBILITY PROJECTION — written only
/// after the authoritative quorum write succeeded (see `marker_store`),
/// never a classification input once the copies exist.
/// The marker's single-file projection write, over the default disk: the
/// marker store is this writer's only caller, and it rides the vendored
/// store's own boundary for its durable path.
pub(crate) fn write_marker(
    path: &Path,
    system: u16,
    crash: u16,
    marker: Marker,
) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut temporary = OsString::from(".");
    temporary.push(base);
    temporary.push(format!(".tmp-{}-{unique}", std::process::id()));
    let temporary = parent.join(temporary);
    let mut file = StdDisk.create_new_write(&temporary)?;
    let result = (|| {
        file.write_all(marker_line(system, crash, marker).as_bytes())?;
        file.sync_all()?;
        StdDisk.rename(&temporary, path)?;
        sync_parent(&StdDisk, path)
    })();
    if result.is_err() {
        let _ = StdDisk.remove_file(&temporary);
    }
    result
}

/// The boot gate's decision record: the session whose type fixes the
/// schedule, the identity the boot decided, and the tokens the replica
/// constructors require.
#[derive(Debug)]
struct BootDecision {
    /// The latched session (a First or Clean classification): the
    /// Running typestate the stop path drives. `None` on the crashed
    /// path, whose engine session is held for the seated witness's
    /// latch.
    session: Option<Running<GateStore>>,
    /// The crashed classification's engine session: the engine's
    /// typestate latches it only once the seated observation mints the
    /// witness (landing the same marker round the emission gate already
    /// made durable at boot). `None` on the latched paths.
    deferred: Option<Crashed<GateStore>>,
    /// The identity the boot decided — the packed pair: the genesis
    /// pair for a first boot, the quorum-resolved pair for a clean
    /// continue, the bumped pair for a crashed boot.
    identity: NodeId,
    /// The clean start's proof token (`Replica::resume` requires it).
    vouched: Option<Vouched>,
    /// The crashed classification's replacement pair
    /// (`Replica::reincarnate` requires it; the pair is the commitment
    /// the wire announcement carries).
    pair: Option<Bumped>,
    /// The E2 experiment variant's flush outcome at the recovery
    /// boundary. The C ABI's boot never configures a variant.
    flush: Option<FlushOutcome>,
    /// The clean classification's view record: the view the stop's
    /// drain-window write carried, the ballot the resume continues at.
    /// `None` on the other classifications (a first life and a
    /// reincarnation open at the genesis view) and when no stop ever
    /// wrote one (the genesis-view fallback).
    restored: Option<ViewRecord>,
}

/// The boot gate (`vrr::lifecycle::boot` over the superblock quorum
/// store): the engine reads the durable
/// markers, classifies the start, and hands back the session whose type
/// fixes the write schedule. The verdicts:
///
/// * No marker ever written — the FIRST life: the first latch anchors
///   the genesis life (crash counter 1), so a later crash reads as a
///   crash.
/// * A stopped quorum — the CLEAN start: the drain was proven. One
///   latch round (the running sentinel) and the same-identity resume
///   behind the `Vouched` token.
/// * No stopped quorum — a CRASH: the identity is dead. The replacement
///   pair is decided here and THE EMISSION GATE lands the bump's one
///   durable marker round — the next life at the running sentinel —
///   before the driver releases the first announcement, unconditional,
///   seated or not. The engine's session is held so its typestate can
///   latch the same round again once the seated observation mints the
///   witness (the same identity and state: idempotent on the marker).
/// * An old-format marker (INCOMPATIBLE) or any other unreadable
///   marker shape (rotted beyond the read quorum, a fork, any store
///   refusal) — the boot REFUSES. A host that cannot classify from
///   durable state is unsafe and must not start; old-format markers
///   are never converted; there is no projection fallback and no
///   guessed identity.
///
/// The crashed classification is the recovery boundary (the experiment
/// design's §4): when a variant is configured, its forced flush executes
/// right here — at the classification point, before the reincarnated
/// node rejoins serving — against the caller-provided scratch directory.
/// Variant 0 (diskless) writes nothing. The flush carries fake data only
/// and is never read back; its measured latency is returned so the boot
/// can report it. A flush failure refuses the boot: the boundary is
/// load-bearing for the measurement, so a failure here means the disk is
/// not usable.
///
/// The adapter's identity law rides the marker pair: the crash counter's
/// next life must stay spellable through the packing's sixteen-bit
/// counter half, so the boot refuses there exactly as the engine's
/// checked bump refuses at exhaustion.
///
/// Every path below names itself on the lifecycle census tape
/// ([`trace_line!`]), classification and refusal alike, and the proving
/// test asserts each name against a fixture that reaches it.
fn boot_gate(
    disk: &dyn Disk,
    store: GateStore,
    system: SystemId,
    recovery: Option<(&RecoveryFlush, &Path)>,
) -> Result<BootDecision, i32> {
    let refuse = |what: String| -> i32 {
        eprintln!(
            "lunet-advisory-lock: the boot gate refuses to start ({what}); \
             no identity is guessed from the durable state"
        );
        CONFIG
    };
    match lifecycle::boot(store) {
        Err((_, BootError::QuorumLost)) => {
            trace_line!("boot.refuse.quorum-lost");
            Err(refuse(
                "the marker set is torn beyond the quorum read".to_string(),
            ))
        }
        Err((_, BootError::Exhausted(identity))) => {
            trace_line!("boot.refuse.exhausted");
            Err(refuse(format!(
                "the identity {identity:?} cannot be bumped"
            )))
        }
        Err((_, BootError::Store(error))) => {
            trace_line!("boot.refuse.store");
            Err(refuse(format!(
                "the marker store refused the boot read: {error}"
            )))
        }
        Ok(BootOutcome::First(first)) => {
            trace_line!("boot.first");
            // The genesis pair: the descriptor's system half, the first
            // life's counter.
            let genesis = NodeId::new(
                system,
                CrashCounter::new(1).expect("the genesis life's counter is non-zero"),
            );
            let session = match first.latch(genesis) {
                Ok(session) => session,
                Err((_, error)) => {
                    trace_line!("boot.refuse.first-latch");
                    return Err(refuse(format!("the first latch write failed: {error}")));
                }
            };
            trace_line!("boot.first-latch");
            Ok(BootDecision {
                session: Some(session),
                deferred: None,
                identity: genesis,
                vouched: None,
                pair: None,
                flush: None,
                restored: None,
            })
        }
        Ok(BootOutcome::Clean(clean)) => {
            trace_line!("boot.clean");
            let identity = clean.identity();
            let restored = clean.store().view_record().map_err(|error| {
                trace_line!("boot.refuse.view-record");
                refuse(format!("the clean start's view record refused: {error}"))
            })?;
            if restored.is_some() {
                trace_line!("boot.restored-view");
            }
            let (session, vouched) = match clean.latch() {
                Ok(latched) => latched,
                Err((_, error)) => {
                    trace_line!("boot.refuse.clean-latch");
                    return Err(refuse(format!("the clean latch write failed: {error}")));
                }
            };
            trace_line!("boot.clean-latch");
            Ok(BootDecision {
                session: Some(session),
                deferred: None,
                identity,
                vouched: Some(vouched),
                pair: None,
                flush: None,
                restored,
            })
        }
        Ok(BootOutcome::Crashed(crashed)) => {
            trace_line!("boot.crashed");
            let pair = match crashed.pair() {
                Ok(pair) => pair,
                Err(refusal) => {
                    trace_line!("boot.refuse.pair");
                    return Err(refuse(format!("the replacement pair refused: {refusal:?}")));
                }
            };
            // THE EMISSION GATE: the crash bump's one durable marker
            // round — the next life at the running sentinel — completes
            // here, before the driver releases the first announcement,
            // unconditionally (seated or not). The argument is the
            // bumped pair itself. A failed round refuses the boot: the
            // node is never half-announced.
            crashed.store().emission_gate(pair.new).map_err(|error| {
                trace_line!("boot.refuse.emission-gate");
                refuse(format!("the crash bump's marker write failed: {error}"))
            })?;
            trace_line!("boot.emission-gate");
            let flush = match recovery {
                None | Some((RecoveryFlush::Diskless, _)) => None,
                Some((variant, scratch)) => Some(
                    recovery_flush::execute_on(disk, scratch, *variant, u64::from(pair.new.0))
                        .map_err(|error| {
                            trace_line!("boot.refuse.recovery-flush");
                            refuse(format!("the recovery-boundary flush failed: {error}"))
                        })?,
                ),
            };
            if flush.is_some() {
                trace_line!("boot.recovery-flush");
            }
            Ok(BootDecision {
                session: None,
                deferred: Some(crashed),
                identity: pair.new,
                vouched: None,
                pair: Some(pair),
                flush,
                restored: None,
            })
        }
    }
}

fn sync_parent(disk: &dyn Disk, path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    sync_dir(disk, parent)
}

// Fsyncing the containing directory after create/rename is a POSIX crash-
// consistency idiom (persist the new directory entry, not just the file's
// data). Windows has no equivalent: opening a directory fails with
// ERROR_ACCESS_DENIED (std does not set FILE_FLAG_BACKUP_SEMANTICS), and NTFS
// does not require or support an explicit directory fsync for this guarantee
// the way POSIX filesystems do. Other Rust crates with the same durability
// pattern (e.g. `atomicwrites`) no-op this step on Windows for the same
// reason; do the same here rather than fail every nonce write on Windows.
#[cfg(unix)]
fn sync_dir(disk: &dyn Disk, dir: &Path) -> std::io::Result<()> {
    disk.sync_path(dir)
}

#[cfg(windows)]
fn sync_dir(_disk: &dyn Disk, _dir: &Path) -> std::io::Result<()> {
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

/// The blocking journal's open, shared by every node-construction path
/// that takes one: a failure logs and disables journaling for the
/// process; the replication path is never poisoned by observability.
fn open_journal(
    disk: Arc<dyn Disk>,
    journal_dir: Option<&str>,
    roll_bytes: u32,
) -> Option<JournalSink> {
    journal_dir.and_then(|dir| {
        match LockJournal::open_on(Arc::clone(&disk), Path::new(dir), roll_bytes as u64) {
            Ok(j) => Some(JournalSink::Blocking(j)),
            Err(e) => {
                eprintln!(
                    "lunet-advisory-lock: journal open failed ({e}); \
                 journaling disabled for this process"
                );
                None
            }
        }
    })
}

/// The construction body shared by the C ABI's `lunet_lock_node_new` and
/// the embedded host's `Node::open`: parse the member buffer, classify the
/// boot from the durable incarnation marker, and build the replica. Each
/// failure returns its ABI code.
fn node_from_parts(
    members_data: &[u8],
    own_data: &[u8],
    state_data: &[u8],
    journal_dir: Option<&str>,
    roll_bytes: u32,
    primary_timeout: u64,
    disk: Arc<dyn Disk>,
) -> Result<Node, i32> {
    let journal = open_journal(Arc::clone(&disk), journal_dir, roll_bytes);
    node_from_sink(
        members_data,
        own_data,
        state_data,
        journal,
        None,
        None,
        Construction {
            primary_timeout,
        },
        disk,
    )
}

/// The AOF-backed variant: the committed-transition hook enqueues to the
/// async write-behind writer instead of the blocking journal. Same node
/// construction; the C ABI never takes this path.
fn node_from_aof(
    members_data: &[u8],
    own_data: &[u8],
    state_data: &[u8],
    aof_dir: &str,
    flush_interval: Option<Duration>,
    disk: Arc<dyn Disk>,
) -> Result<Node, i32> {
    let config = AofConfig {
        flush_interval,
        ..AofConfig::default()
    };
    let sink = match AofWriter::open_on(Arc::clone(&disk), Path::new(aof_dir), config) {
        Ok(writer) => Some(JournalSink::Aof(writer)),
        Err(e) => {
            eprintln!(
                "lunet-advisory-lock: aof open failed ({e}); \
                 telemetry disabled for this process"
            );
            None
        }
    };
    node_from_sink(
        members_data,
        own_data,
        state_data,
        sink,
        None,
        None,
        Construction {
            primary_timeout: PRIMARY_TIMEOUT_MS,
        },
        disk,
    )
}

// The parameter list IS the node construction grammar, one caller-supplied
// fact per parameter: the three descriptor buffers, the event sink, the
// recovery boundary, the bench control socket, the runtime policy, and the
// disk the whole construction crosses. It pays here because folding them
// into a carrier struct would rewrite five call sites' signatures — a
// change to the construction grammar's shape that has nothing to do with
// the disk seam, and the seam is the only thing this row may move.
#[allow(clippy::too_many_arguments)]
fn node_from_sink(
    members_data: &[u8],
    own_data: &[u8],
    state_data: &[u8],
    journal: Option<JournalSink>,
    recovery: Option<(RecoveryFlush, PathBuf)>,
    store_ctl: Option<&str>,
    construction: Construction,
    disk: Arc<dyn Disk>,
) -> Result<Node, i32> {
    // The descriptor's grammar refusals: one named path, every branch of
    // it — the grammar is a single shape, so a violation is a single
    // observation on the census tape.
    let refused_descriptor = || {
        trace_line!("boot.refuse.descriptor");
        CONFIG
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
        return Err(refused_descriptor());
    };
    let Ok(own) = std::str::from_utf8(own_data) else {
        return Err(refused_descriptor());
    };
    let Ok(state) = std::str::from_utf8(state_data) else {
        return Err(refused_descriptor());
    };
    if state.is_empty()
        || members.is_empty()
        || members.len() > MAX_MEMBERS as usize
        || members
            .iter()
            .any(|member| member.name.is_empty() || !provisioned_identity(member.id))
    {
        return Err(refused_descriptor());
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
        return Err(refused_descriptor());
    }
    // The admin-assigned identity law: the descriptor's genesis (plain)
    // ids in buffer order are both the live NodeIds of the founding
    // membership and the genesis succession sequence. Every id is the
    // member's PROVISIONED identity — the packed pair (system, crash
    // counter 1) — so the system half is what the descriptor assigns and
    // the marker's crash counter carries the life.
    let genesis_order: Vec<NodeId> = members
        .iter()
        .filter(|member| !member.joined)
        .map(|member| NodeId(member.id))
        .collect();
    let Some(own_member) = members.iter().find(|member| member.name == own) else {
        return Err(refused_descriptor());
    };
    let system = match SystemId::new((own_member.id >> SYSTEM_HALF_SHIFT) as u16) {
        Some(system) => system,
        None => return Err(refused_descriptor()),
    };
    let knobs = ViewChangeKnobs {
        primary_timeout: construction.primary_timeout,
        view_change_budget: EVIDENCE_BUDGET,
    };
    // The boot gate: the engine reads the durable markers, classifies
    // the start, and hands back the session whose type fixes the write
    // schedule (see the `marker_store` bridge note and `boot_gate`). The
    // crashed classification is the recovery boundary: a configured E2
    // variant's forced flush executes there, against the caller-provided
    // scratch directory. The bench harness substitutes the store itself:
    // `store_ctl` names the driver's control socket and every marker
    // call rides it (`docs/src/bench-harness.md`); the durable path is
    // unchanged when no control socket is given.
    let state_path = PathBuf::from(state);
    let sink: SinkDoor = Arc::new(Mutex::new(journal));
    // The boot gate's marker-round schedule: every machine commit and the
    // drain between the halt's rounds, in write order. The store owns the
    // record.
    let marker_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    #[cfg(unix)]
    let store = match store_ctl {
        Some(ctl) => {
            match GateStore::mem(Path::new(ctl), Arc::clone(&sink), Arc::clone(&marker_log)) {
                Ok(store) => store,
                Err(error) => {
                    trace_line!("boot.refuse.store-control");
                    eprintln!(
                        "lunet-advisory-lock: the bench store control socket is not \
                     reachable ({error}); the boot refuses"
                    );
                    return Err(CONFIG);
                }
            }
        }
        None => GateStore::new(
            &state_path,
            Arc::clone(&sink),
            system.get(),
            Arc::clone(&marker_log),
        ),
    };
    #[cfg(not(unix))]
    let store = {
        let _ = store_ctl;
        GateStore::new(
            &state_path,
            Arc::clone(&sink),
            system.get(),
            Arc::clone(&marker_log),
        )
    };
    let decision = boot_gate(
        &*disk,
        store,
        system,
        recovery
            .as_ref()
            .map(|(variant, dir)| (variant, dir.as_path())),
    )?;
    let own_id = decision.identity;
    // The life's number for the run's logs: the decided pair's crash
    // counter (the pair is lawful — the boot gate's decision is
    // asserted below).
    let counter = own_id.crash_counter().map_or(0, CrashCounter::get);
    let incarnation = u64::from(counter);
    if let Some(outcome) = decision.flush {
        info!(
            ts = log_millis(),
            event = "recovery-flush",
            variant = outcome.variant,
            bytes = outcome.bytes_written,
            latency_us = outcome.latency.as_micros() as u64,
            incarnation,
            "recovery-boundary flush executed"
        );
    }
    // The live identity is the pair the boot gate decided: the genesis
    // pair at the first life, the quorum-resolved pair on a clean
    // continue, the bumped pair on a crashed boot — whose emission gate
    // already landed the next life's round before this point.
    // The superseded identity on the CRASHED path: the bumped node
    // re-announces `Reincarnation(old, new)` on every fenced-boot drive
    // and, while it is below voting weight, on the host's §8 re-announce
    // cadence. The old identity is the one the node LAST OPERATED AS —
    // the marker pair the boot classified (the replacement pair's old
    // life), not the descriptor id: from the second bump on, the
    // descriptor id was already evicted by the previous life's forced
    // walk, so announcing it would name a non-member, the transport's
    // remap could never chain (the previous life's id is the row the
    // peers still attribute the socket to), and the leader-side
    // `from == new` gate would refuse the announcement forever. A clean
    // resume or a first life carries no pair and announces nothing: the
    // pair is the crashed path's commitment.
    let reincarnate_from = decision.pair.as_ref().map(|pair| pair.old);
    // Invariant (asserted, always): the announced identity is lawful and
    // names the descriptor's system half; on the crashed path it is the
    // replacement pair's new identity — the strict next life of the
    // superseded one.
    assert!(
        own_id.is_lawful()
            && own_id.system_id() == Some(system)
            && reincarnate_from.map_or(own_id.0 == own_member.id, |old| own_id
                == old.next_life().expect("a lawful life has a next")),
        "the announced identity does not follow the marker pair (system={}, life={counter})",
        system.get(),
    );
    if incarnation > 1 {
        info!(
            ts = log_millis(),
            event = "identity-restart",
            old = reincarnate_from.map_or(0, |old| old.0),
            new = own_id.0,
            incarnation,
            "restart: the identity is a later life of the same system"
        );
    }
    info!(
        ts = log_millis(),
        event = "node-provisioned",
        own = own_id.0,
        incarnation,
        boot = if own_member.joined {
            "joiner"
        } else {
            "genesis"
        },
        "node provisioned"
    );
    let known_ids = members
        .iter()
        .map(|member| member.id)
        .collect::<HashSet<_>>();
    // The constructor the classification chose. Every later life reopens
    // over the deployment's genesis — the honest Volatile shape (there
    // is no durable journal to carry forward) — fenced until the stream
    // proves currency; only the FIRST life of a founding member
    // provisions the genesis itself.
    // Every later life reopens over the deployment's genesis; the era
    // table that genesis folds cannot be built for a descriptor the
    // grammar already accepted.
    let refused_genesis = || {
        trace_line!("boot.refuse.genesis");
        CONFIG
    };
    let replica = if let Some(pair) = decision.pair {
        // The crashed classification: `Replica::reincarnate` behind the
        // engine's `Bumped` pair. The durable bump defers — the marker
        // machine latches the new identity only once the engine's seated
        // observation mints the witness.
        let (journal, persisted, config) =
            joiner_parts(genesis_order).map_err(|_| refused_genesis())?;
        trace_line!("boot.reincarnate");
        Replica::reincarnate(
            pair,
            own_id,
            WeightedMajority,
            journal,
            persisted,
            config,
            Stability::Volatile,
            knobs,
        )
    } else if let Some(vouched) = decision.vouched {
        // The clean classification: `Replica::resume` behind the
        // engine's `Vouched` token — the only same-identity constructor.
        // The view record the stop's drain window wrote restores the
        // stopped view; without one the node re-fences at the genesis
        // view (the record is absent only when no stop ever wrote one).
        // The restored era must be one the deployment's genesis era
        // table covers — the adapter carries no durable journal, so a
        // post-reconfiguration era is not reconstructible and the boot
        // refuses rather than resuming over a table that cannot name
        // the view.
        let (journal, mut persisted, config) =
            joiner_parts(genesis_order).map_err(|_| refused_genesis())?;
        if let Some(record) = decision.restored {
            if record.era != config.current().era.0 {
                trace_line!("boot.refuse.view-era");
                eprintln!(
                    "lunet-advisory-lock: the boot gate refuses to start \
                     (the clean start's view record names era {}, which the \
                     deployment's era table does not carry); no identity is \
                     guessed from the durable state",
                    record.era
                );
                return Err(CONFIG);
            }
            let ballot = Ballot {
                era: Era(record.era),
                view: View(record.view),
            };
            persisted.current = ballot;
            persisted.retained = ballot;
        }
        trace_line!("boot.resume");
        Replica::resume(
            vouched,
            own_id,
            WeightedMajority,
            journal,
            persisted,
            config,
            Stability::Volatile,
            knobs,
        )
    } else if own_member.joined {
        // A post-genesis member's first life: `Replica::join` over the
        // deployment's genesis, fenced until the stream proves currency.
        let (journal, persisted, config) =
            joiner_parts(genesis_order).map_err(|_| refused_genesis())?;
        trace_line!("boot.join");
        Replica::join(
            own_id,
            WeightedMajority,
            journal,
            persisted,
            config,
            Stability::Volatile,
            knobs,
        )
    } else {
        // The first life of a founding member: the genesis provision.
        trace_line!("boot.provision");
        Replica::provision(
            own_id,
            genesis_order,
            WeightedMajority,
            SegmentedLog::new(),
            Stability::Volatile,
            knobs,
        )
    }
    .map_err(|_| {
        trace_line!("boot.refuse.constructor");
        CONFIG
    })?;
    let node = Node {
        replica,
        outputs: VecDeque::new(),
        service: Service::default(),
        replies: HashMap::new(),
        pending: HashMap::new(),
        last_tick: 0,
        poisoned: false,
        fault_note: None,
        reincarnate_from,
        known_ids,
        last_view: None,
        last_leader: None,
        last_config_era: None,
        sink,
        state_path: state_path.clone(),
        disk: Arc::clone(&disk),
        session: decision.session,
        deferred: decision.deferred,
        stopped: false,
        #[cfg(feature = "flight-recorder")]
        flight: crate::flight::FlightRecorder::open_from_env_on(&disk, own_id.0),
    };
    // The bumped node's entry ticket (§4) rides the fenced-boot drive
    // (`recover`): the announcement is emitted on the host's first
    // fenced-boot drive, never at the boot itself — a reopened node
    // answers only what it is driven with, and the §8 re-announce
    // cadence carries the pair until the node seats. The boot gate's
    // classification is complete: the crash bump's durable round is
    // already landed (the emission gate), so the drive's announcement
    // vouches for a durable identity from the first wire emission.
    Ok(node)
}

/// The read-only information console: this build's own identity facts,
/// as the Maven `version.properties` text (the release tag naming the
/// build or `unknown`, the commit, the dirty flag, the feature shape),
/// NUL-terminated into `out_data` with its length (excluding the NUL)
/// in `out_len`. The pull-style contract of `lunet_lock_node_fault`:
/// when the text does not fit `capacity` the call reports TOO_LARGE and
/// writes the needed size.
///
/// The call takes NO node handle — that is the whole of its safety
/// argument. Nothing reachable from here reaches a [`Node`], so the
/// console is read-only by construction: there is no path through it
/// that writes protocol state or mutates the store. Every
/// value is stamped at build time, so the answer is the same on every
/// call and on every host that loaded this cdylib.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_version_properties(
    out_data: *mut u8,
    capacity: usize,
    out_len: *mut usize,
) -> i32 {
    guarded(|| {
        if out_len.is_null() {
            return INVALID;
        }
        let text = crate::info::properties();
        unsafe { *out_len = text.len() };
        if text.len() + 1 > capacity {
            return TOO_LARGE;
        }
        if !out_data.is_null() {
            unsafe {
                ptr::copy_nonoverlapping(text.as_ptr(), out_data, text.len());
                *out_data.add(text.len()) = 0;
            }
        }
        OK
    })
}

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
        let journal_dir = if journal_dir_data.is_empty() {
            None
        } else {
            std::str::from_utf8(journal_dir_data)
                .ok()
                .filter(|s| !s.is_empty())
        };
        match catch_unwind(AssertUnwindSafe(|| {
            node_from_parts(
                members_data,
                own_data,
                state_data,
                journal_dir,
                roll_bytes,
                PRIMARY_TIMEOUT_MS,
                std_disk(),
            )
        })) {
            Ok(Ok(node)) => {
                unsafe { *out = Box::into_raw(Box::new(node)).cast() };
                OK
            }
            Ok(Err(code)) => code,
            Err(_) => PANIC,
        }
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

/// The node's live identity: the descriptor id at incarnation 0, the
/// bumped high-band id after a dirty restart. The host compares leaders
/// against it (`status.leader == own_id`) — after a bump the descriptor
/// row still names the superseded id, so the live value is the only honest
/// self-check.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_own_id(node: *mut c_void, out_id: *mut u32) -> i32 {
    guarded(|| {
        if node.is_null() || out_id.is_null() {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        unsafe { *out_id = node.replica.own().0 };
        OK
    })
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
        node.request(json)
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
        node.reconfigure(op, member, position)
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
        node.receive(from, data)
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_idle(node: *mut c_void) -> i32 {
    // Heartbeat tick. The tag has a single liveness input, Input::Tick;
    // there is no separate idle input. The routed method carries the
    // drain point's STOPPED gate: a drive here must not bypass it.
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.idle())
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_leader_timeout(node: *mut c_void) -> i32 {
    // Election tick: same Input::Tick — tick-driven suspicion is the tag's
    // only view-change trigger (ViewChangeKnobs::primary_timeout). The
    // routed method carries the drain point's STOPPED gate: a drive here
    // must not bypass it.
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.leader_timeout())
    })
}

/// §14.2 host-forced view change: the leader timeout's conclusion
/// that the primary is dead (`era` the node's current era, `view` strictly
/// ahead). Returns [`OK`], [`SERVICE`] (poisoned), or [`CONFIG`]-class
/// refusals for a target that does not advance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_force_view(node: *mut c_void, era: u32, view: u32) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.force_view(era, view))
    })
}

#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_recover(node: *mut c_void) -> i32 {
    guarded(|| unsafe { node.cast::<Node>().as_mut().map_or(INVALID, Node::recover) })
}

/// The graceful stop (the uVRR termination obligations, host-side): the
/// wire closes BEFORE any marker write — every further inbound entry
/// reports STOPPED and processes nothing — then the `stopped` marker, the
/// sink drain to quiescence, and the `flushed` marker, in the contract's
/// §2 write order. Synchronous on the caller's thread: no thread spawn,
/// no callback, no yield — the stop-contract invariants. Idempotent.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_stop(node: *mut c_void) -> i32 {
    guarded(|| unsafe { node.cast::<Node>().as_mut().map_or(INVALID, Node::stop) })
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

/// The self-arrest report: a node that has self-arrested (the core's
/// never-repair fault, or a boundary panic) no longer serves — every
/// further entry reports SERVICE — and the runbook needs the reason
/// without restarting anything. Writes the recorded reason as a
/// NUL-terminated string into `out_data` and its length (excluding the
/// NUL) into `out_len`; a node that has not arrested reports OK with
/// `out_len` 0. When the note does not fit `capacity`, the call reports
/// TOO_LARGE and writes the needed size — the mirror of
/// `lunet_lock_node_next`'s contract. The call never drives the node:
/// observation only, valid on a poisoned node by design.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_fault(
    node: *mut c_void,
    out_data: *mut u8,
    capacity: usize,
    out_len: *mut usize,
) -> i32 {
    guarded(|| {
        if node.is_null() || out_len.is_null() {
            return INVALID;
        }
        let node = unsafe { &mut *node.cast::<Node>() };
        let Some(note) = node.fault_note.as_deref() else {
            unsafe { *out_len = 0 };
            return OK;
        };
        unsafe { *out_len = note.len() };
        if note.len() + 1 > capacity {
            return TOO_LARGE;
        }
        if !out_data.is_null() {
            unsafe {
                ptr::copy_nonoverlapping(note.as_ptr(), out_data, note.len());
                *out_data.add(note.len()) = 0;
            }
        }
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
