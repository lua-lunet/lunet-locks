//! Host-side FFI adapter between the LuaJIT host and the uVRR core
//! (uvrr-core tag v0.13.1 @ 37549d1 — the lifecycle boot gate; the core's constructors all sit in `node_from_sink` in this file: `node_from_sink` :2831, `lifecycle::boot` :2593, `Replica::reincarnate` :3070, `Replica::resume` :3113, `Replica::join` :3129, `Replica::provision` :3141).
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
use crate::journal::{self, Journal as LockJournal, JournalEvent};
use crate::locks::{Service, Transition};
use crate::recovery_flush::{self, FlushOutcome, RecoveryFlush};
#[cfg(any(test, debug_assertions))]
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsString, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
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
use vrr::journal::{Journal, JournalView, LogEntry, Payload, SegmentedLog};
use vrr::lifecycle::{self, BootError, BootOutcome, Bumped, Crashed, Marker, Running, Vouched};
use vrr::message::{Body, Message};
use vrr::observe::Diagnostic;
use vrr::progress::Status;
use vrr::quorum::{WeightedMajority, construct_pivot};
use vrr::replica::{
    Input, PersistedProgress, Pivot, PlanRefusal, PublishOutcome, PublishRefusal, Replica,
    TimedInput, ViewChangeKnobs,
};
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
/// passes its own value (the compliance suite's corpus clusters pass the
/// case's provision timeout).
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

/// The compliance suite's harness rules (`docs/uvrr-host-compliance.md`),
/// armed by [`Node::open_compliance`]. The executor drives the clock —
/// every drive carries the executor's logical tick, never the wall
/// clock — so the corpus replays byte-identically on every run. The
/// boundary rules that ride the flag: the application boundary is the
/// opaque acknowledge (the corpus's raw payloads journal without a
/// Service decode and are never executed, the §11.1 acknowledgement the
/// reference host never sends), and the peer gate accepts the corpus's
/// outside identities (the gossip sender, the fabricated votes) — the
/// core drops what it drops, by name.
#[derive(Default)]
struct Compliance {
    /// The executor's logical tick, carried by every drive between the
    /// executor's settings. `0` until the executor first advances it.
    clock: u64,
}

/// The node construction's runtime policy: the view-change timeout the
/// cluster plays by, and the compliance rules when the constructor is
/// the compliance suite's (`None` is the prod shape — the wall clock,
/// the Service boundary, the descriptor's address space).
struct Construction {
    primary_timeout: u64,
    compliance: Option<Compliance>,
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
    /// The compliance suite's harness rules. `None` on every other
    /// constructor (the prod shape: the wall clock, the Service
    /// boundary, the descriptor's address space).
    compliance: Option<Compliance>,
    /// The boot gate's marker-round schedule (the machine's commits and
    /// the drain between its rounds), shared with this boot's
    /// `GateStore`. The handle outlives the node: the compliance
    /// executor retains it across crash and halt, exactly like the
    /// marker files themselves.
    marker_log: Arc<Mutex<Vec<String>>>,
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
    /// marker, not a tick source. Under the compliance rules the tick is
    /// the executor's logical clock instead: the executor sets it before
    /// every drive, the corpus's determinism (no wall clock anywhere).
    fn tick(&mut self) -> Result<u64, i32> {
        let now = match &self.compliance {
            Some(compliance) => compliance.clock,
            None => unix_millis()?,
        };
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
    /// after every drive (the harness pattern: the latch fires on the
    /// first step after which the witness mints). A failed latch write
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
                // The compliance rules' opaque acknowledge: the corpus's
                // payloads are raw bytes, never Service JSON, and the
                // reference host never executes an apply — the effect is
                // absorbed with no reply, no journal, and no §11.1
                // acknowledgement (the applied frontier stays where the
                // core's own system-op walk left it).
                if self.compliance.is_some() {
                    return Ok(());
                }
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
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// The compliance suite's constructor (`docs/uvrr-host-compliance.md`):
    /// [`Node::open`] over the same marker store with the lock-event
    /// journal disabled and the corpus's deterministic harness rules
    /// armed — the opaque-payload application boundary (the raw corpus
    /// payloads commit without a Service decode), the unbounded
    /// view-change suffix budget the corpus clusters run, and the
    /// executor-driven clock (every drive carries the executor's logical
    /// tick, never the wall clock).
    pub fn open_compliance(
        members: &str,
        own: &str,
        state: &str,
        primary_timeout: u64,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            node_from_sink(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                None,
                None,
                None,
                Construction {
                    primary_timeout,
                    compliance: Some(Compliance::default()),
                },
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
            let journal = open_journal(journal_dir.filter(|dir| !dir.is_empty()), roll_bytes);
            node_from_sink(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                journal,
                Some((variant, PathBuf::from(scratch_dir))),
                None,
                Construction {
                    primary_timeout: PRIMARY_TIMEOUT_MS,
                    compliance: None,
                },
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
            let journal = open_journal(journal_dir, roll_bytes);
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
                    compliance: None,
                },
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
        // legitimate caller and exempt. The compliance rules lift the
        // gate: the corpus drives outside identities (the gossip
        // sender, the fabricated votes) whose verdict is the core's,
        // dropped by name.
        if self.compliance.is_none()
            && !self.known_ids.contains(&from)
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
        // core. The compliance rules lift it — the corpus's payloads are
        // opaque bytes (B2), never Service JSON, and the reference host
        // validates none of them.
        if self.compliance.is_none() && !valid_message_payloads(&message) {
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
                           the markers hold the crash's evidence"
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
                eprintln!("lunet-advisory-lock: the stop's first marker round failed ({error:?})");
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
        if let Err(error) = write_view_record(&self.state_path, &record) {
            trace_line!("stop.refuse.drain-window");
            eprintln!(
                "lunet-advisory-lock: the stop's view-record write failed ({error}); \
                        the markers hold the halt's first round"
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
                           the marker stays at the first round"
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
            eprintln!("lunet-advisory-lock: the stop's second marker round failed ({error:?})");
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
    /// The compliance executor's logical clock: every drive between the
    /// executor's settings carries `at` as its tick. The corpus's
    /// determinism — the wall clock is never read under the compliance
    /// rules. Outside compliance this is a no-op (the wall clock is the
    /// prod tick source).
    pub fn set_compliance_clock(&mut self, at: u64) {
        if let Some(compliance) = &mut self.compliance {
            compliance.clock = at;
        }
    }

    /// One opaque proposal (the compliance suite's `propose` op): the
    /// payload is raw bytes the core carries opaque (B2) — no Service
    /// decode, no reply correlation, the corpus's own `OperationId` the
    /// executor assigns. Outside compliance this reports INVALID: the
    /// prod boundary is the Service request.
    ///
    /// # Panics
    ///
    /// Never deliberately; a drive's boundary panic poisons the node and
    /// reports `PANIC` through the usual path.
    pub fn propose_opaque(&mut self, id: OperationId, payload: &[u8]) -> i32 {
        if self.compliance.is_none() {
            return INVALID;
        }
        self.drive(Input::Propose {
            operation: Operation {
                id,
                payload: payload.to_vec().into_boxed_slice(),
            },
        })
    }

    /// One typed cluster operation over the ordinary consensus pipeline
    /// (the compliance suite's `reconfigure` op), at the reference
    /// host's pivot policy: `pivot: None`, the stop-the-world fallback
    /// — a latency outcome, never an error. Outside compliance this
    /// reports INVALID (the prod boundary derives the pivot itself).
    pub fn reconfigure_opaque(&mut self, op: SystemOperation) -> i32 {
        if self.compliance.is_none() {
            return INVALID;
        }
        self.drive(Input::Reconfigure { op, pivot: None })
    }

    /// The published frontiers: `(accepted, committed, applied)` as the
    /// observation carries them.
    #[must_use]
    pub fn frontiers(&self) -> (u64, u64, u64) {
        let snapshot = self.replica.observer().read();
        (snapshot.accepted, snapshot.committed, snapshot.applied)
    }

    /// The whole journal, from the void slot through the accepted
    /// frontier: the entries the corpus's journal grammar renders.
    ///
    /// # Panics
    ///
    /// A journal whose retained window no longer covers the history
    /// (a published checkpoint reclaimed the prefix) — the compliance
    /// corpus never publishes one.
    #[must_use]
    pub fn journal_entries(&self) -> Vec<LogEntry> {
        let view = self.replica.journal().view();
        let Some(frontier) = view.accepted() else {
            return Vec::new();
        };
        let mut entries = Vec::new();
        match view.copy_out(VOID_SLOT, frontier, &mut entries) {
            vrr::journal::RangeOutcome::Complete => entries,
            other => panic!("the compliance journal is unreclaimed: {other:?}"),
        }
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

    /// The boot gate's marker-round schedule handle: every machine
    /// commit (`commit:<Marker>@<packed identity>`) and the halt's drain
    /// (`drain`), in write order. The handle is this boot's own record —
    /// it is shared with the store and outlives the node, so the
    /// compliance executor retains a crashed or halted node's schedule
    /// exactly as the marker files persist.
    #[must_use]
    pub fn marker_log(&self) -> Arc<Mutex<Vec<String>>> {
        Arc::clone(&self.marker_log)
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
/// a stale Prepare from below its own view then drops by name, exactly
/// as the compliance corpus pins. The journal is not carried (the
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
pub(crate) fn write_view_record(path: &Path, record: &ViewRecord) -> std::io::Result<()> {
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
        let mut file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(record.line().as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, view_record_path(path))?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Reads the view record. `None` when the file does not exist (no stop
/// ever wrote one); any existing-but-unreadable record is an error.
pub(crate) fn read_view_record(path: &Path) -> std::io::Result<Option<ViewRecord>> {
    let path = view_record_path(path);
    if !path.exists() {
        return Ok(None);
    }
    let text = fs::read_to_string(&path)?;
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

pub(crate) fn read_marker(path: &Path) -> std::io::Result<(u16, u16, Marker)> {
    parse_marker(&fs::read_to_string(path)?)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid marker"))
}

/// One durable marker write: fsync+rename+dir-sync (POSIX crash
/// consistency — persist the new directory entry, not just the file's
/// data; Windows no-ops the directory sync, see `sync_dir`).
///
/// The single-file write is the COMPATIBILITY PROJECTION — written only
/// after the authoritative quorum write succeeded (see `marker_store`),
/// never a classification input once the copies exist.
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
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| {
        file.write_all(marker_line(system, crash, marker).as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
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
                    recovery_flush::execute(scratch, *variant, u64::from(pair.new.0)).map_err(
                        |error| {
                            trace_line!("boot.refuse.recovery-flush");
                            refuse(format!("the recovery-boundary flush failed: {error}"))
                        },
                    )?,
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

/// The blocking journal's open, shared by every node-construction path
/// that takes one: a failure logs and disables journaling for the
/// process; the replication path is never poisoned by observability.
fn open_journal(journal_dir: Option<&str>, roll_bytes: u32) -> Option<JournalSink> {
    journal_dir.and_then(
        |dir| match LockJournal::open(Path::new(dir), roll_bytes as u64) {
            Ok(j) => Some(JournalSink::Blocking(j)),
            Err(e) => {
                eprintln!(
                    "lunet-advisory-lock: journal open failed ({e}); \
                 journaling disabled for this process"
                );
                None
            }
        },
    )
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
) -> Result<Node, i32> {
    let journal = open_journal(journal_dir, roll_bytes);
    node_from_sink(
        members_data,
        own_data,
        state_data,
        journal,
        None,
        None,
        Construction {
            primary_timeout,
            compliance: None,
        },
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
) -> Result<Node, i32> {
    let config = AofConfig {
        flush_interval,
        ..AofConfig::default()
    };
    let sink = match AofWriter::open(Path::new(aof_dir), config) {
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
            compliance: None,
        },
    )
}

fn node_from_sink(
    members_data: &[u8],
    own_data: &[u8],
    state_data: &[u8],
    journal: Option<JournalSink>,
    recovery: Option<(RecoveryFlush, PathBuf)>,
    store_ctl: Option<&str>,
    construction: Construction,
) -> Result<Node, i32> {
    // Every construction in this shape announces that the nine
    // `lunet_lock_node_unsafe_*` exports are present, before anything is
    // built: a boot of a `compatibility_suite` build is a misconfiguration
    // and must be unmissable in the log the runbook reads
    // (docs/src/compliance-abi.md). The default shape compiles no such
    // exports and no such line.
    #[cfg(feature = "compatibility_suite")]
    crate::info::announce_compatibility_exposure();
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
        // The compliance suite's clusters run the corpus's unbounded
        // suffix budget; the host's own budget is the datagram-sized
        // EVIDENCE_BUDGET (see the constant's note).
        view_change_budget: if construction.compliance.is_some() {
            usize::MAX
        } else {
            EVIDENCE_BUDGET
        },
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
    // drain between the halt's rounds, in write order. The executor of
    // the compliance suite asserts the schedule through it; the store
    // owns the record.
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
        session: decision.session,
        deferred: decision.deferred,
        stopped: false,
        compliance: construction.compliance,
        marker_log,
        #[cfg(feature = "flight-recorder")]
        flight: crate::flight::FlightRecorder::open_from_env(own_id.0),
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
/// that writes protocol state, mutates the store, or arms the
/// compliance rules (`src/info.rs`, docs/src/compliance-abi.md). Every
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

/// The Compliance ABI — the locked door.
///
/// Nine exports over the [`Node`] methods the upstream compliance corpus
/// drives (`docs/uvrr-host-compliance.md`). The whole module is behind
/// the `compatibility_suite` cargo feature, which is OFF in `default`:
/// with the feature off these functions DO NOT COMPILE, so a production
/// cdylib carries no such symbol at all — an absence, not a runtime
/// refusal. `tests/abi_door_test.rs` reads the built library's symbol
/// table and fails if any `unsafe_` symbol is present in the default
/// build, or if these nine are absent from the feature build; that gate
/// runs on every `make ext-test`.
///
/// Most of these write protocol state, directly or by arming the
/// compliance rules; the rest read the state the corpus's expectations
/// are written against. All of them exist for the corpus and for nothing
/// else, and a build carrying them must never be booted in production:
/// the build script holds the clean-commit guard and every node boot
/// announces the exposure at error severity (`src/info.rs`).
///
/// The vector returns mirror the pull-style buffer contract of
/// `lunet_lock_node_fault`: the payload is written NUL-terminated with
/// its length (excluding the NUL) in `out_len`, and a caller whose
/// buffer is too small gets `TOO_LARGE` and the size it needs. Nothing
/// here allocates across the boundary.
#[cfg(feature = "compatibility_suite")]
mod unsafe_abi {
    use super::*;
    use std::sync::PoisonError;

    /// Copies a NUL-terminated payload out, on the `lunet_lock_node_fault`
    /// contract: `out_len` carries the length excluding the NUL, a
    /// buffer that cannot hold it plus the NUL is `TOO_LARGE` with the
    /// needed size written, and a null `out_data` is a sizing probe.
    unsafe fn copy_out(text: &str, out_data: *mut u8, capacity: usize, out_len: *mut usize) -> i32 {
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
    }

    /// [`Node::open_compliance`] — the compliance constructor: the same
    /// marker store and lock-event journal disabled, with the corpus's
    /// harness rules armed. This is the ONLY entry that arms them, and
    /// the constructor a production host never reaches
    /// (`lunet_lock_node_new` does not arm them).
    ///
    /// Writes: a node, its marker store, and its incarnation markers.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_open_compliance(
        members_len: usize,
        members_data: *const u8,
        own_len: usize,
        own_data: *const u8,
        state_len: usize,
        state_data: *const u8,
        primary_timeout: u64,
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
            let (Ok(members), Ok(own), Ok(state)) = (
                std::str::from_utf8(members_data),
                std::str::from_utf8(own_data),
                std::str::from_utf8(state_data),
            ) else {
                return INVALID;
            };
            match Node::open_compliance(members, own, state, primary_timeout) {
                Ok(node) => {
                    unsafe { *out = Box::into_raw(Box::new(node)).cast() };
                    OK
                }
                Err(code) => code,
            }
        })
    }

    /// [`Node::set_compliance_clock`] — the executor's logical tick,
    /// carried by every drive until the executor advances it again.
    ///
    /// Writes: the node's clock source, and through it every subsequent
    /// drive's timeout arithmetic.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_set_compliance_clock(
        node: *mut c_void,
        at: u64,
    ) -> i32 {
        guarded(|| {
            let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
                return INVALID;
            };
            node.set_compliance_clock(at);
            OK
        })
    }

    /// [`Node::propose_opaque`] — one opaque proposal: the payload is raw
    /// bytes the core carries opaque, and `id_msb`/`id_lsb` are the
    /// executor's own `OperationId` (first eight bytes big-endian, last
    /// eight big-endian — the core's wire order). Outside a compliance
    /// node the method reports `INVALID`; the rules are the ABI's own
    /// refusal, not this wrapper's.
    ///
    /// Writes: protocol state — this is a proposal into the replicated
    /// log.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_propose_opaque(
        node: *mut c_void,
        id_msb: u64,
        id_lsb: u64,
        payload_len: usize,
        payload: *const u8,
    ) -> i32 {
        guarded(|| {
            let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
                return INVALID;
            };
            let Ok(payload) = (unsafe { bytes(payload_len, payload) }) else {
                return INVALID;
            };
            node.propose_opaque(
                OperationId {
                    msb: id_msb,
                    lsb: id_lsb,
                },
                payload,
            )
        })
    }

    /// [`Node::reconfigure_opaque`] — one typed cluster operation over
    /// the ordinary consensus pipeline, at the reference host's pivot
    /// policy (the stop-the-world fallback, a latency outcome). The
    /// operation crosses as the core's own JSON encoding of
    /// `SystemOperation` (the `vrr/serde` derives, which the feature
    /// enables); an unparseable body is `CLIENT_JSON`.
    ///
    /// Writes: protocol state — this is a reconfiguration into the
    /// replicated log, and on commit the folded configuration itself.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_reconfigure_opaque(
        node: *mut c_void,
        op_json_len: usize,
        op_json: *const u8,
    ) -> i32 {
        guarded(|| {
            let Some(node) = (unsafe { node.cast::<Node>().as_mut() }) else {
                return INVALID;
            };
            let Ok(op_json) = (unsafe { bytes(op_json_len, op_json) }) else {
                return INVALID;
            };
            let Ok(op) = serde_json::from_slice::<SystemOperation>(op_json) else {
                return CLIENT_JSON;
            };
            node.reconfigure_opaque(op)
        })
    }

    /// [`Node::frontiers`] — `(accepted, committed, applied)` as the
    /// observation carries them.
    ///
    /// Reads: protocol state. Drives nothing.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_frontiers(
        node: *mut c_void,
        out_accepted: *mut u64,
        out_committed: *mut u64,
        out_applied: *mut u64,
    ) -> i32 {
        guarded(|| {
            if node.is_null()
                || out_accepted.is_null()
                || out_committed.is_null()
                || out_applied.is_null()
            {
                return INVALID;
            }
            let node = unsafe { &*node.cast::<Node>() };
            let (accepted, committed, applied) = node.frontiers();
            unsafe {
                *out_accepted = accepted;
                *out_committed = committed;
                *out_applied = applied;
            }
            OK
        })
    }

    /// [`Node::journal_entries`] — the whole journal, void slot through
    /// the accepted frontier, as the core's own JSON encoding of the
    /// entries (slot, era, payload; an operation payload is its opaque
    /// bytes as a JSON byte array, a system payload its `SystemOperation`).
    ///
    /// Reads: protocol state. Drives nothing.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_journal_entries(
        node: *mut c_void,
        out_data: *mut u8,
        capacity: usize,
        out_len: *mut usize,
    ) -> i32 {
        guarded(|| {
            if node.is_null() || out_len.is_null() {
                return INVALID;
            }
            let node = unsafe { &*node.cast::<Node>() };
            let Ok(text) = serde_json::to_string(&node.journal_entries()) else {
                return SERVICE;
            };
            unsafe { copy_out(&text, out_data, capacity, out_len) }
        })
    }

    /// [`Node::membership`] — the folded configuration's current record:
    /// the succession order and each member's weight, in the same order,
    /// as `out_count` `u32` ids and `u64` weights. A node holding no era
    /// record reports zero members. A caller whose arrays cannot hold the
    /// record gets `TOO_LARGE` and the count it needs.
    ///
    /// Reads: protocol state. Drives nothing.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_membership(
        node: *mut c_void,
        out_count: *mut usize,
        out_ids: *mut u32,
        out_weights: *mut u64,
        capacity: usize,
    ) -> i32 {
        guarded(|| {
            if node.is_null() || out_count.is_null() {
                return INVALID;
            }
            let node = unsafe { &*node.cast::<Node>() };
            let Some((order, weights)) = node.membership() else {
                unsafe { *out_count = 0 };
                return OK;
            };
            unsafe { *out_count = order.len() };
            if order.len() > capacity || out_ids.is_null() || out_weights.is_null() {
                return TOO_LARGE;
            }
            for (index, (&id, &weight)) in order.iter().zip(&weights).enumerate() {
                unsafe {
                    *out_ids.add(index) = id.0;
                    *out_weights.add(index) = weight;
                }
            }
            OK
        })
    }

    /// [`Node::witnesses`] — the gossip-witness list, in list order, as
    /// `out_count` `u32` ids. A caller's array that cannot hold the list
    /// gets `TOO_LARGE` and the count it needs.
    ///
    /// Reads: protocol state. Drives nothing.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_witnesses(
        node: *mut c_void,
        out_count: *mut usize,
        out_ids: *mut u32,
        capacity: usize,
    ) -> i32 {
        guarded(|| {
            if node.is_null() || out_count.is_null() {
                return INVALID;
            }
            let node = unsafe { &*node.cast::<Node>() };
            let witnesses = node.witnesses();
            unsafe { *out_count = witnesses.len() };
            if witnesses.len() > capacity || out_ids.is_null() {
                return TOO_LARGE;
            }
            for (index, &id) in witnesses.iter().enumerate() {
                unsafe { *out_ids.add(index) = id.0 };
            }
            OK
        })
    }

    /// [`Node::marker_log`] — this boot's own marker-round schedule, in
    /// write order, one raw line per record and LF-separated: every
    /// machine commit (`commit:<Marker>@<packed identity>`) and the
    /// halt's drain (`drain`). The lines are the store's own, so the
    /// reader renders them; this call does not interpret them.
    ///
    /// Reads: the marker store's schedule. Drives nothing.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn lunet_lock_node_unsafe_marker_log(
        node: *mut c_void,
        out_data: *mut u8,
        capacity: usize,
        out_len: *mut usize,
    ) -> i32 {
        guarded(|| {
            if node.is_null() || out_len.is_null() {
                return INVALID;
            }
            let node = unsafe { &*node.cast::<Node>() };
            let log = node.marker_log();
            let text = log
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .join("\n");
            unsafe { copy_out(&text, out_data, capacity, out_len) }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use lunet_locks_aof::marker;
    use std::fs;
    use std::sync::PoisonError;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The two-member descriptor: the smallest cluster whose client
    /// proposals round-trip the wire (the leader's phase-2 needs the
    /// follower's ack). Each member's id is its provisioned identity —
    /// its own system half, crash counter 1.
    const MEMBERS_TWO: &str = "65537:n1\x00131073:n2";

    /// One full exchange: every queued send is delivered to the member
    /// whose live identity carries the addressed system half (the
    /// transport's remap: a bumped life answers its old address), and the
    /// cascade repeats until the wire goes quiet, inside a bound. The
    /// outputs a node queues for its own caller (the replies) are NOT
    /// wire traffic: they are returned in arrival order.
    #[allow(clippy::type_complexity)]
    fn exchange_all(nodes: &mut [&mut Node], rounds: usize) -> Vec<(u32, [u8; 16], Vec<u8>)> {
        let mut replies = Vec::new();
        for _ in 0..rounds {
            let mut wire: Vec<(u32, u32, Vec<u8>)> = Vec::new();
            for node in nodes.iter_mut() {
                while let Some(output) = node.next_output() {
                    if output.kind == OUTPUT_SEND {
                        wire.push((node.own_id(), output.to, output.bytes));
                    } else {
                        replies.push((node.own_id(), output.message_id, output.bytes));
                    }
                }
            }
            if wire.is_empty() {
                return replies;
            }
            for (from, to, bytes) in wire {
                let target = nodes
                    .iter_mut()
                    .find(|node| node.own_id() >> SYSTEM_HALF_SHIFT == to >> SYSTEM_HALF_SHIFT);
                if let Some(target) = target {
                    target.receive(from, &bytes);
                }
            }
        }
        replies
    }

    /// The two-node prod-shape cluster settles: both members drive ticks
    /// (the wall clock given its next millisecond), the wire exchanges,
    /// and both report Normal.
    fn settle_two(primary: &mut Node, follower: &mut Node) {
        for _ in 0..1_000 {
            std::thread::sleep(Duration::from_millis(1));
            primary.idle();
            follower.idle();
            let _ = exchange_all(&mut [primary, follower], 64);
            if primary.status().state == 0
                && follower.status().state == 0
                && primary.status().leader == primary.own_id()
            {
                return;
            }
        }
        panic!("the two-node cluster did not settle inside the bound");
    }

    /// Drive a system operation on whichever member currently leads, then
    /// drive the cluster until every member's SERVING era has caught its
    /// CONFIGURATION era. Returns the leader's return code; a refused
    /// operation never drives and so never waits.
    ///
    /// The wait is the whole contract of a reconfiguration: a transition
    /// is complete only once the view has entered the folded era, and an
    /// operation driven inside a still-establishing era refuses with
    /// `SERVICE` (`reconfigure`'s `POSITION_APPEND` fold at ffi.rs:1545
    /// is the same shape of refusal). Waiting on the configuration era
    /// alone is waiting on nothing — it advances the moment the
    /// transition is proposed, not the moment it commits.
    fn drive_on_leader(
        nodes: &mut [&mut Node],
        clock: &mut u64,
        op: u32,
        member: u32,
        position: u32,
    ) -> i32 {
        let lead = nodes
            .iter()
            .position(|node| node.status().leader == node.own_id())
            .expect("a member leads the settled cluster");
        let code = nodes[lead].reconfigure(op, member, position);
        if code != OK {
            return code;
        }
        for _ in 0..1_000 {
            *clock += 1;
            for node in nodes.iter_mut() {
                node.set_compliance_clock(*clock);
                node.idle();
            }
            let _ = exchange_all(nodes, 64);
            if nodes.iter().all(|node| {
                let status = node.status();
                status.state == 0 && status.era == status.config_era
            }) {
                break;
            }
        }
        code
    }

    /// The single-member descriptor: a one-node cluster elects its own
    /// leader, so every lifecycle shape is drivable alone.
    const MEMBERS_ONE: &str = "65537:n1";
    const OWN_ONE: &str = "n1";
    /// The genesis pair (system 1, life 1) as the wire names it.
    const GENESIS_ID: u32 = 65_537;

    /// The scratch tree, inside the repo (`.tmp` is scratch).
    fn scratch(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/ffi-state-machine");
        fs::create_dir_all(&root).expect("the scratch root creates");
        let dir = root.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&dir).expect("the case directory creates");
        dir
    }

    /// The single-node compliance boot over a scratch state path: the
    /// executor's logical clock, the corpus's opaque boundary.
    fn one_node_compliance(state: &Path) -> Node {
        Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50)
            .expect("the first life boots")
    }

    /// The settle loop: tick under the logical clock and drain the wire
    /// until the node reports Normal, inside a bound that a stuck node
    /// exceeds and fails. Returns the clock it left off at.
    fn settle(node: &mut Node) -> u64 {
        let mut clock = 0u64;
        for _ in 0..1_000 {
            clock += 1;
            node.set_compliance_clock(clock);
            node.idle();
            while node.next_output().is_some() {}
            if node.status().state == 0 {
                return clock;
            }
        }
        panic!("the single node did not settle inside the bound");
    }

    /// The superblock file a state path carries.
    fn superblock_of(state: &Path) -> PathBuf {
        let mut os = state.as_os_str().to_os_string();
        os.push(".superblock");
        PathBuf::from(os)
    }

    /// The quorum copies' verdict: the identity pair and the engine state.
    fn copies(state: &Path) -> (u16, u16, marker::MarkerState) {
        marker::classify(&superblock_of(state))
            .map(|classified| {
                (
                    classified.identity.system_identifier(),
                    classified.identity.crash_counter(),
                    classified.state,
                )
            })
            .expect("the quorum copies read")
    }

    /// The compatibility projection's line: `system crash word`.
    fn projection(state: &Path) -> String {
        fs::read_to_string(state).expect("the projection reads")
    }

    /// The boot gate's marker-round schedule, in write order.
    fn marker_schedule(node: &Node) -> Vec<String> {
        node.marker_log()
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    // --------------------------------------------------------------
    // The lifecycle path census.
    //
    // `CENSUS` is the declared list of every path through boot and
    // through stop. Two laws hold it honest, and both are asserted:
    //
    // - `every_declared_census_path_is_named_by_the_code` scans the
    //   adapter's own sources for every `trace_line!` emission and
    //   demands the scanned set and the declared set be EQUAL. A new
    //   path that forgets to declare itself fails; a declared path that
    //   no code emits fails.
    // - `every_boot_path_shows_itself` and `every_stop_path_shows_itself`
    //   drive a fixture per reachable path and assert the line came off
    //   the tape, then demand that their slice of the census was fully
    //   covered. A declared path no fixture reaches is a FAILING
    //   assertion, never a skipped one — the paths that no fixture in
    //   this tree can reach are listed by name in
    //   `CENSUS_UNREACHABLE`, which the same coverage assertion names in
    //   its failure message so a newly-unreachable path is impossible to
    //   miss.
    // --------------------------------------------------------------

    /// Every lifecycle path name the adapter declares it takes.
    const CENSUS: &[&str] = &[
        // The store's read verdicts.
        "marker.read.absent",
        "marker.read.projection",
        "marker.read.quorum",
        "marker.read.cross-system",
        "marker.read.projection-refused",
        "marker.read.corrupt",
        "marker.read.incompatible",
        "marker.read.torn",
        "marker.write.corrupt",
        // The boot gate's classifications and latch rounds.
        "boot.first",
        "boot.first-latch",
        "boot.clean",
        "boot.clean-latch",
        "boot.restored-view",
        "boot.crashed",
        "boot.emission-gate",
        "boot.deferred-latch",
        "boot.deferred-latch-refused",
        // The recovery boundary's flush.
        "boot.recovery-flush",
        // The constructor the classification chose.
        "boot.provision",
        "boot.join",
        "boot.resume",
        "boot.reincarnate",
        // The boot gate's refusals.
        "boot.refuse.quorum-lost",
        "boot.refuse.exhausted",
        "boot.refuse.store",
        "boot.refuse.pair",
        "boot.refuse.first-latch",
        "boot.refuse.clean-latch",
        "boot.refuse.view-record",
        "boot.refuse.view-era",
        "boot.refuse.emission-gate",
        "boot.refuse.recovery-flush",
        "boot.refuse.genesis",
        "boot.refuse.constructor",
        "boot.refuse.descriptor",
        "boot.refuse.store-control",
        // The stop schedule.
        "stop.idempotent",
        "stop.wire-closed",
        "stop.round.begin",
        "stop.drain-window.open",
        "stop.drain-window.close",
        "stop.round.finish",
        "stop.complete",
        "stop.unseated",
        "stop.refuse.first-round",
        "stop.refuse.drain-window",
        "stop.refuse.drain",
        "stop.refuse.second-round",
        "stop.refuse.unseated-drain",
    ];

    /// The census paths whose lines exist and are emitted, but which NO
    /// fixture in this tree can reach — each one an arm the shape of the
    /// store or the pinned core makes unreachable, named here so the
    /// coverage assertion below can say so rather than counting them as
    /// covered. An entry moves out of this list the day a fixture reaches
    /// its path.
    const CENSUS_UNREACHABLE: &[&str] = &[
        // The Zig store refuses INCOMPATIBLE only on a checksum-valid
        // copy carrying a foreign format version; `marker::write` stamps
        // the current version, so no fixture can produce one.
        "marker.read.incompatible",
        // The write path's CORRUPT is a foreign system half on the
        // write's read-back: the state file's system cannot change
        // between the boot read and the write in one thread.
        "marker.write.corrupt",
        // `GateStore::read_copies` hands the engine a UNIFORM 4x
        // `SuperblockCopies` or refuses outright, so the engine's
        // `QuorumLost` verdict never arises on this store.
        "boot.refuse.quorum-lost",
        // `vrr::lifecycle::boot` (uvrr-core 37549d1) returns only
        // `QuorumLost` or `Store`; `BootError::Exhausted` is produced
        // only by the deferred latch, which `settle_deferred_latch`
        // handles.
        "boot.refuse.exhausted",
        // The deployment's genesis era table cannot fail to build for a
        // descriptor the grammar already accepted (distinct ids, at most
        // MAX_MEMBERS).
        "boot.refuse.genesis",
        // The four core constructors accept the genesis the adapter
        // builds.
        "boot.refuse.constructor",
        // The deferred latch's refusal needs an exhausted crash counter
        // AND a seated witness on the same boot; the counter refuses
        // earlier, at `crashed.pair()`.
        "boot.deferred-latch-refused",
        // The two halt rounds are the same write, so no fixture makes the
        // second fail and not the first.
        "stop.refuse.second-round",
        // A sink-drain failure needs a fault-injection seam the host
        // does not carry: the AOF writer drains through an already-open
        // handle and the blocking journal's flush is an `fsync` on one.
        "stop.refuse.drain",
        "stop.refuse.unseated-drain",
    ];

    /// The census tape as this fixture sees it: every line the thread
    /// emitted since the fixture's last read, accumulated (the reader
    /// drains, so each drain is folded in here before the next assertion).
    #[derive(Default)]
    struct Tape(Vec<&'static str>);

    impl Tape {
        /// Drains the thread's census tape into this fixture's view.
        fn take(&mut self) {
            self.0.extend(crate::census_paths());
        }

        /// Asserts `path` was taken, and records it as covered.
        fn assert_path(&mut self, covered: &mut Vec<&'static str>, path: &'static str) {
            self.take();
            assert!(
                self.0.contains(&path),
                "the census tape carries no `{path}` line; this fixture saw {:?}",
                self.0
            );
            covered.push(path);
        }
    }

    /// Demands every declared path in `declared` either was asserted by a
    /// fixture or is named in `CENSUS_UNREACHABLE` — a path that is
    /// neither is a census entry nobody proved and nobody explained.
    fn assert_census_covered(declared: &[&'static str], covered: &[&'static str]) {
        let missing: Vec<&str> = declared
            .iter()
            .copied()
            .filter(|path| !covered.contains(path) && !CENSUS_UNREACHABLE.contains(path))
            .collect();
        assert!(
            missing.is_empty(),
            "every declared path needs a fixture that reaches it or an entry in the \\
             unreachable list; these have neither: {missing:?}"
        );
        for path in covered {
            assert!(
                declared.contains(path),
                "the fixture asserted `{path}`, which the census does not declare"
            );
            assert!(
                !CENSUS_UNREACHABLE.contains(path),
                "the fixture reached `{path}`, which the unreachable list claims no fixture \\
                 can reach — delete it from that list"
            );
        }
    }

    /// The adapter's own sources are the census's ground truth: every
    /// `trace_line!` emission, named. Scanned at compile time from the
    /// two files that carry the boot and stop paths.
    #[test]
    fn every_declared_census_path_is_named_by_the_code() {
        // The needle is assembled at run time so this scanner's own source
        // does not contain a literal emission.
        let needle = ["trace_line", "!(", "\""].concat();
        let sources = [include_str!("ffi.rs"), include_str!("marker_store.rs")];
        let mut emitted: Vec<&str> = Vec::new();
        for source in sources {
            let mut cursor = 0;
            while let Some(found) = source[cursor..].find(&needle) {
                let begin = cursor + found + needle.len();
                let end = begin
                    + source[begin..]
                        .find('"')
                        .expect("every emission's name is closed");
                emitted.push(&source[begin..end]);
                cursor = end + 1;
            }
        }
        emitted.sort_unstable();
        emitted.dedup();
        let mut declared = CENSUS.to_vec();
        declared.sort_unstable();
        let undeclared: Vec<&str> = emitted
            .iter()
            .copied()
            .filter(|path| !declared.contains(path))
            .collect();
        let unemitted: Vec<&str> = declared
            .iter()
            .copied()
            .filter(|path| !emitted.contains(path))
            .collect();
        assert!(
            undeclared.is_empty() && unemitted.is_empty(),
            "the census and the code must be the same set; the code names but the census \
             does not declare {undeclared:?}, the census declares but no code emits \
             {unemitted:?}"
        );
        let unreachable: Vec<&str> = CENSUS_UNREACHABLE.to_vec();
        assert!(
            unreachable.iter().all(|path| declared.contains(path)),
            "the unreachable list names a path the census does not declare"
        );
    }

    /// The first life anchors the genesis pair (the descriptor's system
    /// half, crash counter 1) and the boot itself emits nothing; after a
    /// crash (the drop without the stop contract) the NEXT boot's decided
    /// identity is the bumped life, durable before any emission — the
    /// emission gate's round already landed by the time `Node::open`
    /// returns.
    #[test]
    fn first_boot_anchors_the_genesis_life_and_the_crash_bump_is_durable_at_boot() {
        let dir = scratch("first-boot");
        let state = dir.join("node.state");
        // The first boot: no marker anywhere. The genesis life is anchored.
        let mut node = one_node_compliance(&state);
        assert_eq!(
            node.own_id(),
            GENESIS_ID,
            "the genesis pair (system 1, life 1)"
        );
        assert!(
            node.next_output().is_none(),
            "the boot itself emits nothing: a reopened node answers only what it is driven with"
        );
        assert_eq!(
            copies(&state),
            (1, 1, marker::MarkerState::Unflushed),
            "the first latch anchors the running sentinel"
        );
        assert_eq!(projection(&state), "1 1 unflushed\n");
        // The crash: dropped without the stop contract — the running
        // sentinel stands for the next boot's classification.
        drop(node);
        // The bumped boot: the identity is the strict next life, and the
        // emission gate's round is already durable when open returns.
        let mut life_two = one_node_compliance(&state);
        assert_eq!(life_two.own_id(), 65_538, "the crash counter bumped");
        assert_eq!(
            copies(&state),
            (1, 2, marker::MarkerState::Unflushed),
            "the emission gate's round: the next life at the running sentinel, durable before any emission"
        );
        assert_eq!(projection(&state), "1 2 unflushed\n");
        assert!(
            life_two.next_output().is_none(),
            "the announcement waits for the fenced-boot drive, never the boot"
        );
    }

    /// The dirty restart's announced identity is the marker pair's strict
    /// next life: it never reuses the old id, and a chain of crashes walks
    /// the counter one life at a time — no value is ever revisited.
    #[test]
    fn reincarnated_identity_never_reuses_the_old_id() {
        let dir = scratch("no-reuse");
        let state = dir.join("node.state");
        let mut seen = Vec::new();
        for _ in 0..4 {
            let node = one_node_compliance(&state);
            let live = node.own_id();
            assert!(
                !seen.contains(&live),
                "the identity {live} was already used by an earlier life"
            );
            let counter = NodeId(live).crash_counter().expect("a lawful life").get();
            assert_eq!(
                counter as usize,
                seen.len() + 1,
                "each life is one counter advance past the last identity the disk saw"
            );
            seen.push(live);
            // The running sentinel is a crash: the drop walks the counter.
            drop(node);
        }
    }

    /// The crash bump's marker round lands at the crashed classification
    /// (the emission gate), copy-free rig state included: a single-file
    /// projection in the running sentinel's spelling classifies crashed,
    /// the bump round writes the quorum copies and the projection.
    #[test]
    fn existing_unflushed_files_boot_the_emission_gate_round() {
        let dir = scratch("emission-gate");
        let state = dir.join("node.state");
        // The copy-free rig state: the projection alone, at the running
        // sentinel — a crash by classification, no superblock anywhere.
        fs::write(&state, "1 1 unflushed\n").expect("the projection writes");
        assert!(!superblock_of(&state).exists(), "no quorum copies yet");
        // The crashed classification: the bump is decided at boot and the
        // emission gate lands its one durable round — the quorum copies
        // AND the projection — before the driver releases the first
        // announcement.
        let mut node = one_node_compliance(&state);
        assert_eq!(node.own_id(), 65_538, "the strict next life");
        assert_eq!(copies(&state), (1, 2, marker::MarkerState::Unflushed));
        assert_eq!(projection(&state), "1 2 unflushed\n");
        assert!(
            node.next_output().is_none(),
            "the round is durable before any announcement the drive releases"
        );
        // The unseated window's stop: the wire closes, the sink drains,
        // and NO marker round is lawful — the markers hold the emission
        // gate's round for the next boot's derivation.
        let before = marker_schedule(&node);
        assert_eq!(node.stop(), OK, "the unseated stop drains and exits");
        assert!(
            !before.iter().any(|op| op.starts_with("commit:Stopping")),
            "no halt round runs: the session is not latched in the unseated window"
        );
        assert_eq!(copies(&state), (1, 2, marker::MarkerState::Unflushed));
        let next_life = one_node_compliance(&state);
        assert_eq!(
            next_life.own_id(),
            65_539,
            "the next boot derives the next life"
        );
    }

    /// Makes the scratch directory read-only, so the store's create-shaped
    /// writes (the projection's temp+rename, the superblock's first write)
    /// fail while its reads still succeed. The obstruction that reaches the
    /// latch, emission-gate, and halt-round refusal arms.
    #[cfg(unix)]
    fn seal(dir: &Path) {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dir, fs::Permissions::from_mode(0o500)).expect("the directory seals");
    }

    /// Every boot path shows itself: one fixture per reachable path in the
    /// census, each asserted against the tape the path wrote. Refusals are
    /// first-class — a refused boot hands back no node, which is exactly
    /// why the census tape is thread-local rather than node-scoped.
    #[test]
    fn every_boot_path_shows_itself() {
        let mut covered: Vec<&'static str> = Vec::new();
        let mut tape = Tape::default();

        // --- the first life: nothing durable anywhere ------------------
        tape.take();
        {
            let dir = scratch("census-first");
            let state = dir.join("node.state");
            let node = one_node_compliance(&state);
            assert_eq!(node.own_id(), GENESIS_ID);
            for path in [
                "marker.read.absent",
                "boot.first",
                "boot.first-latch",
                "boot.provision",
            ] {
                tape.assert_path(&mut covered, path);
            }
        }

        // --- the clean continue: a stopped quorum, view record restored -
        tape.take();
        {
            let dir = scratch("census-clean");
            let state = dir.join("node.state");
            let mut node = one_node_compliance(&state);
            settle(&mut node);
            assert_eq!(node.stop(), OK);
            tape.take();
            let resumed = one_node_compliance(&state);
            assert_eq!(resumed.own_id(), GENESIS_ID);
            for path in [
                "marker.read.quorum",
                "boot.clean",
                "boot.clean-latch",
                "boot.restored-view",
                "boot.resume",
            ] {
                tape.assert_path(&mut covered, path);
            }
        }

        // --- the crashed boot: the emission gate, then the latch -------
        tape.take();
        {
            let dir = scratch("census-crashed");
            let state = dir.join("node.state");
            let mut life_one = one_node_compliance(&state);
            settle(&mut life_one);
            drop(life_one); // the crash: the running sentinel stands
            tape.take();
            let life_two = one_node_compliance(&state);
            assert_eq!(life_two.own_id(), 65_538);
            for path in [
                "marker.read.quorum",
                "boot.crashed",
                "boot.emission-gate",
                "boot.reincarnate",
            ] {
                tape.assert_path(&mut covered, path);
            }
        }

        // --- the unseated window closes when the engine seats: the
        // deferred latch lands the emission gate's round again ----------
        tape.take();
        {
            let dir = scratch("census-deferred-latch");
            const TRIAD: &str = "65537:n1\x00131073:n2\x00196609:n3";
            const RISEN: u32 = 196_610;
            let mut leader =
                Node::open_compliance(TRIAD, OWN_ONE, &dir.join("n1.state").to_string_lossy(), 50)
                    .expect("the leader boots");
            let mut second =
                Node::open_compliance(TRIAD, "n2", &dir.join("n2.state").to_string_lossy(), 50)
                    .expect("the second boots");
            let mut third =
                Node::open_compliance(TRIAD, "n3", &dir.join("n3.state").to_string_lossy(), 50)
                    .expect("the third boots");
            let mut clock = 0u64;
            let mut served = false;
            for _ in 0..1_000 {
                clock += 1;
                for node in [&mut leader, &mut second, &mut third] {
                    node.set_compliance_clock(clock);
                    node.idle();
                }
                let _ = exchange_all(&mut [&mut leader, &mut second, &mut third], 64);
                if [&leader, &second, &third]
                    .iter()
                    .all(|node| node.status().state == 0)
                {
                    served = true;
                    break;
                }
            }
            assert!(served, "the triad serves");
            drop(third); // the crash: the running sentinel is the evidence
            tape.take();
            let mut risen =
                Node::open_compliance(TRIAD, "n3", &dir.join("n3.state").to_string_lossy(), 50)
                    .expect("the crashed member reopens");
            assert_eq!(risen.own_id(), RISEN, "the strict next life of system 3");
            // The leader takes the entry ticket, then drives the forced
            // walk until the new life sits at weight 1 — the seat is what
            // mints the witness the deferred latch needs.
            clock += 1;
            risen.set_compliance_clock(clock);
            assert_eq!(risen.recover(), OK, "the announcement drives");
            let mut leader_copy = None;
            while let Some(output) = risen.next_output() {
                if output.to == 65_537 {
                    leader_copy = Some(output.bytes);
                }
            }
            assert!(
                leader_copy.is_some(),
                "the (old, new) pair is announced to the leader"
            );
            assert_eq!(
                leader.receive(RISEN, &leader_copy.expect("the leader's copy")),
                OK,
                "the leader takes the ticket"
            );
            let mut seated = false;
            for _ in 0..4_000 {
                clock += 1;
                for node in [&mut leader, &mut second, &mut risen] {
                    node.set_compliance_clock(clock);
                    node.idle();
                }
                let _ = exchange_all(&mut [&mut leader, &mut second, &mut risen], 64);
                if risen.voting_weight() != Some(1) {
                    risen.set_compliance_clock(clock);
                    risen.recover();
                }
                if risen.voting_weight() == Some(1) && risen.status().state == 0 {
                    seated = true;
                    break;
                }
            }
            assert!(seated, "the forced walk seats the new life");
            tape.assert_path(&mut covered, "boot.deferred-latch");
        }

        // --- the copy-free projection: clean and crashed spellings -----
        for (name, line, expected) in [
            ("projection-clean", "1 1 flushed\n", GENESIS_ID),
            ("projection-crashed", "1 1 unflushed\n", 65_538),
        ] {
            tape.take();
            let dir = scratch(name);
            let state = dir.join("node.state");
            fs::write(&state, line).expect("the projection writes");
            let node = Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50)
                .expect("the migrating boot");
            assert_eq!(node.own_id(), expected);
            tape.assert_path(&mut covered, "marker.read.projection");
            if expected == GENESIS_ID {
                tape.assert_path(&mut covered, "boot.clean");
                tape.assert_path(&mut covered, "boot.resume");
            } else {
                tape.assert_path(&mut covered, "boot.crashed");
                tape.assert_path(&mut covered, "boot.emission-gate");
                tape.assert_path(&mut covered, "boot.reincarnate");
            }
        }

        // --- the joiner: a post-genesis member's first life ------------
        tape.take();
        {
            let dir = scratch("census-joiner");
            Node::open_compliance(
                MEMBERS_THREE_J,
                "n3",
                &dir.join("n3.state").to_string_lossy(),
                50,
            )
            .expect("the joiner boots");
            tape.assert_path(&mut covered, "boot.join");
        }

        // --- the descriptor's grammar ---------------------------------
        tape.take();
        {
            let dir = scratch("census-descriptor");
            let state = dir.join("node.state");
            assert_eq!(
                Node::open_compliance("65537", OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG)
            );
            tape.assert_path(&mut covered, "boot.refuse.descriptor");
        }

        // --- the bench store's control socket is unreachable ----------
        tape.take();
        #[cfg(unix)]
        {
            let dir = scratch("census-store-control");
            assert_eq!(
                Node::open_bench(
                    MEMBERS_ONE,
                    OWN_ONE,
                    None,
                    0,
                    &dir.join("absent.sock").to_string_lossy(),
                )
                .err(),
                Some(CONFIG),
                "an unreachable control socket refuses the boot"
            );
            tape.assert_path(&mut covered, "boot.refuse.store-control");
        }

        // --- the store's read verdicts, one fixture each --------------
        let store_cases: &[(&str, &str, &str)] = &[
            (
                "projection-refused",
                "not a marker line\n",
                "marker.read.projection-refused",
            ),
            (
                "cross-system",
                "2 1 unflushed\n",
                "marker.read.cross-system",
            ),
        ];
        for (name, line, verdict) in store_cases {
            tape.take();
            let dir = scratch(&format!("census-{name}"));
            let state = dir.join("node.state");
            fs::write(&state, *line).expect("the projection writes");
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "the {name} projection refuses the boot"
            );
            tape.assert_path(&mut covered, verdict);
            tape.assert_path(&mut covered, "boot.refuse.store");
        }

        // --- the torn quorum: below the read threshold ----------------
        tape.take();
        {
            let dir = scratch("census-torn");
            let state = dir.join("node.state");
            let mut node = one_node_compliance(&state);
            settle(&mut node);
            assert_eq!(node.stop(), OK);
            let record = superblock_of(&state);
            let full = fs::read(&record).expect("the store reads");
            let zone = full.len() / marker::geometry().expect("the geometry reports").copies;
            fs::write(&record, &full[..zone + zone / 2]).expect("the tear writes");
            tape.take();
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "a torn quorum refuses the boot"
            );
            tape.assert_path(&mut covered, "marker.read.torn");
            tape.assert_path(&mut covered, "boot.refuse.store");
        }

        // --- the rotted copy: the boot-read safety law's PANIC --------
        tape.take();
        {
            let dir = scratch("census-corrupt");
            let state = dir.join("node.state");
            let mut node = one_node_compliance(&state);
            settle(&mut node);
            assert_eq!(node.stop(), OK);
            let record = superblock_of(&state);
            let before = fs::read(&record).expect("the store reads");
            let zone = before.len() / marker::geometry().expect("the geometry reports").copies;
            let mut rotted = before.clone();
            for byte in &mut rotted[2 * zone..3 * zone] {
                *byte = 0xA5;
            }
            fs::write(&record, &rotted).expect("the rot writes");
            tape.take();
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(PANIC),
                "a rotted copy panics the boot"
            );
            tape.assert_path(&mut covered, "marker.read.corrupt");
        }

        // --- the exhausted crash counter: the replacement pair ---------
        tape.take();
        {
            let dir = scratch("census-exhausted");
            let state = dir.join("node.state");
            marker::write(
                &superblock_of(&state),
                marker::NodeIdentity::new(1, u16::MAX).expect("the ceiling pair spells"),
                marker::MarkerState::Unflushed,
            )
            .expect("the ceiling round writes");
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "the ceiling identity refuses the boot"
            );
            tape.assert_path(&mut covered, "boot.refuse.pair");
        }

        // --- the clean start's view record is unreadable --------------
        tape.take();
        {
            let dir = scratch("census-view-record");
            let state = dir.join("node.state");
            let mut node = one_node_compliance(&state);
            settle(&mut node);
            assert_eq!(node.stop(), OK);
            fs::write(view_record_path(&state), "not a record\n").expect("the view record rots");
            tape.take();
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "an unreadable view record refuses the boot"
            );
            tape.assert_path(&mut covered, "boot.refuse.view-record");
        }

        // --- the recovery boundary's flush, run and refused -----------
        tape.take();
        {
            let dir = scratch("census-flush");
            let state = dir.join("node.state");
            let mut life_one = one_node_compliance(&state);
            settle(&mut life_one);
            drop(life_one);
            tape.take();
            let node = Node::open_with_recovery_flush(
                MEMBERS_ONE,
                OWN_ONE,
                &state.to_string_lossy(),
                None,
                0,
                RecoveryFlush::SingleBlock,
                &dir.join("scratch").to_string_lossy(),
            )
            .expect("the flushed recovery boundary boots");
            tape.assert_path(&mut covered, "boot.recovery-flush");
            drop(node);
        }
        tape.take();
        {
            let dir = scratch("census-flush-refused");
            let state = dir.join("node.state");
            let mut life_one = one_node_compliance(&state);
            settle(&mut life_one);
            drop(life_one);
            let blocked = dir.join("not-a-directory");
            fs::write(&blocked, "the scratch path is a file\n").expect("the obstruction writes");
            tape.take();
            assert_eq!(
                Node::open_with_recovery_flush(
                    MEMBERS_ONE,
                    OWN_ONE,
                    &state.to_string_lossy(),
                    None,
                    0,
                    RecoveryFlush::SingleBlock,
                    &blocked.to_string_lossy(),
                )
                .err(),
                Some(CONFIG),
                "a failed recovery-boundary flush refuses the boot"
            );
            tape.assert_path(&mut covered, "boot.refuse.recovery-flush");
        }

        // --- the view record names an era the genesis table cannot ----
        tape.take();
        {
            let dir = scratch("census-view-era");
            const TRIAD: &str = "65537:n1\x00131073:n2\x00196609:n3";
            const JOINED: u32 = 262_145;
            let mut n1 =
                Node::open_compliance(TRIAD, "n1", &dir.join("n1.state").to_string_lossy(), 50)
                    .expect("n1 boots");
            let mut n2 =
                Node::open_compliance(TRIAD, "n2", &dir.join("n2.state").to_string_lossy(), 50)
                    .expect("n2 boots");
            let mut n3 =
                Node::open_compliance(TRIAD, "n3", &dir.join("n3.state").to_string_lossy(), 50)
                    .expect("n3 boots");
            let mut clock = 0u64;
            let mut settled = false;
            for _ in 0..1_000 {
                clock += 1;
                for node in [&mut n1, &mut n2, &mut n3] {
                    node.set_compliance_clock(clock);
                    node.idle();
                }
                let _ = exchange_all(&mut [&mut n1, &mut n2, &mut n3], 64);
                if [&n1, &n2, &n3].iter().all(|node| {
                    let status = node.status();
                    status.state == 0 && status.era == status.config_era
                }) {
                    settled = true;
                    break;
                }
            }
            assert!(settled, "the triad settles");
            assert_eq!(
                drive_on_leader(
                    &mut [&mut n1, &mut n2, &mut n3],
                    &mut clock,
                    RECONFIGURE_JOIN,
                    JOINED,
                    POSITION_APPEND,
                ),
                OK,
                "the join folds a fourth row and the era advances"
            );
            assert_eq!(n1.status().era, 2, "the fold established era 2");
            assert_eq!(n1.stop(), OK, "the drain window writes the era-2 ballot");
            let state = dir.join("n1.state");
            assert_eq!(
                fs::read_to_string(view_record_path(&state)).expect("the view record reads"),
                "2 1\n"
            );
            tape.take();
            assert_eq!(
                Node::open_compliance(TRIAD, "n1", &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "a view record naming an era the genesis table cannot is refused"
            );
            tape.assert_path(&mut covered, "boot.refuse.view-era");
        }

        // --- the store's write refusals, three arms --------------------
        #[cfg(unix)]
        {
            // The first latch: the read found nothing, the write cannot
            // create.
            tape.take();
            let dir = scratch("census-first-latch");
            let state = dir.join("node.state");
            seal(&dir);
            let refused =
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err();
            assert_eq!(refused, Some(CONFIG), "the first latch write is refused");
            tape.assert_path(&mut covered, "boot.refuse.first-latch");
        }
        #[cfg(unix)]
        {
            // The clean latch: the projection read, the routed write
            // cannot create the copies.
            tape.take();
            let dir = scratch("census-clean-latch");
            let state = dir.join("node.state");
            fs::write(&state, "1 1 flushed\n").expect("the projection writes");
            seal(&dir);
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "the clean latch write is refused"
            );
            tape.assert_path(&mut covered, "boot.refuse.clean-latch");
        }
        #[cfg(unix)]
        {
            // The emission gate: the crash classified, its round cannot
            // be written.
            tape.take();
            let dir = scratch("census-emission-gate");
            let state = dir.join("node.state");
            let mut life_one = one_node_compliance(&state);
            settle(&mut life_one);
            drop(life_one);
            seal(&dir);
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "the emission gate's round is refused"
            );
            tape.assert_path(&mut covered, "boot.refuse.emission-gate");
        }

        let boot_paths: Vec<&'static str> = CENSUS
            .iter()
            .copied()
            .filter(|path| !path.starts_with("stop."))
            .collect();
        assert_census_covered(&boot_paths, &covered);
    }

    /// Every stop path shows itself: the idempotent re-entry, the drain
    /// point, both marker rounds with the drain window opening and closing
    /// between them, the unseated window, and the two reachable refusal
    /// arms.
    #[test]
    fn every_stop_path_shows_itself() {
        let mut covered: Vec<&'static str> = Vec::new();
        let mut tape = Tape::default();

        // --- the graceful stop and its idempotent re-entry -------------
        {
            let dir = scratch("census-stop");
            let state = dir.join("node.state");
            let mut node = one_node_compliance(&state);
            settle(&mut node);
            tape.take();
            assert_eq!(node.stop(), OK);
            for path in [
                "stop.wire-closed",
                "stop.round.begin",
                "stop.drain-window.open",
                "stop.drain-window.close",
                "stop.round.finish",
                "stop.complete",
            ] {
                tape.assert_path(&mut covered, path);
            }
            // The halt's two rounds with the drain strictly between them:
            // the schedule the store records is the census's witness that
            // the window really closed between them.
            let schedule = marker_schedule(&node);
            let tail = &schedule[schedule.len() - 3..];
            assert!(
                tail[0].starts_with("commit:Stopping@")
                    && tail[1] == "drain"
                    && tail[2].starts_with("commit:Stopped@"),
                "the halt schedule is first round, drain, second round; got {tail:?}"
            );
            assert_eq!(node.stop(), OK, "the stop is idempotent");
            tape.assert_path(&mut covered, "stop.idempotent");
        }

        // --- the unseated window: the wire closes, no round is lawful --
        {
            let dir = scratch("census-unseated");
            let state = dir.join("node.state");
            let mut life_one = one_node_compliance(&state);
            settle(&mut life_one);
            drop(life_one);
            let mut life_two = one_node_compliance(&state);
            tape.take();
            assert_eq!(life_two.stop(), OK, "the unseated stop drains and exits");
            tape.assert_path(&mut covered, "stop.wire-closed");
            tape.assert_path(&mut covered, "stop.unseated");
        }

        // --- the drain window's own write is refused -------------------
        {
            let dir = scratch("census-drain-window");
            let state = dir.join("node.state");
            let mut node = one_node_compliance(&state);
            settle(&mut node);
            fs::create_dir(view_record_path(&state)).expect("the obstruction places");
            tape.take();
            assert_eq!(
                node.stop(),
                SERVICE,
                "the failed window write reports SERVICE"
            );
            tape.assert_path(&mut covered, "stop.wire-closed");
            tape.assert_path(&mut covered, "stop.round.begin");
            tape.assert_path(&mut covered, "stop.refuse.drain-window");
        }

        // --- the halt's first round is refused -------------------------
        #[cfg(unix)]
        {
            let dir = scratch("census-first-round");
            let state = dir.join("node.state");
            let mut node = one_node_compliance(&state);
            settle(&mut node);
            seal(&dir);
            tape.take();
            assert_eq!(node.stop(), SERVICE, "the first round's write is refused");
            tape.assert_path(&mut covered, "stop.wire-closed");
            tape.assert_path(&mut covered, "stop.round.begin");
            tape.assert_path(&mut covered, "stop.refuse.first-round");
        }

        let stop_paths: Vec<&'static str> = CENSUS
            .iter()
            .copied()
            .filter(|path| path.starts_with("stop."))
            .collect();
        assert_census_covered(&stop_paths, &covered);
    }

    /// The migration path, the clean-stop spelling: a copy-free rig state
    /// whose single file reads `flushed` (the pre-routing boot's end
    /// state) migrates at boot — the classification reads the file, the
    /// first routed write seeds the copies, and the boot continues under
    /// the SAME identity.
    #[test]
    fn legacy_flushed_file_migrates_and_continues_clean() {
        let dir = scratch("legacy-migration");
        let state = dir.join("node.state");
        fs::write(&state, "1 1 flushed\n").expect("the legacy projection writes");
        // The clean classification reads the file (no copies to read): the
        // same identity resumes, and the clean latch's one round — the
        // first routed write — seeds the quorum copies from the file's
        // own state.
        let mut node = one_node_compliance(&state);
        assert_eq!(node.own_id(), GENESIS_ID, "the migration never bumps");
        assert_eq!(
            copies(&state),
            (1, 1, marker::MarkerState::Unflushed),
            "the clean latch seeded the copies at the file's identity"
        );
        assert_eq!(projection(&state), "1 1 unflushed\n");
        // The migrated node serves the ordinary lifecycle: settle, stop,
        // and the next boot continues under the same identity.
        settle(&mut node);
        assert_eq!(node.stop(), OK);
        assert_eq!(copies(&state), (1, 1, marker::MarkerState::Flushed));
        let resumed = one_node_compliance(&state);
        assert_eq!(
            resumed.own_id(),
            GENESIS_ID,
            "the clean continue never bumps"
        );
    }

    /// The boot gate refuses every marker it cannot classify: a zero half
    /// is no identity (the projection's parser refuses it, no identity is
    /// guessed), an unreadable line is no marker, and a full crash counter
    /// cannot be bumped — the exhausted identity refuses the boot rather
    /// than serving under a recycled value.
    #[test]
    fn the_boot_gate_refuses_malformed_identities_and_exhausted_counters() {
        let dir = scratch("boot-refusals");
        let cases: &[(&str, &str)] = &[
            ("zero-system", "0 1 unflushed\n"),
            ("zero-counter", "1 0 flushed\n"),
            ("garbage", "not a marker line\n"),
            ("one-field", "65537\n"),
            ("unknown-word", "1 1 ancient\n"),
        ];
        for (name, line) in cases {
            let state = dir.join(format!("{name}.state"));
            fs::write(&state, line).expect("the malformed marker writes");
            assert_eq!(
                Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
                Some(CONFIG),
                "the boot refuses the malformed marker {name}"
            );
            assert_eq!(
                projection(&state),
                *line,
                "the refused boot rewrites nothing"
            );
        }
        // The exhausted counter: the marker names the last lawful life
        // (crash counter 65535, the u16 ceiling), so the replacement pair
        // cannot be spelled — the boot refuses.
        let state = dir.join("exhausted.state");
        marker::write(
            &superblock_of(&state),
            marker::NodeIdentity::new(1, u16::MAX).expect("the ceiling pair spells"),
            marker::MarkerState::Unflushed,
        )
        .expect("the ceiling round writes");
        fs::write(&state, format!("1 {} unflushed\n", u16::MAX)).expect("the projection writes");
        assert_eq!(
            Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
            Some(CONFIG),
            "the exhausted identity refuses the boot"
        );
    }

    /// The purge's law at the boot gate: copies that exist but cannot be
    /// read to a quorum verdict refuse the boot — the projection never
    /// rescues an unreadable quorum and no identity is guessed. The
    /// torn-away shape: three copies' zones read short (never fully
    /// written), so only one readable copy stands — below the 2/4 open
    /// threshold, no verdict, the boot refuses.
    #[test]
    fn an_unreadable_marker_quorum_refuses_the_boot() {
        let dir = scratch("unreadable-quorum");
        let state = dir.join("node.state");
        // The clean-stop artifact: flushed at the genesis pair.
        let mut node = one_node_compliance(&state);
        settle(&mut node);
        assert_eq!(node.stop(), OK);
        assert_eq!(copies(&state), (1, 1, marker::MarkerState::Flushed));
        // The tear: the file is cut inside copy 1's zone (the zones are
        // the file's four uniform spans), so copy 0 is the only fully
        // readable copy — below the 2/4 open threshold.
        let full = fs::read(superblock_of(&state)).expect("the store reads");
        let zone = full.len() / marker::geometry().expect("the geometry reports").copies;
        let torn = &full[..zone + zone / 2];
        fs::write(superblock_of(&state), torn).expect("the tear writes");
        let projection_before = projection(&state);
        // The boot refuses: no quorum, no verdict, no identity guessed —
        // and the projection never rescues the unreadable quorum.
        assert_eq!(
            Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
            Some(CONFIG),
            "the unreadable quorum refuses the boot"
        );
        assert_eq!(
            fs::read(superblock_of(&state)).expect("the store reads"),
            torn,
            "the refused boot rewrites nothing"
        );
        assert_eq!(projection(&state), projection_before);
    }

    /// THE BOOT-READ SAFETY LAW: a bad checksum on ANY copy is a loud log
    /// and a panic — the boot refuses loudly and the store never clears,
    /// repairs, or falls back from a bad block. The lifecycle reaches its
    /// clean-stop end state, one copy's zone is rotted (garbage over the
    /// whole zone), and the next boot panics inside the boot gate —
    /// `Node::open`'s boundary reports it as the PANIC code — with the
    /// corrupted bytes standing exactly as they were: no self-heal.
    #[test]
    fn a_rotted_marker_copy_panics_the_boot_and_is_never_healed() {
        let dir = scratch("rotted-copy");
        let state = dir.join("node.state");
        let mut node = one_node_compliance(&state);
        settle(&mut node);
        assert_eq!(node.stop(), OK);
        assert_eq!(copies(&state), (1, 1, marker::MarkerState::Flushed));
        // The rot: garbage over copy 2's whole zone — a checksum failure
        // on a readable copy, not a tear. The zones are the file's four
        // uniform spans.
        let record = superblock_of(&state);
        let before = fs::read(&record).expect("the store reads");
        let zone_count = marker::geometry().expect("the geometry reports").copies;
        let zone = before.len() / zone_count;
        let mut rotted = before.clone();
        let start = 2 * zone;
        for byte in &mut rotted[start..start + zone] {
            *byte = 0xA5;
        }
        fs::write(&record, &rotted).expect("the rot writes");
        let inspected = marker::inspect(&record).expect("the store inspects");
        assert_eq!(inspected[2].valid_checksum, 0, "copy 2 rotted");
        assert!(
            inspected
                .iter()
                .enumerate()
                .all(|(index, one)| index == 2 || one.valid_checksum != 0),
            "the other three copies stand"
        );
        // The next boot panics inside the boot gate; the boundary reports
        // the PANIC code — the call returns, it never hangs.
        assert_eq!(
            Node::open_compliance(MEMBERS_ONE, OWN_ONE, &state.to_string_lossy(), 50).err(),
            Some(PANIC),
            "the boot refuses loudly, never hangs"
        );
        // The rot stands after the refused boot: no self-heal.
        assert_eq!(
            fs::read(&record).expect("the store reads"),
            rotted,
            "the corrupt bytes stand exactly as written"
        );
        assert_eq!(
            marker::classify(&superblock_of(&state)),
            Err(marker::CORRUPT),
            "the read refuses again: nothing was healed"
        );
    }

    /// The clean-stop lifecycle end to end through `Node::open` and
    /// `Node::stop`: the stopped node's marker reads clean on the next
    /// boot — same identity, no bump, no reincarnation announcement, the
    /// running sentinel rewritten as operating begins. The drain-window
    /// view record comes back: the node resumes at the view it stopped
    /// at, not the genesis view.
    #[test]
    fn clean_stop_boot_continues_the_same_incarnation_no_bump() {
        let dir = scratch("clean-stop");
        let state = dir.join("node.state");
        // The first life: settle, then advance the view — the balloted
        // force view is the host's own detector conclusion (§14.2). The
        // drives stay inside the primary-timeout window: the loop breaks
        // the moment the new view installs Normal.
        let mut node = one_node_compliance(&state);
        let mut clock = settle(&mut node);
        assert_eq!(node.force_view(1, 2), OK, "the forced view advances");
        let mut installed = false;
        for _ in 0..40 {
            clock += 1;
            node.set_compliance_clock(clock);
            node.idle();
            while node.next_output().is_some() {}
            if node.status().view == 2 && node.status().state == 0 {
                installed = true;
                break;
            }
        }
        assert!(installed, "the new view installs Normal inside the bound");
        assert_eq!(node.status().view, 2, "the new view installs");
        // The graceful stop: the halt's two rounds with the drain between.
        assert_eq!(node.stop(), OK);
        assert_eq!(copies(&state), (1, 1, marker::MarkerState::Flushed));
        // The drain window's own durable write: the view record.
        assert_eq!(
            fs::read_to_string(view_record_path(&state)).expect("the view record reads"),
            "1 2\n",
            "the drain window wrote the ballot the node held"
        );
        // The clean start: the same identity, the stopped view restored.
        let mut resumed = one_node_compliance(&state);
        assert_eq!(resumed.own_id(), GENESIS_ID, "the clean start never bumps");
        assert_eq!(resumed.status().view, 2, "the view record restores");
        assert!(!resumed.status().poisoned);
        // No reincarnation announcement: the resumed node's first drives
        // announce nothing — a clean resume carries no pair.
        assert!(resumed.next_output().is_none(), "no announcement at boot");
        resumed.set_compliance_clock(20_000);
        assert_eq!(resumed.recover(), OK, "the resumed node drives");
        while resumed.next_output().is_some() {}
        assert!(
            !marker_schedule(&resumed)
                .iter()
                .any(|op| op.contains("Joining@") && op.ends_with("@131074")),
            "no bump round: the same incarnation continues"
        );
    }

    /// The mandatory obligation's proof: once stopped, NO further inbound
    /// entry is picked up — request, receive, ticks, and the admin drives
    /// all refuse, and the node's state stays exactly as the drain point
    /// left it.
    #[test]
    fn stopped_node_refuses_every_inbound_entry_and_the_state_is_final() {
        let dir = scratch("stopped-refusals");
        let state = dir.join("node.state");
        let mut node = one_node_compliance(&state);
        settle(&mut node);
        assert_eq!(node.stop(), OK, "the graceful stop");
        let final_status = node.status();
        // Every inbound entry refuses with the drain point's code.
        assert_eq!(node.request(b"{\"op\":\"get\",\"message_id\":\"00000000-0000-0000-0000-000000000001\",\"client_id\":1,\"request_num\":1,\"lock_id\":1}"), STOPPED);
        assert_eq!(node.receive(GENESIS_ID, &[0, 1, 2]), STOPPED);
        assert_eq!(node.idle(), STOPPED);
        assert_eq!(node.leader_timeout(), STOPPED);
        assert_eq!(node.force_view(1, 3), STOPPED);
        assert_eq!(
            node.reconfigure(RECONFIGURE_JOIN, 131_074, POSITION_APPEND),
            STOPPED
        );
        assert_eq!(node.recover(), STOPPED);
        // The state is final: exactly as the drain point left it.
        let after = node.status();
        assert_eq!(after.era, final_status.era);
        assert_eq!(after.view, final_status.view);
        assert_eq!(after.state, final_status.state);
        assert_eq!(after.leader, final_status.leader);
        assert!(node.next_output().is_none(), "the refusals queue nothing");
        // The stop is idempotent: a second stop reports OK, writes nothing.
        let schedule = marker_schedule(&node);
        assert_eq!(node.stop(), OK);
        assert_eq!(marker_schedule(&node), schedule);
    }

    /// The stop path's write ordering with the async AOF sink (§2): the
    /// `flushed` marker is written only after the writer drained, so every
    /// event enqueued before the stop is durable on disk by the time the
    /// marker lands. The contract's failure arm is the mirror: a failed
    /// drain-window write reports SERVICE with the markers standing at the
    /// halt's first round — a death past the first round reads crashed, the
    /// purge's law: nothing vouches before the drain.
    #[test]
    fn stop_drains_the_aof_writer_before_the_flushed_marker() {
        let dir = scratch("aof-drain");
        // The AOF-backed leader over the two-member cluster: the
        // committed-transition hook enqueues to the write-behind writer.
        let aof_dir = dir.join("aof");
        let mut primary = Node::open_aof(
            MEMBERS_TWO,
            OWN_ONE,
            &dir.join("n1.state").to_string_lossy(),
            &aof_dir.to_string_lossy(),
            None,
        )
        .expect("the AOF-backed boot");
        let mut follower = Node::open(
            MEMBERS_TWO,
            "n2",
            &dir.join("n2.state").to_string_lossy(),
            None,
            0,
            PRIMARY_TIMEOUT_MS,
        )
        .expect("the follower boots");
        settle_two(&mut primary, &mut follower);
        // A holder-changing SET: the Hold transition enqueues to the AOF.
        let set = b"{\"op\":\"set\",\"message_id\":\"00000001-0000-0000-0000-0000000000aa\",\"client_id\":1,\"request_num\":1,\"lock_id\":1,\"lease\":{\"lease_id\":1,\"holder\":\"00000001-0000-0000-0000-0000000000bb\",\"lease_ms\":1000}}";
        assert_eq!(primary.request(set), OK, "the SET proposes");
        let committed_before = primary.frontiers().1;
        let mut committed = false;
        for _ in 0..1_000 {
            std::thread::sleep(Duration::from_millis(1));
            primary.idle();
            follower.idle();
            let _ = exchange_all(&mut [&mut primary, &mut follower], 64);
            if primary.frontiers().1 > committed_before {
                committed = true;
                break;
            }
        }
        assert!(committed, "the SET commits inside the bound");
        // The graceful stop: the schedule is the halt's two rounds with
        // the drain strictly between them.
        assert_eq!(primary.stop(), OK);
        let schedule = marker_schedule(&primary);
        let tail = &schedule[schedule.len() - 3..];
        assert!(
            tail[0].starts_with("commit:Stopping@")
                && tail[1] == "drain"
                && tail[2].starts_with("commit:Stopped@"),
            "the halt schedule is first round, drain, second round; got {tail:?}"
        );
        // Every event enqueued before the stop is durable on disk: the
        // AOF series carries the Hold record the drain flushed.
        let mut durable = Vec::new();
        for entry in fs::read_dir(&aof_dir).expect("the AOF series reads") {
            let path = entry.expect("the entry reads").path();
            if path.extension().is_some_and(|ext| ext == "bin") {
                durable.extend(journal::parse_file(
                    &fs::read(&path).expect("the file reads"),
                ));
            }
        }
        assert!(
            durable
                .iter()
                .any(|event| event.kind == journal::KIND_HOLD && event.lock_id == 1),
            "the Hold event is durable in the AOF series: {durable:?}"
        );
    }

    /// The StoppingNotFlushed shape, as the adapter returns it TODAY: the
    /// drain-window write failing reports SERVICE with the markers at the
    /// halt's first round (`Stopping`), and the next boot classifies
    /// crashed — nothing vouches before the drain. The operator-surface
    /// Sorry{runbook} verdict is the matcher's opinion; the adapter's
    /// contract here is the SERVICE refusal and the crashed next boot.
    #[test]
    fn a_failed_drain_window_write_reports_service_and_the_markers_stand_at_the_first_round() {
        let dir = scratch("stopping-not-flushed");
        let state = dir.join("node.state");
        let mut node = one_node_compliance(&state);
        settle(&mut node);
        // The obstruction: a DIRECTORY where the drain window's view
        // record would land — the rename fails, the drain never runs.
        fs::create_dir(view_record_path(&state)).expect("the obstruction places");
        assert_eq!(node.stop(), SERVICE, "the failed write reports SERVICE");
        // The markers stand at the halt's first round: the projection
        // spells the first round's word, the schedule has no second round.
        assert_eq!(projection(&state), "1 1 stopped\n");
        let schedule = marker_schedule(&node);
        let last = schedule.last().expect("a schedule");
        assert!(
            last.starts_with("commit:Stopping@"),
            "the halt's first round is the last completed transition: {schedule:?}"
        );
        // The next boot re-classifies crashed: the strictly next life.
        let life_two = one_node_compliance(&state);
        assert_eq!(
            life_two.own_id(),
            65_538,
            "nothing vouches before the drain"
        );
    }

    /// The member buffer's grammar is the descriptor: every violation is a
    /// CONFIG refusal at `Node::open` — no members, an unparseable entry,
    /// an empty name, duplicate ids, duplicate names, an `own` that names
    /// no member, a `:j` entry in the founding succession.
    #[test]
    fn node_new_refuses_bad_membership() {
        let dir = scratch("bad-membership");
        let state = dir.join("node.state");
        let empty = "";
        let cases: &[(&str, &str)] = &[
            ("no-members", empty),
            ("unparseable", "65537"),
            ("empty-name", "65537:"),
            ("duplicate-id", "65537:n1\0:-"),
            ("duplicate-ids", "65537:n1\x0065537:n2"),
            ("duplicate-names", "65537:n1\x0065538:n1"),
            ("own-absent", "65537:n1\x0065538:n2"),
        ];
        for (name, members) in cases {
            assert_eq!(
                Node::open(
                    members,
                    OWN_ONE,
                    &state.to_string_lossy(),
                    None,
                    0,
                    PRIMARY_TIMEOUT_MS,
                )
                .err(),
                Some(CONFIG),
                "the descriptor refuses {name}"
            );
        }
        // The absent `own` by name: ids and names both unique, but `own`
        // names neither member.
        assert_eq!(
            Node::open(
                "65537:n1\x0065538:n2",
                "n3",
                &state.to_string_lossy(),
                None,
                0,
                PRIMARY_TIMEOUT_MS,
            )
            .err(),
            Some(CONFIG),
            "the descriptor refuses an own that names no member"
        );
    }

    /// The descriptor's provisioned-identity law, through the C ABI: a
    /// member id whose system half is zero (no system assigned) or whose
    /// crash half is not the genesis life (not a provisioned identity)
    /// refuses `lunet_lock_node_new` — the marker's counter carries the
    /// life, the descriptor's ids are all counter 1.
    #[test]
    fn abi_refuses_unlawful_descriptor_ids() {
        let dir = scratch("unlawful-ids");
        let state = dir.join("node.state");
        let state_text = state.to_string_lossy().into_owned();
        let journal = "";
        let cases: &[(&str, &str)] = &[
            ("zero-system-half", "1:n1"),
            ("crash-half-not-one", "65539:n1"),
            ("zero-id", "0:n1"),
        ];
        for (name, members) in cases {
            let mut out: *mut c_void = std::ptr::null_mut();
            let code = unsafe {
                lunet_lock_node_new(
                    members.len(),
                    members.as_ptr(),
                    OWN_ONE.len(),
                    OWN_ONE.as_ptr(),
                    state_text.len(),
                    state_text.as_ptr(),
                    journal.len(),
                    journal.as_ptr(),
                    0,
                    &raw mut out,
                )
            };
            assert_eq!(code, CONFIG, "the ABI refuses {name}");
        }
    }

    /// The lock-event journal records the committed transitions in order
    /// (the Hold, the Release), rolls the event file at the configured
    /// threshold, and writes the atomic metafile per rolled file — the
    /// observability replay surface reads every record back.
    #[test]
    fn journal_records_committed_transitions_with_roll_and_meta() {
        let dir = scratch("journal-roll");
        let journal_dir = dir.join("journal");
        // The two-node prod rig: the leader executes the Service and the
        // blocking journal records the transitions. The roll threshold is
        // two records and a bit: the third record rolls the file.
        let mut primary = Node::open(
            MEMBERS_TWO,
            OWN_ONE,
            &dir.join("n1.state").to_string_lossy(),
            Some(&journal_dir.to_string_lossy()),
            128,
            PRIMARY_TIMEOUT_MS,
        )
        .expect("the journaling boot");
        let mut follower = Node::open(
            MEMBERS_TWO,
            "n2",
            &dir.join("n2.state").to_string_lossy(),
            None,
            0,
            PRIMARY_TIMEOUT_MS,
        )
        .expect("the follower boots");
        settle_two(&mut primary, &mut follower);
        // The client conversation: SET (a Hold), RELEASE, SET again (a
        // second Hold) — the reply carries the granted lease.
        let set1 = b"{\"op\":\"set\",\"message_id\":\"00000001-0000-0000-0000-0000000000aa\",\"client_id\":1,\"request_num\":1,\"lock_id\":1,\"lease\":{\"lease_id\":1,\"holder\":\"00000001-0000-0000-0000-0000000000bb\",\"lease_ms\":60000}}";
        let release1 = b"{\"op\":\"release\",\"message_id\":\"00000001-0000-0000-0000-0000000000cc\",\"client_id\":1,\"request_num\":2,\"lock_id\":1,\"holder\":\"00000001-0000-0000-0000-0000000000bb\",\"lease_id\":1}";
        let set2 = b"{\"op\":\"set\",\"message_id\":\"00000001-0000-0000-0000-0000000000dd\",\"client_id\":1,\"request_num\":3,\"lock_id\":2,\"lease\":{\"lease_id\":1,\"holder\":\"00000001-0000-0000-0000-0000000000bb\",\"lease_ms\":60000}}";
        let mut kinds = Vec::new();
        for request in [&set1[..], &release1[..], &set2[..]] {
            let committed_before = primary.frontiers().1;
            assert_eq!(primary.request(request), OK, "the request proposes");
            let mut committed = false;
            for _ in 0..1_000 {
                std::thread::sleep(Duration::from_millis(1));
                primary.idle();
                follower.idle();
                let replies = exchange_all(&mut [&mut primary, &mut follower], 64);
                for (owner, _, bytes) in replies {
                    assert_eq!(owner, primary.own_id(), "the reply queues on the proposer");
                    kinds.push(bytes.len());
                }
                if primary.frontiers().1 > committed_before {
                    committed = true;
                    break;
                }
            }
            assert!(committed, "the request commits inside the bound");
        }
        assert_eq!(kinds.len(), 3, "every request draws its reply: {kinds:?}");
        // The journal's replay: every record parses back, the transition
        // kinds ride in commit order, the roll left the metafile.
        let mut events = Vec::new();
        let mut metas = 0;
        for entry in fs::read_dir(&journal_dir).expect("the journal dir reads") {
            let path = entry.expect("the entry reads").path();
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or("");
            if name.ends_with(".meta") {
                metas += 1;
                let bytes = fs::read(&path).expect("the metafile reads");
                assert_eq!(&bytes[0..4], b"LKM1", "the metafile's magic");
            } else if name.ends_with(".bin") {
                events.extend(journal::parse_file(
                    &fs::read(&path).expect("the file reads"),
                ));
            }
        }
        assert!(metas >= 1, "the roll wrote a metafile");
        let logged: Vec<u8> = events.iter().map(|event| event.kind).collect();
        assert_eq!(
            logged,
            vec![
                journal::KIND_HOLD,
                journal::KIND_RELEASE,
                journal::KIND_HOLD
            ],
            "the transitions replay in commit order: {logged:?}"
        );
        assert_eq!(events[0].lock_id, 1);
        assert_eq!(events[2].lock_id, 2, "the second SET's lock");
    }

    /// The duplicate-request path replays the cached reply without
    /// re-proposing: the second `request` queues exactly one reply output
    /// (identical bytes) and zero send outputs — the Service is never
    /// re-executed and never re-proposed.
    #[test]
    fn duplicate_request_replays_the_cached_reply_without_reproposing() {
        let dir = scratch("duplicate-replay");
        let mut primary = Node::open(
            MEMBERS_TWO,
            OWN_ONE,
            &dir.join("n1.state").to_string_lossy(),
            None,
            0,
            PRIMARY_TIMEOUT_MS,
        )
        .expect("the boot");
        let mut follower = Node::open(
            MEMBERS_TWO,
            "n2",
            &dir.join("n2.state").to_string_lossy(),
            None,
            0,
            PRIMARY_TIMEOUT_MS,
        )
        .expect("the follower boots");
        settle_two(&mut primary, &mut follower);
        let get = b"{\"op\":\"get\",\"message_id\":\"00000001-0000-0000-0000-0000000000aa\",\"client_id\":1,\"request_num\":1,\"lock_id\":1}";
        // The first request: the proposal commits and the correlated
        // reply arrives as a kind-2 output on the proposer.
        assert_eq!(primary.request(get), OK);
        let mut first_reply = None;
        for _ in 0..1_000 {
            std::thread::sleep(Duration::from_millis(1));
            primary.idle();
            follower.idle();
            let replies = exchange_all(&mut [&mut primary, &mut follower], 64);
            for (owner, message_id, bytes) in replies {
                assert_eq!(owner, primary.own_id(), "the reply queues on the proposer");
                if message_id[15] == 0xaa {
                    first_reply = Some(bytes);
                }
            }
            if first_reply.is_some() {
                break;
            }
        }
        let first_reply = first_reply.expect("the reply correlates inside the bound");
        // The duplicate: the cached bytes replay, the state never moves.
        let settled = primary.frontiers();
        assert_eq!(primary.request(get), OK);
        let mut replies = Vec::new();
        for (owner, _, bytes) in exchange_all(&mut [&mut primary, &mut follower], 64) {
            assert_eq!(owner, primary.own_id());
            replies.push(bytes);
        }
        assert_eq!(replies.len(), 1, "exactly one replayed reply");
        assert_eq!(replies[0], first_reply, "the identical bytes replay");
        assert_eq!(
            primary.frontiers(),
            settled,
            "the Service was never re-proposed: no slot moved"
        );
    }

    /// The status snapshot reports the published view: the state word, the
    /// current view's primary (this single node is its own leader), the
    /// era and the view the node serves, and the folded configuration
    /// era beside them.
    #[test]
    fn status_and_leader_report_the_published_view() {
        let dir = scratch("published-view");
        let state = dir.join("node.state");
        let mut node = one_node_compliance(&state);
        settle(&mut node);
        let status = node.status();
        assert_eq!(status.state, 0, "Normal");
        assert_eq!(status.leader, node.own_id(), "the node is its own leader");
        assert_eq!(status.era, 1, "the genesis era");
        assert_eq!(status.config_era, 1, "the folded configuration era");
        assert!(!status.poisoned);
        assert_eq!(status.fault_note, None);
        // The view the node serves is published beside the era: the
        // genesis view's number, before any fence moves it.
        assert_eq!(status.view, 0, "the genesis serve view");
        // The ballot moves and the published view follows.
        assert_eq!(node.force_view(1, 2), OK);
        let mut clock = settle(&mut node);
        let mut advanced = false;
        for _ in 0..40 {
            clock += 1;
            node.set_compliance_clock(clock);
            node.idle();
            while node.next_output().is_some() {}
            if node.status().view == 2 && node.status().state == 0 {
                advanced = true;
                break;
            }
        }
        assert!(advanced, "the forced view installs");
        assert_eq!(
            node.status().view,
            2,
            "the published view follows the ballot"
        );
        assert_eq!(
            node.status().leader,
            node.own_id(),
            "still this node's primary"
        );
    }

    /// The C ABI round trip: `new` builds the node, `status` reads the
    /// published view, `next` drains the queued datagrams (1 when one is
    /// produced, 0 when the queue empties), `free` retires it.
    #[test]
    fn abi_new_status_next_and_free_round_trip() {
        let dir = scratch("abi-roundtrip");
        let state = dir.join("node.state");
        let state_text = state.to_string_lossy().into_owned();
        let members = MEMBERS_ONE;
        let own = OWN_ONE;
        let journal = "";
        let mut out: *mut c_void = std::ptr::null_mut();
        let code = unsafe {
            lunet_lock_node_new(
                members.len(),
                members.as_ptr(),
                own.len(),
                own.as_ptr(),
                state_text.len(),
                state_text.as_ptr(),
                journal.len(),
                journal.as_ptr(),
                0,
                &raw mut out,
            )
        };
        assert_eq!(code, OK, "the ABI builds the node");
        assert!(!out.is_null());
        // The status: the published view at boot.
        let mut status = 0u32;
        let mut leader = 0u32;
        let mut era = 0u32;
        let mut view = 0u32;
        let code = unsafe {
            lunet_lock_node_status(
                out,
                &raw mut status,
                &raw mut leader,
                &raw mut era,
                &raw mut view,
            )
        };
        assert_eq!(code, OK);
        assert_eq!(leader, GENESIS_ID, "the single member is its own primary");
        assert_eq!(era, 1);
        // The next: the boot queued nothing; the fenced-boot drive queues
        // sends; the queue drains to zero.
        let mut drained = 0;
        loop {
            let mut kind = 0u32;
            let mut to = 0u32;
            let mut out_era = 0u32;
            let mut out_view = 0u32;
            let mut slot_hi = 0u32;
            let mut slot_lo = 0u32;
            let mut message_id = [0u8; 16];
            let mut len = 0usize;
            let mut data = [0u8; 1024];
            let code = unsafe {
                lunet_lock_node_next(
                    out,
                    &raw mut kind,
                    &raw mut to,
                    &raw mut out_era,
                    &raw mut out_view,
                    &raw mut slot_hi,
                    &raw mut slot_lo,
                    message_id.as_mut_ptr(),
                    data.len(),
                    &raw mut len,
                    data.as_mut_ptr(),
                )
            };
            if code == 0 {
                break;
            }
            assert_eq!(code, 1, "next reports one output or an empty queue");
            drained += 1;
            assert!(kind == OUTPUT_SEND || kind == OUTPUT_REPLY);
            if drained > 10_000 {
                panic!("the output queue never empties");
            }
        }
        unsafe { lunet_lock_node_free(out) };
    }

    // ------------------------------------------------------------------
    // Invariants (asserted, always) and maybes (test builds crash,
    // release warns and continues). Red/green per the discipline: the
    // maybe tests were run red against the unwired paths before the
    // maybe_invariant! call sites landed.
    // ------------------------------------------------------------------

    /// The two-member compliance descriptor plus a joined-later entry for
    /// the reconfiguration leg.
    const MEMBERS_THREE_J: &str = "65537:n1\x00131073:n2\x00196609:n3:j";

    /// Every output the adapter ever queues carries kind 1 (send) or 2
    /// (reply) — drained across a boot, a stream, a fence, and a
    /// reconfiguration.
    #[test]
    fn output_queue_carries_only_send_and_reply_kinds() {
        let dir = scratch("output-kinds");
        let mut primary = Node::open_compliance(
            MEMBERS_THREE_J,
            OWN_ONE,
            &dir.join("n1.state").to_string_lossy(),
            50,
        )
        .expect("the primary boots");
        let mut follower = Node::open_compliance(
            MEMBERS_THREE_J,
            "n2",
            &dir.join("n2.state").to_string_lossy(),
            50,
        )
        .expect("the follower boots");
        // The boot: no output. The drives that follow queue sends; the
        // drain asserts every kind the queue ever carries.
        assert!(primary.next_output().is_none(), "the boot emits nothing");
        assert!(follower.next_output().is_none(), "the boot emits nothing");
        // One round: tick both under the shared clock, then cascade the
        // wire, recording every output kind along the way.
        let mut kinds: Vec<u32> = Vec::new();
        let round = |nodes: &mut [&mut Node], kinds: &mut Vec<u32>, clock: u64| {
            for node in nodes.iter_mut() {
                node.set_compliance_clock(clock);
                node.idle();
            }
            for _ in 0..64 {
                let mut wire: Vec<(u32, u32, Vec<u8>)> = Vec::new();
                for node in nodes.iter_mut() {
                    while let Some(output) = node.next_output() {
                        kinds.push(output.kind);
                        if output.kind == OUTPUT_SEND {
                            wire.push((node.own_id(), output.to, output.bytes));
                        }
                    }
                }
                if wire.is_empty() {
                    break;
                }
                for (from, to, bytes) in wire {
                    let target = nodes
                        .iter_mut()
                        .find(|node| node.own_id() >> SYSTEM_HALF_SHIFT == to >> SYSTEM_HALF_SHIFT);
                    if let Some(target) = target {
                        target.receive(from, &bytes);
                    }
                }
            }
        };
        // The settle: the election's sends flow.
        let mut clock = 0u64;
        let mut settled = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            if primary.status().state == 0
                && follower.status().state == 0
                && primary.status().leader == primary.own_id()
            {
                settled = true;
                break;
            }
        }
        assert!(settled, "the pair settles");
        // The fence: the follower's forced view drives its evidence flow.
        assert_eq!(follower.force_view(1, 2), OK, "the follower fences");
        let mut fenced = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            if primary.status().view == 2
                && follower.status().view == 2
                && primary.status().state == 0
            {
                fenced = true;
                break;
            }
        }
        assert!(fenced, "the fence installs on both");
        // The stream: the opaque proposals commit through the new view.
        for index in 0..8u64 {
            assert_eq!(
                primary.propose_opaque(
                    OperationId {
                        msb: 0x100 + index,
                        lsb: index
                    },
                    b"the output-kind stream's operation",
                ),
                OK,
                "the stream proposes"
            );
        }
        let mut streamed = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            // The compliance boundary absorbs the applies (the opaque
            // acknowledge): the committed frontier is the stream's proof.
            if primary.frontiers().1 >= 10 && follower.frontiers().1 >= 10 {
                streamed = true;
                break;
            }
        }
        assert!(
            streamed,
            "the stream commits on both: p{:?} f{:?} k{}",
            primary.frontiers(),
            follower.frontiers(),
            kinds.len()
        );
        // The reconfiguration: the join folds a new configuration era.
        assert_eq!(
            primary.reconfigure(RECONFIGURE_JOIN, 196_609, POSITION_APPEND),
            OK,
            "the join drives"
        );
        let mut joined = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            if primary.status().config_era > 1 && primary.status().state == 0 {
                joined = true;
                break;
            }
        }
        assert!(joined, "the join folds");
        // THE INVARIANT: every output the queue ever carried is a send or
        // a reply.
        assert!(
            kinds.len() >= 8,
            "the queue carried outputs: {}",
            kinds.len()
        );
        assert!(
            kinds
                .iter()
                .all(|kind| *kind == OUTPUT_SEND || *kind == OUTPUT_REPLY),
            "every queued output carries kind 1 or 2"
        );
    }

    /// The deployment ruling (docs/src/decisions.md): a cluster never
    /// drops below three VOTING members. At three, a demotion and a
    /// departure both refuse before they drive. The floor is a floor and
    /// not a ceiling: a fourth member joins at weight zero, is promoted to
    /// a fourth voter, and its demotion contracts the cluster back to
    /// three — which drives, folds, and leaves the floor standing.
    #[test]
    fn reconfiguration_never_drops_below_three_voters() {
        let dir = scratch("three-voter-floor");
        const MEMBERS: &str = "65537:n1\x00131073:n2\x00196609:n3";
        const JOINED: u32 = 262_145;
        let mut n1 =
            Node::open_compliance(MEMBERS, "n1", &dir.join("n1.state").to_string_lossy(), 50)
                .expect("n1 boots");
        let mut n2 =
            Node::open_compliance(MEMBERS, "n2", &dir.join("n2.state").to_string_lossy(), 50)
                .expect("n2 boots");
        let mut n3 =
            Node::open_compliance(MEMBERS, "n3", &dir.join("n3.state").to_string_lossy(), 50)
                .expect("n3 boots");
        let mut clock = 0u64;
        let mut settled = false;
        for _ in 0..1_000 {
            clock += 1;
            for node in [&mut n1, &mut n2, &mut n3] {
                node.set_compliance_clock(clock);
                node.idle();
            }
            let _ = exchange_all(&mut [&mut n1, &mut n2, &mut n3], 64);
            if [&n1, &n2, &n3].iter().all(|node| {
                let status = node.status();
                status.state == 0 && status.era == status.config_era
            }) {
                settled = true;
                break;
            }
        }
        assert!(settled, "the three-member cluster settles");
        // The floor. Both departure routes refuse before they drive, so
        // the cluster stays at three voting members.
        assert_eq!(
            drive_on_leader(
                &mut [&mut n1, &mut n2, &mut n3],
                &mut clock,
                RECONFIGURE_DECREMENT,
                131_073,
                0,
            ),
            CONFIG,
            "the demotion of a voter at three is refused"
        );
        assert_eq!(
            drive_on_leader(
                &mut [&mut n1, &mut n2, &mut n3],
                &mut clock,
                RECONFIGURE_LEAVE,
                131_073,
                0,
            ),
            CONFIG,
            "the departure of a voter at three is refused"
        );
        // The contraction. The join folds the fourth row at weight zero;
        // the promotion makes it a fourth voter; the demotion takes the
        // cluster back to three. Each transition waits for its era on all
        // three live members before the next operation drives.
        assert_eq!(
            drive_on_leader(
                &mut [&mut n1, &mut n2, &mut n3],
                &mut clock,
                RECONFIGURE_JOIN,
                JOINED,
                POSITION_APPEND,
            ),
            OK,
            "the join of a fourth member drives"
        );
        assert_eq!(
            drive_on_leader(
                &mut [&mut n1, &mut n2, &mut n3],
                &mut clock,
                RECONFIGURE_INCREMENT,
                JOINED,
                0,
            ),
            OK,
            "the promotion to a fourth voter drives"
        );
        assert_eq!(
            drive_on_leader(
                &mut [&mut n1, &mut n2, &mut n3],
                &mut clock,
                RECONFIGURE_DECREMENT,
                JOINED,
                0,
            ),
            OK,
            "the contraction from four voters back to three drives"
        );
        // And the floor stands again on the contracted cluster.
        assert_eq!(
            drive_on_leader(
                &mut [&mut n1, &mut n2, &mut n3],
                &mut clock,
                RECONFIGURE_DECREMENT,
                131_073,
                0,
            ),
            CONFIG,
            "the floor holds after the contraction"
        );
    }

    /// Ticks are nondecreasing: the clamp holds a wall-clock regression
    /// back to the last tick — the drives keep working across it and the
    /// tick invariant's assertion never fires.
    #[test]
    fn ticks_are_nondecreasing_and_clamped() {
        let dir = scratch("tick-clamp");
        let state = dir.join("node.state");
        let mut node = one_node_compliance(&state);
        // The clock runs forward, then regresses hard. A tick past the
        // regression samples the clamped value: the drive succeeds and
        // the invariant's assert (next >= last) never trips.
        node.set_compliance_clock(10_000);
        assert_eq!(node.idle(), OK);
        node.set_compliance_clock(5);
        assert_eq!(
            node.idle(),
            OK,
            "the regressed clock clamps to the last tick"
        );
        assert_eq!(node.leader_timeout(), OK);
        assert_eq!(node.recover(), OK);
        assert!(!node.status().poisoned, "the clamp kept the drives clean");
        // The clock runs forward again and the drives continue.
        node.set_compliance_clock(10_001);
        assert_eq!(node.idle(), OK);
    }

    /// Poison means poisoned: a poisoned node executes nothing — every
    /// entry reports SERVICE and the queues stay empty. The poison is
    /// planted directly (the same state the drive's fault arms leave) —
    /// a genuine core fault cannot be manufactured through the public
    /// API, and the invariant holds however the poison arrived.
    #[test]
    fn poisoned_node_executes_nothing() {
        let dir = scratch("poisoned");
        let state = dir.join("node.state");
        let mut node = one_node_compliance(&state);
        settle(&mut node);
        node.poisoned = true;
        assert_eq!(node.request(b"{\"op\":\"get\",\"message_id\":\"00000001-0000-0000-0000-0000000000aa\",\"client_id\":1,\"request_num\":1,\"lock_id\":1}"), SERVICE);
        // A garbage datagram is refused at the wire's decode gate before
        // any drive — the poisoned node executes nothing either way.
        assert_eq!(node.receive(GENESIS_ID, &[0, 1, 2]), VRR_MESSAGE);
        assert_eq!(node.idle(), SERVICE);
        assert_eq!(node.leader_timeout(), SERVICE);
        assert_eq!(node.force_view(1, 2), SERVICE);
        assert_eq!(
            node.reconfigure(RECONFIGURE_JOIN, 131_073, POSITION_APPEND),
            SERVICE
        );
        assert_eq!(node.recover(), SERVICE);
        assert!(
            node.next_output().is_none(),
            "the poisoned node queues nothing"
        );
    }

    /// Every output the adapter ever queues carries kind 1 (send) or 2
    /// (reply) — drained across a boot, a stream, a fence, and a
    /// reconfiguration.
    /// The unknown-peer-id maybe: a datagram attributed to a low-band id
    /// outside the descriptor address space crashes a test build (the
    /// maybe fires) and passes silently in release (warn-and-continue).
    /// Red was demonstrated against the unwired `receive` (the call
    /// returned OK under `catch_unwind` in a debug build).
    #[test]
    fn maybe_unknown_low_band_peer_id_fires_in_test_builds() {
        let dir = scratch("unknown-peer");
        let state = dir.join("node.state");
        // The PROD shape: the compliance rules lift the peer gate, so the
        // maybe only exists where the host's own address space rules.
        let mut node = Node::open(
            MEMBERS_ONE,
            OWN_ONE,
            &state.to_string_lossy(),
            None,
            0,
            PRIMARY_TIMEOUT_MS,
        )
        .expect("the boot");
        // The descriptor knows 65537 (system 1) alone: a low-band id (its
        // system half names no member) is the unknown-peer shape.
        let result = catch_unwind(AssertUnwindSafe(|| node.receive(2, &[1, 2, 3])));
        assert!(
            result.is_err(),
            "the maybe fires in test builds: the unknown peer id panics"
        );
        // The node itself survives the caught boundary panic (the test
        // caught it) and keeps its state: no poison was armed by the
        // maybe outside the drive.
        assert!(!node.status().poisoned);
    }

    /// The folded-era regression helper: true exactly when the folded
    /// configuration era moved backwards. Wired as a maybe in `report`.
    #[test]
    fn folded_era_regression_is_detected() {
        // The first observation arms no regression.
        assert!(!folded_era_regressed(None, 1));
        // Same era, forward era: no regression.
        assert!(!folded_era_regressed(Some(1), 1));
        assert!(!folded_era_regressed(Some(1), 2));
        assert!(!folded_era_regressed(Some(2), 5));
        // A move backwards is the regression.
        assert!(folded_era_regressed(Some(2), 1));
        assert!(folded_era_regressed(Some(5), 4));
    }

    /// The recovery boundary executes the configured variant's flush exactly
    /// at the crashed classification: its write lands in the variant's
    /// scratch (variant 1's block file), a re-crash replay reports it again
    /// (the pair is re-decided from the landed round), and a clean continue
    /// never flushes — no first boot, no clean restart.
    #[test]
    fn dirty_boot_executes_the_recovery_flush_clean_continue_does_not() {
        let dir = scratch("recovery-flush");
        let state = dir.join("node.state");
        let flush_scratch = dir.join("flush-scratch");
        let members = MEMBERS_ONE;
        let own = OWN_ONE;
        // The first life: no classification boundary has run — no flush.
        let mut node = Node::open_with_recovery_flush(
            members,
            own,
            &state.to_string_lossy(),
            None,
            0,
            RecoveryFlush::SingleBlock,
            &flush_scratch.to_string_lossy(),
        )
        .expect("the first life boots");
        assert_eq!(node.own_id(), GENESIS_ID);
        assert!(
            !flush_scratch.join("recovery-flush-single.bin").exists(),
            "the first boot is not the recovery boundary: no flush"
        );
        // The clean restart path is armed the same way: settle, stop — the
        // clean continue never flushes.
        for _ in 0..50 {
            std::thread::sleep(Duration::from_millis(1));
            node.idle();
            while node.next_output().is_some() {}
            if node.status().state == 0 {
                break;
            }
        }
        assert_eq!(node.stop(), OK);
        assert!(
            !flush_scratch.join("recovery-flush-single.bin").exists(),
            "the clean continue never flushes"
        );
        let clean = Node::open_with_recovery_flush(
            members,
            own,
            &state.to_string_lossy(),
            None,
            0,
            RecoveryFlush::SingleBlock,
            &flush_scratch.to_string_lossy(),
        )
        .expect("the clean continue boots");
        assert_eq!(clean.own_id(), GENESIS_ID, "the clean continue never bumps");
        assert!(
            !flush_scratch.join("recovery-flush-single.bin").exists(),
            "the clean classification is not the recovery boundary"
        );
        drop(clean);
        // The crash: the running sentinel stands, so the reopen is dirty
        // by construction — the boundary executes the variant's flush.
        let dirty = Node::open_with_recovery_flush(
            members,
            own,
            &state.to_string_lossy(),
            None,
            0,
            RecoveryFlush::SingleBlock,
            &flush_scratch.to_string_lossy(),
        )
        .expect("the crashed boot executes the boundary flush");
        assert_eq!(dirty.own_id(), 65_538, "the crashed classification bumps");
        let flush_file = flush_scratch.join("recovery-flush-single.bin");
        assert!(flush_file.exists(), "the boundary flush wrote the block");
        assert_eq!(
            fs::metadata(&flush_file)
                .expect("the flush file reads")
                .len(),
            4096,
            "variant 1's block is exactly one 4 KiB block"
        );
        // The re-crash replay: dropped dirty again, the pair is re-decided
        // from the landed round and the flush reports again.
        drop(dirty);
        let replay = Node::open_with_recovery_flush(
            members,
            own,
            &state.to_string_lossy(),
            None,
            0,
            RecoveryFlush::SingleBlock,
            &flush_scratch.to_string_lossy(),
        )
        .expect("the re-crash boot re-runs the boundary");
        assert_eq!(
            replay.own_id(),
            65_539,
            "the re-crash derives the next life"
        );
        assert!(flush_file.exists(), "the replay's flush stands");
    }

    /// A full protocol run — boot, stream, fence, join, promote — trips no
    /// maybe and no invariant: the green run the wired paths must survive.
    #[test]
    fn protocol_run_trips_no_maybe() {
        let dir = scratch("protocol-run");
        let mut primary = Node::open_compliance(
            MEMBERS_THREE_J,
            OWN_ONE,
            &dir.join("n1.state").to_string_lossy(),
            50,
        )
        .expect("the primary boots");
        let mut follower = Node::open_compliance(
            MEMBERS_THREE_J,
            "n2",
            &dir.join("n2.state").to_string_lossy(),
            50,
        )
        .expect("the follower boots");
        let mut kinds: Vec<u32> = Vec::new();
        let round = |nodes: &mut [&mut Node], kinds: &mut Vec<u32>, clock: u64| {
            for node in nodes.iter_mut() {
                node.set_compliance_clock(clock);
                node.idle();
            }
            for _ in 0..64 {
                let mut wire: Vec<(u32, u32, Vec<u8>)> = Vec::new();
                for node in nodes.iter_mut() {
                    while let Some(output) = node.next_output() {
                        kinds.push(output.kind);
                        if output.kind == OUTPUT_SEND {
                            wire.push((node.own_id(), output.to, output.bytes));
                        }
                    }
                }
                if wire.is_empty() {
                    break;
                }
                for (from, to, bytes) in wire {
                    let target = nodes
                        .iter_mut()
                        .find(|node| node.own_id() >> SYSTEM_HALF_SHIFT == to >> SYSTEM_HALF_SHIFT);
                    if let Some(target) = target {
                        target.receive(from, &bytes);
                    }
                }
            }
        };
        let mut clock = 0u64;
        let mut settled = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            if primary.status().state == 0 && follower.status().state == 0 {
                settled = true;
                break;
            }
        }
        assert!(settled, "the cluster serves");
        // The stream.
        for index in 0..4u64 {
            assert_eq!(
                primary.propose_opaque(
                    OperationId {
                        msb: 0x200 + index,
                        lsb: index
                    },
                    b"the protocol run's client stream",
                ),
                OK
            );
        }
        let mut streamed = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            if primary.frontiers().1 >= 6 && follower.frontiers().1 >= 6 {
                streamed = true;
                break;
            }
        }
        assert!(
            streamed,
            "the stream commits: p{:?} f{:?}",
            primary.frontiers(),
            follower.frontiers()
        );
        // The fence.
        assert_eq!(follower.force_view(1, 2), OK);
        let mut fenced = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            if primary.status().view == 2 && primary.status().state == 0 {
                fenced = true;
                break;
            }
        }
        assert!(fenced, "the fence installs");
        // The join, then the promote: two more configuration eras fold.
        // A transition is complete only when the view has ENTERED the
        // folded era (the serving era caught the config era) — a
        // reconfiguration driven inside an establishing era refuses.
        assert_eq!(
            primary.reconfigure(RECONFIGURE_JOIN, 196_609, POSITION_APPEND),
            OK
        );
        let mut joined = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut follower], &mut kinds, clock);
            if primary.status().era == primary.status().config_era
                && primary.status().era > 1
                && primary.status().state == 0
            {
                joined = true;
                break;
            }
        }
        assert!(joined, "the join's era establishes");
        // The promote drives on the published leader (NOT_LEADER is the
        // actionable code: re-forward to the named primary — the view the
        // establishing era entered may seat a different member).
        let leader_is_primary = primary.status().leader == primary.own_id();
        let (driver, rider) = if leader_is_primary {
            (&mut primary, &mut follower)
        } else {
            (&mut follower, &mut primary)
        };
        assert_eq!(
            driver.status().leader,
            driver.own_id(),
            "the leader is one of ours"
        );
        assert_eq!(driver.reconfigure(RECONFIGURE_INCREMENT, 196_609, 0), OK);
        let mut promoted = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [driver, rider], &mut kinds, clock);
            if driver.status().era == driver.status().config_era
                && driver.status().era > 2
                && driver.status().state == 0
            {
                promoted = true;
                break;
            }
        }
        assert!(promoted, "the promote's era establishes");
        // THE GREEN RUN: no maybe, no invariant, no arrest anywhere.
        for node in [&primary, &follower] {
            let status = node.status();
            assert!(!status.poisoned, "no member self-arrests");
            assert_eq!(status.fault_note, None, "no fault was recorded");
            assert_eq!(status.state, 0, "every member serves Normal");
        }
        assert!(
            kinds
                .iter()
                .all(|kind| *kind == OUTPUT_SEND || *kind == OUTPUT_REPLY),
            "the run's outputs are all lawful kinds"
        );
    }

    /// A self-arrested node names its fault. The never-repair contract is
    /// unchanged — poison is sticky and every further entry reports
    /// SERVICE — but the arrest is OBSERVABLE: the first fault
    /// observation is recorded once (later observations of the same
    /// sticky fault do not overwrite it), the status reports the
    /// arrest, and the fault ABI returns the recorded reason. On the
    /// run-4 rig (locks2, 2026-09-15) the voters self-arrested one by
    /// one with code -7 and NO reason anywhere — the silent wedge this
    /// test refuses. A genuine core fault cannot be manufactured
    /// through the public API (faults require real breaches — the
    /// rig's own lesson), so the recording seam is driven directly:
    /// the observation path it serves is the drive's plan/publish
    /// fault arms.
    #[test]
    fn a_self_arrested_node_names_its_fault() {
        let dir = scratch("self-arrest");
        let state = dir.join("node.state");
        let mut node = one_node_compliance(&state);
        settle(&mut node);
        // The arrest: the drive's fault arms record the reason, then
        // poison. The seam is driven exactly as the arms drive it.
        node.record_fault("sticky fault: the manufactured breach".to_string());
        node.poisoned = true;
        // The first observation stands: a later observation of the same
        // sticky fault does not overwrite it.
        node.record_fault("a later observation never overwrites".to_string());
        assert_eq!(
            node.fault_note.as_deref(),
            Some("sticky fault: the manufactured breach"),
            "the FIRST observation is the recorded one"
        );
        // The status reports the arrest and the reason.
        let status = node.status();
        assert!(status.poisoned);
        assert_eq!(
            status.fault_note.as_deref(),
            Some("sticky fault: the manufactured breach")
        );
        // The fault ABI returns the recorded reason: the len-then-bytes
        // contract, the TOO_LARGE mirror of next's. Observation only:
        // valid on a poisoned node by design.
        let out = &mut node as *mut Node as *mut c_void;
        let note = b"sticky fault: the manufactured breach";
        unsafe {
            let mut len = 0usize;
            // The length is written before the capacity verdict: capacity
            // zero reports TOO_LARGE and still names the needed size.
            assert_eq!(
                lunet_lock_node_fault(out, std::ptr::null_mut(), 0, &raw mut len),
                TOO_LARGE
            );
            assert_eq!(len, note.len());
            assert_eq!(
                lunet_lock_node_fault(out, std::ptr::null_mut(), len, &raw mut len),
                TOO_LARGE,
                "one short of the note + NUL is TOO_LARGE"
            );
            let mut buffer = vec![0u8; len + 1];
            assert_eq!(
                lunet_lock_node_fault(out, buffer.as_mut_ptr(), len + 1, &raw mut len),
                OK
            );
            assert_eq!(len, note.len());
            assert_eq!(buffer[len], 0, "the NUL terminator");
            buffer.truncate(len);
            assert_eq!(buffer, note);
        }
        // Every further entry reports SERVICE: the poison is sticky.
        assert_eq!(node.idle(), SERVICE);
        assert_eq!(node.recover(), SERVICE);
        assert!(
            node.next_output().is_none(),
            "the arrested node queues nothing"
        );
    }

    /// Mirrors upstream's `tests/reincarnation.rs::reincarnate_backup`
    /// (class B) plus the membership-discard (class E) and learner (class
    /// F) classes, through the adapter ABI: a backup crashed while running
    /// — the durable marker holds the running sentinel, so the restart is
    /// dirty by construction — bumps its identity, announces the
    /// `(old, new)` pair, and the leader drives the forced sequence one
    /// batch per era until the new identity sits at weight 1 in the old
    /// succession position and the old identity is evicted. The
    /// reincarnated node reopens over the deployment's genesis (the honest
    /// Volatile `restart_as`), stays fenced, and acquires nothing — the
    /// learner's streamed catch-up is upstream §10 future work, so the
    /// named drops are the proof of arrival and no lock state is
    /// fabricated.
    #[test]
    fn reincarnation_abi_runs_the_two_era_resurrection() {
        let dir = scratch("reincarnation");
        // The triad: the corpus's shape (the forced walk's commits need
        // the surviving pair's quorum while the crashed member is dead).
        const MEMBERS: &str = "65537:n1\x00131073:n2\x00196609:n3";
        let mut primary = Node::open_compliance(
            MEMBERS,
            OWN_ONE,
            &dir.join("n1.state").to_string_lossy(),
            50,
        )
        .expect("the primary boots");
        let mut second =
            Node::open_compliance(MEMBERS, "n2", &dir.join("n2.state").to_string_lossy(), 50)
                .expect("the second boots");
        let mut third =
            Node::open_compliance(MEMBERS, "n3", &dir.join("n3.state").to_string_lossy(), 50)
                .expect("the third boots");
        let round = |nodes: &mut [&mut Node], clock: u64| {
            for node in nodes.iter_mut() {
                node.set_compliance_clock(clock);
                node.idle();
            }
            for _ in 0..64 {
                let mut wire: Vec<(u32, u32, Vec<u8>)> = Vec::new();
                for node in nodes.iter_mut() {
                    while let Some(output) = node.next_output() {
                        if output.kind == OUTPUT_SEND {
                            wire.push((node.own_id(), output.to, output.bytes));
                        }
                    }
                }
                if wire.is_empty() {
                    break;
                }
                for (from, to, bytes) in wire {
                    let target = nodes
                        .iter_mut()
                        .find(|node| node.own_id() >> SYSTEM_HALF_SHIFT == to >> SYSTEM_HALF_SHIFT);
                    if let Some(target) = target {
                        target.receive(from, &bytes);
                    }
                }
            }
        };
        let mut clock = 0u64;
        let mut settled = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut primary, &mut second, &mut third], clock);
            if primary.status().state == 0
                && second.status().state == 0
                && third.status().state == 0
            {
                settled = true;
                break;
            }
        }
        assert!(settled, "the triad serves");
        // THE CRASH: the third member is dropped without the stop
        // contract — the running sentinel is the durable crash evidence.
        drop(third);
        // The restart is dirty by construction: the bumped identity is
        // decided at the boot gate, durable before any emission.
        let mut risen =
            Node::open_compliance(MEMBERS, "n3", &dir.join("n3.state").to_string_lossy(), 50)
                .expect("the crashed member reopens");
        assert_eq!(risen.own_id(), 196_610, "the strict next life of system 3");
        assert!(
            risen.next_output().is_none(),
            "no emission before the drive"
        );
        // The fenced-boot drive: the §8 re-announce rides it. The first
        // copy names the leader; the second rides the old row (the
        // transport's remap).
        clock += 1;
        risen.set_compliance_clock(clock);
        assert_eq!(risen.recover(), OK, "the announcement drives");
        let mut announced: Vec<(u32, Vec<u8>)> = Vec::new();
        while let Some(output) = risen.next_output() {
            assert_eq!(output.kind, OUTPUT_SEND, "the announcement is a send");
            announced.push((output.to, output.bytes));
        }
        assert!(
            announced.iter().any(|(to, _)| *to == 65_537),
            "the (old, new) pair is announced to the leader"
        );
        let leader_copy = announced
            .iter()
            .find(|(to, _)| *to == 65_537)
            .expect("the leader's copy");
        // `receive` attributes the datagram to the SENDER: the live
        // identity the transport's remap carries.
        assert_eq!(
            primary.receive(196_610, &leader_copy.1),
            OK,
            "the leader takes the ticket"
        );
        // The leader drives the forced sequence: one batch per era, tick
        // driven, with the re-announce cadence carrying the pair until
        // the new life seats.
        let mut walked = false;
        for _ in 0..4_000 {
            clock += 1;
            round(&mut [&mut primary, &mut second, &mut risen], clock);
            if risen.voting_weight() != Some(1) {
                risen.set_compliance_clock(clock);
                risen.recover();
            }
            if let Some((members, weights)) = primary.membership() {
                let ids: Vec<u32> = members.iter().map(|id| id.0).collect();
                let risen_position = ids.iter().position(|id| *id == 196_610);
                if !ids.contains(&196_609)
                    && risen_position == Some(2)
                    && weights.get(2) == Some(&1)
                    && risen.voting_weight() == Some(1)
                {
                    walked = true;
                    break;
                }
            }
        }
        assert!(
            walked,
            "the forced walk seats the new life and evicts the old: p{:?} w{:?}",
            primary.membership(),
            risen.voting_weight()
        );
        // The resurrection is clean: no member self-arrests.
        for node in [&primary, &second, &risen] {
            let status = node.status();
            assert!(!status.poisoned, "no member self-arrests in the walk");
            assert_eq!(status.fault_note, None);
        }
    }

    /// THE run-4 kill#3 shape (locks2, 2026-09-15): a voter triad with a
    /// weight-0 learner joined, long settled in its final era and serving
    /// a client stream, meets its FIRST post-join view change — and then
    /// the ping-pong the first view-change warm-up produces: a second forced view
    /// within moments of the new view's install, then a third. On the rig
    /// the voters then self-arrested one by one (the silent
    /// FAULTED→SERVICE poison): the commit stream died mid-second, the
    /// views churned 15→510 with zero commits, and the wire went silent.
    /// The regression: every forced view installs Normal, no member
    /// self-arrests, and the client stream commits through each new view.
    #[test]
    fn rapid_fences_with_learners_keep_the_voters_serving() {
        let dir = scratch("rapid-fences");
        // The voter triad plus the joined-later learner's entry.
        const MEMBERS: &str = "65537:n1\x00131073:n2\x00196609:n3\x00262145:n4:j";
        let mut n1 = Node::open_compliance(
            MEMBERS,
            OWN_ONE,
            &dir.join("n1.state").to_string_lossy(),
            50,
        )
        .expect("the primary boots");
        let mut n2 =
            Node::open_compliance(MEMBERS, "n2", &dir.join("n2.state").to_string_lossy(), 50)
                .expect("the second boots");
        let mut n3 =
            Node::open_compliance(MEMBERS, "n3", &dir.join("n3.state").to_string_lossy(), 50)
                .expect("the third boots");
        let round = |nodes: &mut [&mut Node], clock: u64| {
            for node in nodes.iter_mut() {
                node.set_compliance_clock(clock);
                node.idle();
            }
            for _ in 0..64 {
                let mut wire: Vec<(u32, u32, Vec<u8>)> = Vec::new();
                for node in nodes.iter_mut() {
                    while let Some(output) = node.next_output() {
                        if output.kind == OUTPUT_SEND {
                            wire.push((node.own_id(), output.to, output.bytes));
                        }
                    }
                }
                if wire.is_empty() {
                    break;
                }
                for (from, to, bytes) in wire {
                    let target = nodes
                        .iter_mut()
                        .find(|node| node.own_id() >> SYSTEM_HALF_SHIFT == to >> SYSTEM_HALF_SHIFT);
                    if let Some(target) = target {
                        target.receive(from, &bytes);
                    }
                }
            }
        };
        let mut clock = 0u64;
        let mut settled = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut n1, &mut n2, &mut n3], clock);
            if n1.status().state == 0 && n2.status().state == 0 && n3.status().state == 0 {
                settled = true;
                break;
            }
        }
        assert!(settled, "the triad serves");
        // The weight-0 learner joins (the run-4 rig's post-join shape):
        // the establishing era completes.
        let leader_id = serving_leader_id(&[&n1, &n2, &n3]);
        assert_eq!(
            member(&mut [&mut n1, &mut n2, &mut n3], leader_id).reconfigure(
                RECONFIGURE_JOIN,
                262_145,
                POSITION_APPEND
            ),
            OK,
            "the join drives on the leader"
        );
        let mut joined = false;
        for _ in 0..1_000 {
            clock += 1;
            round(&mut [&mut n1, &mut n2, &mut n3], clock);
            if triad_established(&n1, &n2, &n3) {
                joined = true;
                break;
            }
        }
        assert!(joined, "the learner's join establishes");
        // The FIRST post-join view change, then the warm-up's
        // ping-pong: three rapid forced views, a client operation through
        // each, one committed per view. The (era, view) balloted target is
        // the leader's own published view, one view ahead.
        for fence in 0..3 {
            let (era, view) = {
                let status = serving_leader(&[&n1, &n2, &n3]).status();
                (status.era, status.view)
            };
            let leader_id = serving_leader_id(&[&n1, &n2, &n3]);
            assert_eq!(
                member(&mut [&mut n1, &mut n2, &mut n3], leader_id).force_view(era, view + 1),
                OK,
                "the fence {fence} drives (era {era}, view {view})"
            );
            let mut installed = false;
            for _ in 0..1_000 {
                clock += 1;
                round(&mut [&mut n1, &mut n2, &mut n3], clock);
                if triad_serving(&n1, &n2, &n3, era, view + 1) {
                    installed = true;
                    break;
                }
            }
            assert!(
                installed,
                "the forced view {} installs Normal on every voter: {:?} {:?} {:?}",
                view + 1,
                n1.status(),
                n2.status(),
                n3.status()
            );
            // The stream commits through the new view.
            let leader_id = serving_leader_id(&[&n1, &n2, &n3]);
            let trio = &mut [&mut n1, &mut n2, &mut n3];
            let leader = member(trio, leader_id);
            let committed_before = leader.frontiers().1;
            assert_eq!(
                leader.propose_opaque(
                    OperationId {
                        msb: 0x300 + u64::try_from(fence).expect("the fence count fits"),
                        lsb: u64::try_from(fence).expect("the fence count fits"),
                    },
                    b"the rapid-fence stream's client operation",
                ),
                OK,
                "the stream proposes through view {}",
                view + 1
            );
            let mut committed = false;
            for _ in 0..1_000 {
                clock += 1;
                let trio = &mut [&mut n1, &mut n2, &mut n3];
                round(trio, clock);
                if member(trio, leader_id).frontiers().1 > committed_before {
                    committed = true;
                    break;
                }
            }
            assert!(
                committed,
                "the client stream commits through view {}",
                view + 1
            );
        }
        // THE REGRESSION: no member self-arrests (the silent
        // FAULTED→SERVICE poison is the defect this test refuses).
        for node in [&n1, &n2, &n3] {
            let status = node.status();
            assert!(!status.poisoned, "no voter self-arrests");
            assert_eq!(status.fault_note, None, "no fault recorded");
            assert_eq!(status.state, 0, "every voter serves Normal");
        }
    }

    /// The current leader's id among the members, by the published view.
    fn serving_leader_id(nodes: &[&Node]) -> u32 {
        nodes
            .iter()
            .find(|node| node.status().leader == node.own_id() && node.status().state == 0)
            .expect("a serving leader exists")
            .own_id()
    }

    /// The current leader among the members, by the published view.
    fn serving_leader<'a>(nodes: &[&'a Node]) -> &'a Node {
        nodes
            .iter()
            .find(|node| node.status().leader == node.own_id() && node.status().state == 0)
            .expect("a serving leader exists")
    }

    /// The member with the named live id.
    fn member<'a>(nodes: &'a mut [&mut Node], id: u32) -> &'a mut Node {
        nodes
            .iter_mut()
            .find(|node| node.own_id() == id)
            .expect("the member is one of ours")
    }

    /// Whether every voter serves Normal in the named view.
    fn triad_serving(a: &Node, b: &Node, c: &Node, era: u32, view: u32) -> bool {
        [a, b, c].iter().all(|node| {
            node.status().state == 0 && node.status().era == era && node.status().view == view
        })
    }

    /// Whether the triad's serving era has caught its folded configuration
    /// era (a transition's establishing era completed).
    fn triad_established(a: &Node, b: &Node, c: &Node) -> bool {
        [a, b, c].iter().all(|node| {
            let status = node.status();
            status.state == 0 && status.era == status.config_era
        })
    }
}
