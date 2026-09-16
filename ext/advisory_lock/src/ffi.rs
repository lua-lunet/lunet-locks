//! Host-side FFI adapter between the LuaJIT host and the uVRR core
//! (vrr-core @ 0fc6380).
//!
//! Concrete core: `Replica<SegmentedLog, WeightedMajority>` running
//! `Stability::Volatile` — nothing is persisted but the boot state file, so
//! a process that died while operating has no same-identity clean restart
//! in this embedder: its restart is DIRTY. A graceful stop is the one
//! same-identity restart (the termination lifecycle below). The restart
//! story is
//! upstream's Crash-Stop-Self-Evict protocol
//! (`src/replica/reincarnation.rs`): the durable state file is the
//! incarnation marker (the four-superblock discipline collapsed to one
//! copy), a dirty boot bumps the identity, the bumped node drives
//! `Input::Reincarnate { old }` — the `Reincarnation(old, new)` entry
//! ticket — and the stable leader computes `forced_steps` idempotently from
//! the committed configuration, proposing each remaining era's batch
//! through the ordinary reconfiguration pipeline, continued tick-driven,
//! until the new identity sits at weight 1 in the old succession position
//! and the old identity is evicted. The reincarnated node reopens over the
//! deployment's genesis (the only true shared history a Volatile process
//! has) and acquires nothing beyond it — the learner's streamed catch-up is
//! upstream §10 future work — so it replays no lock state it did not
//! commit, exactly like the joiner.
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
//! - **Incarnation (the restart story).** The durable state file is the
//!   incarnation marker: one line `<incarnation> <flushed|unflushed>`,
//!   written atomically (fsync+rename+dir-sync). The boot classifies
//!   upstream-style (`SuperblockCopies::classify`): marker `flushed` -> a
//!   clean continue under the same incarnation; `unflushed` (the running
//!   sentinel every operating process leaves behind) -> DIRTY -> the
//!   incarnation bumps and the marker is rewritten `(new, flushed)` — the
//!   bump's commitment — then `(new, unflushed)` as operating begins. A
//!   bumped identity is derived deterministically, without operator
//!   intervention: descriptor ids are incarnation-0 ids in the low band
//!   `[0, 16777214]`, and the k-th incarnation's identity is
//!   `low + k * 2^24` — a unique high-band id that can never alias a
//!   descriptor id (the low band is the whole descriptor space), never
//!   overflow u32, and never reach the reserved LEADER_UNKNOWN value
//!   `u32::MAX` (descriptor id 16777215 is forbidden precisely because
//!   `255 * 2^24 + 16777215 = 4294967295`); the bump refuses at exhaustion
//!   (incarnation 255), mirroring upstream's checked `Incarnation::bump`.
//!   A bumped boot reopens the reincarnation way — a later life over the
//!   deployment's genesis, the honest Volatile equivalent of upstream's
//!   `restart_as` (`reopen` under the new `own`; there is no durable
//!   journal to carry forward) — and drives `Input::Reincarnate { old }`
//!   immediately and on every later fenced-boot drive (the §8 re-announce;
//!   the core self-gates: a member already voting at weight ≥ 1 has
//!   nothing to announce). `lunet_lock_node_own_id` reports the live
//!   identity so the host can compare leaders against it after a bump.
//! - **Termination (the stop story).** The adapter meets the uVRR
//!   termination obligations (uvrr-core v0.6.1,
//!   `docs/uvrr-termination-obligations.md`) host-side, with the core pin
//!   untouched. The lifecycle is the contract's: startup writes the
//!   running sentinel BEFORE the loop starts (every boot path ends
//!   `unflushed`); a graceful stop — [`Node::stop`] and the C ABI's
//!   `lunet_lock_node_stop` — first CLOSES THE WIRE (the mandatory drain
//!   point: the `stopped` flag refuses every further inbound entry —
//!   `request`, `receive`, `idle`, `leader_timeout`, `force_view`,
//!   `recover`, `reconfigure` — before any marker write and before any
//!   task processing, making the in-memory state final), then writes
//!   `stopped` (termination begins), then drains the committed-transition
//!   sink to quiescence (every queued record appended and fsynced — the
//!   durable-state write), and only then writes `flushed` at the drain
//!   point. The write ordering carries the safety argument: a marker at
//!   `stopped` or later vouches for the state beneath it, so boot reads a
//!   partial shutdown (died between the marker writes) as a CONTROLLED
//!   ending, never as a crash. On-disk spelling: the contract spells the
//!   operating state `running`; this adapter keeps the running sentinel's
//!   on-disk word as `unflushed` for compatibility with every existing
//!   rig state file — `flushed` and the new `stopped` spell as the
//!   contract does. Startup classification: `stopped`/`flushed` → clean
//!   continue under the SAME incarnation (no reincarnation), rewritten
//!   `unflushed` before operating; `unflushed` → DIRTY bump (unchanged).
//!   SIGKILL leaves the running sentinel behind and stays the crash
//!   shape. Marker storage: the lifecycle rides the vendored Zig store's
//!   quorum-of-copies superblock construction (four fixed sector-aligned
//!   Aegis-checksummed copies, hash-chained sequence/parent, quorum write
//!   with forced I/O verified at the 3/4 threshold, quorum read resolving
//!   by highest sequence at the 2/4 threshold — the contract §4
//!   construction, reached through the AOF C ABI's marker exports and
//!   linked statically so this cdylib stays self-contained); the item08
//!   single fsynced flag file remains as the compatibility projection and
//!   the conservative fallback (see `marker_store`).
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
use crate::marker_store;
use crate::recovery_flush::{self, FlushOutcome, RecoveryFlush};
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsString, c_void};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, trace, warn};
use vrr::configuration::{EraTable, INIT_SLOT, MAX_MEMBERS, SystemOperation, VOID_SLOT};
use vrr::effects::{Effect, Stability};
use vrr::ids::{Era, NodeId, Operation, OperationId, Slot, Tick, View, ViewId};
use vrr::journal::{Journal, LogEntry, Payload, SegmentedLog};
use vrr::message::{Body, Message};
use vrr::observe::Diagnostic;
use vrr::progress::Status;
use vrr::quorum::{WeightedMajority, construct_pivot};
use vrr::replica::{
    Input, PersistedProgress, Pivot, PlanRefusal, PublishOutcome, PublishRefusal, Replica,
    TimedInput, ViewChangeKnobs,
};
use vrr::wire::{Pack, Unpack, UnpackError};

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
/// convention is proposed upstream in the drafted issue (see
/// `.tmp/delegation/item13-upstream-invariant-issue.md`) and this macro is
/// the reference implementation. It is exported so downstream embedders
/// (e.g. the `lease-sequencer` example) report their host-side maybes under
/// the same convention.
#[macro_export]
macro_rules! maybe_invariant {
    ($($arg:tt)*) => {
        if cfg!(test) || cfg!(debug_assertions) {
            panic!("maybe-invariant violation: {}", format_args!($($arg)*));
        } else {
            ::tracing::warn!($($arg)*);
        }
    };
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

const OUTPUT_SEND: u32 = 1;
const OUTPUT_REPLY: u32 = 2;

/// Host packetization bound (W5: the core owns no size limit). One IPv4/IPv6
/// UDP datagram, matching `transport.tl`.
const MAX_DATAGRAM: usize = 65507;

/// Ticks (milliseconds) of primary silence before a backup fences into the
/// next view. Host policy; correctness never depends on it.
const PRIMARY_TIMEOUT_MS: u64 = 5000;

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

/// The descriptor id band's top: incarnation-0 ids live in
/// `[0, INCARNATION_LOW_MAX]`; the value `INCARNATION_LOW_MAX + 1` is
/// forbidden because at incarnation 255 it would derive the reserved
/// `LEADER_UNKNOWN` value (see the module's Incarnation note).
const INCARNATION_LOW_MAX: u64 = (1u64 << 24) - 2; // 16777214

/// The bump base: the k-th incarnation's identity is
/// `low + k * INCARNATION_BASE`, placing every bumped identity in the
/// high band `[2^24, u32::MAX - 1]`, disjoint from the whole descriptor
/// space by construction.
const INCARNATION_BASE: u64 = 1u64 << 24;

/// The highest incarnation a bump may produce. The next one would overflow
/// the band arithmetic; the bump refuses instead of wrapping a superseded
/// identity into circulation (upstream `Incarnation::bump`,
/// `src/replica/reincarnation.rs:413-417`).
const INCARNATION_MAX: u64 = 255;

/// A superblock copy's marker, upstream-style
/// (`src/replica/reincarnation.rs:423-430`), extended with the uVRR
/// termination lifecycle (`docs/uvrr-termination-obligations.md` §2):
///
/// - `Unflushed` = the running sentinel, left behind by every process
///   that has been operating on volatile state. On-disk spelling note:
///   the contract spells this state `running`; the on-disk word stays
///   `unflushed` so every existing rig state file boots unchanged.
/// - `Stopped` = termination has begun at a graceful stop: the wire was
///   closed before this write, so the state beneath the marker is final.
///   A `stopped` copy mixed with `unflushed` copies (a death between the
///   marker writes) is evidence of a controlled ending, never a crash.
/// - `Flushed` = the durable-state write completed at the drain point.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub(crate) enum Marker {
    Unflushed,
    Stopped,
    Flushed,
}

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
    journal: Option<JournalSink>,
    /// The durable incarnation-marker path, kept so the stop path can
    /// write `stopped` and `flushed` against the boot's own marker file.
    state_path: PathBuf,
    /// The boot's incarnation (0 for a first boot and for a clean
    /// continue): the marker line's first field at stop time.
    incarnation: u64,
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
enum JournalSink {
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
        code
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
                "tag": format!("{:?}", message.header.tag),
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
            warn!(?diagnostic, "peer input dropped with a named diagnostic");
        }
        let snapshot = self.replica.observer().read();
        if self.last_view != Some((snapshot.era, snapshot.view)) {
            debug!(
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
                    let mut disable_blocking = false;
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
                    match self.journal.as_mut() {
                        Some(JournalSink::Blocking(journal)) => {
                            if let Err(e) = journal.append(&event) {
                                eprintln!(
                                    "lunet-advisory-lock: journal append failed ({e}); \
                                     journaling disabled for this process"
                                );
                                disable_blocking = true;
                            }
                        }
                        Some(JournalSink::Aof(writer)) => writer.enqueue(event),
                        None => {}
                    }
                    if disable_blocking {
                        self.journal = None;
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
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            node_from_parts(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                journal_dir.filter(|dir| !dir.is_empty()),
                roll_bytes,
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
            let journal = journal_dir.filter(|dir| !dir.is_empty()).and_then(|dir| {
                match LockJournal::open(Path::new(dir), roll_bytes as u64) {
                    Ok(j) => Some(JournalSink::Blocking(j)),
                    Err(e) => {
                        eprintln!(
                            "lunet-advisory-lock: journal open failed ({e}); \
                         journaling disabled for this process"
                        );
                        None
                    }
                }
            });
            node_from_sink(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                journal,
                Some((variant, PathBuf::from(scratch_dir))),
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
            // The drain point closed the wire: no further task processing
            // (docs/uvrr-termination-obligations.md §1).
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
            // The drain point closed the wire: no further inbound reads
            // (docs/uvrr-termination-obligations.md §1).
            return STOPPED;
        }
        if data.len() > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        // A message attributed to a low-band id outside the descriptor's
        // address space is a maybe: unexpected, not provably impossible (a
        // misconfigured or hostile sender), and survivable — the core drops
        // it by name (Diagnostic::UnknownSender). Bumped (high-band) ids are
        // the reincarnation story's legitimate callers and exempt.
        if !self.known_ids.contains(&from) && from < (1u32 << 24) {
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
                warn!(
                    from,
                    len = data.len(),
                    "undecodable peer datagram discarded"
                );
                return VRR_MESSAGE;
            }
        };
        trace!(
            from,
            len = data.len(),
            era = message.header.view.era.0,
            view = message.header.view.view.0,
            slot = message.header.slot.0,
            tag = ?message.header.tag,
            "datagram in"
        );
        if !valid_message_payloads(&message) {
            warn!(
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

    /// Host-forced view change (§14.2): the phi-accrual detector's
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
            target: ViewId {
                era: Era(era),
                view: View(view),
            },
        })
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
                node = self.replica.own().0,
                old = old.0,
                "fenced-boot drive: re-announcing the reincarnation"
            );
            let result = self.drive(Input::Reincarnate { old });
            if result != OK {
                return result;
            }
        }
        debug!(node = self.replica.own().0, "fenced-boot drive: tick");
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

    /// The graceful stop (the uVRR termination lifecycle,
    /// `docs/uvrr-termination-obligations.md` §1-§2, host-side):
    ///
    /// 1. **The wire closes first** — the mandatory drain point. The
    ///    `stopped` flag is set BEFORE any marker write, so every further
    ///    inbound entry (`request`, `receive`, ticks, admin drives)
    ///    refuses and processes nothing: the in-memory state becomes
    ///    final and nothing arriving later can contradict it. The caller
    ///    may still drain already-queued outputs (outbound flush is the
    ///    desirable obligation and must never delay this one).
    /// 2. **`stopped` is written** as termination begins.
    /// 3. **The durable-state write completes**: the committed-transition
    ///    sink drains to quiescence (the AOF writer's queue appended and
    ///    fsynced; the blocking journal fsynced).
    /// 4. **`flushed` is written** at the drain point — only after the
    ///    durable write completed. A `stopped`-or-later marker vouches
    ///    for the state beneath it, so a death between the writes is a
    ///    partial shutdown that boot reads CLEAN.
    ///
    /// Both marker writes go through the routed storage
    /// (`marker_store::write`): the quorum-of-copies superblock write
    /// (forced I/O, verified at the write quorum) first, then the
    /// compatibility projection.
    ///
    /// Idempotent: a second stop reports OK without rewriting anything.
    /// A failed marker write or drain reports SERVICE and leaves the
    /// marker at `stopped` (still a controlled ending). SIGKILL takes
    /// none of this path: the running sentinel stays behind and the next
    /// boot classifies DIRTY.
    pub fn stop(&mut self) -> i32 {
        if self.stopped {
            return OK;
        }
        // The drain point: the wire closes BEFORE any marker write.
        self.stopped = true;
        info!(
            node = self.replica.own().0,
            "stop: the wire is closed, the in-memory state is final"
        );
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "marker",
            serde_json::json!({
                "what": "stopped written as termination begins",
                "incarnation": self.incarnation,
            }),
        );
        if marker_store::write(&self.state_path, self.incarnation, Marker::Stopped).is_err() {
            eprintln!("lunet-advisory-lock: the stopped marker write failed");
            return SERVICE;
        }
        // The durable-state write, completing BEFORE the flushed marker.
        if let Err(error) = drain_sink(&mut self.journal) {
            eprintln!(
                "lunet-advisory-lock: the stop drain failed ({error}); \
                       the marker stays at stopped"
            );
            #[cfg(feature = "flight-recorder")]
            self.flight_log(
                "stop-drain",
                serde_json::json!({ "what": "the stop drain failed", "error": error.to_string() }),
            );
            return SERVICE;
        }
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "marker",
            serde_json::json!({
                "what": "flushed written at the drain point",
                "incarnation": self.incarnation,
            }),
        );
        if marker_store::write(&self.state_path, self.incarnation, Marker::Flushed).is_err() {
            eprintln!("lunet-advisory-lock: the flushed marker write failed");
            return SERVICE;
        }
        info!(
            node = self.replica.own().0,
            "stop: drained and flushed; the next boot continues under the same incarnation"
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

/// Whether the folded configuration era moved backwards across two
/// observations. The folded era advances exactly at a committed
/// reconfiguration's establishing operation; it never regresses. A
/// regression is a maybe (not provably impossible for a host juggling era
/// snapshots, and survivable), not an assert.
fn folded_era_regressed(previous: Option<u32>, current: u32) -> bool {
    matches!(previous, Some(previous) if current < previous)
}

/// Map a plan refusal onto the ABI error codes. `NotPrimary` is the one a
/// caller can act on (re-forward to the named primary); the rest — the
/// fault, the reconfiguration gates, the outstanding-transition bookkeeping
/// — are internal states the host cannot repair in place.
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

/// The marker file's on-disk line: `<incarnation>
/// <unflushed|stopped|flushed>`.
fn marker_line(incarnation: u64, marker: Marker) -> String {
    let marker_text = match marker {
        Marker::Flushed => "flushed",
        Marker::Stopped => "stopped",
        Marker::Unflushed => "unflushed",
    };
    format!("{incarnation} {marker_text}\n")
}

/// Parses the marker line. Anything else is an unreadable marker: the boot
/// refuses rather than guessing an identity.
pub(crate) fn parse_marker(text: &str) -> Option<(u64, Marker)> {
    let line = text.trim();
    let (incarnation_text, marker_text) = line.split_once(' ')?;
    let incarnation = incarnation_text.parse::<u64>().ok()?;
    if incarnation > INCARNATION_MAX {
        return None;
    }
    let marker = match marker_text {
        "flushed" => Marker::Flushed,
        "stopped" => Marker::Stopped,
        "unflushed" => Marker::Unflushed,
        _ => return None,
    };
    Some((incarnation, marker))
}

pub(crate) fn read_marker(path: &Path) -> std::io::Result<(u64, Marker)> {
    parse_marker(&fs::read_to_string(path)?)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid marker"))
}

/// One durable marker write: fsync+rename+dir-sync (POSIX crash
/// consistency — persist the new directory entry, not just the file's
/// data; Windows no-ops the directory sync, see `sync_dir`).
///
/// The item08 single-file write, retained verbatim: since the lifecycle
/// marker routed through the quorum-of-copies superblock copies, this is
/// the COMPATIBILITY PROJECTION — written only after the authoritative
/// quorum write succeeded (see `marker_store`), and the conservative
/// fallback at boot when the copies predate the routing or lose their
/// quorum.
pub(crate) fn write_marker(path: &Path, incarnation: u64, marker: Marker) -> std::io::Result<()> {
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
        file.write_all(marker_line(incarnation, marker).as_bytes())?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result
}

/// Boot-time marker discipline (upstream `SuperblockCopies::restart`,
/// `src/replica/reincarnation.rs:526-541`, extended with the uVRR
/// termination lifecycle `docs/uvrr-termination-obligations.md` §2-§3):
/// the classification reads the routed storage (`marker_store::current`)
/// — the quorum-of-copies superblock copies when they exist (the
/// authoritative read: the highest-sequence valid quorum, so a torn,
/// rotted, or stale single copy cannot decide the classification), or the
/// item08 single file when the copies predate the routing (legacy
/// migration: the file's state is the truth; the first routed write below
/// seeds the copies). A missing marker is a first boot at incarnation 0,
/// left `unflushed` (the running sentinel — the contract's startup
/// `running`, written before the loop starts). A `stopped` or `flushed`
/// marker is a CLEAN STOP: the previous process reached the drain point
/// (a `flushed` copy mixed with `stopped` copies is the normal mid-flush
/// shape), so the state is final — the boot continues under the SAME
/// incarnation, no reincarnation, rewritten `unflushed` as operating
/// begins. An `unflushed` marker is DIRTY: the previous process cannot be
/// shown to have reached the drain point, the incarnation bumps (refusing
/// at exhaustion), the marker is rewritten `(new, flushed)` — the bump's
/// commitment — and then `(new, unflushed)` as operating begins. On-disk
/// spelling: the running sentinel stays `unflushed` for compatibility
/// with every existing rig state file; the contract's `running` never
/// appears on disk.
///
/// The DIRTY branch is the recovery boundary (the experiment design's §4):
/// when a variant is configured, its forced flush executes right at the
/// classification point — after the bump's commitment, before the
/// reincarnated node rejoins serving — against the caller-provided scratch
/// directory. Variant 0 (diskless) writes nothing. The flush carries fake
/// data only and is never read back; its measured latency is returned so
/// the boot can report it. A flush failure refuses the boot: the boundary
/// is load-bearing for the measurement, and the marker's own fsync just
/// succeeded, so a failure here means the disk is not usable.
fn boot_marker(
    path: &Path,
    recovery: Option<(&RecoveryFlush, &Path)>,
) -> Result<(u64, Option<FlushOutcome>), i32> {
    let (incarnation, marker) = match marker_store::current(path).map_err(|_| CONFIG)? {
        Some(current) => current,
        None => {
            // First boot: create the single-file projection (fsync +
            // dir-sync, the item08 first-boot write, still the file the
            // operators read), then seed the quorum copies with the
            // running sentinel.
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(path)
                .map_err(|_| CONFIG)?;
            (|| {
                file.write_all(marker_line(0, Marker::Unflushed).as_bytes())?;
                file.sync_all()?;
                sync_parent(path)
            })()
            .map_err(|_| CONFIG)?;
            marker_store::write(path, 0, Marker::Unflushed).map_err(|_| CONFIG)?;
            return Ok((0, None));
        }
    };
    match marker {
        // A clean stop (`docs/uvrr-termination-obligations.md`
        // §3): `stopped` and `flushed` copies both show the
        // previous process reached the drain point — the state is
        // final, the node continues under the same incarnation
        // with NO reincarnation, and the running sentinel is
        // rewritten before operating begins.
        Marker::Stopped | Marker::Flushed => {
            marker_store::write(path, incarnation, Marker::Unflushed).map_err(|_| CONFIG)?;
            Ok((incarnation, None))
        }
        Marker::Unflushed => {
            let bumped = incarnation
                .checked_add(1)
                .filter(|next| *next <= INCARNATION_MAX)
                .ok_or(CONFIG)?;
            marker_store::write(path, bumped, Marker::Flushed).map_err(|_| CONFIG)?;
            let outcome = match recovery {
                None | Some((RecoveryFlush::Diskless, _)) => None,
                Some((variant, scratch)) => {
                    let outcome =
                        recovery_flush::execute(scratch, *variant, bumped).map_err(|_| CONFIG)?;
                    Some(outcome)
                }
            };
            marker_store::write(path, bumped, Marker::Unflushed).map_err(|_| CONFIG)?;
            Ok((bumped, outcome))
        }
    }
}

/// The stop path's durable-state write: drain the committed-transition
/// sink to quiescence — every record the node enqueued is appended and
/// fsynced (the AOF writer), or the journal file is fsynced (the blocking
/// journal). A disabled sink (`None`) has nothing to drain.
fn drain_sink(sink: &mut Option<JournalSink>) -> std::io::Result<()> {
    match sink {
        Some(JournalSink::Aof(writer)) => writer.drain(),
        Some(JournalSink::Blocking(journal)) => journal.flush(),
        None => Ok(()),
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
) -> Result<Node, i32> {
    let journal =
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
        );
    node_from_sink(members_data, own_data, state_data, journal, None)
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
    node_from_sink(members_data, own_data, state_data, sink, None)
}

fn node_from_sink(
    members_data: &[u8],
    own_data: &[u8],
    state_data: &[u8],
    journal: Option<JournalSink>,
    recovery: Option<(RecoveryFlush, PathBuf)>,
) -> Result<Node, i32> {
    // Member entries are "<u32-id>:<name>"; a post-genesis (joined)
    // entry is "<u32-id>:<name>:j". The plain-entry order is the
    // descriptor's genesis succession sequence and each id is the
    // member's live NodeId.
    let Some(members) = members_data
        .split(|byte| *byte == 0)
        .map(parse_member_entry)
        .collect::<Option<Vec<_>>>()
    else {
        return Err(CONFIG);
    };
    let Ok(own) = std::str::from_utf8(own_data) else {
        return Err(CONFIG);
    };
    let Ok(state) = std::str::from_utf8(state_data) else {
        return Err(CONFIG);
    };
    if state.is_empty()
        || members.is_empty()
        || members.len() > MAX_MEMBERS as usize
        || members
            .iter()
            .any(|member| member.name.is_empty() || member.id as u64 > INCARNATION_LOW_MAX)
    {
        return Err(CONFIG);
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
        return Err(CONFIG);
    }
    let Some(own_member) = members.iter().find(|member| member.name == own) else {
        return Err(CONFIG);
    };
    // Explicit admin-assigned identity: the descriptor's genesis (plain)
    // ids in buffer order are both the live NodeIds of the founding
    // membership and the genesis succession sequence. Every buffer id
    // is an incarnation-0 id (the low band, validated above), which is
    // what keeps a bumped identity's high band disjoint from it.
    let genesis_order: Vec<NodeId> = members
        .iter()
        .filter(|member| !member.joined)
        .map(|member| NodeId(member.id))
        .collect();
    let knobs = ViewChangeKnobs {
        primary_timeout: PRIMARY_TIMEOUT_MS,
        view_change_budget: EVIDENCE_BUDGET,
    };
    // The durable incarnation marker: first boot 0, a clean continue
    // keeps the incarnation, a dirty boot bumps it (see the module's
    // Incarnation note and `boot_marker`). The dirty branch is the
    // recovery boundary: a configured E2 variant's forced flush executes
    // there, against the caller-provided scratch directory.
    let state_path = PathBuf::from(state);
    let (incarnation, flush_outcome) = match boot_marker(
        &state_path,
        recovery
            .as_ref()
            .map(|(variant, dir)| (variant, dir.as_path())),
    ) {
        Ok(boot) => boot,
        Err(_) => return Err(CONFIG),
    };
    if let Some(outcome) = flush_outcome {
        info!(
            variant = outcome.variant,
            bytes = outcome.bytes_written,
            latency_us = outcome.latency.as_micros() as u64,
            incarnation,
            "recovery-boundary flush executed"
        );
    }
    let own_id = if incarnation == 0 {
        NodeId(own_member.id)
    } else {
        let bumped = match (own_member.id as u64)
            .checked_add(incarnation * INCARNATION_BASE)
            .and_then(|value| u32::try_from(value).ok())
        {
            Some(value) => value,
            None => return Err(CONFIG),
        };
        NodeId(bumped)
    };
    // Invariant (asserted, always): a reincarnated identity never reuses
    // the old id — the bump moves the identity into the high band, disjoint
    // from the whole descriptor space by construction.
    assert!(
        incarnation == 0 || own_id.0 != own_member.id && own_id.0 >= (1u32 << 24),
        "reincarnated identity reuses the old id (old={}, new={})",
        own_member.id,
        own_id.0
    );
    if incarnation > 0 {
        info!(
            old = own_member.id,
            new = own_id.0,
            incarnation,
            "restart: the incarnation bumped, the node is a reincarnated later life"
        );
    }
    info!(
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
    // The superseded identity when this node booted a DIRTY restart: the
    // bumped node re-announces `Reincarnation(old, new)` on every
    // fenced-boot drive and, while it is below voting weight, on the host's
    // §8 re-announce cadence. The old identity is the one the node LAST
    // OPERATED AS — `low + (k-1) * INCARNATION_BASE` for the k-th bump —
    // not the descriptor id: from the second bump on, the descriptor id was
    // already evicted by the previous life's forced walk, so announcing it
    // would name a non-member, the transport's remap could never chain
    // (the previous bumped id is the row the peers still attribute the
    // socket to), and the leader-side `from == new` gate would refuse the
    // announcement forever.
    let reincarnate_from = if incarnation > 0 {
        match (own_member.id as u64)
            .checked_add((incarnation - 1) * INCARNATION_BASE)
            .and_then(|value| u32::try_from(value).ok())
        {
            Some(previous) => Some(NodeId(previous)),
            None => return Err(CONFIG),
        }
    } else {
        None
    };
    // Invariant (asserted, always): a reincarnated identity never reuses
    // the old id — the bump moves the identity one band step past the
    // previous life, disjoint from the whole descriptor space by
    // construction.
    assert!(
        incarnation == 0
            || (own_id.0 != own_member.id
                && own_id.0 >= (1u32 << 24)
                && Some(own_id.0) != reincarnate_from.map(|old| old.0)),
        "reincarnated identity reuses the old id (old={:?}, new={})",
        reincarnate_from.map(|old| old.0),
        own_id.0
    );
    let replica = match (incarnation > 0, own_member.joined) {
        // A bumped boot is a later life over the deployment's genesis —
        // the honest Volatile equivalent of upstream's `restart_as`
        // (`Replica::reopen` under the new own; there is no durable
        // journal to carry forward). The node holds exactly the shared
        // committed root and nothing else, fenced until (if ever) the
        // stream proves currency.
        (true, _) => match joiner_replica(own_id, genesis_order, knobs) {
            Ok(replica) => replica,
            Err(_) => return Err(CONFIG),
        },
        // A post-genesis member boots as a joiner: a later life over the
        // deployment's genesis, fenced until the stream proves currency.
        (false, true) => match joiner_replica(own_id, genesis_order, knobs) {
            Ok(replica) => replica,
            Err(_) => return Err(CONFIG),
        },
        (false, false) => {
            match Replica::provision(
                own_id,
                genesis_order,
                WeightedMajority,
                SegmentedLog::new(),
                Stability::Volatile,
                knobs,
            ) {
                Ok(replica) => replica,
                Err(_) => return Err(CONFIG),
            }
        }
    };
    let mut node = Node {
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
        journal,
        state_path: state_path.clone(),
        incarnation,
        stopped: false,
        #[cfg(feature = "flight-recorder")]
        flight: crate::flight::FlightRecorder::open_from_env(own_id.0),
    };
    // The bumped node's entry ticket (§4): the wire phase always
    // follows the bump. The announcement is emitted at boot; every
    // later fenced-boot drive re-announces (§8) while the node stays
    // fenced. A failure here is a boot failure: the node is destroyed
    // and the error reported, never half-announced.
    if let Some(old) = node.reincarnate_from {
        let result = node.drive(Input::Reincarnate { old });
        if result != OK {
            return Err(result);
        }
    }
    // Clean start otherwise: provision leaves the node fenced
    // `Recovering` with an empty output queue. There is no boot
    // recovery handshake; the host's fenced-boot drive
    // (`lunet_lock_node_recover`, a tick) and the primary's messages
    // bring the node into the protocol.
    Ok(node)
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
            node_from_parts(members_data, own_data, state_data, journal_dir, roll_bytes)
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

/// §14.2 host-forced view change: the phi-accrual detector's conclusion
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
/// no callback, no yield (the item07 invariants). Idempotent.
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
    fn incarnation_marker_is_created_then_bumps_and_continues() {
        let path = state_path("marker");
        assert_eq!(boot_marker(&path, None).expect("first boot").0, 0);
        assert_eq!(fs::read_to_string(&path).unwrap(), "0 unflushed\n");
        // The first boot seeds the quorum copies with the running
        // sentinel.
        assert_eq!(
            marker_store::current(&path)
                .expect("current")
                .expect("the copies carry the sentinel"),
            (0, Marker::Unflushed)
        );
        // A restart over the running sentinel is dirty: the incarnation
        // bumps and the marker is rewritten (new, flushed), then
        // (new, unflushed) as operating begins.
        assert_eq!(boot_marker(&path, None).expect("dirty boot bumps").0, 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), "1 unflushed\n");
        assert_eq!(boot_marker(&path, None).expect("second bump").0, 2);
        assert_eq!(fs::read_to_string(&path).unwrap(), "2 unflushed\n");
        // A clean checkpoint (flushed) continues under the same incarnation:
        // the running sentinel replaces it, the identity never regresses.
        // Hand-writing the single file here simulates the pre-routing
        // projection, so the copies are dropped: this is the legacy
        // migration path the copy-free rig states boot through.
        fs::remove_file(marker_store::superblock_path(&path)).unwrap();
        write_marker(&path, 7, Marker::Flushed).unwrap();
        assert_eq!(boot_marker(&path, None).expect("clean continue").0, 7);
        assert_eq!(fs::read_to_string(&path).unwrap(), "7 unflushed\n");
        // The migration seeded the copies from the single file: the next
        // classification reads them, not the projection.
        assert_eq!(
            marker_store::current(&path)
                .expect("current")
                .expect("seeded"),
            (7, Marker::Unflushed)
        );
        fs::remove_file(&path).unwrap();
        fs::remove_file(marker_store::superblock_path(&path)).unwrap();
    }

    #[test]
    fn incarnation_marker_refuses_malformed_and_exhausted_identities() {
        let path = state_path("marker-bad");
        fs::write(&path, "not a marker\n").unwrap();
        assert_eq!(boot_marker(&path, None), Err(CONFIG));
        fs::write(&path, "999 unflushed\n").unwrap();
        assert_eq!(boot_marker(&path, None), Err(CONFIG));
        fs::write(&path, "3 stale\n").unwrap();
        assert_eq!(boot_marker(&path, None), Err(CONFIG));
        // The bump refuses at exhaustion instead of wrapping a superseded
        // identity into circulation (upstream `Incarnation::bump`,
        // reincarnation.rs:413-417).
        fs::write(&path, format!("{} unflushed\n", INCARNATION_MAX)).unwrap();
        assert_eq!(boot_marker(&path, None), Err(CONFIG));
        fs::write(&path, format!("{} flushed\n", INCARNATION_MAX)).unwrap();
        assert_eq!(
            boot_marker(&path, None)
                .expect("the exhausted identity still continues")
                .0,
            255
        );
        fs::remove_file(path).unwrap();
    }

    /// The recovery boundary executes the configured variant's flush exactly
    /// at the dirty-boot classification: the flush lands between the bump's
    /// commitment and the running sentinel, its latency is reported, and a
    /// clean continue never flushes.
    #[test]
    fn dirty_boot_executes_the_recovery_flush_clean_continue_does_not() {
        use crate::recovery_flush::RecoveryFlush;
        let path = state_path("marker-flush");
        let scratch = state_path("marker-flush-scratch");
        fs::remove_dir_all(&scratch).ok();
        assert_eq!(boot_marker(&path, None).expect("first boot").1, None);
        assert!(
            !scratch.exists(),
            "boot_marker with no variant writes nothing"
        );
        let (incarnation, outcome) =
            boot_marker(&path, Some((&RecoveryFlush::SingleBlock, &scratch)))
                .expect("dirty boot with the flush variant");
        assert_eq!(incarnation, 1);
        let outcome = outcome.expect("the dirty boot reports the flush");
        assert_eq!(outcome.variant, "single");
        assert_eq!(outcome.bytes_written, 4096);
        assert!(outcome.latency.as_nanos() > 0);
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "1 unflushed\n",
            "the marker discipline is unchanged by the flush"
        );
        let (incarnation, outcome) =
            boot_marker(&path, Some((&RecoveryFlush::Diskless, &scratch))).expect("variant 0");
        assert_eq!(incarnation, 2);
        assert_eq!(outcome, None, "variant 0 writes nothing");
        fs::remove_file(path).unwrap();
        fs::remove_dir_all(&scratch).unwrap();
    }

    /// On-disk compatibility: a marker file in the pre-lifecycle spelling
    /// (`<incarnation> unflushed`) boots exactly as before — the DIRTY
    /// bump — with no interpretation change.
    #[test]
    fn existing_unflushed_files_boot_unchanged() {
        let path = state_path("marker-compat");
        fs::write(&path, "0 unflushed\n").unwrap();
        assert_eq!(
            boot_marker(&path, None)
                .expect("unchanged classification")
                .0,
            1,
            "the running sentinel still classifies DIRTY"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "1 unflushed\n");
        fs::remove_file(&path).unwrap();
        fs::remove_file(marker_store::superblock_path(&path)).unwrap();
    }

    /// On-disk compatibility, the clean-stop spelling: a copy-free rig
    /// state whose single file reads `flushed` (the pre-routing boot's
    /// end state) migrates at boot — the classification reads the file,
    /// the first routed write seeds the copies, and the boot continues
    /// under the SAME incarnation.
    #[test]
    fn legacy_flushed_file_migrates_and_continues_clean() {
        let path = state_path("marker-compat-clean");
        fs::write(&path, "4 flushed\n").unwrap();
        assert_eq!(
            boot_marker(&path, None).expect("migrated classification").0,
            4,
            "the flush from the pre-routing era continues clean"
        );
        assert_eq!(fs::read_to_string(&path).unwrap(), "4 unflushed\n");
        // The migration seeded the copies: they now carry the running
        // sentinel, and the projection was rewritten alongside them.
        assert_eq!(
            marker_store::current(&path)
                .expect("current")
                .expect("seeded"),
            (4, Marker::Unflushed)
        );
        fs::remove_file(&path).unwrap();
        fs::remove_file(marker_store::superblock_path(&path)).unwrap();
    }

    /// The contract §4 point of the quorum-of-copies storage: a rotted
    /// copy (an Aegis checksum that no longer verifies) cannot flip a
    /// boot classification — the surviving quorum decides. The lifecycle
    /// reaches its clean-stop end state, one copy's zone is rotted
    /// (garbage over its leading sector), and the next boot still
    /// continues under the same incarnation.
    #[test]
    fn a_rotted_marker_copy_cannot_flip_a_boot_classification() {
        use lunet_locks_aof::marker as marker_ffi;
        let path = state_path("marker-rot");
        let state = path.to_str().expect("state path");
        let superblock = marker_store::superblock_path(&path);
        let members = "10:a\x000:b\x0030:c";
        let mut node = Node::open(members, "a", state, None, 0).expect("first boot");
        assert_eq!(node.stop(), OK, "the stop leaves the copies at flushed");
        drop(node);
        assert_eq!(
            marker_store::current(&path)
                .expect("current")
                .expect("routed"),
            (0, Marker::Flushed)
        );

        let geometry = marker_ffi::geometry().expect("geometry");
        let rot_slot = 2;
        let rot_offset = (geometry.copy_size * rot_slot) as u64;
        {
            use std::io::{Seek, SeekFrom, Write};
            let mut file = fs::OpenOptions::new()
                .write(true)
                .open(&superblock)
                .expect("the copies file");
            file.seek(SeekFrom::Start(rot_offset)).expect("seek slot");
            file.write_all(&[0xA5u8; 4096]).expect("rot the copy");
        }
        let node = Node::open(members, "a", state, None, 0).expect("clean continue");
        assert_eq!(
            node.own_id(),
            10,
            "the rotted copy did not flip the classification: no DIRTY bump"
        );
        assert_eq!(
            marker_store::current(&path)
                .expect("current")
                .expect("routed"),
            (0, Marker::Unflushed),
            "the running sentinel was rewritten as operating begins"
        );
        fs::remove_file(&path).unwrap();
        fs::remove_file(&superblock).unwrap();
    }

    /// The contract's partial-marker-write rule
    /// (`docs/uvrr-termination-obligations.md` §2): a death between the
    /// marker writes leaves `stopped` without `flushed`, and the next
    /// boot reads it CLEAN — a controlled ending, never a crash. RED
    /// before the lifecycle landed: `stopped` did not parse at all.
    #[test]
    fn partial_shutdown_stopped_without_flushed_reads_clean() {
        let path = state_path("marker-partial");
        // An operating process's running sentinel, then the stop's first
        // marker write, then death before the durable flush completed.
        fs::write(&path, "5 unflushed\n").unwrap();
        write_marker(&path, 5, Marker::Stopped).unwrap();
        assert_eq!(
            boot_marker(&path, None)
                .expect("partial shutdown reads clean")
                .0,
            5,
            "same incarnation, no DIRTY bump"
        );
        // The running sentinel is rewritten before operating begins.
        assert_eq!(fs::read_to_string(&path).unwrap(), "5 unflushed\n");
        fs::remove_file(path).unwrap();
    }

    /// The clean-stop lifecycle end to end through `Node::open` and
    /// `Node::stop` (RED before the lifecycle landed: nothing wrote the
    /// stop markers, so every restart was DIRTY): the stopped node's
    /// marker reads clean on the next boot — same incarnation, no bump,
    /// no reincarnation announcement, the running sentinel rewritten as
    /// operating begins.
    #[test]
    fn clean_stop_boot_continues_the_same_incarnation_no_bump() {
        let path = state_path("clean-stop");
        let state = path.to_str().expect("state path");
        let members = "10:a\x000:b\x0030:c";
        let mut node = Node::open(members, "a", state, None, 0).expect("first boot");
        let own_before = node.own_id();
        // The running sentinel the operating process leaves behind.
        assert_eq!(fs::read_to_string(&path).unwrap(), "0 unflushed\n");
        assert_eq!(node.stop(), OK);
        // The stop's end state: `flushed` (the intermediate `stopped`
        // write was atomically replaced at the drain point).
        assert_eq!(fs::read_to_string(&path).unwrap(), "0 flushed\n");
        drop(node);
        let node = Node::open(members, "a", state, None, 0).expect("clean continue");
        assert_eq!(
            node.own_id(),
            own_before,
            "no reincarnation after a clean stop"
        );
        assert_eq!(
            fs::read_to_string(&path).unwrap(),
            "0 unflushed\n",
            "the running sentinel is rewritten before operating begins"
        );
        fs::remove_file(path).unwrap();
    }

    /// The mandatory obligation's proof
    /// (`docs/uvrr-termination-obligations.md` §1): once stopped, NO
    /// further inbound entry is picked up — request, receive, ticks, and
    /// the admin drives all refuse, and the node's state stays exactly as
    /// the drain point left it. RED before the lifecycle landed: there
    /// was no stop and no refusal.
    #[test]
    fn stopped_node_refuses_every_inbound_entry_and_the_state_is_final() {
        let mut nodes = boot_cluster();
        // A committed operation so the state is nontrivial, and the send
        // traffic settles before the drain point.
        assert_eq!(
            request(&mut nodes[0], &request_json(Uuid::from_bytes([61; 16]))),
            OK
        );
        route_until_quiet(&mut nodes, &TEST_IDS);
        let before = nodes[0].status();
        let outputs_before = nodes[0].outputs.len();

        assert_eq!(nodes[0].stop(), OK);
        let datagram = vec![0u8; 64];
        assert_eq!(receive(&mut nodes[0], TEST_IDS[1], &datagram), STOPPED);
        assert_eq!(
            request(&mut nodes[0], &request_json(Uuid::from_bytes([62; 16]))),
            STOPPED
        );
        assert_eq!(nodes[0].idle(), STOPPED);
        assert_eq!(nodes[0].leader_timeout(), STOPPED);
        assert_eq!(nodes[0].recover(), STOPPED);
        assert_eq!(nodes[0].force_view(1, 2), STOPPED);
        assert_eq!(
            nodes[0].reconfigure(RECONFIGURE_INCREMENT, TEST_IDS[1], 0),
            STOPPED
        );

        // The in-memory state is final: identical status, no new outputs
        // queued by any refused entry.
        let after = nodes[0].status();
        assert_eq!(
            (
                after.state,
                after.leader,
                after.era,
                after.view,
                after.config_era
            ),
            (
                before.state,
                before.leader,
                before.era,
                before.view,
                before.config_era
            ),
            "no inbound entry changed the state after the drain point"
        );
        assert_eq!(
            nodes[0].outputs.len(),
            outputs_before,
            "refused entries queue nothing"
        );
        for node in &mut nodes {
            assert_eq!(node.stop(), OK, "stop is part of the lifecycle");
        }
    }

    /// The stop path's write ordering with the async AOF sink (§2): the
    /// `flushed` marker is written only after the writer drained, so
    /// every event enqueued before the stop is durable on disk by the
    /// time the marker lands. RED before the drain existed (no stop, no
    /// marker writes at all).
    #[test]
    fn stop_drains_the_aof_writer_before_the_flushed_marker() {
        let mut nodes = boot_cluster();
        let dir = std::env::temp_dir().join(format!(
            "lunet-advisory-lock-stop-aof-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after Unix epoch")
                .as_nanos()
        ));
        let _ = fs::remove_dir_all(&dir);
        let config = AofConfig {
            flush_interval: None,
            ..AofConfig::default()
        };
        let writer = AofWriter::open(&dir, config).expect("aof opens");
        nodes[0].journal = Some(JournalSink::Aof(writer));
        // A committed Hold transition enqueues its journal event onto the
        // writer thread (try_send — possibly still queued when the stop
        // begins; the drain must make it durable before `flushed` lands).
        assert_eq!(
            request(
                &mut nodes[0],
                &serde_json::to_vec(&crate::locks::Request::Set {
                    message_id: Uuid::from_bytes([63; 16]),
                    client_id: 11,
                    request_num: 13,
                    lock_id: 17,
                    lease: crate::locks::LeaseCandidate {
                        lease_id: 5,
                        holder: Uuid::from_bytes([64; 16]),
                        lease_ms: 500,
                    },
                    name: None,
                    labels: None,
                    sent_at_ms: None,
                })
                .unwrap()
            ),
            OK
        );
        route_until_quiet(&mut nodes, &TEST_IDS);
        assert_eq!(nodes[0].stop(), OK);
        assert_eq!(
            fs::read_to_string(&nodes[0].state_path).unwrap(),
            "0 flushed\n",
            "the marker lands only after the drain"
        );
        // Every file in the AOF series parses; the committed Hold is
        // durable.
        let mut holds = 0usize;
        let mut total = 0usize;
        for entry in fs::read_dir(&dir).expect("aof dir").flatten() {
            let data = fs::read(entry.path()).expect("aof file");
            for event in journal::parse_file(&data) {
                total += 1;
                if event.kind == journal::KIND_HOLD {
                    holds += 1;
                }
            }
        }
        assert!(total > 0, "the drain landed the enqueued events");
        assert!(holds > 0, "the committed Hold transition is durable");
        fs::remove_dir_all(&dir).unwrap();
    }

    /// The sparse admin-assigned member ids every test cluster uses, in
    /// deployment-descriptor (genesis succession) order.
    const TEST_IDS: [u32; 3] = [10, 20, 30];

    fn provision(name: &str, own: u32, members: u32) -> Node {
        provision_at(&state_path(name), own, members)
    }

    /// A provisioned node over an explicit state path, so the reincarnation
    /// test can restart the same durable marker file through the real ABI.
    fn provision_at(path: &Path, own: u32, members: u32) -> Node {
        boot_marker(path, None).expect("marker file");
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
                view_change_budget: EVIDENCE_BUDGET,
            },
        )
        .expect("provision");
        Node {
            replica,
            outputs: VecDeque::new(),
            service: Service::default(),
            replies: HashMap::new(),
            pending: HashMap::new(),
            last_tick: 0,
            poisoned: false,
            fault_note: None,
            reincarnate_from: None,
            known_ids: TEST_IDS.iter().copied().collect(),
            state_path: path.to_path_buf(),
            incarnation: 0,
            stopped: false,
            last_view: None,
            last_leader: None,
            last_config_era: None,
            journal: None,
            #[cfg(feature = "flight-recorder")]
            flight: None,
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

    /// The phi-accrual actuation surface: the host detector that has
    /// concluded the primary is dead drives `Node::force_view` — no timed
    /// tick, no `PRIMARY_TIMEOUT_MS` wait. The dead primary is node 0; the
    /// first backup forces one view past its last known view, the fence
    /// choreography routes to quiescence skipping the dead socket, and the
    /// surviving quorum installs a primary that is not the dead id.
    #[test]
    fn force_view_abi_actuates_the_phi_detection() {
        let mut nodes = boot_cluster();
        for node in nodes.iter_mut() {
            assert_eq!(node.recover(), OK);
        }
        route_until_quiet(&mut nodes, &TEST_IDS);
        let before = nodes[1].replica.observer().read();
        assert_eq!(before.status, 0, "the cluster converged");

        // The primary dies: every subsequent route skips its socket. The
        // backup's detector fires at ~16 ms (item19) — here, zero wait.
        let target = ViewId {
            era: Era(before.era),
            view: View(before.view + 1),
        };
        assert_eq!(nodes[1].force_view(target.era.0, target.view.0), OK);
        route_until_quiet_drop(&mut nodes, &TEST_IDS, TEST_IDS[0]);

        let after = nodes[1].replica.observer().read();
        assert_eq!(
            (after.status, after.era, after.view),
            (0, before.era, before.view + 1),
            "the forced view installed"
        );
        assert_ne!(
            nodes[1].status().leader,
            TEST_IDS[0],
            "the dead id is not the primary"
        );
    }

    /// The joiner member of the four-node tests: id 40, booted the joiner
    /// way — a later life over the deployment's genesis, fenced
    /// `Recovering`, addressed, and outside every configuration until a
    /// committed `Join` admits it.
    fn provision_joiner(name: &str, own: u32, genesis: &[u32]) -> Node {
        boot_marker(&state_path(name), None).expect("marker file");
        let replica = match joiner_replica(
            NodeId(own),
            genesis.iter().map(|id| NodeId(*id)).collect(),
            ViewChangeKnobs {
                primary_timeout: PRIMARY_TIMEOUT_MS,
                view_change_budget: EVIDENCE_BUDGET,
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
            last_tick: 0,
            poisoned: false,
            fault_note: None,
            reincarnate_from: None,
            known_ids: genesis.iter().copied().chain([own]).collect(),
            state_path: state_path(name),
            incarnation: 0,
            stopped: false,
            last_view: None,
            last_leader: None,
            last_config_era: None,
            journal: None,
            #[cfg(feature = "flight-recorder")]
            flight: None,
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
    /// upstream learner: the fence's StartView arrives one era past its
    /// boot table, the §10 learner acquisition serves its fetch and folds
    /// the era that admitted it at the boot fence, and the retained offer
    /// installs on the next ordinary tick — the joiner catches up without
    /// ever voting. The leave completes the non-stop overlap with the
    /// caught-up learner answering the planned solicitation.
    #[test]
    fn reconfigure_abi_joins_a_learner_then_leaves_it_at_zero() {
        let (mut nodes, ids) = boot_four_and_join();
        drive_fence(&mut nodes, &ids, 2);
        let snapshot = nodes[1].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 2, 1));

        // The §10 learner acquisition, through the ABI: the StartView one
        // era past the boot table retains its offer and fetches the
        // missing range under the boot view; the primary serves the fetch
        // (the learner is a member of the primary's current
        // configuration); the boot-fenced acquisition folds the era that
        // admitted it and walks the accepted frontier. The learner stays
        // fenced: it adopted no view and its vote is never counted.
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (
                snapshot.status,
                nodes[3].replica.progress().config().current().era.0,
                snapshot.accepted
            ),
            (2, 2, 3),
            "the joiner folded its admitting era at the boot fence and caught up to the \
             incumbents' frontier"
        );

        // The era-2 stream: the fan-out follows the view's configuration
        // and reaches the weight-0 learner. The learner is still fenced at
        // its boot view, so the stream is the higher-view signal: it
        // fences into the leader's view and opens its own fetch — never
        // installation evidence. An ordinary tick re-runs the retained
        // offer and the install completes the catch-up.
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([31; 16]))),
            OK
        );
        let to_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the learner is in the view configuration's fan-out");
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (1, 2, 1),
            "the learner fenced into the leader's view, never installation evidence"
        );
        assert_eq!(nodes[3].drive(Input::Tick), OK);
        route_until_quiet(&mut nodes, &ids);
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (
                snapshot.status,
                snapshot.era,
                snapshot.view,
                snapshot.committed
            ),
            (0, 2, 1, 4),
            "the retained offer installs: the learner is caught up to the commit cascade, \
             still vote-less"
        );
        nodes[3].outputs.clear();
        // The learner acknowledges the stream — and the primary discards
        // the vote by name: weight 0 counts against no quorum. A fresh
        // proposal commits on the leader's and one voting member's
        // acknowledgment alone; the learner's alone does not clear it.
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([32; 16]))),
            OK
        );
        let to_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the caught-up learner is in the fan-out");
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        let ok_learner = pop_send(&mut nodes[3], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the caught-up learner acknowledges");
        deliver_hop(&mut nodes, &ids, 3, ok_learner);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(4),
            "the learner's vote is not counted"
        );

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
            Slot(5),
            "the learner's weight is not needed"
        );

        // Leave the zero-weight member: the host's pivot policy drives
        // membership changes stop-the-world (`pivot: None`) — the probed
        // non-stop machine for a weight-0 departure is the liveness hole
        // the upstream-issue draft records (its solicited evidence races
        // the proposer's own establishing commit and the one-shot
        // solicitation never re-fires). The establishing Prepare reaches
        // every backup, and the commit advances the era to 3: the
        // departed identity is gone from the folded configuration.
        assert_eq!(reconfigure(&mut nodes[1], RECONFIGURE_LEAVE, 40, 0), OK);
        let prepare = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        let prepare_three = pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        let prepare_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        assert!(
            pop_send(
                &mut nodes[1],
                TEST_IDS[1],
                vrr::wire::Tag::PlannedViewChange
            )
            .is_none(),
            "membership changes solicit nothing: the fallback is stop-the-world"
        );
        deliver_hop(&mut nodes, &ids, 1, prepare);
        deliver_hop(&mut nodes, &ids, 1, prepare_three);
        deliver_hop(&mut nodes, &ids, 1, prepare_learner);
        // One voter's acknowledgment alongside the primary's own vote
        // commits (weights [1,1,1,0], total 3, threshold 2).
        let ok_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok_one);
        route_until_quiet(&mut nodes, &ids);
        assert_eq!(
            nodes[1].replica.progress().config().current().era,
            Era(3),
            "the leave commits era 3"
        );
        // The era the leave established awaits the ordinary view change
        // (§8.7.8); the fence completes the entry.
        assert_eq!(
            nodes[1].replica.observer().read().view,
            1,
            "the era-3 entry awaits the ordinary fence"
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
        // (§8.7.8): the fence completes the entry — the latency outcome
        // the departing-member pivot costs.
        drive_fence(&mut nodes, &ids, 0);
        let snapshot = nodes[2].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 3, 2));
        // The departed learner: it folded the era that departs it (the
        // establishing entry's fold at its own accept made the entry era
        // evaluable), so the era-3 fence reaches it while it is still a
        // member of the establishing configuration — it fences into the
        // era-3 view change and holds there: the era-3 configuration no
        // longer names it, so no StartView is ever addressed to it. Its
        // future messages are foreign.
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (1, 3, 2),
            "the departed identity fences into the era its own fold opened and stays there"
        );
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

    /// The full seven-step join, now completing: the joined learner folds
    /// era 2 at the boot fence (the §10 acquisition), the retained offer
    /// installs it into the leader's view, the committed Increment promotes
    /// it through the non-stop overlap, and the promoted member — caught
    /// up, weight 1 — participates in the promoted arithmetic: with one
    /// voting member silent, its acknowledgment completes the era-3
    /// quorum.
    #[test]
    fn reconfigure_abi_joined_learner_completes_the_seven_step_join() {
        let (mut nodes, ids) = boot_four_and_join();
        drive_fence(&mut nodes, &ids, 2);
        assert_eq!(
            nodes[1].replica.observer().read().view,
            1,
            "member 20 leads era 2"
        );

        // The acquisition: the fence's StartView arrived one era past the
        // boot table, the fetch was served, the admitting era folded, and
        // the ordinary tick installs the retained offer.
        assert_eq!(nodes[3].drive(Input::Tick), OK);
        route_until_quiet(&mut nodes, &ids);
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 2, 1));
        assert_eq!(
            nodes[3].replica.progress().config().current().era,
            Era(2),
            "the joined learner folded the era that admitted it"
        );

        // The promotion on the era-2 primary, through the non-stop
        // overlap: the adapter's derived pivot places the weight-0 learner
        // inside qII, the commit folds era 3, the planned quorum over
        // qI = {L, id 30} completes, and the ONE switch installs v' =
        // (3, 5) with StartView to every member of config(e+1).
        assert_eq!(reconfigure(&mut nodes[1], RECONFIGURE_INCREMENT, 40, 0), OK);
        let to_voter = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the pivot routes the establishing Prepare");
        let to_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the learner is inside qII: it receives the copy");
        assert!(
            pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare).is_none(),
            "never outside qII"
        );
        // The caught-up learner ACCEPTS the establishing copy: the era is
        // evaluable now. Its vote is discarded at the primary — weight 0.
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        let ok_learner = pop_send(&mut nodes[3], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the caught-up learner acknowledges the establishing copy");
        deliver_hop(&mut nodes, &ids, 3, ok_learner);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(3),
            "the learner's vote is not counted while its weight is 0"
        );

        // The commit through qII folds era 3 (weights [1,1,1,1]) and
        // solicits planned evidence from `qI - {L}` = {id 30}; the
        // commit cascade carries the promotion to every member of the
        // view's configuration — the caught-up learner folds the era
        // that promotes it. The planned answer completes the quorum —
        // ONE published transition — and StartView(v') reaches every
        // member of config(e+1).
        deliver_hop(&mut nodes, &ids, 1, to_voter);
        let ok = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the qII member acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok);
        route_until_quiet(&mut nodes, &ids);
        assert_eq!(nodes[1].replica.progress().config().current().era, Era(3));
        let snapshot = nodes[1].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (0, 3, 5),
            "the single switch: v' = (3, 5) selects the primary under the new order"
        );
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (
                snapshot.status,
                snapshot.era,
                snapshot.view,
                snapshot.committed
            ),
            (0, 3, 5, 4),
            "the promoted member installs the promoted view, caught up"
        );

        // The stream continues under the promoted arithmetic: threshold 3
        // under weights [1,1,1,1]. With one voting member's copy
        // undelivered, the promoted member's acknowledgment is the
        // difference: the leader's own plus one incumbent is not a
        // quorum; the promoted member's vote completes it.
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([42; 16]))),
            OK
        );
        let p_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the promoted member is in the fan-out");
        deliver_hop(&mut nodes, &ids, 1, p_learner);
        let ok_learner = pop_send(&mut nodes[3], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the promoted member acknowledges — it evaluates the stream now");
        deliver_hop(&mut nodes, &ids, 3, ok_learner);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(4),
            "the leader and the promoted member are not an era-3 quorum alone"
        );
        let p_one = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the voter's copy");
        deliver_hop(&mut nodes, &ids, 1, p_one);
        let ok_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok_one);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(5),
            "the promoted member's vote completes the era-3 quorum"
        );
        route_until_quiet(&mut nodes, &ids);
        assert!(
            nodes[3]
                .replica
                .progress()
                .config()
                .current()
                .config
                .weight_of(NodeId(40))
                == Some(vrr::configuration::Weight(1)),
            "the promoted member is a full voter in the era it joined"
        );
    }

    /// The four-node cluster with a full voting member: the join
    /// established era 2 at weight 0, the learner folded its admitting era
    /// and caught up, and the committed Increment promoted it through the
    /// non-stop overlap. The state is era 3, weights [1,1,1,1], view (3, 5)
    /// led by member id 20, every machine quiet.
    fn boot_four_join_promote() -> ([Node; 4], [u32; 4]) {
        let (mut nodes, ids) = boot_four_and_join();
        drive_fence(&mut nodes, &ids, 2);
        assert_eq!(nodes[1].replica.observer().read().view, 1);

        // The learner folds its admitting era at the boot fence and the
        // ordinary tick installs the retained offer: it is caught up.
        assert_eq!(nodes[3].drive(Input::Tick), OK);
        route_until_quiet(&mut nodes, &ids);
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!((snapshot.status, snapshot.era, snapshot.view), (0, 2, 1));

        // The promotion through the non-stop overlap: the pivot places the
        // weight-0 learner inside qII, the commit folds era 3, the planned
        // quorum over qI completes, and the ONE switch installs v' = (3, 5).
        assert_eq!(reconfigure(&mut nodes[1], RECONFIGURE_INCREMENT, 40, 0), OK);
        let to_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the learner is inside qII: it receives the copy");
        let to_voter = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the pivot routes the establishing Prepare");
        assert!(
            pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare).is_none(),
            "never outside qII"
        );
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        let ok_learner = pop_send(&mut nodes[3], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the caught-up learner acknowledges the establishing copy");
        deliver_hop(&mut nodes, &ids, 3, ok_learner);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(3),
            "the learner's vote is not counted while its weight is 0"
        );
        deliver_hop(&mut nodes, &ids, 1, to_voter);
        let ok = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the qII member acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok);
        route_until_quiet(&mut nodes, &ids);
        let snapshot = nodes[1].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (0, 3, 5),
            "the single switch installs the promoted view"
        );
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (0, 3, 5),
            "the promoted member folds the era that promotes it"
        );
        // Settle the lagging incumbent: the ordinary tick re-runs the
        // retained offers and every incumbent ends in the promoted view.
        for node in nodes[..3].iter_mut() {
            assert_eq!(node.drive(Input::Tick), OK);
        }
        route_until_quiet(&mut nodes, &ids);
        for node in nodes[..3].iter_mut() {
            let snapshot = node.replica.observer().read();
            assert_eq!(
                (snapshot.status, snapshot.era, snapshot.view),
                (0, 3, 5),
                "every incumbent settles in the promoted view"
            );
        }
        (nodes, ids)
    }

    /// The full voter departure through the ABI — the core's one departure
    /// route as a four-era sequence: the join establishes era 2 (weight 0),
    /// the Increment promotes (era 3, weight 1), the Decrement lowers the
    /// voter back to a learner (era 4, weight 0), and the Leave removes the
    /// weight-0 member (era 5, out of the configuration). Every step is its
    /// own committed era; the operator's sequence is decrement, wait for
    /// the era to commit, then leave.
    #[test]
    fn reconfigure_abi_departs_a_voter_by_decrement_then_leave() {
        let (mut nodes, ids) = boot_four_join_promote();

        // The decrement: the promoted voter is lowered back to weight 0.
        // No legal pivot exists for this geometry — the pivot condition's
        // legs cannot be satisfied when a voting member's weight drops in
        // the four-member universe (qI and qII each need the full
        // threshold, and the two sets share only the leader) — so the
        // adapter drives the stop-the-world fallback: the establishing
        // Prepare reaches every backup, and the era awaits the ordinary
        // fence. A latency outcome, never an error.
        assert_eq!(reconfigure(&mut nodes[1], RECONFIGURE_DECREMENT, 40, 0), OK);
        let to_one = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        let to_three = pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        let to_learner = pop_send(&mut nodes[1], 40, vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        assert!(
            pop_send(
                &mut nodes[1],
                TEST_IDS[1],
                vrr::wire::Tag::PlannedViewChange
            )
            .is_none(),
            "no legal pivot: the fallback solicits nothing"
        );
        deliver_hop(&mut nodes, &ids, 1, to_one);
        deliver_hop(&mut nodes, &ids, 1, to_three);
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        let ok_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        let ok_three = pop_send(&mut nodes[2], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok_one);
        deliver_hop(&mut nodes, &ids, 2, ok_three);
        route_until_quiet(&mut nodes, &ids);
        assert_eq!(
            nodes[1].replica.progress().config().current().era,
            Era(4),
            "the decrement commits era 4"
        );
        assert_eq!(
            nodes[1]
                .replica
                .progress()
                .config()
                .current()
                .config
                .weight_of(NodeId(40)),
            Some(vrr::configuration::Weight(0)),
            "the voter is a learner again in the era the decrement established"
        );
        assert_eq!(
            nodes[1]
                .replica
                .progress()
                .config()
                .current()
                .config
                .weight_of(NodeId(TEST_IDS[0])),
            Some(vrr::configuration::Weight(1)),
            "the incumbents keep their weight"
        );
        // The era the decrement established awaits the ordinary view change
        // (§8.7.8): the fence completes the entry.
        drive_fence(&mut nodes, &ids, 0);
        for node in nodes[..3].iter_mut() {
            let snapshot = node.replica.observer().read();
            assert_eq!(
                (snapshot.status, snapshot.era, snapshot.view),
                (0, 4, 6),
                "the fence completes the era-4 entry at every incumbent"
            );
        }
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (0, 4, 6),
            "the learner folds the era the decrement established and holds the settled view"
        );

        // The leave: the weight-0 member departs. Driven on the era-4
        // primary (view 6 selects id 10 under the voter-only succession:
        // the voters are {10, 20}, view 6 picks the first). The host's
        // pivot policy drives membership changes stop-the-world
        // (`pivot: None`): the establishing Prepare reaches every backup
        // and the commit folds era 5.
        assert_eq!(reconfigure(&mut nodes[0], RECONFIGURE_LEAVE, 40, 0), OK);
        let prepare_one = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        let prepare_three = pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        let prepare_learner = pop_send(&mut nodes[0], 40, vrr::wire::Tag::Prepare)
            .expect("the stop-the-world fallback reaches every backup");
        assert!(
            pop_send(
                &mut nodes[0],
                TEST_IDS[0],
                vrr::wire::Tag::PlannedViewChange
            )
            .is_none(),
            "membership changes solicit nothing: the fallback is stop-the-world"
        );
        deliver_hop(&mut nodes, &ids, 0, prepare_one);
        deliver_hop(&mut nodes, &ids, 0, prepare_three);
        deliver_hop(&mut nodes, &ids, 0, prepare_learner);
        let ok = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the voter acknowledges");
        deliver_hop(&mut nodes, &ids, 1, ok);
        route_until_quiet(&mut nodes, &ids);

        // The commit folds era 5: the departed identity is out of the
        // folded configuration, and the commit cascade no longer reaches
        // it — it stands at its last caught-up frontier.
        assert_eq!(
            nodes[2].replica.progress().config().current().era,
            Era(5),
            "the leave commits era 5"
        );
        assert!(
            nodes[2]
                .replica
                .progress()
                .config()
                .current()
                .config
                .weight_of(NodeId(40))
                .is_none(),
            "the departed identity is out of the configuration"
        );
        assert_eq!(
            nodes[2].replica.progress().committed(),
            Slot(6),
            "the incumbents fold the leave"
        );
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (
                snapshot.status,
                snapshot.era,
                snapshot.view,
                snapshot.committed
            ),
            (0, 4, 6, 6),
            "the departed learner commits the leave that departs it (the commit cascade              reached it before the fan-out excluded it)"
        );
        // The era the leave established awaits the ordinary view change
        // (§8.7.8): the leader's view stands still until the fence.
        assert_eq!(
            nodes[2].replica.observer().read().view,
            6,
            "the era-5 entry awaits the ordinary fence"
        );
        // The era the leave established awaits the ordinary view change
        // (§8.7.8): the fence completes the entry — the latency outcome
        // the departing-member pivot costs.
        drive_fence(&mut nodes, &ids, 0);
        for node in nodes[..3].iter_mut() {
            let snapshot = node.replica.observer().read();
            assert_eq!(
                (snapshot.status, snapshot.era, snapshot.view),
                (0, 5, 7),
                "the fence completes the era-5 entry at every incumbent"
            );
        }
        let snapshot = nodes[3].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (1, 5, 7),
            "the departed identity fences into the era its own accepting fold opened and \
             stays there: the era-5 configuration no longer names it, so no StartView is \
             ever addressed to it and its future messages are foreign"
        );
    }

    /// Refusals keep the log untouched: a non-primary is NOT_LEADER (the one
    /// actionable code), a reconfigure while a transition is outstanding is
    /// SERVICE, a fold-refused operation is SERVICE, and a bad op code is
    /// INVALID.
    #[test]
    fn reconfigure_abi_refusals_never_touch_the_log() {
        let (mut nodes, ids) = boot_four_and_join();

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
        // transition-outstanding gate — internal, SERVICE. The decrement is
        // refused by the same gate.
        let frontier = nodes[0].replica.observer().read().accepted;
        assert_eq!(
            reconfigure(&mut nodes[0], RECONFIGURE_INCREMENT, TEST_IDS[0], 0),
            SERVICE
        );
        assert!(nodes[0].outputs.is_empty());
        assert_eq!(nodes[0].replica.observer().read().accepted, frontier);
        assert_eq!(
            reconfigure(&mut nodes[0], RECONFIGURE_DECREMENT, 40, 0),
            SERVICE,
            "a decrement while a transition is outstanding is internal"
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

        // Enter era 2's own view, then the fold-gate refusals on the
        // primary: a leave of a weight>0 member (the core's NonZeroWeight —
        // the departure route is decrement first), a decrement of a
        // non-member, and a decrement of the weight-0 learner (the core's
        // WeightUnderflow). All internal, all SERVICE, none in the log.
        drive_fence(&mut nodes, &ids, 2);
        let primary = &mut nodes[1];
        let frontier = primary.replica.observer().read().accepted;
        assert_eq!(
            reconfigure(primary, RECONFIGURE_LEAVE, TEST_IDS[0], 0),
            SERVICE,
            "a leave of a voter is fold-refused: the departure route is decrement first"
        );
        assert!(primary.outputs.is_empty());
        assert_eq!(primary.replica.observer().read().accepted, frontier);
        assert_eq!(
            reconfigure(primary, RECONFIGURE_DECREMENT, 99, 0),
            SERVICE,
            "a decrement of a non-member is fold-refused"
        );
        assert!(primary.outputs.is_empty());
        assert_eq!(primary.replica.observer().read().accepted, frontier);
        assert_eq!(
            reconfigure(primary, RECONFIGURE_DECREMENT, 40, 0),
            SERVICE,
            "a decrement of a weight-0 member is fold-refused: a learner has no weight to give"
        );
        assert!(primary.outputs.is_empty());
        assert_eq!(primary.replica.observer().read().accepted, frontier);

        // Bad op codes are invalid arguments. (4 is Decrement now.)
        assert_eq!(reconfigure(&mut nodes[0], 0, 40, 0), INVALID);
        assert_eq!(reconfigure(&mut nodes[0], 5, 40, 0), INVALID);
        assert_eq!(reconfigure(&mut nodes[0], 99, 40, 0), INVALID);
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
                (u64::from(slot_hi) << 32) | u64::from(slot_lo),
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

    /// The bumped node of the reincarnation test: member id 30 restarted
    /// dirty, its incarnation bumped to 1, identity derived into the high
    /// band (`30 + 2^24`).
    const BUMPED_ID: u32 = 30 + (1 << 24);

    /// Routes like `route_until_quiet` but drops sends addressed to `dead`
    /// — the dead old-identity socket, undeliverable exactly as the live
    /// transport drops a datagram whose id has no endpoint row.
    fn route_until_quiet_drop(nodes: &mut [Node], ids: &[u32], dead: u32) {
        loop {
            let mut moved = false;
            for source in 0..nodes.len() {
                let drained: VecDeque<Queued> = std::mem::take(&mut nodes[source].outputs);
                let (sends, kept): (Vec<Queued>, Vec<Queued>) = drained
                    .into_iter()
                    .partition(|output| output.kind == OUTPUT_SEND);
                nodes[source].outputs = kept.into_iter().collect();
                for send in sends {
                    if send.to == dead {
                        continue;
                    }
                    moved = true;
                    deliver_hop(nodes, ids, source, send);
                }
            }
            if !moved {
                return;
            }
        }
    }

    /// Drives a suspicion fence on `driver` and routes to quiescence,
    /// dropping the dead socket's copies.
    fn drive_fence_drop(nodes: &mut [Node], ids: &[u32], driver: usize, dead: u32) {
        let at = nodes[driver].last_tick + PRIMARY_TIMEOUT_MS + 1;
        assert_eq!(nodes[driver].drive_at(at, Input::Tick), OK);
        route_until_quiet_drop(nodes, ids, dead);
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
        let third_path = state_path("reinc-three");
        let mut nodes = vec![
            provision("reinc-one", TEST_IDS[0], 3),
            provision("reinc-two", TEST_IDS[1], 3),
            provision_at(&third_path, TEST_IDS[2], 3),
        ];
        for node in &mut nodes {
            assert_eq!(
                node.replica.progress().status(),
                vrr::progress::Status::Recovering,
                "node boots fenced"
            );
            node.outputs.clear();
        }
        assert_eq!(nodes[0].drive(Input::Tick), OK);
        route_until_quiet(&mut nodes, &TEST_IDS);
        for node in &mut nodes {
            assert_eq!(node.replica.observer().read().status, 0, "node is Normal");
        }

        // The node is RUNNING when the volatile state is lost: a committed
        // operation, then the crash. The marker file holds the running
        // sentinel, so the restart below is dirty by construction (§2).
        let set_payload = serde_json::to_vec(&crate::locks::Request::Set {
            message_id: Uuid::from_bytes([51; 16]),
            client_id: 1,
            request_num: 1,
            lock_id: 9001,
            lease: crate::locks::LeaseCandidate {
                lease_id: 1,
                holder: Uuid::from_bytes([0xBB; 16]),
                lease_ms: 60_000,
            },
            name: None,
            labels: None,
            sent_at_ms: None,
        })
        .unwrap();
        assert_eq!(request(&mut nodes[0], &set_payload), OK);
        route_until_quiet(&mut nodes, &TEST_IDS);
        for node in &mut nodes {
            assert_eq!(node.replica.observer().read().applied, 3);
            node.outputs.clear();
        }
        let crashed = nodes.pop().expect("the third node");
        drop(crashed);

        // The dirty restart runs through the real ABI: the marker bumps
        // (0 -> 1) and the identity is derived into the high band.
        let state_bytes = third_path.as_os_str().as_encoded_bytes();
        let members = b"10:n1\x0020:n2\x0030:n3";
        let mut handle: *mut c_void = ptr::null_mut();
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    members.len(),
                    members.as_ptr(),
                    b"n3".len(),
                    b"n3".as_ptr(),
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
        let mut live_id = 0u32;
        assert_eq!(unsafe { lunet_lock_node_own_id(handle, &mut live_id) }, OK);
        assert_eq!(
            live_id, BUMPED_ID,
            "the bump derives the high-band identity"
        );
        let reincarnated = unsafe { *Box::from_raw(handle.cast::<Node>()) };
        nodes.push(reincarnated);
        let ids = [TEST_IDS[0], TEST_IDS[1], BUMPED_ID];

        // The bumped node reopens over the deployment's genesis and is
        // fenced; its boot announcement (the §4 entry ticket) goes to every
        // member of the configuration it can name.
        assert_eq!(
            nodes[2].replica.progress().status(),
            vrr::progress::Status::Recovering,
            "the reincarnated node stays fenced"
        );
        let drained: VecDeque<Queued> = std::mem::take(&mut nodes[2].outputs);
        let mut announced_to: Vec<u32> = Vec::new();
        for output in &drained {
            assert_eq!(output.kind, OUTPUT_SEND, "the boot announces only");
            let message = Message::unpack_from(&output.bytes).expect("wire round trip");
            assert_eq!(message.header.tag, vrr::wire::Tag::Reincarnation);
            let Body::Reincarnation { old, new } = message.body else {
                panic!("the announcement body")
            };
            assert_eq!(old, NodeId(TEST_IDS[2]), "the superseded identity");
            assert_eq!(new, NodeId(BUMPED_ID), "the derived identity");
            announced_to.push(output.to);
        }
        announced_to.sort_unstable();
        assert_eq!(
            announced_to,
            vec![TEST_IDS[0], TEST_IDS[1], TEST_IDS[2]],
            "cluster-wide: every member of the genesis configuration it can name"
        );
        nodes[2].outputs = drained;
        // The copies to the live members are delivered attributed to the new
        // identity; the copy addressed to the old identity's socket
        // self-delivers through the transport's remap and is refused by
        // name at the fenced node (a non-leader never arms the machine) —
        // here it is asserted present and dropped with the dead socket.
        let announce_one = pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::Reincarnation)
            .expect("announce to the leader");
        deliver_hop(&mut nodes, &ids, 2, announce_one);
        let announce_two = pop_send(&mut nodes[2], TEST_IDS[1], vrr::wire::Tag::Reincarnation)
            .expect("announce to the backup");
        deliver_hop(&mut nodes, &ids, 2, announce_two);
        assert!(
            pop_send(&mut nodes[2], TEST_IDS[2], vrr::wire::Tag::Reincarnation).is_some(),
            "the self-socket copy exists (transport remap; refused by name)"
        );

        // The leader arms the machine and proposes the FIRST forced batch —
        // `[Decrement(old), Join(new)]` — as ONE establishing operation,
        // stop-the-world (the core passes pivot None): the Prepare reaches
        // every backup of the current configuration, the dead old-identity
        // socket included.
        let crossing = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::Prepare)
            .expect("the establishing Prepare");
        assert!(
            pop_send(&mut nodes[0], TEST_IDS[2], vrr::wire::Tag::Prepare).is_some(),
            "the dead old-identity copy (undeliverable)"
        );
        assert_eq!(nodes[0].replica.progress().config().current().era, Era(1));
        let Body::Prepare { entry, .. } = &Message::unpack_from(&crossing.bytes)
            .expect("wire round trip")
            .body
        else {
            panic!("the send is the establishing Prepare")
        };
        let Payload::System(SystemOperation::Batch(ops)) = &entry.payload else {
            panic!("the establishing operation is a Batch")
        };
        assert_eq!(
            ops.as_slice(),
            &[
                SystemOperation::Decrement(NodeId(TEST_IDS[2])),
                SystemOperation::Join {
                    node: NodeId(BUMPED_ID),
                    position: 2,
                },
            ],
            "the crossing batch: the new identity takes the old succession position"
        );
        deliver_hop(&mut nodes, &ids, 0, crossing);
        let ok = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::PrepareOk)
            .expect("the voting backup acknowledges");
        deliver_hop(&mut nodes, &ids, 1, ok);
        assert_eq!(
            nodes[0].replica.progress().config().current().era,
            Era(2),
            "the crossing batch commits era 2"
        );
        // The commit cascade folds era 2 at the incumbent backup (the
        // old-identity copy is undeliverable; the learner's copy arrives
        // and is dropped by name — its genesis table covers era 1 only).
        route_until_quiet_drop(&mut nodes, &ids, TEST_IDS[2]);
        let config = &nodes[0].replica.progress().config().current().config;
        assert_eq!(
            config
                .order()
                .iter()
                .map(|member| member.node.0)
                .collect::<Vec<_>>(),
            vec![10, 20, BUMPED_ID, 30],
            "era 2: the learner joined at weight 0 in the old succession position"
        );
        assert_eq!(
            config
                .order()
                .iter()
                .map(|member| member.weight.0)
                .collect::<Vec<_>>(),
            vec![1, 1, 0, 0]
        );
        assert_eq!(
            nodes[0].replica.observer().read().status,
            0,
            "the leader keeps operating in its view"
        );
        assert!(nodes[2].outputs.is_empty(), "the learner emits nothing");

        // The next forced batch waits for the view change into era 2, which
        // the ordinary suspicion machinery drives (an idle primary fences
        // itself). The fence lands view (2, 1) whose primary is member 20 —
        // leadership rotates; the bumped node re-announces to the stable
        // leader (§8) and the recompute proposes the remaining era.
        drive_fence_drop(&mut nodes, &ids, 0, TEST_IDS[2]);
        let snapshot = nodes[1].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.era, snapshot.view),
            (0, 2, 1),
            "member 20 leads the successor view"
        );
        assert_eq!(nodes[2].recover(), OK, "the fenced drive re-announces");
        let reannounce = pop_send(&mut nodes[2], TEST_IDS[1], vrr::wire::Tag::Reincarnation)
            .expect("the re-announce reaches the new leader");
        deliver_hop(&mut nodes, &ids, 2, reannounce);
        assert!(
            pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::Reincarnation).is_some(),
            "the re-announce reaches the former leader (dropped by name)"
        );
        assert!(
            pop_send(&mut nodes[2], TEST_IDS[2], vrr::wire::Tag::Reincarnation).is_some(),
            "the self-socket copy (refused by name)"
        );
        let promotion = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the leader proposes the remaining era");
        assert!(
            pop_send(&mut nodes[1], BUMPED_ID, vrr::wire::Tag::Prepare).is_some(),
            "the learner receives a copy it cannot evaluate"
        );
        assert!(
            pop_send(&mut nodes[1], TEST_IDS[2], vrr::wire::Tag::Prepare).is_some(),
            "the dead old-identity copy (undeliverable)"
        );
        let Body::Prepare { entry, .. } = &Message::unpack_from(&promotion.bytes)
            .expect("wire round trip")
            .body
        else {
            panic!("the send is the establishing Prepare")
        };
        let Payload::System(SystemOperation::Batch(ops)) = &entry.payload else {
            panic!("the establishing operation is a Batch")
        };
        assert_eq!(
            ops.as_slice(),
            &[
                SystemOperation::Increment(NodeId(BUMPED_ID)),
                SystemOperation::Leave(NodeId(TEST_IDS[2])),
            ],
            "the recompute from the intermediate era: promote, then the zero-weight departure"
        );
        deliver_hop(&mut nodes, &ids, 1, promotion);
        let ok = pop_send(&mut nodes[0], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the voting member acknowledges");
        deliver_hop(&mut nodes, &ids, 0, ok);
        route_until_quiet_drop(&mut nodes, &ids, TEST_IDS[2]);
        assert_eq!(
            nodes[1].replica.progress().config().current().era,
            Era(3),
            "the promotion batch commits era 3"
        );
        let config = &nodes[1].replica.progress().config().current().config;
        assert_eq!(
            config
                .order()
                .iter()
                .map(|member| member.node.0)
                .collect::<Vec<_>>(),
            vec![10, 20, BUMPED_ID],
            "the rejoin: the old identity gone, the new identity at weight 1"
        );
        assert_eq!(
            config
                .order()
                .iter()
                .map(|member| member.weight.0)
                .collect::<Vec<_>>(),
            vec![1, 1, 1]
        );
        assert!(
            config.weight_of(NodeId(TEST_IDS[2])).is_none(),
            "the old identity is out of the configuration"
        );

        // Idempotence (§8): a further announcement recomputes an empty
        // sequence — the machine clears, nothing is proposed.
        assert_eq!(nodes[2].recover(), OK);
        let reannounce_clear = pop_send(&mut nodes[2], TEST_IDS[1], vrr::wire::Tag::Reincarnation)
            .expect("the re-announce");
        deliver_hop(&mut nodes, &ids, 2, reannounce_clear);
        let _ = pop_send(&mut nodes[2], TEST_IDS[0], vrr::wire::Tag::Reincarnation);
        let _ = pop_send(&mut nodes[2], TEST_IDS[2], vrr::wire::Tag::Reincarnation);
        assert!(
            nodes[1].outputs.is_empty(),
            "the complete sequence proposes nothing"
        );

        // Class E: the superseded identity is discarded by name — its
        // messages are foreign once it is evicted.
        let forged = Message {
            header: vrr::wire::Header {
                tag: vrr::wire::Tag::PrepareOk,
                view: nodes[1].replica.progress().current(),
                slot: Slot(4),
            },
            body: Body::PrepareOk {},
        };
        assert_eq!(
            nodes[1].drive(Input::Peer {
                from: NodeId(TEST_IDS[2]),
                message: forged,
            }),
            OK,
            "the discard never faults the drive"
        );
        assert!(
            nodes[1].outputs.is_empty(),
            "the forged vote queued nothing"
        );

        // Class F: the leader streams to the rejoining member. With the §10
        // learner acquisition the stream is no longer a named drop: the
        // era-3 stream arrives past the learner's frontier, the gap ruling
        // fetches the missing range (slot 5's promotion batch) from the
        // primary, and the served chunk folds era 3 at the learner. The
        // rejoining member is a weight-1 member of the era now: its
        // acknowledgment counts, and its applied history is the real
        // committed one.
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([52; 16]))),
            OK
        );
        let to_learner = pop_send(&mut nodes[1], BUMPED_ID, vrr::wire::Tag::Prepare)
            .expect("the learner is in the fan-out");
        deliver_hop(&mut nodes, &ids, 1, to_learner);
        // The gap ruling fetches the missing range; the primary serves it
        // (the rejoining member is a member of its current configuration)
        // and the chunk folds the promotion era. The routing also commits
        // the client operation through the voting members.
        route_until_quiet_drop(&mut nodes, &ids, TEST_IDS[2]);
        let snapshot = nodes[2].replica.observer().read();
        assert_eq!(
            (snapshot.status, snapshot.committed),
            (0, 6),
            "the learner folded the era that promoted it and caught up to the frontier"
        );
        assert_eq!(
            snapshot.applied, 6,
            "the learner applies the real committed history: no lock state fabricated"
        );
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(6),
            "the promotion batch and the client operation are committed"
        );

        // The rejoining member's vote now counts: with one voting member
        // silent, the leader's own and the member's acknowledgment clear
        // the era-3 threshold (2 of total 3).
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([53; 16]))),
            OK
        );
        let to_voter = pop_send(&mut nodes[1], TEST_IDS[0], vrr::wire::Tag::Prepare)
            .expect("the voter's copy (staged, never delivered)");
        drop(to_voter);
        let to_member = pop_send(&mut nodes[1], BUMPED_ID, vrr::wire::Tag::Prepare)
            .expect("the rejoining member is in the fan-out");
        deliver_hop(&mut nodes, &ids, 1, to_member);
        let ok_member = pop_send(&mut nodes[2], TEST_IDS[1], vrr::wire::Tag::PrepareOk)
            .expect("the caught-up member acknowledges");
        deliver_hop(&mut nodes, &ids, 2, ok_member);
        assert_eq!(
            nodes[1].replica.progress().committed(),
            Slot(7),
            "the rejoining member's vote counts under the era-3 arithmetic"
        );
    }

    #[test]
    fn abi_refuses_high_band_descriptor_ids() {
        let state = state_path("abi-high-band");
        let state_bytes = state.as_os_str().as_encoded_bytes();
        let mut handle: *mut c_void = ptr::null_mut();
        // A member id outside the incarnation-0 low band would collide with
        // the bump arithmetic's derived identities; the descriptor parser
        // rejects it and so does the ABI.
        assert_eq!(
            unsafe {
                lunet_lock_node_new(
                    b"10:n1\x0016777215:n2".len(),
                    b"10:n1\x0016777215:n2".as_ptr(),
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
        use crate::locks::{LeaseCandidate, Request};

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
                lease: LeaseCandidate {
                    lease_id,
                    holder,
                    lease_ms: 60_000,
                },
                name: None,
                labels: None,
                sent_at_ms: None,
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

    // ------------------------------------------------------------------
    // Invariants (asserted, always) and maybes (test builds crash,
    // release warns and continues). Red/green per the discipline: the
    // maybe tests were run red against the unwired paths before the
    // maybe_invariant! call sites landed.
    // ------------------------------------------------------------------

    /// The duplicate-request path replays the cached reply without
    /// re-proposing: the second `request` queues exactly one reply output
    /// (identical bytes) and zero send outputs — the Service is never
    /// re-executed and never re-proposed.
    #[test]
    fn duplicate_request_replays_the_cached_reply_without_reproposing() {
        let mut nodes = boot_cluster();
        let json = request_json(Uuid::from_bytes([7; 16]));
        assert_eq!(request(&mut nodes[0], &json), OK);
        route_until_quiet(&mut nodes, &TEST_IDS);
        let mut first = None;
        while let Some(output) = nodes[0].next_output() {
            if output.kind == OUTPUT_REPLY {
                assert!(first.is_none(), "exactly one reply for the request");
                first = Some(output.bytes);
            }
        }
        let first = first.expect("the request's reply is queued");
        // The duplicate: replay, no re-propose.
        assert_eq!(request(&mut nodes[0], &json), OK);
        let mut replies = Vec::new();
        let mut sends = 0;
        while let Some(output) = nodes[0].next_output() {
            if output.kind == OUTPUT_REPLY {
                replies.push(output.bytes);
            } else {
                sends += 1;
            }
        }
        assert_eq!(replies, vec![first], "the cached bytes replay verbatim");
        assert_eq!(sends, 0, "the duplicate proposes nothing");
    }

    /// Every output the adapter ever queues carries kind 1 (send) or 2
    /// (reply) — drained across a boot, a stream, a fence, and a
    /// reconfiguration.
    #[test]
    fn output_queue_carries_only_send_and_reply_kinds() {
        let (mut nodes, ids) = boot_four_and_join();
        assert_eq!(
            request(&mut nodes[0], &request_json(Uuid::from_bytes([9; 16]))),
            OK
        );
        drive_fence(&mut nodes, &ids, 2);
        for node in nodes.iter_mut() {
            let drained = std::mem::take(&mut node.outputs);
            for output in drained {
                assert!(
                    output.kind == OUTPUT_SEND || output.kind == OUTPUT_REPLY,
                    "kind {} in the output queue",
                    output.kind
                );
            }
        }
    }

    /// The dirty restart bumps the identity into the high band: the
    /// reincarnated identity never reuses the old id (the asserted boot
    /// invariant, exercised through `Node::open`).
    #[test]
    fn reincarnated_identity_never_reuses_the_old_id() {
        let path = state_path("reincarnate-invariant");
        let members = TEST_IDS
            .iter()
            .map(|id| format!("{id}:member{id}"))
            .collect::<Vec<_>>()
            .join("\0");
        let first =
            Node::open(&members, "member10", path.to_str().unwrap(), None, 0).expect("first boot");
        assert_eq!(first.own_id(), TEST_IDS[0]);
        drop(first);
        let second = Node::open(&members, "member10", path.to_str().unwrap(), None, 0)
            .expect("dirty boot bumps");
        let bumped = second.own_id();
        assert_ne!(bumped, TEST_IDS[0], "the old id is never reused");
        assert!(bumped >= (1u32 << 24), "the bumped id is high band");
        assert_eq!(bumped, TEST_IDS[0] + (1u32 << 24));
    }

    /// Ticks are nondecreasing: the clamp holds a wall-clock regression
    /// back to the last tick.
    #[test]
    fn ticks_are_nondecreasing_and_clamped() {
        let mut node = provision("ticks", TEST_IDS[0], 3);
        let first = node.tick().expect("clock after epoch");
        let second = node.tick().expect("clock after epoch");
        assert!(second >= first);
        // A future last_tick (a synthetic monotone stamp) holds the clamp:
        // the returned tick never goes below it.
        node.last_tick = first + 10_000;
        assert_eq!(node.tick().unwrap(), first + 10_000);
    }

    /// Poison means poisoned: a poisoned node executes nothing — every
    /// entry reports SERVICE and the queues stay empty.
    #[test]
    fn poisoned_node_executes_nothing() {
        let mut node = provision("poison", TEST_IDS[0], 3);
        node.poisoned = true;
        assert_eq!(node.idle(), SERVICE);
        assert_eq!(node.leader_timeout(), SERVICE);
        assert_eq!(node.recover(), SERVICE);
        assert_eq!(
            request(&mut node, &request_json(Uuid::from_bytes([3; 16]))),
            SERVICE
        );
        assert!(
            node.next_output().is_none(),
            "a poisoned node emits nothing"
        );
        assert!(node.outputs.is_empty());
    }

    /// The unknown-peer-id maybe: a datagram attributed to a low-band id
    /// outside the descriptor address space crashes a test build (the
    /// maybe fires) and passes silently in release (warn-and-continue).
    /// Red was demonstrated against the unwired `receive` (the call
    /// returned OK under `catch_unwind` in a debug build).
    #[test]
    fn maybe_unknown_low_band_peer_id_fires_in_test_builds() {
        let mut node = provision("unknown-peer", TEST_IDS[0], 3);
        let junk = vec![0u8; 8];
        let outcome = std::panic::catch_unwind(AssertUnwindSafe(|| node.receive(99_999, &junk)));
        if cfg!(test) || cfg!(debug_assertions) {
            let error = outcome.expect_err("the maybe crashes a test build");
            let message = error
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| error.downcast_ref::<&str>().map(|s| s.to_string()))
                .unwrap_or_default();
            assert!(
                message.contains("maybe-invariant violation"),
                "the panic names the maybe: {message}"
            );
        } else {
            outcome.expect("release warns and continues");
        }
    }

    /// The folded-era regression helper: true exactly when the folded
    /// configuration era moved backwards. Wired as a maybe in `report`.
    #[test]
    fn folded_era_regression_is_detected() {
        assert!(!folded_era_regressed(None, 1));
        assert!(!folded_era_regressed(Some(1), 1));
        assert!(!folded_era_regressed(Some(1), 2));
        assert!(folded_era_regressed(Some(2), 1));
    }

    /// A full protocol run — boot, stream, fence, join, promote — trips no
    /// maybe and no invariant: the green run the wired paths must survive.
    #[test]
    fn protocol_run_trips_no_maybe() {
        let (mut nodes, ids) = boot_four_and_join();
        drive_fence(&mut nodes, &ids, 2);
        assert_eq!(
            request(&mut nodes[1], &request_json(Uuid::from_bytes([11; 16]))),
            OK
        );
        route_until_quiet(&mut nodes, &ids);
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
        let mut node = provision("fault-report", TEST_IDS[0], 3);
        // The first fault observation is recorded and exported.
        node.record_fault("sticky fault: IllegalTransition".to_string());
        assert_eq!(
            node.status().fault_note.as_deref(),
            Some("sticky fault: IllegalTransition"),
            "the first observation is the recorded reason"
        );
        // The sticky fault repeats on every drive; only the first
        // observation is kept.
        node.record_fault("sticky fault: LaterBreach".to_string());
        assert_eq!(
            node.status().fault_note.as_deref(),
            Some("sticky fault: IllegalTransition"),
            "a later observation of the sticky fault never overwrites the first"
        );
        // The arrest itself: poison is sticky, every entry reports
        // SERVICE, and the status says so.
        node.poisoned = true;
        assert_eq!(node.idle(), SERVICE);
        let status = node.status();
        assert!(status.poisoned, "the status reports the self-arrest");
        assert_eq!(
            status.fault_note.as_deref(),
            Some("sticky fault: IllegalTransition")
        );
        // The ABI report — the runbook's live probe, valid on a
        // poisoned node: a short buffer reports TOO_LARGE with the
        // needed size, a fitting one receives the NUL-terminated reason.
        let note = "sticky fault: IllegalTransition";
        let mut short = [0u8; 4];
        let mut len = 0usize;
        assert_eq!(
            unsafe {
                lunet_lock_node_fault(
                    (&raw mut node).cast(),
                    short.as_mut_ptr(),
                    short.len(),
                    &mut len,
                )
            },
            TOO_LARGE
        );
        assert_eq!(len, note.len(), "the needed size is reported");
        let mut buf = vec![0u8; len + 1];
        assert_eq!(
            unsafe {
                lunet_lock_node_fault(
                    (&raw mut node).cast(),
                    buf.as_mut_ptr(),
                    buf.len(),
                    &mut len,
                )
            },
            OK
        );
        assert_eq!(buf[len], 0, "the note is NUL-terminated");
        assert_eq!(
            &buf[..len],
            note.as_bytes(),
            "the ABI reports the recorded reason"
        );
        // A node that has not arrested reports empty.
        let mut fresh = provision("fault-report-clean", TEST_IDS[0], 3);
        let mut len = 0usize;
        assert_eq!(
            unsafe {
                lunet_lock_node_fault((&raw mut fresh).cast(), std::ptr::null_mut(), 0, &mut len)
            },
            OK
        );
        assert_eq!(len, 0, "a serving node reports no fault");
    }

    /// One client op committed through the named leader: propose, route to
    /// quiescence, and require the committed frontier to advance.
    fn commit_client_op(nodes: &mut [Node], ids: &[u32], leader: usize) {
        static OP: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let op = OP.fetch_add(1, std::sync::atomic::Ordering::Relaxed) as u8;
        let before = nodes[leader].replica.progress().committed();
        assert_eq!(
            request(
                &mut nodes[leader],
                &request_json(Uuid::from_bytes([op; 16]))
            ),
            OK,
            "the serving leader accepts the op"
        );
        route_until_quiet(nodes, ids);
        let after = nodes[leader].replica.progress().committed();
        assert!(
            after > before,
            "the op committed through view {}: {before:?} -> {after:?}",
            nodes[leader].replica.observer().read().view
        );
    }

    /// The health bar every voter must clear after any fence: Normal
    /// status, an unpoisoned request path (the leader accepts, a backup
    /// refuses NOT_LEADER — a self-arrested node reports SERVICE), and a
    /// committed frontier that still advances.
    fn assert_cluster_serving(nodes: &mut [Node], ids: &[u32]) -> usize {
        let mut leader = None;
        for (index, node) in nodes.iter().enumerate() {
            let snapshot = node.status();
            assert_eq!(
                snapshot.state, 0,
                "member {index} must be Normal after the fence, got status {} era {} view {}",
                snapshot.state, snapshot.era, snapshot.view
            );
            if snapshot.leader == node.replica.own().0 {
                leader = Some(index);
            }
        }
        for (index, node) in nodes.iter_mut().enumerate() {
            let rc = request(node, &request_json(Uuid::from_bytes([7; 16])));
            assert!(
                rc == OK || rc == NOT_LEADER,
                "member {index} is serving after the fence: rc={rc} \
                 (SERVICE/FAULTED is the silent self-arrest the run-4 rig died of)"
            );
        }
        let leader = leader.expect("a Normal member leads the installed view");
        route_until_quiet(nodes, ids);
        commit_client_op(nodes, ids, leader);
        leader
    }

    /// The phi actuation against a LIVE leader: a backup concludes the
    /// primary is dead and forces the next view — the run-4 rig's first
    /// fence of a long-settled cluster (the leader was never dead, only
    /// suspected).
    fn phi_fence_live_leader(nodes: &mut [Node], ids: &[u32], suspector: usize) {
        let before = nodes[suspector].status();
        let target = (before.era, before.view + 1);
        assert_eq!(nodes[suspector].force_view(target.0, target.1), OK);
        route_until_quiet(nodes, ids);
        let after = nodes[suspector].status();
        assert_eq!(
            (after.state, after.view),
            (0, target.1),
            "the forced view installed"
        );
    }

    /// THE run-4 kill#3 shape (locks2, 2026-09-15): a voter triad with a
    /// weight-0 learner joined, long settled in its final era and serving
    /// a client stream, meets its FIRST post-join view change — and then
    /// the ping-pong the phi warm-up produces: a second forced view
    /// within moments of the new view's install, then a third. On the rig
    /// the voters then self-arrested one by one (the silent
    /// FAULTED→SERVICE poison): the commit stream died mid-second, the
    /// views churned 15→510 with zero commits, and the wire went silent.
    /// The regression: every forced view installs Normal, no member
    /// self-arrests, and the client stream commits through each new view.
    #[test]
    fn rapid_fences_with_learners_keep_the_voters_serving() {
        let (mut nodes, ids) = boot_four_and_join();
        // The era-completion fence: the cluster settles at era 2, view 1,
        // with the caught-up weight-0 learner in the fan-out.
        drive_fence(&mut nodes, &ids, 2);
        assert_eq!(nodes[1].replica.observer().read().status, 0);

        // The long-settled stream: committed client ops through the
        // incumbent leader.
        for _ in 0..20 {
            commit_client_op(&mut nodes, &ids, 1);
        }

        // The first post-join fence of a healthy cluster: the live
        // incumbent is suspected (never dead).
        phi_fence_live_leader(&mut nodes, &ids, 0);
        let leader = assert_cluster_serving(&mut nodes, &ids);
        // The settled stream continues under the new view.
        for _ in 0..5 {
            commit_client_op(&mut nodes, &ids, leader);
        }

        // The ping-pong: the freshly installed leader is suspected within
        // moments — twice.
        phi_fence_live_leader(&mut nodes, &ids, 1);
        let leader = assert_cluster_serving(&mut nodes, &ids);
        for _ in 0..5 {
            commit_client_op(&mut nodes, &ids, leader);
        }
        phi_fence_live_leader(&mut nodes, &ids, 2);
        let leader = assert_cluster_serving(&mut nodes, &ids);
        for _ in 0..5 {
            commit_client_op(&mut nodes, &ids, leader);
        }

        // Every member agrees on the committed history and stays caught
        // up — voters and the weight-0 learner alike.
        let frontiers: Vec<_> = nodes
            .iter()
            .map(|node| node.replica.progress().committed())
            .collect();
        assert!(
            frontiers.iter().all(|slot| *slot == frontiers[0]),
            "the commit cascade reached every member: {frontiers:?}"
        );
    }
}
