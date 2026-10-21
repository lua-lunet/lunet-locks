//! Host-side FFI adapter between the LuaJIT host and the uVRR core
//! (`Replica<SegmentedLog, WeightedMajority>` under `Stability::Volatile`).
//!
//! The boundary's law: a panic never crosses into C — every ABI entry catches
//! a panic at the boundary and aborts the process (the let-it-crash law).

use crate::aof::{AofConfig, AofWriter};
use crate::disk::{Disk, StdDisk, std_disk};
use crate::journal::{self, Journal as LockJournal, JournalEvent};
use crate::locks::{Service, Transition};
use crate::recovery_flush::{self, FlushOutcome, RecoveryFlush};
use crate::spi::{CommitHook, NoOpHook};
use crate::state::{self, FileStateStore, StateStore};
#[cfg(any(test, debug_assertions))]
use std::cell::RefCell;
use std::collections::{HashMap, HashSet, VecDeque};
use std::ffi::{OsString, c_void};
use std::io;
use std::mem::{offset_of, size_of};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::{Path, PathBuf};
use std::ptr;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, info, trace, warn};
use vrr::configuration::{EraTable, INIT_SLOT, MAX_MEMBERS, SystemOperation, VOID_SLOT};
use vrr::effects::{Effect, Stability};
use vrr::ids::{
    Ballot, CrashCounter, Era, NodeId, Operation, OperationId, Slot, SystemId, Tick, View,
};
use vrr::journal::{Journal, LogEntry, Payload, SegmentedLog};
use vrr::lifecycle::{
    self, BootError, BootOutcome, Bumped, Crashed, LifecycleStore, Marker, Running, Vouched,
};
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

/// An invariant of the adapter: the condition either holds or the process
/// dies — in every build, tests and release alike. Exported so downstream
/// embedders state their host-side invariants under the same convention.
#[macro_export]
macro_rules! invariant {
    ($condition:expr, $($arg:tt)*) => {
        assert!(
            $condition,
            "invariant violation: {}",
            format_args!($($arg)*)
        )
    };
}

/// The wall-clock millisecond every log line this crate emits carries.
#[doc(hidden)]
#[must_use]
pub fn log_millis() -> u64 {
    unix_millis().unwrap_or(0)
}

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

/// One lifecycle path line on the calling thread's census tape
/// ([`census_paths`]); the expansion is nothing in a release build.
#[macro_export]
macro_rules! trace_line {
    ($path:expr) => {{
        #[cfg(any(test, debug_assertions))]
        $crate::census_push($path);
    }};
}

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
/// The drain point's refusal code: the node has stopped and the wire is closed.
pub const STOPPED: i32 = -10;
pub const PANIC: i32 = -127;

/// The queued output's kinds (`NodeOutput.kind`): a unicast peer datagram or a client reply.
pub const OUTPUT_SEND: u32 = 1;
pub const OUTPUT_REPLY: u32 = 2;

/// The output kind's name for human surfaces: `send`, `reply`, `unknown(N)`.
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
/// next view. Host policy; the default every [`Node::open`] caller passes.
pub const PRIMARY_TIMEOUT_MS: u64 = 5000;

/// The host's evidence-and-transfer datagram budget (W5): a default UDP send
/// buffer (9216 bytes on macOS) rejects larger sends with `EMSGSIZE` and they
/// silently never arrive; the transfer machinery resumes overflow through the
/// fetch cursor, so correctness never depends on the number — only liveness.
const EVIDENCE_BUDGET: usize = 8192;

/// Leader/primary unknown (era outside the core's retention window, or the void configuration).
const LEADER_UNKNOWN: u32 = u32::MAX;

/// The packing's halves: the system identifier is the high sixteen bits of a
/// packed `NodeId`, the crash counter the low sixteen.
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

struct Construction {
    primary_timeout: u64,
}

pub struct Node {
    replica: Core,
    outputs: VecDeque<Queued>,
    service: Service,
    /// message_id bytes -> cached response bytes, for duplicate replay (B2).
    replies: HashMap<[u8; 16], Vec<u8>>,
    /// Proposed through `request` and not yet applied: the ids that get replies.
    pending: HashMap<OperationId, [u8; 16]>,
    last_tick: u64,
    poisoned: bool,
    /// The sweeper's own request_num sequence.
    sweep_seq: u64,
    /// The recorded reason of the first self-arrest observation.
    fault_note: Option<String>,
    /// The superseded identity on a dirty restart: the fenced-boot drives
    /// re-announce `Reincarnation(old, new)` until the node seats.
    reincarnate_from: Option<NodeId>,
    /// The descriptor's member ids: the address space `receive` accepts.
    known_ids: HashSet<u32>,
    last_view: Option<(u32, u32)>,
    last_leader: Option<u32>,
    last_config_era: Option<u32>,
    /// The lock-event sink: `None` when journaling is disabled or errored.
    sink: SinkDoor,
    /// The durable incarnation-marker path.
    state_path: PathBuf,
    /// The node's disk seam; dynamic so the C ABI stays monomorph-free.
    disk: Arc<dyn Disk>,
    /// The lock table's persistence seam (eager flush at stop, lazy load on demand).
    state_store: Box<dyn StateStore>,
    /// Whether the boot gate's verdict was a CLEAN stop (the lazy-load guard).
    clean_boot: bool,
    /// Whether the lazy load has already been taken this life.
    state_loaded: bool,
    /// The commit seam: a trigger fired at each applied commit.
    commit_hook: Arc<Mutex<dyn CommitHook>>,
    /// The boot gate's latched session: the Running typestate the stop path drives.
    session: Option<Running<GateStore>>,
    /// The crashed classification's engine session, held for the seated witness's latch.
    deferred: Option<Crashed<GateStore>>,
    /// The drain point's wire-closed flag: set before any marker write at stop.
    stopped: bool,
    /// The Flight Recorder's tape (`flight-recorder` feature); `None` without it.
    #[cfg(feature = "flight-recorder")]
    flight: Option<crate::flight::FlightRecorder>,
}

pub(crate) enum JournalSink {
    Blocking(LockJournal),
    Aof(AofWriter),
}

impl Node {
    fn tick(&mut self) -> Result<u64, i32> {
        let now = unix_millis()?;
        let next = self.last_tick.max(now);
        assert!(
            next >= self.last_tick,
            "tick regression: last={} next={}",
            self.last_tick,
            next
        );
        self.last_tick = next;
        Ok(self.last_tick)
    }

    fn drive(&mut self, event: Input) -> i32 {
        let at = match self.tick() {
            Ok(at) => at,
            Err(error) => return error,
        };
        self.drive_at(at, event)
    }

    fn drive_at(&mut self, at: u64, event: Input) -> i32 {
        #[cfg(feature = "flight-recorder")]
        self.flight_log("drive-in", Self::flight_input_summary(&event));
        let code = self.drive_at_inner(at, event);
        #[cfg(feature = "flight-recorder")]
        self.flight_log("drive-out", serde_json::json!({ "code": code }));
        self.settle_deferred_latch();
        code
    }

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
                assert!(!self.poisoned, "post-poison execution");
                let planned = match self.replica.plan(&input, &self.replica.journal().view()) {
                    Ok(planned) => planned,
                    Err(PlanRefusal::Faulted(fault)) => {
                        self.record_fault(format!("sticky fault: {fault:?}"));
                        return Err(FAULTED);
                    }
                    Err(refusal) => return Err(plan_error(refusal)),
                };
                match self.replica.publish(planned) {
                    Ok(PublishOutcome::Published { effects, .. }) => Ok(effects),
                    Ok(PublishOutcome::Parked { revision, .. }) => {
                        self.record_fault(format!(
                            "publish parked under Stability::Volatile (revision {revision})"
                        ));
                        Err(FAULTED)
                    }
                    Err(PublishRefusal::IllegalCandidate(fault)) => {
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
                    let message = payload
                        .downcast_ref::<String>()
                        .cloned()
                        .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                        .unwrap_or_else(|| "an unprintable panic".to_string());
                    self.record_fault(format!("panic: {message}"));
                    eprintln!(
                        "lunet-advisory-lock: a panic unwound at the boundary (panic: {message}); \
                         the let-it-crash law aborts the process"
                    );
                    std::process::abort();
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

    fn sweep_expired(&mut self) {
        const SWEEP_BUDGET: usize = 8;
        const SYSTEM_CLIENT: u64 = 0;
        if self.primary_index() != self.replica.own().0 {
            return;
        }
        let Ok(now) = unix_millis() else {
            return;
        };
        let snapshot = self.service.snapshot();
        let expired: Vec<u64> = snapshot
            .locks
            .iter()
            .filter(|(_, lease)| lease.expiry <= now)
            .map(|(lock_id, _)| *lock_id)
            .take(SWEEP_BUDGET)
            .collect();
        for lock_id in expired {
            self.sweep_seq += 1;
            let request = serde_json::json!({
                "op": "break",
                "message_id": uuid::Uuid::new_v4().to_string(),
                "client_id": SYSTEM_CLIENT,
                "request_num": self.sweep_seq,
                "lock_id": lock_id,
            });
            let payload = serde_json::to_vec(&request).unwrap_or_default();
            if payload.is_empty() {
                continue;
            }
            let _ = self.request(&payload);
        }
    }

    fn load_state_on_demand(&mut self) {
        if self.state_loaded {
            return;
        }
        self.state_loaded = true;
        let clean_boot = self.clean_boot;
        let table_empty = self.service.is_empty();
        if !clean_boot {
            trace_line!("state.load-distrusted");
            eprintln!(
                "lunet-advisory-lock: crashed boot: the state artefact is \
                 distrusted; the lock table rebuilds from the replica stream"
            );
        }
        match state::lazy_load(&*self.state_store, clean_boot, table_empty) {
            Ok(Some(snapshot)) => {
                for lock_id in snapshot.locks.keys() {
                    self.state_store.note(*lock_id);
                }
                self.service.restore(snapshot);
                info!(
                    ts = log_millis(),
                    event = "state-loaded",
                    node = self.replica.own().0,
                    "the lock table materialised from the clean stop's state file"
                );
                eprintln!(
                    "lunet-advisory-lock: the lock table materialised from \
                     the clean stop's state artefact"
                );
            }
            Ok(None) => {}
            Err(error) => {
                trace_line!("state.load-refused");
                eprintln!(
                    "lunet-advisory-lock: the state load failed ({error}); \
                     the lock table stays empty and the stream rebuilds it"
                );
            }
        }
    }

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

    #[cfg(feature = "flight-recorder")]
    fn flight_log(&mut self, kind: &str, detail: serde_json::Value) {
        if let Some(recorder) = self.flight.as_mut() {
            recorder.event(kind, detail);
        }
    }

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
        invariant!(
            !folded_era_regressed(self.last_config_era, config_era),
            "folded configuration era regressed (previous={}, current={})",
            self.last_config_era.unwrap_or_default(),
            config_era
        );
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
                commit_hook_guard(&self.commit_hook).on_commit(slot);
                let message_id = operation_id_bytes(operation_id);
                let (response, transition) = if let Some(cached) = self.replies.get(&message_id) {
                    (cached.clone(), None)
                } else {
                    assert!(
                        !self.replies.contains_key(&message_id),
                        "exactly-once: a cached reply must never re-execute the Service \
                         (message_id={message_id:?})"
                    );
                    let request = Service::decode(&payload).map_err(|_| SERVICE)?;
                    let (id, client_id, request_num) = request.ids();
                    if id.as_bytes() != &message_id {
                        return Err(SERVICE);
                    }
                    let execution_time = unix_millis()?;
                    self.load_state_on_demand();
                    let (bytes, transition) = self
                        .service
                        .execute(id, client_id, request_num, execution_time, &payload)
                        .map_err(|_| SERVICE)?;
                    self.replies.insert(message_id, bytes.clone());
                    (bytes, transition)
                };
                if let Some(transition) = transition {
                    self.state_store.note(transition.lock_id());
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
                                     the let-it-crash law aborts the process"
                                );
                                std::process::abort();
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
            Effect::Persist(_) => Err(FAULTED),
            Effect::AdminResponse { .. } => Err(FAULTED),
        }
    }

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
/// peer datagram (`kind == 1`) or a client reply (`kind == 2`).
pub struct NodeOutput {
    pub kind: u32,
    pub to: u32,
    pub era: u32,
    pub view: u32,
    pub slot: u64,
    pub message_id: [u8; 16],
    pub bytes: Vec<u8>,
}

/// One status snapshot for an embedded host (`Node::status`): the replication
/// state, the current view's primary, era, view and folded config era, and
/// the self-arrest flag and its recorded reason.
#[derive(Debug)]
pub struct NodeStatus {
    pub state: u32,
    pub leader: u32,
    pub era: u32,
    pub view: u32,
    pub config_era: u32,
    /// Whether the node has self-arrested: every further entry reports SERVICE.
    pub poisoned: bool,
    /// The self-arrest's recorded reason, when the arrest named one.
    pub fault_note: Option<String>,
}

/// The replication state word's name for human surfaces (`state=normal`, never `state=0`).
pub fn replication_state_name(word: u32) -> &'static str {
    match vrr::progress::Status::from_word(word) {
        Some(status) => status.name(),
        None => "invalid",
    }
}

impl NodeStatus {
    /// The status's replication state spelled for humans.
    pub fn state_name(&self) -> &'static str {
        replication_state_name(self.state)
    }
}

/// The state store's counters, the C ABI's layout: one field set per stream
/// plus the last completion's timestamp and a `present` flag; mirrored in
/// `src/advisory_lock.tl`'s cdef.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StateStatusRaw {
    pub eph_completed_generations: u64,
    pub eph_completed_records: u64,
    pub eph_refusals: u64,
    pub eph_in_flight: u32,
    pub dur_completed_generations: u64,
    pub dur_completed_records: u64,
    pub dur_refusals: u64,
    pub dur_in_flight: u32,
    pub last_completed_unix_ns: u64,
    pub present: u32,
}

/// The binding layout pinned at compile time on every build: a field added,
/// reordered or retyped on either side (here or the Teal cdef) is a compile
/// error, never a silently shifted read.
const STATE_STATUS_RAW_LAYOUT: () = assert!(
    size_of::<StateStatusRaw>() == 80
        && offset_of!(StateStatusRaw, eph_completed_generations) == 0
        && offset_of!(StateStatusRaw, eph_completed_records) == 8
        && offset_of!(StateStatusRaw, eph_refusals) == 16
        && offset_of!(StateStatusRaw, eph_in_flight) == 24
        && offset_of!(StateStatusRaw, dur_completed_generations) == 32
        && offset_of!(StateStatusRaw, dur_completed_records) == 40
        && offset_of!(StateStatusRaw, dur_refusals) == 48
        && offset_of!(StateStatusRaw, dur_in_flight) == 56
        && offset_of!(StateStatusRaw, last_completed_unix_ns) == 64
        && offset_of!(StateStatusRaw, present) == 72
);

impl StateStatusRaw {
    fn from(status: Option<&crate::state::StoreStatus>) -> Self {
        match status {
            None => Self::default(),
            Some(status) => Self {
                eph_completed_generations: status.ephemeral.completed_generations,
                eph_completed_records: status.ephemeral.completed_records,
                eph_refusals: status.ephemeral.refusals,
                eph_in_flight: status.ephemeral.in_flight,
                dur_completed_generations: status.durable.completed_generations,
                dur_completed_records: status.durable.completed_records,
                dur_refusals: status.durable.refusals,
                dur_in_flight: status.durable.in_flight,
                last_completed_unix_ns: status.last_completed_unix_ns,
                present: 1,
            },
        }
    }
}

fn default_state_store(disk: &Arc<dyn Disk>, state: &str) -> Box<dyn StateStore> {
    Box::new(FileStateStore::new_on(Arc::clone(disk), Path::new(state)))
}

fn wal_state_store(state: &str) -> Box<dyn StateStore> {
    let mut os = PathBuf::from(state).into_os_string();
    os.push(".wal");
    let path = PathBuf::from(os);
    match crate::wal_snapshot::WalSnapshotStore::new(&path) {
        Ok(store) => {
            info!(
                ts = log_millis(),
                event = "wal-mounted",
                path = %path.display(),
                "the WAL snapshot store is mounted (the lock table persists beside the state path)"
            );
            Box::new(store)
        }
        Err(error) => {
            warn!(
                ts = log_millis(),
                event = "wal-mount-refused",
                path = %path.display(),
                error = %error,
                "the WAL snapshot store refused to open; booting without the \
                 artefact — the lock table rebuilds from the replica stream"
            );
            eprintln!(
                "lunet-advisory-lock: the WAL snapshot store refused to open at \
                 {} ({error}); booting without the artefact — the lock table \
                 rebuilds from the replica stream",
                path.display()
            );
            Box::new(FileStateStore::new_on(std_disk(), Path::new(state)))
        }
    }
}

fn default_commit_hook() -> Arc<Mutex<dyn CommitHook>> {
    Arc::new(Mutex::new(NoOpHook))
}

fn commit_hook_guard<'a, 'b>(
    hook: &'a Mutex<dyn CommitHook + 'b>,
) -> MutexGuard<'a, dyn CommitHook + 'b> {
    hook.lock().unwrap_or_else(PoisonError::into_inner)
}

impl Node {
    /// Safe constructor over the same grammar the C ABI takes: `members` is
    /// the NUL-separated `<u32-id>:<name>` buffer in genesis order (`:j` for
    /// post-genesis joiners), `own` the local member's name, `state` the
    /// marker path, `journal_dir` the optional journal dir (empty disables).
    pub fn open(
        members: &str,
        own: &str,
        state: &str,
        journal_dir: Option<&str>,
        roll_bytes: u32,
        primary_timeout: u64,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            let disk = std_disk();
            node_from_parts(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                journal_dir.filter(|dir| !dir.is_empty()),
                roll_bytes,
                primary_timeout,
                Arc::clone(&disk),
                default_state_store(&disk, state),
                default_commit_hook(),
                None,
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// The full-shape constructor: the embedding host's public injection
    /// point. Same grammar as [`Node::open`] plus the four seams the
    /// machinery is written against — `disk`, `state_store`, `commit_hook`,
    /// `fence` (the boot gate's marker store; `None` is the default).
    // The parameter list IS the mounting grammar, one caller-supplied fact
    // per parameter; folding the seams into carriers would hide it.
    #[allow(clippy::too_many_arguments)]
    pub fn open_with_seams(
        members: &str,
        own: &str,
        state: &str,
        journal_dir: Option<&str>,
        roll_bytes: u32,
        primary_timeout: u64,
        disk: Arc<dyn Disk>,
        state_store: Box<dyn StateStore>,
        commit_hook: Arc<Mutex<dyn CommitHook>>,
        fence: Option<Box<dyn LifecycleStore<Error = io::Error> + Send + Sync>>,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            node_from_parts(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                journal_dir.filter(|dir| !dir.is_empty()),
                roll_bytes,
                primary_timeout,
                disk,
                state_store,
                commit_hook,
                fence,
            )
        }))
        .unwrap_or(Err(PANIC))
    }
    /// The AOF-backed constructor: the committed-transition hook enqueues to
    /// the AOF write-behind writer instead of the blocking journal. Same
    /// member/own/state grammar as [`Node::open`]; `aof_dir` is required,
    /// `flush_ms` the periodic fsync knob (`None` fsyncs at roll and shutdown).
    ///
    /// The C ABI takes this path through [`lunet_lock_node_new_aof`].
    pub fn open_aof(
        members: &str,
        own: &str,
        state: &str,
        aof_dir: &str,
        flush_ms: Option<u64>,
    ) -> Result<Node, i32> {
        catch_unwind(AssertUnwindSafe(|| {
            let disk = std_disk();
            node_from_aof(
                members.as_bytes(),
                own.as_bytes(),
                state.as_bytes(),
                aof_dir,
                flush_ms.map(Duration::from_millis),
                Arc::clone(&disk),
                default_state_store(&disk, state),
                default_commit_hook(),
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// The E2 experiment variant: same grammar as [`Node::open`], plus the
    /// recovery-boundary flush variant and its scratch directory. The C ABI
    /// never takes this path.
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
                Arc::clone(&disk),
                default_state_store(&disk, state),
                default_commit_hook(),
                None,
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// The bench-harness variant (unix only): the lifecycle marker store
    /// rides the harness driver's control socket while the blocking journal
    /// stays real (the run's CAS-chain evidence). The C ABI never takes this path.
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
                b"bench",
                journal,
                None,
                Some(store_ctl),
                Construction {
                    primary_timeout: PRIMARY_TIMEOUT_MS,
                },
                Arc::clone(&disk),
                default_state_store(&disk, "bench"),
                default_commit_hook(),
                None,
            )
        }))
        .unwrap_or(Err(PANIC))
    }

    /// Propose a client request (the lock-verb JSON). `0` on acceptance —
    /// the correlated reply arrives later as a kind-2 `NodeOutput`.
    pub fn request(&mut self, json: &[u8]) -> i32 {
        trace!(len = json.len(), "node request entry");
        #[cfg(feature = "flight-recorder")]
        self.flight_bytes_in("request-in", 0, json);
        if self.stopped {
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
            return STOPPED;
        }
        if data.len() > MAX_DATAGRAM {
            return TOO_LARGE;
        }
        invariant!(
            self.known_ids.contains(&from)
                || self
                    .known_ids
                    .iter()
                    .any(|known| (known >> SYSTEM_HALF_SHIFT) == (from >> SYSTEM_HALF_SHIFT)),
            "message from an unknown peer id (from={from}, len={})",
            data.len()
        );
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
            return STOPPED;
        }
        self.drive(Input::Tick)
    }

    /// Election tick: the same liveness input, tick-driven suspicion.
    pub fn leader_timeout(&mut self) -> i32 {
        trace!("node leader timeout entry");
        if self.stopped {
            return STOPPED;
        }
        self.drive(Input::Tick)
    }

    /// Host-forced view change (§14.2): `era` must be the node's current
    /// era and `view` must strictly advance the view number.
    pub fn force_view(&mut self, era: u32, view: u32) -> i32 {
        trace!(era, view, "node force view entry");
        if self.stopped {
            return STOPPED;
        }
        self.drive(Input::AdminForceView {
            target: Ballot {
                era: Era(era),
                view: View(view),
            },
        })
    }

    /// One timeout toggle's event capture (`docs/src/failure-detection.md`),
    /// recorded in the regular log and the Flight Recorder. No-op on the
    /// replication path.
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

    /// One fenced-boot drive: ticks the node, re-announcing a dirty
    /// restart's `Input::Reincarnate { old }` first on every drive until
    /// it stops being fenced (§8). Never fabricates recovered state.
    pub fn recover(&mut self) -> i32 {
        if self.poisoned {
            return SERVICE;
        }
        if self.stopped {
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
    /// join succession position (`POSITION_APPEND` appends at the end).
    pub fn reconfigure(&mut self, op: u32, member: u32, position: u32) -> i32 {
        if self.stopped {
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
        let pivot = match op {
            RECONFIGURE_JOIN | RECONFIGURE_LEAVE => None,
            _ => self.derived_pivot(&system),
        };
        self.drive(Input::Reconfigure { op: system, pivot })
    }

    /// The graceful stop, in the contract's write order: the wire closes
    /// first (every further inbound entry reports STOPPED), the sink drains,
    /// the lock table flushes eagerly, then the two marker rounds write.
    /// Idempotent; a failed round or drain reports SERVICE.
    pub fn stop(&mut self) -> i32 {
        if self.stopped {
            trace_line!("stop.idempotent");
            return OK;
        }
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
                "state": tbio::marker::MarkerState::Stopped.code(),
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
        let snapshot = self.replica.observer().read();
        let record = ViewRecord {
            era: snapshot.era,
            view: snapshot.view,
        };
        if let Err(error) = write_view_record(&*self.disk, &self.state_path, &record) {
            trace_line!("stop.refuse.drain-window");
            eprintln!(
                "lunet-advisory-lock: the stop's view-record write failed ({error}); \
                        the markers hold the halt's first round; {UNFLUSHED_RUNBOOK}; \
                        the let-it-crash law aborts the process"
            );
            std::process::abort();
        }
        trace_line!("stop.drain-window.open");
        let draining = match halting.drain() {
            Ok(draining) => draining,
            Err((_, error)) => {
                trace_line!("stop.refuse.drain");
                eprintln!(
                    "lunet-advisory-lock: the stop drain failed ({error}); \
                        the marker stays at the first round; {UNFLUSHED_RUNBOOK}; \
                        the let-it-crash law aborts the process"
                );
                #[cfg(feature = "flight-recorder")]
                self.flight_log(
                    "stop-drain",
                    serde_json::json!({ "what": "the stop drain failed", "error": error.to_string() }),
                );
                std::process::abort();
            }
        };
        trace_line!("stop.drain-window.close");
        self.load_state_on_demand();
        if let Err(error) = self.state_tick_path() {
            trace_line!("stop.refuse.state-tick");
            eprintln!(
                "lunet-advisory-lock: the stop's state tick path failed \
                 ({error}); the marker stays at the first round; \
                 {UNFLUSHED_RUNBOOK}"
            );
            return SERVICE;
        }
        trace_line!("state.tick");
        if let Err(error) = self.state_store.flush(&self.service.snapshot()) {
            trace_line!("stop.refuse.state-flush");
            eprintln!(
                "lunet-advisory-lock: the stop's state flush failed ({error}); \
                     the marker stays at the first round; {UNFLUSHED_RUNBOOK}"
            );
            return SERVICE;
        }
        trace_line!("state.flush");
        trace_line!("stop.round.finish");
        #[cfg(feature = "flight-recorder")]
        self.flight_log(
            "marker",
            serde_json::json!({
                "what": "the drain-proven second round (Stopped) begins",
                "identity": identity.0,
                "state": tbio::marker::MarkerState::Flushed.code(),
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

    fn state_tick_path(&mut self) -> std::io::Result<()> {
        self.state_store.pump()?;
        self.state_store.tick(&self.service.snapshot())?;
        self.state_store.pump()
    }

    /// The steady tick (~1000 ms timer): the changed set rides the tick
    /// path with zero fsync. A capacity refusal is counted in the store's
    /// `refusals` and left to the next tick; the answer is still `OK`.
    pub fn state_tick(&mut self) -> i32 {
        if self.stopped {
            return STOPPED;
        }
        self.sweep_expired();
        if let Err(error) = self.state_tick_path() {
            warn!(
                ts = log_millis(),
                event = "state-tick-refused",
                node = self.replica.own().0,
                error = %error,
                "the state tick path was refused; the changed set retries on \
                 the next tick"
            );
        }
        OK
    }

    /// The store's per-stream counters for the status surface; `None` when
    /// the mounted store carries no counters (the file store).
    pub fn state_status(&self) -> Option<crate::state::StoreStatus> {
        self.state_store.state_status()
    }

    /// This node's voting weight in the current folded configuration.
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

fn folded_era_regressed(previous: Option<u32>, current: u32) -> bool {
    matches!(previous, Some(previous) if current < previous)
}

fn plan_error(rejection: PlanRefusal) -> i32 {
    debug!(?rejection, "plan refused");
    match rejection {
        PlanRefusal::NotPrimary { .. } => NOT_LEADER,
        PlanRefusal::Faulted(_) => FAULTED,
        _ => SERVICE,
    }
}

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

struct MemberEntry {
    id: u32,
    name: String,
    joined: bool,
}

fn provisioned_identity(id: u32) -> bool {
    NodeId::from(id).system_id().is_some() && (id & 0xffff) == 1
}

/// Reconfiguration operation codes for `lunet_lock_node_reconfigure`.
pub const RECONFIGURE_JOIN: u32 = 1;
pub const RECONFIGURE_INCREMENT: u32 = 2;
pub const RECONFIGURE_LEAVE: u32 = 3;
pub const RECONFIGURE_DECREMENT: u32 = 4;

/// `lunet_lock_node_reconfigure`'s Join position sentinel: append at the succession end.
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

fn marker_line(system: u16, crash: u16, marker: Marker) -> String {
    format!(
        "{system} {crash} {}\n",
        crate::marker_store::projection_word(marker)
    )
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ViewRecord {
    pub era: u32,
    pub view: u32,
}

impl ViewRecord {
    fn line(&self) -> String {
        format!("{} {}\n", self.era, self.view)
    }

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

fn view_record_path(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".view");
    PathBuf::from(os)
}

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
    parse_marker(&StdDisk.read_to_string(path)?)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid marker"))
}

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

#[derive(Debug)]
struct BootDecision {
    session: Option<Running<GateStore>>,
    deferred: Option<Crashed<GateStore>>,
    identity: NodeId,
    vouched: Option<Vouched>,
    pair: Option<Bumped>,
    flush: Option<FlushOutcome>,
    restored: Option<ViewRecord>,
}

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

pub(crate) fn sync_parent(disk: &dyn Disk, path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    sync_dir(disk, parent)
}

// Fsyncing the containing directory after create/rename is a POSIX crash-
// consistency idiom. Windows cannot open a directory (std does not set
// FILE_FLAG_BACKUP_SEMANTICS) and NTFS needs no directory fsync for the
// guarantee, so the step no-ops there (cf. `atomicwrites`).
#[cfg(unix)]
fn sync_dir(disk: &dyn Disk, dir: &Path) -> std::io::Result<()> {
    disk.sync_path(dir)
}

#[cfg(windows)]
fn sync_dir(_disk: &dyn Disk, _dir: &Path) -> std::io::Result<()> {
    Ok(())
}

fn guarded(run: impl FnOnce() -> i32) -> i32 {
    // The let-it-crash law: a panic at the boundary aborts the process.
    match catch_unwind(AssertUnwindSafe(run)) {
        Ok(code) => code,
        Err(payload) => {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                .unwrap_or_else(|| "an unprintable panic".to_string());
            eprintln!(
                "lunet-advisory-lock: a panic unwound at the boundary (panic: {message}); \
                 the let-it-crash law aborts the process"
            );
            std::process::abort();
        }
    }
}

/// Conservative worst-case size of the Prepare datagram a `payload_len`-byte
/// proposal produces, per the wire module's fixed-width layout; admission
/// refuses before proposing.
const PREPARE_OVERHEAD: usize = 20 + 1 + 8 + 4 + 1 + 16 + 4 + 8;

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

// The parameter list IS the construction grammar, one caller-supplied fact
// per parameter; folding the seams into a carrier would hide it.
#[allow(clippy::too_many_arguments)]
fn node_from_parts(
    members_data: &[u8],
    own_data: &[u8],
    state_data: &[u8],
    journal_dir: Option<&str>,
    roll_bytes: u32,
    primary_timeout: u64,
    disk: Arc<dyn Disk>,
    state_store: Box<dyn StateStore>,
    commit_hook: Arc<Mutex<dyn CommitHook>>,
    fence: Option<Box<dyn LifecycleStore<Error = io::Error> + Send + Sync>>,
) -> Result<Node, i32> {
    let journal = open_journal(Arc::clone(&disk), journal_dir, roll_bytes);
    node_from_sink(
        members_data,
        own_data,
        state_data,
        journal,
        None,
        None,
        Construction { primary_timeout },
        disk,
        state_store,
        commit_hook,
        fence,
    )
}

// The parameter list IS the construction grammar, one caller-supplied fact
// per parameter; folding the seams into a carrier would hide it.
#[allow(clippy::too_many_arguments)]
fn node_from_aof(
    members_data: &[u8],
    own_data: &[u8],
    state_data: &[u8],
    aof_dir: &str,
    flush_interval: Option<Duration>,
    disk: Arc<dyn Disk>,
    state_store: Box<dyn StateStore>,
    commit_hook: Arc<Mutex<dyn CommitHook>>,
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
        state_store,
        commit_hook,
        None,
    )
}

// The parameter list IS the construction grammar, one caller-supplied fact
// per parameter; folding the seams into a carrier would hide it.
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
    mut state_store: Box<dyn StateStore>,
    commit_hook: Arc<Mutex<dyn CommitHook>>,
    fence: Option<Box<dyn LifecycleStore<Error = io::Error> + Send + Sync>>,
) -> Result<Node, i32> {
    let refused_descriptor = || {
        trace_line!("boot.refuse.descriptor");
        CONFIG
    };
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
    let state_path = PathBuf::from(state);
    let sink: SinkDoor = Arc::new(Mutex::new(journal));
    let marker_log: Arc<Mutex<Vec<String>>> = Arc::new(Mutex::new(Vec::new()));
    #[cfg(unix)]
    let store = match (fence, store_ctl) {
        (Some(fence), _) => GateStore::over(fence, Arc::clone(&sink), Arc::clone(&marker_log)),
        (None, Some(ctl)) => {
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
        (None, None) => GateStore::new(
            &state_path,
            Arc::clone(&sink),
            system.get(),
            Arc::clone(&marker_log),
        ),
    };
    #[cfg(not(unix))]
    let store = match (fence, store_ctl) {
        (Some(fence), _) => GateStore::over(fence, Arc::clone(&sink), Arc::clone(&marker_log)),
        (None, _) => {
            let _ = store_ctl;
            GateStore::new(
                &state_path,
                Arc::clone(&sink),
                system.get(),
                Arc::clone(&marker_log),
            )
        }
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
    let not_a_clean_boot = decision.vouched.is_none();
    if not_a_clean_boot {
        state_store.forget();
    }
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
    let reincarnate_from = decision.pair.as_ref().map(|pair| pair.old);
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
    let refused_genesis = || {
        trace_line!("boot.refuse.genesis");
        CONFIG
    };
    let replica = if let Some(pair) = decision.pair {
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
        sweep_seq: 0,
        fault_note: None,
        reincarnate_from,
        known_ids,
        last_view: None,
        last_leader: None,
        last_config_era: None,
        sink,
        state_path: state_path.clone(),
        disk: Arc::clone(&disk),
        state_store,
        clean_boot: decision.vouched.is_some(),
        state_loaded: false,
        commit_hook,
        session: decision.session,
        deferred: decision.deferred,
        stopped: false,
        #[cfg(feature = "flight-recorder")]
        flight: crate::flight::FlightRecorder::open_from_env_on(&disk, own_id.0),
    };
    Ok(node)
}

/// The read-only information console: this build's own identity facts as the
/// Maven `version.properties` text, NUL-terminated into `out_data` with its
/// length in `out_len`; TOO_LARGE with the needed size when it does not fit.
/// Takes no node handle — read-only by construction.
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

/// Create a node: `members_data` is the NUL-separated `<u32-id>:<name>` buffer
/// in genesis order (`:j` for post-genesis joiners), `own_data` the local
/// member's name, `state_data` the durable marker path, `journal_dir_data` the
/// optional journal dir (empty disables); on success writes the handle to `*out`.
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
            let disk = std_disk();
            let state = std::str::from_utf8(state_data).unwrap_or_default();
            node_from_parts(
                members_data,
                own_data,
                state_data,
                journal_dir,
                roll_bytes,
                PRIMARY_TIMEOUT_MS,
                Arc::clone(&disk),
                wal_state_store(state),
                default_commit_hook(),
                None,
            )
        })) {
            Ok(Ok(node)) => {
                unsafe { *out = Box::into_raw(Box::new(node)).cast() };
                OK
            }
            Ok(Err(code)) => code,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                    .unwrap_or_else(|| "an unprintable panic".to_string());
                eprintln!(
                    "lunet-advisory-lock: a panic unwound at the boundary (panic: {message}); \
                     the let-it-crash law aborts the process"
                );
                std::process::abort();
            }
        }
    })
}

/// The AOF-backed creator: same grammar as [`lunet_lock_node_new`] with
/// `journal_dir`/`roll_bytes` replaced by the required `aof_dir` and the
/// `flush_ms` cadence (0 fsyncs only at roll and shutdown).
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_new_aof(
    members_len: usize,
    members_data: *const u8,
    own_len: usize,
    own_data: *const u8,
    state_len: usize,
    state_data: *const u8,
    aof_dir_len: usize,
    aof_dir_data: *const u8,
    flush_ms: u64,
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
        let Ok(aof_dir_data) = (unsafe { bytes(aof_dir_len, aof_dir_data) }) else {
            return INVALID;
        };
        let Ok(aof_dir) = std::str::from_utf8(aof_dir_data) else {
            return INVALID;
        };
        if aof_dir.is_empty() {
            return INVALID;
        }
        let flush_interval = if flush_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(flush_ms))
        };
        match catch_unwind(AssertUnwindSafe(|| {
            let disk = std_disk();
            let state = std::str::from_utf8(state_data).unwrap_or_default();
            node_from_aof(
                members_data,
                own_data,
                state_data,
                aof_dir,
                flush_interval,
                Arc::clone(&disk),
                wal_state_store(state),
                default_commit_hook(),
            )
        })) {
            Ok(Ok(node)) => {
                unsafe { *out = Box::into_raw(Box::new(node)).cast() };
                OK
            }
            Ok(Err(code)) => code,
            Err(payload) => {
                let message = payload
                    .downcast_ref::<String>()
                    .cloned()
                    .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                    .unwrap_or_else(|| "an unprintable panic".to_string());
                eprintln!(
                    "lunet-advisory-lock: a panic unwound at the boundary (panic: {message}); \
                     the let-it-crash law aborts the process"
                );
                std::process::abort();
            }
        }
    })
}

/// Free a node handle; NULL is a no-op.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_free(node: *mut c_void) {
    if !node.is_null() {
        let _ = catch_unwind(AssertUnwindSafe(|| unsafe {
            drop(Box::from_raw(node.cast::<Node>()));
        }))
        .inspect_err(|payload| {
            let message = payload
                .downcast_ref::<String>()
                .cloned()
                .or_else(|| payload.downcast_ref::<&str>().map(|text| text.to_string()))
                .unwrap_or_else(|| "an unprintable panic".to_string());
            eprintln!(
                "lunet-advisory-lock: a panic unwound freeing the node (panic: {message}); \
                 the let-it-crash law aborts the process"
            );
            std::process::abort();
        });
    }
}

/// The node's live identity: the descriptor id at incarnation 0, the bumped
/// high-band id after a dirty restart.
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

/// Route to [`Node::request`].
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

/// Route to [`Node::reconfigure`].
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

/// Route to [`Node::receive`].
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

/// Heartbeat tick: route to [`Node::idle`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_idle(node: *mut c_void) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.idle())
    })
}

/// Election tick: route to [`Node::leader_timeout`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_leader_timeout(node: *mut c_void) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.leader_timeout())
    })
}

/// §14.2 host-forced view change: [`OK`], [`SERVICE`] (poisoned), or
/// [`CONFIG`]-class refusals for a target that does not advance.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_force_view(node: *mut c_void, era: u32, view: u32) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, |node| node.force_view(era, view))
    })
}

/// Route to [`Node::recover`].
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_recover(node: *mut c_void) -> i32 {
    guarded(|| unsafe { node.cast::<Node>().as_mut().map_or(INVALID, Node::recover) })
}

/// The graceful stop (the termination obligations, host-side): the wire closes
/// before any marker write, then the `stopped` marker, the sink drain, and the
/// `flushed` marker, in the contract's write order. Synchronous; idempotent.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_stop(node: *mut c_void) -> i32 {
    guarded(|| unsafe { node.cast::<Node>().as_mut().map_or(INVALID, Node::stop) })
}

/// The steady tick (the ~1000 ms timer's entry): a capacity refusal is
/// counted in the store's per-stream `refusals` and leaves the serving
/// state untouched (the answer is still [`OK`]); `STOPPED` past the drain
/// point.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_state_tick(node: *mut c_void) -> i32 {
    guarded(|| unsafe {
        node.cast::<Node>()
            .as_mut()
            .map_or(INVALID, Node::state_tick)
    })
}

/// The state store's per-stream counters, the C ABI's mirror of
/// `state::StoreStatus`: `present` is 1 when the mounted store reports
/// counters (the WAL snapshot store), 0 when it does not. Pull-style.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn lunet_lock_node_state_status(
    node: *mut c_void,
    out_status: *mut StateStatusRaw,
) -> i32 {
    guarded(|| {
        if node.is_null() || out_status.is_null() {
            return INVALID;
        }
        let node = unsafe { &*node.cast::<Node>() };
        let status = node.state_status();
        unsafe {
            const _: () = STATE_STATUS_RAW_LAYOUT;
            *out_status = StateStatusRaw::from(status.as_ref());
        }
        OK
    })
}

/// The status snapshot's four words: state, leader, era, view.
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

/// The leader for `(era, view)`; [`LEADER_UNKNOWN`] when unknown.
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

/// The self-arrest report: the recorded reason, NUL-terminated into
/// `out_data` with its length in `out_len`; OK with `out_len` 0 on a node
/// that has not arrested; TOO_LARGE with the needed size when it does not
/// fit. Observation only, valid on a poisoned node.
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

/// Pop the next queued output: `out_kind` 1 = send (unicast to `to`) or
/// 2 = reply (`message_id`-keyed). Returns 1 when an output was produced, 0
/// when the queue is empty, negative on error; when the bytes exceed
/// `capacity`, reports the needed size in `out_len` and does NOT pop.
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
