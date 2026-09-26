//! The routed lifecycle marker: the engine's `LifecycleStore` over the
//! superblock copies.
//!
//! The engine (`vrr::lifecycle`, uvrr-core tag v0.9.0 @ b2ba0d2) owns the marker
//! machine — which marker, which copies, when, and in what order
//! (`docs/uvrr-boot-gate.md` §3). This module is the host's durable
//! mechanics: the vendored Zig store's quorum-of-copies construction,
//! reached through the AOF C ABI's marker exports (`ext/lunet-locks-aof`),
//! plus the single-file compatibility projection.
//!
//! The bridge to the engine's machine: the durable marker is the
//! `NodeIdentity` pair (system identifier, crash counter) — it vouches for
//! the life — and the engine's lifecycle input is the packed pair itself,
//! read at this boot-gate boundary. The packed u32 is what goes on the
//! wire; this module spells the pair on every write and reads it back on
//! every boot.
//!
//! - **Read** (`read_copies`) — the working quorum's verdict: the
//!   highest-sequence valid copies at the `.open` threshold (2/4). THE
//!   BOOT-READ SAFETY LAW: every block read validates its checksum
//!   before any classification logic, and a checksum failure on ANY copy
//!   is a loud log and a PANIC — the Zig store refuses with the distinct
//!   `CORRUPT` code (the FFI boundary cannot panic across the ABI) and
//!   this adapter panics on it: the boot never hangs, never clears,
//!   never repairs, never falls back — deleting the marker file is the
//!   recovery path. A tear is the spread writes being inconsistent
//!   across the copies (checksum-valid copies at differing states), and
//!   it resolves by the stated thresholds with the non-unanimity logged
//!   in full at the moment of resolution (in the Zig store, which
//!   resolves). The on-disk states map onto the engine's markers:
//!   `flushed` is the drain-proven `Stopped` (a controlled ending),
//!   `stopped` is `Stopping` (the halt has begun — it vouches for
//!   nothing), and `unflushed` is the running sentinel (`Joining` — an
//!   operating or freshly-latched process). An existing copies file that
//!   cannot be read to a verdict is an error — the boot refuses, it
//!   never falls back to the projection. When the copies never existed
//!   (no superblock file), the single file is the boot input: the legacy
//!   migration path, whose first routed write seeds the copies.
//! - **Write** (`commit`) — the engine's decided rewrite: the quorum
//!   write of `(identity, state)` — four fixed sector-aligned
//!   Aegis-checksummed copies, hash-chained sequence/parent, forced I/O
//!   (the fsync lands before the write reports success), verified
//!   against the `.verify` threshold (3/4) — then the single-file
//!   projection mirrors the state (fsync+rename+dir-sync, unchanged).
//!   The projection is written only after the quorum write succeeded;
//!   it is never a classification input once the copies exist.
//! - **Drain** (`drain`) — the committed-transition sink forced to
//!   quiescence: every record the node enqueued is appended and fsynced
//!   (the AOF writer) or the journal file is fsynced (the blocking
//!   journal). The driver calls it strictly between the two halt rounds.
//!
//! The sink rides a shared door (`SinkDoor`): the Node appends through
//! it during operation; the halt's drain forces it through the engine's
//! schedule.
//!
//! # On-disk layout
//!
//! The superblock copies live in a sibling file, `<state>.superblock`.
//! The single `<system> <crash> <unflushed|stopped|flushed>` text file
//! (the identity pair spelled as its halves, then the state word) is the
//! projection legacy rigs and operators read, and the boot input only
//! while the copies predate this routing.
//!
//! Windows keeps the item08 single-file discipline: the vendored AOF
//! build is unix-only, and no Windows asset is packaged.
use std::io;
#[cfg(unix)]
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use vrr::ids::{CrashCounter, NodeId, SystemId};
use vrr::lifecycle::{CopyState, LifecycleStore, Marker, SuperblockCopies};

use crate::ffi::{JournalSink, read_marker, write_marker};

#[cfg(not(target_os = "windows"))]
use lunet_locks_aof::marker;

/// The shared door to the committed-transition sink: the Node appends
/// through it during operation, and the halt's `drain` forces it.
pub(crate) type SinkDoor = Arc<Mutex<Option<JournalSink>>>;

/// Locks the sink door. A poisoned lock (a panic while held) is unlocked:
/// the sink's state is a queue plus flags, not an invariant.
pub(crate) fn sink_guard(door: &SinkDoor) -> MutexGuard<'_, Option<JournalSink>> {
    door.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The stop path's durable-state write: drain the committed-transition
/// sink to quiescence — every record the node enqueued is appended and
/// fsynced (the AOF writer), or the journal file is fsynced (the blocking
/// journal). A disabled sink (`None`) has nothing to drain.
pub(crate) fn drain_sink(sink: &mut Option<JournalSink>) -> io::Result<()> {
    match sink {
        Some(JournalSink::Aof(writer)) => writer.drain(),
        Some(JournalSink::Blocking(journal)) => journal.flush(),
        None => Ok(()),
    }
}

/// THE BOOT-READ SAFETY LAW's panic: a readable marker copy failed its
/// checksum. The Zig store already logged the copy in full (which slot,
/// which checksum, which sequence); this adapter panics on the distinct
/// refusal code — the loudest error that exits the boot — and the boot
/// never clears, repairs, or falls back from a bad block. Deleting the
/// marker file is the recovery path (the host re-seeds from the
/// compatibility projection).
fn boot_read_checksum_panic(superblock: &Path, what: &str) -> ! {
    panic!(
        "BOOT-READ SAFETY LAW: {what} of {} found a bad marker checksum \
         (the store's refusal code); the marker store is never cleared, \
         never repaired, never fallen back — delete the marker file to \
         re-seed from the compatibility projection",
        superblock.display(),
    )
}

/// The engine marker's on-disk Zig state: `Stopping` is the halt's first
/// round (it vouches for nothing), `Stopped` is the drain-proven second
/// round, and the latches (`Restarting`, `Joining`) are the running
/// sentinel the next boot reads as an operating process.
#[cfg(not(target_os = "windows"))]
fn zig_state(marker: Marker) -> lunet_locks_aof::marker::MarkerState {
    use lunet_locks_aof::marker::MarkerState;
    match marker {
        Marker::Stopping => MarkerState::Stopped,
        Marker::Stopped => MarkerState::Flushed,
        Marker::Restarting | Marker::Joining => MarkerState::Unflushed,
    }
}

/// The projection's word for an engine marker (the on-disk spelling the
/// single file has carried since item08).
pub(crate) fn projection_word(marker: Marker) -> &'static str {
    match marker {
        Marker::Stopping => "stopped",
        Marker::Stopped => "flushed",
        Marker::Restarting | Marker::Joining => "unflushed",
    }
}

/// The engine marker a resolved Zig state reads as: `flushed` proves the
/// drain (a stopped quorum), `stopped` proves only that the halt began,
/// `unflushed` is the running sentinel — the anchor a crash reads as a
/// crash through.
#[cfg(not(target_os = "windows"))]
fn engine_marker(state: lunet_locks_aof::marker::MarkerState) -> Marker {
    use lunet_locks_aof::marker::MarkerState;
    match state {
        MarkerState::Unflushed => Marker::Joining,
        MarkerState::Stopped => Marker::Stopping,
        MarkerState::Flushed => Marker::Stopped,
    }
}

/// Four uniform copies: the store's read resolved one verdict (the Zig
/// quorum machinery's own minimum-progress discipline), and the engine's
/// cohort classification over a uniform set reads exactly that verdict.
fn uniform(identity: NodeId, marker: Marker) -> SuperblockCopies {
    SuperblockCopies {
        copies: [CopyState { identity, marker }; 4],
    }
}

/// The superblock-copies file for a state path.
pub(crate) fn superblock_path(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".superblock");
    PathBuf::from(os)
}

/// The engine marker's word on the bench store RPC (the engine's own
/// vocabulary — `joining`/`restarting` are the running sentinels).
#[cfg(unix)]
fn marker_word(marker: Marker) -> &'static str {
    match marker {
        Marker::Stopping => "stopping",
        Marker::Stopped => "stopped",
        Marker::Restarting => "restarting",
        Marker::Joining => "joining",
    }
}

/// The engine marker a bench read reply names: the driver's answer is
/// already in the collapsed engine vocabulary — the sentinels read as
/// `Joining`, exactly as the disk path's `engine_marker` collapses them.
#[cfg(unix)]
fn word_marker(word: &str) -> Option<Marker> {
    match word {
        "joining" | "restarting" => Some(Marker::Joining),
        "stopping" => Some(Marker::Stopping),
        "stopped" => Some(Marker::Stopped),
        _ => None,
    }
}

/// The bench backend's end of the store RPC: synchronous NDJSON over the
/// harness driver's unix socket, one request one reply.
#[cfg(unix)]
struct MemBackend {
    /// The driver's socket: the write half and the read half behind one
    /// lock, so the store's RPCs (the engine's scheduled calls and the
    /// emission gate's round) share the one wire discipline.
    socket: std::sync::Mutex<(UnixStream, BufReader<UnixStream>)>,
}

#[cfg(unix)]
impl MemBackend {
    fn connect(path: &Path) -> io::Result<MemBackend> {
        let writer = UnixStream::connect(path)?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(MemBackend {
            socket: std::sync::Mutex::new((writer, reader)),
        })
    }

    fn rpc(&self, request: &serde_json::Value) -> io::Result<serde_json::Value> {
        let mut line = request.to_string();
        line.push('\n');
        let mut socket = self.socket.lock().unwrap_or_else(PoisonError::into_inner);
        socket.0.write_all(line.as_bytes())?;
        socket.0.flush()?;
        let mut reply = String::new();
        let read = socket.1.read_line(&mut reply)?;
        if read == 0 {
            return Err(io::Error::other(
                "the bench store driver closed the control socket",
            ));
        }
        serde_json::from_str(reply.trim_end()).map_err(|error| {
            io::Error::other(format!("the bench store reply did not parse: {error}"))
        })
    }

    /// The reply's `ok` flag, or the driver's refusal as the store error.
    fn ack(reply: &serde_json::Value) -> io::Result<()> {
        if reply.get("ok").and_then(|ok| ok.as_bool()) == Some(true) {
            return Ok(());
        }
        let reason = reply
            .get("error")
            .and_then(|error| error.as_str())
            .unwrap_or("no reason given");
        Err(io::Error::other(format!(
            "the bench store driver refused: {reason}"
        )))
    }
}

/// The store backend: the real application's durable mechanics (`Disk` —
/// the quorum plus the projection), or the bench harness's force-feed
/// (`Mem` — every engine call rides the driver's socket and the driver
/// answers from memory, enforcing the signalled discipline; unix-only,
/// like the bench).
#[cfg(unix)]
enum Backend {
    Disk { state: PathBuf },
    Mem(MemBackend),
}

/// The durable-only backend: non-unix builds carry no bench.
#[cfg(not(unix))]
enum Backend {
    Disk { state: PathBuf },
}

/// The host's durable mechanics for the boot gate: the superblock quorum
/// store plus the single-file projection, wired to the committed-
/// transition sink for the halt's drain. The system identifier is the
/// descriptor's own member's system half — the projection read is
/// checked against it.
pub(crate) struct GateStore {
    backend: Backend,
    sink: SinkDoor,
    system: u16,
}

impl GateStore {
    pub(crate) fn new(state: &Path, sink: SinkDoor, system: u16) -> GateStore {
        GateStore {
            backend: Backend::Disk {
                state: state.to_path_buf(),
            },
            sink,
            system,
        }
    }

    /// The bench-harness store: no durable marker I/O at all — every
    /// engine call (`read_copies`, `commit`, `drain`) is an RPC to the
    /// harness driver, which holds the identity's state in memory and
    /// fails the run on an unsignalled or out-of-order call
    /// (`docs/src/bench-harness.md`). The connect failure refuses the
    /// boot exactly as a durable read failure does.
    #[cfg(unix)]
    pub(crate) fn mem(ctl: &Path, sink: SinkDoor) -> io::Result<GateStore> {
        Ok(GateStore {
            backend: Backend::Mem(MemBackend::connect(ctl)?),
            sink,
            system: 0,
        })
    }

    /// The emission gate's one durable round: the crash bump's marker
    /// write — the next life at the running sentinel — fsync-complete
    /// before the driver releases the first announcement. Unconditional:
    /// the round lands seated or not. The argument is the bumped pair
    /// itself; an identity with a zero half is no identity and refuses
    /// here.
    pub(crate) fn emission_gate(&self, identity: NodeId) -> io::Result<()> {
        match &self.backend {
            Backend::Disk { state } => {
                #[cfg(not(target_os = "windows"))]
                {
                    let pair = marker::NodeIdentity::from_packed(identity.0).ok_or_else(|| {
                        io::Error::other(format!(
                            "the identity {identity} is not spellable: \
                             a zero half is no identity"
                        ))
                    })?;
                    write_round(state, pair, marker::MarkerState::Unflushed)
                }
                #[cfg(target_os = "windows")]
                {
                    let system = identity.system_id().ok_or_else(|| {
                        io::Error::other(format!(
                            "the identity {identity} is not spellable: a zero half is no identity"
                        ))
                    })?;
                    let counter = identity.crash_counter().ok_or_else(|| {
                        io::Error::other(format!(
                            "the identity {identity} is not spellable: a zero half is no identity"
                        ))
                    })?;
                    write_marker(state, system.get(), counter.get(), Marker::Joining)
                }
            }
            #[cfg(unix)]
            Backend::Mem(mem) => {
                let reply = mem.rpc(&serde_json::json!({
                    "op": "commit",
                    "incarnation": identity.0,
                    "marker": "joining",
                }))?;
                MemBackend::ack(&reply)
            }
        }
    }
}

/// The durable read: the working quorum's verdict, or the compatibility
/// projection's while the copies predate this routing. The marker's
/// `NodeIdentity` pair vouches for the life; the engine's lifecycle
/// input is the packed pair.
#[cfg(not(target_os = "windows"))]
fn read_copies_disk(state: &Path, system: u16) -> io::Result<Option<SuperblockCopies>> {
    let superblock = superblock_path(state);
    if !superblock.exists() {
        // The copies never existed: the single file is the boot input
        // (the legacy migration — the first routed write seeds the
        // copies from the file's own state). Neither storage present
        // is the first life, which has no durable identity yet.
        if !state.exists() {
            return Ok(None);
        }
        let (file_system, crash, marker) = read_marker(state)?;
        if file_system != system {
            return Err(io::Error::other(format!(
                "the projection names system {file_system} but the descriptor names system {system}"
            )));
        }
        // The projection's parser refuses a zero half, so the pair's
        // constructors always succeed here.
        let identity = NodeId::new(
            SystemId::new(file_system).expect("the projection refuses a zero half"),
            CrashCounter::new(crash).expect("the projection refuses a zero half"),
        );
        return Ok(Some(uniform(identity, marker)));
    }
    // The copies exist: they are the authoritative read. THE
    // BOOT-READ SAFETY LAW: a bad checksum on ANY copy is a loud log
    // and a PANIC (the adapter panics on the Zig store's distinct
    // refusal code) — never cleared, never repaired, never fallen
    // back, never "unclear". An old-format marker (INCOMPATIBLE) is
    // invalid, never converted. Any other unreadable shape (no quorum,
    // a fork, any refusal) is an error — the boot refuses rather than
    // guessing an identity or falling back to the projection.
    let classified = marker::classify(&superblock).map_err(|code| {
        if code == marker::CORRUPT {
            boot_read_checksum_panic(&superblock, "the quorum read");
        }
        if code == marker::INCOMPATIBLE {
            return io::Error::other(format!(
                "the marker is an old-format file (FFI code {code}): invalid, never converted"
            ));
        }
        io::Error::other(format!("the marker quorum read failed (FFI code {code})"))
    })?;
    Ok(Some(uniform(
        NodeId(classified.identity.packed()),
        engine_marker(classified.state),
    )))
}

/// The durable read: Windows keeps the single-file discipline.
#[cfg(target_os = "windows")]
fn read_copies_disk(state: &Path, system: u16) -> io::Result<Option<SuperblockCopies>> {
    if !state.exists() {
        return Ok(None);
    }
    let (file_system, crash, marker) = read_marker(state)?;
    if file_system != system {
        return Err(io::Error::other(format!(
            "the projection names system {file_system} but the descriptor names system {system}"
        )));
    }
    let identity = NodeId::new(
        SystemId::new(file_system).expect("the projection refuses a zero half"),
        CrashCounter::new(crash).expect("the projection refuses a zero half"),
    );
    Ok(Some(uniform(identity, marker)))
}

/// The durable write: the quorum write, then the projection mirror. The
/// pair the engine decided is spelled as it stands — the identity's own
/// halves, no re-derivation.
#[cfg(not(target_os = "windows"))]
fn commit_disk(state: &Path, copy: CopyState) -> io::Result<()> {
    let identity = marker::NodeIdentity::from_packed(copy.identity.0).ok_or_else(|| {
        io::Error::other(format!(
            "the identity {} is not spellable: a zero half is no identity",
            copy.identity.0
        ))
    })?;
    marker::write(&superblock_path(state), identity, zig_state(copy.marker)).map_err(|code| {
        if code == marker::CORRUPT {
            boot_read_checksum_panic(&superblock_path(state), "the write's read");
        }
        io::Error::other(format!("the marker quorum write failed (FFI code {code})"))
    })?;
    // The compatibility projection: the single-file write, only AFTER
    // the quorum write succeeded.
    write_marker(
        state,
        identity.system_identifier(),
        identity.crash_counter(),
        copy.marker,
    )
}

/// The durable write: Windows keeps the single-file discipline.
#[cfg(target_os = "windows")]
fn commit_disk(state: &Path, copy: CopyState) -> io::Result<()> {
    let system = copy.identity.system_id().ok_or_else(|| {
        io::Error::other(format!(
            "the identity {} is not spellable: a zero half is no identity",
            copy.identity.0
        ))
    })?;
    let counter = copy.identity.crash_counter().ok_or_else(|| {
        io::Error::other(format!(
            "the identity {} is not spellable: a zero half is no identity",
            copy.identity.0
        ))
    })?;
    write_marker(state, system.get(), counter.get(), copy.marker)
}

/// The one durable marker round: the quorum write of `(identity,
/// state)` with forced I/O, then the projection mirror.
#[cfg(not(target_os = "windows"))]
fn write_round(
    state: &Path,
    identity: marker::NodeIdentity,
    round: marker::MarkerState,
) -> io::Result<()> {
    marker::write(&superblock_path(state), identity, round).map_err(|code| {
        if code == marker::CORRUPT {
            boot_read_checksum_panic(&superblock_path(state), "the write's read");
        }
        io::Error::other(format!("the marker quorum write failed (FFI code {code})"))
    })?;
    write_marker(
        state,
        identity.system_identifier(),
        identity.crash_counter(),
        engine_marker(round),
    )
}

impl LifecycleStore for GateStore {
    type Error = io::Error;

    #[cfg(unix)]
    fn read_copies(&mut self) -> Result<Option<SuperblockCopies>, Self::Error> {
        match &self.backend {
            Backend::Disk { state } => read_copies_disk(state, self.system),
            Backend::Mem(mem) => {
                let reply = mem.rpc(&serde_json::json!({"op": "read"}))?;
                match reply.get("verdict").and_then(|v| v.as_str()) {
                    Some("none") => Ok(None),
                    Some(word) => {
                        let marker = word_marker(word).ok_or_else(|| {
                            io::Error::other(format!(
                                "the bench store read named an unknown verdict '{word}'"
                            ))
                        })?;
                        let packed = reply
                            .get("incarnation")
                            .and_then(|i| i.as_u64())
                            .ok_or_else(|| {
                                io::Error::other(
                                    "the bench store read reply carried no incarnation",
                                )
                            })?;
                        let identity = NodeId(u32::try_from(packed).map_err(|_| {
                            io::Error::other(
                                "the bench store read's identity does not pack into \
                                 the pair's thirty-two bits",
                            )
                        })?);
                        Ok(Some(uniform(identity, marker)))
                    }
                    None => Err(io::Error::other(
                        "the bench store read reply carried no verdict",
                    )),
                }
            }
        }
    }

    #[cfg(not(unix))]
    fn read_copies(&mut self) -> Result<Option<SuperblockCopies>, Self::Error> {
        match &self.backend {
            Backend::Disk { state } => read_copies_disk(state, self.system),
        }
    }

    fn commit(&mut self, copies: &SuperblockCopies) -> Result<(), Self::Error> {
        // Invariant (asserted, always): the engine's rewrites are uniform
        // 4x — a non-uniform set has no representation on this store.
        let copy = copies.copies[0];
        assert!(
            copies.copies.iter().all(|one| *one == copy),
            "the boot gate's rewrites are uniform 4x"
        );
        match &self.backend {
            Backend::Disk { state } => commit_disk(state, copy),
            #[cfg(unix)]
            Backend::Mem(mem) => {
                let reply = mem.rpc(&serde_json::json!({
                    "op": "commit",
                    "incarnation": copy.identity.0,
                    "marker": marker_word(copy.marker),
                }))?;
                MemBackend::ack(&reply)
            }
        }
    }

    fn drain(&mut self) -> Result<(), Self::Error> {
        drain_sink(&mut sink_guard(&self.sink))?;
        #[cfg(unix)]
        if let Backend::Mem(mem) = &self.backend {
            let reply = mem.rpc(&serde_json::json!({"op": "drain"}))?;
            return MemBackend::ack(&reply);
        }
        Ok(())
    }
}
