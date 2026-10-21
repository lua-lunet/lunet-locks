//! The routed lifecycle marker: the engine's `LifecycleStore` over the
//! superblock copies, plus the single-file compatibility projection. THE
//! BOOT-READ SAFETY LAW: a checksum failure on ANY copy is a loud log and
//! a PANIC — never cleared, never repaired, never fallen back; deleting
//! the marker file is the recovery path.
use std::io;
#[cfg(unix)]
use std::io::{BufRead, BufReader, Write};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

use vrr::ids::{CrashCounter, NodeId, SystemId};
use vrr::lifecycle::{CopyState, LifecycleStore, Marker, SuperblockCopies};

use crate::ffi::{JournalSink, ViewRecord, read_marker, read_view_record, write_marker};

#[cfg(not(target_os = "windows"))]
use tbio::marker;

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
/// checksum — never cleared, never repaired, never fallen back; delete
/// the marker file to re-seed from the compatibility projection.
fn boot_read_checksum_panic(superblock: &Path, what: &str) -> ! {
    panic!(
        "BOOT-READ SAFETY LAW: {what} of {} found a bad marker checksum \
         (the store's refusal code); the marker store is never cleared, \
         never repaired, never fallen back — delete the marker file to \
         re-seed from the compatibility projection",
        superblock.display(),
    )
}

/// The engine marker's on-disk Zig state (`Stopping` → `Stopped`,
/// `Stopped` → `Flushed`, the latches → `Unflushed`).
#[cfg(not(target_os = "windows"))]
fn zig_state(marker: Marker) -> tbio::marker::MarkerState {
    use tbio::marker::MarkerState;
    match marker {
        Marker::Stopping => MarkerState::Stopped,
        Marker::Stopped => MarkerState::Flushed,
        Marker::Restarting | Marker::Joining => MarkerState::Unflushed,
    }
}

/// The projection's word for an engine marker (the on-disk spelling the
/// single file has always carried).
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
fn engine_marker(state: tbio::marker::MarkerState) -> Marker {
    use tbio::marker::MarkerState;
    match state {
        MarkerState::Unflushed => Marker::Joining,
        MarkerState::Stopped => Marker::Stopping,
        MarkerState::Flushed => Marker::Stopped,
    }
}

/// Four uniform copies: the store's read resolved one verdict.
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

/// The engine marker a bench read reply names (the sentinels read as
/// `Joining`).
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
    /// The driver's socket, one wire discipline behind one lock.
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

/// The store backend: the real application's durable mechanics (`Disk`),
/// the bench harness's force-feed (`Mem`), or the embedder's mounted
/// fence (`Mounted`).
#[cfg(unix)]
enum Backend {
    Disk { state: PathBuf },
    Mem(MemBackend),
    Mounted(MountedFence),
}

/// The durable-only backend: non-unix builds carry no bench.
#[cfg(not(unix))]
enum Backend {
    Disk { state: PathBuf },
    Mounted(MountedFence),
}

/// The mounted fence, in both builds: the embedder's own
/// [`LifecycleStore`], handed to [`GateStore::over`]. The mutex is the
/// `&mut` the trait's contract names; `Send + Sync` because the node
/// carries it the way it carries its other seams.
type MountedFence = Mutex<Box<dyn LifecycleStore<Error = io::Error> + Send + Sync>>;

/// Locks the mounted fence. A poisoned lock is unlocked: the fence's
/// durable state is the embedder's own machine's.
fn fence_guard(
    fence: &MountedFence,
) -> MutexGuard<'_, Box<dyn LifecycleStore<Error = io::Error> + Send + Sync>> {
    fence.lock().unwrap_or_else(PoisonError::into_inner)
}

/// The engine marker's name, spelled as the engine's own vocabulary —
/// the word the marker-round schedule records.
fn machine_word(marker: Marker) -> &'static str {
    match marker {
        Marker::Stopping => "Stopping",
        Marker::Stopped => "Stopped",
        Marker::Restarting => "Restarting",
        Marker::Joining => "Joining",
    }
}

/// The host's durable mechanics for the boot gate: the superblock quorum
/// store plus the single-file projection — or the bench harness's
/// in-memory driver store, or the embedding constructor's mounted fence
/// ([`GateStore::over`]). The `ops` log records every marker round the
/// machine wrote and the drain the halt forces, in write order.
pub(crate) struct GateStore {
    backend: Backend,
    sink: SinkDoor,
    system: u16,
    ops: Arc<Mutex<Vec<String>>>,
}

impl GateStore {
    pub(crate) fn new(
        state: &Path,
        sink: SinkDoor,
        system: u16,
        ops: Arc<Mutex<Vec<String>>>,
    ) -> GateStore {
        GateStore {
            backend: Backend::Disk {
                state: state.to_path_buf(),
            },
            sink,
            system,
            ops,
        }
    }

    /// The mounted fence: the embedder's own boot-gate store, handed up
    /// by the embedding constructor.
    pub(crate) fn over(
        fence: Box<dyn LifecycleStore<Error = io::Error> + Send + Sync>,
        sink: SinkDoor,
        ops: Arc<Mutex<Vec<String>>>,
    ) -> GateStore {
        GateStore {
            backend: Backend::Mounted(Mutex::new(fence)),
            sink,
            system: 0,
            ops,
        }
    }

    /// The bench-harness store: every engine call is an RPC to the
    /// harness driver, which holds the identity's state in memory and
    /// fails the run on an unsignalled or out-of-order call.
    #[cfg(unix)]
    pub(crate) fn mem(
        ctl: &Path,
        sink: SinkDoor,
        ops: Arc<Mutex<Vec<String>>>,
    ) -> io::Result<GateStore> {
        Ok(GateStore {
            backend: Backend::Mem(MemBackend::connect(ctl)?),
            sink,
            system: 0,
            ops,
        })
    }

    /// The emission gate's one durable round: the crash bump's marker
    /// write — the next life at the running sentinel — fsync-complete
    /// before the driver releases the first announcement. A zero half is
    /// no identity and refuses here.
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
            Backend::Mounted(fence) => {
                fence_guard(fence).commit(&uniform(identity, Marker::Joining))
            }
        }
    }

    /// The clean classification's view record (the stop's drain-window
    /// write); `None` when no stop ever wrote one, or when the backend is
    /// the mem or mounted fence.
    #[cfg(unix)]
    pub(crate) fn view_record(&self) -> io::Result<Option<ViewRecord>> {
        match &self.backend {
            Backend::Disk { state } => read_view_record(state),
            Backend::Mem(_) | Backend::Mounted(_) => Ok(None),
        }
    }

    #[cfg(not(unix))]
    pub(crate) fn view_record(&self) -> io::Result<Option<ViewRecord>> {
        match &self.backend {
            Backend::Disk { state } => read_view_record(state),
            Backend::Mounted(_) => Ok(None),
        }
    }
}

/// The durable read: the working quorum's verdict, or the compatibility
/// projection's while the copies predate this routing.
#[cfg(not(target_os = "windows"))]
fn read_copies_disk(state: &Path, system: u16) -> io::Result<Option<SuperblockCopies>> {
    let superblock = superblock_path(state);
    if !superblock.exists() {
        // The copies never existed: the single file is the boot input
        // (the legacy migration); neither storage present is the first
        // life, which has no durable identity yet.
        if !state.exists() {
            crate::trace_line!("marker.read.absent");
            return Ok(None);
        }
        let (file_system, crash, marker) = match read_marker(state) {
            Ok(pair) => pair,
            Err(error) => {
                crate::trace_line!("marker.read.projection-refused");
                return Err(error);
            }
        };
        if file_system != system {
            crate::trace_line!("marker.read.cross-system");
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
        crate::trace_line!("marker.read.projection");
        return Ok(Some(uniform(identity, marker)));
    }
    // An old-format marker (INCOMPATIBLE) is invalid, never converted;
    // any other unreadable shape is an error — the boot refuses.
    let classified = marker::classify(&superblock).map_err(|code| {
        if code == marker::CORRUPT {
            crate::trace_line!("marker.read.corrupt");
            boot_read_checksum_panic(&superblock, "the quorum read");
        }
        if code == marker::INCOMPATIBLE {
            crate::trace_line!("marker.read.incompatible");
            return io::Error::other(format!(
                "the marker is an old-format file (FFI code {code}): invalid, never converted"
            ));
        }
        crate::trace_line!("marker.read.torn");
        io::Error::other(format!("the marker quorum read failed (FFI code {code})"))
    })?;
    crate::trace_line!("marker.read.quorum");
    Ok(Some(uniform(
        NodeId(classified.identity.packed()),
        engine_marker(classified.state),
    )))
}

/// The durable read: Windows keeps the single-file discipline.
#[cfg(target_os = "windows")]
fn read_copies_disk(state: &Path, system: u16) -> io::Result<Option<SuperblockCopies>> {
    if !state.exists() {
        crate::trace_line!("marker.read.absent");
        return Ok(None);
    }
    let (file_system, crash, marker) = match read_marker(state) {
        Ok(pair) => pair,
        Err(error) => {
            crate::trace_line!("marker.read.projection-refused");
            return Err(error);
        }
    };
    if file_system != system {
        crate::trace_line!("marker.read.cross-system");
        return Err(io::Error::other(format!(
            "the projection names system {file_system} but the descriptor names system {system}"
        )));
    }
    let identity = NodeId::new(
        SystemId::new(file_system).expect("the projection refuses a zero half"),
        CrashCounter::new(crash).expect("the projection refuses a zero half"),
    );
    crate::trace_line!("marker.read.projection");
    Ok(Some(uniform(identity, marker)))
}

/// The durable write: the quorum write, then the projection mirror.
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
            crate::trace_line!("marker.write.corrupt");
            boot_read_checksum_panic(&superblock_path(state), "the write's read");
        }
        io::Error::other(format!("the marker quorum write failed (FFI code {code})"))
    })?;
    // The compatibility projection: only AFTER the quorum write succeeded.
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
            crate::trace_line!("marker.write.corrupt");
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
        match &mut self.backend {
            Backend::Disk { state } => read_copies_disk(state, self.system),
            Backend::Mounted(fence) => fence_guard(fence).read_copies(),
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
        match &mut self.backend {
            Backend::Disk { state } => read_copies_disk(state, self.system),
            Backend::Mounted(fence) => fence_guard(fence).read_copies(),
        }
    }

    fn commit(&mut self, copies: &SuperblockCopies) -> Result<(), Self::Error> {
        // Invariant (asserted, always): the engine's rewrites are uniform 4x.
        let copy = copies.copies[0];
        assert!(
            copies.copies.iter().all(|one| *one == copy),
            "the boot gate's rewrites are uniform 4x"
        );
        let result = match &mut self.backend {
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
            Backend::Mounted(fence) => fence_guard(fence).commit(copies),
        };
        if result.is_ok() {
            self.ops
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(format!(
                    "commit:{}@{}",
                    machine_word(copy.marker),
                    copy.identity.0
                ));
        }
        result
    }

    fn drain(&mut self) -> Result<(), Self::Error> {
        // The sink drain is the node's own obligation; the mounted
        // fence's `drain` rides the same halt seat beside it.
        let result =
            drain_sink(&mut sink_guard(&self.sink)).and_then(|()| match &mut self.backend {
                Backend::Mounted(fence) => fence_guard(fence).drain(),
                Backend::Disk { .. } => Ok(()),
                #[cfg(unix)]
                Backend::Mem(mem) => {
                    let reply = mem.rpc(&serde_json::json!({"op": "drain"}))?;
                    MemBackend::ack(&reply)
                }
            });
        if result.is_ok() {
            self.ops
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push("drain".to_string());
        }
        result
    }
}
