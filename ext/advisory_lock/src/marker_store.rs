//! The routed lifecycle marker: the engine's `LifecycleStore` over the
//! superblock copies.
//!
//! The engine (`vrr::lifecycle`, uvrr-core tag v0.7.4 @ e5b0a79) owns the marker
//! machine — which marker, which copies, when, and in what order
//! (`docs/uvrr-boot-gate.md` §3). This module is the host's durable
//! mechanics: the vendored Zig store's quorum-of-copies construction,
//! reached through the AOF C ABI's marker exports (`ext/lunet-locks-aof`),
//! plus the item08 single-file compatibility projection.
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
//! The single `<incarnation> <flushed|unflushed|stopped>` text file stays
//! exactly as item08 wrote it — it is the projection legacy rigs and
//! operators read, and the boot input only while the copies predate this
//! routing.
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

use vrr::lifecycle::{CopyState, Incarnation, LifecycleStore, Marker, SuperblockCopies};

use crate::ffi::{JournalSink, read_marker, write_marker};

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
fn uniform(identity: Incarnation, marker: Marker) -> SuperblockCopies {
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
    writer: UnixStream,
    reader: BufReader<UnixStream>,
}

#[cfg(unix)]
impl MemBackend {
    fn connect(path: &Path) -> io::Result<MemBackend> {
        let writer = UnixStream::connect(path)?;
        let reader = BufReader::new(writer.try_clone()?);
        Ok(MemBackend { writer, reader })
    }

    fn rpc(&mut self, request: &serde_json::Value) -> io::Result<serde_json::Value> {
        let mut line = request.to_string();
        line.push('\n');
        self.writer.write_all(line.as_bytes())?;
        self.writer.flush()?;
        let mut reply = String::new();
        let read = self.reader.read_line(&mut reply)?;
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
/// transition sink for the halt's drain.
pub(crate) struct GateStore {
    backend: Backend,
    sink: SinkDoor,
}

impl GateStore {
    pub(crate) fn new(state: &Path, sink: SinkDoor) -> GateStore {
        GateStore {
            backend: Backend::Disk {
                state: state.to_path_buf(),
            },
            sink,
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
        })
    }
}

/// The durable read: the working quorum's verdict, or the compatibility
/// projection's while the copies predate this routing.
#[cfg(not(target_os = "windows"))]
fn read_copies_disk(state: &Path) -> io::Result<Option<SuperblockCopies>> {
    let superblock = superblock_path(state);
    if !superblock.exists() {
        // The copies never existed: the single file is the boot input
        // (the legacy migration — the first routed write seeds the
        // copies from the file's own state). Neither storage present
        // is the first life, which has no durable identity yet.
        if !state.exists() {
            return Ok(None);
        }
        let (incarnation, marker) = read_marker(state)?;
        return Ok(Some(uniform(Incarnation(incarnation), marker)));
    }
    // The copies exist: they are the authoritative read. THE
    // BOOT-READ SAFETY LAW: a bad checksum on ANY copy is a loud log
    // and a PANIC (the adapter panics on the Zig store's distinct
    // refusal code) — never cleared, never repaired, never fallen
    // back, never "unclear". Any other unreadable shape (no quorum, a
    // fork, any refusal) is an error — the boot refuses rather than
    // guessing an identity or falling back to the projection.
    let classified = lunet_locks_aof::marker::classify(&superblock).map_err(|code| {
        if code == lunet_locks_aof::marker::CORRUPT {
            boot_read_checksum_panic(&superblock, "the quorum read");
        }
        io::Error::other(format!("the marker quorum read failed (FFI code {code})"))
    })?;
    Ok(Some(uniform(
        Incarnation(classified.incarnation),
        engine_marker(classified.state),
    )))
}

/// The durable read: Windows keeps the item08 single-file discipline.
#[cfg(target_os = "windows")]
fn read_copies_disk(state: &Path) -> io::Result<Option<SuperblockCopies>> {
    if !state.exists() {
        return Ok(None);
    }
    let (incarnation, marker) = read_marker(state)?;
    Ok(Some(uniform(Incarnation(incarnation), marker)))
}

/// The durable write: the quorum write, then the projection mirror.
#[cfg(not(target_os = "windows"))]
fn commit_disk(state: &Path, copy: CopyState) -> io::Result<()> {
    let identity = copy.identity.0;
    lunet_locks_aof::marker::write(&superblock_path(state), identity, zig_state(copy.marker))
        .map_err(|code| {
            if code == lunet_locks_aof::marker::CORRUPT {
                boot_read_checksum_panic(&superblock_path(state), "the write's read");
            }
            io::Error::other(format!("the marker quorum write failed (FFI code {code})"))
        })?;
    // The compatibility projection: the item08 single-file write,
    // exactly as before, only AFTER the quorum write succeeded.
    write_marker(state, identity, copy.marker)
}

/// The durable write: Windows keeps the item08 single-file discipline.
#[cfg(target_os = "windows")]
fn commit_disk(state: &Path, copy: CopyState) -> io::Result<()> {
    write_marker(state, copy.identity.0, copy.marker)
}

impl LifecycleStore for GateStore {
    type Error = io::Error;

    #[cfg(unix)]
    fn read_copies(&mut self) -> Result<Option<SuperblockCopies>, Self::Error> {
        match &mut self.backend {
            Backend::Disk { state } => read_copies_disk(state),
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
                        let incarnation = reply
                            .get("incarnation")
                            .and_then(|i| i.as_u64())
                            .ok_or_else(|| {
                                io::Error::other(
                                    "the bench store read reply carried no incarnation",
                                )
                            })?;
                        Ok(Some(uniform(Incarnation(incarnation), marker)))
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
            Backend::Disk { state } => read_copies_disk(state),
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
        match &mut self.backend {
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
        if let Backend::Mem(mem) = &mut self.backend {
            let reply = mem.rpc(&serde_json::json!({"op": "drain"}))?;
            return MemBackend::ack(&reply);
        }
        Ok(())
    }
}
