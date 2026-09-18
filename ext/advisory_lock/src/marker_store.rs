//! The routed lifecycle marker: the engine's `LifecycleStore` over the
//! superblock copies.
//!
//! The engine (`vrr::lifecycle`, uvrr-core v0.7.0) owns the marker
//! machine — which marker, which copies, when, and in what order
//! (`docs/uvrr-boot-gate.md` §3). This module is the host's durable
//! mechanics: the vendored Zig store's quorum-of-copies construction,
//! reached through the AOF C ABI's marker exports (`ext/lunet-locks-aof`),
//! plus the item08 single-file compatibility projection.
//!
//! - **Read** (`read_copies`) — the working quorum's verdict: the
//!   highest-sequence valid copies at the `.open` threshold (2/4). The
//!   on-disk states map onto the engine's markers: `flushed` is the
//!   drain-proven `Stopped` (a controlled ending), `stopped` is
//!   `Stopping` (the halt has begun — it vouches for nothing), and
//!   `unflushed` is the running sentinel (`Joining` — an operating or
//!   freshly-latched process). An existing copies file that cannot be
//!   read to a verdict is an error — the boot refuses, it never falls
//!   back to the projection. When the copies never existed (no
//!   superblock file), the single file is the boot input: the legacy
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

/// The host's durable mechanics for the boot gate: the superblock quorum
/// store plus the single-file projection, wired to the committed-
/// transition sink for the halt's drain.
pub(crate) struct GateStore {
    state: PathBuf,
    sink: SinkDoor,
}

impl GateStore {
    pub(crate) fn new(state: &Path, sink: SinkDoor) -> GateStore {
        GateStore {
            state: state.to_path_buf(),
            sink,
        }
    }
}

impl LifecycleStore for GateStore {
    type Error = io::Error;

    #[cfg(not(target_os = "windows"))]
    fn read_copies(&mut self) -> Result<Option<SuperblockCopies>, Self::Error> {
        let superblock = superblock_path(&self.state);
        if !superblock.exists() {
            // The copies never existed: the single file is the boot input
            // (the legacy migration — the first routed write seeds the
            // copies from the file's own state). Neither storage present
            // is the first life, which has no durable identity yet.
            if !self.state.exists() {
                return Ok(None);
            }
            let (incarnation, marker) = read_marker(&self.state)?;
            return Ok(Some(uniform(Incarnation(incarnation), marker)));
        }
        // The copies exist: they are the authoritative read. An unreadable
        // shape (rotted beyond the read quorum, a fork, any refusal) is an
        // error — the boot refuses rather than guessing an identity or
        // falling back to the projection.
        let classified = lunet_locks_aof::marker::classify(&superblock).map_err(|code| {
            io::Error::other(format!("the marker quorum read failed (FFI code {code})"))
        })?;
        Ok(Some(uniform(
            Incarnation(classified.incarnation),
            engine_marker(classified.state),
        )))
    }

    #[cfg(target_os = "windows")]
    fn read_copies(&mut self) -> Result<Option<SuperblockCopies>, Self::Error> {
        if !self.state.exists() {
            return Ok(None);
        }
        let (incarnation, marker) = read_marker(&self.state)?;
        Ok(Some(uniform(Incarnation(incarnation), marker)))
    }

    fn commit(&mut self, copies: &SuperblockCopies) -> Result<(), Self::Error> {
        // Invariant (asserted, always): the engine's rewrites are uniform
        // 4x — a non-uniform set has no representation on this store.
        let copy = copies.copies[0];
        assert!(
            copies.copies.iter().all(|one| *one == copy),
            "the boot gate's rewrites are uniform 4x"
        );
        let identity = copy.identity.0;
        #[cfg(not(target_os = "windows"))]
        lunet_locks_aof::marker::write(
            &superblock_path(&self.state),
            identity,
            zig_state(copy.marker),
        )
        .map_err(|code| {
            io::Error::other(format!("the marker quorum write failed (FFI code {code})"))
        })?;
        // The compatibility projection: the item08 single-file write,
        // exactly as before, only AFTER the quorum write succeeded.
        write_marker(&self.state, identity, copy.marker)
    }

    fn drain(&mut self) -> Result<(), Self::Error> {
        drain_sink(&mut sink_guard(&self.sink))
    }
}
