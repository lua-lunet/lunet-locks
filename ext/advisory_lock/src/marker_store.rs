//! The routed lifecycle marker: the superblock copies are authoritative.
//!
//! The item08 lifecycle rode ONE fsynced flag file — exactly the
//! construction the uVRR termination-obligations contract (v0.6.1) §4
//! warns "is not reliable enough" (Pillai et al., OSDI 2014; Chidambaram
//! et al., SOSP 2013: real filesystems and flushes fail to deliver what
//! applications assume). The authoritative storage is now the vendored
//! Zig store's quorum-of-copies construction, reached through the AOF C
//! ABI's marker exports (`ext/lunet-locks-aof`):
//!
//! - **Write** — every lifecycle transition quorum-writes
//!   `(incarnation, state)`: four fixed sector-aligned Aegis-checksummed
//!   copies, hash-chained sequence/parent, forced I/O (the fsync lands
//!   before the write reports success), verified against the `.verify`
//!   threshold (3/4). The single-file marker then mirrors the state
//!   (fsync+rename+dir-sync, unchanged) — the compatibility projection,
//!   written only after the quorum write succeeded.
//! - **Classify** — boot reads the working quorum (the `.open` threshold,
//!   highest sequence wins): `stopped`/`flushed` is a controlled ending
//!   (a partial marker write whose quorum advanced reads CLEAN — the
//!   advanced copies are truthful); `unflushed` (the running sentinel) is
//!   a crash (DIRTY bump). A lying or stale single copy cannot decide the
//!   read; a write that did not reach its quorum is invisible to it.
//!   The classification logic is the item08 logic; only its storage
//!   moved.
//!
//! # On-disk layout
//!
//! The superblock copies live in a sibling file, `<state>.superblock`.
//! The single `<incarnation> <flushed|unflushed|stopped>` text file stays
//! exactly as item08 wrote it — it is the projection legacy rigs and
//! operators read, and the fallback when the copies predate this routing
//! (migration: the first routed write seeds the copies from the file's
//! own state) or lose their quorum (fail conservative: the projection
//! lags the copies by at most one transition, so it can only ever
//! classify MORE conservatively than the truth, never less).
//!
//! Windows keeps the item08 single-file discipline: the vendored AOF
//! build is unix-only, and no Windows asset is packaged.
use std::io;
use std::path::{Path, PathBuf};

use crate::ffi::{Marker, read_marker, write_marker};

/// The superblock-copies file for a state path.
pub(super) fn superblock_path(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".superblock");
    PathBuf::from(os)
}

/// One lifecycle transition through the routed storage: the quorum write
/// first (authoritative, forced I/O), then the compatibility projection
/// (fsync+rename+dir-sync, unchanged from item08). A projection failure
/// reports the error — the disk is faulting; the copies already vouch for
/// the state, and the next boot re-corrects the projection.
#[cfg(not(target_os = "windows"))]
pub(super) fn write(state: &Path, incarnation: u64, marker: Marker) -> io::Result<()> {
    let code = match marker {
        Marker::Unflushed => lunet_locks_aof::marker::MarkerState::Unflushed,
        Marker::Stopped => lunet_locks_aof::marker::MarkerState::Stopped,
        Marker::Flushed => lunet_locks_aof::marker::MarkerState::Flushed,
    };
    lunet_locks_aof::marker::write(&superblock_path(state), incarnation, code).map_err(|code| {
        io::Error::other(format!("the marker quorum write failed (FFI code {code})"))
    })?;
    // The compatibility projection: the item08 single-file write, exactly
    // as before, only AFTER the quorum write succeeded.
    write_marker(state, incarnation, marker)
}

/// The boot-time current state: the working quorum's classification when
/// the copies exist; otherwise the single file's state (the legacy
/// migration path — the file's state is what the pre-routing process left
/// behind; the first routed write seeds the copies); `None` when neither
/// storage exists (a first boot).
///
/// A lost quorum (rotted beyond the read threshold), a fork, or any other
/// unreadable-copies shape falls back to the projection: the copies are
/// unreadable, the projection is the last honest evidence, and it can
/// only ever classify more conservatively than the truth.
#[cfg(not(target_os = "windows"))]
pub(super) fn current(state: &Path) -> io::Result<Option<(u64, Marker)>> {
    let superblock = superblock_path(state);
    if superblock.exists() {
        match lunet_locks_aof::marker::classify(&superblock) {
            Ok(classified) => {
                let marker = match classified.state {
                    lunet_locks_aof::marker::MarkerState::Unflushed => Marker::Unflushed,
                    lunet_locks_aof::marker::MarkerState::Stopped => Marker::Stopped,
                    lunet_locks_aof::marker::MarkerState::Flushed => Marker::Flushed,
                };
                return Ok(Some((classified.incarnation, marker)));
            }
            Err(code) => {
                eprintln!(
                    "lunet-advisory-lock: the marker superblock copies are unreadable \
                     (FFI code {code}); falling back to the single-file projection"
                );
            }
        }
    }
    if state.exists() {
        return read_marker(state).map(Some);
    }
    Ok(None)
}

/// Windows keeps the item08 single-file marker (the vendored AOF build is
/// unix-only, so there is no quorum store to route through).
#[cfg(target_os = "windows")]
pub(super) fn write(state: &Path, incarnation: u64, marker: Marker) -> io::Result<()> {
    write_marker(state, incarnation, marker)
}

#[cfg(target_os = "windows")]
pub(super) fn current(state: &Path) -> io::Result<Option<(u64, Marker)>> {
    if state.exists() {
        return read_marker(state).map(Some);
    }
    Ok(None)
}
