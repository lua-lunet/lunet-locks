//! The durability pins at the boot gate's emission gate: the crash bump's
//! durable marker round (the next life at the running sentinel) completes
//! before the driver releases the first announcement. Real markers, real
//! quorum-of-copies files, real store failures.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use lunet_advisory_lock::Node;
use lunet_locks_aof::marker as marker_ffi;
use vrr::ids::{CrashCounter, NodeId, SystemId};

fn workdir(name: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock is after Unix epoch")
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "lunet-emission-gate-{name}-{}-{unique}",
        std::process::id()
    ));
    fs::create_dir_all(&dir).expect("tempdir root");
    dir
}

/// The lawful provisioned descriptor: node k's id is the packed pair
/// (system k, crash counter 1).
fn members_string(systems: &[u16]) -> String {
    systems
        .iter()
        .enumerate()
        .map(|(index, system)| format!("{}:n{}", ((*system as u32) << 16) | 1, index + 1))
        .collect::<Vec<_>>()
        .join("\0")
}

/// Plants a crashed marker (the running sentinel) at (system, crash) and
/// returns the state path.
fn plant_crashed_marker(dir: &Path, system: u16, crash: u16) -> PathBuf {
    let state = dir.join("state");
    let identity = marker_ffi::NodeIdentity::new(system, crash).expect("lawful pair");
    marker_ffi::format(
        &dir.join("state.superblock"),
        identity,
        marker_ffi::MarkerState::Unflushed,
    )
    .expect("the fresh format seats the marker");
    state
}

/// The emission gate: a failed marker write at the crash bump emits
/// nothing. The store seam fails (a copy-free crashed projection inside
/// an unwritable directory: the classification reads the projection, and
/// the bump round's copies write cannot), so the boot gate's bump round
/// cannot complete: the boot refuses, there is no node, and no
/// announcement is ever released.
#[test]
fn a_failed_marker_write_at_the_crash_bump_emits_nothing() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// A double crash never re-derives the same identity: two consecutive
/// crash boots against the same marker announce strictly successive lives
/// of the same system, both lawful.
#[test]
fn a_double_crash_never_rederives_the_same_identity() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The announced identity's halves: the system half is the descriptor's
/// system id and the crash half is the marker's next life.
#[test]
fn the_announced_identity_names_the_descriptor_system_and_the_markers_next_life() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
