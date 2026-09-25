//! The durability pins at the boot gate's emission gate: the crash bump's
//! durable marker round (the next life at the running sentinel) completes
//! before the driver releases the first announcement. Real markers, real
//! quorum-of-copies files, real store failures.

use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use lunet_advisory_lock::{Node, OK};
use lunet_locks_aof::marker as marker_ffi;
use vrr::ids::{NodeId, SystemId, CrashCounter};

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
        .map(|(index, system)| {
            format!("{}:n{}", ((*system as u32) << 16) | 1, index + 1)
        })
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
    let dir = workdir("failed-write");
    let store_dir = dir.join("state");
    fs::create_dir_all(&store_dir).expect("store dir");
    let state = store_dir.join("state");
    // The crashed evidence, copy-free: the projection's running sentinel
    // is the boot input, and the bump round must seed the copies.
    fs::write(&state, "1 1 unflushed\n").expect("the projection");

    let members = members_string(&[1, 2, 3]);
    let state_str = state.to_str().expect("utf8 path").to_owned();

    // The store seam fails: the directory refuses every create.
    let mut permissions = fs::metadata(&store_dir).expect("stat").permissions();
    permissions.set_mode(0o555);
    fs::set_permissions(&store_dir, permissions).expect("chmod");

    let boot = Node::open(&members, "n1", &state_str, None, 0);
    assert!(
        boot.is_err(),
        "the crash bump's marker write failure refuses the boot: no node, \
         so zero announcements are released"
    );

    // The failed round did not advance the life and did not half-land:
    // the projection still names the crashed identity, no copies exist.
    assert_eq!(
        fs::read_to_string(&state).unwrap(),
        "1 1 unflushed\n",
        "the failed round advanced nothing"
    );
    assert!(
        !store_dir.join("state.superblock").exists(),
        "the failed round left no copies behind"
    );

    // Restore and confirm the gate was the refusal's cause: the same
    // boot with a writable store opens, and the bump round is durable
    // on disk before the announcement queues.
    let mut permissions = fs::metadata(&store_dir).expect("stat").permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(&store_dir, permissions).expect("chmod");
    let mut node = Node::open(&members, "n1", &state_str, None, 0).expect("the boot opens");
    assert_eq!(node.own_id(), 65538, "the next life of the crashed pair");
    let classified =
        marker_ffi::classify(&store_dir.join("state.superblock")).expect("the marker reads");
    assert_eq!(
        (
            classified.identity.system_identifier(),
            classified.identity.crash_counter(),
            classified.state,
        ),
        (1, 2, marker_ffi::MarkerState::Unflushed),
        "the bump round is durable before the announcement queues"
    );
    drop(node);
    let _ = fs::remove_dir_all(&dir);
    let _ = OK;
}

/// A double crash never re-derives the same identity: two consecutive
/// crash boots against the same marker announce strictly successive lives
/// of the same system, both lawful.
#[test]
fn a_double_crash_never_rederives_the_same_identity() {
    let dir = workdir("double-crash");
    let state = plant_crashed_marker(&dir, 2, 1);
    let members = members_string(&[1, 2, 3]);
    let state_str = state.to_str().expect("utf8 path").to_owned();

    let first_boot = Node::open(&members, "n2", &state_str, None, 0).expect("first crash boot");
    let first = NodeId::from(first_boot.own_id());
    drop(first_boot);

    let second_boot = Node::open(&members, "n2", &state_str, None, 0).expect("second crash boot");
    let second = NodeId::from(second_boot.own_id());
    assert!(first.is_lawful(), "the first announced identity is lawful");
    assert!(second.is_lawful(), "the second announced identity is lawful");
    assert_eq!(
        second,
        first.next_life().expect("the first life has a next"),
        "the second crash announces the strictly next life of the first"
    );
    assert_eq!(
        second.system_id().map(SystemId::get),
        first.system_id().map(SystemId::get),
        "the system half never moves"
    );
    let _ = fs::remove_dir_all(&dir);
}

/// The announced identity's halves: the system half is the descriptor's
/// system id and the crash half is the marker's next life.
#[test]
fn the_announced_identity_names_the_descriptor_system_and_the_markers_next_life() {
    let dir = workdir("halves");
    let state = plant_crashed_marker(&dir, 3, 5);
    let members = members_string(&[3, 2, 1]);
    let state_str = state.to_str().expect("utf8 path").to_owned();

    let node = Node::open(&members, "n1", &state_str, None, 0).expect("the boot opens");
    let announced = NodeId::from(node.own_id());
    assert_eq!(
        announced.system_id().map(SystemId::get),
        Some(3),
        "the system half is the descriptor's system id"
    );
    assert_eq!(
        announced.crash_counter().map(CrashCounter::get),
        Some(6),
        "the crash half is the marker's next life"
    );
    assert_eq!(announced, NodeId::new(SystemId::new(3).unwrap(), CrashCounter::new(6).unwrap()));
    let _ = fs::remove_dir_all(&dir);
}
