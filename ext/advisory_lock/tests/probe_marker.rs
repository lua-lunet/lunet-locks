use lunet_locks_aof::marker as marker_ffi;
#[test]
fn probe() {
    let dir = std::env::temp_dir().join("lunet-probe-marker");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let state = dir.join("state");
    let identity = marker_ffi::NodeIdentity::new(1, 1).unwrap();
    marker_ffi::format(
        &dir.join("state.superblock"),
        identity,
        marker_ffi::MarkerState::Unflushed,
    )
    .unwrap();
    eprintln!("superblock exists: {}", dir.join("state.superblock").exists());
    eprintln!("state exists: {}", state.exists());
    eprintln!("classify: {:#?}", marker_ffi::classify(&dir.join("state.superblock")));
    let members = "65537:a\00131073:b\00196609:c";
    let boot = lunet_advisory_lock::Node::open(members, "a", state.to_str().unwrap(), None, 0);
    eprintln!("boot code: {:?}", boot.as_ref().err());
    std::fs::read_to_string(&dir.join("state")).map(|t| eprintln!("projection: {t:?}"));
}
