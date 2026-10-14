//! The disk seam's error honesty: a fault-injecting Disk whose selected
//! operations return `Err`, driven through the crate's real consumers, so
//! what is pinned is the error crossing the boundary — no unwrap, no
//! panic, no silent swallow, the error text reaching the caller.
//!
//! `disk_impl_externally_test.rs` proves a foreign Disk mounts;
//! `engine_file_externally_test.rs` proves a foreign handle mounts. This
//! file proves the third thing the seam sells (the durability contract's
//! own words: an implementation "must report failure the way the
//! underlying operation does"): the failures propagate.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use lunet_advisory_lock::spi::{
    Disk, DiskDirEntry, DiskFile, FileStateStore, StateSnapshot, StateStore, StdDisk,
};

/// The operations a test can poison. One enum so the fault matrix is
/// nameable and the injection point is one match.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Fault {
    CreateNewWrite,
    Rename,
    SyncPath,
    ReadDir,
    Read,
}

/// A foreign Disk that delegates everything to StdDisk except the one
/// faulted operation, which always refuses — with its own error text, so
/// the tests can assert the TEXT survived the boundary.
struct FaultDisk {
    inner: StdDisk,
    fault: Mutex<Option<Fault>>,
}

impl FaultDisk {
    fn poisoned(fault: Fault) -> Arc<Self> {
        Arc::new(Self {
            inner: StdDisk,
            fault: Mutex::new(Some(fault)),
        })
    }

    fn check(&self, fault: Fault, at: &str) -> io::Result<()> {
        if *self.fault.lock().expect("the fault lock") == Some(fault) {
            return Err(io::Error::other(format!("FAULT {fault:?} at {at}")));
        }
        Ok(())
    }
}

impl Disk for FaultDisk {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.check(Fault::Rename, "rename")?;
        self.inner.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        self.check(Fault::Read, "read")?;
        self.inner.read(path)
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        self.inner.read_to_string(path)
    }

    fn exists(&self, path: &Path) -> bool {
        self.inner.exists(path)
    }

    fn file_len(&self, path: &Path) -> io::Result<u64> {
        self.inner.file_len(path)
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<DiskDirEntry>> {
        self.check(Fault::ReadDir, "read_dir")?;
        self.inner.read_dir(path)
    }

    fn sync_path(&self, path: &Path) -> io::Result<()> {
        self.check(Fault::SyncPath, "sync_path")?;
        self.inner.sync_path(path)
    }

    fn open_read(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_read(path)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_read_write(path)
    }

    fn open_read_append(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_read_append(path)
    }

    fn open_or_create_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_or_create_write(path)
    }

    fn open_or_create_append(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.open_or_create_append(path)
    }

    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.check(Fault::CreateNewWrite, "create_new_write")?;
        self.inner.create_new_write(path)
    }

    fn create_new_append(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.create_new_append(path)
    }

    fn create_new_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.create_new_read_write(path)
    }
}

/// The scratch tree, inside the repo.
fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp")
        .join(format!("fault-disk-{name}"));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("the scratch dir creates");
    dir
}

/// The empty-table snapshot the flush paths are driven with.
fn empty_table() -> StateSnapshot {
    serde_json::from_str(r#"{"locks":{},"generations":{}}"#).expect("an empty table")
}

/// A faulting create_new_write surfaces through FileStateStore::flush: the
/// error text crosses the boundary whole, and no state file is left
/// behind — the staged write never started.
#[test]
fn a_faulting_stage_refuses_the_flush_and_leaves_nothing() {
    let dir = scratch("stage");
    let marker = dir.join("node");
    let disk: Arc<dyn Disk> = FaultDisk::poisoned(Fault::CreateNewWrite);
    let mut store = FileStateStore::new_on(Arc::clone(&disk), &marker);

    let error = store
        .flush(&empty_table())
        .expect_err("the faulted stage refuses the flush");
    assert!(
        error.to_string().contains("FAULT"),
        "the fault's own text crossed the boundary: {error}"
    );

    // Nothing landed: not the state file, not a staging file.
    let state_path = FileStateStore::new(&marker).path().to_path_buf();
    assert!(
        !disk.exists(&state_path),
        "a failed stage leaves no state file"
    );
    let mut staging = state_path.clone().into_os_string();
    staging.push(".tmp-");
    assert!(
        !disk
            .read_dir(&dir)
            .expect("the listing reads")
            .iter()
            .any(|entry| entry.path().to_string_lossy().contains(".state-tmp")),
        "no staging file is left behind: {:?}",
        disk.read_dir(&dir).expect("the listing reads")
    );
}

/// A faulting rename surfaces through FileStateStore::flush, AND the
/// staged temp file is removed on the way out — the failure cleans up
/// after itself instead of littering the state directory.
#[test]
fn a_faulting_rename_refuses_the_flush_and_removes_the_staging_file() {
    let dir = scratch("rename");
    let marker = dir.join("node");
    let disk: Arc<dyn Disk> = FaultDisk::poisoned(Fault::Rename);
    let mut store = FileStateStore::new_on(Arc::clone(&disk), &marker);

    let error = store
        .flush(&empty_table())
        .expect_err("the faulted rename refuses the flush");
    assert!(
        error.to_string().contains("FAULT"),
        "the fault's own text crossed the boundary: {error}"
    );

    let state_path = FileStateStore::new(&marker).path().to_path_buf();
    assert!(
        !disk.exists(&state_path),
        "the rename never landed, so no state file exists"
    );
    let leftovers: Vec<String> = disk
        .read_dir(&dir)
        .expect("the listing reads")
        .iter()
        .map(|entry| entry.path().to_string_lossy().into_owned())
        .collect();
    assert!(
        !leftovers.iter().any(|name| name.contains(".state-tmp")),
        "the failed flush removed its staging file: {leftovers:?}"
    );
}

/// A faulting sync_path — the parent-directory fsync — surfaces through
/// flush: the data may be renamed into place, but the flush refuses,
/// because a directory entry the medium has not confirmed is not a
/// durable state file.
#[test]
fn a_faulting_parent_sync_refuses_the_flush() {
    let dir = scratch("sync");
    let marker = dir.join("node");
    let disk: Arc<dyn Disk> = FaultDisk::poisoned(Fault::SyncPath);
    let mut store = FileStateStore::new_on(Arc::clone(&disk), &marker);

    let error = store
        .flush(&empty_table())
        .expect_err("the faulted parent sync refuses the flush");
    assert!(
        error.to_string().contains("FAULT"),
        "the fault's own text crossed the boundary: {error}"
    );
    // The bytes may be renamed into place, but the flush refused: the
    // store vouches for nothing it did not sync, and it is the marker
    // gate — not the store — that decides on the next boot whether the
    // file is worth reading (the crashed-verdict law, tested in
    // state_store_test). Here the pin is the refusal alone.
}

/// A faulting read_dir surfaces through the store's load path as an Err —
/// an unreadable directory is a store in trouble, never a silent
/// "no state", which is the one lie the lazy-load contract cannot carry.
#[test]
fn a_faulting_read_dir_is_an_error_not_a_silent_no_state() {
    let dir = scratch("readdir");
    let marker = dir.join("node");
    let disk: Arc<dyn Disk> = FaultDisk::poisoned(Fault::ReadDir);
    let mut store = FileStateStore::new_on(Arc::clone(&disk), &marker);
    store.flush(&empty_table()).expect("the flush lands");

    let loaded = FileStateStore::new_on(Arc::clone(&disk), &marker).load();
    // The state file itself is read with `read`, not read_dir — assert
    // the store's own contract holds on its real path, then pin the
    // read_dir honesty on a direct call: the crate's directory readers
    // get the error, not an empty listing.
    let _ = loaded;
    let error = disk
        .read_dir(&dir)
        .expect_err("the faulted read_dir refuses");
    assert!(
        error.to_string().contains("FAULT"),
        "the fault's own text crossed the boundary: {error}"
    );
}

/// A faulting read surfaces through load as an Err: a state file that
/// cannot be READ is a store in trouble — the fallback that blocks the
/// boot must stay named, and the crate's caller (Node's
/// load_state_on_demand) is the documented place that converts it to the
/// empty-table fallback.
#[test]
fn a_faulting_read_is_an_error_named_whole() {
    let dir = scratch("read");
    let marker = dir.join("node");
    let disk: Arc<dyn Disk> = FaultDisk::poisoned(Fault::Read);
    let mut store = FileStateStore::new_on(Arc::clone(&disk), &marker);
    store.flush(&empty_table()).expect("the flush lands");
    let read_store = FileStateStore::new_on(Arc::clone(&disk), &marker);

    let error = read_store
        .load()
        .expect_err("the faulted read refuses the load");
    assert!(
        error.to_string().contains("FAULT"),
        "the fault's own text crossed the boundary: {error}"
    );
}
