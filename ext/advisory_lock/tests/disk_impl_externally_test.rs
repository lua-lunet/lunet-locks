//! The seam hole closed: a `Disk` implementation written OUTSIDE the
//! crate.
//!
//! This file is its own crate. Every name it can reach is the library's
//! public surface: the seam's names all come through `spi` — the seam's
//! front door — and nowhere else, and the round-trip's record type is the
//! crate's public `locks` module. The local `SeamDisk` implements
//! [`Disk`] by delegating to [`StdDisk`], the smallest embedder
//! implementation there is; the store's own injection point
//! (`FileStateStore::new_on`) mounts it, and the [`StateStore`] contract
//! carries the framed round-trip over it. An out-of-crate `impl Disk`
//! compiling at all is the proof the seam's public surface is complete;
//! the behaviour assertions pin the delegation.
//!
//! The node itself has no public injection point: the `Node::open*`
//! entry points hard-default their disk, and the mounted-engine door is
//! a deferred decision. This test proves the trait and its types — not
//! a node mount.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lunet_advisory_lock::locks::Lease;
use lunet_advisory_lock::spi::{
    CommitHook, Disk, DiskDirEntry, DiskFile, FileStateStore, NoOpHook, StateSnapshot, StateStore,
    StdDisk, std_disk,
};
use uuid::Uuid;

/// The out-of-crate implementation: every method delegates to the
/// wrapped [`StdDisk`] through the trait's own surface. Nothing here
/// names a library internal — from out here there are none to name.
struct SeamDisk {
    inner: StdDisk,
}

impl SeamDisk {
    fn new() -> Self {
        Self { inner: StdDisk }
    }
}

impl Disk for SeamDisk {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        self.inner.rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.inner.remove_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
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
        self.inner.read_dir(path)
    }

    fn sync_path(&self, path: &Path) -> io::Result<()> {
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
        self.inner.create_new_write(path)
    }

    fn create_new_append(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.create_new_append(path)
    }

    fn create_new_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        self.inner.create_new_read_write(path)
    }
}

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/disk-impl-externally");
    fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    fs::create_dir_all(&dir).expect("the case directory creates");
    dir
}

#[test]
fn delegation_behaves_like_the_std_implementation() {
    let dir = scratch("delegation");
    let disk = SeamDisk::new();

    // The create-new shape through the impl, and the file-shaped
    // operations on the handle it hands back.
    let file_path = dir.join("tape.bin");
    let mut file: DiskFile = disk
        .create_new_write(&file_path)
        .expect("create-new writes");
    file.write_all(b"frame").expect("the bytes append");
    file.sync_all().expect("the bytes land");
    drop(file);

    assert!(disk.exists(&file_path));
    assert_eq!(disk.file_len(&file_path).expect("the length answers"), 5);
    assert_eq!(disk.read(&file_path).expect("the read answers"), b"frame");
    assert_eq!(
        disk.read_to_string(&file_path).expect("the text answers"),
        "frame"
    );

    // A listing hands back entries built by the public constructor: the
    // one the returned entries must equal for an embedder's engine to
    // serve its own listings.
    let entries = disk.read_dir(&dir).expect("the listing answers");
    assert_eq!(entries, vec![DiskDirEntry::new(file_path.clone())]);
    assert_eq!(entries[0].path(), file_path);

    // Rename, unlink, and the recursive removal close the set.
    let moved = dir.join("tape.moved.bin");
    disk.rename(&file_path, &moved).expect("the rename answers");
    assert!(disk.exists(&moved));
    assert!(!disk.exists(&file_path));
    let opened = disk.open_read(&moved).expect("the open answers");
    drop(opened);
    disk.remove_file(&moved).expect("the unlink answers");
    assert!(!disk.exists(&moved));
    disk.sync_path(&dir).expect("the directory fsync answers");
    disk.remove_dir_all(&dir).expect("the removal answers");
    assert!(!disk.exists(&dir));
}

#[test]
fn the_store_mounts_the_out_of_crate_disk() {
    let dir = scratch("store-mount");
    let disk: Arc<dyn Disk> = Arc::new(SeamDisk::new());
    disk.create_dir_all(&dir).expect("the scratch root creates");

    // The store's own injection point takes the out-of-crate
    // implementation as an `Arc<dyn Disk>` — the same shape a mounted
    // engine rides, through the public surface only.
    let mut store = FileStateStore::new_on(Arc::clone(&disk), &dir.join("node.marker"));

    let holder = Uuid::from_u128(0xabcd_0000_0000_0001u128);
    let snap = StateSnapshot {
        locks: BTreeMap::from([(
            1,
            Lease {
                lease_id: 7,
                holder,
                expiry: 123456,
                lease_ms: 9000,
                name: Some("tape-a".to_string()),
                labels: None,
                taken_at_ms: 42,
                renew_count: 1,
            },
        )]),
    };

    // The eager half through the trait, then the lazy half: the flushed
    // table loads back byte-identical, over the out-of-crate disk.
    store.flush(&snap).expect("the flush lands");
    assert_eq!(
        StateStore::load(&store).expect("the load answers"),
        Some(snap)
    );
}

#[test]
fn the_seam_front_door_exports_the_default_disk_and_the_hook() {
    let dir = scratch("front-door");
    let store = FileStateStore::new_on(std_disk(), &dir.join("node.marker"));

    // The default disk is nameable through `spi`, and an absent state
    // file is the ordinary cold-start answer through it.
    assert_eq!(
        StateStore::load(&store).expect("the load answers"),
        None,
        "an absent state file is `Ok(None)`, never an error"
    );

    // The hook contract and its production wiring are exportable too: a
    // hook is mounted once and lives as long as the node does.
    fn accepts(hook: &dyn CommitHook) -> bool {
        std::ptr::eq(hook, hook)
    }
    assert!(accepts(&NoOpHook));
}
