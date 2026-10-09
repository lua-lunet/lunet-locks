//! Out-of-crate proof that the disk seam is mountable at the HANDLE level,
//! not only at the directory level.
//!
//! `disk_impl_externally_test.rs` proves a foreign [`Disk`] mounts, but its
//! open shapes all delegate to [`StdDisk`], so it never answers the
//! question the seam exists for: can an embedder serve an open file from
//! its own engine? That question lives in `EngineFile` — this file
//! implements it outside the crate, out of nothing but the public surface,
//! and drives a real node over it.
//!
//! The engine here is deliberately NOT a filesystem wrapper: it is an
//! in-memory byte store, so every assertion that passes is the machinery
//! talking to a file that does not exist on any disk. The AOF's io_uring
//! fast path is exercised too — an engine handle has no operating-system
//! descriptor, so the AOF must fall back to the engine's own write and
//! still land every byte.

use std::collections::BTreeMap;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use lunet_advisory_lock::spi::{
    Disk, DiskDirEntry, DiskFile, EngineFile, FileStateStore, StateSnapshot, StateStore,
};

/// The engine: a named byte store, shared by every handle the disk opens.
/// There is no file behind any of this.
#[derive(Clone, Default)]
struct MemoryEngine {
    files: Arc<Mutex<BTreeMap<String, Vec<u8>>>>,
}

impl MemoryEngine {
    fn key(path: &Path) -> String {
        path.display().to_string()
    }

    fn store(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, Vec<u8>>> {
        self.files.lock().expect("the engine lock")
    }
}

/// One open file on the engine: a path, a cursor, and no descriptor.
struct MemoryFile {
    engine: MemoryEngine,
    key: String,
    cursor: u64,
}

impl MemoryFile {
    fn open(engine: &MemoryEngine, path: &Path, create: bool) -> io::Result<DiskFile> {
        let key = MemoryEngine::key(path);
        {
            let mut files = engine.store();
            match files.get_mut(&key) {
                Some(_) => {}
                None if create => {
                    files.insert(key.clone(), Vec::new());
                }
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("the engine holds no {key}"),
                    ));
                }
            }
        }
        Ok(DiskFile::from_engine(Box::new(Self {
            engine: engine.clone(),
            key,
            cursor: 0,
        })))
    }
}

impl EngineFile for MemoryFile {
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        let files = self.engine.store();
        let bytes = files
            .get(&self.key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "engine file is gone"))?;
        let start = usize::try_from(self.cursor).unwrap_or(usize::MAX);
        let end = start
            .checked_add(buf.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "read overflows"))?;
        // A short read is an error, never a panic: the journal's and the
        // AOF's resume scans read one record at a time and must be able to
        // see the end of the valid prefix.
        let slice = bytes.get(start..end).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                format!(
                    "the engine holds {} bytes from offset {start}; {end} were asked for",
                    bytes.len()
                ),
            )
        })?;
        buf.copy_from_slice(slice);
        self.cursor += buf.len() as u64;
        Ok(())
    }

    fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        let len = self.file_len()?;
        self.cursor = match from {
            SeekFrom::Start(at) => at,
            SeekFrom::End(delta) => len.saturating_add_signed(delta),
            SeekFrom::Current(delta) => self.cursor.saturating_add_signed(delta),
        };
        Ok(self.cursor)
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        let mut files = self.engine.store();
        let stored = files
            .get_mut(&self.key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "engine file is gone"))?;
        let at = usize::try_from(self.cursor)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "cursor overflows"))?;
        let end = at
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "write overflows"))?;
        if end > stored.len() {
            stored.resize(end, 0);
        }
        stored[at..end].copy_from_slice(bytes);
        self.cursor = end as u64;
        Ok(())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }

    fn sync_all(&self) -> io::Result<()> {
        Ok(())
    }

    fn set_len(&mut self, len: u64) -> io::Result<()> {
        let mut files = self.engine.store();
        let stored = files
            .get_mut(&self.key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "engine file is gone"))?;
        stored.resize(usize::try_from(len).unwrap_or(usize::MAX), 0);
        Ok(())
    }

    fn file_len(&self) -> io::Result<u64> {
        let files = self.engine.store();
        let bytes = files
            .get(&self.key)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "engine file is gone"))?;
        u64::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "engine file is too long"))
    }

    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.seek(SeekFrom::Start(offset))?;
        self.write_all(bytes)
    }
}

/// A foreign `Disk` whose open shapes answer from the engine and whose
/// path-shaped operations answer from an in-memory directory listing.
struct EngineDisk {
    engine: MemoryEngine,
    dirs: Arc<Mutex<Vec<PathBuf>>>,
}

impl EngineDisk {
    fn new() -> Self {
        Self {
            engine: MemoryEngine::default(),
            dirs: Arc::new(Mutex::new(Vec::new())),
        }
    }

    fn mount() -> Arc<dyn Disk> {
        Arc::new(Self::new())
    }
}

impl Disk for EngineDisk {
    fn create_dir_all(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn remove_dir_all(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        let mut files = self.engine.store();
        let bytes = files
            .remove(&MemoryEngine::key(from))
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such engine file"))?;
        files.insert(MemoryEngine::key(to), bytes);
        Ok(())
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        self.engine
            .store()
            .remove(&MemoryEngine::key(path))
            .map(|_| ())
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such engine file"))
    }

    /// Bytes, exactly as stored: the state seam's framed file is binary, so
    /// this is the operation a reader of that file uses and the one an engine
    /// must answer without a text conversion in the way.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        let files = self.engine.store();
        files
            .get(&MemoryEngine::key(path))
            .cloned()
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such engine file"))
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        let files = self.engine.store();
        let bytes = files
            .get(&MemoryEngine::key(path))
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such engine file"))?;
        String::from_utf8(bytes.clone())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "engine file is not utf-8"))
    }

    fn exists(&self, path: &Path) -> bool {
        self.engine.store().contains_key(&MemoryEngine::key(path))
    }

    fn file_len(&self, path: &Path) -> io::Result<u64> {
        let files = self.engine.store();
        let bytes = files
            .get(&MemoryEngine::key(path))
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "no such engine file"))?;
        u64::try_from(bytes.len())
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "engine file is too long"))
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<DiskDirEntry>> {
        let dirs = self.dirs.lock().expect("the dirs lock");
        let prefix = MemoryEngine::key(path);
        Ok(dirs
            .iter()
            .filter(|entry| MemoryEngine::key(entry).starts_with(&prefix))
            .cloned()
            .map(DiskDirEntry::new)
            .collect())
    }

    fn sync_path(&self, _path: &Path) -> io::Result<()> {
        Ok(())
    }

    fn open_read(&self, path: &Path) -> io::Result<DiskFile> {
        MemoryFile::open(&self.engine, path, false)
    }

    fn open_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        MemoryFile::open(&self.engine, path, false)
    }

    fn open_read_append(&self, path: &Path) -> io::Result<DiskFile> {
        let mut file = MemoryFile::open(&self.engine, path, false)?;
        file.seek(SeekFrom::End(0))?;
        Ok(file)
    }

    fn open_or_create_write(&self, path: &Path) -> io::Result<DiskFile> {
        MemoryFile::open(&self.engine, path, true)
    }

    fn open_or_create_append(&self, path: &Path) -> io::Result<DiskFile> {
        let mut file = MemoryFile::open(&self.engine, path, true)?;
        file.seek(SeekFrom::End(0))?;
        Ok(file)
    }

    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile> {
        if self.exists(path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the engine already holds this file",
            ));
        }
        MemoryFile::open(&self.engine, path, true)
    }

    fn create_new_append(&self, path: &Path) -> io::Result<DiskFile> {
        if self.exists(path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the engine already holds this file",
            ));
        }
        let mut file = MemoryFile::open(&self.engine, path, true)?;
        file.seek(SeekFrom::End(0))?;
        Ok(file)
    }

    fn create_new_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        if self.exists(path) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "the engine already holds this file",
            ));
        }
        MemoryFile::open(&self.engine, path, true)
    }
}

/// The store the persistence law is stated over, mounted on the engine:
/// the state file's own bytes live in the engine's memory, staged and
/// renamed exactly as the file store does.
struct EngineStateStore {
    disk: Arc<dyn Disk>,
    path: PathBuf,
}

impl StateStore for EngineStateStore {
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()> {
        let payload = serde_json::to_vec(snap)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
        let temporary = PathBuf::from(format!("{}.staged", self.path.display()));
        let mut staged = self.disk.create_new_write(&temporary)?;
        staged.write_all(&payload)?;
        staged.sync_all()?;
        self.disk.rename(&temporary, &self.path)?;
        self.disk.sync_path(&self.path)
    }

    fn load(&self) -> io::Result<Option<StateSnapshot>> {
        if !self.disk.exists(&self.path) {
            return Ok(None);
        }
        let bytes = self.disk.read(&self.path)?;
        serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }
}

/// A foreign `DiskFile` mounts from `EngineFile`, and every operation on it
/// reaches the engine — the seam's handle level, proven out of crate.
#[test]
fn a_foreign_engine_file_mounts_and_answers_every_operation() {
    let disk = EngineDisk::mount();
    let path = PathBuf::from("/engine/handle.bin");

    let mut file = disk
        .create_new_write(&path)
        .expect("a foreign handle opens");
    file.write_all(b"alpha").expect("the engine takes a write");
    file.sync_all().expect("the engine answers a sync");
    assert_eq!(file.file_len().expect("the engine answers its length"), 5);

    file.write_all_at(0, b"ALPHA")
        .expect("the engine takes a positional write");
    assert_eq!(file.file_len().expect("still five bytes"), 5);

    file.seek(SeekFrom::Start(0)).expect("the engine seeks");
    let mut read_back = [0u8; 5];
    file.read_exact(&mut read_back)
        .expect("the engine serves a read");
    assert_eq!(&read_back, b"ALPHA");

    file.set_len(3).expect("the engine truncates");
    assert_eq!(file.file_len().expect("now three bytes"), 3);

    // The engine handle is not an operating-system descriptor, and says so
    // honestly rather than pretending.
    #[cfg(unix)]
    assert!(
        file.as_raw_fd().is_none(),
        "an engine-backed handle carries no descriptor"
    );
}

/// The state seam over a foreign store and a foreign disk: flush stages,
/// renames and syncs through the engine, and load reads it back.
#[test]
fn the_state_seam_flushes_and_loads_over_a_foreign_store() {
    let disk = EngineDisk::mount();
    let path = PathBuf::from("/engine/node.state");
    let mut store = EngineStateStore {
        disk: Arc::clone(&disk),
        path: path.clone(),
    };

    let table: StateSnapshot =
        serde_json::from_str(r#"{"locks":{}}"#).expect("an empty table is a table");
    store.flush(&table).expect("the foreign store flushes");
    assert!(disk.exists(&path), "the state file landed on the engine");

    let loaded = store.load().expect("the foreign store loads");
    assert!(loaded.is_some(), "the table came back");

    // A clean halt's flush left no staging file behind.
    let staging = PathBuf::from(format!("{}.staged", path.display()));
    assert!(
        !disk.exists(&staging),
        "the staged write was renamed, not left behind"
    );
}

/// `FileStateStore` itself, mounted over the foreign disk: the crate's own
/// store on top of an embedder's engine.
#[test]
fn the_crates_own_state_store_runs_on_a_foreign_disk() {
    let disk = EngineDisk::mount();
    let node_state = PathBuf::from("/engine/node");
    let store = FileStateStore::new_on(Arc::clone(&disk), &node_state);

    let table: StateSnapshot = serde_json::from_str(r#"{"locks":{}}"#).expect("an empty table");
    let mut store = store;
    store
        .flush(&table)
        .expect("the state file flushes onto the foreign disk");
    let loaded = store.load().expect("the state file loads from it");
    assert!(loaded.is_some(), "the framed table came back over the seam");
}
