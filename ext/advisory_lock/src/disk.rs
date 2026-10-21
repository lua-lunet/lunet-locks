//! The disk seam: every byte this crate reads or writes, named as one
//! contract — nothing above the seam calls `std::fs` on a live path.
//! Durability is two distinct operations, never one: [`DiskFile::flush`]
//! pushes to the page cache; [`DiskFile::sync_all`] is the fsync.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One open file, as the seam hands it back: a thin handle whose
/// operations an embedder's engine can answer exactly as the local
/// filesystem answers them.
pub struct DiskFile {
    inner: DiskFileInner,
}

enum DiskFileInner {
    Std(File),
    /// An embedder's engine handle, erased behind [`EngineFile`]; no
    /// operating-system descriptor of its own.
    Engine(Box<dyn EngineFile>),
}

/// One open file on an embedder's own storage engine — the counterpart to
/// [`StdDisk`] at the handle level, served through [`DiskFile::from_engine`].
/// `flush` and `sync_all` stay distinct: the first pushes to the engine's
/// buffer, the second is the durability barrier.
pub trait EngineFile: Send {
    /// Fill `buf` completely, or fail.
    fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()>;

    /// Move the handle's own cursor.
    fn seek(&mut self, from: SeekFrom) -> io::Result<u64>;

    /// Append every byte, or fail.
    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Push buffered bytes toward the medium. NOT a durability barrier.
    fn flush(&mut self) -> io::Result<()>;

    /// The durability barrier: the bytes are on the medium.
    fn sync_all(&self) -> io::Result<()>;

    /// Truncate (or extend) the file to `len` bytes.
    fn set_len(&mut self, len: u64) -> io::Result<()>;

    /// The file's current length in bytes.
    fn file_len(&self) -> io::Result<u64>;

    /// Positional write: `bytes` at `offset`, independent of the handle's
    /// own cursor, which callers never rely on.
    fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()>;
}

impl DiskFile {
    /// The handle over an operating-system file — for a [`Disk`] that
    /// wraps the filesystem itself.
    pub fn from_std(file: File) -> Self {
        Self {
            inner: DiskFileInner::Std(file),
        }
    }

    /// The handle over an embedder's engine file — the seam's injection
    /// point at the handle level.
    pub fn from_engine(engine: Box<dyn EngineFile>) -> Self {
        Self {
            inner: DiskFileInner::Engine(engine),
        }
    }
    /// Fill `buf` completely, or fail; the resume scans' only read shape.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        match &mut self.inner {
            DiskFileInner::Std(file) => file.read_exact(buf),
            DiskFileInner::Engine(engine) => engine.read_exact(buf),
        }
    }

    /// Move the handle's own cursor.
    pub fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        match &mut self.inner {
            DiskFileInner::Std(file) => file.seek(from),
            DiskFileInner::Engine(engine) => engine.seek(from),
        }
    }

    /// Append every byte, or fail.
    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            DiskFileInner::Std(file) => file.write_all(bytes),
            DiskFileInner::Engine(engine) => engine.write_all(bytes),
        }
    }

    /// Push buffered bytes toward the medium. NOT a durability barrier.
    pub fn flush(&mut self) -> io::Result<()> {
        match &mut self.inner {
            DiskFileInner::Std(file) => file.flush(),
            DiskFileInner::Engine(engine) => engine.flush(),
        }
    }

    /// fsync: the bytes are on the medium.
    pub fn sync_all(&self) -> io::Result<()> {
        match &self.inner {
            DiskFileInner::Std(file) => file.sync_all(),
            DiskFileInner::Engine(engine) => engine.sync_all(),
        }
    }

    /// Truncate (or extend) the file to `len` bytes.
    pub fn set_len(&mut self, len: u64) -> io::Result<()> {
        match &mut self.inner {
            DiskFileInner::Std(file) => file.set_len(len),
            DiskFileInner::Engine(engine) => engine.set_len(len),
        }
    }

    /// The file's current length in bytes.
    pub fn file_len(&self) -> io::Result<u64> {
        match &self.inner {
            DiskFileInner::Std(file) => Ok(file.metadata()?.len()),
            DiskFileInner::Engine(engine) => engine.file_len(),
        }
    }

    /// Positional write: `bytes` at `offset`, independent of the handle's
    /// own cursor.
    #[cfg(unix)]
    pub fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            DiskFileInner::Std(file) => FileExt::write_all_at(file, bytes, offset),
            DiskFileInner::Engine(engine) => engine.write_all_at(offset, bytes),
        }
    }

    /// Positional write: `bytes` at `offset`, independent of the handle's
    /// own cursor.
    #[cfg(windows)]
    pub fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        match &mut self.inner {
            DiskFileInner::Std(file) => FileExt::seek_write(file, bytes, offset).map(|_| ()),
            DiskFileInner::Engine(engine) => engine.write_all_at(offset, bytes),
        }
    }

    /// Positional write: `bytes` at `offset`, independent of the handle's
    /// own cursor.
    #[cfg(not(any(unix, windows)))]
    pub fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.seek(SeekFrom::Start(offset))?;
        self.write_all(bytes)
    }

    /// The raw descriptor, for the io_uring fast path; `None` on an
    /// engine handle (the caller takes the blocking write instead).
    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> Option<std::os::fd::RawFd> {
        match &self.inner {
            DiskFileInner::Std(file) => {
                use std::os::fd::AsRawFd;
                Some(file.as_raw_fd())
            }
            DiskFileInner::Engine(_) => None,
        }
    }
}

/// `BufWriter`'s inner writer (the flight recorder's buffered tape lines);
/// the buffer's per-line flush is a page-cache flush, never an fsync.
impl Write for DiskFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match &mut self.inner {
            DiskFileInner::Std(file) => file.write(bytes),
            // All-or-fail: the whole slice is consumed or the error stands.
            DiskFileInner::Engine(engine) => engine.write_all(bytes).map(|()| bytes.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        DiskFile::flush(self)
    }
}

/// One entry of a directory listing: the path and nothing else (lengths
/// go through [`Disk::file_len`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskDirEntry {
    path: PathBuf,
}

impl DiskDirEntry {
    /// Construct an entry from its own path — the constructor an
    /// out-of-crate [`Disk`] implementation needs.
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    /// The entry's own path, owned.
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

/// Every byte of disk access the crate performs: each method is one
/// operation a live call site performs, named as that call site performs
/// it. An implementation must keep the two durability levels apart and
/// must report failure the way the underlying operation does.
pub trait Disk: Send + Sync {
    /// Create `path` and every missing ancestor. Succeeds when the
    /// directory already exists.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Remove `path` recursively.
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Rename (or move) `from` onto `to`, replacing `to` if it exists.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Unlink `path`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Read a whole file into memory.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// Read a whole file as UTF-8 text.
    fn read_to_string(&self, path: &Path) -> io::Result<String>;

    /// Whether `path` exists; never fails (an unanswerable stat is
    /// reported as absent).
    fn exists(&self, path: &Path) -> bool;

    /// The length of the file at `path` in bytes.
    fn file_len(&self, path: &Path) -> io::Result<u64>;

    /// Every entry of `path`, in whatever order the directory hands them
    /// back.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<DiskDirEntry>>;

    /// fsync `path` itself: after a create or a rename, persist the
    /// directory entry, not just the file's data.
    fn sync_path(&self, path: &Path) -> io::Result<()>;

    /// Open an existing file for reading.
    fn open_read(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open an existing file for reading and writing, creating nothing.
    fn open_read_write(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open an existing file for reading and appending, creating nothing.
    fn open_read_append(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open for writing, creating the file if it is absent and leaving an
    /// existing file's length alone.
    fn open_or_create_write(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open for appending, creating the file if it is absent.
    fn open_or_create_append(&self, path: &Path) -> io::Result<DiskFile>;

    /// Create a new file for writing and fail if it already exists — the
    /// stage every atomic durable write in the crate goes through.
    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile>;

    /// Create a new file for appending and fail if it already exists.
    fn create_new_append(&self, path: &Path) -> io::Result<DiskFile>;

    /// Create a new file readable, writable and appendable, failing if it
    /// already exists.
    fn create_new_read_write(&self, path: &Path) -> io::Result<DiskFile>;
}

/// This crate's own implementation: the local filesystem, one method per
/// operation, no policy and no caching.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StdDisk;

impl StdDisk {
    fn wrap(file: File) -> DiskFile {
        DiskFile::from_std(file)
    }
}

impl Disk for StdDisk {
    fn create_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::create_dir_all(path)
    }

    fn remove_dir_all(&self, path: &Path) -> io::Result<()> {
        fs::remove_dir_all(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> io::Result<()> {
        fs::rename(from, to)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn read(&self, path: &Path) -> io::Result<Vec<u8>> {
        fs::read(path)
    }

    fn read_to_string(&self, path: &Path) -> io::Result<String> {
        fs::read_to_string(path)
    }

    fn exists(&self, path: &Path) -> bool {
        path.exists()
    }

    fn file_len(&self, path: &Path) -> io::Result<u64> {
        Ok(fs::metadata(path)?.len())
    }

    fn read_dir(&self, path: &Path) -> io::Result<Vec<DiskDirEntry>> {
        fs::read_dir(path)?
            .map(|entry| entry.map(|entry| DiskDirEntry::new(entry.path())))
            .collect()
    }

    fn sync_path(&self, path: &Path) -> io::Result<()> {
        Self::wrap(File::open(path)?).sync_all()
    }

    fn open_read(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(File::open(path)?))
    }

    fn open_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new().read(true).write(true).open(path)?,
        ))
    }

    fn open_read_append(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new().read(true).append(true).open(path)?,
        ))
    }

    fn open_or_create_write(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new()
                .create(true)
                // Overwrite in place: the recovery-flush baseline stays
                // exactly one block across boots.
                .truncate(false)
                .write(true)
                .open(path)?,
        ))
    }

    fn open_or_create_append(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new().create(true).append(true).open(path)?,
        ))
    }

    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new().write(true).create_new(true).open(path)?,
        ))
    }

    fn create_new_append(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(path)?,
        ))
    }

    fn create_new_read_write(&self, path: &Path) -> io::Result<DiskFile> {
        Ok(Self::wrap(
            OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(path)?,
        ))
    }
}

/// The default disk: this crate's own, behind the seam's own indirection.
pub fn std_disk() -> Arc<dyn Disk> {
    Arc::new(StdDisk)
}
