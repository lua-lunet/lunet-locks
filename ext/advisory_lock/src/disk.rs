//! The disk seam: every byte this crate reads or writes, named as one
//! contract.
//!
//! Nothing in the crate calls `std::fs`, `std::fs::File` or
//! `OpenOptions` on a live path any more — the AOF writer, the lock-event
//! journal, the recovery-boundary flush, the flight recorder, the marker's
//! single-file projection and the `lunet_locks_nuke` admin tool all name
//! the operation they want and hand it a [`Disk`]. Two implementations
//! target that contract: [`StdDisk`], this crate's own, a one-to-one
//! delegation to the local filesystem, and whatever industrial engine an
//! embedder mounts instead. The crate compiles identically against either,
//! and nothing above the seam knows which one it is holding.
//!
//! Two shapes, one contract. The directory-and-name operations
//! ([`Disk::create_dir_all`], [`Disk::rename`], [`Disk::read`],
//! [`Disk::read_dir`], …) take a path and are what most call sites name.
//! An opened file comes back as a [`DiskFile`], the thin handle the
//! crate's file-shaped operations run on.
//!
//! The seam is dynamic, never generic: every holder is an
//! `Arc<dyn Disk>` and every free function takes a `&dyn Disk`. The C ABI
//! surface therefore stays monomorph-free — one vtable per operation, no
//! per-impl instantiation of the replication engine.
//!
//! Durability is two distinct operations and never one. [`DiskFile::flush`]
//! is a push to the operating system's page cache; [`DiskFile::sync_all`]
//! is an fsync, the only one that survives power loss. The journal, the
//! AOF and the marker projection all depend on which of the two they are
//! asking for, so the seam keeps them apart: an implementation that
//! conflated them would silently weaken every fsync obligation in the
//! crate.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// One open file, as the seam hands it back.
///
/// A thin handle over the operating system's file, not a new file type:
/// the crate's open patterns (append, positional write, truncate-a-torn-tail,
/// fsync) are the ones `std::fs::File` already implements exactly. What
/// the wrapper buys is one place where those operations are named, so an
/// embedder's engine can answer them the same way it answers
/// [`Disk::open_read`] and friends.
pub struct DiskFile {
    file: File,
}

impl DiskFile {
    /// Fill `buf` completely, or fail. The journal's and the AOF's resume
    /// scans read one fixed-size record at a time and treat a short read
    /// as the clean end of the valid prefix, so this is the only read
    /// shape the crate opens files with.
    pub fn read_exact(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.file.read_exact(buf)
    }

    /// Move the handle's own cursor. Callers seek before every positional
    /// write; no two of them share a cursor.
    pub fn seek(&mut self, from: SeekFrom) -> io::Result<u64> {
        self.file.seek(from)
    }

    /// Append every byte, or fail.
    pub fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.file.write_all(bytes)
    }

    /// Push buffered bytes to the operating system's page cache. NOT a
    /// durability barrier — see [`DiskFile::sync_all`].
    pub fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }

    /// fsync: the bytes are on the medium. The one operation whose loss
    /// window the journal's drain, the AOF's checkpoint and the marker's
    /// durable rounds are stated against.
    pub fn sync_all(&self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Truncate (or extend) the file to `len` bytes. The AOF's resume
    /// scan truncates a torn or zero-padded tail away with it.
    pub fn set_len(&self, len: u64) -> io::Result<()> {
        self.file.set_len(len)
    }

    /// The file's current length in bytes.
    pub fn file_len(&self) -> io::Result<u64> {
        Ok(self.file.metadata()?.len())
    }

    /// Positional write, portable across the platforms the crate builds
    /// on: `write_all_at` on unix and windows (both use the file's own
    /// position table, not the shared handle cursor), and an explicit
    /// seek-plus-write everywhere else. Callers never rely on the handle
    /// cursor.
    #[cfg(unix)]
    pub fn write_all_at(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        FileExt::write_all_at(&self.file, bytes, offset)
    }

    /// Positional write, portable across the platforms the crate builds
    /// on: `write_all_at` on unix and windows (both use the file's own
    /// position table, not the shared handle cursor), and an explicit
    /// seek-plus-write everywhere else. Callers never rely on the handle
    /// cursor.
    #[cfg(windows)]
    pub fn write_all_at(&self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        FileExt::seek_write(&self.file, bytes, offset).map(|_| ())
    }

    /// Positional write, portable across the platforms the crate builds
    /// on: `write_all_at` on unix and windows (both use the file's own
    /// position table, not the shared handle cursor), and an explicit
    /// seek-plus-write everywhere else. Callers never rely on the handle
    /// cursor.
    #[cfg(not(any(unix, windows)))]
    pub fn write_all_at(&mut self, offset: u64, bytes: &[u8]) -> io::Result<()> {
        self.seek(SeekFrom::Start(offset))?;
        self.write_all(bytes)
    }

    /// The raw descriptor, for the io_uring fast path: the Linux writer
    /// submits its buffered appends through a ring against this file's
    /// descriptor rather than through [`DiskFile::write_all_at`].
    #[cfg(unix)]
    pub fn as_raw_fd(&self) -> std::os::fd::RawFd {
        use std::os::fd::AsRawFd;
        self.file.as_raw_fd()
    }
}

/// `BufWriter`'s inner writer: the flight recorder buffers its tape lines
/// through the standard trait, and the buffer's per-line flush is the same
/// page-cache flush every other append in the crate makes — never an
/// fsync.
impl Write for DiskFile {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.file.write(bytes)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.file.flush()
    }
}

/// One entry of a directory listing, as the seam hands it back.
///
/// The path and nothing else: the crate's only per-entry interest is the
/// name (`ev-open-*.bin`, `flight-<node_id>-<epoch>.jsonl`) and the
/// length, and the length is taken through [`Disk::file_len`] on the path
/// rather than stat'ed here — a listing never stats a file it did not
/// match, and pre-stating every entry's length would make a disappearing
/// unrelated neighbour fail a listing that never cared about it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DiskDirEntry {
    path: PathBuf,
}

impl DiskDirEntry {
    /// The entry's own path, owned — the same shape `fs::DirEntry::path`
    /// hands back, so a listing's entries outlive the listing itself.
    pub fn path(&self) -> PathBuf {
        self.path.clone()
    }
}

/// Every byte of disk access the crate performs.
///
/// The contract is the crate's own usage and nothing wider: each method
/// is one operation a live call site actually performs, named as that
/// call site performs it. An implementation is free to serve any of them
/// from an engine rather than a filesystem — that is the whole point of
/// the seam — but it must keep the two durability levels apart and must
/// report failure the way the underlying operation does, because every
/// caller here turns an `Err` into a documented disable-and-continue or a
/// documented boot refusal.
pub trait Disk: Send + Sync {
    // -- whole-file and directory operations ---------------------------------

    /// Create `path` and every missing ancestor. Succeeds when the
    /// directory already exists.
    fn create_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Remove `path` recursively.
    fn remove_dir_all(&self, path: &Path) -> io::Result<()>;

    /// Rename (or move) `from` onto `to`, replacing `to` if it exists.
    fn rename(&self, from: &Path, to: &Path) -> io::Result<()>;

    /// Unlink `path`.
    fn remove_file(&self, path: &Path) -> io::Result<()>;

    /// Read a whole file into memory. The AOF writer's scan, the flight
    /// recorder's tape reader and the nuke tool's zone dump all read
    /// whole files.
    fn read(&self, path: &Path) -> io::Result<Vec<u8>>;

    /// Read a whole file as UTF-8 text: the marker line and the view
    /// record, both of which are one short line.
    fn read_to_string(&self, path: &Path) -> io::Result<String>;

    /// Whether `path` exists. Never fails — a stat that cannot answer
    /// (a permission error, a dangling symlink) is reported as absent,
    /// exactly as the `Path::exists` it replaces does.
    fn exists(&self, path: &Path) -> bool;

    /// The length of the file at `path` in bytes. Distinct from
    /// [`Disk::exists`]: the AOF's resume scan needs the number, not the
    /// answer.
    fn file_len(&self, path: &Path) -> io::Result<u64>;

    /// Every entry of `path`, in whatever order the directory hands them
    /// back. Collect-then-iterate: the crate's three directory readers
    /// each need the whole listing (resume scan, tape sweep, series
    /// listing) and none of them mutates the directory while reading it.
    fn read_dir(&self, path: &Path) -> io::Result<Vec<DiskDirEntry>>;

    /// fsync `path` itself, opening it for that purpose. The POSIX
    /// crash-consistency idiom: after a create or a rename, fsync the
    /// containing directory so the new directory entry is persisted, not
    /// just the file's data. The journal, the marker's projection and the
    /// nuke tool all close a durable write this way.
    fn sync_path(&self, path: &Path) -> io::Result<()>;

    // -- the open shapes the crate opens files with --------------------------

    /// Open an existing file for reading.
    fn open_read(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open an existing file for reading and writing, creating nothing.
    /// The AOF's resume scan adopts the series' open file with this shape.
    fn open_read_write(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open an existing file for reading and appending, creating nothing.
    /// The journal's resume scan reads the pre-restart records and then
    /// appends after them through one handle.
    fn open_read_append(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open for writing, creating the file if it is absent and leaving an
    /// existing file's length alone. The recovery-boundary flush's two
    /// variants overwrite exactly one block in place across boots.
    fn open_or_create_write(&self, path: &Path) -> io::Result<DiskFile>;

    /// Open for appending, creating the file if it is absent. The flight
    /// recorder's active tape, appended across restarts.
    fn open_or_create_append(&self, path: &Path) -> io::Result<DiskFile>;

    /// Create a new file for writing and fail if it already exists. Every
    /// atomic durable write in the crate stages through this shape: tmp
    /// file, fsync, rename.
    fn create_new_write(&self, path: &Path) -> io::Result<DiskFile>;

    /// Create a new file for appending and fail if it already exists. The
    /// journal's fresh `ev-open-<ts>.bin`, whose create-new rule is what
    /// makes the suffix a monotonic disambiguator.
    fn create_new_append(&self, path: &Path) -> io::Result<DiskFile>;

    /// Create a new file readable, writable and appendable, failing if it
    /// already exists. The AOF's fresh open file, which the resume scan
    /// later adopts read-write.
    fn create_new_read_write(&self, path: &Path) -> io::Result<DiskFile>;
}

/// This crate's own implementation: the local filesystem, one method per
/// operation, no policy and no caching.
///
/// Every entry point that does not take a disk of its own — the marker
/// projection's readers, whose only caller is the marker store, and the
/// constructors the unit tests drive — defaults to this.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StdDisk;

impl StdDisk {
    fn wrap(file: File) -> DiskFile {
        DiskFile { file }
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
            .map(|entry| entry.map(|entry| DiskDirEntry { path: entry.path() }))
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
///
/// Every entry point that does not take a disk of its own builds one of
/// these, so the default path is the same dynamic dispatch as a mounted
/// engine — never a direct `std::fs` call.
pub fn std_disk() -> Arc<dyn Disk> {
    Arc::new(StdDisk)
}
