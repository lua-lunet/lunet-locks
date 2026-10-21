//! The state seam: the lock table's persistence contract, one trait.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::disk::{Disk, std_disk};
use crate::journal::crc32_ieee;
use crate::locks::StateSnapshot;

/// One stream's completion-driven counters.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StreamCounters {
    /// Generations fully written since the store was opened.
    pub completed_generations: u64,
    /// Records landed since the store was opened.
    pub completed_records: u64,
    /// Submissions refused (capacity) since the store was opened.
    pub refusals: u64,
    /// Submissions outstanding right now: 0 or one generation's worth.
    pub in_flight: u32,
}

/// The per-stream counters the status surface reads, one row per stream
/// plus the last completion's wall timestamp. `state_status` answers this
/// or `None` — a store with no counters answers `None`, never zeros.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StoreStatus {
    /// The ephemeral (one-cache-line record) stream's counters.
    pub ephemeral: StreamCounters,
    /// The durable (fat frame) stream's counters.
    pub durable: StreamCounters,
    /// The last completion's wall timestamp, either stream.
    pub last_completed_unix_ns: u64,
}

/// The on-disk format's version byte; a reader that does not recognise it
/// refuses the file.
const FORMAT_VERSION: u8 = 2;

/// The lock table's persistence contract: FLUSH is EAGER (the shutdown
/// path writes the whole state in the foreground; a stop that cannot
/// flush is a FAILED stop, surfaced, never skipped), LOAD is LAZY (never
/// at boot; a crashed boot distrusts the file and rebuilds from the
/// replica stream). `flush` takes `&mut self` because a store may hold a
/// staging position; `load` takes `&self` because loading never mutates
/// the store.
pub trait StateStore: Send + Sync {
    /// Write the whole lock-table state, durably, before returning; an
    /// `Err` fails the stop — surfaced, never skipped.
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()>;

    /// Record one changed lock; the next [`StateStore::tick`] carries it.
    /// The default records nothing.
    fn note(&mut self, lock_id: u64) {
        let _ = lock_id;
    }

    /// The steady tick's write: submit the noted locks as one generation
    /// per stream, page-cached, zero fsync; a refusal keeps the changed
    /// set so the next tick retries. The default submits nothing.
    fn tick(&mut self, snap: &StateSnapshot) -> io::Result<usize> {
        let _ = snap;
        Ok(0)
    }

    /// Drain completions non-blocking; a no-op success where completions
    /// are inline. The default is that no-op.
    fn pump(&mut self) -> io::Result<()> {
        Ok(())
    }

    /// The per-stream counters, when the store carries them.
    fn state_status(&self) -> Option<StoreStatus> {
        None
    }

    /// Forget the artefact: the dirty boot must not write against the old
    /// artefact's bookkeeping (stale mirrors would fail a correct stop).
    /// The default forgets nothing.
    fn forget(&mut self) {
        let _ = self;
    }

    /// Read the state back; `Ok(None)` is every "nothing trustworthy
    /// here" answer — a content problem is never an `Err` (a fallback
    /// that blocks the boot is an outage). The clean-verdict guard lives
    /// at the call site, never here.
    fn load(&self) -> io::Result<Option<StateSnapshot>>;
}

/// The in-tree store: one file under the node's state path (`.state`
/// suffix), written through the [`Disk`] seam.
pub struct FileStateStore {
    disk: Arc<dyn Disk>,
    path: PathBuf,
}

impl FileStateStore {
    /// The store over the crate's own disk.
    pub fn new(path: &Path) -> Self {
        Self::new_on(std_disk(), path)
    }

    /// The store over a caller-supplied [`Disk`] — the seam's injection
    /// point.
    pub fn new_on(disk: Arc<dyn Disk>, path: &Path) -> Self {
        Self {
            disk,
            path: state_file_path(path),
        }
    }

    /// The file this store reads and writes: the node's state path with
    /// `.state` appended (the file's name is part of the contract).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// The framed bytes for a snapshot: version, length, payload,
    /// checksum. One function so the writer and the reader cannot drift.
    fn frame(snap: &StateSnapshot) -> io::Result<Vec<u8>> {
        let payload = serde_json::to_vec(snap)?;
        let mut framed = Vec::with_capacity(5 + payload.len() + 4);
        framed.push(FORMAT_VERSION);
        framed.extend_from_slice(
            &u32::try_from(payload.len())
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "state snapshot is too large to frame",
                    )
                })?
                .to_be_bytes(),
        );
        framed.extend_from_slice(&payload);
        let checksum = crc32_ieee(&framed);
        framed.extend_from_slice(&checksum.to_be_bytes());
        Ok(framed)
    }

    /// The snapshot a framed file carries, or `None` when the frame is
    /// not one this build trusts. Every refusal is a content refusal.
    fn unframe(bytes: &[u8]) -> Option<StateSnapshot> {
        const HEADER: usize = 1 + 4;
        const CHECKSUM: usize = 4;
        if bytes.len() < HEADER + CHECKSUM || bytes[0] != FORMAT_VERSION {
            return None;
        }
        let length = u32::from_be_bytes(bytes[1..HEADER].try_into().ok()?) as usize;
        let body_end = HEADER.checked_add(length)?;
        // A torn tail is a length that disagrees with the bytes present:
        // a refusal, not a half-decoded table.
        if bytes.len() != body_end + CHECKSUM {
            return None;
        }
        let stored = u32::from_be_bytes(bytes[body_end..].try_into().ok()?);
        if crc32_ieee(&bytes[..body_end]) != stored {
            return None;
        }
        serde_json::from_slice(&bytes[HEADER..body_end]).ok()
    }

    /// The staged durable write: temp file, fsync, rename, parent fsync;
    /// a failure removes the temp file and reports.
    fn write_atomically(&self, framed: &[u8]) -> io::Result<()> {
        let parent = self.path.parent().unwrap_or_else(|| Path::new("."));
        let base = self.path.file_name().unwrap_or_default();
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |elapsed| elapsed.as_nanos());
        let mut temporary = std::ffi::OsString::from(".");
        temporary.push(base);
        temporary.push(format!(".state-tmp-{}-{unique}", std::process::id()));
        let temporary = parent.join(temporary);
        let result = (|| {
            let mut file = self.disk.create_new_write(&temporary)?;
            file.write_all(framed)?;
            file.sync_all()?;
            self.disk.rename(&temporary, &self.path)?;
            // POSIX: persist the new directory entry, not just the data.
            crate::ffi::sync_parent(&*self.disk, &self.path)
        })();
        if result.is_err() {
            let _ = self.disk.remove_file(&temporary);
        }
        result
    }
}

impl StateStore for FileStateStore {
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()> {
        self.write_atomically(&Self::frame(snap)?)
    }

    fn load(&self) -> io::Result<Option<StateSnapshot>> {
        if !self.disk.exists(&self.path) {
            return Ok(None);
        }
        Ok(Self::unframe(&self.disk.read(&self.path)?))
    }
}

/// The state file's path for a node state path: `.state` appended.
fn state_file_path(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".state");
    PathBuf::from(os)
}

/// The lazy load with its guard: `clean_boot` (a crashed boot distrusts
/// the file and rebuilds from the replica stream) and `table_empty` (a
/// populated table is never overwritten) are both refusals, and every
/// refusal is `Ok(None)`. The one-shot clause is the caller's.
pub fn lazy_load(
    store: &dyn StateStore,
    clean_boot: bool,
    table_empty: bool,
) -> io::Result<Option<StateSnapshot>> {
    if !clean_boot {
        crate::trace_line!("state.load-distrusted");
        return Ok(None);
    }
    if !table_empty {
        crate::trace_line!("state.load-skip-populated");
        return Ok(None);
    }
    let loaded = store.load()?;
    match &loaded {
        Some(_) => crate::trace_line!("state.load"),
        None => crate::trace_line!("state.load-absent"),
    }
    Ok(loaded)
}
