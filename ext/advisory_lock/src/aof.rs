//! The AOF write-behind log (the TigerBeetle AOF pattern, adapted for a
//! diskless telemetry target).
//!
//! Producers enqueue complete `LKE1` records to a dedicated writer thread
//! and never wait on disk: the queue is bounded, and an enqueue onto a full
//! queue drops the event and increments a drop counter — the same
//! drop-on-overflow contract the tracing stack follows. The file series
//! itself reuses the lock-event journal's conventions (the same record and
//! metafile codecs, the same `ev-open-*` / `ev-<window>` naming) but is a
//! different file: the deferred durability target, never read by any
//! recovery path.
//!
//! # Flush policy
//!
//! Nothing on the append path fsyncs. The active file is fsync'd on a
//! configurable periodic timer, on an explicit checkpoint request, and on
//! graceful shutdown. The finalized file is fsync'd at roll. The last
//! unflushed bytes may be lost on power loss — documented loss window;
//! nothing depends on the file for safety.
//!
//! # The stop drain
//!
//! The graceful-stop path needs a stronger guarantee than the ordinary
//! fire-and-forget enqueue: [`AofWriter::drain`] blocks until every event
//! enqueued before the call is appended AND fsynced, so the stop path may
//! write its `flushed` marker only after the drain returns (the uVRR
//! termination obligations' write ordering). The queue is FIFO, so the
//! writer's answer to a drain message proves every earlier event landed.
//!
//! # Rolling at one erasure block
//!
//! The active file rolls at exactly [`ROLL_BYTES`] (2 MiB). Records are
//! fixed `RECORD_SIZE` (61) bytes and 61 does not divide 2 MiB, so the
//! writer zero-pads the tail before rolling: **every finalized file is
//! exactly 2 MiB**, write-position aligned to one erasure block. The zero
//! padding fails record parsing (bad magic), so the fixed-size readers'
//! stop-at-first-invalid-record rule terminates cleanly at the pad. The
//! reader must treat any prefix of the rolling series as valid: finalized
//! files may be deleted freely, in any order, at any time.
//!
//! # Backends
//!
//! - **Linux** (`io-uring` feature, default on for Linux targets): buffered
//!   appends are submitted through an `io_uring` ring — page-cached writes,
//!   no `O_DIRECT`, no `O_SYNC`. If the ring cannot be created on the
//!   running kernel the writer falls back to plain buffered `write_all` and
//!   logs once.
//! - **macOS and other non-Linux targets**: a plain dedicated writer thread
//!   with buffered `write_all`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read};
#[cfg(not(any(unix, windows)))]
use std::io::{Seek, SeekFrom, Write};
#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Positional write, portable across the platforms the crate builds on:
/// `write_all_at` on unix and windows (both use the file's own position
/// table, not the shared handle cursor), and an explicit seek-plus-write
/// everywhere else. Callers never rely on the handle cursor.
#[cfg(unix)]
fn write_all_at(file: &mut File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    FileExt::write_all_at(file, bytes, offset)
}

#[cfg(windows)]
fn write_all_at(file: &mut File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    FileExt::seek_write(file, bytes, offset).map(|_| ())
}

#[cfg(not(any(unix, windows)))]
fn write_all_at(file: &mut File, offset: u64, bytes: &[u8]) -> io::Result<()> {
    file.seek(SeekFrom::Start(offset))?;
    file.write_all(bytes)
}

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use tracing::warn;

use crate::journal::{self, JournalEvent, RECORD_SIZE};

/// The roll threshold: one erasure block, 2 MiB.
pub const ROLL_BYTES: u64 = 2 * 1024 * 1024;

/// Default buffered-write threshold: bytes buffered in the writer thread
/// before one write call reaches the file. Page-cached either way; the
/// buffer only batches syscalls.
const DEFAULT_FLUSH_BYTES: usize = 64 * 1024;

/// Default bounded-queue capacity. Producers never block: overflow drops.
const DEFAULT_QUEUE_CAP: usize = 4096;

/// Default periodic-fsync interval (the operator knob; no protocol meaning).
const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(1000);

/// Pump granularity when no periodic timer is configured.
const PUMP_TICK: Duration = Duration::from_millis(200);

/// The writer's tuning knobs.
#[derive(Debug, Clone)]
pub struct AofConfig {
    /// Bytes buffered in the writer thread before one write call.
    pub flush_bytes: usize,
    /// Periodic fsync interval. `None` fsyncs only at roll and shutdown.
    pub flush_interval: Option<Duration>,
    /// Bounded-queue capacity (drop-on-overflow above it).
    pub queue_cap: usize,
}

impl Default for AofConfig {
    fn default() -> Self {
        Self {
            flush_bytes: DEFAULT_FLUSH_BYTES,
            flush_interval: Some(DEFAULT_FLUSH_INTERVAL),
            queue_cap: DEFAULT_QUEUE_CAP,
        }
    }
}

// ---------------------------------------------------------------------------
// File sink: one active file, ring-backed on Linux when available
// ---------------------------------------------------------------------------

/// The active file's writer. Each opened file carries its own io_uring ring
/// when the platform and feature have it (io_uring setup is negligible at a
/// 2 MiB roll cadence); a ring-creation failure falls back to plain
/// buffered `write_all` and logs once per file.
struct FileSink {
    file: File,
    path: PathBuf,
    /// Logical write position (bytes written to the active file).
    offset: u64,
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    ring: Option<io_uring::IoUring>,
}

impl FileSink {
    fn open_file(dir: &Path, started_ms: u64) -> io::Result<(PathBuf, File)> {
        let name = format!("ev-open-{started_ms}.bin");
        let path = dir.join(&name);
        let file = OpenOptions::new()
            .create_new(true)
            .read(true)
            .write(true)
            .open(&path)?;
        Ok((path, file))
    }

    fn new(dir: &Path, started_ms: u64) -> io::Result<Self> {
        let (path, file) = Self::open_file(dir, started_ms)?;
        Self::from_parts(path, file, 0)
    }

    /// Adopt an existing open file after a resume scan.
    fn adopt(path: PathBuf, file: File, offset: u64) -> io::Result<Self> {
        Self::from_parts(path, file, offset)
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    fn from_parts(path: PathBuf, file: File, offset: u64) -> io::Result<Self> {
        let ring = match io_uring::IoUring::new(64) {
            Ok(ring) => Some(ring),
            Err(e) => {
                warn!(
                    ts = crate::log_millis(),
                    event = "aof-io-uring-unavailable",
                    "aof: io_uring unavailable ({e}); falling back to buffered write_all"
                );
                None
            }
        };
        Ok(Self {
            file,
            path,
            offset,
            ring,
        })
    }

    #[cfg(not(all(target_os = "linux", feature = "io-uring")))]
    fn from_parts(path: PathBuf, file: File, offset: u64) -> io::Result<Self> {
        Ok(Self { file, path, offset })
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        if self.ring.is_some() {
            // Take the ring out for the call and put it back: the ring
            // write touches `self.file`/`self.offset` too.
            let mut ring = self.ring.take().expect("ring checked");
            let result = self.write_all_ring(&mut ring, bytes);
            self.ring = Some(ring);
            return result;
        }
        self.write_all_blocking(bytes)
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    fn write_all_ring(&mut self, ring: &mut io_uring::IoUring, bytes: &[u8]) -> io::Result<()> {
        use io_uring::{opcode, types};
        use std::os::fd::AsRawFd;

        let fd = types::Fd(self.file.as_raw_fd());
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let sqe = opcode::Write::new(fd, remaining.as_ptr(), remaining.len() as u32)
                .offset(self.offset)
                .build();
            // The SQE borrows `remaining` until the CQE below reaps it.
            unsafe {
                ring.submission().push(&sqe).map_err(io::Error::other)?;
            }
            ring.submit_and_wait(1)?;
            let cqe = ring
                .completion()
                .next()
                .ok_or_else(|| io::Error::other("aof: ring CQE missing"))?;
            let n = cqe.result();
            if n < 0 {
                return Err(io::Error::from_raw_os_error(-n));
            }
            if n == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "aof: ring wrote zero",
                ));
            }
            self.offset += n as u64;
            remaining = &remaining[n as usize..];
        }
        Ok(())
    }

    fn write_all_blocking(&mut self, bytes: &[u8]) -> io::Result<()> {
        write_all_at(&mut self.file, self.offset, bytes)?;
        self.offset += bytes.len() as u64;
        Ok(())
    }

    fn sync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }
}

// ---------------------------------------------------------------------------
// Core: the deterministic append/roll machine the writer thread pumps
// ---------------------------------------------------------------------------

/// The append/roll machine, driven synchronously by the writer thread (and
/// directly by the unit tests). All methods return errors; the caller owns
/// the disable-on-error policy.
struct AofCore {
    dir: PathBuf,
    flush_bytes: usize,
    sink: FileSink,
    /// Bytes of complete records written to the active file.
    written: u64,
    window: Option<journal::Window>,
    /// Number of fsyncs this core issued (timer, checkpoint, roll,
    /// shutdown).
    syncs: u64,
    /// Pending buffered bytes (complete records plus, just before a roll,
    /// the zero pad).
    buf: Vec<u8>,
}

impl AofCore {
    fn open(dir: &Path, flush_bytes: usize) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        let (sink, written, window) = match journal::find_open_file(dir)? {
            Some(entry) => {
                let path = entry.path();
                let mut file = OpenOptions::new().read(true).write(true).open(&path)?;
                // Scan the valid records to rebuild the position and the
                // metadata window. An open file's tail is either mid-record
                // (torn append) or zero pad (crash between pad and rename);
                // both fail record parsing, and both are truncated away.
                let mut scanned = 0u64;
                let mut window: Option<journal::Window> = None;
                let mut chunk = [0u8; RECORD_SIZE];
                loop {
                    match file.read_exact(&mut chunk) {
                        Ok(()) => {}
                        Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                        Err(e) => return Err(e),
                    }
                    match journal::parse_record(&chunk) {
                        Some((event, _)) => {
                            match &mut window {
                                Some(w) => w.update(&event),
                                None => window = Some(journal::Window::new(&event)),
                            }
                            scanned += RECORD_SIZE as u64;
                        }
                        None => break,
                    }
                }
                let len = file.metadata()?.len();
                if len > scanned {
                    file.set_len(scanned)?;
                }
                (FileSink::adopt(path, file, scanned)?, scanned, window)
            }
            None => {
                let started_ms = unix_millis();
                (FileSink::new(dir, started_ms)?, 0, None)
            }
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            flush_bytes,
            sink,
            written,
            window,
            syncs: 0,
            buf: Vec::with_capacity(flush_bytes.max(RECORD_SIZE) * 2),
        })
    }

    /// Append one event. Rolls first when the record would pass the 2 MiB
    /// mark (zero-padding the file to exactly 2 MiB), and rolls immediately
    /// when the record lands exactly on the mark.
    fn append(&mut self, event: &JournalEvent) -> io::Result<()> {
        if self.written + RECORD_SIZE as u64 > ROLL_BYTES {
            self.roll()?;
        }
        self.buf.extend_from_slice(&event.encode());
        match &mut self.window {
            Some(w) => w.update(event),
            None => self.window = Some(journal::Window::new(event)),
        }
        self.written += RECORD_SIZE as u64;
        if self.buf.len() >= self.flush_bytes {
            self.flush_buf()?;
        }
        if self.written == ROLL_BYTES {
            self.roll()?;
        }
        Ok(())
    }

    /// The checkpoint request: flush the buffer and fsync.
    fn checkpoint(&mut self) -> io::Result<()> {
        self.flush_buf()?;
        self.sink.sync()?;
        self.syncs += 1;
        Ok(())
    }

    /// Drain the buffered bytes into the page cache (no fsync).
    fn flush(&mut self) -> io::Result<()> {
        self.flush_buf()
    }

    /// Graceful shutdown: flush the buffer and fsync.
    fn shutdown(&mut self) -> io::Result<()> {
        self.checkpoint()
    }

    /// Zero-pad the active file to exactly [`ROLL_BYTES`], flush, fsync the
    /// finalized file, rename it to its final name, write the metafile
    /// atomically, and open a fresh open file.
    fn roll(&mut self) -> io::Result<()> {
        let window = match &self.window {
            Some(w) => w.clone(),
            None => return Ok(()), // nothing written yet
        };
        let pad = (ROLL_BYTES - self.written) as usize;
        self.buf.resize(self.buf.len() + pad, 0);
        self.flush_buf()?;
        self.sink.sync()?;
        self.syncs += 1;
        // The pad arithmetic above establishes the alignment: the flush
        // lands exactly at the roll mark. A finalized file of any other
        // size breaks the erasure-block contract silently in release, so
        // the panic is the proof, in every build.
        assert_eq!(
            self.sink.offset, ROLL_BYTES,
            "the roll finalized the erasure block"
        );
        let final_name = format!(
            "ev-{}-{}-{}-{}.bin",
            window.op_min, window.op_max, window.expiry_min, window.expiry_max
        );
        let final_path = self.dir.join(&final_name);
        fs::rename(&self.sink.path, &final_path)?;
        journal::write_meta_atomic(&final_path.with_extension("meta"), &window.meta())?;
        self.sink = FileSink::new(&self.dir, unix_millis())?;
        self.written = 0;
        self.window = None;
        Ok(())
    }

    fn flush_buf(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.buf);
        self.sink.write_all(&pending)
    }
}

fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// ---------------------------------------------------------------------------
// Handle + writer thread
// ---------------------------------------------------------------------------

enum Msg {
    Event(JournalEvent),
    Checkpoint,
    /// The stop path's drain request: append every earlier event, fsync,
    /// then answer through the signal. FIFO queue order makes the answer
    /// prove every event enqueued before the drain is durable.
    Drain(Arc<DrainSignal>),
}

/// The drain completion: `None` while the writer has not answered,
/// `Some(Ok(()))` once the fsync landed, `Some(Err(reason))` when the
/// writer is disabled after an I/O failure.
struct DrainSignal {
    state: Mutex<Option<Result<(), String>>>,
    cvar: Condvar,
}

struct Inner {
    tx: SyncSender<Msg>,
    drops: Arc<AtomicU64>,
}

/// A handle to the AOF writer thread. Producers call [`AofWriter::enqueue`],
/// which never blocks and never fails: overflow drops.
///
/// The thread drains and fsyncs when the last handle drops.
pub struct AofWriter {
    inner: Arc<Inner>,
}

impl AofWriter {
    /// Open (or resume) the AOF series at `dir` and start the writer
    /// thread. An open error here disables telemetry for the process (the
    /// caller logs and proceeds without a sink).
    pub fn open(dir: &Path, config: AofConfig) -> io::Result<Self> {
        let core = AofCore::open(dir, config.flush_bytes)?;
        let (tx, rx) = sync_channel::<Msg>(config.queue_cap);
        let drops = Arc::new(AtomicU64::new(0));
        let thread_drops = Arc::clone(&drops);
        let interval = config.flush_interval;
        std::thread::Builder::new()
            .name("aof-writer".to_string())
            .spawn(move || pump(rx, core, interval, thread_drops))
            .map_err(io::Error::other)?;
        Ok(Self {
            inner: Arc::new(Inner { tx, drops }),
        })
    }

    /// Enqueue one complete event. Never blocks, never fails: a full or
    /// disconnected queue drops the event and increments the drop counter.
    pub fn enqueue(&self, event: JournalEvent) {
        match self.inner.tx.try_send(Msg::Event(event)) {
            Ok(()) => {}
            Err(_) => {
                self.inner.drops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Request an fsync (the checkpoint path). Best-effort: a full queue
    /// drops the request.
    pub fn checkpoint(&self) {
        let _ = self.inner.tx.try_send(Msg::Checkpoint);
    }

    /// Block until every event enqueued before this call is durable: the
    /// queue is FIFO, so when the writer answers the drain request, all
    /// earlier events have been appended and fsynced. The graceful-stop
    /// path writes its `flushed` marker only after this returns. A writer
    /// disabled after an I/O failure reports the failure (the stop path
    /// then leaves its marker at `stopped`); a writer whose thread is
    /// gone (post-panic) is an error.
    pub fn drain(&self) -> io::Result<()> {
        let signal = Arc::new(DrainSignal {
            state: Mutex::new(None),
            cvar: Condvar::new(),
        });
        loop {
            match self.inner.tx.try_send(Msg::Drain(Arc::clone(&signal))) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => {
                    // The queue is full of events the writer is draining
                    // right now; retry until the drain is accepted.
                    std::thread::sleep(Duration::from_millis(1));
                }
                Err(TrySendError::Disconnected(_)) => {
                    return Err(io::Error::other("aof writer thread is gone"));
                }
            }
        }
        let mut state = signal.state.lock().expect("drain state poisoned");
        while state.is_none() {
            state = signal.cvar.wait(state).expect("drain state poisoned");
        }
        state
            .take()
            .expect("completed above")
            .map_err(io::Error::other)
    }

    /// Events dropped by this writer so far (overflow, or post-failure).
    pub fn drops(&self) -> u64 {
        self.inner.drops.load(Ordering::Relaxed)
    }
}

fn pump(rx: Receiver<Msg>, mut core: AofCore, interval: Option<Duration>, drops: Arc<AtomicU64>) {
    // The buffered-write drain runs on its own short cadence: a continuous
    // event stream must not starve it (recv_timeout would then never
    // expire), and the fsync knob stays independent of the byte flow.
    let mut last_flush = Instant::now();
    let mut last_sync = Instant::now();
    let mut failed = false;
    loop {
        match rx.recv_timeout(PUMP_TICK) {
            Ok(Msg::Event(event)) => {
                if failed {
                    // Telemetry disabled for the process; the event is lost.
                    drops.fetch_add(1, Ordering::Relaxed);
                } else if let Err(e) = core.append(&event) {
                    eprintln!(
                        "lunet-advisory-lock: aof append failed ({e}); \
                         telemetry disabled for this process"
                    );
                    failed = true;
                }
            }
            Ok(Msg::Checkpoint) => {
                if !failed {
                    match core.checkpoint() {
                        Ok(()) => {
                            last_sync = Instant::now();
                            last_flush = Instant::now();
                        }
                        Err(e) => {
                            // The checkpoint request was dropped without
                            // an answer. Survivable — telemetry carries
                            // a documented loss window and the next
                            // drain still reports — but never silent.
                            crate::maybe_invariant!(
                                "aof checkpoint request failed ({e}); the fsync was not taken"
                            );
                        }
                    }
                }
            }
            Ok(Msg::Drain(signal)) => {
                let outcome = if failed {
                    Err("telemetry disabled after an earlier error".to_string())
                } else {
                    match core.checkpoint() {
                        Ok(()) => {
                            last_sync = Instant::now();
                            last_flush = Instant::now();
                            Ok(())
                        }
                        Err(e) => {
                            eprintln!(
                                "lunet-advisory-lock: aof drain flush failed ({e}); \
                                 telemetry disabled for this process"
                            );
                            failed = true;
                            Err(e.to_string())
                        }
                    }
                };
                let mut state = signal.state.lock().expect("drain state poisoned");
                *state = Some(outcome);
                signal.cvar.notify_all();
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if failed {
            continue;
        }
        if last_flush.elapsed() >= PUMP_TICK {
            if core.flush().is_err() {
                eprintln!(
                    "lunet-advisory-lock: aof flush failed; telemetry disabled for this process"
                );
                failed = true;
                continue;
            }
            last_flush = Instant::now();
        }
        if interval.is_some_and(|iv| last_sync.elapsed() >= iv) {
            match core.checkpoint() {
                Ok(()) => {
                    last_sync = Instant::now();
                    last_flush = Instant::now();
                }
                Err(e) => {
                    // The window's fsync did not land. Survivable (the
                    // loss window is documented), never silent.
                    crate::maybe_invariant!(
                        "aof periodic fsync failed ({e}); this window's fsync did not land"
                    );
                }
            }
        }
    }
    if failed {
        eprintln!("lunet-advisory-lock: aof shutdown skipped (telemetry disabled after an error)");
    } else if let Err(e) = core.shutdown() {
        eprintln!("lunet-advisory-lock: aof shutdown flush failed ({e})");
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{KIND_BREAK, KIND_HOLD, KIND_RELEASE, KIND_RENEW};
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};

    /// The writer's test knobs: no periodic fsync (the drain's own fsync
    /// is the durability proof) and a queue deep enough that every
    /// enqueue of a test lands (overflow drops would corrupt the counted
    /// assertions).
    fn config() -> AofConfig {
        AofConfig {
            flush_interval: None,
            queue_cap: 65_536,
            ..AofConfig::default()
        }
    }

    fn event(kind: u8, ts: u64, lock: u64, expiry: u64) -> JournalEvent {
        JournalEvent {
            kind,
            ts,
            lock_id: lock,
            lease_id: lock * 10,
            holder: [kind; 16],
            expiry,
        }
    }

    /// The scratch tree, inside the repo (`.tmp` is scratch).
    fn scratch(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/aof-unit");
        fs::create_dir_all(&root).expect("the scratch root creates");
        let dir = root.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&dir).expect("the case directory creates");
        dir
    }

    /// The one active (still-appending) file of the series.
    fn open_file(dir: &Path) -> PathBuf {
        let mut found = Vec::new();
        for entry in fs::read_dir(dir).expect("the series directory reads") {
            let path = entry.expect("the entry reads").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with("ev-open-") {
                found.push(path);
            }
        }
        assert_eq!(found.len(), 1, "exactly one open file");
        found.remove(0)
    }

    /// Every finalized (rolled) file of the series.
    fn finalized_files(dir: &Path) -> Vec<PathBuf> {
        let mut found = Vec::new();
        for entry in fs::read_dir(dir).expect("the series directory reads") {
            let path = entry.expect("the entry reads").path();
            let name = path.file_name().and_then(|n| n.to_str()).unwrap_or("");
            if name.starts_with("ev-")
                && !name.starts_with("ev-open-")
                && path.extension().is_some_and(|e| e == "bin")
            {
                found.push(path);
            }
        }
        found
    }

    /// 61-byte records do not divide the 2 MiB roll mark: the records of
    /// one roll window stop just short of the mark and the writer's
    /// zero pad completes it, so every finalized file is exactly one
    /// erasure block and the record that would pass the mark starts the
    /// fresh file.
    #[test]
    fn records_per_roll_fits_one_block() {
        let records = ROLL_BYTES / RECORD_SIZE as u64;
        assert!(records * RECORD_SIZE as u64 <= ROLL_BYTES);
        assert!(
            (records + 1) * RECORD_SIZE as u64 > ROLL_BYTES,
            "one more record would pass the mark"
        );
        let dir = scratch("per-roll");
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
        for index in 0..records {
            core.append(&event(KIND_HOLD, 1_000 + index, 7, 5_000))
                .expect("the record appends");
        }
        core.flush().expect("the buffer drains");
        assert_eq!(
            fs::metadata(open_file(&dir))
                .expect("the length reads")
                .len(),
            records * RECORD_SIZE as u64,
            "the window stops just short of the mark: no roll yet"
        );
        assert!(
            finalized_files(&dir).is_empty(),
            "no file finalized below the mark"
        );
        // The record that would pass the mark: the roll fires first.
        let last = event(KIND_RENEW, 2_000, 8, 6_000);
        core.append(&last).expect("the record appends");
        core.flush().expect("the buffer drains");
        let finalized = finalized_files(&dir);
        assert_eq!(finalized.len(), 1, "the roll fired");
        let finalized_bytes = fs::read(&finalized[0]).expect("the finalized file reads");
        assert_eq!(
            finalized_bytes.len(),
            ROLL_BYTES as usize,
            "the roll completes the block: exactly 2 MiB"
        );
        let pad = (ROLL_BYTES - records * RECORD_SIZE as u64) as usize;
        assert!(
            finalized_bytes[finalized_bytes.len() - pad..]
                .iter()
                .all(|&byte| byte == 0),
            "the tail is the zero pad"
        );
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        assert_eq!(
            journal::parse_file(&bytes),
            vec![last],
            "the record that passed the mark starts the fresh file"
        );
    }

    /// The append path lands the record bytes byte-exact in the active
    /// file and the rolling window tracks the events' fields directly.
    #[test]
    fn append_lands_record_bytes_and_events() {
        let dir = scratch("append");
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
        let events = vec![
            event(KIND_HOLD, 1_000, 11, 5_000),
            event(KIND_RENEW, 1_100, 11, 6_000),
            event(KIND_RELEASE, 1_200, 11, 7_000),
            event(KIND_HOLD, 1_300, 12, 8_000),
            event(KIND_BREAK, 1_400, 12, 9_000),
        ];
        for one in &events {
            core.append(one).expect("the record appends");
        }
        core.flush().expect("the buffer drains");
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        assert_eq!(bytes.len(), events.len() * RECORD_SIZE);
        assert_eq!(
            journal::parse_file(&bytes),
            events,
            "the records landed byte-exact"
        );
        let window = core.window.as_ref().expect("the window tracks");
        assert_eq!(window.op_min, 1_000);
        assert_eq!(window.op_max, 1_400);
        assert_eq!(window.expiry_min, 5_000);
        assert_eq!(window.expiry_max, 9_000);
        assert_eq!(window.count, 5);
    }

    /// The roll finalizes exactly one 2 MiB erasure block: the records
    /// ride at the front, the rest of the file is the zero pad, the
    /// metafile carries the window, and a fresh open file follows.
    #[test]
    fn roll_finalizes_exactly_two_mib_with_pad() {
        let dir = scratch("roll-pad");
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
        let events = vec![
            event(KIND_HOLD, 1_000, 11, 5_000),
            event(KIND_RENEW, 1_100, 11, 6_000),
            event(KIND_RELEASE, 1_200, 11, 7_000),
        ];
        for one in &events {
            core.append(one).expect("the record appends");
        }
        core.flush().expect("the buffer drains");
        core.roll().expect("the roll fires");
        let finalized = finalized_files(&dir);
        assert_eq!(finalized.len(), 1, "one file finalized");
        let bytes = fs::read(&finalized[0]).expect("the finalized file reads");
        assert_eq!(
            bytes.len(),
            ROLL_BYTES as usize,
            "exactly one erasure block"
        );
        let records_end = events.len() * RECORD_SIZE;
        assert_eq!(
            journal::parse_file(&bytes),
            events,
            "the records ride at the front"
        );
        assert!(
            bytes[records_end..].iter().all(|&byte| byte == 0),
            "the rest of the block is the zero pad"
        );
        let meta = journal::Meta::decode(
            &fs::read(finalized[0].with_extension("meta")).expect("the metafile reads"),
        )
        .expect("the metafile decodes");
        assert_eq!(meta.count as usize, events.len());
        assert_eq!(meta.op_min, 1_000);
        assert_eq!(meta.op_max, 1_200);
        assert_eq!(core.written, 0, "the window restarts");
        assert!(core.window.is_none());
        let bytes = fs::read(open_file(&dir)).expect("the fresh open file reads");
        assert!(
            journal::parse_file(&bytes).is_empty(),
            "the fresh open file starts empty"
        );
    }

    /// The stop drain's guarantee (`AofWriter::drain`): after it returns,
    /// every event enqueued before the call is durable on disk. With the
    /// periodic-fsync knob OFF and 500 records (30.5 KiB) below the 64 KiB
    /// write-buffer threshold, the only thing that could have landed the
    /// buffered bytes is the drain's own flush+fsync — so a full read-back
    /// here proves the drain blocked until durability.
    #[test]
    fn drain_makes_every_queued_event_durable() {
        let dir = scratch("drain");
        let events: Vec<_> = (0..500u64)
            .map(|index| event(KIND_HOLD, 3_000 + index, 20 + index, 9_000))
            .collect();
        let writer = AofWriter::open(&dir, config()).expect("the series opens");
        for one in &events {
            writer.enqueue(one.clone());
        }
        writer.drain().expect("the drain lands every queued record");
        assert_eq!(writer.drops(), 0, "no record drops at this depth");
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        assert_eq!(
            bytes.len(),
            events.len() * RECORD_SIZE,
            "the buffered bytes are on disk now, with the writer still alive"
        );
        assert_eq!(journal::parse_file(&bytes), events);
        drop(writer);
    }

    /// The reader treats any prefix of the rolling series as valid:
    /// deleting the finalized files (in whatever order the listing hands
    /// them back, at any time) leaves the open file's records parsing and
    /// the resume scan rebuilding over them alone.
    #[test]
    fn reader_valid_prefix_deleting_finalized_files() {
        let dir = scratch("valid-prefix");
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
        let windows = [
            vec![
                event(KIND_HOLD, 1_000, 31, 5_000),
                event(KIND_RENEW, 1_100, 31, 6_000),
            ],
            vec![
                event(KIND_HOLD, 2_000, 32, 6_000),
                event(KIND_RELEASE, 2_100, 32, 7_000),
            ],
            vec![
                event(KIND_HOLD, 3_000, 33, 7_000),
                event(KIND_BREAK, 3_100, 33, 8_000),
            ],
        ];
        for (index, window) in windows.iter().enumerate() {
            for one in window {
                core.append(one).expect("the record appends");
            }
            core.flush().expect("the buffer drains");
            if index < windows.len() - 1 {
                core.roll().expect("the roll fires");
            }
        }
        drop(core);
        assert_eq!(
            finalized_files(&dir).len(),
            2,
            "two files finalized, one open"
        );
        // The finalized files delete freely.
        for path in finalized_files(&dir) {
            fs::remove_file(&path).expect("the finalized file deletes");
        }
        assert!(
            finalized_files(&dir).is_empty(),
            "the prefix of the series is gone"
        );
        // The open file's records still parse.
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        assert_eq!(journal::parse_file(&bytes), windows[2]);
        // The resume scan rebuilds over them alone.
        let reopened = AofCore::open(&dir, 64 * 1024).expect("the series reopens");
        assert_eq!(
            reopened.written,
            (windows[2].len() * RECORD_SIZE) as u64,
            "the scan rebuilds the position over the surviving prefix"
        );
        let window = reopened.window.as_ref().expect("the window rebuilds");
        assert_eq!(window.op_min, 3_000);
        assert_eq!(window.count, 2);
    }

    /// The fsync counter moves only on the checkpoint, the roll, and the
    /// shutdown: the open never fsyncs, the buffered-write drain never
    /// fsyncs, and every named site adds exactly one.
    #[test]
    fn syncs_fire_only_on_checkpoint_roll_and_shutdown() {
        let dir = scratch("syncs");
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
        assert_eq!(core.syncs, 0, "the open never fsyncs");
        core.append(&event(KIND_HOLD, 1_000, 11, 5_000))
            .expect("the record appends");
        core.flush().expect("the buffer drains");
        assert_eq!(core.syncs, 0, "the buffered-write drain never fsyncs");
        core.checkpoint().expect("the checkpoint succeeds");
        assert_eq!(core.syncs, 1, "the checkpoint is one fsync");
        core.append(&event(KIND_RENEW, 1_100, 11, 6_000))
            .expect("the record appends");
        core.flush().expect("the buffer drains");
        core.roll().expect("the roll fires");
        assert_eq!(core.syncs, 2, "the roll is one fsync");
        core.shutdown().expect("the shutdown succeeds");
        assert_eq!(core.syncs, 3, "the shutdown is one fsync");
    }

    /// The resume scan rebuilds the position and the metadata window over
    /// the pre-restart records, and the next roll's metafile names the
    /// FULL window — the rebuilt minimum included.
    #[test]
    fn resume_reopen_rebuilds_position_and_window() {
        let dir = scratch("resume");
        let rebuilt = vec![
            event(KIND_HOLD, 1_000, 31, 5_000),
            event(KIND_RENEW, 2_000, 31, 6_000),
        ];
        {
            let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
            for one in &rebuilt {
                core.append(one).expect("the record appends");
            }
            core.flush().expect("the buffer drains");
            // Dropped without the shutdown: the resume scan is the only
            // rebuild.
        }
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the series reopens");
        assert_eq!(
            core.written,
            (rebuilt.len() * RECORD_SIZE) as u64,
            "the scan rebuilds the position"
        );
        let window = core.window.as_ref().expect("the scan rebuilds the window");
        assert_eq!(window.op_min, 1_000);
        assert_eq!(window.op_max, 2_000);
        assert_eq!(window.expiry_min, 5_000);
        assert_eq!(window.expiry_max, 6_000);
        assert_eq!(window.count, 2);
        core.append(&event(KIND_RELEASE, 3_000, 31, 7_000))
            .expect("the record appends");
        core.flush().expect("the buffer drains");
        core.roll().expect("the roll fires");
        let finalized = finalized_files(&dir);
        assert_eq!(finalized.len(), 1);
        let meta = journal::Meta::decode(
            &fs::read(finalized[0].with_extension("meta")).expect("the metafile reads"),
        )
        .expect("the metafile decodes");
        assert_eq!(
            meta.op_min, 1_000,
            "the rebuilt minimum rolls with the stream"
        );
        assert_eq!(meta.op_max, 3_000);
        assert_eq!(meta.count, 3);
    }

    /// The crash shapes — a whole zero-filled record-sized tail (the roll
    /// pad's bytes) and a mid-record cut — ride past the last complete
    /// record after a kill. Both fail record parsing, and the reopen
    /// truncates them away; the appends continue after the truncation.
    #[test]
    fn resume_truncates_a_torn_tail() {
        let dir = scratch("torn-tail");
        let events = vec![
            event(KIND_HOLD, 2_000, 21, 6_000),
            event(KIND_RENEW, 2_100, 21, 7_000),
            event(KIND_RELEASE, 2_200, 21, 7_000),
        ];
        {
            let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
            for one in &events {
                core.append(one).expect("the record appends");
            }
            core.flush().expect("the buffer drains");
        }
        let open_path = open_file(&dir);
        {
            let mut file = OpenOptions::new()
                .append(true)
                .open(&open_path)
                .expect("the open file appends");
            file.write_all(&[0u8; RECORD_SIZE])
                .expect("the pad-shaped tail appends");
            file.write_all(&[0x42u8; 20])
                .expect("the mid-record cut appends");
        }
        assert_eq!(
            fs::metadata(&open_path).expect("the length reads").len(),
            (events.len() as u64 + 1) * RECORD_SIZE as u64 + 20,
            "the torn tail is on disk before the reopen"
        );
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the series reopens");
        assert_eq!(
            fs::metadata(&open_path).expect("the length reads").len(),
            (events.len() * RECORD_SIZE) as u64,
            "the torn tail truncated to the last complete record"
        );
        assert_eq!(
            core.written,
            (events.len() * RECORD_SIZE) as u64,
            "the scan rebuilt the position at the truncation"
        );
        // The appends continue after the truncation.
        let last = event(KIND_BREAK, 4_000, 33, 8_000);
        core.append(&last).expect("the record appends");
        core.flush().expect("the buffer drains");
        drop(core);
        let bytes = fs::read(&open_path).expect("the open file reads");
        let mut expected = events;
        expected.push(last);
        assert_eq!(
            journal::parse_file(&bytes),
            expected,
            "the appends land after the truncation"
        );
        assert_eq!(bytes.len(), expected.len() * RECORD_SIZE);
    }

    /// The writer thread's bounded queue drops on overflow and counts the
    /// drops: under a burst against a one-deep queue, every enqueue
    /// either landed (the file, in enqueue order) or dropped (the
    /// counter) — nothing else — and the drain still answers.
    #[test]
    fn writer_thread_enqueue_lands_bytes_and_drops_on_overflow() {
        let dir = scratch("overflow");
        const TOTAL: u64 = 20_000;
        let writer = AofWriter::open(
            &dir,
            AofConfig {
                flush_interval: None,
                queue_cap: 1,
                ..AofConfig::default()
            },
        )
        .expect("the series opens");
        let events: Vec<_> = (0..TOTAL)
            .map(|index| event(KIND_HOLD, 5_000 + index, 40 + index, 9_000))
            .collect();
        for one in &events {
            writer.enqueue(one.clone());
        }
        writer.drain().expect("the drain lands the survivors");
        let dropped = writer.drops();
        assert!(dropped > 0, "a one-deep queue overflows under the burst");
        assert!(dropped < TOTAL, "the writer kept up at least once");
        drop(writer);
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        let landed = journal::parse_file(&bytes);
        assert!(!landed.is_empty(), "the first events landed");
        assert_eq!(
            landed.len() as u64 + dropped,
            TOTAL,
            "every enqueue either landed or dropped"
        );
        assert!(
            landed.windows(2).all(|pair| pair[0].ts < pair[1].ts),
            "the landed records ride in enqueue order"
        );
        assert!(
            landed
                .iter()
                .all(|one| one.ts >= 5_000 && one.ts < 5_000 + TOTAL),
            "the landed records came from the input"
        );
    }

    /// The writer thread's checkpoint request flushes the buffer to the
    /// file, and the last handle's drop flushes whatever remained — the
    /// shutdown drain. Below the write-buffer threshold, with the
    /// periodic-fsync knob OFF, the named paths are what landed the
    /// bytes (each observed inside a bounded deadline, never an
    /// infinite wait).
    #[test]
    fn writer_thread_checkpoint_and_shutdown_flush() {
        let dir = scratch("checkpoint-shutdown");
        let writer = AofWriter::open(
            &dir,
            AofConfig {
                flush_interval: None,
                flush_bytes: 1024 * 1024,
                ..AofConfig::default()
            },
        )
        .expect("the series opens");
        let first: Vec<_> = (0..20u64)
            .map(|index| event(KIND_HOLD, 6_000 + index, 50 + index, 9_000))
            .collect();
        for one in &first {
            writer.enqueue(one.clone());
        }
        writer.checkpoint();
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let bytes = fs::read(open_file(&dir)).expect("the open file reads");
            if journal::parse_file(&bytes) == first {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the checkpoint flush never landed the buffer"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        // The shutdown flush: twenty more events ride past the last
        // handle's drop, and the thread's parting drain lands them.
        let second: Vec<_> = (0..20u64)
            .map(|index| event(KIND_RENEW, 7_000 + index, 60 + index, 9_000))
            .collect();
        for one in &second {
            writer.enqueue(one.clone());
        }
        drop(writer);
        let mut expected = first;
        expected.extend(second);
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let bytes = fs::read(open_file(&dir)).expect("the open file reads");
            if journal::parse_file(&bytes) == expected {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the shutdown flush never landed the buffer"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
    }

    /// The continuous-load regression: a stream whose inter-event gaps are
    /// shorter than the pump tick must still reach the file — the buffered
    /// drain runs on its own cadence, never on recv_timeout expiry alone,
    /// and the periodic fsync fires while the stream keeps flowing.
    #[test]
    fn writer_thread_flushes_under_continuous_load() {
        let dir = scratch("continuous-load");
        let writer = AofWriter::open(
            &dir,
            AofConfig {
                flush_interval: Some(Duration::from_millis(150)),
                ..AofConfig::default()
            },
        )
        .expect("the series opens");
        let events: Vec<_> = (0..40u64)
            .map(|index| event(KIND_HOLD, 8_000 + index, 70 + index, 9_000))
            .collect();
        for one in &events {
            writer.enqueue(one.clone());
            std::thread::sleep(Duration::from_millis(50));
        }
        // No drain, no checkpoint: the file accumulates while the stream
        // keeps flowing.
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            let bytes = fs::read(open_file(&dir)).expect("the open file reads");
            if journal::parse_file(&bytes) == events {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "the continuous stream never reached the file"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        assert_eq!(writer.drops(), 0, "the stream fit the queue");
        drop(writer);
    }

    /// The writer thread rolls when the block fills: the drain answers
    /// after the roll, the finalized file is exactly one erasure block,
    /// the metafile names the finalized window, and the rollover record
    /// starts the fresh file.
    #[test]
    fn writer_thread_rolls_when_block_fills() {
        let dir = scratch("thread-roll");
        const RECORDS: u64 = ROLL_BYTES / RECORD_SIZE as u64 + 1;
        let writer = AofWriter::open(&dir, config()).expect("the series opens");
        let events: Vec<_> = (0..RECORDS)
            .map(|index| event(KIND_HOLD, 10_000 + index, 80 + index, 9_000))
            .collect();
        for one in &events {
            writer.enqueue(one.clone());
        }
        writer.drain().expect("the drain lands every queued record");
        assert_eq!(writer.drops(), 0, "no record drops at this depth");
        drop(writer);
        let finalized = finalized_files(&dir);
        assert_eq!(finalized.len(), 1, "the writer rolled exactly once");
        assert_eq!(
            fs::metadata(&finalized[0]).expect("the length reads").len(),
            ROLL_BYTES,
            "the finalized file is exactly one erasure block"
        );
        let meta = journal::Meta::decode(
            &fs::read(finalized[0].with_extension("meta")).expect("the metafile reads"),
        )
        .expect("the metafile decodes");
        assert_eq!(
            meta.count as usize,
            (RECORDS - 1) as usize,
            "the roll fired at the record that would pass the mark"
        );
        assert_eq!(meta.op_max, 10_000 + RECORDS - 2);
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        assert_eq!(
            journal::parse_file(&bytes),
            events[(RECORDS - 1) as usize..],
            "the rollover record starts the fresh file"
        );
    }

    /// The metafile written at the roll matches the window: the fixed
    /// 40-byte file decodes to the rolled events' min/max ts and expiry
    /// and the count, and the finalized file's own name spells the same
    /// window.
    #[test]
    fn metafile_written_at_roll_matches_window() {
        let dir = scratch("metafile");
        let mut core = AofCore::open(&dir, 64 * 1024).expect("the core opens");
        let events = vec![
            event(KIND_HOLD, 1_000, 11, 9_000),
            event(KIND_RENEW, 2_000, 12, 5_000),
            event(KIND_RELEASE, 3_000, 13, 7_000),
            event(KIND_BREAK, 4_000, 14, 6_000),
            event(KIND_HOLD, 5_000, 15, 8_000),
        ];
        for one in &events {
            core.append(one).expect("the record appends");
        }
        core.flush().expect("the buffer drains");
        core.roll().expect("the roll fires");
        let finalized = finalized_files(&dir);
        assert_eq!(finalized.len(), 1);
        let meta_bytes = fs::read(finalized[0].with_extension("meta")).expect("the metafile reads");
        assert_eq!(
            meta_bytes.len(),
            journal::META_SIZE,
            "the fixed metafile size"
        );
        let meta = journal::Meta::decode(&meta_bytes).expect("the metafile decodes");
        assert_eq!(
            meta,
            journal::Meta {
                op_min: 1_000,
                op_max: 5_000,
                expiry_min: 5_000,
                expiry_max: 9_000,
                count: 5,
            }
        );
        assert_eq!(
            finalized[0]
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or(""),
            "ev-1000-5000-5000-9000.bin",
            "the finalized file's name is the same window"
        );
    }
}
