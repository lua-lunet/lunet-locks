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
                warn!("aof: io_uring unavailable ({e}); falling back to buffered write_all");
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
        debug_assert_eq!(self.sink.offset, ROLL_BYTES);
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
                if !failed && core.checkpoint().is_ok() {
                    last_sync = Instant::now();
                    last_flush = Instant::now();
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
        if interval.is_some_and(|iv| last_sync.elapsed() >= iv) && core.checkpoint().is_ok() {
            last_sync = Instant::now();
            last_flush = Instant::now();
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
    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn records_per_roll_fits_one_block() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn append_lands_record_bytes_and_events() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn roll_finalizes_exactly_two_mib_with_pad() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    /// The stop drain's guarantee (`AofWriter::drain`): after it returns,
    /// every event enqueued before the call is durable on disk. With the
    /// periodic-fsync knob OFF and 500 records (30.5 KiB) below the 64 KiB
    /// write-buffer threshold, the only thing that could have landed the
    /// buffered bytes is the drain's own flush+fsync — so a full read-back
    /// here proves the drain blocked until durability.
    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn drain_makes_every_queued_event_durable() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn reader_valid_prefix_deleting_finalized_files() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn syncs_fire_only_on_checkpoint_roll_and_shutdown() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn resume_reopen_rebuilds_position_and_window() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn resume_truncates_a_torn_tail() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn writer_thread_enqueue_lands_bytes_and_drops_on_overflow() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn writer_thread_checkpoint_and_shutdown_flush() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    /// The continuous-load regression: a stream whose inter-event gaps are
    /// shorter than the pump tick must still reach the file — the buffered
    /// drain runs on its own cadence, never on recv_timeout expiry alone,
    /// and the periodic fsync fires while the stream keeps flowing.
    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn writer_thread_flushes_under_continuous_load() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn writer_thread_rolls_when_block_fills() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[ignore = "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"]
    #[test]
    fn metafile_written_at_roll_matches_window() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }
}
