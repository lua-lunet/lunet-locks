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
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, sync_channel};
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
    FileExt::seek_write(file, bytes, offset)
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
    use super::*;
    use crate::journal::{self, KIND_HOLD, KIND_RELEASE, KIND_RENEW, parse_file};
    use std::fs;

    fn temp_aof_dir(name: &str) -> PathBuf {
        use std::sync::atomic::AtomicU64;
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lunet-aof-test-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    fn sample_event(kind: u8, ts: u64, lock_id: u64, expiry: u64) -> JournalEvent {
        JournalEvent {
            kind,
            ts,
            lock_id,
            lease_id: 42,
            holder: [0xAA; 16],
            expiry,
        }
    }

    fn read_dir_names(dir: &Path) -> Vec<String> {
        let mut names: Vec<String> = fs::read_dir(dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        names.sort();
        names
    }

    /// Records that fit exactly in one 2 MiB block.
    const RECORDS_PER_ROLL: u64 = ROLL_BYTES / RECORD_SIZE as u64;

    #[test]
    fn records_per_roll_fits_one_block() {
        // 61 * 34379 = 2097119 <= 2097152; one more record would pass the
        // mark, so every finalized file is 34379 records plus the pad.
        assert!(RECORDS_PER_ROLL * RECORD_SIZE as u64 <= ROLL_BYTES);
        assert!(RECORDS_PER_ROLL * RECORD_SIZE as u64 + RECORD_SIZE as u64 > ROLL_BYTES);
    }

    #[test]
    fn append_lands_record_bytes_and_events() {
        let dir = temp_aof_dir("append");
        let mut core = AofCore::open(&dir, 1).unwrap();
        let e1 = sample_event(KIND_HOLD, 1000, 1, 5000);
        let e2 = sample_event(KIND_RENEW, 2000, 2, 6000);
        core.append(&e1).unwrap();
        core.append(&e2).unwrap();
        core.shutdown().unwrap();
        let open_files: Vec<String> = read_dir_names(&dir)
            .into_iter()
            .filter(|n| n.starts_with("ev-open-"))
            .collect();
        assert_eq!(open_files.len(), 1);
        let data = fs::read(dir.join(&open_files[0])).unwrap();
        assert_eq!(data.len(), 2 * RECORD_SIZE);
        let events = parse_file(&data);
        assert_eq!(events, vec![e1, e2]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn roll_finalizes_exactly_two_mib_with_pad() {
        let dir = temp_aof_dir("roll");
        // flush_bytes = 1: every record reaches the file, so the test can
        // read bytes off disk (the sync policy is independent of the buffer
        // threshold and is covered by the syncs test).
        let mut core = AofCore::open(&dir, 1).unwrap();
        let mut expected = Vec::new();
        for i in 0..RECORDS_PER_ROLL {
            let event = sample_event(KIND_HOLD, 1000 + i, 7, 5000 + i);
            expected.push(event.clone());
            core.append(&event).unwrap();
        }
        // All 34379 records fit; no roll yet; the open file holds them all.
        assert_eq!(core.written, RECORDS_PER_ROLL * RECORD_SIZE as u64);
        assert_eq!(core.syncs, 0, "no fsync on the plain append path");
        // One more record cannot fit: it must first pad and roll, then land
        // in a fresh open file.
        let after = sample_event(KIND_RELEASE, 999_999, 7, 999_999);
        core.append(&after).unwrap();
        let names = read_dir_names(&dir);
        let finalized: Vec<&String> = names
            .iter()
            .filter(|n| n.starts_with("ev-") && n.ends_with(".bin") && !n.contains("open"))
            .collect();
        assert_eq!(finalized.len(), 1, "one finalized file: {names:?}");
        let meta_files: Vec<&String> = names.iter().filter(|n| n.ends_with(".meta")).collect();
        assert_eq!(meta_files.len(), 1);
        let finalized_data = fs::read(dir.join(finalized[0])).unwrap();
        assert_eq!(
            finalized_data.len() as u64,
            ROLL_BYTES,
            "finalized file must be exactly 2 MiB (erasure-block aligned)"
        );
        let pad = ROLL_BYTES as usize - RECORDS_PER_ROLL as usize * RECORD_SIZE;
        assert_eq!(pad, 33);
        // The pad's zero bytes end parsing cleanly (bad magic at the tail).
        let events = parse_file(&finalized_data);
        assert_eq!(events, expected);
        // The trailing record landed in the fresh open file.
        let open_files: Vec<String> = read_dir_names(&dir)
            .into_iter()
            .filter(|n| n.starts_with("ev-open-"))
            .collect();
        assert_eq!(open_files.len(), 1);
        let open_data = fs::read(dir.join(&open_files[0])).unwrap();
        assert_eq!(parse_file(&open_data), vec![after]);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reader_valid_prefix_deleting_finalized_files() {
        let dir = temp_aof_dir("prefix");
        let mut core = AofCore::open(&dir, 1).unwrap();
        let mut seen = 0u64;
        let mut expected_tail = Vec::new();
        let total = RECORDS_PER_ROLL * 2 + 1;
        for i in 0..total {
            let event = sample_event(KIND_HOLD, 1000 + i, 7, 5000 + i);
            if i >= RECORDS_PER_ROLL {
                expected_tail.push(event.clone());
            }
            core.append(&event).unwrap();
            seen += 1;
        }
        // Two rolls happened (one when file 1 could not take the
        // (RECORDS_PER_ROLL+1)th record, one when file 2 hit the same mark).
        let finalized: Vec<String> = read_dir_names(&dir)
            .into_iter()
            .filter(|n| n.starts_with("ev-") && n.ends_with(".bin") && !n.contains("open"))
            .collect();
        assert_eq!(
            finalized.len(),
            2,
            "two finalized blocks after 2*{RECORDS_PER_ROLL}+1 records"
        );
        for name in &finalized {
            let data = fs::read(dir.join(name)).unwrap();
            assert_eq!(data.len() as u64, ROLL_BYTES);
            assert_eq!(
                parse_file(&data).len() as u64,
                RECORDS_PER_ROLL,
                "each finalized file parses RECORDS_PER_ROLL records, pad ends parsing"
            );
        }
        // The user deletes the older finalized file freely.
        fs::remove_file(dir.join(&finalized[0])).unwrap();
        fs::remove_file(dir.join(finalized[0].replace(".bin", ".meta"))).unwrap();
        // The remaining series is a valid reader view: the surviving file and
        // the open file hold exactly the events from `RECORDS_PER_ROLL` on.
        let survivor = fs::read(dir.join(&finalized[1])).unwrap();
        let open_files: Vec<String> = read_dir_names(&dir)
            .into_iter()
            .filter(|n| n.starts_with("ev-open-"))
            .collect();
        let open_data = fs::read(dir.join(&open_files[0])).unwrap();
        let mut history = parse_file(&survivor);
        history.extend(parse_file(&open_data));
        assert_eq!(history, expected_tail);
        assert_eq!(history.len() as u64 + RECORDS_PER_ROLL, seen);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn syncs_fire_only_on_checkpoint_roll_and_shutdown() {
        let dir = temp_aof_dir("syncs");
        let mut core = AofCore::open(&dir, ROLL_BYTES as usize).unwrap();
        for i in 0..3 {
            core.append(&sample_event(KIND_HOLD, 1000 + i, 7, 5000 + i))
                .unwrap();
        }
        assert_eq!(core.syncs, 0, "append path never fsyncs");
        core.checkpoint().unwrap();
        assert_eq!(core.syncs, 1, "checkpoint fsyncs");
        // Fill to the roll: the finalized file is fsync'd at roll.
        for i in 0..RECORDS_PER_ROLL {
            core.append(&sample_event(KIND_HOLD, 2000 + i, 7, 6000 + i))
                .unwrap();
        }
        assert_eq!(core.syncs, 2, "roll fsyncs the finalized file");
        core.shutdown().unwrap();
        assert_eq!(core.syncs, 3, "shutdown fsyncs");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resume_reopen_rebuilds_position_and_window() {
        let dir = temp_aof_dir("resume");
        {
            let mut core = AofCore::open(&dir, 1).unwrap();
            core.append(&sample_event(KIND_HOLD, 1000, 1, 5000))
                .unwrap();
            core.append(&sample_event(KIND_RENEW, 2000, 2, 6000))
                .unwrap();
            // No shutdown: simulate a crash with the file already flushed.
        }
        let mut core = AofCore::open(&dir, 1).unwrap();
        assert_eq!(core.written, 2 * RECORD_SIZE as u64);
        let window = core.window.as_ref().unwrap();
        assert_eq!(window.op_min, 1000);
        assert_eq!(window.op_max, 2000);
        assert_eq!(window.count, 2);
        core.append(&sample_event(KIND_RELEASE, 3000, 3, 7000))
            .unwrap();
        core.shutdown().unwrap();
        let open_files: Vec<String> = read_dir_names(&dir)
            .into_iter()
            .filter(|n| n.starts_with("ev-open-"))
            .collect();
        assert_eq!(open_files.len(), 1, "resume keeps the same open file");
        let data = fs::read(dir.join(&open_files[0])).unwrap();
        let events = parse_file(&data);
        assert_eq!(events.len(), 3);
        assert_eq!(events[2].kind, KIND_RELEASE);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resume_truncates_a_torn_tail() {
        let dir = temp_aof_dir("torn");
        {
            let mut core = AofCore::open(&dir, 1).unwrap();
            core.append(&sample_event(KIND_HOLD, 1000, 1, 5000))
                .unwrap();
        }
        // Simulate a torn append: 20 stray bytes after the last record.
        let open_files: Vec<String> = read_dir_names(&dir)
            .into_iter()
            .filter(|n| n.starts_with("ev-open-"))
            .collect();
        let path = dir.join(&open_files[0]);
        let mut file = OpenOptions::new().append(true).open(&path).unwrap();
        use std::io::Write;
        file.write_all(&[0xEE; 20]).unwrap();
        drop(file);
        let mut core = AofCore::open(&dir, 1).unwrap();
        assert_eq!(core.written, RECORD_SIZE as u64, "torn tail truncated");
        core.append(&sample_event(KIND_RENEW, 2000, 2, 6000))
            .unwrap();
        core.shutdown().unwrap();
        let data = fs::read(&path).unwrap();
        let events = parse_file(&data);
        assert_eq!(
            events.len(),
            2,
            "records after the truncation point are clean"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn writer_thread_enqueue_lands_bytes_and_drops_on_overflow() {
        let dir = temp_aof_dir("thread");
        let config = AofConfig {
            flush_bytes: 1,
            flush_interval: None,
            queue_cap: 4,
        };
        let writer = AofWriter::open(&dir, config).unwrap();
        let total: u64 = 4096;
        for i in 0..total {
            writer.enqueue(sample_event(KIND_HOLD, 10_000 + i, 9, 90_000 + i));
        }
        // The bounded queue must have dropped something: the producer never
        // waits and the writer thread drains at disk speed.
        assert!(writer.drops() > 0, "overflow must drop, never block");
        // Accepted = total - drops; every accepted event eventually lands.
        let accepted = total - writer.drops();
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        let landed = loop {
            let mut count = 0u64;
            for name in read_dir_names(&dir) {
                if !name.ends_with(".bin") {
                    continue;
                }
                count += parse_file(&fs::read(dir.join(&name)).unwrap()).len() as u64;
            }
            if count == accepted {
                break count;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "accepted events never all landed (want {accepted}, have {count})"
            );
            std::thread::sleep(Duration::from_millis(20));
        };
        assert_eq!(landed, accepted);
        drop(writer);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn writer_thread_checkpoint_and_shutdown_flush() {
        let dir = temp_aof_dir("checkpoint");
        let config = AofConfig {
            flush_bytes: 1 << 20, // large: nothing flushes via the size path
            flush_interval: None,
            queue_cap: 16,
        };
        let writer = AofWriter::open(&dir, config).unwrap();
        for i in 0..8 {
            writer.enqueue(sample_event(KIND_HOLD, 1000 + i, 3, 5000 + i));
        }
        writer.checkpoint();
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let open_files: Vec<String> = read_dir_names(&dir)
                .into_iter()
                .filter(|n| n.starts_with("ev-open-"))
                .collect();
            let size = open_files
                .first()
                .map(|n| fs::metadata(dir.join(n)).unwrap().len())
                .unwrap_or(0);
            if size == 8 * RECORD_SIZE as u64 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "checkpoint did not flush the buffered bytes"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(writer); // graceful shutdown: drains and fsyncs
        let open_files: Vec<String> = read_dir_names(&dir)
            .into_iter()
            .filter(|n| n.starts_with("ev-open-"))
            .collect();
        assert_eq!(open_files.len(), 1);
        let data = fs::read(dir.join(&open_files[0])).unwrap();
        assert_eq!(parse_file(&data).len(), 8);
        let _ = fs::remove_dir_all(&dir);
    }

    /// The continuous-load regression: a stream whose inter-event gaps are
    /// shorter than the pump tick must still reach the file — the buffered
    /// drain runs on its own cadence, never on recv_timeout expiry alone,
    /// and the periodic fsync fires while the stream keeps flowing.
    #[test]
    fn writer_thread_flushes_under_continuous_load() {
        let dir = temp_aof_dir("continuous");
        let writer = AofWriter::open(
            &dir,
            AofConfig {
                flush_bytes: 1 << 20, // the size path never fires here
                flush_interval: Some(Duration::from_millis(300)),
                queue_cap: 256,
            },
        )
        .unwrap();
        let total = 200u64;
        for i in 0..total {
            writer.enqueue(sample_event(KIND_HOLD, 50_000 + i, 13, 95_000 + i));
            // 5 ms per event: every gap is far below the 200 ms pump tick,
            // so recv_timeout never expires on its own.
            std::thread::sleep(Duration::from_millis(5));
        }
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let open_files: Vec<String> = read_dir_names(&dir)
                .into_iter()
                .filter(|n| n.starts_with("ev-open-"))
                .collect();
            let count = open_files
                .first()
                .map(|n| parse_file(&fs::read(dir.join(n)).unwrap()).len() as u64)
                .unwrap_or(0);
            if count == total {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "continuous stream never reached the file (have {count} of {total})"
            );
            std::thread::sleep(Duration::from_millis(20));
        }
        drop(writer);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn writer_thread_rolls_when_block_fills() {
        let dir = temp_aof_dir("thread-roll");
        let writer = AofWriter::open(&dir, AofConfig::default()).unwrap();
        // Enqueue in batches until the writer's active file crosses the
        // 2 MiB mark and rolls. The queue drops on overflow, so batches
        // pause for the writer to drain; the roll itself is deterministic
        // (proven by the core tests above) — this proves the threaded
        // writer drives it end to end.
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        let mut i = 0u64;
        loop {
            for _ in 0..512 {
                writer.enqueue(sample_event(KIND_HOLD, 20_000 + i, 11, 70_000 + i));
                i += 1;
            }
            let finalized: Vec<String> = read_dir_names(&dir)
                .into_iter()
                .filter(|n| n.starts_with("ev-") && n.ends_with(".bin") && !n.contains("open"))
                .collect();
            if finalized.len() == 1 {
                let data = fs::read(dir.join(&finalized[0])).unwrap();
                assert_eq!(
                    data.len() as u64,
                    ROLL_BYTES,
                    "threaded roll is exactly 2 MiB"
                );
                assert_eq!(parse_file(&data).len() as u64, RECORDS_PER_ROLL);
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "threaded writer never rolled at the 2 MiB mark"
            );
            std::thread::sleep(Duration::from_millis(10));
        }
        drop(writer);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn metafile_written_at_roll_matches_window() {
        let dir = temp_aof_dir("meta");
        let mut core = AofCore::open(&dir, ROLL_BYTES as usize).unwrap();
        let first = sample_event(KIND_HOLD, 1111, 5, 5555);
        core.append(&first).unwrap();
        for i in 1..RECORDS_PER_ROLL {
            core.append(&sample_event(KIND_HOLD, 1111 + i, 5, 5555 + i))
                .unwrap();
        }
        core.append(&sample_event(KIND_RELEASE, 8888, 5, 9999))
            .unwrap();
        let names = read_dir_names(&dir);
        let meta_name = names
            .iter()
            .find(|n| n.ends_with(".meta"))
            .expect("metafile written at roll");
        let meta = journal::Meta::decode(&fs::read(dir.join(meta_name)).unwrap()).unwrap();
        assert_eq!(meta.count, RECORDS_PER_ROLL as u32);
        assert_eq!(meta.op_min, 1111);
        assert_eq!(meta.op_max, 1111 + RECORDS_PER_ROLL - 1);
        assert_eq!(meta.expiry_min, 5555);
        assert_eq!(meta.expiry_max, 5555 + RECORDS_PER_ROLL - 1);
        let _ = fs::remove_dir_all(&dir);
    }
}
