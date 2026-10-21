//! The AOF write-behind log: producers enqueue complete `LKE1` records to a
//! dedicated writer thread and never wait on disk; a full queue or a failed
//! write crashes the process (a dead process is the cheapest signal, a
//! silently dropped event the most expensive bug).

use crate::disk::{Disk, DiskFile, std_disk};
use crate::journal::{self, JournalEvent, RECORD_SIZE};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

#[cfg(all(target_os = "linux", feature = "io-uring"))]
use tracing::warn;

#[cfg(test)]
use std::fs;
#[cfg(test)]
use std::fs::OpenOptions;

/// The roll threshold: one erasure block, 2 MiB.
pub const ROLL_BYTES: u64 = 2 * 1024 * 1024;

const DEFAULT_FLUSH_BYTES: usize = 64 * 1024;

const DEFAULT_QUEUE_CAP: usize = 4096;

const DEFAULT_FLUSH_INTERVAL: Duration = Duration::from_millis(1000);

const PUMP_TICK: Duration = Duration::from_millis(200);

/// The writer's tuning knobs.
#[derive(Debug, Clone)]
pub struct AofConfig {
    /// Bytes buffered in the writer thread before one write call.
    pub flush_bytes: usize,
    /// Periodic fsync interval. `None` fsyncs only at roll and shutdown.
    pub flush_interval: Option<Duration>,
    /// Bounded-queue capacity (an overflow crashes the process above it).
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

struct FileSink {
    file: DiskFile,
    path: PathBuf,
    /// Logical write position (bytes written to the active file).
    offset: u64,
    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    ring: Option<io_uring::IoUring>,
}

impl FileSink {
    fn open_file(disk: &dyn Disk, dir: &Path, started_ms: u64) -> io::Result<(PathBuf, DiskFile)> {
        let name = format!("ev-open-{started_ms}.bin");
        let path = dir.join(&name);
        let file = disk.create_new_read_write(&path)?;
        Ok((path, file))
    }

    fn new(disk: &dyn Disk, dir: &Path, started_ms: u64) -> io::Result<Self> {
        let (path, file) = Self::open_file(disk, dir, started_ms)?;
        Self::from_parts(path, file, 0)
    }

    fn adopt(path: PathBuf, file: DiskFile, offset: u64) -> io::Result<Self> {
        Self::from_parts(path, file, offset)
    }

    #[cfg(all(target_os = "linux", feature = "io-uring"))]
    fn from_parts(path: PathBuf, file: DiskFile, offset: u64) -> io::Result<Self> {
        // The ring needs an operating-system descriptor; an engine-backed
        // `DiskFile` has none and writes through its engine.
        let ring = match file.as_raw_fd() {
            Some(_) => match io_uring::IoUring::new(64) {
                Ok(ring) => Some(ring),
                Err(e) => {
                    warn!(
                        ts = crate::log_millis(),
                        event = "aof-io-uring-unavailable",
                        "aof: io_uring unavailable ({e}); falling back to buffered write_all"
                    );
                    None
                }
            },
            None => {
                warn!(
                    ts = crate::log_millis(),
                    event = "aof-io-uring-engine-file",
                    "aof: the open file is engine-backed and carries no descriptor; \
                     falling back to the engine's own buffered write"
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
    fn from_parts(path: PathBuf, file: DiskFile, offset: u64) -> io::Result<Self> {
        Ok(Self { file, path, offset })
    }

    fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        #[cfg(all(target_os = "linux", feature = "io-uring"))]
        if self.ring.is_some() {
            // Taken out for the call: the ring write touches
            // `self.file`/`self.offset` too.
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

        // The ring is armed only for a descriptor-bearing handle (see
        // `from_parts`).
        let fd = types::Fd(
            self.file
                .as_raw_fd()
                .expect("the ring is armed only for a descriptor-bearing handle"),
        );
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
        self.file.write_all_at(self.offset, bytes)?;
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

struct AofCore {
    dir: PathBuf,
    flush_bytes: usize,
    /// The seam every filesystem operation in this writer crosses.
    disk: Arc<dyn Disk>,
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
    #[cfg(test)]
    fn open(dir: &Path, flush_bytes: usize) -> io::Result<Self> {
        Self::open_on(std_disk(), dir, flush_bytes)
    }

    fn open_on(disk: Arc<dyn Disk>, dir: &Path, flush_bytes: usize) -> io::Result<Self> {
        disk.create_dir_all(dir)?;
        let (sink, written, window) = match journal::find_open_file_on(&*disk, dir)? {
            Some(entry) => {
                let path = entry.path();
                let mut file = disk.open_read_write(&path)?;
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
                let len = file.file_len()?;
                if len > scanned {
                    file.set_len(scanned)?;
                }
                (FileSink::adopt(path, file, scanned)?, scanned, window)
            }
            None => {
                let started_ms = unix_millis();
                (FileSink::new(&*disk, dir, started_ms)?, 0, None)
            }
        };
        Ok(Self {
            dir: dir.to_path_buf(),
            flush_bytes,
            disk,
            sink,
            written,
            window,
            syncs: 0,
            buf: Vec::with_capacity(flush_bytes.max(RECORD_SIZE) * 2),
        })
    }

    /// Append one event; roll before when the record would pass the 2 MiB
    /// mark, and after when it lands exactly on it.
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

    fn shutdown(&mut self) -> io::Result<()> {
        self.checkpoint()
    }

    /// Zero-pad the active file to exactly [`ROLL_BYTES`], fsync the
    /// finalized file, rename it, write the metafile atomically, and open
    /// a fresh open file.
    fn roll(&mut self) -> io::Result<()> {
        let window = match &self.window {
            Some(w) => w.clone(),
            None => return Ok(()),
        };
        let pad = (ROLL_BYTES - self.written) as usize;
        self.buf.resize(self.buf.len() + pad, 0);
        self.flush_buf()?;
        self.sink.sync()?;
        self.syncs += 1;
        // A finalized file of any other size breaks the erasure-block
        // contract; the panic is the proof, in every build.
        assert_eq!(
            self.sink.offset, ROLL_BYTES,
            "the roll finalized the erasure block"
        );
        let final_name = format!(
            "ev-{}-{}-{}-{}.bin",
            window.op_min, window.op_max, window.expiry_min, window.expiry_max
        );
        let final_path = self.dir.join(&final_name);
        self.disk.rename(&self.sink.path, &final_path)?;
        journal::write_meta_atomic(
            &*self.disk,
            &final_path.with_extension("meta"),
            &window.meta(),
        )?;
        self.sink = FileSink::new(&*self.disk, &self.dir, unix_millis())?;
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

enum Msg {
    Event(JournalEvent),
    Checkpoint,
    /// The stop path's drain request: the FIFO answer proves every event
    /// enqueued before the drain is durable.
    Drain(Arc<DrainSignal>),
}

/// The drain completion: `None` until the writer answers.
struct DrainSignal {
    state: Mutex<Option<Result<(), String>>>,
    cvar: Condvar,
}

struct Inner {
    tx: SyncSender<Msg>,
}

/// A handle to the AOF writer thread: [`AofWriter::enqueue`] never blocks
/// (an overflow crashes the process); the last handle's drop drains and
/// fsyncs.
pub struct AofWriter {
    inner: Arc<Inner>,
}

impl AofWriter {
    /// Open (or resume) the AOF series at `dir` and start the writer thread.
    pub fn open(dir: &Path, config: AofConfig) -> io::Result<Self> {
        Self::open_on(std_disk(), dir, config)
    }

    /// [`AofWriter::open`] against a named [`Disk`] — every buffered write,
    /// fsync, roll and metafile crosses that one seam.
    pub fn open_on(disk: Arc<dyn Disk>, dir: &Path, config: AofConfig) -> io::Result<Self> {
        let core = AofCore::open_on(disk, dir, config.flush_bytes)?;
        let (tx, rx) = sync_channel::<Msg>(config.queue_cap);
        let interval = config.flush_interval;
        std::thread::Builder::new()
            .name("aof-writer".to_string())
            .spawn(move || pump(rx, core, interval))
            .map_err(io::Error::other)?;
        Ok(Self {
            inner: Arc::new(Inner { tx }),
        })
    }

    /// Enqueue one complete event. Never blocks; a full queue or a
    /// disconnected writer panics (the let-it-crash law).
    pub fn enqueue(&self, event: JournalEvent) {
        match self.inner.tx.try_send(Msg::Event(event)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) => {
                panic!(
                    "aof: the write-behind queue overflowed; the let-it-crash law asserts and crashes"
                )
            }
            Err(TrySendError::Disconnected(_)) => {
                panic!("aof: the writer thread is gone; the let-it-crash law asserts and crashes")
            }
        }
    }

    /// Request an fsync. Best-effort: a full queue drops the request.
    pub fn checkpoint(&self) {
        let _ = self.inner.tx.try_send(Msg::Checkpoint);
    }

    /// Block until every event enqueued before this call is appended and
    /// fsynced (FIFO: the writer's answer proves every earlier event
    /// landed). The stop path writes its `flushed` marker only after this
    /// returns Ok; an Err aborts the process (the caller's law).
    pub fn drain(&self) -> io::Result<()> {
        let signal = Arc::new(DrainSignal {
            state: Mutex::new(None),
            cvar: Condvar::new(),
        });
        loop {
            match self.inner.tx.try_send(Msg::Drain(Arc::clone(&signal))) {
                Ok(()) => break,
                Err(TrySendError::Full(_)) => {
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
}

fn pump(rx: Receiver<Msg>, mut core: AofCore, interval: Option<Duration>) {
    // The buffered-write drain runs on its own short cadence: a continuous
    // event stream must not starve it, and the fsync knob stays
    // independent of the byte flow.
    let mut last_flush = Instant::now();
    let mut last_sync = Instant::now();
    loop {
        match rx.recv_timeout(PUMP_TICK) {
            Ok(Msg::Event(event)) => {
                if let Err(e) = core.append(&event) {
                    eprintln!(
                        "lunet-advisory-lock: aof append failed ({e}); \
                         the let-it-crash law aborts the process"
                    );
                    std::process::abort();
                }
            }
            Ok(Msg::Checkpoint) => {
                if let Err(e) = core.checkpoint() {
                    eprintln!(
                        "lunet-advisory-lock: aof checkpoint request failed ({e}); \
                         the fsync was not taken; the let-it-crash law aborts the process"
                    );
                    std::process::abort();
                }
                last_sync = Instant::now();
                last_flush = Instant::now();
            }
            Ok(Msg::Drain(signal)) => {
                let outcome = core.checkpoint();
                if let Err(e) = &outcome {
                    eprintln!(
                        "lunet-advisory-lock: aof drain flush failed ({e}); \
                         the let-it-crash law aborts the process"
                    );
                }
                let failed = outcome.is_err();
                let mut state = signal.state.lock().expect("drain state poisoned");
                *state = Some(outcome.map_err(|e| e.to_string()));
                signal.cvar.notify_all();
                if failed {
                    // The stop's caller aborts on the reported failure
                    // too; this thread does not outlive a failed write.
                    std::process::abort();
                }
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if last_flush.elapsed() >= PUMP_TICK {
            if let Err(e) = core.flush() {
                eprintln!(
                    "lunet-advisory-lock: aof flush failed ({e}); \
                     the let-it-crash law aborts the process"
                );
                std::process::abort();
            }
            last_flush = Instant::now();
        }
        if interval.is_some_and(|iv| last_sync.elapsed() >= iv) {
            if let Err(e) = core.checkpoint() {
                eprintln!(
                    "lunet-advisory-lock: aof periodic fsync failed ({e}); \
                     the let-it-crash law aborts the process"
                );
                std::process::abort();
            }
            last_sync = Instant::now();
            last_flush = Instant::now();
        }
    }
    if let Err(e) = core.shutdown() {
        eprintln!(
            "lunet-advisory-lock: aof shutdown flush failed ({e}); \
             the let-it-crash law aborts the process"
        );
        std::process::abort();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::journal::{KIND_BREAK, KIND_HOLD, KIND_RELEASE, KIND_RENEW};
    use std::io::Write as _;
    use std::sync::atomic::{AtomicU64, Ordering};

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
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        assert_eq!(
            bytes.len(),
            events.len() * RECORD_SIZE,
            "the buffered bytes are on disk now, with the writer still alive"
        );
        assert_eq!(journal::parse_file(&bytes), events);
        drop(writer);
    }

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
        for path in finalized_files(&dir) {
            fs::remove_file(&path).expect("the finalized file deletes");
        }
        assert!(
            finalized_files(&dir).is_empty(),
            "the prefix of the series is gone"
        );
        let bytes = fs::read(open_file(&dir)).expect("the open file reads");
        assert_eq!(journal::parse_file(&bytes), windows[2]);
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

    #[test]
    fn enqueue_panics_when_the_queue_overflows() {
        let dir = scratch("overflow");
        let writer = AofWriter::open(
            &dir,
            AofConfig {
                flush_interval: None,
                queue_cap: 1,
                ..AofConfig::default()
            },
        )
        .expect("the series opens");
        writer.enqueue(event(KIND_HOLD, 5_000, 40, 9_000));
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            writer.enqueue(event(KIND_HOLD, 5_001, 41, 9_000));
        }));
        assert!(
            result.is_err(),
            "the overflow enqueue panics: the let-it-crash law's assert-and-crash"
        );
    }

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
        drop(writer);
    }

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

    #[test]
    fn an_append_failure_aborts_the_process() {
        if std::env::var("AOF_ABORT_CHILD").is_ok() {
            use crate::disk::{Disk, DiskFile, EngineFile};
            use std::io::SeekFrom;

            struct FailingEngine;
            impl EngineFile for FailingEngine {
                fn read_exact(&mut self, _buf: &mut [u8]) -> io::Result<()> {
                    Err(io::Error::other("refused"))
                }
                fn seek(&mut self, _from: SeekFrom) -> io::Result<u64> {
                    Ok(0)
                }
                fn write_all(&mut self, _bytes: &[u8]) -> io::Result<()> {
                    Err(io::Error::other("refused"))
                }
                fn flush(&mut self) -> io::Result<()> {
                    Err(io::Error::other("refused"))
                }
                fn sync_all(&self) -> io::Result<()> {
                    Err(io::Error::other("refused"))
                }
                fn set_len(&mut self, _len: u64) -> io::Result<()> {
                    Err(io::Error::other("refused"))
                }
                fn file_len(&self) -> io::Result<u64> {
                    Ok(0)
                }
                fn write_all_at(&mut self, _offset: u64, _bytes: &[u8]) -> io::Result<()> {
                    Err(io::Error::other("refused"))
                }
            }

            struct FailingDisk;
            impl Disk for FailingDisk {
                fn create_dir_all(&self, path: &Path) -> io::Result<()> {
                    std::fs::create_dir_all(path)
                }
                fn remove_dir_all(&self, _path: &Path) -> io::Result<()> {
                    unimplemented!("the child never deletes")
                }
                fn rename(&self, _from: &Path, _to: &Path) -> io::Result<()> {
                    unimplemented!("the child never rolls")
                }
                fn remove_file(&self, _path: &Path) -> io::Result<()> {
                    unimplemented!("the child never deletes")
                }
                fn read(&self, _path: &Path) -> io::Result<Vec<u8>> {
                    unimplemented!("the child never reads a metafile")
                }
                fn read_to_string(&self, _path: &Path) -> io::Result<String> {
                    unimplemented!("the child never reads a metafile")
                }
                fn exists(&self, _path: &Path) -> bool {
                    false
                }
                fn file_len(&self, _path: &Path) -> io::Result<u64> {
                    unimplemented!("the child never stats")
                }
                fn read_dir(&self, _path: &Path) -> io::Result<Vec<crate::disk::DiskDirEntry>> {
                    Ok(vec![])
                }
                fn sync_path(&self, _path: &Path) -> io::Result<()> {
                    Ok(())
                }
                fn open_read(&self, _path: &Path) -> io::Result<DiskFile> {
                    unimplemented!("the child never opens for read")
                }
                fn open_read_write(&self, _path: &Path) -> io::Result<DiskFile> {
                    unimplemented!("the child never resumes")
                }
                fn open_read_append(&self, _path: &Path) -> io::Result<DiskFile> {
                    unimplemented!("the child never appends")
                }
                fn open_or_create_write(&self, _path: &Path) -> io::Result<DiskFile> {
                    unimplemented!("the child never writes metafiles")
                }
                fn open_or_create_append(&self, _path: &Path) -> io::Result<DiskFile> {
                    unimplemented!("the child never appends")
                }
                fn create_new_write(&self, _path: &Path) -> io::Result<DiskFile> {
                    unimplemented!("the child never rolls")
                }
                fn create_new_append(&self, _path: &Path) -> io::Result<DiskFile> {
                    unimplemented!("the child never rolls")
                }
                fn create_new_read_write(&self, _path: &Path) -> io::Result<DiskFile> {
                    Ok(DiskFile::from_engine(Box::new(FailingEngine)))
                }
            }

            let writer = AofWriter::open_on(
                std::sync::Arc::new(FailingDisk),
                &scratch("abort-child"),
                AofConfig {
                    flush_bytes: RECORD_SIZE,
                    flush_interval: None,
                    queue_cap: 16,
                },
            )
            .expect("the core opens: the fresh directory creates");
            writer.enqueue(event(KIND_HOLD, 1_000, 7, 5_000));
            loop {
                std::thread::sleep(Duration::from_millis(50));
            }
        }

        let exe = std::env::current_exe().expect("the test binary resolves");
        let output = std::process::Command::new(exe)
            .args([
                "aof::tests::an_append_failure_aborts_the_process",
                "--exact",
                "--nocapture",
            ])
            .env("AOF_ABORT_CHILD", "1")
            .output()
            .expect("the child spawns");
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            assert_eq!(
                output.status.signal(),
                Some(6),
                "the child died by SIGABRT: the let-it-crash law aborts the process \
                 on a refused write, never a clean exit (stderr: {})",
                String::from_utf8_lossy(&output.stderr)
            );
        }
        #[cfg(not(unix))]
        assert!(!output.status.success(), "the child died abnormally");
    }
}
