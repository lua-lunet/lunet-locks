//! Append-only lock-event journal.
//!
//! Fixed 61-byte binary records written to rolling files under a per-replica
//! directory, with a fixed-size metafile written atomically when a file rolls.
//! The journal is observability data: it must never block, poison, or fail the
//! replication path. A journal error logs to stderr and disables journaling
//! for the process; the node keeps serving.
//!
//! # Event record format (v1, big-endian, fixed 61 bytes)
//!
//! ```text
//! magic "LKE1" (4B) | len u32 (= 53) | kind u8 | ts u64 | lock_id u64 |
//! lease_id u64 | holder [u8;16] | expiry u64 | crc32 u32
//! ```
//!
//! - `kind`: 1 = hold, 2 = renew, 3 = release.
//! - `ts`: unix ms at apply.
//! - Release records carry the released lease's holder/lease_id/expiry.
//! - `crc32` (IEEE, over bytes from `kind` through `expiry` inclusive).
//! - All records are fixed-length; the `len` field is kept for forward
//!   compatibility.
//!
//! # Metafile format (fixed 40 bytes)
//!
//! ```text
//! magic "LKM1" (4B) | op_min u64 | op_max u64 | expiry_min u64 |
//! expiry_max u64 | count u32
//! ```
//!
//! Written once per rolled file, atomically (tmp + rename + dir sync).
//!
//! # File naming
//!
//! - Open (still-appending): `ev-open-<ts_started_ms>.bin` — no metafile.
//! - Rolled: `ev-<op_min>-<op_max>-<expiry_min>-<expiry_max>.bin` + `.meta`.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// Magic bytes for event records.
const RECORD_MAGIC: &[u8; 4] = b"LKE1";
/// Magic bytes for metafiles.
const META_MAGIC: &[u8; 4] = b"LKM1";

/// Fixed size of one event record on disk.
pub const RECORD_SIZE: usize = 61;
/// Fixed size of one metafile on disk.
pub const META_SIZE: usize = 40;

/// The payload length field value (everything after magic+len, before crc32).
const RECORD_PAYLOAD_LEN: u32 = 53;

/// Kind discriminants.
pub const KIND_HOLD: u8 = 1;
pub const KIND_RENEW: u8 = 2;
pub const KIND_RELEASE: u8 = 3;

/// A single lock-event journal entry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JournalEvent {
    pub kind: u8,
    pub ts: u64,
    pub lock_id: u64,
    pub lease_id: u64,
    pub holder: [u8; 16],
    pub expiry: u64,
}

impl JournalEvent {
    /// Encode this event into a fixed-size 61-byte record.
    pub fn encode(&self) -> [u8; RECORD_SIZE] {
        let mut buf = [0u8; RECORD_SIZE];
        buf[0..4].copy_from_slice(RECORD_MAGIC);
        buf[4..8].copy_from_slice(&RECORD_PAYLOAD_LEN.to_be_bytes());
        buf[8] = self.kind;
        buf[9..17].copy_from_slice(&self.ts.to_be_bytes());
        buf[17..25].copy_from_slice(&self.lock_id.to_be_bytes());
        buf[25..33].copy_from_slice(&self.lease_id.to_be_bytes());
        buf[33..49].copy_from_slice(&self.holder);
        buf[49..57].copy_from_slice(&self.expiry.to_be_bytes());
        let crc = crc32_ieee(&buf[8..57]);
        buf[57..61].copy_from_slice(&crc.to_be_bytes());
        buf
    }
}

/// Parse a single record from the front of `data`. Returns `None` if the
/// buffer is too short, the magic/crc is wrong, or the kind is unknown.
/// On success returns `(event, consumed)` where `consumed == RECORD_SIZE`.
pub fn parse_record(data: &[u8]) -> Option<(JournalEvent, usize)> {
    if data.len() < RECORD_SIZE {
        return None;
    }
    if &data[0..4] != RECORD_MAGIC {
        return None;
    }
    // Verify the stored crc32 against the payload region.
    let stored_crc = u32::from_be_bytes(data[57..61].try_into().ok()?);
    let computed_crc = crc32_ieee(&data[8..57]);
    if stored_crc != computed_crc {
        return None;
    }
    let kind = data[8];
    if !matches!(kind, KIND_HOLD | KIND_RENEW | KIND_RELEASE) {
        return None;
    }
    let ts = u64::from_be_bytes(data[9..17].try_into().ok()?);
    let lock_id = u64::from_be_bytes(data[17..25].try_into().ok()?);
    let lease_id = u64::from_be_bytes(data[25..33].try_into().ok()?);
    let mut holder = [0u8; 16];
    holder.copy_from_slice(&data[33..49]);
    let expiry = u64::from_be_bytes(data[49..57].try_into().ok()?);
    Some((
        JournalEvent {
            kind,
            ts,
            lock_id,
            lease_id,
            holder,
            expiry,
        },
        RECORD_SIZE,
    ))
}

/// Parse all valid records from `data`, stopping cleanly at the first
/// invalid or short record (corrupt-tail tolerance).
pub fn parse_file(data: &[u8]) -> Vec<JournalEvent> {
    let mut events = Vec::new();
    let mut offset = 0;
    while offset + RECORD_SIZE <= data.len() {
        match parse_record(&data[offset..]) {
            Some((event, consumed)) => {
                events.push(event);
                offset += consumed;
            }
            None => break,
        }
    }
    events
}

/// Metadata written atomically when an event file rolls.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Meta {
    pub op_min: u64,
    pub op_max: u64,
    pub expiry_min: u64,
    pub expiry_max: u64,
    pub count: u32,
}

impl Meta {
    /// Encode into a fixed-size 40-byte metafile.
    pub fn encode(&self) -> [u8; META_SIZE] {
        let mut buf = [0u8; META_SIZE];
        buf[0..4].copy_from_slice(META_MAGIC);
        buf[4..12].copy_from_slice(&self.op_min.to_be_bytes());
        buf[12..20].copy_from_slice(&self.op_max.to_be_bytes());
        buf[20..28].copy_from_slice(&self.expiry_min.to_be_bytes());
        buf[28..36].copy_from_slice(&self.expiry_max.to_be_bytes());
        buf[36..40].copy_from_slice(&self.count.to_be_bytes());
        buf
    }

    /// Decode a metafile from exactly 40 bytes. Returns `None` on bad magic.
    pub fn decode(data: &[u8]) -> Option<Self> {
        if data.len() < META_SIZE {
            return None;
        }
        if &data[0..4] != META_MAGIC {
            return None;
        }
        let op_min = u64::from_be_bytes(data[4..12].try_into().ok()?);
        let op_max = u64::from_be_bytes(data[12..20].try_into().ok()?);
        let expiry_min = u64::from_be_bytes(data[20..28].try_into().ok()?);
        let expiry_max = u64::from_be_bytes(data[28..36].try_into().ok()?);
        let count = u32::from_be_bytes(data[36..40].try_into().ok()?);
        Some(Self {
            op_min,
            op_max,
            expiry_min,
            expiry_max,
            count,
        })
    }
}

/// Rolling-window accumulator for the current open file's metadata.
#[derive(Debug, Clone)]
struct Window {
    op_min: u64,
    op_max: u64,
    expiry_min: u64,
    expiry_max: u64,
    count: u32,
}

impl Window {
    fn new(event: &JournalEvent) -> Self {
        Self {
            op_min: event.ts,
            op_max: event.ts,
            expiry_min: event.expiry,
            expiry_max: event.expiry,
            count: 1,
        }
    }

    fn update(&mut self, event: &JournalEvent) {
        self.op_min = self.op_min.min(event.ts);
        self.op_max = self.op_max.max(event.ts);
        self.expiry_min = self.expiry_min.min(event.expiry);
        self.expiry_max = self.expiry_max.max(event.expiry);
        self.count += 1;
    }

    fn meta(&self) -> Meta {
        Meta {
            op_min: self.op_min,
            op_max: self.op_max,
            expiry_min: self.expiry_min,
            expiry_max: self.expiry_max,
            count: self.count,
        }
    }
}

/// An append-only lock-event journal over a directory.
pub struct Journal {
    dir: PathBuf,
    roll_bytes: u64,
    file: File,
    open_path: PathBuf,
    started_ms: u64,
    written: u64,
    window: Option<Window>,
}

impl Journal {
    /// Open (or create) a journal rooted at `dir`. If an `ev-open-*.bin`
    /// file already exists, resume appending to it by scanning its records
    /// (bounded by `roll_bytes`) to recompute the window and seeking to end.
    pub fn open(dir: &Path, roll_bytes: u64) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        // Look for an existing open file to resume.
        if let Some(entry) = find_open_file(dir)? {
            let path = entry.path();
            let started_ms = parse_open_timestamp(&path).unwrap_or(0);
            let mut file = OpenOptions::new().read(true).append(true).open(&path)?;
            // Scan existing records to recompute the window.
            let mut window: Option<Window> = None;
            let mut scanned: u64 = 0;
            let mut buf = [0u8; RECORD_SIZE];
            loop {
                if scanned + RECORD_SIZE as u64 > roll_bytes {
                    break;
                }
                match file.read_exact(&mut buf) {
                    Ok(()) => {
                        if let Some((event, _)) = parse_record(&buf) {
                            match &mut window {
                                Some(w) => w.update(&event),
                                None => window = Some(Window::new(&event)),
                            }
                            scanned += RECORD_SIZE as u64;
                        } else {
                            break;
                        }
                    }
                    Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e),
                }
            }
            // Seek to end for further appends.
            file.seek(SeekFrom::End(0))?;
            let written = scanned;
            Ok(Self {
                dir: dir.to_path_buf(),
                roll_bytes,
                file,
                open_path: path,
                started_ms,
                written,
                window,
            })
        } else {
            // No open file; we will create one lazily on first append, but
            // the spec says open creates the dir. We create the file eagerly
            // so the caller can verify the journal is writable.
            let started_ms = current_millis_fallback();
            let (file, open_path) = create_open_file(dir, started_ms)?;
            Ok(Self {
                dir: dir.to_path_buf(),
                roll_bytes,
                file,
                open_path,
                started_ms,
                written: 0,
                window: None,
            })
        }
    }

    /// Append one event. On roll, closes the current file, renames it to
    /// the final name, writes the meta atomically, and opens a fresh file.
    pub fn append(&mut self, event: &JournalEvent) -> io::Result<()> {
        let record = event.encode();
        self.file.write_all(&record)?;
        self.file.flush()?;
        match &mut self.window {
            Some(w) => w.update(event),
            None => self.window = Some(Window::new(event)),
        }
        self.written += RECORD_SIZE as u64;
        if self.written >= self.roll_bytes {
            self.roll()?;
        }
        Ok(())
    }

    /// Roll the current open file: close, rename to final name, write meta
    /// atomically, open a fresh `ev-open-*` file.
    fn roll(&mut self) -> io::Result<()> {
        let window = match &self.window {
            Some(w) => w.clone(),
            None => return Ok(()), // nothing written yet
        };
        let meta = window.meta();
        // Close the current file by dropping and reopening is implicit in
        // the rename flow; we just need to flush (already done in append).
        drop(std::mem::replace(
            &mut self.file,
            File::open("/dev/null").unwrap_or_else(|_| {
                // Fallback for platforms without /dev/null: open the file
                // itself read-only as a placeholder.
                File::open(&self.open_path).expect("placeholder file")
            }),
        ));
        // Rename to final name.
        let final_name = format!(
            "ev-{}-{}-{}-{}.bin",
            meta.op_min, meta.op_max, meta.expiry_min, meta.expiry_max
        );
        let final_path = self.dir.join(&final_name);
        fs::rename(&self.open_path, &final_path)?;
        // Write meta atomically.
        let meta_path = final_path.with_extension("meta");
        write_meta_atomic(&meta_path, &meta)?;
        // Open a fresh open file.
        self.started_ms = current_millis_fallback();
        let (file, open_path) = create_open_file(&self.dir, self.started_ms)?;
        self.file = file;
        self.open_path = open_path;
        self.written = 0;
        self.window = None;
        Ok(())
    }
}

/// Find an existing `ev-open-*.bin` file in the directory.
fn find_open_file(dir: &Path) -> io::Result<Option<fs::DirEntry>> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name_str = name.to_string_lossy();
        if name_str.starts_with("ev-open-") && name_str.ends_with(".bin") {
            return Ok(Some(entry));
        }
    }
    Ok(None)
}

/// Parse the timestamp from an `ev-open-<ts>.bin` filename.
fn parse_open_timestamp(path: &Path) -> Option<u64> {
    let name = path.file_name()?.to_str()?;
    let stripped = name.strip_prefix("ev-open-")?.strip_suffix(".bin")?;
    stripped.parse().ok()
}

/// Create a new `ev-open-<ts>.bin` file in the directory.
fn create_open_file(dir: &Path, started_ms: u64) -> io::Result<(File, PathBuf)> {
    let name = format!("ev-open-{started_ms}.bin");
    let path = dir.join(&name);
    let file = OpenOptions::new()
        .create_new(true)
        .append(true)
        .open(&path)?;
    Ok((file, path))
}

/// Write a metafile atomically: tmp + fsync + rename + dir sync.
fn write_meta_atomic(path: &Path, meta: &Meta) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let unique = current_millis_fallback();
    let tmp_name = format!(".{base:?}.tmp-{unique}");
    let tmp_path = parent.join(tmp_name);
    let encoded = meta.encode();
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&tmp_path)?;
    let result = (|| {
        file.write_all(&encoded)?;
        file.sync_all()?;
        fs::rename(&tmp_path, path)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&tmp_path);
    }
    result
}

/// Fsync the parent directory (POSIX crash-consistency idiom). No-op on
/// Windows (see ffi.rs for rationale).
#[cfg(unix)]
fn sync_parent(path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    File::open(parent)?.sync_all()
}

#[cfg(windows)]
fn sync_parent(_path: &Path) -> io::Result<()> {
    Ok(())
}

/// Best-effort current millis for filenames; falls back to 0 if the clock
/// is unavailable (the journal is observability, not correctness).
fn current_millis_fallback() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

// ---------------------------------------------------------------------------
// CRC-32 IEEE (table-based, no external crate)
// ---------------------------------------------------------------------------

const CRC32_TABLE: [u32; 256] = generate_crc32_table();

const fn generate_crc32_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    let mut i = 0u32;
    while i < 256 {
        let mut crc = i;
        let mut j = 0;
        while j < 8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
            j += 1;
        }
        table[i as usize] = crc;
        i += 1;
    }
    table
}

/// Compute CRC-32 IEEE over `data`.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        let index = ((crc ^ byte as u32) & 0xFF) as usize;
        crc = (crc >> 8) ^ CRC32_TABLE[index];
    }
    crc ^ 0xFFFF_FFFF
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn temp_journal_dir(name: &str) -> PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lunet-journal-test-{name}-{}-{}",
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

    #[test]
    fn record_roundtrip() {
        let event = sample_event(KIND_HOLD, 1000, 7, 2000);
        let encoded = event.encode();
        assert_eq!(encoded.len(), RECORD_SIZE);
        let (decoded, consumed) = parse_record(&encoded).expect("valid record");
        assert_eq!(consumed, RECORD_SIZE);
        assert_eq!(decoded, event);
    }

    #[test]
    fn record_bad_magic_rejected() {
        let mut buf = sample_event(KIND_HOLD, 1000, 7, 2000).encode();
        buf[0] = b'X';
        assert!(parse_record(&buf).is_none());
    }

    #[test]
    fn record_bad_crc_rejected() {
        let mut buf = sample_event(KIND_HOLD, 1000, 7, 2000).encode();
        buf[57] ^= 0xFF;
        assert!(parse_record(&buf).is_none());
    }

    #[test]
    fn record_short_buffer_rejected() {
        let buf = sample_event(KIND_HOLD, 1000, 7, 2000).encode();
        assert!(parse_record(&buf[..60]).is_none());
    }

    #[test]
    fn parse_file_stops_at_corrupt_tail() {
        let e1 = sample_event(KIND_HOLD, 100, 1, 200);
        let e2 = sample_event(KIND_RENEW, 200, 2, 300);
        let mut data = Vec::new();
        data.extend_from_slice(&e1.encode());
        data.extend_from_slice(&e2.encode());
        // Append garbage.
        data.extend_from_slice(&[0xFF; 30]);
        let events = parse_file(&data);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0], e1);
        assert_eq!(events[1], e2);
    }

    #[test]
    fn parse_file_empty_returns_zero() {
        assert!(parse_file(&[]).is_empty());
    }

    #[test]
    fn meta_roundtrip() {
        let meta = Meta {
            op_min: 10,
            op_max: 20,
            expiry_min: 100,
            expiry_max: 200,
            count: 5,
        };
        let encoded = meta.encode();
        assert_eq!(encoded.len(), META_SIZE);
        let decoded = Meta::decode(&encoded).expect("valid meta");
        assert_eq!(decoded, meta);
    }

    #[test]
    fn meta_bad_magic_rejected() {
        let mut buf = Meta {
            op_min: 0,
            op_max: 0,
            expiry_min: 0,
            expiry_max: 0,
            count: 0,
        }
        .encode();
        buf[0] = b'X';
        assert!(Meta::decode(&buf).is_none());
    }

    #[test]
    fn roll_produces_final_file_and_meta() {
        let dir = temp_journal_dir("roll");
        // roll_bytes = 3 records = 183 bytes
        let mut journal = Journal::open(&dir, RECORD_SIZE as u64 * 3).unwrap();
        let e1 = sample_event(KIND_HOLD, 1000, 1, 5000);
        let e2 = sample_event(KIND_RENEW, 2000, 2, 6000);
        let e3 = sample_event(KIND_RELEASE, 3000, 3, 7000);
        journal.append(&e1).unwrap();
        journal.append(&e2).unwrap();
        journal.append(&e3).unwrap(); // triggers roll
        // The rolled file should exist with the correct name.
        let entries: Vec<_> = fs::read_dir(&dir)
            .unwrap()
            .filter_map(|e| e.ok())
            .map(|e| e.file_name().to_string_lossy().to_string())
            .collect();
        let bin_files: Vec<&str> = entries
            .iter()
            .filter(|n| n.starts_with("ev-") && n.ends_with(".bin") && !n.contains("open"))
            .map(|s| s.as_str())
            .collect();
        assert_eq!(bin_files.len(), 1, "exactly one rolled file");
        let meta_files: Vec<&str> = entries
            .iter()
            .filter(|n| n.ends_with(".meta"))
            .map(|s| s.as_str())
            .collect();
        assert_eq!(meta_files.len(), 1, "exactly one meta file");
        // Read and verify the meta.
        let meta_path = dir.join(meta_files[0]);
        let meta_bytes = fs::read(&meta_path).unwrap();
        let meta = Meta::decode(&meta_bytes).expect("valid meta");
        assert_eq!(meta.op_min, 1000);
        assert_eq!(meta.op_max, 3000);
        assert_eq!(meta.expiry_min, 5000);
        assert_eq!(meta.expiry_max, 7000);
        assert_eq!(meta.count, 3);
        // Read back the records from the rolled file.
        let bin_path = dir.join(bin_files[0]);
        let bin_data = fs::read(&bin_path).unwrap();
        let events = parse_file(&bin_data);
        assert_eq!(events.len(), 3);
        assert_eq!(events[0], e1);
        assert_eq!(events[1], e2);
        assert_eq!(events[2], e3);
        // A fresh open file should also exist now.
        let open_files: Vec<&str> = entries
            .iter()
            .filter(|n| n.starts_with("ev-open-"))
            .map(|s| s.as_str())
            .collect();
        assert_eq!(open_files.len(), 1, "fresh open file after roll");
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resume_reopen_recomputes_window() {
        let dir = temp_journal_dir("resume");
        // Write two records, then drop the journal (simulating a crash).
        {
            let mut journal = Journal::open(&dir, RECORD_SIZE as u64 * 10).unwrap();
            journal
                .append(&sample_event(KIND_HOLD, 1000, 1, 5000))
                .unwrap();
            journal
                .append(&sample_event(KIND_RENEW, 2000, 2, 6000))
                .unwrap();
        }
        // Reopen: should find the open file and resume.
        let mut journal = Journal::open(&dir, RECORD_SIZE as u64 * 10).unwrap();
        assert_eq!(journal.written, RECORD_SIZE as u64 * 2);
        assert!(journal.window.is_some());
        let w = journal.window.as_ref().unwrap();
        assert_eq!(w.op_min, 1000);
        assert_eq!(w.op_max, 2000);
        assert_eq!(w.count, 2);
        // Append one more and verify the window extends.
        journal
            .append(&sample_event(KIND_RELEASE, 3000, 3, 7000))
            .unwrap();
        let w = journal.window.as_ref().unwrap();
        assert_eq!(w.op_max, 3000);
        assert_eq!(w.count, 3);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn empty_file_parses_zero_events() {
        let dir = temp_journal_dir("empty");
        let _journal = Journal::open(&dir, 1024).unwrap();
        // The open file exists but is empty.
        let entries: Vec<_> = fs::read_dir(&dir).unwrap().filter_map(|e| e.ok()).collect();
        assert_eq!(entries.len(), 1);
        let data = fs::read(entries[0].path()).unwrap();
        assert!(data.is_empty());
        assert!(parse_file(&data).is_empty());
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn crc32_known_vector() {
        // CRC-32 IEEE of "123456789" is 0xCBF43926.
        let data = b"123456789";
        assert_eq!(crc32_ieee(data), 0xCBF4_3926);
    }
}
