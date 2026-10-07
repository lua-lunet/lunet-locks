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
//! - `kind`: 1 = hold, 2 = renew, 3 = release, 4 = break.
//! - `ts`: unix ms at apply.
//! - Release and break records carry the removed lease's holder/lease_id/expiry.
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

use crate::disk::{Disk, DiskDirEntry, DiskFile, std_disk};
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

// The unit tests below read and stat the series they just wrote with
// `std::fs` directly: they are the tests' own observations of the files
// on disk, not the journal's live path, and this row does not edit tests.
#[cfg(test)]
use std::fs;

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
pub const KIND_BREAK: u8 = 4;

/// The kind's name, for every surface a human reads (a trace line, a
/// diagnostic dump, the flight recorder's replay): `kind=hold`, never
/// `kind=1`. The discriminant and every comparison stay numeric; the
/// name is stated next to the numbering it names so the two cannot drift
/// apart, and a kind outside the table renders `unknown`, never a bare
/// integer.
pub const fn kind_name(kind: u8) -> &'static str {
    match kind {
        KIND_HOLD => "hold",
        KIND_RENEW => "renew",
        KIND_RELEASE => "release",
        KIND_BREAK => "break",
        _ => "unknown",
    }
}

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
    if !matches!(kind, KIND_HOLD | KIND_RENEW | KIND_RELEASE | KIND_BREAK) {
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
/// Shared with the AOF writer, which finalizes files with the same
/// naming and metafile conventions.
#[derive(Debug, Clone)]
pub(crate) struct Window {
    pub(crate) op_min: u64,
    pub(crate) op_max: u64,
    pub(crate) expiry_min: u64,
    pub(crate) expiry_max: u64,
    pub(crate) count: u32,
}

impl Window {
    pub(crate) fn new(event: &JournalEvent) -> Self {
        Self {
            op_min: event.ts,
            op_max: event.ts,
            expiry_min: event.expiry,
            expiry_max: event.expiry,
            count: 1,
        }
    }

    pub(crate) fn update(&mut self, event: &JournalEvent) {
        self.op_min = self.op_min.min(event.ts);
        self.op_max = self.op_max.max(event.ts);
        self.expiry_min = self.expiry_min.min(event.expiry);
        self.expiry_max = self.expiry_max.max(event.expiry);
        self.count += 1;
    }

    pub(crate) fn meta(&self) -> Meta {
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
    file: DiskFile,
    /// The seam every filesystem operation in this journal crosses.
    disk: Arc<dyn Disk>,
    open_path: PathBuf,
    started_ms: u64,
    written: u64,
    window: Option<Window>,
}

impl Journal {
    /// Open (or create) a journal rooted at `dir`, over the default disk.
    /// If an `ev-open-*.bin` file already exists, resume appending to it by
    /// scanning its records (bounded by `roll_bytes`) to recompute the
    /// window and seeking to end.
    pub fn open(dir: &Path, roll_bytes: u64) -> io::Result<Self> {
        Self::open_on(std_disk(), dir, roll_bytes)
    }

    /// [`Journal::open`] against a named disk — the same journal, over
    /// whatever [`Disk`] the caller holds. The journal keeps the disk for
    /// its own life: every roll, every metafile, every directory sync
    /// crosses the same seam.
    pub fn open_on(disk: Arc<dyn Disk>, dir: &Path, roll_bytes: u64) -> io::Result<Self> {
        disk.create_dir_all(dir)?;
        // Look for an existing open file to resume.
        if let Some(entry) = find_open_file_on(&*disk, dir)? {
            let path = entry.path();
            let started_ms = parse_open_timestamp(&path).unwrap_or(0);
            let mut file = disk.open_read_append(&path)?;
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
                disk,
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
            let (file, open_path) = create_open_file(&*disk, dir, started_ms)?;
            Ok(Self {
                dir: dir.to_path_buf(),
                roll_bytes,
                file,
                disk,
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

    /// Force durability: fsync the current open file. The ordinary append
    /// path only flushes to the OS (page cache); the graceful-stop path's
    /// durable-state write needs the fsync before the `flushed` marker
    /// may land.
    pub fn flush(&self) -> io::Result<()> {
        self.file.sync_all()
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
        let placeholder = self
            .disk
            .open_read(Path::new("/dev/null"))
            .unwrap_or_else(|_| {
                // Fallback for platforms without /dev/null: open the file
                // itself read-only as a placeholder.
                self.disk
                    .open_read(&self.open_path)
                    .expect("placeholder file")
            });
        drop(std::mem::replace(&mut self.file, placeholder));
        // Rename to final name.
        let final_name = format!(
            "ev-{}-{}-{}-{}.bin",
            meta.op_min, meta.op_max, meta.expiry_min, meta.expiry_max
        );
        let final_path = self.dir.join(&final_name);
        self.disk.rename(&self.open_path, &final_path)?;
        // Write meta atomically.
        let meta_path = final_path.with_extension("meta");
        write_meta_atomic(&*self.disk, &meta_path, &meta)?;
        // Open a fresh open file.
        self.started_ms = current_millis_fallback();
        let (file, open_path) = create_open_file(&*self.disk, &self.dir, self.started_ms)?;
        self.file = file;
        self.open_path = open_path;
        self.written = 0;
        self.window = None;
        Ok(())
    }
}

/// The unit tests' view of the open-file scan, over the default disk.
#[cfg(test)]
pub(crate) fn find_open_file(dir: &Path) -> io::Result<Option<DiskDirEntry>> {
    find_open_file_on(&*std_disk(), dir)
}

/// Find an existing `ev-open-*.bin` file in the directory. Shared with the
/// AOF writer, which resumes its series with the same conventions.
pub(crate) fn find_open_file_on(disk: &dyn Disk, dir: &Path) -> io::Result<Option<DiskDirEntry>> {
    for entry in disk.read_dir(dir)? {
        let path = entry.path();
        let name_str = path.file_name().unwrap_or_default().to_string_lossy();
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
fn create_open_file(
    disk: &dyn Disk,
    dir: &Path,
    started_ms: u64,
) -> io::Result<(DiskFile, PathBuf)> {
    let name = format!("ev-open-{started_ms}.bin");
    let path = dir.join(&name);
    let file = disk.create_new_append(&path)?;
    Ok((file, path))
}

/// Write a metafile atomically: tmp + fsync + rename + dir sync. Shared
/// with the AOF writer, which finalizes files with the same conventions.
pub(crate) fn write_meta_atomic(disk: &dyn Disk, path: &Path, meta: &Meta) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let unique = current_millis_fallback();
    // to_string_lossy, NOT Debug format: `{:?}` on an OsStr wraps the name
    // in double quotes, and Windows rejects `"` in filenames (os error 123).
    let tmp_name = format!(".{}.tmp-{unique}", base.to_string_lossy());
    let tmp_path = parent.join(tmp_name);
    let encoded = meta.encode();
    let mut file = disk.create_new_write(&tmp_path)?;
    let result = (|| {
        file.write_all(&encoded)?;
        file.sync_all()?;
        disk.rename(&tmp_path, path)?;
        sync_parent(disk, path)
    })();
    if result.is_err() {
        let _ = disk.remove_file(&tmp_path);
    }
    result
}

/// Fsync the parent directory (POSIX crash-consistency idiom). No-op on
/// Windows (see ffi.rs for rationale).
#[cfg(unix)]
fn sync_parent(disk: &dyn Disk, path: &Path) -> io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    disk.sync_path(parent)
}

#[cfg(windows)]
fn sync_parent(_disk: &dyn Disk, _path: &Path) -> io::Result<()> {
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
///
/// The crate's one checksum, shared with the state seam's file framing
/// (`crate::state`) so a rotted byte is detected the same way wherever
/// it rots. `pub(crate)`, not public: the seam is the only other caller
/// and the table behind it is this module's business.
pub(crate) fn crc32_ieee(data: &[u8]) -> u32 {
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
    use std::sync::atomic::{AtomicU64, Ordering};

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
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/journal-unit");
        fs::create_dir_all(&root).expect("the scratch root creates");
        let dir = root.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
        ));
        fs::create_dir_all(&dir).expect("the case directory creates");
        dir
    }

    /// Every kind encodes to its fixed 61 bytes and parses back with
    /// every field surviving and `consumed == RECORD_SIZE`.
    #[test]
    fn record_roundtrip() {
        for kind in [KIND_HOLD, KIND_RENEW, KIND_RELEASE] {
            let one = event(kind, 1_000 + kind as u64, 7, 5_000);
            let encoded = one.encode();
            assert_eq!(encoded.len(), RECORD_SIZE, "the fixed record size");
            assert_eq!(
                parse_record(&encoded),
                Some((one, RECORD_SIZE)),
                "the record parses back byte-exact"
            );
        }
    }

    /// The break record (and its release sibling) carries the removed
    /// lease's holder/lease_id/expiry, and those fields survive the
    /// roundtrip.
    #[test]
    fn break_record_roundtrip() {
        let one = event(KIND_BREAK, 2_000, 12, 7_000);
        assert_eq!(
            parse_record(&one.encode()),
            Some((one, RECORD_SIZE)),
            "the removed lease's fields ride the break record"
        );
    }

    /// A file buffer mixing all four kinds parses back to all four
    /// events, in order.
    #[test]
    fn parse_file_covers_break_records() {
        let events = vec![
            event(KIND_HOLD, 1_000, 11, 5_000),
            event(KIND_RENEW, 1_100, 11, 6_000),
            event(KIND_RELEASE, 1_200, 11, 7_000),
            event(KIND_BREAK, 1_300, 12, 8_000),
        ];
        let mut bytes = Vec::new();
        for one in &events {
            bytes.extend_from_slice(&one.encode());
        }
        assert_eq!(parse_file(&bytes), events);
    }

    /// A kind outside the table fails the parse: the record is never
    /// decoded into an unnamed event.
    #[test]
    fn unknown_kind_rejected() {
        let mut encoded = event(KIND_HOLD, 1_000, 11, 5_000).encode();
        encoded[8] = 9;
        // Re-stamp the crc over the payload so ONLY the kind is invalid.
        let crc = crc32_ieee(&encoded[8..57]);
        encoded[57..61].copy_from_slice(&crc.to_be_bytes());
        assert_eq!(parse_record(&encoded), None, "the unknown kind refuses");
    }

    /// Bad magic fails the parse before any other field is read.
    #[test]
    fn record_bad_magic_rejected() {
        let mut encoded = event(KIND_HOLD, 1_000, 11, 5_000).encode();
        encoded[0..4].copy_from_slice(b"XXXX");
        assert_eq!(parse_record(&encoded), None);
    }

    /// A single flipped payload byte rots the crc: the record refuses.
    #[test]
    fn record_bad_crc_rejected() {
        let mut encoded = event(KIND_HOLD, 1_000, 11, 5_000).encode();
        encoded[10] ^= 0x01;
        assert_eq!(parse_record(&encoded), None, "the rotted crc refuses");
    }

    /// A buffer shorter than one record is no record at any length.
    #[test]
    fn record_short_buffer_rejected() {
        let encoded = event(KIND_HOLD, 1_000, 11, 5_000).encode();
        assert_eq!(parse_record(&encoded[..RECORD_SIZE - 1]), None);
        assert_eq!(parse_record(&[]), None);
    }

    /// The reader stops cleanly at the first invalid record: a rotted
    /// record mid-file ends the parse even with valid records after it,
    /// and garbage bytes past the last record are a corrupt tail.
    #[test]
    fn parse_file_stops_at_corrupt_tail() {
        let first = event(KIND_HOLD, 1_000, 11, 5_000);
        let second = event(KIND_RENEW, 1_100, 11, 6_000);
        let third = event(KIND_RELEASE, 1_200, 11, 7_000);
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&first.encode());
        bytes.extend_from_slice(&second.encode());
        let mut rotted = third.encode();
        rotted[10] ^= 0x01;
        bytes.extend_from_slice(&rotted);
        bytes.extend_from_slice(&third.encode());
        assert_eq!(
            parse_file(&bytes),
            vec![first.clone(), second.clone()],
            "the parse stops at the first invalid record"
        );
        // The corrupt-tail shape: valid records, then garbage.
        let mut tail = Vec::new();
        tail.extend_from_slice(&first.encode());
        tail.extend_from_slice(&second.encode());
        tail.extend_from_slice(&[0u8; 10]);
        assert_eq!(parse_file(&tail), vec![first, second]);
    }

    /// An empty buffer parses to zero events.
    #[test]
    fn parse_file_empty_returns_zero() {
        assert!(parse_file(&[]).is_empty());
    }

    /// The metafile encodes to its fixed 40 bytes and decodes back with
    /// every field surviving.
    #[test]
    fn meta_roundtrip() {
        let meta = Meta {
            op_min: 1_000,
            op_max: 2_000,
            expiry_min: 5_000,
            expiry_max: 9_000,
            count: 7,
        };
        let encoded = meta.encode();
        assert_eq!(encoded.len(), META_SIZE, "the fixed metafile size");
        assert_eq!(Meta::decode(&encoded), Some(meta));
    }

    /// Bad magic and a short buffer both refuse the metafile decode.
    #[test]
    fn meta_bad_magic_rejected() {
        let mut encoded = Meta {
            op_min: 1_000,
            op_max: 2_000,
            expiry_min: 5_000,
            expiry_max: 9_000,
            count: 7,
        }
        .encode();
        encoded[0..4].copy_from_slice(b"XXXX");
        assert_eq!(Meta::decode(&encoded), None, "the bad magic refuses");
        assert_eq!(
            Meta::decode(&encoded[..META_SIZE - 1]),
            None,
            "the short buffer refuses"
        );
    }

    /// The append that reaches the roll threshold finalizes the open
    /// file under the window's name, writes the metafile atomically,
    /// and opens a fresh open file.
    #[test]
    fn roll_produces_final_file_and_meta() {
        let dir = scratch("roll");
        let roll_bytes = 4 * RECORD_SIZE as u64;
        let mut journal = Journal::open(&dir, roll_bytes).expect("the journal opens");
        let events = vec![
            event(KIND_HOLD, 1_000, 11, 5_000),
            event(KIND_RENEW, 1_100, 11, 6_000),
            event(KIND_RELEASE, 1_200, 11, 7_000),
            event(KIND_BREAK, 1_300, 12, 8_000),
        ];
        for one in &events {
            journal.append(one).expect("the record appends");
        }
        // The fourth append reached roll_bytes: the file rolled.
        let finalized = dir.join("ev-1000-1300-5000-8000.bin");
        assert!(
            finalized.exists(),
            "the rolled file carries the window's name"
        );
        let bytes = fs::read(&finalized).expect("the finalized file reads");
        assert_eq!(parse_file(&bytes), events, "the records ride byte-exact");
        let meta =
            Meta::decode(&fs::read(finalized.with_extension("meta")).expect("the metafile reads"))
                .expect("the metafile decodes");
        assert_eq!(
            meta,
            Meta {
                op_min: 1_000,
                op_max: 1_300,
                expiry_min: 5_000,
                expiry_max: 8_000,
                count: 4,
            }
        );
        // A fresh open file follows the roll, and the next record lands
        // in it.
        assert!(
            find_open_file(&dir).expect("the directory reads").is_some(),
            "a fresh open file follows the roll"
        );
        let last = event(KIND_HOLD, 1_400, 13, 9_000);
        journal.append(&last).expect("the record appends");
        drop(journal);
        let bytes = fs::read(
            find_open_file(&dir)
                .expect("the directory reads")
                .expect("the open file")
                .path(),
        )
        .expect("the open file reads");
        assert_eq!(parse_file(&bytes), vec![last]);
    }

    /// The resume scan recomputes the window over the reopened file's
    /// records: the next roll's metafile names the FULL window — the
    /// pre-restart records included.
    #[test]
    fn resume_reopen_recomputes_window() {
        let dir = scratch("resume");
        let roll_bytes = 4 * RECORD_SIZE as u64;
        {
            let mut journal = Journal::open(&dir, roll_bytes).expect("the journal opens");
            journal
                .append(&event(KIND_HOLD, 1_000, 11, 5_000))
                .expect("the first record appends");
            journal
                .append(&event(KIND_RENEW, 1_100, 11, 6_000))
                .expect("the second record appends");
            // Dropped without a stop: the resume scan is the only rebuild.
        }
        let mut resumed = Journal::open(&dir, roll_bytes).expect("the journal reopens");
        resumed
            .append(&event(KIND_RELEASE, 1_200, 11, 7_000))
            .expect("the third record appends");
        resumed
            .append(&event(KIND_BREAK, 1_300, 12, 8_000))
            .expect("the fourth record appends");
        // Two resumed records + two new = the roll threshold: the append
        // rolled, and the metafile names the full window.
        let finalized = dir.join("ev-1000-1300-5000-8000.bin");
        assert!(
            finalized.exists(),
            "the resumed records roll with the new ones"
        );
        let meta =
            Meta::decode(&fs::read(finalized.with_extension("meta")).expect("the metafile reads"))
                .expect("the metafile decodes");
        assert_eq!(
            meta,
            Meta {
                op_min: 1_000,
                op_max: 1_300,
                expiry_min: 5_000,
                expiry_max: 8_000,
                count: 4,
            },
            "the resumed records are in the window"
        );
    }

    /// A fresh journal's open file is empty and parses to zero events.
    #[test]
    fn empty_file_parses_zero_events() {
        let dir = scratch("empty");
        let journal = Journal::open(&dir, 64 * 1024).expect("the journal opens");
        drop(journal);
        let open_path = find_open_file(&dir)
            .expect("the directory reads")
            .expect("the open file exists")
            .path();
        assert_eq!(fs::metadata(&open_path).expect("the length reads").len(), 0);
        let bytes = fs::read(open_path).expect("the open file reads");
        assert!(parse_file(&bytes).is_empty());
    }

    /// The table-free CRC-32 IEEE matches the standard check value, and
    /// the empty input's crc is zero.
    #[test]
    fn crc32_known_vector() {
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32_ieee(&[]), 0);
    }
}
