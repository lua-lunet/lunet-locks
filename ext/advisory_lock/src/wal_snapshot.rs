//! The WAL lock snapshot store: a [`StateStore`] over the WAL C ABI
//! (`wal_open` / `wal_submit` / `wal_pump` / `wal_event_fd` /
//! `wal_flush_sync` / `wal_load` / `wal_status` / `wal_close`, plus the
//! record exchange `wal_record_encode` / `wal_record_decode` over the
//! engine's own codec), backed by the double-ring rotating engine in the
//! vendored Zig store (docs/src/wal-snapshot.md is the binding design).
//!
//! The law, as this store carries it:
//!
//! - **No threads.** The store serializes every record on the calling
//!   thread — the node's loop thread, where every FFI call into the engine
//!   runs — and makes the syscall through the ABI. The handle is only ever
//!   driven from the thread that opened it; the `Send + Sync` the trait
//!   demands is satisfied under that single-thread contract (the same
//!   contract the ABI itself states), never by interior locking.
//! - **Steady state, zero fsync.** The tick path (`WalSnapshotStore::note`
//!   + [`WalSnapshotStore::submit`] + [`WalSnapshotStore::pump`]) writes
//!   page-cached generations; a crash mid-period is a dirty boot and the
//!   dirty boot never reads the artefact. On Linux completions come back
//!   through the ring's eventfd ([`WalSnapshotStore::event_fd`], polled by
//!   the loop); on darwin every write completes inline at submit.
//! - **The shutdown generation.** [`StateStore::flush`] writes the whole
//!   live set in the foreground and the engine fsyncs exactly once,
//!   blocking, before it returns. A refusal (`StorageFull` for capacity,
//!   `InvalidInput` for a diverged set) is a FAILED stop — an `Err`, never
//!   a silent skip.
//! - **LOAD is LAZY and never blocks the boot.** [`StateStore::load`]
//!   answers "nothing trustworthy" with `Ok(None)`: a cold start, and
//!   everything content-shaped the engine cannot vouch for. An `Err` is
//!   only IO trouble or this wrapper's own sizing bug — never a content
//!   refusal.
//! - **Content refusals surface at construction.** The frozen ABI refuses
//!   corrupt content (rotten checksums, twin disagreement, everything
//!   expired) at `wal_open` — it cannot reach `wal_load`. A refused
//!   construction is an `Err` from [`WalSnapshotStore::new`]; the mount
//!   site's fallback (boot without the store, rebuild from the replica
//!   stream) is where the cold-start answer lives, exactly like the
//!   marker gate's verdict.
//! - **The changed set.** The tick path carries only what changed since
//!   the last completed submit: the loop notes each committed lock
//!   transition ([`WalSnapshotStore::note`]), and
//!   [`WalSnapshotStore::submit`] serializes those locks — a live record
//!   for a lock the snapshot still holds, a **tombstone** for one it has
//!   dropped (a release; natural expiry writes nothing — the lock is
//!   simply absent from the next generation). The set clears on a clean
//!   submit; any refusal keeps it so the tick retries.
//! - **Units.** The record's `expiry` rides the engine's clock — wall
//!   nanoseconds (the write stamp's unit, the drift threshold's unit).
//!   The lock table's lease expiry is leader-stamped milliseconds;
//!   this wrapper converts on the way in and out.
//! - **One codec, one checksum.** Exactly one record codec and one
//!   checksum exist — the engine's, over the vendored Aegis-128
//!   primitive. The wrapper carries no codec and no checksum code of
//!   its own: it converts the record's fields through the ABI's
//!   `wal_record_encode` / `wal_record_decode` exports, and the engine
//!   stamps and verifies the checksum behind them.
//!
//! What rides a record is the record format's business, and the format
//! carries no more than a lease's identity: lease id, holder (16 bytes),
//! absolute expiry, and the granted window. A lease's `name`, `labels`,
//! `taken_at_ms` and `renew_count` have no room in 64 bytes and are NOT
//! durable here — a loaded lease carries them defaulted, and the loaded
//! snapshot installs only into an empty table (the lazy-load guard), so
//! live state is never overwritten by the reduced view.

use std::collections::BTreeSet;
use std::io;
use std::mem::{offset_of, size_of};
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use uuid::Uuid;

use crate::locks::{Lease, StateSnapshot};
use crate::state::StateStore;

/// The record size, bytes: exactly one cache line — 32 bytes of identity
/// (stamp, expiry, lock id, lease id), the 16-byte holder, the granted
/// window, the tag, and the checksum. The on-disk bytes ARE the struct's
/// bytes: layout-exact and memcpy-round-trippable (zero padding, every
/// field naturally aligned).
pub const RECORD_SIZE: usize = 64;

/// The engine's default capacity (records per ring). A caller passing
/// [`DEFAULT_CAPACITY_MARKER`] (0) opens with this.
pub const DEFAULT_CAPACITY: u32 = 1024;

/// The capacity argument that selects [`DEFAULT_CAPACITY`] at the engine.
pub const DEFAULT_CAPACITY_MARKER: u32 = 0;

/// Milliseconds per nanosecond: the lease expiry's conversion.
const MS_TO_NS: u64 = 1_000_000;

// The ABI's errno-style result codes (wal_c.zig).
const OK: i32 = 0;
const EAGAIN: i32 = -11;
const EIO: i32 = -5;
const EINVAL: i32 = -22;
const ENOSPC: i32 = -28;
const ENOBUFS: i32 = -105;

/// The opaque WAL handle the Zig side owns.
enum WalHandleRaw {}

/// The ABI's status record (wal_c.zig `WalStatus`), C-ABI layout mirrored.
#[repr(C)]
struct WalStatusRaw {
    completed_generations: u64,
    completed_records: u64,
    last_completed_unix_ns: u64,
    in_flight: u32,
    refusals: u64,
}

unsafe extern "C" {
    /// Open (create or open, never truncate) the fixed-size WAL file.
    /// Returns null on refusal: a bad path, a wrong-size file, a busy
    /// file, corrupt content at open, or an engine-refused config.
    fn wal_open(path: *const i8, capacity: u32) -> *mut WalHandleRaw;

    /// Close the handle. No fsync — the documented dirty-close loss
    /// window; the shutdown flush belongs to `wal_flush_sync`, called
    /// before this.
    fn wal_close(handle: *mut WalHandleRaw);

    /// Submit one serialized generation (a nonzero multiple of 72 bytes).
    fn wal_submit(handle: *mut WalHandleRaw, buf: *const u8, len: usize) -> i32;

    /// Drain completions non-blocking (Linux; a no-op success elsewhere).
    fn wal_pump(handle: *mut WalHandleRaw) -> i32;

    /// The ring's eventfd, or -1 where there is no ring.
    fn wal_event_fd(handle: *mut WalHandleRaw) -> i32;

    /// The shutdown generation: the caller's full live set, written
    /// forward and fsynced exactly once, blocking.
    fn wal_flush_sync(handle: *mut WalHandleRaw, buf: *const u8, len: usize) -> i32;

    /// The boot read: 0 = the live set copied into `buf`, 1 = none,
    /// negative = unexpected.
    fn wal_load(handle: *mut WalHandleRaw, buf: *mut u8, len: usize, out_len: *mut usize) -> i32;

    /// The counters.
    fn wal_status(handle: *mut WalHandleRaw, out: *mut WalStatusRaw) -> i32;

    /// Encode one record through the engine's own codec — the only codec
    /// and the only checksum there is: the struct in, the 64-byte
    /// on-disk form out, the checksum stamped engine-side. Refusals
    /// (a zero lock id, an unknown kind code, a short buffer) are
    /// negative codes.
    fn wal_record_encode(out: *mut u8, out_cap: usize, record: *const WalRecord) -> i32;

    /// Decode one on-disk record through the engine's own codec — the
    /// checksum verified engine-side. Refusals (a rotted checksum, a
    /// foreign format version, an unknown kind, a zero lock id, a wrong
    /// length) are negative codes.
    fn wal_record_decode(out: *mut WalRecord, buf: *const u8, len: usize) -> i32;
}

/// The completion-driven counters (the design doc's status table), as the
/// store reports them: generations and records landed since open, the last
/// completion's wall timestamp, the submission currently outstanding (0 or
/// one generation's worth), and the refusals counted since open.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStatus {
    pub completed_generations: u64,
    pub completed_records: u64,
    pub last_completed_unix_ns: u64,
    pub in_flight: u32,
    pub refusals: u64,
}

/// The record kind, encoded in the tag's low nibble (the format version
/// rides the second byte; version 1 is the only one this build reads).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecordKind {
    /// A live record: acquire/renew.
    Live,
    /// A tombstone: the release record. A natural expiry writes nothing.
    Tombstone,
}

/// The record kind's ABI code (the engine's `Kind` enum values).
const KIND_LIVE: u32 = 1;
const KIND_TOMBSTONE: u32 = 2;

/// The engine's record struct, shared with the C ABI (`WalRecordIn` in
/// wal_c.zig) and byte-exact with the engine's on-disk layout: 64 bytes,
/// zero padding, every field naturally aligned — the on-disk bytes ARE
/// this struct's bytes (memcpy-exact, one cache line). The binding layout
/// is pinned by the const asserts below — a layout drift is a compile
/// error, never a silent corruption. The `tag` the host exchanges is the
/// record kind's code (1 = live, 2 = tombstone); the engine's encode
/// stamps the format-version bits and the checksum into the struct's own
/// fields, decode verifies both and hands the kind code back.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalRecord {
    pub stamp: u64,
    pub expiry: u64,
    pub lock_id: u64,
    pub lease_id: u64,
    pub holder: [u8; 16],
    pub lease_ms: u64,
    pub tag: u32,
    pub checksum: u32,
}

/// The record's binding layout, asserted at compile time on every build
/// (size, every offset, one cache line). Public so downstream builds (the
/// copy canary) can import and reuse the pin.
pub const RECORD_LAYOUT: () = assert!(
    size_of::<WalRecord>() == RECORD_SIZE
        && size_of::<WalRecord>() == 64
        && offset_of!(WalRecord, stamp) == 0
        && offset_of!(WalRecord, expiry) == 8
        && offset_of!(WalRecord, lock_id) == 16
        && offset_of!(WalRecord, lease_id) == 24
        && offset_of!(WalRecord, holder) == 32
        && offset_of!(WalRecord, lease_ms) == 48
        && offset_of!(WalRecord, tag) == 56
        && offset_of!(WalRecord, checksum) == 60
);

/// The one unsafe in this module: the record's layout-exact byte image —
/// the memcpy round trip's Rust half (`std.mem.asBytes` on the engine
/// side). Sound because the const asserts above pin the struct to exactly
/// 64 bytes of plain little-endian fields with no padding.
pub fn record_as_bytes(record: &WalRecord) -> &[u8; RECORD_SIZE] {
    const _: () = assert!(size_of::<WalRecord>() == RECORD_SIZE);
    unsafe { &*(record as *const WalRecord as *const [u8; RECORD_SIZE]) }
}

fn kind_code(kind: RecordKind) -> u32 {
    match kind {
        RecordKind::Live => KIND_LIVE,
        RecordKind::Tombstone => KIND_TOMBSTONE,
    }
}

fn kind_from_code(tag: u32) -> Option<RecordKind> {
    match tag {
        KIND_LIVE => Some(RecordKind::Live),
        KIND_TOMBSTONE => Some(RecordKind::Tombstone),
        _ => None,
    }
}

/// Encode one record into its 64-byte on-disk form through the ABI — the
/// engine's own codec stamps the checksum. This wrapper computes no
/// checksum and carries no codec of its own: there is exactly one of
/// each, in the engine over the vendored primitive. `None` is a refusal:
/// a zero lock id or an unknown kind cannot become a record.
pub fn record_encode(record: &Record) -> Option<[u8; RECORD_SIZE]> {
    let fields = WalRecord {
        stamp: record.stamp,
        expiry: record.expiry,
        lock_id: record.lock_id,
        lease_id: record.lease_id,
        holder: record.holder,
        lease_ms: record.lease_ms,
        tag: kind_code(record.kind),
        checksum: 0,
    };
    let mut bytes = [0u8; RECORD_SIZE];
    let rc = unsafe { wal_record_encode(bytes.as_mut_ptr(), bytes.len(), &fields) };
    (rc == OK).then_some(bytes)
}

/// The record the 64 bytes carry, or `None` when the engine's codec
/// refuses them: a failed checksum, a foreign format version, an unknown
/// kind, or a zero lock id. Every refusal is a content refusal.
pub fn record_decode(bytes: &[u8; RECORD_SIZE]) -> Option<Record> {
    let mut fields = WalRecord {
        stamp: 0,
        expiry: 0,
        lock_id: 0,
        lease_id: 0,
        holder: [0; 16],
        lease_ms: 0,
        tag: 0,
        checksum: 0,
    };
    let rc = unsafe { wal_record_decode(&mut fields, bytes.as_ptr(), bytes.len()) };
    if rc != OK {
        return None;
    }
    Some(Record {
        stamp: fields.stamp,
        expiry: fields.expiry,
        lock_id: fields.lock_id,
        lease_id: fields.lease_id,
        kind: kind_from_code(fields.tag)?,
        holder: fields.holder,
        lease_ms: fields.lease_ms,
    })
}

/// One serialized record appended, or the loud error: the engine refuses
/// fields that cannot become a record (a zero lock id, an unknown kind —
/// both unreachable through this wrapper's own bookkeeping, so a refusal
/// here is a bug named loudly).
fn encode_into(records: &mut Vec<u8>, record: &Record) -> io::Result<()> {
    let bytes = record_encode(record)
        .ok_or_else(|| wal_error(EINVAL, "the engine refused a record's fields"))?;
    records.extend_from_slice(&bytes);
    Ok(())
}

/// One lock-table change, the record format's whole content: the identity
/// fields the 72 bytes carry and nothing more (a lease's `name`/`labels`
/// and counters have no room here — see the module doc).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// The ns write stamp: the epoch selector, wall clock of the write.
    /// The submit path writes the host's stamp through; the shutdown
    /// flush re-stamps with its own write time.
    pub stamp: u64,
    /// Absolute, in the engine's clock (nanoseconds).
    pub expiry: u64,
    pub lock_id: u64,
    pub lease_id: u64,
    pub kind: RecordKind,
    /// The holder's uuid bytes.
    pub holder: [u8; 16],
    /// The granted window in milliseconds (`lease_ms`).
    pub lease_ms: u64,
}

impl Record {
    /// A live record: acquire/renew.
    pub fn live(
        stamp: u64,
        expiry: u64,
        lock_id: u64,
        lease_id: u64,
        holder: [u8; 16],
        lease_ms: u64,
    ) -> Self {
        Self {
            stamp,
            expiry,
            lock_id,
            lease_id,
            kind: RecordKind::Live,
            holder,
            lease_ms,
        }
    }

    /// A tombstone: the release record. `lease_id` rides when the caller
    /// knows it; a tick that learned of the release only through the
    /// table's absence carries zero.
    pub fn tombstone(stamp: u64, lock_id: u64, lease_id: u64) -> Self {
        Self {
            stamp,
            expiry: 0,
            lock_id,
            lease_id,
            kind: RecordKind::Tombstone,
            holder: [0; 16],
            lease_ms: 0,
        }
    }
}

/// The lease expiry's units: the table's leader-stamped milliseconds in,
/// the engine's nanoseconds out.
fn expiry_to_ns(expiry_ms: u64) -> u64 {
    expiry_ms.saturating_mul(MS_TO_NS)
}

fn expiry_to_ms(expiry_ns: u64) -> u64 {
    expiry_ns / MS_TO_NS
}

fn unix_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos() as u64)
}

/// The store: one open WAL handle plus the changed set the tick path
/// carries. The handle is opened at construction and closed at drop; the
/// single-thread contract is the ABI's (see the module doc).
pub struct WalSnapshotStore {
    handle: *mut WalHandleRaw,
    /// The file this store reads and writes (diagnostics; the engine owns
    /// the bytes).
    path: PathBuf,
    /// The effective capacity (records per ring) the engine opened with —
    /// the load buffer's size.
    capacity: u32,
    /// Lock ids changed since the last completed submit, in the loop's
    /// own bookkeeping. Cleared on a clean submit; a refusal keeps it so
    /// the tick retries.
    changed: BTreeSet<u64>,
}

// SAFETY: the raw handle is only ever driven from the thread that opened
// it — the node's loop thread, per the ABI's own single-thread contract
// and the IO ruling above. The crate never moves the store across threads
// except where the trait demands it (`Send + Sync` for the boxed store),
// and no Rust state here is touched except through `&mut self` on that
// one thread. The mount site owns the contract; the store cannot enforce
// what the C ABI itself does not.
unsafe impl Send for WalSnapshotStore {}
unsafe impl Sync for WalSnapshotStore {}

impl WalSnapshotStore {
    /// Open the WAL at `path` with the default capacity
    /// ([`DEFAULT_CAPACITY`]).
    ///
    /// A refusal is an `Err` — a bad path, a wrong-size file, a busy
    /// file, or corrupt content at open (rotten checksums, twin
    /// disagreement, everything expired: the ABI refuses content at
    /// open). The mount site's fallback for a refused construction is
    /// the cold-start answer (boot without the store, rebuild from the
    /// replica stream); a load is never handed a store that failed to
    /// open.
    pub fn new(path: &Path) -> io::Result<Self> {
        Self::new_with_capacity(path, DEFAULT_CAPACITY_MARKER)
    }

    /// [`WalSnapshotStore::new`] with an explicit capacity (records per
    /// ring, the operator's sizing knob). 0 selects the default; a
    /// capacity beyond the ring geometry is refused like any other open
    /// refusal.
    pub fn new_with_capacity(path: &Path, capacity: u32) -> io::Result<Self> {
        let cpath = match std::ffi::CString::new(path.as_os_str().as_bytes()) {
            Ok(cpath) => cpath,
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "wal snapshot path contains a NUL byte",
                ));
            }
        };
        let handle = unsafe { wal_open(cpath.as_ptr(), capacity) };
        if handle.is_null() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("wal snapshot refused to open at {}", path.display()),
            ));
        }
        Ok(Self {
            handle,
            path: path.to_path_buf(),
            // The engine's effective capacity: 0 selects the default.
            capacity: if capacity == DEFAULT_CAPACITY_MARKER {
                DEFAULT_CAPACITY
            } else {
                capacity
            },
            changed: BTreeSet::new(),
        })
    }

    /// The file this store reads and writes: the path it was opened at
    /// (the mount site's state path with a `.wal` suffix).
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Record a changed lock: the loop calls this on every committed lock
    /// transition (acquire, renew, release, break). The next
    /// [`WalSnapshotStore::submit`] carries the lock — a live record if
    /// the snapshot still holds it, a tombstone if it does not.
    pub fn note(&mut self, lock_id: u64) {
        if lock_id == 0 {
            crate::maybe_invariant!("wal snapshot: lock id 0 is not a lock");
            return;
        }
        self.changed.insert(lock_id);
    }

    /// The steady tick's write: every noted lock, serialized as one
    /// generation and submitted forward through the active pair.
    /// Page-cached, zero fsync. Returns the record count.
    ///
    /// A clean submit clears the changed set. Any refusal
    /// ([`io::ErrorKind::WouldBlock`] when a previous batch is still
    /// outstanding — pump first, Linux; [`io::ErrorKind::StorageFull`]
    /// when the ring cannot take the record — counted, serving state
    /// untouched) keeps the set so the next tick retries: the records
    /// that landed are last-wins-identical, and the refused ones are not
    /// lost.
    pub fn submit(&mut self, snap: &StateSnapshot) -> io::Result<usize> {
        if self.changed.is_empty() {
            return Ok(0);
        }
        let stamp = unix_ns();
        let mut records = Vec::with_capacity(self.changed.len() * RECORD_SIZE);
        for &lock_id in &self.changed {
            match snap.locks.get(&lock_id) {
                Some(lease) => encode_into(
                    &mut records,
                    &Record::live(
                        stamp,
                        expiry_to_ns(lease.expiry),
                        lock_id,
                        lease.lease_id,
                        *lease.holder.as_bytes(),
                        lease.lease_ms,
                    ),
                )?,
                None => encode_into(&mut records, &Record::tombstone(stamp, lock_id, 0))?,
            }
        }
        let count = records.len() / RECORD_SIZE;
        let rc = unsafe { wal_submit(self.handle, records.as_ptr(), records.len()) };
        match rc {
            OK => {
                self.changed.clear();
                Ok(count)
            }
            EAGAIN => Err(wal_error(rc, "a previous batch is still outstanding")),
            ENOSPC => Err(wal_error(rc, "the ring cannot take the generation")),
            EINVAL => Err(wal_error(rc, "the engine refused a record")),
            EIO => Err(wal_error(rc, "backend IO failure")),
            other => Err(wal_error(other, "unexpected wal_submit result")),
        }
    }

    /// Drain completions non-blocking. A no-op success on the
    /// inline-completion platforms (darwin); on Linux it reaps the ring's
    /// completions and moves the counters. Call this on the eventfd wake
    /// and before the next submit.
    pub fn pump(&mut self) -> io::Result<()> {
        let rc = unsafe { wal_pump(self.handle) };
        match rc {
            OK => Ok(()),
            EIO => Err(wal_error(rc, "backend IO failure")),
            other => Err(wal_error(other, "unexpected wal_pump result")),
        }
    }

    /// The ring's eventfd to poll (Linux), or `None` where there is no
    /// ring (darwin completes inline).
    pub fn event_fd(&self) -> Option<i32> {
        let fd = unsafe { wal_event_fd(self.handle) };
        (fd >= 0).then_some(fd)
    }

    /// The counters.
    pub fn status(&self) -> io::Result<WalStatus> {
        let mut raw = WalStatusRaw {
            completed_generations: 0,
            completed_records: 0,
            last_completed_unix_ns: 0,
            in_flight: 0,
            refusals: 0,
        };
        let rc = unsafe { wal_status(self.handle, &mut raw) };
        match rc {
            OK => Ok(WalStatus {
                completed_generations: raw.completed_generations,
                completed_records: raw.completed_records,
                last_completed_unix_ns: raw.last_completed_unix_ns,
                in_flight: raw.in_flight,
                refusals: raw.refusals,
            }),
            other => Err(wal_error(other, "unexpected wal_status result")),
        }
    }

    /// The snapshot the artefact carries, as the load path hands it: the
    /// engine's live set decoded. A tombstone in the load stream, or any
    /// record the engine's codec refuses (a failed checksum, a foreign
    /// format version, an unknown kind, a zero lock id) is a content
    /// refusal — `Ok(None)`, the cold-start answer.
    fn decode_records(records: &[u8]) -> Option<StateSnapshot> {
        let mut locks = std::collections::BTreeMap::new();
        for chunk in records.chunks_exact(RECORD_SIZE) {
            let bytes: &[u8; RECORD_SIZE] = chunk.try_into().ok()?;
            let record = record_decode(bytes)?;
            match record.kind {
                RecordKind::Tombstone => return None,
                RecordKind::Live => {}
            }
            locks.insert(
                record.lock_id,
                Lease {
                    lease_id: record.lease_id,
                    holder: Uuid::from_bytes(record.holder),
                    expiry: expiry_to_ms(record.expiry),
                    lease_ms: record.lease_ms,
                    // The record format carries no more than the lease's
                    // identity: the rest is defaulted, never guessed.
                    name: None,
                    labels: None,
                    taken_at_ms: 0,
                    renew_count: 0,
                },
            );
        }
        Some(StateSnapshot { locks })
    }
}

impl StateStore for WalSnapshotStore {
    /// The shutdown generation: the whole live set, serialized here on
    /// the calling thread, written forward by the engine and fsynced
    /// exactly once, blocking. A refusal is a FAILED stop — an `Err`,
    /// never a silent skip.
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()> {
        let stamp = unix_ns();
        let mut records = Vec::with_capacity(snap.locks.len() * RECORD_SIZE);
        for (&lock_id, lease) in &snap.locks {
            if lock_id == 0 {
                crate::maybe_invariant!("wal snapshot: lock id 0 is not a lock");
                continue;
            }
            encode_into(
                &mut records,
                &Record::live(
                    stamp,
                    expiry_to_ns(lease.expiry),
                    lock_id,
                    lease.lease_id,
                    *lease.holder.as_bytes(),
                    lease.lease_ms,
                ),
            )?;
        }
        let rc = unsafe { wal_flush_sync(self.handle, records.as_ptr(), records.len()) };
        match rc {
            OK => Ok(()),
            ENOSPC => Err(wal_error(
                rc,
                "the live set overruns the pair's remaining room",
            )),
            EINVAL => Err(wal_error(
                rc,
                "the live set diverges from the engine's mirror",
            )),
            EIO => Err(wal_error(rc, "backend IO failure")),
            other => Err(wal_error(other, "unexpected wal_flush_sync result")),
        }
    }

    /// The lazy load: the clean-stop artefact's live set, or
    /// `Ok(None)` — the cold-start answer for "there is nothing
    /// trustworthy here". An `Err` is only IO trouble or this wrapper's
    /// own sizing bug; a content refusal cannot block a boot.
    fn load(&self) -> io::Result<Option<StateSnapshot>> {
        let mut buf = vec![0u8; self.capacity as usize * RECORD_SIZE];
        let mut out_len: usize = 0;
        let rc = unsafe { wal_load(self.handle, buf.as_mut_ptr(), buf.len(), &mut out_len) };
        match rc {
            OK => {
                if out_len > buf.len() {
                    return Err(wal_error(ENOBUFS, "the engine copied past the load buffer"));
                }
                Self::decode_records(&buf[..out_len]).map_or(
                    // The engine vouched for the bytes at open; a decode
                    // refusal now is content we cannot trust. Ok(None),
                    // never a boot-blocking error.
                    Ok(None),
                    |snapshot| Ok(Some(snapshot)),
                )
            }
            // 1 = none: the cold start.
            1 => Ok(None),
            ENOBUFS => Err(wal_error(
                rc,
                "the load buffer is smaller than the live set (a sizing bug)",
            )),
            EIO => Err(wal_error(rc, "backend IO failure")),
            other => Err(wal_error(other, "unexpected wal_load result")),
        }
    }
}

impl Drop for WalSnapshotStore {
    fn drop(&mut self) {
        if !self.handle.is_null() {
            unsafe { wal_close(self.handle) };
            self.handle = std::ptr::null_mut();
        }
    }
}

/// The error an ABI result code maps to. The kind carries the discipline
/// (capacity refusals are `StorageFull`, an outstanding batch is
/// `WouldBlock`); the message names the path.
fn wal_error(code: i32, what: &str) -> io::Error {
    let kind = match code {
        EINVAL => io::ErrorKind::InvalidInput,
        EAGAIN => io::ErrorKind::WouldBlock,
        ENOSPC => io::ErrorKind::StorageFull,
        ENOBUFS => io::ErrorKind::InvalidData,
        EIO => io::ErrorKind::Other,
        _ => io::ErrorKind::Other,
    };
    io::Error::new(kind, format!("wal snapshot: {what} (code {code})"))
}
