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

use std::collections::{BTreeMap, BTreeSet};
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

/// The ABI's status record (wal_c.zig `WalStreamStatus`), C-ABI layout
/// mirrored: one stream's completion-driven counters.
#[repr(C)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStreamStatusRaw {
    completed_generations: u64,
    completed_records: u64,
    refusals: u64,
    in_flight: u32,
}

/// The ABI's status record (wal_c.zig `WalStatus`), C-ABI layout
/// mirrored: one row per stream — the ephemeral stream's counters and
/// the durable stream's — plus the last completion's wall timestamp.
#[repr(C)]
struct WalStatusRaw {
    ephemeral: WalStreamStatusRaw,
    durable: WalStreamStatusRaw,
    last_completed_unix_ns: u64,
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

    /// Submit one serialized ephemeral generation (a nonzero multiple of
    /// 64 bytes).
    fn wal_submit(handle: *mut WalHandleRaw, buf: *const u8, len: usize) -> i32;

    /// Submit one serialized durable generation (a nonzero multiple of
    /// 768-byte fat frames, validated engine-side).
    fn wal_durable_submit(handle: *mut WalHandleRaw, frames: *const u8, len: usize) -> i32;

    /// Drain completions non-blocking (Linux; a no-op success elsewhere).
    fn wal_pump(handle: *mut WalHandleRaw) -> i32;

    /// The ring's eventfd, or -1 where there is no ring.
    fn wal_event_fd(handle: *mut WalHandleRaw) -> i32;

    /// The shutdown generation: the caller's full live sets — the
    /// ephemeral set and the durable set — written forward and fsynced
    /// exactly once, blocking, reconciled per stream against the
    /// engine's mirrors.
    fn wal_flush_sync(
        handle: *mut WalHandleRaw,
        buf: *const u8,
        len: usize,
        dur_buf: *const u8,
        dur_len: usize,
    ) -> i32;

    /// The boot read, ephemeral stream: 0 = the live set copied into
    /// `buf`, 1 = none, negative = unexpected.
    fn wal_load(handle: *mut WalHandleRaw, buf: *mut u8, len: usize, out_len: *mut usize) -> i32;

    /// The boot read, durable stream: the same classes over the durable
    /// live set's fat frames.
    fn wal_load_durable(
        handle: *mut WalHandleRaw,
        buf: *mut u8,
        len: usize,
        out_len: *mut usize,
    ) -> i32;

    /// The counters, per stream.
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

    /// Encode one durable fat frame through the engine's own codec: the
    /// header fields (the durable kind's code in `tag`) plus the caller's
    /// name and tag bytes and the counters, the 768-byte on-disk form
    /// out — the sha and the frame checksum stamped engine-side.
    fn wal_frame_encode(
        out: *mut u8,
        out_cap: usize,
        record: *const WalRecord,
        name: *const u8,
        name_len: usize,
        taken_at_ms: u64,
        renew_count: u32,
        tag_bytes: *const u8,
        tag_len: usize,
    ) -> i32;

    /// Decode one on-disk fat frame through the engine's own codec — both
    /// integrity layers verified engine-side, the mirror struct out, the
    /// durable kind's code handed back in `tag`.
    fn wal_frame_decode(out: *mut FatFrame, bytes: *const u8, len: usize) -> i32;
}

/// The completion-driven counters (the design doc's status table, per
/// stream), as the store reports them: each stream's generations and
/// records landed since open, its refusals, and its submission
/// currently outstanding (0 or one generation's worth).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStreamStatus {
    pub completed_generations: u64,
    pub completed_records: u64,
    pub refusals: u64,
    pub in_flight: u32,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct WalStatus {
    /// The ephemeral stream's counters.
    pub ephemeral: WalStreamStatus,
    /// The durable stream's counters.
    pub durable: WalStreamStatus,
    /// The last completion's wall timestamp, either stream.
    pub last_completed_unix_ns: u64,
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

// ---------------------------------------------------------------------------
// The durable stream: the fat frame's binding mirror and its codec
// access, over the engine's own codec behind the ABI.
// ---------------------------------------------------------------------------

/// The fat frame size, bytes: exactly 12 cache lines — the 64-byte
/// header record, the name area (CL1-5), the counters and tag bytes
/// (CL6-7), and the consistency area (CL8-11).
pub const FAT_FRAME_SIZE: usize = 768;

/// The name area's byte budget (the u16 length plus the name bytes).
pub const FAT_NAME_CAPACITY: usize = 518;

/// The tag bytes' budget (a u16 length plus the tag bytes).
pub const FAT_TAG_BYTES_CAPACITY: usize = 114;

// The frame's fixed offsets, the engine's own (data-area offsets are
// frame offsets; the mirror's `data` starts after the 64-byte header).
// Public: the binding's offsets, the mirror tests' and the canary's
// shared pin.
pub const FAT_NAME_LEN_OFFSET: usize = 64;
pub const FAT_NAME_OFFSET: usize = 66;
pub const FAT_TAKEN_AT_OFFSET: usize = 384;
pub const FAT_RENEW_COUNT_OFFSET: usize = 392;
pub const FAT_TAG_LEN_OFFSET: usize = 396;
pub const FAT_TAG_BYTES_OFFSET: usize = 398;

/// The durable record kind, encoded in the header tag's low nibble (the
/// format version rides the second byte; version 1 is the only one this
/// build reads).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurableKind {
    /// A live durable lock: acquire/create.
    Live,
    /// Released but not destroyed: the record stands with its identity,
    /// the fencing token survives.
    Kept,
    /// The destruction: the real tombstone of the durable kind.
    Delete,
}

/// The durable kind's ABI code (the engine's `DurableKind` enum values).
const DKIND_LIVE: u32 = 1;
const DKIND_KEPT: u32 = 2;
const DKIND_DELETE: u32 = 3;

/// The engine's fat frame struct, shared with the C ABI and byte-exact
/// with the engine's on-disk layout: 768 bytes, zero padding, the
/// on-disk bytes ARE this struct's bytes. The binding layout is pinned
/// by the const asserts below — a drift is a compile error, never a
/// silent corruption. The canary (the mirror test below and the engine's
/// own) proves the field-true interpretation across the boundary.
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FatFrame {
    /// CL0: the same record header, its tag a durable kind.
    pub header: WalRecord,
    /// CL1-11: the name area, the counters and tag bytes, the
    /// consistency area — read through the accessors below, never by
    /// hand-rolled offset math outside this module.
    pub data: [u8; FAT_FRAME_SIZE - RECORD_SIZE],
}

/// The fat frame's binding layout, asserted at compile time on every
/// build. Public so downstream builds can import and reuse the pin.
pub const FAT_FRAME_LAYOUT: () = assert!(
    size_of::<FatFrame>() == FAT_FRAME_SIZE
        && size_of::<FatFrame>() == 768
        && offset_of!(FatFrame, header) == 0
        && offset_of!(FatFrame, data) == RECORD_SIZE
        && size_of::<[u8; FAT_FRAME_SIZE - RECORD_SIZE]>() == 704
);

impl FatFrame {
    /// The name bytes (the u16 length's span). Empty = no name.
    pub fn name(&self) -> &[u8] {
        let len = u16::from_le_bytes(
            self.data[FAT_NAME_LEN_OFFSET - RECORD_SIZE..][..2]
                .try_into()
                .expect("the name length reads"),
        ) as usize;
        &self.data[FAT_NAME_OFFSET - RECORD_SIZE..][..len]
    }

    /// The counters' taken-at wall clock, milliseconds.
    pub fn taken_at_ms(&self) -> u64 {
        u64::from_le_bytes(
            self.data[FAT_TAKEN_AT_OFFSET - RECORD_SIZE..][..8]
                .try_into()
                .expect("the taken-at counter reads"),
        )
    }

    pub fn renew_count(&self) -> u32 {
        u32::from_le_bytes(
            self.data[FAT_RENEW_COUNT_OFFSET - RECORD_SIZE..][..4]
                .try_into()
                .expect("the renew counter reads"),
        )
    }

    /// The tag bytes (the u16 length's span).
    pub fn tag_bytes(&self) -> &[u8] {
        let len = u16::from_le_bytes(
            self.data[FAT_TAG_LEN_OFFSET - RECORD_SIZE..][..2]
                .try_into()
                .expect("the tag length reads"),
        ) as usize;
        &self.data[FAT_TAG_BYTES_OFFSET - RECORD_SIZE..][..len]
    }
}

/// The frame's layout-exact byte image — the memcpy round trip's Rust
/// half. Sound because the const asserts above pin the struct to exactly
/// 768 bytes of plain little-endian fields with no padding.
pub fn frame_as_bytes(frame: &FatFrame) -> &[u8; FAT_FRAME_SIZE] {
    unsafe { &*(frame as *const FatFrame as *const [u8; FAT_FRAME_SIZE]) }
}

fn durable_kind_code(kind: DurableKind) -> u32 {
    match kind {
        DurableKind::Live => DKIND_LIVE,
        DurableKind::Kept => DKIND_KEPT,
        DurableKind::Delete => DKIND_DELETE,
    }
}

fn durable_kind_from_code(tag: u32) -> Option<DurableKind> {
    match tag {
        DKIND_LIVE => Some(DurableKind::Live),
        DKIND_KEPT => Some(DurableKind::Kept),
        DKIND_DELETE => Some(DurableKind::Delete),
        _ => None,
    }
}

/// The durable lock's fields, decoded: the header's identity plus the
/// frame's name, counters and tag bytes. The name and tag bytes are the
/// engine-vouched bytes, owned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DurableFrame {
    pub stamp: u64,
    pub expiry: u64,
    pub lock_id: u64,
    pub lease_id: u64,
    pub kind: DurableKind,
    pub holder: [u8; 16],
    pub lease_ms: u64,
    pub name: Vec<u8>,
    pub taken_at_ms: u64,
    pub renew_count: u32,
    pub tag_bytes: Vec<u8>,
}

impl DurableFrame {
    /// The labels the tag bytes carry: the engine's tag-bytes convention
    /// is the labels joined by a single `\n` byte each — sorted and
    /// deduplicated, the table's canonical form, and the admission
    /// validation guarantees a label can never contain `\n`, so the
    /// join is unambiguous. Empty labels = empty tag bytes. This is the
    /// exact encoding the byte-exact tests below hold.
    pub fn labels(&self) -> Vec<String> {
        self.tag_bytes
            .split(|&b| b == b'\n')
            .map(|label| String::from_utf8_lossy(label).into_owned())
            .collect()
    }
}

/// One durable lock's serializable content: what the lock table's lease
/// carries past the record format's identity, exactly the frame areas'
/// fields. `name_bytes` is the validated path's UTF-8 bytes; `tag_bytes`
/// the labels joined per [`DurableFrame::labels`]' convention.
#[derive(Clone)]
pub struct DurableChange {
    pub lock_id: u64,
    pub lease_id: u64,
    pub holder: [u8; 16],
    /// The lease expiry, engine units (nanoseconds).
    pub expiry_ns: u64,
    /// The granted window, milliseconds.
    pub lease_ms: u64,
    pub kind: DurableKind,
    pub name_bytes: Vec<u8>,
    pub taken_at_ms: u64,
    pub renew_count: u32,
    pub tag_bytes: Vec<u8>,
}

/// Encode one durable frame into its 768-byte on-disk form through the
/// ABI — the engine's own codec stamps the sha and the frame checksum.
/// This wrapper computes no checksum and carries no codec of its own.
/// `None` is a refusal: a zero lock id, an unknown kind, or a name/tag
/// span over the frame's budget cannot become a frame.
pub fn frame_encode(stamp: u64, change: &DurableChange) -> Option<[u8; FAT_FRAME_SIZE]> {
    let fields = WalRecord {
        stamp,
        expiry: change.expiry_ns,
        lock_id: change.lock_id,
        lease_id: change.lease_id,
        holder: change.holder,
        lease_ms: change.lease_ms,
        tag: durable_kind_code(change.kind),
        checksum: 0,
    };
    let mut bytes = [0u8; FAT_FRAME_SIZE];
    let rc = unsafe {
        wal_frame_encode(
            bytes.as_mut_ptr(),
            bytes.len(),
            &fields,
            change.name_bytes.as_ptr(),
            change.name_bytes.len(),
            change.taken_at_ms,
            change.renew_count,
            change.tag_bytes.as_ptr(),
            change.tag_bytes.len(),
        )
    };
    (rc == OK).then_some(bytes)
}

/// The frame the 768 bytes carry, or `None` when the engine's codec
/// refuses them: a failed integrity layer, a foreign format version, an
/// unknown kind, a zero lock id. Every refusal is a content refusal.
pub fn frame_decode(bytes: &[u8; FAT_FRAME_SIZE]) -> Option<DurableFrame> {
    let mut mirror = FatFrame {
        header: WalRecord {
            stamp: 0,
            expiry: 0,
            lock_id: 0,
            lease_id: 0,
            holder: [0; 16],
            lease_ms: 0,
            tag: 0,
            checksum: 0,
        },
        data: [0; FAT_FRAME_SIZE - RECORD_SIZE],
    };
    let rc = unsafe { wal_frame_decode(&mut mirror, bytes.as_ptr(), bytes.len()) };
    if rc != OK {
        return None;
    }
    Some(DurableFrame {
        stamp: mirror.header.stamp,
        expiry: mirror.header.expiry,
        lock_id: mirror.header.lock_id,
        lease_id: mirror.header.lease_id,
        kind: durable_kind_from_code(mirror.header.tag)?,
        holder: mirror.header.holder,
        lease_ms: mirror.header.lease_ms,
        name: mirror.name().to_vec(),
        taken_at_ms: mirror.taken_at_ms(),
        renew_count: mirror.renew_count(),
        tag_bytes: mirror.tag_bytes().to_vec(),
    })
}

/// The labels joined to the tag bytes: one `\n` byte between labels,
/// nothing else — the convention [`DurableFrame::labels`] reads back.
/// The admission validation (`valid_label`) guarantees a label can
/// never contain `\n`, so the join is unambiguous.
pub fn labels_to_tag_bytes(labels: &[String]) -> Vec<u8> {
    let mut bytes = Vec::new();
    for (i, label) in labels.iter().enumerate() {
        if i > 0 {
            bytes.push(b'\n');
        }
        bytes.extend_from_slice(label.as_bytes());
    }
    bytes
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

/// One durable lock's last-known identity, the wrapper's own bookkeeping:
/// the fields a released durable lock's kept frame must carry once the
/// table has dropped the lease. Refreshed from every snapshot this store
/// serializes (submit and flush alike) for every lock that carries a
/// name — the durable class — so a later release emits the kept frame
/// from here.
#[derive(Debug, Clone, PartialEq, Eq)]
struct DurableEntry {
    name: String,
    labels: Vec<String>,
    taken_at_ms: u64,
    renew_count: u32,
    /// The fencing token the kept frame retains.
    lease_id: u64,
    /// Released: the identity stands, no holder, no expiry.
    kept: bool,
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
    /// The durable class's last-known identities, keyed by lock id.
    durable: BTreeMap<u64, DurableEntry>,
    /// The forgotten store: `forget` removed the artefact and the
    /// reopen refused — every write-path call reports EIO until the
    /// store is dropped. A forgotten-and-dead store never blocks the
    /// boot (load answers none) and never fails silently (the stop's
    /// flush refuses loudly).
    dead: bool,
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
            durable: BTreeMap::new(),
            dead: false,
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
    /// generation per stream and submitted forward — unnamed locks ride
    /// the ephemeral stream as records (a live record, a tombstone for a
    /// release), named locks ride the durable stream as fat frames (a
    /// live frame; a release's kept frame, the identity retained). Both
    /// calls must succeed for a clean submit; a refusal keeps the set so
    /// the next tick retries — what landed is last-wins-identical, what
    /// was refused is not lost.
    pub fn submit(&mut self, snap: &StateSnapshot) -> io::Result<usize> {
        if self.changed.is_empty() {
            return Ok(0);
        }
        if self.dead {
            return Err(wal_error(
                EIO,
                "the store forgot its artefact and could not reopen",
            ));
        }
        let stamp = unix_ns();
        let mut records = Vec::with_capacity(self.changed.len() * RECORD_SIZE);
        let mut frames = Vec::with_capacity(self.changed.len() * FAT_FRAME_SIZE);
        for &lock_id in &self.changed {
            match snap.locks.get(&lock_id) {
                Some(lease) if lease.name.is_some() => {
                    let name = lease.name.as_ref().expect("the durable lock's name");
                    let labels = lease.labels.clone().unwrap_or_default();
                    // The classification the flush carries: a named lease
                    // with an expiry (a live durable lock) is a live
                    // frame; a named record with none (a loaded kept
                    // lock, or the keeper record a break leaves) is the
                    // kept frame as it stands. Tick and flush must
                    // classify identically — the flush reconciles its
                    // frames against the mirror these submits built.
                    if lease.expiry == 0 {
                        self.durable.insert(
                            lock_id,
                            DurableEntry {
                                name: name.clone(),
                                labels,
                                taken_at_ms: lease.taken_at_ms,
                                renew_count: lease.renew_count,
                                lease_id: lease.lease_id,
                                kept: true,
                            },
                        );
                        frames.extend_from_slice(&self.durable_frame_kept(stamp, lock_id)?);
                    } else {
                        self.durable.insert(
                            lock_id,
                            DurableEntry {
                                name: name.clone(),
                                labels,
                                taken_at_ms: lease.taken_at_ms,
                                renew_count: lease.renew_count,
                                lease_id: lease.lease_id,
                                kept: false,
                            },
                        );
                        frames.extend_from_slice(&self.durable_frame_live(stamp, lock_id, lease)?);
                    }
                }
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
                None => {
                    // A release. The durable class: the kept frame from
                    // the last-known identity (already kept in the
                    // engine's mirror means nothing to write). The
                    // ephemeral class: the tombstone.
                    match self.durable.get_mut(&lock_id) {
                        Some(entry) => {
                            if entry.kept {
                                continue;
                            }
                            entry.kept = true;
                            frames.extend_from_slice(&self.durable_frame_kept(stamp, lock_id)?);
                        }
                        None => encode_into(&mut records, &Record::tombstone(stamp, lock_id, 0))?,
                    }
                }
            }
        }

        if !records.is_empty() {
            let rc = unsafe { wal_submit(self.handle, records.as_ptr(), records.len()) };
            match rc {
                OK => {}
                EAGAIN => return Err(wal_error(rc, "a previous batch is still outstanding")),
                ENOSPC => {
                    return Err(wal_error(
                        rc,
                        "the ephemeral ring cannot take the generation",
                    ));
                }
                EINVAL => Err(wal_error(rc, "the engine refused a record"))?,
                EIO => Err(wal_error(rc, "backend IO failure"))?,
                other => Err(wal_error(other, "unexpected wal_submit result"))?,
            }
        }
        if !frames.is_empty() {
            let rc = unsafe { wal_durable_submit(self.handle, frames.as_ptr(), frames.len()) };
            match rc {
                OK => {}
                EAGAIN => return Err(wal_error(rc, "a previous batch is still outstanding")),
                ENOSPC => return Err(wal_error(rc, "the durable ring cannot take the generation")),
                EINVAL => Err(wal_error(rc, "the engine refused a frame"))?,
                EIO => Err(wal_error(rc, "backend IO failure"))?,
                other => Err(wal_error(other, "unexpected wal_durable_submit result"))?,
            }
        }
        self.changed.clear();
        Ok(records.len() / RECORD_SIZE + frames.len() / FAT_FRAME_SIZE)
    }

    /// The live durable frame for `lock_id`, the fields from the
    /// snapshot's lease (the name and labels through the frame areas'
    /// encoding — see [`DurableFrame::labels`]).
    fn durable_frame_live(
        &self,
        stamp: u64,
        lock_id: u64,
        lease: &Lease,
    ) -> io::Result<[u8; FAT_FRAME_SIZE]> {
        let change = DurableChange {
            lock_id,
            lease_id: lease.lease_id,
            holder: *lease.holder.as_bytes(),
            expiry_ns: expiry_to_ns(lease.expiry),
            lease_ms: lease.lease_ms,
            kind: DurableKind::Live,
            name_bytes: lease
                .name
                .as_ref()
                .expect("the durable lock's name")
                .as_bytes()
                .to_vec(),
            taken_at_ms: lease.taken_at_ms,
            renew_count: lease.renew_count,
            tag_bytes: labels_to_tag_bytes(&lease.labels.clone().unwrap_or_default()),
        };
        frame_encode(stamp, &change).ok_or_else(|| {
            wal_error(
                EINVAL,
                "the engine refused a durable lock's fields (a bug named loudly)",
            )
        })
    }

    /// The kept frame for `lock_id`: the identity the entry retained —
    /// the fencing token, the name, the counters — with the holder
    /// cleared and no expiry (a kept record carries none; the engine's
    /// scan never drops one).
    fn durable_frame_kept(&self, stamp: u64, lock_id: u64) -> io::Result<[u8; FAT_FRAME_SIZE]> {
        let entry = self.durable.get(&lock_id).expect("the durable entry");
        let change = DurableChange {
            lock_id,
            lease_id: entry.lease_id,
            holder: [0; 16],
            expiry_ns: 0,
            lease_ms: 0,
            kind: DurableKind::Kept,
            name_bytes: entry.name.as_bytes().to_vec(),
            taken_at_ms: entry.taken_at_ms,
            renew_count: entry.renew_count,
            tag_bytes: labels_to_tag_bytes(&entry.labels),
        };
        frame_encode(stamp, &change).ok_or_else(|| {
            wal_error(
                EINVAL,
                "the engine refused a kept lock's fields (a bug named loudly)",
            )
        })
    }

    /// Drain completions non-blocking. A no-op success on the
    /// inline-completion platforms (darwin); on Linux it reaps the ring's
    /// completions and moves the counters. Call this on the eventfd wake
    /// and before the next submit.
    pub fn pump(&mut self) -> io::Result<()> {
        if self.dead {
            return Err(wal_error(
                EIO,
                "the store forgot its artefact and could not reopen",
            ));
        }
        let rc = unsafe { wal_pump(self.handle) };
        match rc {
            OK => Ok(()),
            EIO => Err(wal_error(rc, "backend IO failure")),
            other => Err(wal_error(other, "unexpected wal_pump result")),
        }
    }

    /// THE DIRTY BOOT'S FORGET: the write half of the law's third
    /// clause. A boot whose verdict is not a clean stop never reads the
    /// artefact — and this store must not carry the old artefact's
    /// bookkeeping forward either: the engine's mirrors are seeded from
    /// the open scan, and a flush reconciles the caller's sets against
    /// them. A stale mirror would fail a correct stop. So the dirty
    /// boot's store closes the handle, removes the artefact (it is a
    /// fallback; nothing vouches for it), and reopens the pre-laid-out
    /// empty file: the mirrors start empty and the tick path rebuilds
    /// them from every lock the replica stream re-commits (each is
    /// noted as it lands).
    pub fn forget_artifact(&mut self) {
        if self.dead {
            return;
        }
        unsafe { wal_close(self.handle) };
        self.handle = std::ptr::null_mut();
        match std::fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                self.dead = true;
                eprintln!(
                    "lunet-advisory-lock: the WAL snapshot store could not \
                     forget its artefact at {} ({error}); the store is dead — \
                     the flush at stop will refuse loudly",
                    self.path.display()
                );
                return;
            }
        }
        match Self::new(&self.path) {
            Ok(reopened) => {
                // Take the fresh handle; the bookkeeping the old life
                // held (the changed set, the durable map) is dropped
                // with the replaced fields.
                self.handle = reopened.handle;
                self.changed = BTreeSet::new();
                self.durable = BTreeMap::new();
                std::mem::forget(reopened);
            }
            Err(error) => {
                self.dead = true;
                eprintln!(
                    "lunet-advisory-lock: the WAL snapshot store could not \
                     reopen after forgetting its artefact at {} ({error}); \
                     the store is dead — the flush at stop will refuse loudly",
                    self.path.display()
                );
            }
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
        if self.dead {
            return Err(wal_error(
                EIO,
                "the store forgot its artefact and could not reopen",
            ));
        }
        let mut raw = WalStatusRaw {
            ephemeral: WalStreamStatusRaw::default(),
            durable: WalStreamStatusRaw::default(),
            last_completed_unix_ns: 0,
        };
        let rc = unsafe { wal_status(self.handle, &mut raw) };
        match rc {
            OK => Ok(WalStatus {
                ephemeral: WalStreamStatus {
                    completed_generations: raw.ephemeral.completed_generations,
                    completed_records: raw.ephemeral.completed_records,
                    refusals: raw.ephemeral.refusals,
                    in_flight: raw.ephemeral.in_flight,
                },
                durable: WalStreamStatus {
                    completed_generations: raw.durable.completed_generations,
                    completed_records: raw.durable.completed_records,
                    refusals: raw.durable.refusals,
                    in_flight: raw.durable.in_flight,
                },
                last_completed_unix_ns: raw.last_completed_unix_ns,
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

    /// The durable stream's live set decoded: one named lease per fat
    /// frame, the display fields from the frame areas. A delete kind in
    /// the load stream, or any frame the engine's codec refuses, is a
    /// content refusal — `None`, the cold-start answer.
    fn decode_frames(frames: &[u8]) -> Option<StateSnapshot> {
        let mut locks = std::collections::BTreeMap::new();
        for chunk in frames.chunks_exact(FAT_FRAME_SIZE) {
            let bytes: &[u8; FAT_FRAME_SIZE] = chunk.try_into().ok()?;
            let frame = frame_decode(bytes)?;
            let kind = match frame.kind {
                DurableKind::Live | DurableKind::Kept => frame.kind,
                DurableKind::Delete => return None,
            };
            let name = String::from_utf8(frame.name.clone()).ok()?;
            let labels = if frame.tag_bytes.is_empty() {
                None
            } else {
                Some(frame.labels())
            };
            locks.insert(
                frame.lock_id,
                Lease {
                    lease_id: frame.lease_id,
                    holder: Uuid::from_bytes(frame.holder),
                    expiry: expiry_to_ms(frame.expiry),
                    lease_ms: frame.lease_ms,
                    name: Some(name),
                    labels,
                    taken_at_ms: frame.taken_at_ms,
                    renew_count: frame.renew_count,
                },
            );
            let _ = kind;
        }
        Some(StateSnapshot { locks })
    }

    /// The kept frame for a lease the snapshot still holds with no
    /// expiry (a loaded kept lock, re-flushed as it stands): the
    /// identity from the lease itself.
    fn durable_frame_kept_from_lease(
        &self,
        stamp: u64,
        lock_id: u64,
        lease: &Lease,
    ) -> io::Result<[u8; FAT_FRAME_SIZE]> {
        let change = DurableChange {
            lock_id,
            lease_id: lease.lease_id,
            holder: [0; 16],
            expiry_ns: 0,
            lease_ms: 0,
            kind: DurableKind::Kept,
            name_bytes: lease
                .name
                .as_ref()
                .expect("the durable lock's name")
                .as_bytes()
                .to_vec(),
            taken_at_ms: lease.taken_at_ms,
            renew_count: lease.renew_count,
            tag_bytes: labels_to_tag_bytes(&lease.labels.clone().unwrap_or_default()),
        };
        frame_encode(stamp, &change).ok_or_else(|| {
            wal_error(
                EINVAL,
                "the engine refused a kept lock's fields (a bug named loudly)",
            )
        })
    }
}

impl StateStore for WalSnapshotStore {
    /// One changed lock, the loop's note.
    fn note(&mut self, lock_id: u64) {
        self.note(lock_id)
    }

    /// The steady tick's write: the noted locks, one generation per
    /// stream (see [`WalSnapshotStore::submit`]).
    fn tick(&mut self, snap: &StateSnapshot) -> io::Result<usize> {
        self.submit(snap)
    }

    /// Drain completions (see [`WalSnapshotStore::pump`]).
    fn pump(&mut self) -> io::Result<()> {
        self.pump()
    }

    /// The dirty boot's forget (see [`WalSnapshotStore::forget_artifact`]).
    fn forget(&mut self) {
        self.forget_artifact()
    }

    /// The per-stream counters, mapped to the seam's record.
    fn state_status(&self) -> Option<crate::state::StoreStatus> {
        let status = self.status().ok()?;
        Some(crate::state::StoreStatus {
            ephemeral: crate::state::StreamCounters {
                completed_generations: status.ephemeral.completed_generations,
                completed_records: status.ephemeral.completed_records,
                refusals: status.ephemeral.refusals,
                in_flight: status.ephemeral.in_flight,
            },
            durable: crate::state::StreamCounters {
                completed_generations: status.durable.completed_generations,
                completed_records: status.durable.completed_records,
                refusals: status.durable.refusals,
                in_flight: status.durable.in_flight,
            },
            last_completed_unix_ns: status.last_completed_unix_ns,
        })
    }

    /// The shutdown generation: the whole live sets, serialized here on
    /// the calling thread — the ephemeral set as records, the durable set
    /// as fat frames — written forward by the engine and fsynced exactly
    /// once, blocking. The engine reconciles each set against its
    /// stream's mirror; a refusal is a FAILED stop — an `Err`, never a
    /// silent skip.
    fn flush(&mut self, snap: &StateSnapshot) -> io::Result<()> {
        if self.dead {
            return Err(wal_error(
                EIO,
                "the store forgot its artefact and could not reopen",
            ));
        }
        let stamp = unix_ns();
        let mut records = Vec::with_capacity(snap.locks.len() * RECORD_SIZE);
        let mut frames = Vec::with_capacity(snap.locks.len() * FAT_FRAME_SIZE);

        for (&lock_id, lease) in &snap.locks {
            if lock_id == 0 {
                crate::maybe_invariant!("wal snapshot: lock id 0 is not a lock");
                continue;
            }
            if lease.name.is_some() {
                // The durable class: a live frame while the lease holds
                // (a nonzero expiry), the kept frame once it does not
                // (a released durable lock carries no expiry — the
                // loaded kept locks come back exactly so).
                let kind = if lease.expiry == 0 {
                    DurableKind::Kept
                } else {
                    DurableKind::Live
                };
                if kind == DurableKind::Live {
                    frames.extend_from_slice(&self.durable_frame_live(stamp, lock_id, lease)?);
                    self.durable.insert(
                        lock_id,
                        DurableEntry {
                            name: lease.name.clone().expect("the durable lock's name"),
                            labels: lease.labels.clone().unwrap_or_default(),
                            taken_at_ms: lease.taken_at_ms,
                            renew_count: lease.renew_count,
                            lease_id: lease.lease_id,
                            kept: false,
                        },
                    );
                } else {
                    frames.extend_from_slice(
                        &self.durable_frame_kept_from_lease(stamp, lock_id, lease)?,
                    );
                    self.durable.insert(
                        lock_id,
                        DurableEntry {
                            name: lease.name.clone().expect("the durable lock's name"),
                            labels: lease.labels.clone().unwrap_or_default(),
                            taken_at_ms: lease.taken_at_ms,
                            renew_count: lease.renew_count,
                            lease_id: lease.lease_id,
                            kept: true,
                        },
                    );
                }
            } else {
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
        }

        // The durable locks the snapshot no longer names: released (or
        // expired) since the last serialize — the kept frame from the
        // last-known identity, and it stays in the set: the engine's
        // mirror holds it and every flush rewrites the full durable set.
        for (&lock_id, entry) in self.durable.iter_mut() {
            if snap
                .locks
                .get(&lock_id)
                .is_some_and(|lease| lease.name.is_some())
            {
                continue;
            }
            entry.kept = true;
        }
        let named_in_snap = |lock_id: &u64| {
            snap.locks
                .get(lock_id)
                .is_some_and(|lease| lease.name.is_some())
        };
        let kept_ids: Vec<u64> = self
            .durable
            .iter()
            .filter(|(lock_id, entry)| entry.kept && !named_in_snap(lock_id))
            .map(|(&lock_id, _)| lock_id)
            .collect();
        for lock_id in kept_ids {
            frames.extend_from_slice(&self.durable_frame_kept(stamp, lock_id)?);
        }

        let rc = unsafe {
            wal_flush_sync(
                self.handle,
                records.as_ptr(),
                records.len(),
                frames.as_ptr(),
                frames.len(),
            )
        };
        match rc {
            OK => Ok(()),
            ENOSPC => Err(wal_error(
                rc,
                "a live set overruns its pair's remaining room",
            )),
            EINVAL => Err(wal_error(
                rc,
                &format!(
                    "a live set diverges from the engine's mirror \
                     (caller: {} ephemeral records, {} durable frames, \
                     {} durable identities tracked) — the stop's tick did \
                     not catch the mirror up, or a content refusal refused \
                     a member",
                    records.len() / RECORD_SIZE,
                    frames.len() / FAT_FRAME_SIZE,
                    self.durable.len(),
                ),
            )),
            EIO => Err(wal_error(rc, "backend IO failure")),
            other => Err(wal_error(other, "unexpected wal_flush_sync result")),
        }
    }

    /// The lazy load: both streams' live sets, the ephemeral records and
    /// the durable frames decoded into one snapshot (a named lock's
    /// display fields ride the frame areas; a loaded kept lock carries
    /// its identity, no holder, no expiry). `Ok(None)` — the cold-start
    /// answer for "there is nothing trustworthy here", for either
    /// stream. An `Err` is only IO trouble or this wrapper's own sizing
    /// bug; a content refusal cannot block a boot.
    fn load(&self) -> io::Result<Option<StateSnapshot>> {
        let mut buf = vec![0u8; self.capacity as usize * RECORD_SIZE];
        let mut out_len: usize = 0;
        let rc = unsafe { wal_load(self.handle, buf.as_mut_ptr(), buf.len(), &mut out_len) };
        let ephemeral_found = rc == OK;
        let ephemeral = match rc {
            OK => {
                if out_len > buf.len() {
                    return Err(wal_error(ENOBUFS, "the engine copied past the load buffer"));
                }
                Self::decode_records(&buf[..out_len])
            }
            // 1 = none: the cold start.
            1 => Some(StateSnapshot::default()),
            ENOBUFS => {
                return Err(wal_error(
                    rc,
                    "the load buffer is smaller than the live set (a sizing bug)",
                ));
            }
            EIO => return Err(wal_error(rc, "backend IO failure")),
            other => return Err(wal_error(other, "unexpected wal_load result")),
        };

        let mut durable_buf = vec![0u8; self.capacity as usize * FAT_FRAME_SIZE];
        let mut durable_len: usize = 0;
        let rc = unsafe {
            wal_load_durable(
                self.handle,
                durable_buf.as_mut_ptr(),
                durable_buf.len(),
                &mut durable_len,
            )
        };
        let durable_found = rc == OK;
        let durable = match rc {
            OK => {
                if durable_len > durable_buf.len() {
                    return Err(wal_error(ENOBUFS, "the engine copied past the load buffer"));
                }
                Self::decode_frames(&durable_buf[..durable_len])
            }
            1 => Some(StateSnapshot::default()),
            ENOBUFS => {
                return Err(wal_error(
                    rc,
                    "the durable load buffer is smaller than the live set (a sizing bug)",
                ));
            }
            EIO => return Err(wal_error(rc, "backend IO failure")),
            other => return Err(wal_error(other, "unexpected wal_load_durable result")),
        };

        let (Some(ephemeral), Some(durable)) = (ephemeral, durable) else {
            return Ok(None);
        };
        if !ephemeral_found && !durable_found {
            // Both streams answered none: the cold start.
            return Ok(None);
        }
        let mut ephemeral = ephemeral;
        let mut durable = durable;
        // The streams' id spaces are separated by design; an id in both
        // is content nothing vouches for.
        for lock_id in ephemeral.locks.keys() {
            if durable.locks.contains_key(lock_id) {
                return Ok(None);
            }
        }
        ephemeral.locks.append(&mut durable.locks);
        Ok(Some(ephemeral))
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
