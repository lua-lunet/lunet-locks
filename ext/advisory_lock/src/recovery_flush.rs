//! The E2 recovery-boundary flush variants (the experiment design's §4).
//!
//! At the recovery boundary — the dirty-boot/reincarnation classification
//! point in the adapter — a variant may execute a forced flush against a
//! caller-provided scratch directory before the reincarnated node rejoins
//! serving:
//!
//! - **Variant 0 — diskless (the default).** Nothing is written. Quorum
//!   memory is the only protocol state.
//! - **Variant 1 — naive single write (the latency baseline).** One 4 KiB
//!   block write followed by `fsync`. Per FAST'18 (Alagappan et al.), even
//!   this single write plus `fsync` is not fault-safe against real disk
//!   faults; it is the latency baseline, never a safety claim.
//! - **Variant 2 — double-ring write.** The TigerBeetle-style double write:
//!   one 4 KiB block to the first ring carrying a 64-byte checksum header
//!   plus one 64-byte data line, then the header+checksum copy — with NO
//!   payload — to the second ring. The two zones are fixed, 4 KiB-sector
//!   aligned, and spaced [`RING_SPACING_BYTES`] apart in the same data file,
//!   so the two copies of the checksum sit in different erasure blocks.
//!
//! BOTH flush variants carry fake data only and NEVER read back: the
//! variants measure write-and-flush latency only and never reconstruct
//! state from what was written. The unit tests below do inspect the bytes —
//! that is geometry verification, not a read-back path; no runtime code
//! ever reads these files.
//!
//! The flush outcome reports the measured latency (flush start to write
//! completion, `fsync` included) and the bytes written.

use std::fs::OpenOptions;
use std::io::{self, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

/// The recovery-boundary flush variant selected at boot.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryFlush {
    /// Variant 0: no durable writes at the recovery boundary.
    Diskless,
    /// Variant 1: one 4 KiB block write + `fsync` (the latency baseline).
    SingleBlock,
    /// Variant 2: the double-ring write (checksum header + data line to
    /// ring one, header copy without payload to ring two).
    DoubleRing,
}

impl RecoveryFlush {
    /// Parses the variant's command-line spelling.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "diskless" => Some(Self::Diskless),
            "single" => Some(Self::SingleBlock),
            "double-ring" => Some(Self::DoubleRing),
            _ => None,
        }
    }

    /// The variant's label as it appears in logs and results.
    pub fn label(&self) -> &'static str {
        match self {
            Self::Diskless => "diskless",
            Self::SingleBlock => "single",
            Self::DoubleRing => "double-ring",
        }
    }
}

/// One 4 KiB block: the sector/erasure unit every variant writes in.
pub const BLOCK_BYTES: usize = 4096;
/// One 64-byte cache line of data: the payload ring one carries.
pub const DATA_LINE_BYTES: usize = 64;
/// The 64-byte checksum header that precedes the data line and is copied,
/// without payload, to ring two.
pub const HEADER_BYTES: usize = 64;
/// The spacing between the two rings: 1 MiB, a multiple of the 4 KiB
/// sector, so the two checksum copies land in different erasure blocks.
pub const RING_SPACING_BYTES: u64 = 1024 * 1024;

const RING_MAGIC: &[u8; 8] = b"LVRRFLSH";

/// One flush execution's report: the variant, the bytes written, and the
/// measured write-and-flush latency.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FlushOutcome {
    pub variant: &'static str,
    pub bytes_written: u64,
    pub latency: Duration,
}

/// The 64-byte ring header: magic, the FNV-1a checksum over the data line,
/// the flush sequence (the boot's incarnation drives it), and the payload
/// length (64 on ring one; ring two carries the identical header — the
/// checksum copy — with the payload region absent).
fn ring_header(checksum: u64, sequence: u64, data_len: u64) -> [u8; HEADER_BYTES] {
    let mut header = [0u8; HEADER_BYTES];
    header[..8].copy_from_slice(RING_MAGIC);
    header[8..16].copy_from_slice(&checksum.to_be_bytes());
    header[16..24].copy_from_slice(&sequence.to_be_bytes());
    header[24..32].copy_from_slice(&data_len.to_be_bytes());
    header
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf29ce484222325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    hash
}

/// Deterministic fake data (the design's junk payload): the seed fills the
/// bytes; nothing in the runtime ever reads them back.
fn fake_line(seed: u64) -> [u8; DATA_LINE_BYTES] {
    let mut line = [0u8; DATA_LINE_BYTES];
    line[..8].copy_from_slice(&seed.to_be_bytes());
    let mut state = seed | 1;
    for byte in line[8..].iter_mut() {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        *byte = (state >> 24) as u8;
    }
    line
}

/// Executes the selected variant's flush against `dir` (created on demand)
/// and reports the measured latency. Variant 0 writes nothing and reports a
/// zero latency. A flush failure here is a boot failure at the recovery
/// boundary: the caller refuses the boot rather than rejoin unmeasured.
pub fn execute(dir: &Path, variant: RecoveryFlush, sequence: u64) -> io::Result<FlushOutcome> {
    match variant {
        RecoveryFlush::Diskless => Ok(FlushOutcome {
            variant: variant.label(),
            bytes_written: 0,
            latency: Duration::ZERO,
        }),
        RecoveryFlush::SingleBlock => {
            std::fs::create_dir_all(dir)?;
            let path = dir.join("recovery-flush-single.bin");
            let start = Instant::now();
            let mut file = OpenOptions::new()
                .create(true)
                // Overwrite in place: the baseline stays exactly one block
                // across boots.
                .truncate(false)
                .write(true)
                .open(&path)?;
            file.seek(SeekFrom::Start(0))?;
            let block = fake_block(sequence);
            file.write_all(&block)?;
            file.sync_all()?;
            drop(file);
            Ok(FlushOutcome {
                variant: variant.label(),
                bytes_written: BLOCK_BYTES as u64,
                latency: start.elapsed(),
            })
        }
        RecoveryFlush::DoubleRing => {
            std::fs::create_dir_all(dir)?;
            let path = dir.join("recovery-flush-double.bin");
            let start = Instant::now();
            let mut file = OpenOptions::new()
                .create(true)
                .truncate(false)
                .write(true)
                .open(&path)?;
            let line = fake_line(sequence);
            let checksum = fnv1a(&line);
            let header = ring_header(checksum, sequence, DATA_LINE_BYTES as u64);
            // Ring one: checksum header + the 64-byte data line, in one
            // 4 KiB-aligned block at the file's start.
            file.seek(SeekFrom::Start(0))?;
            let mut ring_one = [0u8; BLOCK_BYTES];
            ring_one[..HEADER_BYTES].copy_from_slice(&header);
            ring_one[HEADER_BYTES..HEADER_BYTES + DATA_LINE_BYTES].copy_from_slice(&line);
            file.write_all(&ring_one)?;
            // Ring two: the header+checksum copy with NO payload, in its own
            // 4 KiB-aligned zone spaced RING_SPACING_BYTES from ring one.
            file.seek(SeekFrom::Start(RING_SPACING_BYTES))?;
            let mut ring_two = [0u8; BLOCK_BYTES];
            ring_two[..HEADER_BYTES].copy_from_slice(&header);
            file.write_all(&ring_two)?;
            file.sync_all()?;
            drop(file);
            Ok(FlushOutcome {
                variant: variant.label(),
                bytes_written: 2 * BLOCK_BYTES as u64,
                latency: start.elapsed(),
            })
        }
    }
}

fn fake_block(seed: u64) -> [u8; BLOCK_BYTES] {
    let mut block = [0u8; BLOCK_BYTES];
    let mut state = seed | 1;
    for chunk in block.chunks_mut(8) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        let bytes = state.to_be_bytes();
        chunk.copy_from_slice(&bytes[..chunk.len()]);
    }
    block
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    fn scratch(name: &str) -> std::path::PathBuf {
        use std::sync::atomic::{AtomicU64, Ordering};
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let dir = std::env::temp_dir().join(format!(
            "lunet-recovery-flush-{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::Relaxed),
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    #[test]
    fn variant_labels_round_trip_through_parse() {
        for (text, variant) in [
            ("diskless", RecoveryFlush::Diskless),
            ("single", RecoveryFlush::SingleBlock),
            ("double-ring", RecoveryFlush::DoubleRing),
        ] {
            assert_eq!(RecoveryFlush::parse(text), Some(variant));
            assert_eq!(RecoveryFlush::parse(variant.label()), Some(variant));
        }
        assert_eq!(RecoveryFlush::parse("tiger"), None);
    }

    #[test]
    fn variant_0_writes_nothing() {
        let dir = scratch("diskless");
        let outcome = execute(&dir, RecoveryFlush::Diskless, 7).unwrap();
        assert_eq!(outcome.bytes_written, 0);
        assert_eq!(outcome.latency, Duration::ZERO);
        assert!(!dir.exists(), "variant 0 must not create the scratch dir");
    }

    #[test]
    fn variant_1_writes_exactly_one_4k_block_and_fsyncs() {
        let dir = scratch("single");
        let outcome = execute(&dir, RecoveryFlush::SingleBlock, 7).unwrap();
        assert_eq!(outcome.variant, "single");
        assert_eq!(outcome.bytes_written, BLOCK_BYTES as u64);
        assert!(outcome.latency > Duration::ZERO, "latency is measured");
        let data = fs::read(dir.join("recovery-flush-single.bin")).unwrap();
        assert_eq!(data.len(), BLOCK_BYTES, "exactly one 4 KiB block on disk");
        // The fake data is written, not derived from anything durable.
        assert_ne!(&data[..8], &[0u8; 8]);
        // A repeat boot overwrites the same single block in place.
        let again = execute(&dir, RecoveryFlush::SingleBlock, 8).unwrap();
        assert_eq!(again.bytes_written, BLOCK_BYTES as u64);
        assert_eq!(
            fs::metadata(dir.join("recovery-flush-single.bin"))
                .unwrap()
                .len(),
            BLOCK_BYTES as u64,
            "the baseline stays exactly one block across boots"
        );
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn variant_2_writes_both_rings_with_the_documented_geometry() {
        let dir = scratch("double");
        let outcome = execute(&dir, RecoveryFlush::DoubleRing, 42).unwrap();
        assert_eq!(outcome.variant, "double-ring");
        assert_eq!(outcome.bytes_written, 2 * BLOCK_BYTES as u64);
        assert!(outcome.latency > Duration::ZERO, "latency is measured");
        let data = fs::read(dir.join("recovery-flush-double.bin")).unwrap();
        // The file spans both zones: ring one at 0, ring two one spacing
        // later, both 4 KiB aligned (the spacing is a sector multiple).
        assert_eq!(data.len() as u64, RING_SPACING_BYTES + BLOCK_BYTES as u64);
        assert_eq!(RING_SPACING_BYTES % 4096, 0, "zones are 4 KiB aligned");
        // Ring one: checksum header, then the 64-byte data line.
        let line = &data[HEADER_BYTES..HEADER_BYTES + DATA_LINE_BYTES];
        assert_eq!(&line[..8], &42u64.to_be_bytes(), "seeded fake data");
        let header_one = &data[..HEADER_BYTES];
        assert_eq!(&header_one[..8], RING_MAGIC);
        let checksum_one = u64::from_be_bytes(header_one[8..16].try_into().unwrap());
        assert_eq!(checksum_one, fnv1a(line), "ring one checksums its payload");
        assert_eq!(
            u64::from_be_bytes(header_one[24..32].try_into().unwrap()),
            DATA_LINE_BYTES as u64,
            "ring one declares its payload length"
        );
        // Ring two: the identical header copy — the checksum sits in a
        // different erasure block — with NO payload behind it.
        let header_two =
            &data[RING_SPACING_BYTES as usize..RING_SPACING_BYTES as usize + HEADER_BYTES];
        assert_eq!(header_two, header_one, "the header copy is byte-identical");
        let payload_two = &data[RING_SPACING_BYTES as usize + HEADER_BYTES
            ..RING_SPACING_BYTES as usize + HEADER_BYTES + DATA_LINE_BYTES];
        assert!(
            payload_two.iter().all(|byte| *byte == 0),
            "ring two carries no payload"
        );
        let _ = fs::remove_dir_all(&dir);
    }
}
