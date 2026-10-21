//! The E2 recovery-boundary flush variants: diskless (the default), one
//! 4 KiB block write + fsync (the latency baseline), or the TigerBeetle-style
//! double-ring write whose two checksum copies sit in different erasure
//! blocks. Both flush variants carry fake data only and NEVER read back:
//! they measure write-and-flush latency, never reconstruct state.

use crate::disk::{Disk, std_disk};
use std::io;
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
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "diskless" => Some(Self::Diskless),
            "single" => Some(Self::SingleBlock),
            "double-ring" => Some(Self::DoubleRing),
            _ => None,
        }
    }

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

/// The 64-byte ring header: magic, the FNV-1a checksum over the data
/// line, the flush sequence, the payload length.
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

/// Executes the selected variant's flush against `dir` and reports the
/// measured latency; variant 0 writes nothing. A flush failure is a boot
/// failure at the recovery boundary.
pub fn execute(dir: &Path, variant: RecoveryFlush, sequence: u64) -> io::Result<FlushOutcome> {
    execute_on(&*std_disk(), dir, variant, sequence)
}

/// [`execute`] against a named [`Disk`].
pub fn execute_on(
    disk: &dyn Disk,
    dir: &Path,
    variant: RecoveryFlush,
    sequence: u64,
) -> io::Result<FlushOutcome> {
    match variant {
        RecoveryFlush::Diskless => Ok(FlushOutcome {
            variant: variant.label(),
            bytes_written: 0,
            latency: Duration::ZERO,
        }),
        RecoveryFlush::SingleBlock => {
            disk.create_dir_all(dir)?;
            let path = dir.join("recovery-flush-single.bin");
            let start = Instant::now();
            // Overwrite in place: the baseline stays exactly one block
            // across boots.
            let mut file = disk.open_or_create_write(&path)?;
            file.seek(std::io::SeekFrom::Start(0))?;
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
            disk.create_dir_all(dir)?;
            let path = dir.join("recovery-flush-double.bin");
            let start = Instant::now();
            let mut file = disk.open_or_create_write(&path)?;
            let line = fake_line(sequence);
            let checksum = fnv1a(&line);
            let header = ring_header(checksum, sequence, DATA_LINE_BYTES as u64);
            // Ring one: checksum header + the 64-byte data line, in one
            // 4 KiB-aligned block at the file's start.
            file.seek(std::io::SeekFrom::Start(0))?;
            let mut ring_one = [0u8; BLOCK_BYTES];
            ring_one[..HEADER_BYTES].copy_from_slice(&header);
            ring_one[HEADER_BYTES..HEADER_BYTES + DATA_LINE_BYTES].copy_from_slice(&line);
            file.write_all(&ring_one)?;
            // Ring two: the header+checksum copy with NO payload, in its own
            // 4 KiB-aligned zone spaced RING_SPACING_BYTES from ring one.
            file.seek(std::io::SeekFrom::Start(RING_SPACING_BYTES))?;
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
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn scratch(name: &str) -> PathBuf {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/recovery-flush-unit");
        let dir = root.join(format!(
            "{name}-{}-{}",
            std::process::id(),
            SEQ.fetch_add(1, Ordering::SeqCst)
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
            assert_eq!(variant.label(), text, "the label spells the spelling");
        }
        assert_eq!(RecoveryFlush::parse(""), None);
        assert_eq!(RecoveryFlush::parse("double"), None);
        assert_eq!(RecoveryFlush::parse("DOUBLE-RING"), None);
    }

    #[test]
    fn variant_0_writes_nothing() {
        let dir = scratch("variant-0");
        let outcome = execute(&dir, RecoveryFlush::Diskless, 7).expect("variant 0 never fails");
        assert_eq!(outcome.variant, "diskless");
        assert_eq!(outcome.bytes_written, 0);
        assert_eq!(outcome.latency, Duration::ZERO);
        assert!(
            !dir.exists(),
            "the diskless variant never creates the scratch tree"
        );
    }

    #[test]
    fn variant_1_writes_exactly_one_4k_block_and_fsyncs() {
        let dir = scratch("variant-1");
        let outcome = execute(&dir, RecoveryFlush::SingleBlock, 9).expect("the flush executes");
        assert_eq!(outcome.variant, "single");
        assert_eq!(outcome.bytes_written, BLOCK_BYTES as u64);
        let path = dir.join("recovery-flush-single.bin");
        assert_eq!(
            fs::metadata(&path).expect("the block file exists").len(),
            BLOCK_BYTES as u64,
            "exactly one 4 KiB block"
        );
        let bytes = fs::read(&path).expect("the block reads");
        assert_eq!(bytes, fake_block(9), "the block is the seed's fake data");
        let outcome =
            execute(&dir, RecoveryFlush::SingleBlock, 10).expect("the second flush executes");
        assert_eq!(outcome.bytes_written, BLOCK_BYTES as u64);
        assert_eq!(
            fs::metadata(&path).expect("the length reads").len(),
            BLOCK_BYTES as u64,
            "the baseline stays one block across boots"
        );
        let bytes = fs::read(&path).expect("the block reads");
        assert_eq!(
            bytes,
            fake_block(10),
            "the overwrite landed: the new seed's block"
        );
    }

    #[test]
    fn variant_2_writes_both_rings_with_the_documented_geometry() {
        let dir = scratch("variant-2");
        let outcome = execute(&dir, RecoveryFlush::DoubleRing, 11).expect("the flush executes");
        assert_eq!(outcome.variant, "double-ring");
        assert_eq!(outcome.bytes_written, 2 * BLOCK_BYTES as u64);
        let path = dir.join("recovery-flush-double.bin");
        let bytes = fs::read(&path).expect("the rings read");
        assert_eq!(
            bytes.len(),
            RING_SPACING_BYTES as usize + BLOCK_BYTES,
            "ring two sits RING_SPACING_BYTES past ring one"
        );
        assert_eq!(&bytes[..8], &RING_MAGIC[..], "the ring magic");
        let line = &bytes[HEADER_BYTES..HEADER_BYTES + DATA_LINE_BYTES];
        assert_eq!(
            u64::from_be_bytes(bytes[8..16].try_into().expect("the checksum field")),
            fnv1a(line),
            "the checksum covers the data line"
        );
        assert_eq!(
            u64::from_be_bytes(bytes[16..24].try_into().expect("the sequence field")),
            11,
            "the boot's sequence rides the header"
        );
        assert_eq!(
            u64::from_be_bytes(bytes[24..32].try_into().expect("the length field")),
            DATA_LINE_BYTES as u64,
            "ring one carries the payload length"
        );
        let ring_two = &bytes[RING_SPACING_BYTES as usize..];
        assert_eq!(
            &ring_two[..HEADER_BYTES],
            &bytes[..HEADER_BYTES],
            "the header copy is identical"
        );
        assert!(
            ring_two[HEADER_BYTES..BLOCK_BYTES]
                .iter()
                .all(|&byte| byte == 0),
            "ring two carries no payload"
        );
    }
}
