//! The lift cursor, durable: the frontier reached — the last written
//! transition's (era, view, slot) — survives the witness process's
//! restart. The series' LKE1 record is 61 fixed bytes and cannot carry
//! the frontier, and the series' tail (kind, ts, lease id) maps back to
//! no frontier soundly, so the cursor lives in its own file beside the
//! series: one frame, the newest content the only content, written with
//! the repo's staged durable write — temp file, fsync, rename, parent
//! fsync — so a torn write never replaces a good cursor.
//!
//! An absent or unreadable frame is the cold-start answer, not a
//! failure: the first ever boot's cursor is nothing, and the first tick
//! lifts the whole tape (the series is empty, so nothing duplicates).

use std::io;
use std::io::Write;
use std::path::Path;

use lunet_advisory_lock::witness::Frontier;

const FILE_NAME: &str = "lift-cursor.bin";
const MAGIC: &[u8; 4] = b"LFC1";
/// magic(4) | era u32 | view u32 | slot u64 | crc32 over the first 20.
const FRAME_LEN: usize = 24;

/// The cursor file's path beside the series. The name collides with
/// nothing: the tape's `{epoch}.aof` names, the series' `ev-*` records
/// and their metafiles all parse elsewhere, and neither sweep touches
/// this name.
pub fn file_path(dir: &Path) -> std::path::PathBuf {
    dir.join(FILE_NAME)
}

/// The durable boot cursor: the frontier the last life's lift reached.
/// An absent file is `None`; a frame that fails the magic, the length
/// or the CRC is `None` too — a file the rename never replaced is not a
/// state to trust.
pub fn load(dir: &Path) -> Option<Frontier> {
    let bytes = std::fs::read(file_path(dir)).ok()?;
    unframe(&bytes)
}

/// The durable cursor store: the staged write (temp file, fsync,
/// rename, parent fsync). A failure removes the temp file and reports;
/// the caller keeps its previous cursor, so the next tick retries the
/// store and the boot cursor never claims work the series lacks.
pub fn store(dir: &Path, frontier: &Frontier) -> io::Result<()> {
    let path = file_path(dir);
    let framed = frame(frontier);
    let base = path.file_name().unwrap_or_default();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    let mut temporary = std::ffi::OsString::from(".");
    temporary.push(base);
    temporary.push(format!(".tmp-{}-{unique}", std::process::id()));
    let temporary = dir.join(temporary);
    let result = (|| {
        let mut file = std::fs::File::create(&temporary)?;
        file.write_all(&framed)?;
        file.sync_all()?;
        std::fs::rename(&temporary, &path)?;
        // POSIX: persist the new directory entry, not just the data.
        std::fs::File::open(dir)?.sync_all()
    })();
    if result.is_err() {
        let _ = std::fs::remove_file(&temporary);
    }
    result
}

fn frame(frontier: &Frontier) -> [u8; FRAME_LEN] {
    let mut buf = [0u8; FRAME_LEN];
    buf[..4].copy_from_slice(MAGIC);
    buf[4..8].copy_from_slice(&frontier.era.to_be_bytes());
    buf[8..12].copy_from_slice(&frontier.view.to_be_bytes());
    buf[12..20].copy_from_slice(&frontier.slot.to_be_bytes());
    let crc = crc32_ieee(&buf[..20]);
    buf[20..24].copy_from_slice(&crc.to_be_bytes());
    buf
}

fn unframe(bytes: &[u8]) -> Option<Frontier> {
    if bytes.len() != FRAME_LEN || &bytes[..4] != MAGIC {
        return None;
    }
    let stored_crc = u32::from_be_bytes(bytes[20..24].try_into().ok()?);
    if crc32_ieee(&bytes[..20]) != stored_crc {
        return None;
    }
    Some(Frontier {
        era: u32::from_be_bytes(bytes[4..8].try_into().ok()?),
        view: u32::from_be_bytes(bytes[8..12].try_into().ok()?),
        slot: u64::from_be_bytes(bytes[12..20].try_into().ok()?),
    })
}

/// The IEEE CRC-32 (the reflected form the LKE1 records and the snapshot
/// frames share), computed bitwise — no table, no dependency.
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_frame_round_trips_through_the_unframe() {
        let frontier = Frontier {
            era: 7,
            view: 3,
            slot: 1_234_567_890,
        };
        assert_eq!(unframe(&frame(&frontier)), Some(frontier));
    }

    #[test]
    fn a_torn_frame_is_not_a_cursor() {
        let mut framed = frame(&Frontier {
            era: 1,
            view: 1,
            slot: 1,
        });
        framed[19] ^= 0xFF;
        assert_eq!(unframe(&framed), None);
        assert_eq!(unframe(&framed[..15]), None);
        assert_eq!(unframe(b"XXXXXXXXXXXXXXXXXXXXXXXXXXXXXX"), None);
    }

    #[test]
    fn the_crc_is_the_ieee_one() {
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
    }
}
