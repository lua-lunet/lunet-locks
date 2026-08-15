//! Host-side FFI support for the advisory-lock adapter.
//!
//! The uVRR v0.2.0 rebuild removes the previous alpha adapter surface. What
//! remains here are the host concerns the new core deliberately does not own:
//! the durable recovery nonce file, the Unix wall clock, crash-consistency
//! helpers, and the panic guard / error-code constants for the C ABI. The
//! extern "C" entry points return with the rebuilt adapter.

#![allow(dead_code)]

use std::ffi::OsString;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

const OK: i32 = 0;
const INVALID: i32 = -1;
const CONFIG: i32 = -2;
const CLIENT_JSON: i32 = -4;
const VRR_MESSAGE: i32 = -5;
const TOO_LARGE: i32 = -6;
const SERVICE: i32 = -7;
const PANIC: i32 = -127;

unsafe fn bytes<'a>(len: usize, data: *const u8) -> Result<&'a [u8], i32> {
    if data.is_null() {
        return if len == 0 { Ok(&[]) } else { Err(INVALID) };
    }
    Ok(unsafe { std::slice::from_raw_parts(data, len) })
}

fn unix_millis() -> Result<u64, i32> {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_err(|_| SERVICE)
        .and_then(|duration| u64::try_from(duration.as_millis()).map_err(|_| SERVICE))
}

fn initialize_nonce(path: &Path) -> std::io::Result<bool> {
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(mut file) => {
            file.write_all(b"0\n")?;
            file.sync_all()?;
            sync_parent(path)?;
            Ok(false)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            read_nonce(path)?;
            Ok(true)
        }
        Err(error) => Err(error),
    }
}

fn read_nonce(path: &Path) -> std::io::Result<u64> {
    fs::read_to_string(path)?
        .trim()
        .parse()
        .map_err(|_| std::io::Error::new(std::io::ErrorKind::InvalidData, "invalid nonce"))
}

fn next_nonce(path: &Path) -> std::io::Result<u64> {
    let nonce = read_nonce(path)?
        .checked_add(1)
        .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidData, "nonce overflow"))?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let base = path.file_name().unwrap_or_default();
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let mut temporary = OsString::from(".");
    temporary.push(base);
    temporary.push(format!(".tmp-{}-{unique}", std::process::id()));
    let temporary = parent.join(temporary);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&temporary)?;
    let result = (|| {
        writeln!(file, "{nonce}")?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_parent(path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temporary);
    }
    result.map(|_| nonce)
}

fn sync_parent(path: &Path) -> std::io::Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    sync_dir(parent)
}

// Fsyncing the containing directory after create/rename is a POSIX crash-
// consistency idiom (persist the new directory entry, not just the file's
// data). Windows has no equivalent: `File::open` on a directory fails with
// ERROR_ACCESS_DENIED (std does not set FILE_FLAG_BACKUP_SEMANTICS), and NTFS
// does not require or support an explicit directory fsync for this guarantee
// the way POSIX filesystems do. Other Rust crates with the same durability
// pattern (e.g. `atomicwrites`) no-op this step on Windows for the same
// reason; do the same here rather than fail every nonce write on Windows.
#[cfg(unix)]
fn sync_dir(dir: &Path) -> std::io::Result<()> {
    File::open(dir)?.sync_all()
}

#[cfg(windows)]
fn sync_dir(_dir: &Path) -> std::io::Result<()> {
    Ok(())
}

fn guarded(run: impl FnOnce() -> i32) -> i32 {
    catch_unwind(AssertUnwindSafe(run)).unwrap_or(PANIC)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn state_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "lunet-advisory-lock-{name}-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock is after Unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn recovery_nonces_are_created_then_durably_incremented() {
        let path = state_path("nonce");
        assert!(!initialize_nonce(&path).expect("first boot creates nonce"));
        assert_eq!(fs::read_to_string(&path).unwrap(), "0\n");
        assert_eq!(next_nonce(&path).unwrap(), 1);
        assert_eq!(fs::read_to_string(&path).unwrap(), "1\n");
        assert_eq!(next_nonce(&path).unwrap(), 2);
        assert_eq!(fs::read_to_string(&path).unwrap(), "2\n");
        fs::remove_file(path).unwrap();
    }
}
