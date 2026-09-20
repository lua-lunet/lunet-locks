//! The `lunet_locks_nuke` admin tool: an operator's raw look at a marker
//! store and a deliberate, confirmed reset of it. A testing-time
//! instrument — NOT part of the shipped surface, NOT referenced from any
//! documentation.
//!
//! THE OPERATOR'S LAW (plan item25.24, confirmed):
//!
//! - The tool is EXPLICIT operator intent, outside the never-self-heal
//!   rule: a commanded write over a corrupt block is the operator's own
//!   call (hand-deleting the marker file instead would destroy strictly
//!   more, projection included).
//! - It NEVER panics, including on a corrupt store: the corruption is
//!   shown, labelled, and every failure is a one-line stderr message
//!   with a nonzero exit.
//! - Default (no write args): print-only what is there — a corrupt
//!   store gets per-copy `BAD CHECKSUM` rows and an overall corrupt
//!   verdict line, and exit 0 (the investigation succeeded).
//! - Overwrite: show the dump first (corrupt labelled), prompt on
//!   STDERR (N/y, default NO), act, exit success — and NO reprint of
//!   what it wrote (the operator re-runs with no args to confirm).
//! - `--dangerously-skip-review` alone is non-interactive (the flag IS
//!   the consent); `-q/--quiet` is additionally quiet (stdout minimal,
//!   errors still on stderr; it does NOT suppress the prompt).
//! - The write argument is the stringified state NAME — resolved via
//!   the shared const tables (`marker::STATE_NAMES`/`state_name`) —
//!   never a number.
//! - GNU unix conventions: stdout = data, stderr = errors; 0 success
//!   (a declined prompt is not an error, POSIX `rm -i` style); 2 usage
//!   (clap's convention too); 66 (BSD sysexits `EX_NOINPUT`) not-found;
//!   1 any real failure. Corrupt-copy garbage never prints as fake
//!   decimal facts — hex only.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use clap::Parser;

use lunet_locks_aof::marker::{self, CopyInfo};

const PREFIX: &str = "lunet_locks_nuke";
const EXIT_FAILURE: i32 = 1;
const EXIT_USAGE: i32 = 2;
const EXIT_NOINPUT: i32 = 66;

/// The documented alias for the on-disk running sentinel (`unflushed`):
/// the contract's word for the state the process operates under.
const RUNNING_ALIAS: &str = "running";

#[derive(Parser)]
#[command(
    name = PREFIX,
    about = "Print everything in a lifecycle marker store (corruption \
              labelled), and optionally reset it — a destructive, \
              confirmed operator action."
)]
struct Args {
    /// The marker's state path (the single-file projection's path; the
    /// superblock copies live beside it as `<state>.superblock`).
    state: PathBuf,

    /// Reset target state: `running` (the on-disk `unflushed` sentinel),
    /// `unflushed`, `stopped`, or `flushed`.
    #[arg(long)]
    set_state: Option<String>,

    /// Reset target incarnation. Defaults to the working quorum's
    /// current incarnation when the store resolves one; required when
    /// it does not.
    #[arg(long)]
    set_incarnation: Option<u64>,

    /// Skip the interactive y/n review (the ONLY override; the flag IS
    /// the consent).
    #[arg(long)]
    dangerously_skip_review: bool,

    /// Suppress informational stdout (the dump and the reset plan).
    /// Errors still go to stderr; the interactive prompt is NOT
    /// suppressed.
    #[arg(short = 'q', long)]
    quiet: bool,
}

fn main() -> std::process::ExitCode {
    let args = Args::parse();
    std::process::ExitCode::from(run(&args) as u8)
}

fn run(args: &Args) -> i32 {
    let writing = args.set_state.is_some() || args.set_incarnation.is_some();
    let superblock = superblock_path(&args.state);

    // Not-found (print-only): neither the projection nor the superblock
    // exists — a one-line stderr error, EX_NOINPUT, no stdout data.
    // (A write over a fresh path is the store's seeding path: the fresh
    // format creates the copies.)
    if !writing && !args.state.exists() && !superblock.exists() {
        eprintln!(
            "{PREFIX}: {}: no such file or directory",
            args.state.display()
        );
        return EXIT_NOINPUT;
    }

    let geometry = match marker::geometry() {
        Ok(geometry) => geometry,
        Err(err) => {
            eprintln!("{PREFIX}: the marker zone geometry failed: {err}");
            return EXIT_FAILURE;
        }
    };

    if !args.quiet {
        print_dump(args, &superblock, geometry);
    }

    if !writing {
        return 0;
    }

    let copies = marker::inspect(&superblock).unwrap_or_default();

    // The reset target: an explicit incarnation, else the working
    // quorum's — the highest-sequence checksum-valid copy's identity.
    // Nothing is guessed beyond that: with no valid copy, the operator
    // names the incarnation.
    let incarnation = match args.set_incarnation {
        Some(incarnation) => incarnation,
        None => {
            let working = copies
                .iter()
                .filter(|copy| copy.readable == 1 && copy.valid_checksum == 1)
                .max_by_key(|copy| copy.sequence)
                .map(|copy| copy.incarnation);
            match working {
                Some(incarnation) => incarnation,
                None => {
                    eprintln!(
                        "{PREFIX}: no --set-incarnation given and no valid copy resolves one; \
                         the operator names the identity"
                    );
                    return EXIT_USAGE;
                }
            }
        }
    };

    let state = match &args.set_state {
        Some(name) => match resolve_state_name(name) {
            Some(state) => state,
            None => {
                eprintln!(
                    "{PREFIX}: unknown state {name:?}; the lifecycle states are \
                     running | unflushed | stopped | flushed"
                );
                return EXIT_USAGE;
            }
        },
        // State kept: the working quorum's state (or the running
        // sentinel when nothing resolves).
        None => copies
            .iter()
            .filter(|copy| copy.readable == 1 && copy.valid_checksum == 1)
            .max_by_key(|copy| copy.sequence)
            .and_then(|copy| marker::MarkerState::from_code(copy.state))
            .unwrap_or(marker::MarkerState::Unflushed),
    };
    let word = match marker::state_name(state.code()) {
        Some(word) => word,
        None => {
            eprintln!("{PREFIX}: the target state has no name in the shared table");
            return EXIT_FAILURE;
        }
    };

    if !args.quiet {
        println!("reset plan:");
        println!(
            "  superblock copies <- fresh format: incarnation={incarnation} state={word} \
             (4x, forced I/O, verified)"
        );
        println!("  projection        <- \"{incarnation} {word}\\n\"");
        println!();
    }

    if !args.dangerously_skip_review {
        // The prompt lives on STDERR (POSIX `rm -i`): stdout carries
        // data only. EOF, an empty line, or anything but y/Y declines —
        // the default is NO, and a decline is not an error.
        eprint!("{PREFIX}: proceed with the reset? [y/N] ");
        let _ = std::io::stderr().flush();
        let mut answer = String::new();
        if std::io::stdin().read_line(&mut answer).is_err() || !matches!(answer.trim(), "y" | "Y") {
            eprintln!("{PREFIX}: aborted: not confirmed");
            return 0;
        }
    }

    // The write: the operator's explicit intent. No rotted-store
    // refusal — a commanded reset over a corrupt block IS the command
    // (the fresh format reads nothing first; the boot-path read/write
    // refusals are untouched). Every failure is a one-line stderr
    // message + exit 1; NO reprint of what it wrote.
    if let Err(code) = marker::format(&superblock, incarnation, state) {
        eprintln!(
            "{PREFIX}: the reset failed (FFI code {code}; -1 = the named incarnation \
             would regress the marker or the state is outside the lifecycle, \
             -7 = storage)"
        );
        return EXIT_FAILURE;
    }
    if let Err(message) = write_projection(&args.state, incarnation, word) {
        eprintln!("{PREFIX}: the projection write failed: {message}");
        return EXIT_FAILURE;
    }
    0
}

/// The `--set-state` argument resolved through the shared const tables:
/// any `marker::STATE_NAMES` spelling is accepted, plus the documented
/// alias `running` for the on-disk running sentinel (`unflushed`). No
/// private lookup table — the tool consumes the one shared table.
fn resolve_state_name(name: &str) -> Option<marker::MarkerState> {
    if name == RUNNING_ALIAS {
        return Some(marker::MarkerState::Unflushed);
    }
    let index = marker::STATE_NAMES.iter().position(|word| *word == name)?;
    marker::MarkerState::from_code(index as u32)
}

fn print_dump(args: &Args, superblock: &Path, geometry: marker::Geometry) {
    println!("marker state: {}", args.state.display());
    println!("superblock copies: {}", superblock.display());
    println!();
    match read_projection(&args.state) {
        Some(line) => println!("projection ({}): {line}", args.state.display()),
        None => println!("projection ({}): absent", args.state.display()),
    }
    println!();
    match marker::inspect(superblock) {
        Ok(copies) => {
            println!(
                "superblock copies ({} copies, {}-byte zones):",
                geometry.copies, geometry.copy_size
            );
            print_copies(superblock, &copies, geometry);
            println!();
            println!("{}", verdict(&copies));
        }
        Err(_) => {
            println!("superblock copies: the marker store file does not exist");
            println!("verdict: ABSENT (no marker store file)");
        }
    }
    println!();
}

fn print_copies(superblock: &Path, copies: &[CopyInfo], geometry: marker::Geometry) {
    let raw = fs::read(superblock).ok();
    for (slot, copy) in copies.iter().enumerate() {
        if copy.readable == 0 {
            println!("copy {slot}: ABSENT (torn zone; no header bytes)");
            continue;
        }
        if copy.valid_checksum == 0 {
            // Corrupt garbage is never reinterpreted into fake decimal
            // sequence/incarnation facts: the raw zone head prints as
            // hex, nothing else.
            let hex = raw
                .as_deref()
                .and_then(|bytes| zone_head_hex(bytes, slot, geometry))
                .unwrap_or_else(|| "<unreadable>".to_owned());
            println!("copy {slot}: CORRUPT (BAD CHECKSUM) raw=0x{hex}…");
            continue;
        }
        let state = match marker::state_name(copy.state) {
            Some(word) => word.to_owned(),
            None => "INVALID STATE CODE".to_owned(),
        };
        let state_string = raw
            .as_deref()
            .map(|bytes| on_block_state_string(bytes, slot, geometry))
            .unwrap_or_else(|| "<unreadable>".to_owned());
        println!(
            "copy {slot}: checksum=valid sequence={} state={state} state_string=\"{state_string}\" \
             incarnation={} checksum=0x{:016x}{:016x}",
            copy.sequence, copy.incarnation, copy.checksum_hi, copy.checksum_lo,
        );
    }
}

/// The overall verdict the law demands: the store's state SAID in one
/// line, computed from the copies.
fn verdict(copies: &[CopyInfo]) -> String {
    let corrupt: Vec<usize> = copies
        .iter()
        .enumerate()
        .filter(|(_, copy)| copy.readable == 1 && copy.valid_checksum == 0)
        .map(|(slot, _)| slot)
        .collect();
    if !corrupt.is_empty() {
        let named = corrupt
            .iter()
            .map(|slot| format!("copy {slot}"))
            .collect::<Vec<_>>()
            .join(", ");
        let plural = if corrupt.len() == 1 { "" } else { "s" };
        return format!("verdict: CORRUPT ({named}: bad checksum{plural})");
    }
    let readable: Vec<&CopyInfo> = copies.iter().filter(|copy| copy.readable == 1).collect();
    if readable.is_empty() {
        return "verdict: ABSENT (no readable copy)".to_owned();
    }
    let unanimous = readable.len() == copies.len()
        && readable.windows(2).all(|pair| {
            pair[0].sequence == pair[1].sequence
                && pair[0].state == pair[1].state
                && pair[0].incarnation == pair[1].incarnation
        });
    if unanimous {
        "verdict: OK".to_owned()
    } else {
        "verdict: TORN (working copies disagree)".to_owned()
    }
}

/// The zone head's hex dump (the first 16 bytes): the corrupt copy's
/// raw bytes are shown, never its garbage fields reinterpreted as
/// decimal facts.
fn zone_head_hex(bytes: &[u8], slot: usize, geometry: marker::Geometry) -> Option<String> {
    let start = geometry.copy_size.checked_mul(slot)?;
    let head = bytes.get(start..)?.iter().take(16);
    let mut hex = String::with_capacity(16 * 2);
    for byte in head {
        use std::fmt::Write as _;
        let _ = write!(hex, "{byte:02x}");
    }
    Some(hex)
}

/// The on-block state string (the fixed-width, space-padded name at the
/// store's state-string offset): the same bytes a raw hexdump reads.
fn on_block_state_string(bytes: &[u8], slot: usize, geometry: marker::Geometry) -> String {
    let start = match geometry
        .copy_size
        .checked_mul(slot)
        .and_then(|zone| zone.checked_add(marker::state_string_offset()))
    {
        Some(start) => start,
        None => return "<unreadable>".to_owned(),
    };
    let end = start + marker::STATE_STRING_LEN;
    match bytes.get(start..end) {
        Some(zone) => String::from_utf8_lossy(zone).trim_end().to_owned(),
        None => "<unreadable>".to_owned(),
    }
}

fn superblock_path(state: &Path) -> PathBuf {
    let mut os = state.as_os_str().to_os_string();
    os.push(".superblock");
    PathBuf::from(os)
}

fn read_projection(state: &Path) -> Option<String> {
    fs::read_to_string(state)
        .ok()
        .map(|text| text.trim_end().to_string())
}

/// The projection write with the adapter's durability discipline:
/// fsync, rename, dir-sync (the item08 single-file pattern). Every
/// failure surfaces as an error — no panic, no expect.
fn write_projection(state: &Path, incarnation: u64, word: &str) -> Result<(), String> {
    let parent = state.parent().unwrap_or_else(|| Path::new("."));
    let base = state.file_name().unwrap_or_default();
    let unique = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    let temporary = parent.join(format!(
        ".{}.tmp-{}-{unique}",
        base.to_string_lossy(),
        std::process::id()
    ));
    let line = format!("{incarnation} {word}\n");
    if let Err(err) = (|| -> std::io::Result<()> {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)?;
        file.write_all(line.as_bytes())?;
        file.sync_all()?;
        Ok(())
    })() {
        let _ = fs::remove_file(&temporary);
        return Err(format!("{}: {err}", temporary.display()));
    }
    if let Err(err) = fs::rename(&temporary, state) {
        let _ = fs::remove_file(&temporary);
        return Err(format!(
            "{} -> {}: {err}",
            temporary.display(),
            state.display()
        ));
    }
    if let Some(dir) = parent.to_str() {
        let _ = fs::File::open(dir).and_then(|file| file.sync_all());
    }
    Ok(())
}

#[cfg(test)]
mod zone_head_hex_test {
    use super::*;

    /// The defensive test for the CI clippy fix: the rendered head is
    /// the plain two-hex-digit-per-byte form, unchanged by the rewrite.
    #[test]
    fn renders_two_hex_digits_per_byte() {
        let geometry = marker::Geometry {
            copies: 4,
            copy_size: 256,
        };
        let mut bytes = vec![0u8; 256];
        bytes[..3].copy_from_slice(&[0x00, 0x0a, 0xff]);
        assert_eq!(
            zone_head_hex(&bytes, 0, geometry).as_deref(),
            Some("000aff00000000000000000000000000")
        );
        // An empty slice renders an empty head: the tool's dump shape.
        assert_eq!(zone_head_hex(&[], 0, geometry), Some(String::new()));
    }
}
