//! The `nuke` tool: an operator's raw look at a marker store and a
//! deliberate, confirmed reset of it. A testing-time instrument — NOT
//! part of the shipped surface, NOT referenced from any documentation.
//!
//! It prints EVERYTHING in the marker store: the four superblock copies'
//! raw facts (presence, checksum status, sequence, marker state,
//! incarnation) and the single-file compatibility projection if present.
//! With `--set-state` and/or `--set-incarnation` it resets the
//! superblock to the named target — an explicit operator action behind
//! an interactive y/n review (or `--dangerously-skip-review`), printed
//! in full before and after.
//!
//! The law it obeys: a bad checksum on ANY copy is never cleared, never
//! repaired, never fallen back. The reset goes through the real store's
//! fresh format; a rotted store refuses and the operator deletes the
//! marker file instead.

use std::fs;
use std::io::Write as _;
use std::path::{Path, PathBuf};

use clap::Parser;

use lunet_locks_aof::marker::{self, CopyInfo};

#[derive(Parser)]
#[command(
    name = "nuke",
    about = "Print everything in a lifecycle marker store, and optionally \
              reset it to a named state/incarnation (a destructive, \
              confirmed operator action)."
)]
struct Args {
    /// The marker's state path (the single-file projection's path; the
    /// superblock copies live beside it as `<state>.superblock`).
    state: PathBuf,

    /// Reset target state: `running` (the running sentinel, on-disk
    /// `unflushed`), `stopped`, or `flushed`.
    #[arg(long)]
    set_state: Option<String>,

    /// Reset target incarnation. Defaults to the working quorum's
    /// current incarnation when the store resolves one; required when it
    /// does not.
    #[arg(long)]
    set_incarnation: Option<u64>,

    /// Skip the interactive y/n review (the ONLY override).
    #[arg(long)]
    dangerously_skip_review: bool,
}

/// The projection word for an on-disk state code (the adapter's
/// spelling; `running` is the on-disk `unflushed` sentinel).
fn projection_word(code: u32) -> Option<&'static str> {
    match code {
        0 => Some("unflushed"),
        1 => Some("stopped"),
        2 => Some("flushed"),
        _ => None,
    }
}

fn state_code(word: &str) -> Option<(marker::MarkerState, &'static str)> {
    match word {
        "running" => Some((marker::MarkerState::Unflushed, "unflushed")),
        "stopped" => Some((marker::MarkerState::Stopped, "stopped")),
        "flushed" => Some((marker::MarkerState::Flushed, "flushed")),
        _ => None,
    }
}

fn print_copies(copies: &[CopyInfo]) {
    for (slot, copy) in copies.iter().enumerate() {
        if copy.readable == 0 {
            println!("copy {slot}: ABSENT (torn zone; no header bytes)");
            continue;
        }
        let checksum = if copy.valid_checksum == 1 {
            "valid"
        } else {
            "BAD CHECKSUM (the boot read refuses; never cleared, never repaired)"
        };
        let state = projection_word(copy.state).unwrap_or("INVALID STATE CODE");
        println!(
            "copy {slot}: checksum={checksum} sequence={} state={state} incarnation={} checksum=0x{:016x}{:016x}",
            copy.sequence, copy.incarnation, copy.checksum_hi, copy.checksum_lo,
        );
    }
}

fn main() {
    let args = Args::parse();
    let superblock = superblock_path(&args.state);

    println!("marker state: {}", args.state.display());
    println!("superblock copies: {}", superblock.display());
    println!();

    let projection = read_projection(&args.state);
    match &projection {
        Some(line) => println!("projection ({}): {line}", args.state.display()),
        None => println!("projection ({}): absent", args.state.display()),
    }
    println!();

    let geometry = marker::geometry().expect("the marker zone geometry");
    let copies = marker::inspect(&superblock);
    match &copies {
        Ok(copies) => {
            println!(
                "superblock copies ({} copies, {}-byte zones):",
                geometry.copies, geometry.copy_size
            );
            print_copies(copies);
        }
        Err(_) => println!("superblock copies: the marker store file does not exist"),
    }
    println!();

    let copies = copies.unwrap_or_default();
    if args.set_state.is_none() && args.set_incarnation.is_none() {
        return;
    }

    // The reset target: an explicit incarnation, else the working
    // quorum's — the highest-sequence checksum-valid copy's identity.
    // Nothing is guessed beyond that: with no valid copy, the operator
    // names the incarnation.
    let incarnation = match args.set_incarnation {
        Some(incarnation) => incarnation,
        None => copies
            .iter()
            .filter(|copy| copy.readable == 1 && copy.valid_checksum == 1)
            .max_by_key(|copy| copy.sequence)
            .map(|copy| copy.incarnation)
            .unwrap_or_else(|| {
                eprintln!(
                    "nuke: no --set-incarnation given and no valid copy resolves one; \
                     the operator names the identity"
                );
                std::process::exit(2);
            }),
    };
    let (state, word) = match &args.set_state {
        Some(word) => match state_code(word) {
            Some(resolved) => resolved,
            None => {
                eprintln!(
                    "nuke: unknown state {word:?}; the lifecycle states are \
                     running | stopped | flushed"
                );
                std::process::exit(2);
            }
        },
        None => {
            // State kept: the current working quorum's state (or the
            // running sentinel when nothing resolves).
            copies
                .iter()
                .filter(|copy| copy.readable == 1 && copy.valid_checksum == 1)
                .max_by_key(|copy| copy.sequence)
                .and_then(|copy| projection_word(copy.state))
                .and_then(state_code)
                .unwrap_or((marker::MarkerState::Unflushed, "unflushed"))
        }
    };

    println!("reset plan:");
    println!(
        "  superblock copies <- fresh format: incarnation={incarnation} state={word} (4x, forced I/O, verified)"
    );
    println!("  projection        <- \"{incarnation} {word}\\n\"");
    println!();

    // THE LAW at the tool's own gate: a bad checksum on ANY copy is
    // never cleared, never repaired — a reset over a rotted store is a
    // clear. The operator deletes the marker file instead (the recovery
    // path; the projection re-seeds the next boot).
    if copies
        .iter()
        .any(|copy| copy.readable == 1 && copy.valid_checksum == 0)
    {
        eprintln!(
            "nuke: the reset refused: at least one copy has a bad checksum. THE LAW: the \
             store is never cleared, never repaired, never fallen back — delete the \
             marker file to re-seed from the projection."
        );
        std::process::exit(1);
    }

    if !args.dangerously_skip_review {
        print!("proceed with the reset? [y/N] ");
        let _ = std::io::stdout().flush();
        let mut answer = String::new();
        if std::io::stdin().read_line(&mut answer).is_err() || !matches!(answer.trim(), "y" | "Y") {
            println!("reset refused: no confirmation");
            return;
        }
    }

    match marker::format(&superblock, incarnation, state) {
        Ok(()) => {}
        Err(marker::CORRUPT) => {
            eprintln!(
                "nuke: the reset refused: a copy has a bad checksum. THE LAW: the store \
                 is never cleared, never repaired, never fallen back — delete the \
                 marker file to re-seed from the projection."
            );
            std::process::exit(1);
        }
        Err(code) => {
            eprintln!(
                "nuke: the reset refused (FFI code {code}; -1 = the named incarnation \
                 would regress the marker or the state is outside the lifecycle, \
                 -7 = storage)"
            );
            std::process::exit(1);
        }
    }
    write_projection(&args.state, incarnation, word);

    println!();
    println!("wrote:");
    let copies = marker::inspect(&superblock).expect("the store reads after its own write");
    print_copies(&copies);
    println!(
        "projection ({}): \"{} {word}\"",
        args.state.display(),
        incarnation
    );
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
/// fsync, rename, dir-sync (the item08 single-file pattern).
fn write_projection(state: &Path, incarnation: u64, word: &str) {
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
    {
        let mut file = fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temporary)
            .expect("the projection's temp file");
        file.write_all(line.as_bytes()).expect("projection write");
        file.sync_all().expect("projection fsync");
    }
    fs::rename(&temporary, state).expect("projection rename");
    if let Some(dir) = parent.to_str() {
        let _ = std::fs::File::open(dir).and_then(|file| file.sync_all());
    }
}
