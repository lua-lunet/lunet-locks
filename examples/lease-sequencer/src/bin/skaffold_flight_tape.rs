//! The Flight Recorder's skaffold tape bin (item12, the corfu
//! inverse-paste shape): a pure-Rust debugger — no Lua, no py, no
//! network. It reads ONE node's flight recording and streams the exact
//! message/event sequence as the `from,to,{json}` tape the scenario
//! playback engine consumes — the same engine the telemetry tape feeds.
//!
//! Two reads, one commit gate: the default read is the stable
//! `from,to,jsonl` playback slice; `--deep` (or any kind beyond `wire`)
//! is the deep read of the full internal event log, allowed ONLY by the
//! code as-at the recording's commit and loudly refused otherwise.
//!
//! The third read is not a tape at all: `--check-shutdown` reads a run
//! directory or a `snapshot_run.sh` archive and cross-checks the logs
//! against the durable markers (`src/shutdown_check.rs`).

use lease_sequencer::flight_tape::{FlightTapeOptions, stream_recording};
use lease_sequencer::shutdown_check;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

const HELP: &str = "\
skaffold_flight_tape — stream a node's Flight Recorder recording as the replay tape

usage: skaffold_flight_tape --file PATH [--node N] [--from N] [--to N]
                             [--from-any] [--to-any] [--kinds LIST] [--deep]
                             [--out PATH]
       skaffold_flight_tape --check-shutdown RUN_DIR_OR_ARCHIVE [--out PATH]

The output: one CSV line per kept event, `from,to,{json}`, stdout (or --out),
in recording order. The trivial shell filter works on the plain output:

  skaffold_flight_tape --file .tmp/flight/flight-44.jsonl | grep '^66,44,'

The two reads and the commit gate:

  The DEFAULT read is the stable `from,to,jsonl` playback slice (the wire
  kinds receive-in / request-in / emit). It streams from any recording
  whose format this reader knows — cross-commit too, best-effort: the CSV
  shape is the stable surface, the bytes under it are not (mangled lines
  are skipped and counted, never guessed).

  The DEEP read (--deep, or --kinds naming anything beyond `wire`)
  exposes the full internal event log: drive outcomes, faults, maybes,
  journal flushes, markers, stop drains. It is allowed ONLY when the
  reader build's commit equals the recording's commit and is loudly
  refused otherwise — a recording is readable ONLY by the code as-at its
  commit (check out the recording's commit to read it). The deep output
  is NOT a stable format.

from/to derivation rules (the flight event carries better attribution
than the wire header, which names no sender):

  receive-in / request-in   to = the recording node's own id;
                            from = the recorded sender when the record
                            carries it (the node's own host named the
                            peer; exact, never a guess), else '?'.
  emit                      from = the recording node's own id;
                            to = the record's target, else the node's
                            own id.
  internal kinds (drive-in, drive-out, fault, maybe, journal, marker,
  stop-drain, ...)          from = the record's own 'from' when it
                            carries one, else '?'; to = the record's
                            own 'to' when it carries one, else the
                            recording node's own id.

The jsonl payload: the event's detail as one JSON line — kind (the event
kind string), ts_ms, and for the wire kinds frame_hex (the raw bytes
hex-encoded — byte-exact playback needs the original datagram). The deep
read stamps each line's seq.

flags:
  --file PATH       the recording (flight-<node>.jsonl; required unless
                    --check-shutdown)
  --node N          require the recording to be node N's.
  --from N          keep only lines whose derived from == N. '?' lines
                    are dropped unless --from-any.
  --to N            keep only lines whose derived to == N. '?' lines
                    are dropped unless --to-any.
  --from-any        include '?'-from lines when --from is set.
  --to-any          include '?'-to lines when --to is set.
  --kinds LIST      comma-joined union of wire|internal|<event-kind>.
                    Default: the playback surface (wire) only. Naming
                    anything beyond `wire` is a deep read (same-commit
                    only).
  --deep            the full internal event log: every event with its
                    seq, plus the header facts and per-kind counts on
                    stderr. Same-commit only; --kinds does not narrow it.
  --check-shutdown PATH
                    the shutdown-restart consistency check over a run
                    directory (raw files) or a snapshot_run.sh gzip tar
                    archive — both read transparently. Cross-checks the
                    logs against the superblock/single-file markers
                    (every drained-and-flushed stop record must stand on
                    a final marker showing flushed/stopped at that
                    identity, and the reverse), orders the stop-path
                    records (stop-begin, drain, flushed) and flags
                    inversions: records out of sequence, a marker
                    written before the stop began, and work logged after
                    the persist order completed. Exit 0 consistent, 1
                    findings, 2 usage or input errors.
  --out PATH        write the tape (or the check's report) to PATH
                    (else stdout).
  --help            this text.
";

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut file = None;
    let mut options = FlightTapeOptions::default();
    let mut out_path: Option<PathBuf> = None;
    let mut check_shutdown_path: Option<PathBuf> = None;
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        // The valueless flags consume only themselves.
        match arg {
            "--help" | "-h" => {
                print!("{HELP}");
                return ExitCode::SUCCESS;
            }
            "--from-any" => {
                options.from_any = true;
                index += 1;
                continue;
            }
            "--to-any" => {
                options.to_any = true;
                index += 1;
                continue;
            }
            "--deep" => {
                options.deep = true;
                index += 1;
                continue;
            }
            _ => {}
        }
        let Some(value) = args.get(index + 1) else {
            eprintln!("{arg} needs a value; see --help");
            return ExitCode::from(2);
        };
        match arg {
            "--file" => file = Some(PathBuf::from(value)),
            "--node" => match value.parse::<u32>() {
                Ok(node) => options.node = Some(node),
                Err(_) => {
                    eprintln!("--node needs a u32");
                    return ExitCode::from(2);
                }
            },
            "--from" => match value.parse::<u32>() {
                Ok(from) => options.from = Some(from),
                Err(_) => {
                    eprintln!("--from needs a u32");
                    return ExitCode::from(2);
                }
            },
            "--to" => match value.parse::<u32>() {
                Ok(to) => options.to = Some(to),
                Err(_) => {
                    eprintln!("--to needs a u32");
                    return ExitCode::from(2);
                }
            },
            "--kinds" => options.kinds = value.split(',').map(|kind| kind.to_string()).collect(),
            "--out" => out_path = Some(PathBuf::from(value)),
            "--check-shutdown" => check_shutdown_path = Some(PathBuf::from(value)),
            other => {
                eprintln!("unknown flag {other}; see --help");
                return ExitCode::from(2);
            }
        }
        index += 2;
    }

    if let Some(input) = check_shutdown_path {
        return run_check_shutdown(&input, out_path.as_deref());
    }

    let Some(file) = file else {
        eprintln!("see --help");
        return ExitCode::from(2);
    };

    let mut out: Box<dyn Write> = match &out_path {
        Some(path) => match std::fs::File::create(path) {
            Ok(file) => Box::new(std::io::BufWriter::new(file)),
            Err(error) => {
                eprintln!("skaffold_flight_tape: --out create failed ({error})");
                return ExitCode::from(2);
            }
        },
        None => Box::new(std::io::stdout()),
    };
    match stream_recording(&file, &options, &mut out) {
        Ok((lines, mangled)) => {
            let _ = out.flush();
            if out_path.is_none() {
                eprintln!(
                    "skaffold_flight_tape: {} tape lines from {}{}",
                    lines,
                    file.display(),
                    if mangled > 0 {
                        format!(" ({mangled} mangled lines skipped)")
                    } else {
                        String::new()
                    }
                );
            }
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("skaffold_flight_tape: {error}");
            ExitCode::from(2)
        }
    }
}

fn run_check_shutdown(input: &Path, out_path: Option<&Path>) -> ExitCode {
    let mut out: Box<dyn Write> = match out_path {
        Some(path) => match std::fs::File::create(path) {
            Ok(file) => Box::new(std::io::BufWriter::new(file)),
            Err(error) => {
                eprintln!("skaffold_flight_tape: --out create failed ({error})");
                return ExitCode::from(2);
            }
        },
        None => Box::new(std::io::stdout()),
    };
    match shutdown_check::check_shutdown_to(input, &mut out) {
        Ok(shutdown_check::Verdict::Consistent) => ExitCode::SUCCESS,
        Ok(shutdown_check::Verdict::Inconsistent) => ExitCode::from(1),
        Err(error) => {
            eprintln!("skaffold_flight_tape: {error}");
            ExitCode::from(2)
        }
    }
}
