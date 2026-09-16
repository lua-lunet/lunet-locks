//! The Flight Recorder's skaffold tape bin (item12, the corfu
//! inverse-paste shape): a pure-Rust debugger — no Lua, no py, no
//! network. It reads ONE node's flight recording, enforces the commit
//! gate (a recording is readable ONLY by the code as-at its commit), and
//! streams the exact message/event sequence as the `from,to,{json}` tape
//! the scenario playback engine consumes — the same engine the telemetry
//! tape feeds.
//!
//! Usage:
//!
//! ```text
//! cargo run -p lease-sequencer --bin skaffold_flight_tape -- \
//!     --file .tmp/flight/flight-44.jsonl [--node 44] [--from N] [--to N] \
//!     [--kinds wire|internal|<kind>[,...]] [--out PATH]
//! ```
//!
//! Default kinds: the playback surface only (`receive-in`, `request-in`,
//! `emit`). `--kinds internal` adds the node's private story (drives,
//! faults, maybes, journal flushes, markers). The trivial shell filter
//! `... | grep "^66,44,"` works on the plain output, exactly like the
//! telemetry tape's.

use lease_sequencer::flight_tape::{FlightTapeOptions, stream_recording};
use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

fn usage() -> String {
    "usage: skaffold_flight_tape --file PATH [--node N] [--from N] [--to N] \
     [--kinds LIST] [--out PATH]"
        .to_string()
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    let mut file = None;
    let mut options = FlightTapeOptions::default();
    let mut out_path: Option<PathBuf> = None;
    let mut index = 0;
    while index < args.len() {
        let arg = args[index].as_str();
        let Some(value) = args.get(index + 1) else {
            eprintln!("{arg} needs a value; {}", usage());
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
            other => {
                eprintln!("unknown flag {other}; {}", usage());
                return ExitCode::from(2);
            }
        }
        index += 2;
    }
    let Some(file) = file else {
        eprintln!("{}", usage());
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
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("skaffold_flight_tape: {error}");
            ExitCode::from(2)
        }
    }
}
