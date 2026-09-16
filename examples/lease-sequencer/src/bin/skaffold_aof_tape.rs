//! The AOF tape streamer (item03): one telemetry AOF directory in,
//! the `from,to,{jsonl}` replay tape out — one CSV line per record, in
//! file order (epoch order = ns order), through the vendored AOF's C
//! ABI iterator (the same FFI the host uses; never the python tool).
//!
//! ```text
//! skaffold_aof_tape --dir PATH [--recorder N] [--from N] [--to N]
//!                   [--from-any] [--to-any] [--kinds KINDS] [--out PATH]
//! ```
//!
//! The acceptance shape: the plain output feeds a shell filter
//! directly — `skaffold_aof_tape --dir D --recorder 99 | grep "^66,99,"`
//! reads the leader-66 → node-99 stream as raw jsonl lines.

use lease_sequencer::tape::{Kind, TapeOptions, stream_dir};
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::process::exit;

const HELP: &str = "\
skaffold_aof_tape — stream a rig telemetry AOF directory as the replay tape

usage: skaffold_aof_tape --dir PATH [--recorder N] [--from N] [--to N]
                         [--from-any] [--to-any] [--kinds KINDS] [--out PATH]

The output: one CSV line per record, `from,to,{jsonl}`, stdout (or --out),
in file order (epoch order = ns order). The trivial shell filter works on
the plain output:

  skaffold_aof_tape --dir .tmp/telemetry/run4/dc3 | grep '^66,99,'

from/to derivation rules (the envelope carries no sender; the endpoints
come from what each record actually carries):

  marker-1 wire      to = the recorder's node id;
                     from = the Commit trailer's leader when the frame
                     carries the 22-byte phi trailer (magic C0 0B),
                     else '?' — the wire header (tag|era|view|slot) names
                     no sender.
  marker-2 decision  from = the recorder (the deciding node), to = the
                     recorder.
  marker-3 transition from = the recorder, to = the recorder.
  marker-4 outbound  from = the recorder, to = the record's target (its
                     JSON 'to' field).
  marker-5 sample    from = the sample's node (the recording monitor's
                     own id), to = the recorder.

The jsonl payload: the full parsed record as one JSON line — ts_ms, ns,
and for wire records tag/era/view/slot, the committed frontier when
present, the lock fields when the payload is a lock verb, the phi fields
when the 22-byte trailer rides the frame, and frame_hex (the raw wire
bytes hex-encoded, trailer included — byte-exact playback needs the
original datagram).

flags:
  --dir PATH        the telemetry AOF directory (required)
  --recorder N      the recording node's id. Default: derived from the
                    first marker-5 sample's 'node' field (the monitor's
                    own id); without samples, recorder-side endpoints
                    render as '?'.
  --from N          keep only lines whose derived from == N. '?' lines
                    are dropped unless --from-any.
  --to N            keep only lines whose derived to == N. '?' lines
                    are dropped unless --to-any.
  --from-any        include '?'-from lines when --from is set.
  --to-any          include '?'-to lines when --to is set.
  --kinds KINDS     a comma/space-joined union of
                    wire|decision|transition|outbound|sample|all.
                    Default: all.
  --out PATH        write the tape to PATH (else stdout).
  --help            this text.
";

fn main() {
    let mut argv = std::env::args().skip(1);
    let mut options = TapeOptions::default();
    let mut dir: Option<PathBuf> = None;
    let mut out: Option<PathBuf> = None;
    let mut kind_text = String::new();
    while let Some(arg) = argv.next() {
        let mut value = |name: &str| {
            argv.next().unwrap_or_else(|| {
                eprintln!("skaffold_aof_tape: missing value for {name}");
                exit(2);
            })
        };
        match arg.as_str() {
            "--help" | "-h" => {
                print!("{HELP}");
                exit(0);
            }
            "--dir" => dir = Some(PathBuf::from(value("--dir"))),
            "--recorder" => {
                options.recorder = Some(
                    value("--recorder")
                        .parse()
                        .unwrap_or_else(|_| die("--recorder needs a node id (u32)")),
                );
            }
            "--from" => {
                options.from = Some(
                    value("--from")
                        .parse()
                        .unwrap_or_else(|_| die("--from needs a node id (u32)")),
                );
            }
            "--to" => {
                options.to = Some(
                    value("--to")
                        .parse()
                        .unwrap_or_else(|_| die("--to needs a node id (u32)")),
                );
            }
            "--from-any" => options.from_any = true,
            "--to-any" => options.to_any = true,
            "--kinds" => kind_text.push_str(&value("--kinds")),
            "--out" => out = Some(PathBuf::from(value("--out"))),
            other => die(&format!("unknown option {other} (see --help)")),
        }
    }
    options.kinds = parse_kinds(&kind_text);
    let Some(dir) = dir else {
        die("missing --dir (see --help)");
    };
    if !dir.is_dir() {
        die(&format!("--dir {} is not a directory", dir.display()));
    }
    let stdout = std::io::stdout();
    let mut sink: Box<dyn Write> = match &out {
        Some(path) => Box::new(
            std::fs::File::create(path)
                .unwrap_or_else(|e| die(&format!("cannot create {}: {e}", path.display()))),
        ),
        None => Box::new(BufWriter::new(stdout.lock())),
    };
    let counts = match stream_dir(&dir, &options, sink.as_mut()) {
        Ok(counts) => counts,
        Err(e) => die(&format!("{e}")),
    };
    if out.is_some() {
        eprintln!(
            "skaffold_aof_tape: {} -> {} records={} lines={} undecodable={} unnamed={} filtered={}",
            dir.display(),
            out.unwrap().display(),
            counts.records,
            counts.lines,
            counts.undecodable,
            counts.unnamed,
            counts.filtered
        );
    }
}

/// The --kinds union: comma/space joined, `all` expands to every kind.
fn parse_kinds(text: &str) -> Vec<Kind> {
    let mut kinds = Vec::new();
    for token in text.split([',', ' ', '\t']) {
        match token {
            "" => continue,
            "all" => {
                kinds = Kind::all();
                break;
            }
            other => match Kind::parse(other) {
                Some(kind) => {
                    if !kinds.contains(&kind) {
                        kinds.push(kind);
                    }
                }
                None => die(&format!("--kinds: unknown kind {other} (see --help)")),
            },
        }
    }
    kinds
}

fn die(message: &str) -> ! {
    eprintln!("skaffold_aof_tape: {message}");
    exit(2);
}
