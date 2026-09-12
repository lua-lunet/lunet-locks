//! The AOF console bridge: the standby's TigerBeetle-format AOF series
//! served as the vanilla JS console's admin surface.
//!
//! ```text
//! aof-console-bridge --aof-dir PATH [--bind ADDR:PORT] [--follow]
//! ```
//!
//! Read-only on the series: the standby owns writes. The decode path
//! reuses the existing surfaces end to end — the typed envelope reader,
//! the core's wire parser, and the advisory-lock Service's decode/execute
//! state machine (see `lease_sequencer::bridge` for the contract).

use lease_sequencer::bridge::Server;
use std::path::PathBuf;

fn main() {
    let mut aof_dir: Option<PathBuf> = None;
    let mut bind = String::from("127.0.0.1:8619");
    let mut follow = false;
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--aof-dir" => {
                aof_dir = Some(PathBuf::from(
                    argv.next().unwrap_or_else(|| die("--aof-dir needs a path")),
                ));
            }
            "--bind" => {
                bind = argv.next().unwrap_or_else(|| die("--bind needs ADDR:PORT"));
            }
            "--follow" => follow = true,
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => die(&format!("unknown option {other}")),
        }
    }
    let Some(dir) = aof_dir else {
        print_usage();
        std::process::exit(2);
    };
    if !dir.is_dir() {
        die(&format!("not a directory: {}", dir.display()));
    }

    let server = Server::spawn(&dir, &bind, follow)
        .unwrap_or_else(|e| die(&format!("cannot bind {bind}: {e}")));
    println!(
        "aof-console-bridge: {} on http://{} (series {}, follow {follow})",
        "serving",
        server.port(),
        dir.display()
    );
    loop {
        std::thread::park();
    }
}

fn print_usage() {
    eprintln!(
        "usage: aof-console-bridge --aof-dir PATH [--bind ADDR:PORT] [--follow]"
    );
}

fn die(message: &str) -> ! {
    eprintln!("aof-console-bridge: {message}");
    std::process::exit(2);
}
