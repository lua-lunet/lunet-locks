use lease_sequencer::bridge::{Server, log_series_inventory};
use signal_hook::consts::SIGUSR1;
use std::path::PathBuf;

fn main() {
    let mut aof_dir: Option<PathBuf> = None;
    let mut log_dir: Option<PathBuf> = None;
    let mut bind = String::from("127.0.0.1:8619");
    let mut follow = false;
    let mut quiet = false;
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--aof-dir" => {
                aof_dir = Some(PathBuf::from(
                    argv.next().unwrap_or_else(|| die("--aof-dir needs a path")),
                ));
            }
            "--log-dir" => {
                log_dir = Some(PathBuf::from(
                    argv.next().unwrap_or_else(|| die("--log-dir needs a path")),
                ));
            }
            "--bind" => {
                bind = argv.next().unwrap_or_else(|| die("--bind needs ADDR:PORT"));
            }
            "--follow" => follow = true,
            "--quiet" => quiet = true,
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
    if let Some(log_dir) = log_dir.as_ref()
        && !log_dir.is_dir()
    {
        die(&format!("not a directory: {}", log_dir.display()));
    }

    if !quiet {
        log_series_inventory(&dir);
    }
    let dir_for_signals = dir.clone();
    let mut signals = signal_hook::iterator::Signals::new([SIGUSR1])
        .unwrap_or_else(|e| die(&format!("cannot register SIGUSR1: {e}")));
    std::thread::spawn(move || {
        for _ in signals.forever() {
            log_series_inventory(&dir_for_signals);
        }
    });

    let server = Server::spawn(&dir, log_dir.as_deref(), &bind, follow)
        .unwrap_or_else(|e| die(&format!("cannot bind {bind}: {e}")));
    println!(
        "aof-console-bridge: serving on http://{} (series {}, log series {}, follow {follow})",
        server.port(),
        dir.display(),
        log_dir
            .as_ref()
            .map_or("—".to_string(), |dir| dir.display().to_string()),
    );
    loop {
        std::thread::park();
    }
}

fn print_usage() {
    eprintln!(
        "usage: aof-console-bridge --aof-dir PATH [--log-dir PATH] [--bind ADDR:PORT] \
         [--follow] [--quiet]\n       SIGUSR1 re-prints the ui-tape cassette inventory"
    );
}

fn die(message: &str) -> ! {
    eprintln!("aof-console-bridge: {message}");
    std::process::exit(2);
}
