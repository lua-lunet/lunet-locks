//! The witness replay: the operator's cold-start recovery over the
//! witness's series.
//!
//! ```text
//! witness-replay --aof-dir PATH [--json]
//! ```
//!
//! A full-cluster cold start re-forms the cluster empty. The witness
//! series is the shutdown certificate and the operator's backup: an
//! ordered shutdown leaves the whole stream durable, and THIS tool is
//! the recovery — the stream replayed through the real `Service::execute`
//! state machine, the recovered lock table reported. No startup path
//! replays anything automatically: the recovery is driven, always.

use lease_sequencer::bridge::replay_series;
use std::path::PathBuf;

fn main() {
    let mut aof_dir: Option<PathBuf> = None;
    let mut json = false;
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--aof-dir" => {
                aof_dir = Some(PathBuf::from(
                    argv.next().unwrap_or_else(|| die("--aof-dir needs a path")),
                ));
            }
            "--json" => json = true,
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

    let (events, state, metrics) = replay_series(&dir);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    if json {
        let locks: Vec<serde_json::Value> =
            state.locks.values().map(|view| view.to_json(now)).collect();
        println!(
            "{}",
            serde_json::json!({
                "locks": locks,
                "derivedEvents": events.len(),
                "records": metrics.records,
                "undecodable": metrics.undecodable,
                "firstNs": metrics.first_ns,
                "lastNs": metrics.last_ns,
            })
        );
        return;
    }

    println!(
        "witness-replay: {} records, {} derived events, stream through ns {:?}",
        metrics.records,
        events.len(),
        metrics.last_ns
    );
    let held = state
        .locks
        .values()
        .filter(|view| {
            view.holder.is_some()
                && view.holder.as_deref() != Some("00000000-0000-0000-0000-000000000000")
                && view.expires_at_ms.is_some_and(|expiry| expiry > now)
        })
        .count();
    println!(
        "witness-replay: {} locks in the recovered table, {} currently held",
        state.locks.len(),
        held
    );
    for (lock_id, view) in &state.locks {
        println!(
            "witness-replay: lock {} name={:?} holder={:?} fencing={} renews={} expiry={:?}",
            lock_id,
            view.name,
            view.holder,
            view.fencing_token,
            view.renew_count,
            view.expires_at_ms
        );
    }
    if state.locks.is_empty() {
        println!("witness-replay: the series records no locks — an empty table is a recovery too");
    }
}

fn print_usage() {
    eprintln!("usage: witness-replay --aof-dir PATH [--json]");
}

fn die(message: &str) -> ! {
    eprintln!("witness-replay: {message}");
    std::process::exit(2);
}
