//! The three-AOF timeline aligner (item22 M4): reads one or more telemetry
//! AOF directories through the typed envelope reader, classifies every
//! record (Wire / TelemetryTimeoutDecision / TelemetryStateTransition /
//! TelemetryOutbound), merges the traces by their local nanosecond clocks,
//! and prints an aligned timeline excerpt. Unknown markers are rejected
//! loudly — the reader never guesses.
//!
//! ```text
//! aof-timeline <dir>... [--limit N] [--stats]
//! ```

use lunet_locks_aof::envelope::{Marker, Record};
use std::path::{Path, PathBuf};

fn main() {
    let mut dirs: Vec<PathBuf> = Vec::new();
    let mut limit: usize = 40;
    let mut stats = false;
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--limit" => {
                limit = argv
                    .next()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or_else(|| die("--limit needs a number"));
            }
            "--stats" => stats = true,
            other if other.starts_with('-') => die(&format!("unknown option {other}")),
            other => dirs.push(PathBuf::from(other)),
        }
    }
    if dirs.is_empty() {
        die("usage: aof-timeline <aof-dir>... [--limit N] [--stats]");
    }

    let mut timeline: Vec<(String, Record)> = Vec::new();
    let mut per_dir_stats: Vec<(String, [usize; 5])> = Vec::new();
    for dir in &dirs {
        let name = dir
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| dir.display().to_string());
        let mut counts = [0usize; 5];
        for record in read_series(dir) {
            match record.marker {
                Marker::Wire => counts[0] += 1,
                Marker::TelemetryTimeoutDecision => counts[1] += 1,
                Marker::TelemetryStateTransition => counts[2] += 1,
                Marker::TelemetryOutbound => counts[3] += 1,
                Marker::TelemetryIntervalSample => counts[4] += 1,
            }
            timeline.push((name.clone(), record));
        }
        per_dir_stats.push((name, counts));
    }

    timeline.sort_by_key(|(_, record)| record.ns);
    let total = timeline.len();
    let third = limit / 3;
    let excerpt: Vec<&(String, Record)> = if total <= limit {
        timeline.iter().collect()
    } else {
        let mid = (total - third) / 2;
        timeline[..third]
            .iter()
            .chain(timeline[mid..mid + third].iter())
            .chain(timeline[total - third..].iter())
            .collect()
    };

    if stats {
        for (name, counts) in &per_dir_stats {
            println!(
                "# {name} wire={} timeout_decisions={} transitions={} outbound={} interval_samples={}",
                counts[0], counts[1], counts[2], counts[3], counts[4]
            );
        }
    }
    println!(
        "# merged timeline ({total} records; showing {}), sorted by local ns clock",
        excerpt.len()
    );
    for (name, record) in excerpt {
        let payload = match record.marker {
            Marker::TelemetryIntervalSample => String::from_utf8_lossy(&record.payload).into_owned(),
            Marker::Wire => wire_summary(&record.payload),
            _ => String::from_utf8_lossy(&record.payload).into_owned(),
        };
        println!(
            "{:>13}.{:09} {:<8} {}",
            record.ns / 1_000_000_000,
            record.ns % 1_000_000_000,
            name,
            payload
        );
    }
}

/// The 20-byte VRR wire header's summary for a Wire record's payload:
/// `tag(4 BE) era(4) view(4) slot(8)`, then the payload length.
fn wire_summary(payload: &[u8]) -> String {
    if payload.len() < 20 {
        return format!("wire({} bytes, short)", payload.len());
    }
    let tag = u32::from_be_bytes(payload[0..4].try_into().expect("4"));
    let era = u32::from_be_bytes(payload[4..8].try_into().expect("4"));
    let view = u32::from_be_bytes(payload[8..12].try_into().expect("4"));
    let slot = u64::from_be_bytes(payload[12..20].try_into().expect("8"));
    format!(
        "wire tag={tag} era={era} view={view} slot={slot} bytes={}",
        payload.len()
    )
}

/// All records of one node's `.aof` series, oldest file first, decoded
/// through the typed reader over the vendored checksum-validating
/// iterator. An undecodable entry is stated, never guessed.
fn read_series(dir: &Path) -> Vec<Record> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| die(&format!("cannot read {}: {e}", dir.display())))
        .filter_map(|entry| {
            let name = entry.ok()?.file_name().to_string_lossy().to_string();
            name.ends_with(".aof").then(|| dir.join(name))
        })
        .collect();
    files.sort();
    let mut records = Vec::new();
    for file in files {
        let mut it =
            unsafe { lunet_locks_aof::ffi::RawIter::open(file.as_os_str().as_encoded_bytes()) }
                .unwrap_or_else(|e| die(&format!("cannot open {}: code {e}", file.display())));
        while let Ok(Some(entry)) = it.next_entry() {
            match Record::decode(&entry.bytes) {
                Some(record) => records.push(record),
                None => eprintln!(
                    "aof-timeline: undecodable entry in {} (op {})",
                    file.display(),
                    entry.op
                ),
            }
        }
    }
    records
}

fn die(message: &str) -> ! {
    eprintln!("aof-timeline: {message}");
    std::process::exit(2);
}
