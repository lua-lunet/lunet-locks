//! Point-to-point RTT probe for the lease-sequencer cluster nodes: for each
//! node's TCP client port it opens one fresh connection per sample, sends a
//! lease `get` poll (the same read the load client's getters send), reads the
//! reply line, and reports per-node min/median/p99 round trips. Std-only and
//! compiled with plain `rustc` (see the `lease_failover_sim` pattern).
//!
//! Single-VM numbers are loopback-shaped and indicative only; the tool is the
//! drill's instrument for the cross-VM and cross-datacentre ladders.
//!
//! Usage: rtt_probe --config cluster.jsonl [--samples N] [--interval-ms N]
//! [--client-port-offset N]. Node endpoints come from the same descriptor the
//! node binary parses; the TCP client port is the peer UDP port + 1000.

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Options {
    config: String,
    samples: usize,
    interval_ms: u64,
    client_port_offset: u16,
}

fn parse_options() -> Options {
    let mut options = Options {
        config: String::new(),
        samples: 20,
        interval_ms: 100,
        client_port_offset: 1000,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        match argv[index].as_str() {
            "--config" => {
                options.config = argv[index + 1].clone();
                index += 2;
            }
            "--samples" => {
                options.samples = argv[index + 1].parse().expect("--samples number");
                index += 2;
            }
            "--interval-ms" => {
                options.interval_ms = argv[index + 1].parse().expect("--interval-ms number");
                index += 2;
            }
            "--client-port-offset" => {
                options.client_port_offset = argv[index + 1]
                    .parse()
                    .expect("--client-port-offset number");
                index += 2;
            }
            other => {
                eprintln!("rtt_probe: unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    if options.config.is_empty() {
        eprintln!(
            "usage: rtt_probe --config cluster.jsonl [--samples N] \
             [--interval-ms N] [--client-port-offset N]"
        );
        std::process::exit(2);
    }
    options
}

struct Node {
    name: String,
    client: String,
}

/// Minimal scalar-field extraction from one descriptor line (the descriptor
/// fields this tool needs are plain unescaped JSON strings and integers).
fn field(line: &str, key: &str) -> Option<String> {
    let needle = format!("\"{key}\":");
    let start = line.find(&needle)? + needle.len();
    let rest = &line[start..];
    let rest = rest.trim_start();
    if let Some(rest) = rest.strip_prefix('"') {
        let end = rest.find('"')?;
        Some(rest[..end].to_string())
    } else {
        let end = rest
            .find(|c: char| !(c.is_ascii_digit() || c == '-'))
            .unwrap_or(rest.len());
        Some(rest[..end].to_string())
    }
}

fn parse_config(path: &str, client_port_offset: u16) -> Vec<Node> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| {
        eprintln!("rtt_probe: read {path} failed: {e}");
        std::process::exit(2);
    });
    let mut nodes = Vec::new();
    for line in text.lines() {
        let line = line.trim_start();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(host) = field(line, "host") else {
            continue;
        };
        let Some(port) = field(line, "port").and_then(|p| p.parse::<u64>().ok()) else {
            continue;
        };
        nodes.push(Node {
            name: field(line, "name").unwrap_or_default(),
            client: format!("{host}:{}", port + u64::from(client_port_offset)),
        });
    }
    nodes
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let index = ((sorted.len() as f64) * fraction).round() as usize;
    sorted[index.min(sorted.len() - 1)]
}

fn probe_once(client: &str) -> Option<u64> {
    let start = Instant::now();
    let mut stream = TcpStream::connect(client).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .ok()?;
    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?;
    let client_id: u64 = 950000 + (std::process::id() as u64);
    let message_id = format!("{:x}-{:x}", now.as_nanos(), std::process::id());
    let json = format!(
        "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{},\"lock_id\":{}}}",
        now.subsec_nanos(),
        0x0DDBA11
    );
    stream.write_all(json.as_bytes()).ok()?;
    stream.write_all(b"\n").ok()?;
    stream.flush().ok()?;
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    reader.read_line(&mut line).ok()?;
    if line.trim().is_empty() {
        return None;
    }
    Some(start.elapsed().as_micros() as u64)
}

fn main() {
    let options = parse_options();
    let nodes = parse_config(&options.config, options.client_port_offset);
    if nodes.is_empty() {
        eprintln!("rtt_probe: no node descriptors in {}", options.config);
        std::process::exit(2);
    }
    let mut results: HashMap<String, Vec<u64>> = HashMap::new();
    for round in 0..options.samples {
        if round > 0 {
            std::thread::sleep(Duration::from_millis(options.interval_ms));
        }
        for node in &nodes {
            if let Some(rt_us) = probe_once(&node.client) {
                results.entry(node.name.clone()).or_default().push(rt_us);
            }
        }
    }
    println!("rtt_probe: {} sample(s) per node, us", options.samples);
    for node in &nodes {
        let empty: Vec<u64> = Vec::new();
        let mut samples = results.get(&node.name).unwrap_or(&empty).clone();
        if samples.is_empty() {
            println!("rtt {} NO-REPLY", node.name);
            continue;
        }
        samples.sort_unstable();
        let count = samples.len();
        let mean = samples.iter().sum::<u64>() / count as u64;
        println!(
            "rtt {} n={} min={} p50={} p90={} p99={} max={} mean={}",
            node.name,
            count,
            samples[0],
            percentile(&samples, 0.50),
            percentile(&samples, 0.90),
            percentile(&samples, 0.99),
            samples[count - 1],
            mean
        );
    }
}
