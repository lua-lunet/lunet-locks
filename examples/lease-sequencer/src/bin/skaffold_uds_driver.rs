//! The harness driver binary: boots the node hosts (in-process or as
//! `skaffold_uds_node` child processes), wires every peer and client
//! message through the cluster-wide trace AOF, and runs one scenario
//! (stage1 | stage2 | stage3) with its asserted invariants. Every scenario
//! step prints a `[pass]`/`[fail]` line; exit 0 only when all pass.

use lease_sequencer::uds_harness::{
    Cluster, ClusterConfig, print_verdicts, stage1, stage2, stage3,
};
use std::path::PathBuf;

fn parse_members(text: &str) -> Vec<(u32, String)> {
    text.split(',')
        .filter_map(|entry| {
            let mut parts = entry.splitn(2, ':');
            let id = parts.next()?.parse().ok()?;
            Some((id, parts.next()?.to_string()))
        })
        .collect()
}

fn parse_ids(text: &str) -> Vec<u32> {
    text.split(',').filter_map(|id| id.parse().ok()).collect()
}

fn parse_clients(text: &str) -> Vec<(String, u32)> {
    text.split(',')
        .filter_map(|entry| {
            let mut parts = entry.splitn(2, ':');
            let name = parts.next()?.to_string();
            Some((name, parts.next()?.parse().ok()?))
        })
        .collect()
}

fn parse_num(text: &str) -> u64 {
    text.strip_prefix("0x")
        .and_then(|hex| u64::from_str_radix(hex, 16).ok())
        .or_else(|| text.parse().ok())
        .unwrap_or(0)
}

fn main() {
    let mut run_dir = PathBuf::new();
    let mut members_text = String::new();
    let mut boot_text = String::new();
    let mut clients_text = String::new();
    let mut scenario = String::from("stage3");
    let mut node_bin: Option<PathBuf> = None;
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        let flag = &argv[index];
        let Some(value) = argv.get(index + 1) else {
            eprintln!("skaffold_uds_driver: missing value for {flag}");
            std::process::exit(2);
        };
        match flag.as_str() {
            "--run-dir" => run_dir = PathBuf::from(value),
            "--members" => members_text = value.clone(),
            "--boot" => boot_text = value.clone(),
            "--clients" => clients_text = value.clone(),
            "--scenario" => scenario = value.clone(),
            "--node-bin" => node_bin = Some(PathBuf::from(value)),
            other => {
                eprintln!("skaffold_uds_driver: unknown option {other}");
                std::process::exit(2);
            }
        }
        index += 2;
    }
    if run_dir.as_os_str().is_empty() || members_text.is_empty() {
        eprintln!(
            "usage: skaffold_uds_driver --run-dir PATH --members 44:node44,55:node55,66:node66 \
             [--boot 44,55,66] [--clients client1:44,client2:55,client3:66] \
             [--scenario stage1|stage2|stage3] [--node-bin PATH]"
        );
        std::process::exit(2);
    }
    let members = parse_members(&members_text);
    let boot = if boot_text.is_empty() {
        members.iter().map(|(id, _)| *id).collect()
    } else {
        parse_ids(&boot_text)
    };
    let clients = if clients_text.is_empty() {
        Vec::new()
    } else {
        parse_clients(&clients_text)
    };
    let config = ClusterConfig::new(run_dir, members, boot).with_clients(clients);
    let cluster = match Cluster::launch(config) {
        Ok(cluster) => cluster,
        Err(error) => {
            eprintln!("skaffold_uds_driver: launch failed: {error}");
            std::process::exit(2);
        }
    };
    let verdicts = match scenario.as_str() {
        "stage1" => stage1(cluster),
        "stage2" => stage2(cluster),
        "stage3" => stage3(cluster),
        other => {
            eprintln!("skaffold_uds_driver: unknown scenario {other}");
            std::process::exit(2);
        }
    };
    let all = print_verdicts(&verdicts);
    std::process::exit(if all { 0 } else { 1 });
}
