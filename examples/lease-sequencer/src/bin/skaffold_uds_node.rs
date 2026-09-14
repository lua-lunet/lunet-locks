//! The harness node host binary: boots ONE uVRR + phi + lock node with the
//! same service wiring the `lease-sequencer` host drives, listening on a
//! request UDS and writing every output to the driver path the caller
//! gives. Transport substitution only — the payloads are the rig's.

use lease_sequencer::uds_harness::{NodeHost, NodeOptions};
use std::path::PathBuf;

fn main() {
    let mut options = NodeOptions::default();
    let mut members: Vec<(u32, String)> = Vec::new();
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        let flag = &argv[index];
        let Some(value) = argv.get(index + 1) else {
            eprintln!("skaffold_uds_node: missing value for {flag}");
            std::process::exit(2);
        };
        match flag.as_str() {
            "--name" => options.name = value.clone(),
            "--members" => {
                members = value
                    .split(',')
                    .filter_map(|entry| {
                        let mut parts = entry.splitn(2, ':');
                        let id = parts.next()?.parse().ok()?;
                        Some((id, parts.next()?.to_string()))
                    })
                    .collect();
            }
            "--request" => options.request_path = PathBuf::from(value),
            "--driver" => options.driver_path = PathBuf::from(value),
            "--state" => options.state = PathBuf::from(value),
            "--log" => options.log_path = Some(PathBuf::from(value)),
            "--heartbeat-ms" => options.heartbeat_ms = value.parse().unwrap_or(10),
            "--election-ms" => options.election_ms = value.parse().unwrap_or(1000),
            "--recovery-ms" => options.recovery_ms = value.parse().unwrap_or(1000),
            "--phi-threshold" => options.phi_threshold = value.parse().unwrap_or(1.0),
            "--phi-safety" => options.phi_safety = value.parse().unwrap_or(2.0),
            "--phi-timeout-min-ms" => options.phi_timeout_min_ms = value.parse().unwrap_or(500),
            "--phi-timeout-max-ms" => options.phi_timeout_max_ms = value.parse().unwrap_or(1000),
            other => {
                eprintln!("skaffold_uds_node: unknown option {other}");
                std::process::exit(2);
            }
        }
        index += 2;
    }
    if options.name.is_empty() || members.is_empty() || options.request_path.as_os_str().is_empty()
    {
        eprintln!(
            "usage: skaffold_uds_node --name NAME --members 44:node44,55:node55 \
             --request PATH --driver PATH --state PATH --log PATH \
             [--heartbeat-ms N] [--election-ms N] [--phi-threshold F] [--phi-safety F]"
        );
        std::process::exit(2);
    }
    options.members = members;
    let mut host = NodeHost::bind(options).unwrap_or_else(|e| {
        eprintln!("skaffold_uds_node: bind failed: {e}");
        std::process::exit(2);
    });
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    host.run(&stop);
}
