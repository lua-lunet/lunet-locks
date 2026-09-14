//! The UDS harness stage scenarios: one genesis node speaks anything
//! (stage 1); a two-node quorum of a three-member cluster stabilizes and
//! serves polite lock traffic, and the third client joins politely
//! (stage 2); the full three-node cluster takes over from a PAUSED holder
//! inside the honest bound and re-enters cleanly (stage 3). Every stage
//! asserts its invariants on the cluster-wide trace AOF the driver writes.

use lease_sequencer::uds_harness::{Cluster, ClusterConfig, Verdict};
use std::path::PathBuf;
use std::sync::{Mutex, MutexGuard};
use std::time::{SystemTime, UNIX_EPOCH};

/// One scenario at a time: the stages share the process's scheduler and
/// the RTT bucket is honest only without contention.
///
/// Disposition 2026-09-14: one red run (stage2+stage3) was observed
/// immediately after a full `--release` rebuild burst, with the RTT
/// bucket verdicts starving under scheduler contention; the same
/// binary then passed green three consecutive parallel runs and one
/// solo run on the unchanged tree. Recorded as resolved
/// unreproducible-under-normal-load: the bucket is honest, the
/// contention was external. Re-measure only if a red recurs WITHOUT a
/// concurrent compile storm.
static SCENARIO_LOCK: Mutex<()> = Mutex::new(());

fn lock_scenarios() -> MutexGuard<'static, ()> {
    SCENARIO_LOCK.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn scratch(name: &str) -> PathBuf {
    // UDS paths must fit SUN_LEN (104 on macOS): keep the run-dir name
    // short and canonicalize the base.
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp/harness");
    let base = std::fs::canonicalize(&base).expect("scratch base");
    let dir = base.join(format!(
        "{}p{}n{}",
        name,
        std::process::id() % 100_000,
        ns % 1_000_000_000
    ));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

fn members(ids: &[u32]) -> Vec<(u32, String)> {
    ids.iter().map(|id| (*id, format!("node{id}"))).collect()
}

fn summarize(verdicts: &[Verdict]) -> String {
    verdicts
        .iter()
        .map(|v| format!("[{}] {} ({})", if v.pass { "pass" } else { "fail" }, v.name, v.detail))
        .collect::<Vec<_>>()
        .join(";\n")
}

#[test]
fn stage1_single_node_speaks_get_and_set() {
    let _guard = lock_scenarios();
    let config = ClusterConfig::new(scratch("uds-stage1"), members(&[44, 55]), vec![44, 55])
        .with_clients(vec![("probe1".into(), 44)]);
    let verdicts = lease_sequencer::uds_harness::stage1(Cluster::launch(config).expect("launch"));
    assert!(
        verdicts.iter().all(|v| v.pass),
        "stage1 verdicts: {}",
        summarize(&verdicts)
    );
}

#[test]
fn stage2_two_nodes_of_three_stabilize_and_serve() {
    let _guard = lock_scenarios();
    let config = ClusterConfig::new(scratch("uds-stage2"), members(&[44, 55, 66]), vec![44, 55])
        .with_clients(vec![("client1".into(), 44), ("client2".into(), 55)]);
    let verdicts = lease_sequencer::uds_harness::stage2(Cluster::launch(config).expect("launch"));
    assert!(
        verdicts.iter().all(|v| v.pass),
        "stage2 verdicts: {}",
        summarize(&verdicts)
    );
}

#[test]
fn stage3_pause_holder_takeover() {
    let _guard = lock_scenarios();
    let config = ClusterConfig::new(scratch("uds-stage3"), members(&[44, 55, 66]), vec![44, 55, 66])
        .with_clients(vec![
            ("client1".into(), 44),
            ("client2".into(), 55),
            ("client3".into(), 66),
        ]);
    let verdicts = lease_sequencer::uds_harness::stage3(Cluster::launch(config).expect("launch"));
    assert!(
        verdicts.iter().all(|v| v.pass),
        "stage3 verdicts: {}",
        summarize(&verdicts)
    );
}
