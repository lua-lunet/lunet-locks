//! The UDS harness stage scenarios: one genesis node speaks anything
//! (stage 1); a two-node quorum of a three-member cluster stabilizes and
//! serves polite lock traffic, and the third client joins politely
//! (stage 2); the full three-node cluster takes over from a PAUSED holder
//! inside the honest bound and re-enters cleanly (stage 3); three polite
//! contenders started together race one free lock — the two denied
//! contenders must return to the probe cadence and take over through
//! the probe→SET-race path (stage 4). Every stage asserts its invariants
//! on the cluster-wide trace AOF the driver writes.

use lease_sequencer::uds_harness::{Cluster, ClusterConfig, Verdict, parse_line};
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
    SCENARIO_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn scratch(name: &str) -> PathBuf {
    // UDS paths must fit SUN_LEN (104 on macOS): keep the run-dir name
    // short and canonicalize the base.
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/harness");
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
    ids.iter()
        .map(|id| (provisioned(*id), format!("node{id}")))
        .collect()
}

/// The provisioned identity: node k's descriptor id is the packed pair
/// (system k, crash counter 1).
fn provisioned(system: u32) -> u32 {
    (system << 16) | 1
}

fn summarize(verdicts: &[Verdict]) -> String {
    verdicts
        .iter()
        .map(|v| {
            format!(
                "[{}] {} ({})",
                if v.pass { "pass" } else { "fail" },
                v.name,
                v.detail
            )
        })
        .collect::<Vec<_>>()
        .join(";\n")
}

#[test]
fn stage1_single_node_speaks_get_and_set() {
    let _guard = lock_scenarios();
    let config = ClusterConfig::new(
        scratch("uds-stage1"),
        members(&[44, 55]),
        vec![provisioned(44), provisioned(55)],
    )
    .with_clients(vec![("probe1".into(), provisioned(44))]);
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
    let config = ClusterConfig::new(
        scratch("uds-stage2"),
        members(&[44, 55, 66]),
        vec![provisioned(44), provisioned(55)],
    )
    .with_clients(vec![
        ("client1".into(), provisioned(44)),
        ("client2".into(), provisioned(55)),
    ]);
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
    let config = ClusterConfig::new(
        scratch("uds-stage3"),
        members(&[44, 55, 66]),
        vec![provisioned(44), provisioned(55), provisioned(66)],
    )
    .with_clients(vec![
        ("client1".into(), provisioned(44)),
        ("client2".into(), provisioned(55)),
        ("client3".into(), provisioned(66)),
    ]);
    let verdicts = lease_sequencer::uds_harness::stage3(Cluster::launch(config).expect("launch"));
    assert!(
        verdicts.iter().all(|v| v.pass),
        "stage3 verdicts: {}",
        summarize(&verdicts)
    );
}

/// The simultaneous bring-up race: three polite contenders against one
/// free lock. The two denied contenders must withdraw their stakes,
/// re-probe, and take a paused holder's lease through the probe→SET
/// race — the regression for the renewal loop that misread the leader's
/// `granted:false` refusal as a renewal and never probed again.
///
/// Disposition: the sustain, denial, and re-probe verdicts are
/// ordering facts and hold under load. The paused-holder takeover
/// needs a leader that stays stable through a ~1-2 s window after the
/// pause: under a heavily loaded host the in-process cluster's phi
/// detector can churn views for the whole wait (observed 2026-09-15 at
/// sustained load averages 4-6 while the rest of the suite stayed
/// green), and the 20 s liveness bound does not cover a storm that
/// long. The verdict's truth is the successor's op mix (a second set
/// op), which no amount of waiting can fake; re-measure on a quiet
/// host when it goes red alongside a view-change storm.
#[test]
fn stage4_three_clients_race_one_free_lock() {
    let _guard = lock_scenarios();
    let config = ClusterConfig::new(
        scratch("uds-stage4"),
        members(&[44, 55, 66]),
        vec![provisioned(44), provisioned(55), provisioned(66)],
    )
    .with_clients(vec![
        ("client1".into(), provisioned(44)),
        ("client2".into(), provisioned(55)),
        ("client3".into(), provisioned(66)),
    ]);
    let verdicts = lease_sequencer::uds_harness::stage4(Cluster::launch(config).expect("launch"));
    assert!(
        verdicts.iter().all(|v| v.pass),
        "stage4 verdicts: {}",
        summarize(&verdicts)
    );
}

/// The driver-hiccup tolerance: the driver is the cluster's only switch
/// fabric and runs on the test thread, so a scheduling stall of the test
/// thread IS wire silence to every follower — even though every node host
/// stayed up and every heartbeat was emitted on time. The phi detector
/// must not read that manufactured silence as leader death: the observed
/// red runs (item09: a 422 ms frame gap, then view churn 1→16 through the
/// 3 s takeover window, the successors' ops refused not_leader mid-churn)
/// were exactly this — a driver-side stall storming the cluster. The
/// harness's phi timeout knobs must exceed the hiccup a loaded host
/// produces, so the view stays put, the fence stays silent, and the
/// takeover machinery runs on the lease clock, not the churn.
#[test]
fn driver_hiccup_is_not_leader_death() {
    let _guard = lock_scenarios();
    let config = ClusterConfig::new(
        scratch("uds-hiccup"),
        members(&[44, 55, 66]),
        vec![provisioned(44), provisioned(55), provisioned(66)],
    )
    .with_clients(vec![("client1".into(), provisioned(44))]);
    let mut cluster = Cluster::launch(config).expect("launch");
    let ready = cluster.wait_until(8000, |lines| {
        lines.iter().filter_map(|l| parse_line(l)).any(|l| {
            l.from.starts_with("node")
                && l.to.starts_with("node")
                && l.json.get("tag").and_then(|v| v.as_u64()) == Some(4)
        })
    });
    assert!(
        ready,
        "the cluster never stabilized: {}",
        cluster.trace_tail(8)
    );
    cluster.client_start("client1");
    let held = cluster.wait_until(3000, |lines| {
        lines.iter().filter_map(|l| parse_line(l)).any(|l| {
            l.to == "client1" && l.json.get("granted").and_then(|v| v.as_bool()) == Some(true)
        })
    });
    assert!(
        held,
        "the contender never acquired: {}",
        cluster.trace_tail(8)
    );
    let settled = cluster.lines.len();
    let views_before: Vec<u64> = cluster.lines[..settled]
        .iter()
        .filter_map(|l| parse_line(l))
        .filter(|l| l.from.starts_with("node") && l.to.starts_with("node"))
        .filter_map(|l| l.json.get("view").and_then(|v| v.as_u64()))
        .collect();
    let view_before = views_before.last().copied().unwrap_or(0);
    let leader_before = cluster.lines[..settled]
        .iter()
        .rev()
        .find_map(|l| {
            parse_line(l).filter(|p| {
                p.from.starts_with("node") && p.json.get("tag").and_then(|v| v.as_u64()) == Some(4)
            })
        })
        .map(|l| l.from.to_string());

    // THE HICCUP: no polling for 1.8 s — the driver emits and forwards
    // nothing while every node host keeps stepping on its own thread.
    // To the followers this is wire silence (their phi monitors are fed
    // only by the driver's forwarded arrivals); the leases ride their own
    // clocks and expire on schedule, so the successor machinery — if the
    // view survives — is exercised purely by the expiry, never by churn.
    std::thread::sleep(std::time::Duration::from_millis(1800));

    // Recovery: resume the polls; the backlog drains and service resumes.
    let resume_at = lease_sequencer::uds_harness::millis();
    while lease_sequencer::uds_harness::millis() < resume_at + 2500 {
        cluster.poll(lease_sequencer::uds_harness::millis());
        std::thread::sleep(std::time::Duration::from_millis(1));
    }
    let window = &cluster.lines[settled..];
    let parsed: Vec<_> = window.iter().filter_map(|l| parse_line(l)).collect();
    let fences = parsed
        .iter()
        .filter(|l| l.from.starts_with("node") && l.to.starts_with("node"))
        .filter(|l| matches!(l.json.get("tag").and_then(|v| v.as_u64()), Some(5..=7)))
        .count();
    let views_after: Vec<u64> = parsed
        .iter()
        .filter(|l| l.from.starts_with("node") && l.to.starts_with("node"))
        .filter_map(|l| l.json.get("view").and_then(|v| v.as_u64()))
        .collect();
    let max_view = views_after.iter().copied().max().unwrap_or(0);
    let leader_after = parsed
        .iter()
        .rev()
        .find(|l| {
            l.from.starts_with("node") && l.json.get("tag").and_then(|v| v.as_u64()) == Some(4)
        })
        .map(|l| l.from.to_string());
    let serving = parsed.iter().any(|l| {
        l.to == "client1"
            && l.json.get("op").and_then(|v| v.as_str()) == Some("set")
            && l.json.get("granted").and_then(|v| v.as_bool()) == Some(true)
    });
    let mut failures = Vec::new();
    if fences != 0 {
        failures.push(format!("view-change fences through the stall: {fences}"));
    }
    if max_view > view_before {
        failures.push(format!(
            "the view churned: before={view_before} max_after={max_view}"
        ));
    }
    if leader_before.is_none() || leader_after != leader_before {
        failures.push(format!(
            "the leader moved: before={leader_before:?} after={leader_after:?}"
        ));
    }
    if !serving {
        failures.push("the lock never served a granted set after recovery".to_string());
    }
    assert!(
        failures.is_empty(),
        "driver-hiccup invariants broken: {}; tail:\n{}",
        failures.join("; "),
        cluster.trace_tail(8)
    );
}
