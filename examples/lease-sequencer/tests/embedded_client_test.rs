//! The embedded lock client (item04): the sequencer host's contender
//! loops driven in-process against a two-node localhost harness — the
//! gap-test's direct-routing pump (no TCP, no sockets: node outputs are
//! delivered to the addressed peer inside the process). The scenario is
//! the exp1/exp2 client-discipline story, asserted through
//! Service-visible lock state (GET probes through the leader), never
//! stdout:
//!
//! 1. both embedded clients boot OFF — no op is ever built, the lock
//!    stays free;
//! 2. SIGUSR2 starts them: the SET race lands deterministically (the
//!    leader-side client's op is submitted and committed first) and the
//!    holder renews on the leader-echoed cadence;
//! 3. SIGUSR1 silences the HOLDER's host: the lease lapses unrenewed
//!    and the OTHER client takes over within TTL+slack with a fresh
//!    take (a new `taken_at_ms` past the silence, never a renewal of the
//!    silenced client's lease);
//! 4. SIGUSR2 restarts the silenced client: it re-enters as a
//!    NON-holder — it probes, sees the incumbent, and polls; the holder
//!    keeps holdership across the window.

use lease_sequencer::client_gate::Mode;
use lease_sequencer::embedded_client::{Action, Config, Runner, Signals};
use lunet_advisory_lock::{NOT_LEADER, Node, OK};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use uuid::Uuid;

const LOCK_ID: u64 = 0x0DDBA12;
const TTL_MS: u64 = 500;
const RENEW_FRACTION: f64 = 0.5;
/// The per-op reply deadline (the host driver's own op deadline).
const OP_DEADLINE_MS: u64 = 1000;
/// The takeover bound: the natural TTL, the poll jitter (<100 ms), and
/// the pump's own slack.
const TAKEOVER_BOUND_MS: u64 = TTL_MS + 700;
const PROBE_CLIENT_ID: u64 = 999_999;
const OUTPUT_SEND: u32 = 1;
const OUTPUT_REPLY: u32 = 2;
const STATE_NORMAL: u32 = 0;
const LEADER_UNKNOWN: u32 = u32::MAX;

fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "lease-sequencer-embedded-{name}-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The two-node localhost harness: node 1 (`a`, the genesis primary)
/// leads, node 2 (`b`) follows. The pump delivers each node's outputs
/// to the addressed peer in-process — the same host loop the sequencer
/// runs, minus the UDP sockets.
struct Harness {
    node_a: Node,
    node_b: Node,
}

impl Harness {
    fn boot(root: &Path) -> Harness {
        let members = "65537:a\x00131073:b";
        let node_a = Node::open(
            members,
            "a",
            root.join("a-state").to_str().expect("path"),
            None,
            0,
            lunet_advisory_lock::PRIMARY_TIMEOUT_MS,
        )
        .expect("node a boots");
        let node_b = Node::open(
            members,
            "b",
            root.join("b-state").to_str().expect("path"),
            None,
            0,
            lunet_advisory_lock::PRIMARY_TIMEOUT_MS,
        )
        .expect("node b boots");
        let mut harness = Harness { node_a, node_b };
        // Genesis: the primary self-promotes on the first tick and the
        // backup adopts the view through the announce. The status's
        // `leader` field names view 0's primary from boot, so the state
        // is the promotion's real signal.
        let deadline = millis() + 2000;
        while harness.node_a.status().state != STATE_NORMAL
            || harness.node_b.status().state != STATE_NORMAL
        {
            assert!(
                millis() < deadline,
                "the two-node harness never settled on a leader"
            );
            let _ = harness.node_a.idle();
            let _ = harness.node_b.idle();
            harness.pump();
            std::thread::sleep(Duration::from_millis(5));
        }
        assert_eq!(harness.node_a.status().state, STATE_NORMAL);
        assert_eq!(harness.node_b.status().state, STATE_NORMAL);
        harness
    }

    /// Drain both nodes' outputs until quiet: kind-1 sends are delivered
    /// to the addressed peer in-process, kind-2 replies are returned in
    /// queue order (the order the leader committed them in — the SET
    /// race's determinism rides it).
    fn pump(&mut self) -> Vec<([u8; 16], Vec<u8>)> {
        let mut replies = Vec::new();
        for _ in 0..1000 {
            // a first, then b: the fixed order keeps the leader's queue
            // order (the SET race's determinism rides it).
            let moved_a =
                Harness::drain_one(65537, &mut self.node_a, &mut self.node_b, &mut replies);
            let moved_b =
                Harness::drain_one(131073, &mut self.node_b, &mut self.node_a, &mut replies);
            if !moved_a && !moved_b {
                return replies;
            }
        }
        panic!("the pump never went quiet");
    }

    /// Drain one node's output queue to quiet: sends are delivered to
    /// the peer in-process, replies are collected. `true` when anything
    /// moved.
    fn drain_one(
        sender_id: u32,
        sender: &mut Node,
        other: &mut Node,
        replies: &mut Vec<([u8; 16], Vec<u8>)>,
    ) -> bool {
        let mut moved = false;
        while let Some(out) = sender.next_output() {
            moved = true;
            if out.kind == OUTPUT_SEND {
                let _ = other.receive(sender_id, &out.bytes);
            } else if out.kind == OUTPUT_REPLY {
                replies.push((out.message_id, out.bytes));
            }
        }
        moved
    }
}

/// One embedded action's submission through host `host_index`'s node:
/// propose locally when it leads, forward to the leader's own node
/// otherwise (the follower host's UDP forward, minus the socket).
fn submit_via(harness: &mut Harness, host_index: usize, action: &Action) -> bool {
    let (own, rc) = if host_index == 0 {
        let rc = harness.node_a.request(action.request.as_bytes());
        (&mut harness.node_a, rc)
    } else {
        let rc = harness.node_b.request(action.request.as_bytes());
        (&mut harness.node_b, rc)
    };
    if rc == OK {
        return true;
    }
    if rc != NOT_LEADER {
        return false;
    }
    let leader = own.status().leader;
    if leader == LEADER_UNKNOWN {
        return false;
    }
    let rc = if leader == 65537 {
        harness.node_a.request(action.request.as_bytes())
    } else {
        harness.node_b.request(action.request.as_bytes())
    };
    rc == OK
}

/// One host-loop round: tick both runners, pump the cluster, and feed
/// every reply to the runners (first claim wins). The replies no runner
/// claims are returned (the probes live there).
fn drive_round(
    harness: &mut Harness,
    runner_a: &mut Runner,
    runner_b: &mut Runner,
) -> Vec<([u8; 16], Vec<u8>)> {
    let now = millis();
    runner_a.tick(now, &mut |action| submit_via(harness, 0, action));
    runner_b.tick(now, &mut |action| submit_via(harness, 1, action));
    let mut unclaimed = Vec::new();
    for (message_id, bytes) in harness.pump() {
        let now = millis();
        if runner_a.absorb(now, &message_id, &bytes, &mut |action| {
            submit_via(harness, 0, action)
        }) {
            continue;
        }
        if runner_b.absorb(now, &message_id, &bytes, &mut |action| {
            submit_via(harness, 65537, action)
        }) {
            continue;
        }
        unclaimed.push((message_id, bytes));
    }
    unclaimed
}

/// Submit one GET probe through the leader (a wire client's read-only
/// verb) and return its message id; the reply surfaces in the rounds'
/// unclaimed lists.
fn submit_probe(harness: &mut Harness, probe_num: &mut u64) -> [u8; 16] {
    *probe_num += 1;
    let message_id = Uuid::new_v4();
    let json = format!(
        "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":{PROBE_CLIENT_ID},\
         \"request_num\":{},\"lock_id\":{LOCK_ID}}}",
        *probe_num
    );
    assert_eq!(
        harness.node_a.request(json.as_bytes()),
        OK,
        "the probe proposes on the leader"
    );
    *message_id.as_bytes()
}

/// Drive rounds, submitting a fresh probe each time, until one probe
/// reply satisfies `want` — Service-visible state, never stdout. The
/// probe cadence paces the host loop: the runners' chase schedules are
/// wall-clock, so real time must be allowed to pass between probes.
fn wait_for(
    harness: &mut Harness,
    runner_a: &mut Runner,
    runner_b: &mut Runner,
    probe_num: &mut u64,
    timeout_ms: u64,
    want: &dyn Fn(&Value) -> bool,
    what: &str,
) -> Value {
    let start = millis();
    loop {
        assert!(
            millis() - start < timeout_ms,
            "timed out waiting for {what}"
        );
        let mid = submit_probe(harness, probe_num);
        let mut seen: Option<Vec<u8>> = None;
        let probe_deadline = millis() + 500;
        loop {
            for (message_id, bytes) in drive_round(harness, runner_a, runner_b) {
                if message_id == mid {
                    seen = Some(bytes);
                }
            }
            if let Some(bytes) = seen {
                let reply: Value = serde_json::from_slice(&bytes).expect("json reply");
                if want(&reply) {
                    return reply;
                }
                break;
            }
            assert!(millis() < probe_deadline, "a probe reply never arrived");
            std::thread::sleep(Duration::from_millis(2));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
}

fn holder_uuid(reply: &Value) -> Uuid {
    Uuid::parse_str(reply["lease"]["holder"].as_str().expect("a holder string"))
        .expect("the holder is a uuid")
}

fn runner_holder_uuid(runner: &Runner, index: usize) -> Uuid {
    Uuid::parse_str(runner.holder(index).expect("the client exists")).expect("holder identity")
}

#[test]
fn embedded_clients_gate_takeover_and_rejoin_as_non_holders() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
