//! deleteme_wedge — in-process reproduction of the P2 wedge
//! (local-softball2-2026-09-17): leader TERM + same-boot-line restart
//! never converges while the survivors sit fenced at the dead primary's
//! next view.
//!
//! Shape (mirrors the recorded run):
//!   1. 3 genesis voters; node 1 self-promotes at view 0; the followers'
//!      bootstrap window forces view 1; node 2 (primary of view 1) wins
//!      and serves.
//!   2. P2: node 2 is dropped from the fabric (wire silence) mid-service.
//!   3. The survivors' phi proxy fires on the learned sketch and forces
//!      the view up one step per fire — the churn that walked 34 → 37 in
//!      the real run. The walk lands on the first view whose primary is
//!      the dead node; the fence quorum closes, each survivor records its
//!      evidence ONCE, the DoViewChange datagrams to the dead primary are
//!      lost, and the attempt stalls.
//!   4. Phase A (the host as-is): the viewchange poll drives
//!      `leader_timeout()` (an ordinary tick). The core cannot advance a
//!      `ViewChange`-status node on a tick (ext/uvrr-core
//!      src/replica/mod.rs:1546-1547), and the host's toggle latched on
//!      the VIEW_CHANGE entry (examples/lease-sequencer/src/main.rs
//!      timers: `suspect` every tick while state==VIEW_CHANGE), so
//!      phi_step never re-fires. Expected: NO convergence.
//!   5. Phase B (the missing step): the same run, but the poll drives the
//!      §14.2 forced view (`force_view(era, view + 1)`) in the limbo.
//!      Expected: convergence — a NEW attempt re-broadcasts its fence,
//!      the other survivor joins and votes, the new primary installs, and
//!      the restarted node is dragged in through the StartView gap ruling
//!      (§13.1 step 5 fetch).

use lunet_advisory_lock::{Node, NodeStatus};
use std::collections::VecDeque;

const TICK_MS: u64 = 6;
const POLL_MS: u64 = 150;
const STALL_MS: u64 = 20;
const WATCH_MS: u64 = 500;

const STATE_NORMAL: u32 = 0;
const STATE_VIEW_CHANGE: u32 = 1;
const STATE_RECOVERING: u32 = 2;

const ROOT: &str = "/Users/Shared/lua-lunet/lunet-locks/.tmp/deleteme-wedge";

type Delivery = (u32, u32, Vec<u8>);

struct Host {
    node: Node,
    /// The (era, leader) the phi proxy watches, with the watch's birth.
    watch: Option<((u32, u32), u64)>,
    /// The proxy's last fire.
    last_fire: u64,
    last_poll: u64,
    /// The last Commit datagram this node emitted or accepted.
    last_commit_ms: u64,
}

impl Host {
    fn status(&self) -> NodeStatus {
        self.node.status()
    }

    fn own(&self) -> u32 {
        self.node.own_id()
    }

    fn drain(&mut self, queue: &mut VecDeque<Delivery>, now: u64) {
        while let Some(out) = self.node.next_output() {
            if out.kind == 1 {
                if out.bytes.len() >= 21
                    && u32::from_be_bytes(out.bytes[0..4].try_into().unwrap()) == 4
                {
                    self.last_commit_ms = now;
                }
                queue.push_back((self.own(), out.to, out.bytes));
            }
            // Replies (kind 2) are the client surface; not exercised here.
        }
    }
}

struct Fabric {
    now: u64,
    /// Live routing: dead nodes are absent — wire silence.
    live: Vec<u32>,
    hosts: Vec<Host>,
    queue: VecDeque<Delivery>,
}

impl Fabric {
    fn pump(&mut self) {
        while let Some((from, to, bytes)) = self.queue.pop_front() {
            if !self.live.contains(&to) {
                continue;
            }
            let index = match self.hosts.iter().position(|h| h.own() == to) {
                Some(index) => index,
                None => continue,
            };
            let _ = self.hosts[index].node.receive(from, &bytes);
            self.hosts[index].drain(&mut self.queue, self.now);
        }
    }
}

fn open(members: &str, own: &str, state: &str) -> Node {
    Node::open(members, own, state, None, 0).expect("node open")
}

fn state_path(id: u32) -> String {
    format!("{ROOT}/n{id}.state")
}

fn snapshot(fabric: &Fabric) -> Vec<(u32, u32, u32, u32)> {
    fabric
        .hosts
        .iter()
        .map(|h| {
            let s = h.status();
            (h.own(), s.state, s.view, s.leader)
        })
        .collect()
}

fn main() {
    let _ = std::fs::remove_dir_all(ROOT);
    std::fs::create_dir_all(ROOT).expect("scratch root");
    let members = ["1:n1", "2:n2", "3:n3"].join("\0");
    let mut fabric = Fabric {
        now: 0,
        live: vec![1, 2, 3],
        hosts: [1usize, 2, 3]
            .iter()
            .map(|id| Host {
                node: open(&members, &format!("n{id}"), &state_path(*id as u32)),
                watch: None,
                last_fire: 0,
                last_poll: 0,
                last_commit_ms: 0,
            })
            .collect::<Vec<_>>(),
        queue: VecDeque::new(),
    };
    for index in 0..fabric.hosts.len() {
        let _ = fabric.hosts[index].node.recover();
        fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
    }
    fabric.pump();

    // Warm-up: boot, promote node 1 at view 0, let the followers'
    // bootstrap window force view 1 (node 2 primary) and serve.
    drive(&mut fabric, WATCH_MS * 3, false);
    println!("warm-up t={}: {:?}", fabric.now, snapshot(&fabric));
    assert!(
        fabric
            .hosts
            .iter()
            .filter(|h| h.status().state == STATE_NORMAL)
            .count()
            >= 2,
        "warm-up did not reach a serving cluster"
    );
    let serving = fabric
        .hosts
        .iter()
        .any(|h| h.status().state == STATE_NORMAL && h.status().leader == h.own());
    assert!(serving, "warm-up has no serving primary");

    // P2: drop node 2 mid-service.
    let killed_at = fabric.now;
    fabric.live.retain(|&id| id != 2);
    println!("P2 kill at t={killed_at}: {:?}", snapshot(&fabric));

    // The churn: survivors' phi proxy fires on the learned sketch and
    // forces the view up. Stop when at least two nodes sit in the limbo
    // (the fence quorum closed at the dead primary's view) or the budget
    // ends.
    let mut limbo = false;
    for _ in 0..32 {
        step(&mut fabric, POLL_MS, false);
        fabric.pump();
        let limbo_count = fabric
            .hosts
            .iter()
            .filter(|h| h.status().state == STATE_VIEW_CHANGE)
            .count();
        if limbo_count >= 2 {
            limbo = true;
            break;
        }
        if fabric.now > killed_at + 4_000 {
            break;
        }
    }
    println!("post-churn t={}: {:?}", fabric.now, snapshot(&fabric));
    assert!(limbo, "the kill did not strand the survivors in the limbo");

    // Phase A: the host as-is — the poll drives leader_timeout only.
    let deadline = fabric.now + 10_000;
    while fabric.now < deadline {
        step(&mut fabric, POLL_MS, false);
        fabric.pump();
    }
    let stuck = snapshot(&fabric);
    let committed_after = fabric
        .hosts
        .iter()
        .any(|h| h.status().state == STATE_NORMAL && h.last_commit_ms > killed_at);
    println!("phase A (poll = leader_timeout only) after 10s: {stuck:?}");
    assert!(
        !committed_after,
        "phase A converged — the wedge did not reproduce"
    );
    println!("phase A: WEDGE REPRODUCED — no commit after the kill");

    // Reopen node 2 fresh (same boot line, same state file): the clean
    // stop's `flushed` marker keeps incarnation 0, so the core boots
    // fenced at view 0 under the SAME identity — exactly the recorded
    // run's restart.
    let index = fabric
        .hosts
        .iter()
        .position(|h| h.own() == 2)
        .expect("node 2 slot");
    let _ = fabric.hosts[index].node.stop();
    let fresh = open(&members, "n2", &state_path(2));
    fabric.hosts[index].node = fresh;
    fabric.live.push(2);
    drive(&mut fabric, WATCH_MS * 3, false);
    fabric.pump();
    println!("restarted node 2 t={}: {:?}", fabric.now, snapshot(&fabric));

    // Phase B: the missing step — the poll drives the §14.2 forced view
    // in the limbo.
    let deadline = fabric.now + 3_000;
    let mut converged = false;
    while fabric.now < deadline {
        step(&mut fabric, POLL_MS, true);
        fabric.pump();
        if fabric
            .hosts
            .iter()
            .all(|h| h.status().state == STATE_NORMAL)
        {
            converged = true;
            break;
        }
    }
    println!(
        "phase B (poll = force_view in the limbo) t={}: {:?} converged={converged}",
        fabric.now,
        snapshot(&fabric)
    );
    assert!(
        converged,
        "phase B did not converge with the missing step supplied"
    );
    serve_and_assert(&mut fabric);
    println!(
        "ISOLATE COMPLETE: phase A reproduces the wedge; the §14.2 poll actuation unwedges it"
    );
}

/// One host-loop step: tick everyone, run the host timer plane, queue.
fn step(fabric: &mut Fabric, dt: u64, poll_forces: bool) {
    fabric.now += dt;
    for index in 0..fabric.hosts.len() {
        let status = fabric.hosts[index].status();
        let own = fabric.hosts[index].own();

        // The phi proxy's watched key: (config_era, leader), re-stamped
        // on a leader change.
        let watch_key = (status.config_era, status.leader);
        let _watch_born = match fabric.hosts[index].watch {
            Some((held, born)) if held == watch_key => born,
            _ => {
                fabric.hosts[index].watch = Some((watch_key, fabric.now));
                fabric.now
            }
        };

        // The phi fire:
        // - A Normal follower on a live sketch: the learned-sketch fire
        //   (signal 1) — any silence >= STALL_MS against a leader that
        //   is not this node.
        // - A node in the limbo: phi stands down under the `timedout`
        //   toggle (the host as-is); with the missing step the
        //   viewchange POLL carries the actuation instead.
        let fires = if status.state == STATE_NORMAL {
            status.leader != own
                && status.leader != u32::MAX
                && fabric
                    .now
                    .saturating_sub(fabric.hosts[index].last_commit_ms)
                    >= STALL_MS
        } else {
            poll_forces
        };
        if fires && fabric.now.saturating_sub(fabric.hosts[index].last_fire) >= WATCH_MS {
            fabric.hosts[index].last_fire = fabric.now;
            let forced = fabric.hosts[index]
                .node
                .force_view(status.era, status.view + 1);
            if forced != 0 {
                let _ = fabric.hosts[index].node.leader_timeout();
            }
            fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
        }

        // The cluster viewchange poll (main.rs timers): while the toggle
        // holds it polls on the randomized schedule. As-is the drive is
        // leader_timeout (an ordinary tick — a no-op for a ViewChange
        // node). With the missing step the drive is the §14.2 forced
        // view.
        if status.state == STATE_VIEW_CHANGE
            && fabric.now.saturating_sub(fabric.hosts[index].last_poll) >= POLL_MS
        {
            fabric.hosts[index].last_poll = fabric.now;
            if poll_forces {
                let forced = fabric.hosts[index]
                    .node
                    .force_view(status.era, status.view + 1);
                if forced != 0 {
                    let _ = fabric.hosts[index].node.leader_timeout();
                }
            } else {
                let _ = fabric.hosts[index].node.leader_timeout();
            }
            fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
        }

        // Fenced-boot drive while Recovering (main.rs timers).
        if status.state == STATE_RECOVERING
            && fabric.now.saturating_sub(fabric.hosts[index].last_poll) >= POLL_MS
        {
            fabric.hosts[index].last_poll = fabric.now;
            let _ = fabric.hosts[index].node.recover();
            fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
        }

        let _ = fabric.hosts[index].node.idle();
        fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
    }
}

fn drive(fabric: &mut Fabric, ms: u64, poll_forces: bool) {
    let deadline = fabric.now + ms;
    while fabric.now < deadline {
        step(fabric, TICK_MS, poll_forces);
        fabric.pump();
    }
}

/// Prove the unwedged cluster serves again: a fresh proposal commits
/// through the converged primary.
fn serve_and_assert(fabric: &mut Fabric) {
    let json = format!(
        "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":77,\"request_num\":1,\"lock_id\":1}}",
        mid = uuid::Uuid::new_v4()
    );
    let server = fabric
        .hosts
        .iter()
        .position(|h| h.status().state == STATE_NORMAL && h.status().leader == h.own())
        .expect("a Normal primary after convergence");
    let before = fabric.hosts[server].last_commit_ms;
    let code = fabric.hosts[server].node.request(json.as_bytes());
    assert_eq!(code, 0, "the fresh proposal was refused: {code}");
    for _ in 0..16 {
        step(fabric, POLL_MS, true);
        fabric.pump();
    }
    let after = fabric.hosts[server].last_commit_ms;
    assert!(
        after > before,
        "the fresh proposal never committed (last commit {before})"
    );
}
