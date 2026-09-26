//! The clean-restart wedge regression (upstream uvrr-core issue #57's
//! shape): a leader cleanly stopped and restarted while the cluster
//! advances to a HIGHER view must converge — the restarted node
//! re-synchronises in-cluster exactly as though a network partition had
//! healed — never wedging in mutual ViewMismatch/StaleTransfer drops.
//!
//! The intake pins the classification the convergence rides on: the
//! stop's Stopped quorum classifies CLEAN through the boot gate
//! (`lifecycle::boot`), the same identity resumes behind the engine's
//! `Vouched` token, and the returning node chases the cluster's advanced
//! view to Normal. A boot that mis-classified the stop (the pre-intake
//! DIRTY reading) would restart the leader under a bumped high-band
//! identity the cluster drops by name — the wedge.

use lease_sequencer::phi::{self, PollActuation};
use lunet_advisory_lock::Node;
use std::collections::VecDeque;

const TICK_MS: u64 = 6;
const POLL_MS: u64 = 150;
const STALL_MS: u64 = 20;
const WATCH_MS: u64 = 500;

const STATE_NORMAL: u32 = 0;
const STATE_RECOVERING: u32 = 2;

const ROOT: &str = "/Users/Shared/lua-lunet/lunet-locks/.tmp/clean-restart-test";

type Delivery = (u32, u32, Vec<u8>);

struct Host {
    node: Node,
    last_fire: u64,
    last_poll: u64,
    last_commit_ms: u64,
}

impl Host {
    fn state(&self) -> u32 {
        self.node.status().state
    }

    fn view(&self) -> u32 {
        self.node.status().view
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
        }
    }
}

struct Fabric {
    now: u64,
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

/// One host-loop step mirroring `main.rs::timers`: the phi fire (a
/// Normal follower's sketch stall), the fenced-boot drive, and the
/// cluster viewchange poll — the poll's drive comes from
/// `phi::poll_actuation`, so the churn that advances the view while the
/// leader is down is the live host's own behavior.
fn step(fabric: &mut Fabric, dt: u64) {
    fabric.now += dt;
    for index in 0..fabric.hosts.len() {
        let status = fabric.hosts[index].node.status();
        let own = fabric.hosts[index].own();

        let fires = if status.state == STATE_NORMAL {
            status.leader != own
                && status.leader != u32::MAX
                && fabric
                    .now
                    .saturating_sub(fabric.hosts[index].last_commit_ms)
                    >= STALL_MS
        } else {
            // The limbo: phi stands down under the toggle.
            false
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

        if fabric.hosts[index].state() != STATE_NORMAL
            && fabric.now.saturating_sub(fabric.hosts[index].last_poll) >= POLL_MS
        {
            fabric.hosts[index].last_poll = fabric.now;
            match phi::poll_actuation(true, fabric.hosts[index].state(), true) {
                PollActuation::ForceView => {
                    let forced = fabric.hosts[index]
                        .node
                        .force_view(status.era, status.view + 1);
                    if forced != 0 {
                        let _ = fabric.hosts[index].node.leader_timeout();
                    }
                }
                PollActuation::LeaderTimeout => {
                    let _ = fabric.hosts[index].node.leader_timeout();
                }
                PollActuation::None => {}
            }
            fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
        }

        if fabric.hosts[index].state() == STATE_RECOVERING
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

fn drive(fabric: &mut Fabric, ms: u64) {
    let deadline = fabric.now + ms;
    while fabric.now < deadline {
        step(fabric, TICK_MS);
        fabric.pump();
    }
}

/// The #57 shape: elect a leader, serve, stop it CLEANLY (the Stopped
/// quorum lands), let the survivors advance the cluster to a HIGHER
/// view, then restart the leader on the same boot line — it must resume
/// the same identity (no bump) and converge to Normal at the cluster's
/// current view.
#[test]
fn clean_restart_while_the_cluster_advances_converges() {
    let _ = std::fs::remove_dir_all(ROOT);
    std::fs::create_dir_all(ROOT).expect("scratch root");
    let members = ["65537:n1", "131073:n2", "196609:n3"].join("\0");
    let mut fabric = Fabric {
        now: 0,
        live: vec![65537, 131073, 196609],
        hosts: [65537u32, 131073, 196609]
            .iter()
            .enumerate()
            .map(|(index, _id)| Host {
                node: Node::open(
                    &members,
                    &format!("n{}", index + 1),
                    &format!("{ROOT}/n{}.state", index + 1),
                    None,
                    0,
                )
                .expect("node open"),
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

    // Warm-up: the cluster reaches a serving state with a primary.
    drive(&mut fabric, WATCH_MS * 3);
    let leader = fabric
        .hosts
        .iter()
        .position(|h| h.state() == STATE_NORMAL && h.node.status().leader == h.own())
        .expect("a serving primary after warm-up");

    // The clean stop: the wire closes, the drain runs, the Stopped quorum
    // lands — the boot gate's clean classification is earned.
    let leader_id = fabric.hosts[leader].own();
    let leader_view = fabric.hosts[leader].view();
    assert_eq!(fabric.hosts[leader].node.stop(), 0, "the clean stop");

    // The survivors keep running: the phi fires and the limbo poll walk
    // the view up past the stopped primary. The cluster must be at a
    // HIGHER view than the leader's stop-time view before the restart.
    fabric.live.retain(|&id| id != leader_id);
    let deadline = fabric.now + 6_000;
    while fabric.now < deadline {
        drive(&mut fabric, POLL_MS);
        let cluster_view = fabric
            .hosts
            .iter()
            .filter(|h| h.own() != leader_id)
            .map(|h| h.view())
            .max()
            .expect("a survivor's view");
        if cluster_view > leader_view {
            break;
        }
    }
    let cluster_view = fabric
        .hosts
        .iter()
        .filter(|h| h.own() != leader_id)
        .map(|h| h.view())
        .max()
        .expect("a survivor's view");
    assert!(
        cluster_view > leader_view,
        "the cluster never advanced past the stopped leader's view: \
         stop-time {leader_view}, survivors {:?}",
        fabric
            .hosts
            .iter()
            .map(|h| (h.own(), h.state(), h.view()))
            .collect::<Vec<_>>()
    );

    // The restart on the same boot line: the Stopped quorum classifies
    // CLEAN — the same identity resumes, never a bumped stranger the
    // cluster would drop by name.
    let fresh = Node::open(
        &members,
        &format!("n{}", leader + 1),
        &format!("{ROOT}/n{}.state", leader + 1),
        None,
        0,
    )
    .expect("the restart boots");
    assert_eq!(
        fresh.own_id(),
        leader_id,
        "the clean restart keeps the same identity (no incarnation bump)"
    );
    fabric.hosts[leader].node = fresh;
    fabric.live.push(leader_id);

    // The convergence gate: the restarted node chases the cluster's
    // advanced view and every member ends Normal at the same view.
    let deadline = fabric.now + 8_000;
    let mut converged = false;
    while fabric.now < deadline {
        drive(&mut fabric, POLL_MS);
        if fabric.hosts.iter().all(|h| h.state() == STATE_NORMAL)
            && fabric.hosts.iter().all(|h| h.view() >= cluster_view)
        {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "the restarted leader never converged to the cluster's advanced view \
         {cluster_view}: {:?}",
        fabric
            .hosts
            .iter()
            .map(|h| (h.own(), h.state(), h.view()))
            .collect::<Vec<_>>()
    );
    let views: Vec<u32> = fabric.hosts.iter().map(|h| h.view()).collect();
    assert!(
        views.iter().all(|view| *view == views[0]),
        "the converged views agree: {views:?}"
    );

    // The converged cluster serves again: a fresh proposal commits.
    let json = format!(
        "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":77,\"request_num\":1,\"lock_id\":1}}",
        mid = uuid::Uuid::new_v4()
    );
    let server = fabric
        .hosts
        .iter()
        .position(|h| h.state() == STATE_NORMAL && h.node.status().leader == h.own())
        .expect("a Normal primary after convergence");
    let before = fabric.hosts[server].last_commit_ms;
    let code = fabric.hosts[server].node.request(json.as_bytes());
    assert_eq!(code, 0, "the fresh proposal was refused: {code}");
    for _ in 0..16 {
        step(&mut fabric, POLL_MS);
        fabric.pump();
    }
    assert!(
        fabric.hosts[server].last_commit_ms > before,
        "the fresh proposal never committed"
    );
}
