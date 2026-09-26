//! The P2 wedge regression (local-softball2-2026-09-17): a leader TERM
//! followed by a same-boot-line restart wedged the cluster — the
//! survivors sat fenced at the dead primary's next view for minutes
//! while their randomized viewchange poll fired `leader_timeout`
//! (an ordinary tick) every 100-200 ms and the tick cannot advance a
//! `ViewChange`-status node (ext/uvrr-core/src/replica/mod.rs:1546-1547).
//! The poll in the limbo must carry the §14.2 host-forced view instead
//! (`docs/src/phi-and-timeouts.md`: the viewchange timer takes over).

use lease_sequencer::phi::{self, PollActuation};
use lunet_advisory_lock::Node;
use std::collections::VecDeque;

const TICK_MS: u64 = 6;
const POLL_MS: u64 = 150;
const STALL_MS: u64 = 20;
const WATCH_MS: u64 = 500;

const STATE_NORMAL: u32 = 0;
const STATE_VIEW_CHANGE: u32 = 1;
const STATE_RECOVERING: u32 = 2;

const ROOT: &str = "/Users/Shared/lua-lunet/lunet-locks/.tmp/wedge-test";

type Delivery = (u32, u32, Vec<u8>);

struct Host {
    node: Node,
    watch: Option<((u32, u32), u64)>,
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
/// `phi::poll_actuation`, so the decision under test IS the behavior
/// under test.
fn step(fabric: &mut Fabric, dt: u64) {
    fabric.now += dt;
    for index in 0..fabric.hosts.len() {
        let status = fabric.hosts[index].node.status();
        let own = fabric.hosts[index].own();

        let watched_key = (status.config_era, status.leader);
        let _watch_born = match fabric.hosts[index].watch {
            Some((held, born)) if held == watched_key => born,
            _ => {
                fabric.hosts[index].watch = Some((watched_key, fabric.now));
                fabric.now
            }
        };

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

/// The limbo's poll carries the §14.2 forced view; every other case
/// keeps the ordinary suspicion tick.
#[test]
fn the_limbos_poll_carries_the_forced_view() {
    assert_eq!(
        phi::poll_actuation(true, STATE_VIEW_CHANGE, true),
        PollActuation::ForceView,
        "a latched node inside a view change drives the §14.2 forced view"
    );
    assert_eq!(
        phi::poll_actuation(true, STATE_RECOVERING, true),
        PollActuation::LeaderTimeout,
        "a recovering node keeps the ordinary suspicion tick"
    );
    assert_eq!(
        phi::poll_actuation(false, STATE_VIEW_CHANGE, true),
        PollActuation::None,
        "an un-timed-out node's poll does nothing"
    );
    assert_eq!(
        phi::poll_actuation(true, STATE_VIEW_CHANGE, false),
        PollActuation::None,
        "an undue poll does nothing"
    );
}

/// The full P2 shape: elect node 2, serve, TERM it, restart it clean on
/// the same boot line, and converge with the poll carrying the §14.2
/// forced view. As recorded the survivors sit fenced at the dead
/// primary's next view; the poll's forced advance walks the cluster out.
#[test]
fn leader_kill_restart_converges_through_the_limbos_poll() {
    let _ = std::fs::remove_dir_all(ROOT);
    std::fs::create_dir_all(ROOT).expect("scratch root");
    let members = ["65537:n1", "131073:n2", "196609:n3"].join("\0");
    let mut fabric = Fabric {
        now: 0,
        live: vec![65537, 131073, 196609],
        hosts: [65537u32, 131073, 196609]
            .iter()
            .enumerate()
            .map(|(index, id)| Host {
                node: Node::open(
                    &members,
                    &format!("n{}", index + 1),
                    &format!("{ROOT}/n{}.state", index + 1),
                    None,
                    0,
                )
                .expect("node open"),
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

    // Warm-up: node 1 promotes at view 0; the followers' bootstrap
    // window forces view 1 (node 2, the primary) and the cluster serves.
    drive(&mut fabric, WATCH_MS * 3);
    assert!(
        fabric
            .hosts
            .iter()
            .filter(|h| h.state() == STATE_NORMAL)
            .count()
            >= 2,
        "the warm-up did not reach a serving cluster: {:?}",
        fabric
            .hosts
            .iter()
            .map(|h| (h.own(), h.state(), h.view()))
            .collect::<Vec<_>>()
    );
    let leader = fabric
        .hosts
        .iter()
        .position(|h| h.state() == STATE_NORMAL && h.node.status().leader == h.own())
        .expect("a serving primary after warm-up");

    // P2: TERM the leader (a clean stop — the marker ends `flushed`),
    // restart it on the same boot line.
    let killed_id = fabric.hosts[leader].own();
    let _ = fabric.hosts[leader].node.stop();
    fabric.live.retain(|&id| id != killed_id);
    let fresh = Node::open(
        &members,
        &format!("n{}", leader + 1),
        &format!("{ROOT}/n{}.state", leader + 1),
        None,
        0,
    )
    .expect("the restart boots");
    fabric.hosts[leader].node = fresh;
    assert_eq!(
        fabric.hosts[leader].own(),
        killed_id,
        "the clean restart keeps the same identity (no incarnation bump)"
    );

    // The churn: the survivors' phi fires walk the view up. The walk
    // stalls at the first view whose designated primary is the killed
    // node — the fence quorum closes, each survivor's DoViewChange to
    // the dead primary is lost, and the attempt jams. With the host
    // as-is (the poll driving an ordinary tick) nothing moves from
    // here; the jam must form before convergence is attempted.
    let deadline = fabric.now + 4_000;
    let mut jammed = false;
    while fabric.now < deadline {
        drive(&mut fabric, POLL_MS);
        let limbo = fabric
            .hosts
            .iter()
            .filter(|h| h.state() == STATE_VIEW_CHANGE)
            .count();
        if limbo >= 2 {
            jammed = true;
            break;
        }
    }
    assert!(
        jammed,
        "the kill did not strand the survivors in the limbo: {:?}",
        fabric
            .hosts
            .iter()
            .map(|h| (h.own(), h.state(), h.view()))
            .collect::<Vec<_>>()
    );
    let jam_view = fabric
        .hosts
        .iter()
        .filter(|h| h.state() == STATE_VIEW_CHANGE)
        .map(|h| h.view())
        .max()
        .expect("a limbo view");

    // Convergence: the poll's forced advance walks every node out of
    // the limbo and the restarted node catches up. The restart rejoins
    // the fabric exactly here (the recorded run's ~8 s later).
    fabric.live.push(killed_id);
    let deadline = fabric.now + 8_000;
    let mut converged = false;
    while fabric.now < deadline {
        drive(&mut fabric, POLL_MS);
        if fabric.hosts.iter().all(|h| h.state() == STATE_NORMAL) {
            converged = true;
            break;
        }
    }
    assert!(
        converged,
        "the cluster never converged out of the jam at view {jam_view}: {:?}",
        fabric
            .hosts
            .iter()
            .map(|h| (h.own(), h.state(), h.view()))
            .collect::<Vec<_>>()
    );
    assert!(
        fabric.hosts.iter().all(|h| h.view() > jam_view),
        "the converged views must have advanced past the jam: {:?}",
        fabric
            .hosts
            .iter()
            .map(|h| (h.own(), h.view()))
            .collect::<Vec<_>>()
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
