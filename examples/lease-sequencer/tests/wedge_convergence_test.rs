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
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The full P2 shape: elect node 2, serve, TERM it, restart it clean on
/// the same boot line, and converge with the poll carrying the §14.2
/// forced view. As recorded the survivors sit fenced at the dead
/// primary's next view; the poll's forced advance walks the cluster out.
#[test]
fn leader_kill_restart_converges_through_the_limbos_poll() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
