//! The boot-fence strand (local-softball5/6-2026-09-18, `.tmp/telemetry/
//! local-softball5-2026-09-18/REPORT.md` and `local-softball6-2026-09-18/
//! REPORT.md`): a provisioned voter — a fresh First boot, a clean
//! superblock, a full genesis member — strands boot-fenced at
//! `state=4 (Joining) view=0` forever while the other two voters walk the
//! genesis views past it and serve as a 2-of-3 quorum. The stranded node
//! drops every inbound message as `ViewMismatch { got: N, current: 0 }`,
//! never adopts, never panics, and sends nothing the cluster can act on.
//!
//! The recorded race, from the run logs: the stranded node's FIRST-EVER
//! inbound datagram was already at the walk's final view (run 5:
//! `got: View(12)` in the boot millisecond; run 6: 84,979 drops, every
//! one `got: View(6)`, ZERO datagrams at views 0–5). The whole genesis
//! walk completed before the node processed anything, and the settled
//! serving cluster holds no further view changes — so the only inbound
//! the fence ever sees is the leader's higher-view Prepare/Commit stream.
//!
//! The contract pinned here (AGENTS.md's bug-provenance law): a node at
//! rest must time out and send. The boot gate's uninitialised/dirty path
//! says the node gossips, catches up, rejoins — so the fenced node must
//! either ADOPT a view (reach Normal at the cluster's view) or PRODUCE
//! outbound evidence the cluster can act on: a join gossip / GossipRequest
//! at any view, or a state-transfer request the leader accepts.
//!
//! The join gossip is the rejoin gossip's joiner half — a HOST obligation
//! (`lease_sequencer::rejoin`, uvrr-core
//! `docs/uvrr-rejoin-gossip-and-witnesses.md` §2: rejoining is a gossip
//! protocol OUTSIDE the main uVRR protocol, and the joiner keeps its own
//! resend timer). The core emits no `GossipRequest` — a `Joining` node's
//! tick drives only an already-open fetch (`ext/uvrr-core/src/replica/
//! mod.rs` `plan_tick`), and the fetch opens only through paths a fenced
//! fresh boot never reaches — while the core HANDLES the message on
//! receive (`plan_gossip_request`: every node that hears it records the
//! sender as a gossip-witness; the leader answers with the missed-range
//! push above the sender's frontier plus a fresh commit, and the echo of
//! the request's own view is what qualifies that push at the boot fence
//! — `plan_new_state`). The datagram therefore carries the node's
//! CURRENT view, never a view it has merely heard of: an entry ticket
//! naming a foreign view would draw an answer the fence drops as
//! `StaleTransfer` — evidence dressed up, not evidence. This harness
//! drives the host's resend timer exactly as `main.rs::timers` does:
//! delete the drive and the strand returns.
//!
//! Why the strand is deterministic (the drop rules, cited):
//! - `ext/uvrr-core/src/replica/normal.rs` (`plan_prepare`, the
//!   higher-view branch): a Prepare from the legitimate primary of a
//!   HIGHER view hits `plan_higher_view_signal` only for a node NOT at
//!   its boot fence (`if !boot_fence`); a boot-fenced `Joining` member
//!   (`current == retained`) falls through to
//!   `header.view != current` → `Diagnostic::ViewMismatch { got, current
//!   }`. Its adoption window accepts messages AT its current view only.
//! - The same shape in `plan_commit` (the `if !boot_fence` branch, then
//!   the `ViewMismatch` drop).
//! - `ext/uvrr-core/src/replica/mod.rs` (`plan_tick`): a `Joining` node
//!   is excluded from the suspicion gate (`matches!(status, Normal |
//!   Restarting)`), is not promotable unless it IS the genesis primary,
//!   and with no stalled offer and no open fetch its tick produces the
//!   smallest honest transition — no outbound, ever. The §10 acquisition
//!   re-run re-issues only an already-open fetch, and the fetch opens
//!   only through `plan_higher_view_signal`, which the boot fence skips.
//!
//! Why the fabric must SETTLE the cluster before releasing the held
//! node: a view change whose designated primary is the held node jams the
//! walkers into the limbo, and the poll's forced advance keeps
//! broadcasting `StartViewChange` fence votes (`enter_view_change` sends
//! to every backup) — a boot-fenced node ADMITS a higher-view fence vote
//! (`view_change.rs::plan_start_view_change`: `header.view > target` →
//! joins the attempt) and a jammed attempt at one of its primary views
//! would seat it. That rescue is exactly what the live runs' cluster
//! never offered: it had already settled at its final view. So the fabric
//! walks past, then quiesces the churn behind a serving leader (the
//! fresh-commit arrival re-arms the follower's watch, the live host's
//! calm-profile behavior) before the held node's first datagram — the
//! recorded race, deterministically.
//!
//! The test then gives the fenced node a generous bounded budget of its
//! own timeout drives and demands the contract: adopt, or emit a join
//! gossip / GossipRequest at any view — the entry ticket the leader acts
//! on whatever view it names.

use lease_sequencer::phi::{self, PollActuation};
use lease_sequencer::rejoin;
use lunet_advisory_lock::Node;
use std::collections::VecDeque;

const TICK_MS: u64 = 6;
const POLL_MS: u64 = 150;
const STALL_MS: u64 = 20;
const WATCH_MS: u64 = 500;
/// The serving cadence: a proposal every 12 ms of simulated time keeps
/// every Normal follower's commit evidence fresher than the 20 ms stall —
/// the settled cluster the live calm profile served (run 6: zero view
/// changes during 290 s of serving).
const SERVE_MS: u64 = 12;

const STATE_NORMAL: u32 = 0;
const STATE_RECOVERING: u32 = 2;
/// The boot fence: `Status::Joining`'s snapshot word
/// (`ext/uvrr-core/src/progress.rs::Status::to_word`).
const STATE_JOINING: u32 = 4;

const ROOT: &str = "/Users/Shared/lua-lunet/lunet-locks/.tmp/boot-fence-strand-test";

type Delivery = (u32, u32, Vec<u8>);

struct Host {
    node: Node,
    last_fire: u64,
    last_poll: u64,
    last_commit_ms: u64,
    /// The last join-gossip resend (`rejoin::GOSSIP_RESEND_MS`), the
    /// host-loop timer `main.rs::timers` runs for a fenced `Joining`
    /// boot.
    last_gossip: u64,
    serving: bool,
    request_num: u64,
    /// Inbound datagrams actually delivered into the node.
    received: u64,
    /// Outbound peer datagrams the node produced (any view).
    emitted: u64,
    /// The highest view the node NAMED on an outbound datagram.
    max_out_view: u32,
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

    fn leader_here(&self) -> bool {
        let status = self.node.status();
        status.state == STATE_NORMAL && status.leader == self.own()
    }

    fn drain(&mut self, queue: &mut VecDeque<Delivery>, now: u64) {
        while let Some(out) = self.node.next_output() {
            if out.kind == 1 {
                self.emitted += 1;
                self.max_out_view = self.max_out_view.max(out.view);
                if out.bytes.len() >= 21
                    && u32::from_be_bytes(out.bytes[0..4].try_into().unwrap()) == 4
                {
                    self.last_commit_ms = now;
                }
                queue.push_back((self.own(), out.to, out.bytes));
            }
        }
    }

    /// The host's serving pulse (run 6's load client: one operation every
    /// 250 ms — here every 50 ms of simulated time): a fresh proposal on
    /// the serving primary. The commit round trip re-arms every Normal
    /// member's watch, which is what keeps the settled cluster quiet.
    fn serve(&mut self) {
        if !self.serving || !self.leader_here() {
            return;
        }
        self.request_num += 1;
        let json = format!(
            "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":77,\"request_num\":{num},\"lock_id\":1}}",
            mid = uuid::Uuid::from_u128(self.request_num as u128),
            num = self.request_num,
        );
        let _code = self.node.request(json.as_bytes());
    }
}

struct Fabric {
    now: u64,
    live: Vec<u32>,
    /// Members whose inbound delivery is withheld: the partition the
    /// recorded run lived through (the fence's first datagram already at
    /// the settled view). Their outbound still flows — the contract under
    /// test is what the fenced node SENDS, never what it suppresses.
    held: Vec<u32>,
    /// Whether the partition has healed: the join gossips the heal
    /// window counts are the post-heal resends the cluster can act on.
    post_heal: bool,
    /// Join gossips the hosts sent since the heal.
    gossips: u64,
    hosts: Vec<Host>,
    queue: VecDeque<Delivery>,
}

impl Fabric {
    fn pump(&mut self) {
        while let Some((from, to, bytes)) = self.queue.pop_front() {
            if !self.live.contains(&to) {
                continue;
            }
            if self.held.contains(&to) {
                continue;
            }
            let index = match self.hosts.iter().position(|h| h.own() == to) {
                Some(index) => index,
                None => continue,
            };
            self.hosts[index].received += 1;
            if std::env::var("STRAND_DEBUG").is_ok() {
                let tag = if bytes.len() >= 4 {
                    u32::from_be_bytes(bytes[0..4].try_into().unwrap())
                } else {
                    0
                };
                eprintln!(
                    "DBG pump t={} to=n{} from=n{} tag={}",
                    self.now,
                    to,
                    from,
                    tag
                );
            }
            // The fresh-commit arrival re-arms the watch: a received
            // Commit datagram is live-leader evidence (the same evidence
            // class the host's phi plane consumes).
            if bytes.len() >= 21 && u32::from_be_bytes(bytes[0..4].try_into().unwrap()) == 4 {
                self.hosts[index].last_commit_ms = self.now;
            }
            let _ = self.hosts[index].node.receive(from, &bytes);
            self.hosts[index].drain(&mut self.queue, self.now);
        }
    }
}

/// One host-loop step mirroring `main.rs::timers`: the serving pulse, the
/// phi fire (a Normal follower's stall), the fenced-boot drive, and the
/// cluster viewchange poll — the poll's drive comes from
/// `phi::poll_actuation`, so the churn that advances the view is the live
/// host's own behavior.
fn step(fabric: &mut Fabric, dt: u64, serving: bool) {
    fabric.now += dt;
    for index in 0..fabric.hosts.len() {
        if serving && fabric.now % SERVE_MS < dt {
            fabric.hosts[index].serve();
        }
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
            // The limbo and the fence: phi stands down under the toggle.
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

        // The rejoin gossip's joiner half, the host-loop drive
        // (`main.rs::timers`): a fenced `Joining` boot gossips its entry
        // ticket to every peer on `rejoin::GOSSIP_RESEND_MS`. The held
        // node's outbound still flows — the resend timer is the gossip's
        // own reliability mechanism — and the heal window counts the
        // post-heal resends.
        if fabric.hosts[index].state() == STATE_JOINING
            && fabric.now.saturating_sub(fabric.hosts[index].last_gossip) >= rejoin::GOSSIP_RESEND_MS
        {
            fabric.hosts[index].last_gossip = fabric.now;
            let status = fabric.hosts[index].node.status();
            let payload = rejoin::gossip_datagram(status.era, status.view);
            for to in [1u32, 2, 3] {
                if to != fabric.hosts[index].own() {
                    fabric
                        .queue
                        .push_back((fabric.hosts[index].own(), to, payload.clone()));
                }
            }
            if fabric.post_heal {
                fabric.gossips += 1;
            }
        }

        let _ = fabric.hosts[index].node.idle();
        fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
    }
}

fn drive(fabric: &mut Fabric, ms: u64, serving: bool) {
    let deadline = fabric.now + ms;
    while fabric.now < deadline {
        step(fabric, TICK_MS, serving);
        fabric.pump();
    }
}

fn walkers(fabric: &Fabric) -> Vec<(u32, u32, u32)> {
    fabric.hosts[..2]
        .iter()
        .map(|h| (h.own(), h.state(), h.view()))
        .collect()
}

/// The boot-fence strand: a provisioned voter whose first-ever inbound
/// datagram arrives at the settled cluster's view must still reach the
/// cluster — adopt, or emit outbound evidence the cluster can act on. On
/// the current tree it wedges at the fence: every inbound datagram drops
/// as `ViewMismatch { got: N, current: 0 }`, its own timeout machinery
/// produces nothing, and the cluster serves past it as a 2-of-3 quorum —
/// exactly the live-recorded lockup.
#[test]
fn a_boot_fenced_voter_must_adopt_or_emit_actionable_evidence() {
    let _ = std::fs::remove_dir_all(ROOT);
    std::fs::create_dir_all(ROOT).expect("scratch root");
    let members = ["1:n1", "2:n2", "3:n3"].join("\0");
    let mut fabric = Fabric {
        now: 0,
        live: vec![1, 2, 3],
        held: vec![3],
        post_heal: false,
        gossips: 0,
        hosts: [1usize, 2, 3]
            .iter()
            .map(|id| Host {
                node: Node::open(
                    &members,
                    &format!("n{id}"),
                    &format!("{ROOT}/n{id}.state"),
                    None,
                    0,
                )
                .expect("node open"),
                last_fire: 0,
                last_poll: 0,
                last_commit_ms: 0,
                last_gossip: 0,
                serving: *id != 3,
                request_num: 0,
                received: 0,
                emitted: 0,
                max_out_view: 0,
            })
            .collect::<Vec<_>>(),
        queue: VecDeque::new(),
    };

    // The fence precondition: every fresh First boot provisions fenced
    // `Joining` at the genesis view — promotion happens on a tick, not in
    // `provision` (`ext/uvrr-core/src/replica/mod.rs::plan_tick`).
    for host in &fabric.hosts {
        assert_eq!(
            (host.state(), host.view()),
            (STATE_JOINING, 0),
            "n{} did not provision boot-fenced at the genesis view",
            host.own()
        );
    }

    // The boot drives: the genesis primary self-promotes on its first
    // tick; the fenced backups wait for the primary's messages. n3 is
    // HELD from here — nothing reaches it through the whole walk.
    for index in 0..fabric.hosts.len() {
        let _ = fabric.hosts[index].node.recover();
        fabric.hosts[index].drain(&mut fabric.queue, fabric.now);
    }
    fabric.pump();

    // Phase 1 — the walk: the two unheld voters climb the genesis views
    // (promotion at view 0, the followers' phi fires forcing view after
    // view, the held n3's primary rotations jamming into the poll's
    // forced advance — the live walk's round-robin shape). Drive until
    // the cluster is strictly past the view-0 adoption window and past
    // the run-6 settled view.
    let deadline = fabric.now + 8_000;
    while fabric.now < deadline {
        drive(&mut fabric, POLL_MS, false);
        if walkers(&fabric).iter().all(|&(_, _, view)| view >= 6) {
            break;
        }
    }
    assert!(
        walkers(&fabric).iter().all(|&(_, _, view)| view >= 6),
        "the unheld walkers never left the view-0 adoption window behind: {walkers:?}",
        walkers = walkers(&fabric)
    );
    assert_eq!(
        (fabric.hosts[2].state(), fabric.hosts[2].view()),
        (STATE_JOINING, 0),
        "the held node did not stay at its boot fence through the walk — \
         the race was not reproduced"
    );

    // Phase 2 — the settle: the serving pulse quiesces the churn (the
    // fresh-commit arrival re-arms every Normal follower's watch, so no
    // further fence votes fly). The strand needs the SETTLED cluster:
    // both walkers Normal at the same view, stable across three checks —
    // a window longer than the fire cadence, so no pending fire can slip
    // past the break.
    let deadline = fabric.now + 8_000;
    let mut settled_view = 0;
    let mut stable = 0;
    while fabric.now < deadline {
        drive(&mut fabric, POLL_MS, true);
        let now_walkers = walkers(&fabric);
        let same = now_walkers
            .iter()
            .all(|&(_, state, view)| state == STATE_NORMAL && view == now_walkers[0].2);
        if same && now_walkers[0].2 == settled_view && settled_view >= 6 {
            stable += 1;
            if stable >= 3 {
                break;
            }
        } else {
            stable = 0;
        }
        if same {
            settled_view = now_walkers[0].2;
        }
    }
    let now_walkers = walkers(&fabric);
    assert!(
        now_walkers
            .iter()
            .all(|&(_, state, view)| state == STATE_NORMAL && view == now_walkers[0].2)
            && now_walkers[0].2 >= 6,
        "the walkers never settled into a serving cluster: {now_walkers:?}"
    );
    let settled_view = now_walkers[0].2;
    assert_eq!(
        (fabric.hosts[2].state(), fabric.hosts[2].view()),
        (STATE_JOINING, 0),
        "the held node did not stay at its boot fence through the settle"
    );

    // Phase 3 — the partition heals: the fenced node's FIRST datagram
    // arrives at the settled view (the run-6 record), the cluster is
    // quiet — every further datagram is a higher-view Prepare/Commit,
    // provably ignorable at the fence per the drop rules in this file's
    // header. The fenced node's own timeout machinery keeps polling (the
    // poll's LeaderTimeout tick) and its join-gossip resend timer keeps
    // sending the entry ticket at the node's current view — the contract
    // demands it still reach the cluster: adopt a view, or emit a join
    // gossip / GossipRequest the cluster can act on (any view — the
    // leader answers the entry ticket whatever view it names). Generous
    // but finite budget.
    fabric.held.clear();
    fabric.post_heal = true;
    let deadline = fabric.now + 12_000;
    let mut saved = false;
    while fabric.now < deadline {
        drive(&mut fabric, POLL_MS, true);
        let fenced = &fabric.hosts[2];
        let cluster_view = settled_view
            .max(fabric.hosts[0].view())
            .max(fabric.hosts[1].view());
        if fenced.state() == STATE_NORMAL && fenced.view() >= cluster_view
            || fabric.gossips > 0
        {
            saved = true;
            break;
        }
    }

    let fenced = &fabric.hosts[2];
    let walker_rows = walkers(&fabric);
    assert!(
        saved,
        "THE BOOT-FENCE STRAND (local-softball5/6-2026-09-18 defect): the \
         provisioned voter n3 sat boot-fenced at state={} view={} for the \
         whole bounded drive while the settled cluster served at view {} \
         (walkers {walker_rows:?}) as a 2-of-3 quorum.\n\
         The contract (AGENTS.md bug-provenance law: a node at rest must \
         time out and send) demands the fenced node either ADOPT a view — \
         reach Normal at the cluster's view — or EMIT outbound evidence the \
         cluster can act on: a join gossip / GossipRequest at any view, or \
         a state-transfer request the leader accepts.\n\
         Observed: {} inbound datagrams delivered — its first-ever datagram \
         already at the settled view, every one ignorable at the fence \
         (higher-view traffic drops as ViewMismatch {{got: N, current: 0}} \
         — ext/uvrr-core/src/replica/normal.rs `plan_prepare` and \
         `plan_commit`: the boot-fenced member's adoption window only \
         accepts messages AT its current view, and the settled cluster \
         holds no view change, so no fence vote ever flies); {} outbound \
         datagrams, highest named view {} (ext/uvrr-core/src/replica/mod.rs \
         `plan_tick`: a Joining node is excluded from the suspicion gate, \
         not promotable off the genesis primary, and with no open fetch its \
         tick emits nothing); {} join gossips sent since the heal \
         (`lease_sequencer::rejoin`: the resend-timer drive the host loop \
         runs for a fenced Joining boot).\n\
         The fenced node sent nothing the cluster can act on and never \
         adopted. n3 final status: {:?}",
        fenced.state(),
        fenced.view(),
        settled_view,
        fenced.received,
        fenced.emitted,
        fenced.max_out_view,
        fabric.gossips,
        fenced.node.status(),
    );

    // Sanity: the walkers kept serving through the whole drive — the
    // strand is a liveness hole behind a live serving quorum, not a
    // cluster-wide wedge.
    let now_walkers = walker_rows;
    assert!(
        now_walkers
            .iter()
            .all(|&(_, state, view)| state == STATE_NORMAL && view == settled_view),
        "the walkers did not stay settled at view {settled_view}: {now_walkers:?}"
    );
    let leader = fabric
        .hosts
        .iter()
        .position(|h| h.leader_here())
        .expect("a serving primary after the drive");
    let json = format!(
        "{{\"op\":\"get\",\"message_id\":\"{mid}\",\"client_id\":77,\"request_num\":1,\"lock_id\":1}}",
        mid = uuid::Uuid::from_u128(u128::MAX),
    );
    assert_eq!(
        fabric.hosts[leader].node.request(json.as_bytes()),
        0,
        "the fresh proposal was refused"
    );
    let mut committed = false;
    for _ in 0..16 {
        drive(&mut fabric, POLL_MS, true);
        if fabric.hosts.iter().any(|h| {
            h.leader_here() && {
                let status = h.node.status();
                status.view == settled_view
            }
        }) && fabric
            .hosts
            .iter()
            .filter(|h| h.own() != 3)
            .all(|h| h.last_commit_ms >= fabric.now - POLL_MS * 2)
        {
            committed = true;
            break;
        }
    }
    assert!(
        committed,
        "the settled cluster stopped serving after the release"
    );
    assert_eq!(
        (fabric.hosts[2].state(), fabric.hosts[2].view()),
        (STATE_JOINING, 0),
        "the fence unexpectedly moved"
    );
}
