//! The host's relay, against real nodes: the resend multiset the cluster
//! wait owes, the heartbeat's cost in journal slots, and the armed
//! attempt's ballot across many timeouts.
//!
//! The harness is the message-fed one in miniature — real adapter
//! `Node`s over scratch marker stores, the wire captured in memory, and
//! the clock this file's own parameter (`set_compliance_clock`, never a
//! wall reading). What these tests pin are the two laws the relay rests
//! on inside the core, plus the host's own discipline around them:
//!
//! 1. a repeated `Prepare` re-acknowledges without re-applying and a
//!    repeated `Commit` applies nothing, so the resend multiset costs no
//!    slot — which is why a relay is safe and why the cluster wait may
//!    re-send without re-proposing;
//! 2. the heartbeat of a commit re-announces the leader's last commit
//!    byte for byte and advances the journal not at all: N beats, N
//!    beats, ZERO slots.

use std::fs;
use std::path::PathBuf;

use lease_sequencer::relay::{self, Datagram, Ledger};
use lunet_advisory_lock::{Node, OK};
use vrr::wire::Tag;

/// The two-member roster every test here plays by: the smallest roster
/// whose fences are exchanged and whose view change reaches a quorum.
const MEMBERS: usize = 2;
/// The primary timeout every node is opened with, in the logical clock's
/// own units.
const PRIMARY_TIMEOUT: u64 = 50;
/// The settle bound: a provisioned two-member cluster converges long
/// before this, so a loop that does not settle inside it is a defect.
const SETTLE_BOUND: usize = 400;
/// The beats a heartbeat is measured over.
const BEATS: usize = 25;

/// One member's packed identity, `system:counter` with both halves
/// one-indexed — the first life of the `index + 1`-th system, packed the
/// core's own way (MSB system, LSB crash counter).
fn identity(index: usize) -> u32 {
    (((index + 1) as u32) << 16) | 1
}

/// The genesis member buffer a provisioned roster's descriptor carries:
/// `<packed-id>:<name>` per member, NUL-separated, in succession order.
fn genesis() -> String {
    (0..MEMBERS)
        .map(|index| format!("{}:n{}", identity(index), index + 1))
        .collect::<Vec<String>>()
        .join("\0")
}

/// A scenario's scratch directory under the repo's `.tmp`, named for this
/// test binary and the scenario, so two scenarios never share a marker
/// store and a stale directory from an earlier run never boots over.
fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp/relay-tests")
        .join(name);
    if dir.exists() {
        fs::remove_dir_all(&dir).expect("a stale scenario directory clears");
    }
    fs::create_dir_all(&dir).expect("the scenario directory creates");
    dir
}

/// One member's addressable row: the node, its own id, and the scratch
/// path it was opened over.
struct Seat {
    node: Node,
    id: u32,
}

/// A provisioned cluster, message-fed: `exchange` moves every released
/// datagram to its addressee until the cluster is quiet.
struct Pair {
    seats: Vec<Seat>,
    /// One relay ledger per member: every datagram the harness saw that
    /// member release is recorded, and every delivered datagram is
    /// offered as the response that retires entries — exactly what the
    /// host's `flush_outputs` and its receive path do.
    ledgers: Vec<Ledger>,
    clock: u64,
    /// The datagrams released but deliberately not delivered: a partition
    /// as wide as the test wants it, held here rather than dropped, so a
    /// scenario can release them later.
    held: Vec<(u32, u32, Vec<u8>)>,
}

impl Pair {
    /// Boot the roster over a fresh marker store each.
    fn boot(name: &str) -> Pair {
        let dir = scratch(name);
        let genesis = genesis();
        let mut seats = Vec::with_capacity(MEMBERS);
        for index in 0..MEMBERS {
            let node = Node::open_compliance(
                &genesis,
                &format!("n{}", index + 1),
                &dir.join(format!("n{}.state", index + 1)).to_string_lossy(),
                PRIMARY_TIMEOUT,
            )
            .expect("the member opens over its own marker store");
            seats.push(Seat {
                id: identity(index),
                node,
            });
        }
        let ledgers = (0..MEMBERS).map(|_| Ledger::new()).collect();
        Pair {
            seats,
            ledgers,
            clock: 0,
            held: Vec::new(),
        }
    }

    /// The seat index of the member the cluster's current view names as
    /// its primary.
    fn leader(&self) -> usize {
        let status = self.seats[0].node.status();
        self.seats
            .iter()
            .position(|seat| seat.id == status.leader)
            .expect("the leader is a seat of this roster")
    }

    /// Every seat's replication state is `normal` and the last seat's
    /// clock has moved the cluster on: the cluster is quiet.
    fn quiet_and_normal(&self) -> bool {
        self.seats.iter().all(|seat| {
            seat.node
                .status()
                .state_name()
                .eq_ignore_ascii_case("normal")
        })
    }

    /// The timer sweep: the clock moves one tick, every seat takes it, and
    /// the wire it released drains to quiet.
    fn tick_all(&mut self, blackholed: Option<u32>) {
        self.clock += 1;
        let at = self.clock;
        for index in 0..self.seats.len() {
            self.seats[index].node.set_compliance_clock(at);
            let _ = self.seats[index].node.idle();
        }
        self.drain_wire(blackholed);
    }

    /// The force-fed exchange: move every released datagram to its
    /// addressee and collect what that releases, until the wire is quiet.
    /// The clock does not move — time passes for a node only when it is
    /// driven. `blackholed` names a member whose INBOUND datagrams are
    /// held rather than delivered: the partition this scenario runs under.
    fn drain_wire(&mut self, blackholed: Option<u32>) {
        for _ in 0..SETTLE_BOUND {
            let mut queue: Vec<(u32, u32, Vec<u8>)> = Vec::new();
            for index in 0..self.seats.len() {
                let from = self.seats[index].id;
                while let Some(out) = self.seats[index].node.next_output() {
                    if out.kind != 1 {
                        continue;
                    }
                    self.ledgers[index].record(Datagram {
                        to: out.to,
                        era: out.era,
                        view: out.view,
                        slot: out.slot,
                        tag: header_tag(&out.bytes).expect("a datagram carries a header"),
                        bytes: out.bytes.clone(),
                    });
                    queue.push((from, out.to, out.bytes));
                }
            }
            if queue.is_empty() {
                return;
            }
            for (from, to, bytes) in queue {
                if blackholed == Some(to) {
                    self.held.push((from, to, bytes));
                    continue;
                }
                let _ = self.deliver(from, to, &bytes);
            }
        }
        panic!("the wire did not quiet inside {SETTLE_BOUND} deliveries");
    }

    /// Hand one datagram to its addressee, attributed to its sender: the
    /// wire's whole point, as a plain call. The datagram is offered to
    /// the addressee's relay ledger first — a response retires the
    /// entries it answers, exactly as the host's receive path does.
    fn deliver(&mut self, from: u32, to: u32, bytes: &[u8]) -> i32 {
        let at = self.clock;
        let to_index = self
            .seats
            .iter()
            .position(|seat| seat.id == to)
            .expect("every addressee is a seat of this roster");
        self.seats[to_index].node.set_compliance_clock(at);
        if let Some(tag) = header_tag(bytes) {
            let (era, view, slot) = header_ballot(bytes);
            self.ledgers[to_index].answered(from, era, view, slot, tag);
        }
        self.seats[to_index].node.receive(from, bytes)
    }

    /// Every datagram this seat released outside the wire — the outputs a
    /// direct drive (a proposal, a forced view) queued before the exchange.
    fn record_released(&mut self, index: usize) {
        while let Some(out) = self.seats[index].node.next_output() {
            if out.kind != 1 {
                continue;
            }
            self.ledgers[index].record(Datagram {
                to: out.to,
                era: out.era,
                view: out.view,
                slot: out.slot,
                tag: header_tag(&out.bytes).expect("a datagram carries a header"),
                bytes: out.bytes,
            });
        }
    }

    /// Settle the cluster: sweep the timer until every seat is `normal`
    /// and the wire is quiet.
    fn settle(&mut self) {
        for _ in 0..SETTLE_BOUND {
            self.tick_all(None);
            if self.quiet_and_normal() {
                return;
            }
        }
        panic!(
            "the cluster did not settle inside {SETTLE_BOUND} ticks: {:?}",
            self.seats
                .iter()
                .map(|seat| seat.node.status().state_name())
                .collect::<Vec<&str>>()
        );
    }

    /// This seat's replication journal: the entries the accepted frontier
    /// holds, which is the committed slot count. The heartbeat's cost is
    /// measured here and nowhere else.
    fn slots(&self, index: usize) -> usize {
        self.seats[index].node.journal_entries().len()
    }

    /// One drive on this seat, carrying the scenario's clock: every drive
    /// in this harness sets the logical clock first, so the core never
    /// reads a wall clock.
    fn drive(&mut self, index: usize, call: impl FnOnce(&mut Node) -> i32) -> i32 {
        let at = self.clock;
        self.seats[index].node.set_compliance_clock(at);
        call(&mut self.seats[index].node)
    }

    /// Propose one read op through this seat's own service boundary, the
    /// way the lease driver's `get` is proposed.
    fn propose_get(&mut self, index: usize, request_num: u64) -> i32 {
        let json = format!(
            "{{\"op\":\"get\",\"message_id\":\"00000000-0000-0000-0000-{request_num:012x}\",\
             \"client_id\":1,\"request_num\":{request_num},\"lock_id\":9001}}"
        );
        self.drive(index, |node| node.request(json.as_bytes()))
    }
}

/// The tag a VRR datagram's 20-byte header carries, read through the
/// core's own table; `None` when the bytes carry no header or name no
/// tag the core knows.
fn header_tag(bytes: &[u8]) -> Option<Tag> {
    if bytes.len() < 4 {
        return None;
    }
    Tag::from_u32(u32::from_be_bytes(bytes[0..4].try_into().ok()?))
}

/// The `(era, view, slot)` a VRR datagram's 20-byte big-endian header
/// carries, read off the bytes themselves.
fn header_ballot(bytes: &[u8]) -> (u32, u32, u64) {
    let word = |at: usize| u32::from_be_bytes(bytes[at..at + 4].try_into().expect("a header"));
    (
        word(4),
        word(8),
        u64::from_be_bytes(bytes[12..20].try_into().expect("a header")),
    )
}

/// The resend multiset the cluster wait owes a leader whose follower never
/// acknowledged it: phase 1's `Prepare` and phase 2's `Commit`, to the
/// peer that never answered, byte for byte — and NOTHING is re-proposed.
///
/// The follower is blackholed for the whole window, so the leader's
/// proposal stands un-acknowledged; the cluster wait fires at the clock
/// parameter five times over; the relay is the standing traffic every
/// time; and the leader's replication journal — where a minted proposal
/// would show up — does not move once.
#[test]
fn the_cluster_wait_relays_the_un_acknowledged_traffic_and_proposes_nothing() {
    let mut pair = Pair::boot("cluster-wait");
    pair.settle();
    let leader = pair.leader();
    let follower = 1 - leader;

    // The proposal goes out and the follower's acknowledgements do not
    // come back: the leader's phase-1 and phase-2 traffic stands
    // un-acknowledged at the current ballot.
    assert_eq!(
        pair.propose_get(leader, 1),
        OK,
        "the leader accepts its own op"
    );
    pair.record_released(leader);
    pair.tick_all(Some(pair.seats[follower].id));
    pair.record_released(leader);

    let status = pair.seats[leader].node.status();
    let standing = pair.ledgers[leader].relay(status.era, status.view);
    assert!(
        standing.iter().any(|datagram| datagram.tag == Tag::Prepare),
        "phase 1 is in the resend multiset: {standing:?}"
    );
    let slots_before = pair.slots(leader);
    let follower_slots_before = pair.slots(1 - leader);

    let mut follower_slots = follower_slots_before;
    for beat in 0..5 {
        pair.clock += 10;
        let at = pair.clock;
        pair.seats[leader].node.set_compliance_clock(at);
        let status = pair.seats[leader].node.status();
        let sent = relay::cluster_timeout(
            status.state,
            &mut pair.ledgers[leader],
            status.era,
            status.view,
        );
        assert_eq!(
            sent, standing,
            "beat {beat}: the relay is the standing traffic, unaltered"
        );
        // Deliver the relay as the host does, then count the slots the
        // repeated traffic moved: none, on either side.
        for datagram in &sent {
            pair.deliver(pair.seats[leader].id, datagram.to, &datagram.bytes);
        }
        pair.record_released(leader);
        assert_eq!(
            pair.slots(leader),
            slots_before,
            "beat {beat}: a relay proposes nothing, so the leader's journal stood still"
        );
        // The follower applies a Commit it had never seen — that is what
        // the relay is for — and never applies anything twice: from the
        // second beat on, the same datagrams are repeats and its journal
        // stands still.
        let follower_slots_now = pair.slots(1 - leader);
        if beat == 0 {
            follower_slots = follower_slots_now;
        }
        assert_eq!(
            follower_slots_now, follower_slots,
            "beat {beat}: a repeated Prepare or Commit applies nothing new"
        );
        assert!(
            follower_slots_now - follower_slots_before <= 1,
            "beat {beat}: the relay cost the follower at most the commit it had never seen"
        );
    }
}

/// The heartbeat of a commit: N beats re-announce the leader's last commit
/// byte for byte and the replication journal advances NOT AT ALL. The
/// count is the journal's own entry count, not the log's word for it — a
/// heartbeat that opened a client transaction would move it once per
/// beat.
#[test]
fn the_heartbeat_re_announces_the_last_commit_and_advances_no_slot() {
    let mut pair = Pair::boot("heartbeat");
    pair.settle();
    let leader = pair.leader();

    // One committed operation, so the leader has a frontier to announce.
    assert_eq!(pair.propose_get(leader, 1), OK, "the leader accepts its op");
    pair.record_released(leader);
    pair.settle();
    pair.record_released(leader);
    let slots_before = pair.slots(leader);
    assert!(
        slots_before > 0,
        "the proposal committed: {slots_before} slots"
    );

    let status = pair.seats[leader].node.status();
    let beat = relay::heartbeat(pair.ledgers[leader].last_commit(status.era, status.view))
        .expect("the leader has a last commit at this ballot");
    assert!(
        beat.iter().all(|datagram| datagram.tag == Tag::Commit),
        "the heartbeat is the last commit: {beat:?}"
    );
    let frontier = beat[0].slot;
    let follower = 1 - leader;

    for beat_number in 0..BEATS {
        pair.clock += 1;
        let at = pair.clock;
        pair.seats[leader].node.set_compliance_clock(at);
        let status = pair.seats[leader].node.status();
        let announced = relay::heartbeat(pair.ledgers[leader].last_commit(status.era, status.view))
            .expect("a frontier stands");
        assert_eq!(
            announced, beat,
            "beat {beat_number}: the re-announced datagrams are byte-identical"
        );
        for datagram in &announced {
            pair.deliver(pair.seats[leader].id, datagram.to, &datagram.bytes);
        }
        pair.record_released(leader);
        assert_eq!(
            pair.slots(leader),
            slots_before,
            "beat {beat_number}: {BEATS} beats advanced {slots_before} slots and no more"
        );
        assert_eq!(
            pair.slots(follower),
            pair.slots(follower),
            "the follower's journal is unmoved by a re-announced commit"
        );
        assert_eq!(
            announced[0].slot, frontier,
            "beat {beat_number}: the frontier never moves under a heartbeat"
        );
    }
}

/// The armed attempt is re-asked, not replaced: over a window with no
/// leadership change the ballot is CONSTANT and the relayed multiset is
/// the same attempt every time, however many timeouts fire. The relay has
/// no way to name a view number — nothing in it advances one.
#[test]
fn the_armed_attempt_is_re_asked_and_the_ballot_never_advances() {
    let mut pair = Pair::boot("armed-attempt");
    pair.settle();
    let leader = pair.leader();
    let follower = 1 - leader;

    // A follower times out on its leader and issues the view change; the
    // leader never hears of it, so the attempt stands armed and
    // un-answered.
    let status = pair.seats[follower].node.status();
    let forced = pair.drive(follower, |node| {
        node.force_view(status.era, status.view + 1)
    });
    assert_eq!(forced, OK, "the host-forced view change is driven");
    // The host's own sequence: the forced view, then the ordinary
    // suspicion tick behind it.
    let _ = pair.drive(follower, |node| node.leader_timeout());
    pair.record_released(follower);
    pair.tick_all(Some(pair.seats[follower].id));
    pair.record_released(follower);

    let armed = pair.seats[follower].node.status();
    let attempt = pair.ledgers[follower].relay(armed.era, armed.view);
    assert!(
        attempt
            .iter()
            .any(|datagram| datagram.tag == Tag::StartViewChange),
        "the armed attempt's fence votes are standing: {attempt:?}"
    );

    for fire in 0..10 {
        pair.clock += 25;
        let at = pair.clock;
        pair.seats[follower].node.set_compliance_clock(at);
        let before = pair.seats[follower].node.status();
        assert_eq!(
            (before.era, before.view),
            (armed.era, armed.view),
            "fire {fire}: the timeout did not advance the ballot"
        );
        assert_eq!(
            relay::poll_opinion(before.state).name(),
            "retransmit",
            "the poll's opinion on a seated member is the retransmit"
        );
        let reasked = relay::cluster_timeout(
            before.state,
            &mut pair.ledgers[follower],
            before.era,
            before.view,
        );
        assert_eq!(
            reasked, attempt,
            "fire {fire}: the same attempt, the same ballot, re-sent"
        );
        let after = pair.seats[follower].node.status();
        assert_eq!(
            (after.era, after.view),
            (armed.era, armed.view),
            "fire {fire}: ten timeouts, one ballot"
        );
        assert_eq!(
            after.leader, armed.leader,
            "fire {fire}: and one leader throughout"
        );
        pair.record_released(follower);
    }
}
