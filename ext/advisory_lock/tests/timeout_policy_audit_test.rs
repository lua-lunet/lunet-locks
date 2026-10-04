//! The timeout-policy audit (`docs/src/timeout-policy.md`, "The pairs
//! our host acts on"; `docs/src/test-scaffold.md`, "The timeout-policy
//! audit").
//!
//! Two things are proved here, and neither is prose:
//!
//! 1. **The pin.** The flavoured-timeout matcher
//!    (`ext/uvrr-core/src/timeout.rs`) is a total function over eight
//!    states and eight timeout flavours. [`PINNED`] is that function's
//!    64-cell table, the one the upstream `timeout-policy` tool prints,
//!    and every cell is asserted against the matcher itself, so a
//!    matcher that moves cannot move quietly. The pin is also asserted
//!    against `docs/src/timeout-policy.md` cell for cell, which is what
//!    makes that document living documentation of the matcher rather
//!    than a copy of it.
//!
//! 2. **The drives.** [`DRIVES`] is the census of the clock events our
//!    hosts act on, enumerated from the hosts' own timer code — every
//!    cadence, deadline and forced transition in the adapter, the Rust
//!    example host and the Teal host — and every row names the pair it
//!    acts on, the site it lands at, and its disposition: the matcher's
//!    opinion, or one of the named divergences in [`DIVERGENCES`] that
//!    the documentation records with its reason. Four laws keep the
//!    census falsifiable rather than decorative: every site's anchor
//!    tokens must still be in the host's source, so a drive that is
//!    deleted or renamed is red; every pair the matcher answers with a
//!    protocol opinion must be claimed by a row, so an opinion with no
//!    host drive is red; every named divergence a row uses must be in
//!    the closed set AND stated in the documentation, so a silent
//!    exception is red; and every row's pair must agree with the pin.
//!
//! Where the seam can reach a drive, the drive is proved by observation
//! — the adapter's `(steady, steady)` view change, the `(booted,
//! booted)` re-announce, the `(stopping, stopping)` stop contract — and
//! a drive that names a site in a host binary the seam cannot boot is
//! proved by that site's anchors. Every drive the hosts own is one or
//! the other; none is asserted in prose alone.

// This audit drives the seam's three protocol levers and reads its
// emissions; the rest of the layer's vocabulary is named by the other
// binaries that include the module, so the re-exports this one does not
// reach are unused here rather than dead. The allow is at this line for
// that reason, the shape `tests/seam/mod.rs` documents for an including
// binary.
#[allow(dead_code, unused_imports)]
#[path = "seam/mod.rs"]
mod seam;

use std::fs;
use std::path::PathBuf;

use seam::{Restart, Seam, hex};
use vrr::timeout::{Opinion, State, Timeout, UNFLUSHED_RUNBOOK, matcher};
use vrr::wire::{Header, Tag, Unpack, UnpackCursor};

/// The two-member roster every seam proof below plays by: the smallest
/// roster whose fences are exchanged on the wire and whose view change
/// reaches a quorum.
const ONE: &str = "1:1";
const TWO: &str = "2:1";

/// The primary timeout every seam proof advances the logical clock past.
const TIMEOUT: u64 = 50;

// ----------------------------------------------------------------------
// The pin: the matcher's table, and the documentation of it
// ----------------------------------------------------------------------

/// The matcher's opinion for one (state, flavour) cell, named the way the
/// tool names it. `None` is `do-nothing`, the cell where a clock event
/// is not protocol and nothing follows from it.
type Cell = Option<&'static str>;

/// The pinned table: one row per [`State::ALL`] variant in the enum's
/// declared order, one cell per [`Timeout::ALL`] variant in its
/// declared order. Every cell is the upstream `timeout-policy` tool's
/// own verdict for that pair, and every one of them is checked against
/// the matcher below.
const PINNED: [[Cell; 8]; 8] = [
    // in-the-cluster
    [
        Some("retransmit"),
        None,
        None,
        None,
        None,
        Some("heartbeat"),
        None,
        None,
    ],
    // witness
    [None, None, None, None, None, None, None, None],
    // unknown
    [None, None, None, None, None, None, None, None],
    // booted
    [None, None, None, None, None, None, None, None],
    // crashed
    [None, None, None, None, None, None, None, None],
    // steady
    [
        Some("retransmit"),
        None,
        None,
        None,
        None,
        Some("start-view-change"),
        None,
        None,
    ],
    // stopping
    [None, None, None, None, None, None, None, None],
    // stopping-not-flushed: the sorry verdict under every flavour
    [
        Some("sorry"),
        Some("sorry"),
        Some("sorry"),
        Some("sorry"),
        Some("sorry"),
        Some("sorry"),
        Some("sorry"),
        Some("sorry"),
    ],
];

/// The pinned cell for a pair.
fn pinned(state: State, timeout: Timeout) -> Cell {
    let row = State::ALL
        .iter()
        .position(|s| *s == state)
        .expect("the state is in the closed domain");
    let column = Timeout::ALL
        .iter()
        .position(|t| *t == timeout)
        .expect("the flavour is in the closed domain");
    PINNED[row][column]
}

/// An opinion rendered the pinned way, so the pin and the matcher are
/// compared in one vocabulary.
fn rendered(opinion: Opinion) -> Cell {
    match opinion {
        Opinion::DoNothing => None,
        Opinion::Sorry { .. } => Some("sorry"),
        other => Some(other.name()),
    }
}

/// The matcher is total over the closed domain and every one of its 64
/// verdicts is the pinned one: the pin is the tool's table, checked
/// against the matcher rather than trusted.
#[test]
fn the_pinned_table_is_the_matchers_own_verdict_for_every_pair() {
    let mut cells = 0;
    for state in State::ALL {
        for timeout in Timeout::ALL {
            let opinion = matcher(state, timeout);
            assert_eq!(
                rendered(opinion),
                pinned(state, timeout),
                "the pin disagrees with the matcher at ({}, {}): the matcher says {}",
                state.name(),
                timeout.name(),
                opinion.name()
            );
            if let Opinion::Sorry { runbook } = opinion {
                assert_eq!(
                    runbook, UNFLUSHED_RUNBOOK,
                    "the sorry verdict carries the runbook statement, verbatim"
                );
            }
            cells += 1;
        }
    }
    assert_eq!(cells, 64, "the domain is eight states by eight flavours");
}

/// The domain the tool's usage enumerates is the domain the matcher
/// closes over, and the names are the tool's argument spellings: a
/// variant added to either enum fails here, and the pin would then be
/// missing a row.
#[test]
fn the_closed_domain_is_eight_states_by_eight_flavours() {
    assert_eq!(State::ALL.len(), 8);
    assert_eq!(Timeout::ALL.len(), 8);
    assert_eq!(
        State::ALL.map(State::name),
        [
            "in-the-cluster",
            "witness",
            "unknown",
            "booted",
            "crashed",
            "steady",
            "stopping",
            "stopping-not-flushed"
        ]
    );
    assert_eq!(
        Timeout::ALL.map(Timeout::name),
        [
            "cluster",
            "witness",
            "unknown",
            "booted",
            "crashed",
            "steady",
            "stopping",
            "stopping-not-flushed"
        ]
    );
}

/// The documentation of the pin is the pin: the table in
/// `docs/src/timeout-policy.md` is the matcher's, cell for cell. The doc
/// is written from the tool's output and this is the gate that keeps it
/// so — a matcher that moves, or a doc edited away from it, is red.
#[test]
fn the_documentation_states_the_matcher_cell_for_cell() {
    let doc = fs::read_to_string(repo_file("docs/src/timeout-policy.md"))
        .expect("the timeout-policy doc is in the tree");
    let documented = documented_table(&doc);
    assert_eq!(
        documented.len(),
        State::ALL.len(),
        "the doc states one row per state: {documented:?}"
    );
    for (row, state) in State::ALL.iter().enumerate() {
        for (column, timeout) in Timeout::ALL.iter().enumerate() {
            assert_eq!(
                documented[row][column + 1],
                pinned(*state, *timeout).unwrap_or("do-nothing"),
                "the doc's cell for ({}, {}) disagrees with the matcher",
                state.name(),
                timeout.name()
            );
        }
    }
    assert!(
        states_text(&doc, UNFLUSHED_RUNBOOK),
        "the doc states the sorry verdict's runbook statement, verbatim"
    );
}

// ----------------------------------------------------------------------
// The drives: the census of the clock events the hosts act on
// ----------------------------------------------------------------------

/// One clock event a host acts on: the pair it lands on, the host's own
/// name for the drive, the host and the file that own it, the tokens
/// that file must still carry for the drive to be there at all, and the
/// disposition — the matcher's opinion, or one of the named
/// divergences.
struct Drive {
    /// The protocol condition the host is in when its clock fires.
    state: State,
    /// The flavour of the wait that fired.
    timeout: Timeout,
    /// The drive, in the host's words.
    drive: &'static str,
    /// The host that owns the drive.
    host: &'static str,
    /// The source file, relative to the repository root.
    file: &'static str,
    /// The tokens the drive's site must carry: the function it lives in
    /// and the calls that make it this drive. A drive that is deleted,
    /// renamed or hollowed out loses one of them, and this audit is red.
    anchor: &'static [&'static str],
    /// The disposition: `None` when the drive IS the matcher's opinion,
    /// the divergence's name when it is not.
    divergence: Option<&'static str>,
}

/// The named divergences, each recorded in `docs/src/timeout-policy.md`
/// with its reason. A row names no divergence or one of these; the audit
/// carries no silent exception and no widened tolerance.
const DIVERGENCES: [(&str, &str); 1] = [(
    "boot-machine-reask",
    "a booted node's cadence re-asks what the boot machine owes, and the matcher \
         answers do-nothing on every flavour of a booted node because the machine owns \
         its own progress",
)];

/// The census. Enumerated from the hosts' timer code — the adapter's
/// drives, the Rust example host's timer plane and its client
/// deadlines, the Teal host's spawned loops — and not from the matcher's
/// table: a pair no host acts on is not a row here, and every pair the
/// matcher answers with a protocol opinion has at least one.
const DRIVES: [Drive; 18] = [
    // ---- the Rust example host, examples/lease-sequencer/src/
    Drive {
        state: State::InTheCluster,
        timeout: Timeout::Cluster,
        drive: "the lease driver's op correlation deadline: the un-acknowledged traffic is \
                relayed and the still-in-flight op is re-armed, never re-proposed",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "fn driver_step(&mut self, now: u64, rng: &mut Rng)",
            "if self.cluster_timeout(\"op-deadline\") > 0 {",
            "pending.deadline = now + OP_DEADLINE_MS;",
            "self.driver.pending = None;",
        ],
        divergence: None,
    },
    Drive {
        state: State::InTheCluster,
        timeout: Timeout::Cluster,
        drive: "the embedded client's op deadline: the overdue pending is counted for the \
                host, which relays the un-acknowledged traffic before the next op goes out",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/embedded_client.rs",
        anchor: &[
            "fn step( &mut self, now_ms: u64, deadline_ms: u64, submit: &mut dyn FnMut(&Action) -> bool, ) -> u32 {",
            "if now_ms >= pending.deadline {",
            "expired += 1;",
        ],
        divergence: None,
    },
    Drive {
        state: State::InTheCluster,
        timeout: Timeout::Cluster,
        drive: "the client connection's pending deadline: the verb's own protocol traffic is \
                relayed before the lock reply's correlation window closes the connection and \
                the admin verb's answers `deadline`",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "fn pending_deadline(host: &mut Host, index: usize, now: u64) -> bool",
            "host.cluster_timeout(\"admin-deadline\");",
            "host.cluster_timeout(\"lock-deadline\");",
        ],
        divergence: None,
    },
    Drive {
        state: State::InTheCluster,
        timeout: Timeout::Steady,
        drive: "the leader's idle beat: the leader's last commit re-announced, byte for \
                byte, which every follower's suspicion gate reads as idle-alive evidence",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "fn heartbeat_op(&mut self, now: u64) {",
            "relay::heartbeat(self.relay.last_commit(status.era, status.view))",
            "event = \"heartbeat-commit\"",
        ],
        divergence: None,
    },
    Drive {
        state: State::Steady,
        timeout: Timeout::Steady,
        drive: "the leader-failure detector's conclusion that its primary is dead: the \
                host-forced view at the next view number, then the timeout toggle",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "fn leader_timeout_step(&mut self, now: u64, rng: &mut Rng)",
            "let forced = self.node.force_view(status.era, status.view + 1); \
             if forced != 0 { let _ = self.node.leader_timeout(); }",
            "self.suspect(now, \"leader-timeout\");",
        ],
        divergence: None,
    },
    Drive {
        state: State::Steady,
        timeout: Timeout::Steady,
        drive: "the tick loop's own election wait: the core's ordinary suspicion input, \
                then the timeout toggle",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "fn timers(host: &mut Host, now: u64, rng: &mut Rng)",
            "host.leader_elapsed = 0; \
             let _ = host.node.leader_timeout(); \
             host.flush_outputs(now, rng); \
             host.suspect(now, \"election-wait\");",
            "event = \"election-wait-fire\"",
        ],
        divergence: None,
    },
    Drive {
        state: State::Steady,
        timeout: Timeout::Cluster,
        drive: "the cluster viewchange poll while the timeout toggle holds: inside the \
                view-change limbo the armed attempt is re-asked, the same ballot and the \
                same request set, and no view is manufactured",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "if host.timedout.timed_out() {",
            "let opinion = relay::poll_opinion(status.state);",
            "host.relay_out(datagram, \"viewchange-poll\")",
            "opinion = opinion.name(),",
        ],
        divergence: None,
    },
    Drive {
        state: State::Booted,
        timeout: Timeout::Booted,
        drive: "the recovery cadence: the fenced-boot drive's re-announce of the \
                `Reincarnation(old, new)` pair, then the tick",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "now.saturating_sub(host.last_recovery) >= host.recovery_ms",
            "let _ = host.node.recover();",
        ],
        divergence: Some("boot-machine-reask"),
    },
    Drive {
        state: State::Booted,
        timeout: Timeout::Booted,
        drive: "the rejoin gossip's resend: the entry ticket to every peer, until the \
                cluster's answer installs",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &["rejoin::GOSSIP_RESEND_MS", "host.join_gossip();"],
        divergence: Some("boot-machine-reask"),
    },
    Drive {
        state: State::Booted,
        timeout: Timeout::Booted,
        drive: "the boot-time discovery rounds on the 100 ms cadence, bounded by the \
                15 s deadline, after which the ordinary fenced boot proceeds",
        host: "the Rust example host",
        file: "examples/lease-sequencer/src/main.rs",
        anchor: &[
            "fn discovery_step(&mut self, now: u64)",
            "self.discovery.next_request_ms = now + 100;",
        ],
        divergence: Some("boot-machine-reask"),
    },
    // ---- the adapter, ext/advisory_lock/src/ffi.rs
    Drive {
        state: State::Booted,
        timeout: Timeout::Booted,
        drive: "the fenced-boot drive: the re-announce of the reincarnation pair on \
                every fenced drive until the node stops being fenced, then the tick",
        host: "the adapter",
        file: "ext/advisory_lock/src/ffi.rs",
        anchor: &[
            "pub fn recover(&mut self) -> i32 {",
            "let result = self.drive(Input::Reincarnate { old });",
            "self.drive(Input::Tick)",
        ],
        divergence: Some("boot-machine-reask"),
    },
    Drive {
        state: State::Stopping,
        timeout: Timeout::Stopping,
        drive: "the stop contract: the wire closes before any marker write, the first \
                marker round, the drain window, the drain-proven second round",
        host: "the adapter",
        file: "ext/advisory_lock/src/ffi.rs",
        anchor: &[
            "pub fn stop(&mut self) -> i32 {",
            "self.stopped = true;",
            "session.begin_stop()",
        ],
        divergence: None,
    },
    Drive {
        state: State::StoppingNotFlushed,
        timeout: Timeout::StoppingNotFlushed,
        drive: "the stop failure surface: every refusal arm prints the failure it hit \
                with the runbook statement and reports `SERVICE`; no timer re-drives the \
                flush",
        host: "the adapter",
        file: "ext/advisory_lock/src/ffi.rs",
        anchor: &[
            "trace_line!(\"stop.refuse.unseated-drain\");",
            "trace_line!(\"stop.refuse.first-round\");",
            "trace_line!(\"stop.refuse.drain-window\");",
            "trace_line!(\"stop.refuse.drain\");",
            "trace_line!(\"stop.refuse.second-round\");",
        ],
        divergence: None,
    },
    // ---- the Teal host, src/server.tl
    Drive {
        state: State::InTheCluster,
        timeout: Timeout::Steady,
        drive: "the idle beat: the cadence drives the core's liveness input, and a leader \
                with no Commit in the last interval re-announces its last commit",
        host: "the Teal host",
        file: "src/server.tl",
        anchor: &[
            "local function heartbeat_loop()",
            "node:idle()",
            "send_vrr(replica, commit.bytes)",
            "lunet.sleep(options.heartbeat_ms)",
        ],
        divergence: None,
    },
    Drive {
        state: State::Steady,
        timeout: Timeout::Steady,
        drive: "the election loop: the staggered election wait drives the leader-silence \
                detection",
        host: "the Teal host",
        file: "src/server.tl",
        anchor: &[
            "local function election_loop()",
            "leader_elapsed_ms >= options.election_ms + stagger",
            "node:leader_timeout()",
        ],
        divergence: None,
    },
    Drive {
        state: State::Booted,
        timeout: Timeout::Booted,
        drive: "the recovery loop: the fenced-boot drive while the replica is recovering",
        host: "the Teal host",
        file: "src/server.tl",
        anchor: &[
            "local function recovery_loop()",
            "node:status().state == \"recovering\" then",
            "node:recover()",
        ],
        divergence: Some("boot-machine-reask"),
    },
    Drive {
        state: State::Booted,
        timeout: Timeout::Booted,
        drive: "the boot-time discovery rounds across the remembered set, bounded by \
                the discovery deadline",
        host: "the Teal host",
        file: "src/server.tl",
        anchor: &[
            "local function discovery_loop()",
            "DISCOVERY_ROUND_TICKS",
            "discovery_done",
        ],
        divergence: Some("boot-machine-reask"),
    },
    Drive {
        state: State::Stopping,
        timeout: Timeout::Stopping,
        drive: "the stop hook: the runtime's teardown machinery runs the hosted node's \
                graceful stop exactly once, synchronous only",
        host: "the Teal host",
        file: "src/server.tl",
        anchor: &["lunet.on_stop(", "node:stop()"],
        divergence: None,
    },
];

/// Every drive the census names is still in its host's source: an anchor
/// is the drive's own function and the calls that make it this drive, so
/// a drive that is deleted, renamed, moved to a different call or
/// hollowed out takes this test red.
#[test]
fn every_drive_the_census_names_is_still_in_its_host() {
    for drive in &DRIVES {
        let source = flattened(
            &fs::read_to_string(repo_file(drive.file))
                .unwrap_or_else(|e| panic!("the host source {}: {e}", drive.file)),
        );
        for token in drive.anchor {
            assert!(
                source.contains(&flattened(token)),
                "{host}'s drive `{}` is no longer at {}: the source carries no `{token}`",
                drive.drive,
                drive.file,
                host = drive.host
            );
        }
    }
}

/// Every row sits on a pair the pin states, and every divergence a row
/// names is one of the named set: the census is either the matcher's
/// opinion or a named divergence, and never an exception in between.
#[test]
fn every_row_sits_on_a_pinned_pair_and_names_a_known_divergence() {
    for drive in &DRIVES {
        let opinion = matcher(drive.state, drive.timeout);
        assert_eq!(
            rendered(opinion),
            pinned(drive.state, drive.timeout),
            "the drive `{}` sits on ({}, {}), and the pin and the matcher disagree there",
            drive.drive,
            drive.state.name(),
            drive.timeout.name()
        );
        if let Some(name) = drive.divergence {
            assert!(
                DIVERGENCES
                    .iter()
                    .any(|(divergence, _)| *divergence == name),
                "the drive `{}` names the divergence `{name}`, which is not one of the \
                 named divergences",
                drive.drive
            );
        }
    }
}

/// Every opinion the matcher answers with, that is not `do-nothing`, is
/// claimed by the census. Where a state draws the same verdict whatever
/// the flavour — the unflushed stop answers `sorry` under all eight —
/// one claim covers the state, because there is one condition to drive.
/// Where a state answers a different verdict per flavour, every one of
/// those flavours is claimed separately: an opinion no host drive
/// answers is the hole this audit exists to close, and until its row is
/// written this is red.
#[test]
fn every_protocol_opinion_the_matcher_answers_is_claimed_by_a_drive() {
    for state in State::ALL {
        let answered: Vec<(Timeout, Opinion)> = Timeout::ALL
            .iter()
            .map(|timeout| (*timeout, matcher(state, *timeout)))
            .filter(|(_, opinion)| !matches!(opinion, Opinion::DoNothing))
            .collect();
        if answered.is_empty() {
            continue;
        }
        let uniform = answered
            .iter()
            .all(|(_, opinion)| rendered(*opinion) == rendered(answered[0].1));
        if uniform {
            assert!(
                DRIVES.iter().any(|drive| drive.state == state),
                "the matcher answers every ({}, *) pair with {} and no drive claims the \
                 state",
                state.name(),
                answered[0].1.name()
            );
            continue;
        }
        for (timeout, opinion) in answered {
            assert!(
                DRIVES
                    .iter()
                    .any(|drive| drive.state == state && drive.timeout == timeout),
                "the matcher answers ({}, {}) with {} and no drive claims it",
                state.name(),
                timeout.name(),
                opinion.name()
            );
        }
    }
}

/// The census names no drive twice: every row is a distinct clock event
/// in a distinct place.
#[test]
fn no_two_rows_name_the_same_drive() {
    for (index, drive) in DRIVES.iter().enumerate() {
        for other in &DRIVES[index + 1..] {
            assert!(
                !(drive.file == other.file
                    && drive.drive == other.drive
                    && drive.state == other.state
                    && drive.timeout == other.timeout),
                "two rows name the same drive: `{}` and `{}`",
                drive.drive,
                other.drive
            );
        }
    }
}

/// Every named divergence the census uses is recorded in the
/// documentation with its reason, and every named divergence the
/// documentation states is used by the census: a divergence nobody
/// drives is a stale one, and one the census uses that the doc omits is
/// a silent exception.
#[test]
fn the_named_divergences_are_the_documented_ones() {
    let doc = fs::read_to_string(repo_file("docs/src/timeout-policy.md"))
        .expect("the timeout-policy doc is in the tree");
    for (name, reason) in DIVERGENCES {
        assert!(
            doc.contains(name),
            "the divergence `{name}` is named by the census and the doc does not state it"
        );
        assert!(
            states_text(&doc, reason),
            "the divergence `{name}` is named by the census and the doc does not state its \
             reason: {reason}"
        );
        assert!(
            DRIVES.iter().any(|drive| drive.divergence == Some(name)),
            "the divergence `{name}` is named by the doc and no drive uses it"
        );
    }
}

/// A closed divergence cannot come back unnamed. Three drives took the
/// matcher's opinion by relay, and each one is forbidden the shape it
/// used to have: the leader's idle beat may not open a client
/// transaction (it re-announces the last commit), the viewchange poll
/// may not manufacture a view at the next view number (it re-asks the
/// armed attempt), and a correlation deadline may not retire an op whose
/// traffic is still in flight. Each is checked on the drive's OWN body,
/// so the check cannot be satisfied by a token elsewhere in the file.
#[test]
fn no_relay_drive_mints_anything() {
    let host = fs::read_to_string(repo_file("examples/lease-sequencer/src/main.rs"))
        .expect("the Rust host is in the tree");

    let beat = body(&host, "fn heartbeat_op(&mut self, now: u64) {");
    assert!(
        !beat.contains("node.request(") && !beat.contains("Uuid::new_v4"),
        "the idle beat re-announces the leader's last commit and opens no client \
         transaction: {beat}"
    );
    assert!(
        beat.contains("relay::heartbeat("),
        "and it is the heartbeat of a commit that it sends: {beat}"
    );

    let poll = body(&host, "if host.timedout.timed_out() {");
    assert!(
        !poll.contains("force_view("),
        "the viewchange poll re-asks the armed attempt and manufactures no view: {poll}"
    );
    assert!(
        poll.contains("relay::poll_opinion(") && poll.contains("relay_out("),
        "and the retransmit is the poll's own drive: {poll}"
    );

    let deadline = body(&host, "if let Some(pending) = &self.driver.pending {");
    assert!(
        deadline.contains("self.cluster_timeout(\"op-deadline\")"),
        "the correlation deadline relays before it decides the op's fate: {deadline}"
    );
    assert!(
        deadline.find("self.cluster_timeout(") < deadline.find("self.driver.pending = None;"),
        "the relay comes first and the retirement only follows an empty relay: {deadline}"
    );

    let teal =
        fs::read_to_string(repo_file("src/server.tl")).expect("the Teal host is in the tree");
    let beat = body(&teal, "local function heartbeat_loop()");
    assert!(
        beat.contains("last_commit") && beat.contains("send_vrr(replica, commit.bytes)"),
        "the Teal host's idle beat re-announces the last commit it released: {beat}"
    );
    assert!(
        !beat.contains("node:request("),
        "and it opens no client transaction: {beat}"
    );
}

// ----------------------------------------------------------------------
// The drives proved by observation, through the seam
// ----------------------------------------------------------------------

/// The `(steady, steady)` drive, observed: the follower that has heard
/// nothing for longer than its primary timeout, and takes the tick,
/// starts a view change. The tag multiset is the fence exchange the
/// opinion names — `StartViewChange` out, the leader's own back,
/// `DoViewChange` carrying the evidence, `StartView` installing the
/// ballot — and the ballot advances by exactly one view.
#[test]
fn the_steady_members_own_wait_starts_a_view_change() {
    let mut pair = settled_pair();
    let follower = Seam::identity(TWO);
    let before = pair.state(follower);
    pair.advance(4 * TIMEOUT);
    let frames = pair
        .leader_timeout(follower)
        .expect("the leader timeout is driven");
    assert_eq!(
        tags(&frames),
        vec![
            Tag::StartViewChange,
            Tag::StartViewChange,
            Tag::DoViewChange,
            Tag::StartView
        ],
        "the tick's whole exchange is the view change the opinion names: {frames:#?}"
    );
    let after = pair.state(follower);
    assert_eq!(
        after.view,
        before.view.map(|view| view + 1),
        "the ballot advanced by exactly one view and no further: {before:#?} then {after:#?}"
    );
    assert_eq!(
        after.era, before.era,
        "the era does not move on a suspicion"
    );
}

/// The same drive by the host-forced call: the ballot advances to exactly
/// the view named, and the core refuses a ballot that does not strictly
/// advance the view.
#[test]
fn the_host_forced_view_advances_the_ballot_to_the_view_it_names() {
    let mut pair = settled_pair();
    let follower = Seam::identity(TWO);
    let before = pair.state(follower);
    let era = before.era.expect("the era is observed");
    let view = before.view.expect("the view is observed");

    let mark = pair.mark();
    let sideways = pair
        .force_view(follower, era, view)
        .expect("the forced view is driven");
    assert_eq!(
        pair.emissions_since(mark),
        &[] as &[seam::Emission],
        "a ballot that does not strictly advance the view puts nothing on the wire: \
         {sideways:#?}"
    );
    assert_eq!(
        pair.state(follower).view,
        before.view,
        "a refused ballot moves nothing"
    );

    pair.force_view(follower, era, view + 1)
        .expect("the forced view is driven");
    assert_eq!(
        pair.state(follower).view,
        Some(view + 1),
        "the forced view at the next view number starts the change"
    );
}

/// The `(booted, booted)` drive, observed: the fenced-boot drive on a
/// bumped life re-announces its `Reincarnation(old, new)` pair to every
/// peer before the tick. The tag multiset is the re-announce and the
/// forced sequence the leader answers it with.
#[test]
fn the_booted_nodes_cadence_re_announces_the_reincarnation_pair() {
    let mut pair = settled_pair();
    pair.crash(Seam::identity(TWO))
        .expect("the follower is dropped");
    let risen = pair
        .restart(Seam::identity(TWO), Restart::Crashed)
        .expect("the reopen bumps the life");
    let mark = pair.mark();
    let frames = pair.announce(risen).expect("the fenced-boot drive runs");
    assert_eq!(
        tags(&frames),
        vec![
            Tag::Reincarnation,
            Tag::Reincarnation,
            Tag::Prepare,
            Tag::Prepare,
            Tag::PrepareOk
        ],
        "the fenced-boot drive's whole exchange is the re-announce and the leader's \
         forced sequence: {frames:#?}"
    );
    assert!(
        !pair.emissions_since(mark).is_empty(),
        "the announcement went out on the wire"
    );
}

/// The `(stopping, stopping)` drive, observed: the stop contract closes
/// the wire, lands its marker rounds and puts nothing on the wire at
/// all. No clock event re-drives the drain, which is what `do-nothing`
/// means on a stopping node.
#[test]
fn the_stopping_nodes_own_wait_emits_nothing() {
    let mut one = Seam::boot(1, TIMEOUT).expect("the member boots");
    one.settle().expect("the member settles");
    let mark = one.mark();
    let before = one.state(Seam::identity(ONE));
    one.shutdown(Seam::identity(ONE))
        .expect("the stop contract runs");
    one.assert_emissions_since(mark, &[]);
    let after = one.state(Seam::identity(ONE));
    let rounds = after.markers.as_deref().unwrap_or_default();
    assert!(
        rounds.len() > before.markers.as_deref().unwrap_or_default().len(),
        "the stop landed its marker rounds: {:?} then {rounds:?}",
        before.markers
    );
    assert!(
        rounds.contains(&"drain".to_string()),
        "the drain is proven, and says so: {rounds:?}"
    );
}

/// The `(stopping-not-flushed, *)` drive: the `sorry` verdict's opinion
/// IS the runbook statement, so the stop failure surface must carry it.
/// The stop contract's refusal arms are counted in the adapter's own
/// source and every one of them must carry the statement: a refusal arm
/// that stops carrying it leaves an operator with a failure and no
/// runbook.
#[test]
fn every_stop_refusal_arm_carries_the_runbook_statement() {
    let source = fs::read_to_string(repo_file("ext/advisory_lock/src/ffi.rs"))
        .expect("the adapter is in the tree");
    let body = stop_contract(&source);
    let refusals = body.matches("return SERVICE;").count();
    assert!(
        refusals > 1,
        "the stop contract has its refusal arms: {refusals}"
    );
    assert_eq!(
        body.matches("UNFLUSHED_RUNBOOK").count(),
        refusals,
        "every one of the stop contract's {refusals} refusal arms surfaces the runbook \
         statement the sorry verdict is"
    );
}

// ----------------------------------------------------------------------
// The harness's own helpers
// ----------------------------------------------------------------------

/// A two-member cluster, settled.
fn settled_pair() -> Seam {
    let mut pair = Seam::boot(2, TIMEOUT).expect("the pair boots");
    pair.settle().expect("the pair settles");
    pair
}

/// The wire tags of an emission list, in order, read off the wire
/// through the core's own header decoder: a drive's message multiset.
/// The header is the datagram's fixed-width head, so the cursor stops
/// there and the body that follows is not this function's business.
fn tags(frames: &[seam::Emission]) -> Vec<Tag> {
    frames
        .iter()
        .map(|frame| {
            let bytes = hex(&frame.wire);
            Header::unpack(&mut UnpackCursor::new(&bytes))
                .unwrap_or_else(|e| panic!("the frame {} -> {}: {e:?}", frame.from, frame.to))
                .tag
        })
        .collect()
}

/// The stop contract's own body in the adapter's source: from its
/// signature to the next method, so the refusal arms counted are its own
/// and not the file's.
fn stop_contract(source: &str) -> &str {
    let start = source
        .find("pub fn stop(&mut self) -> i32 {")
        .expect("the stop contract is in the adapter");
    let end = source[start..]
        .find("\n    pub fn ")
        .expect("the stop contract is a method among others");
    &source[start..start + end]
}

/// The doc's own table, read out of its markdown: the rows whose first
/// cell names a state, split into their cells.
fn documented_table(doc: &str) -> Vec<Vec<String>> {
    doc.lines()
        .filter_map(|line| {
            let cells: Vec<String> = line
                .split('|')
                .map(str::trim)
                .filter(|cell| !cell.is_empty())
                .map(str::to_string)
                .collect();
            (cells.len() == 9 && State::ALL.iter().any(|state| state.name() == cells[0]))
                .then_some(cells)
        })
        .collect()
}

/// Whether a document states a piece of text, comparing word by word so
/// that markdown's line wrapping and its blockquote markers are not read
/// as differences.
fn states_text(doc: &str, text: &str) -> bool {
    let words: Vec<&str> = doc
        .lines()
        .map(|line| line.trim_start_matches('>').trim())
        .flat_map(|line| line.split_whitespace())
        .collect();
    let wanted: Vec<&str> = text.split_whitespace().collect();
    !wanted.is_empty()
        && words
            .windows(wanted.len())
            .any(|window| window == wanted.as_slice())
}

/// A source with every run of whitespace collapsed to one space, so an
/// anchor names a sequence of statements and not the indentation they
/// happen to carry today.
fn flattened(source: &str) -> String {
    source.split_whitespace().collect::<Vec<&str>>().join(" ")
}

/// A named site's own body in a host's source: from the signature to the
/// line that closes the block, so the tokens read below are the site's
/// own and not the file's. The block's opening brace is the site's too —
/// in a Rust signature it is on the signature's own line, and in Teal it
/// is the next line — so the count starts there and not at the first
/// brace in sight, which may be a cast's.
fn body<'a>(source: &'a str, opening: &str) -> &'a str {
    let start = source
        .find(opening)
        .unwrap_or_else(|| panic!("the site `{opening}` is in the host"));
    let rest = &source[start..];
    let mut at = 0usize;
    let mut depth = 0i32;
    let mut opened = false;
    for line in rest.split_inclusive('\n') {
        let opens = opened || at == 0 && line.contains('{') || line.trim() == "{";
        if opens {
            opened = true;
            for character in line.chars() {
                match character {
                    '{' => depth += 1,
                    '}' => {
                        depth -= 1;
                        if depth == 0 {
                            return &rest[..at + line.len()];
                        }
                    }
                    _ => {}
                }
            }
        }
        at += line.len();
    }
    panic!("the site `{opening}` never closes");
}

/// A file in the repository, resolved from this crate's manifest
/// directory upwards: the documentation and the hosts the audit reads
/// are the repository's, not this crate's.
fn repo_file(relative: &str) -> PathBuf {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    for _ in 0..2 {
        root = root
            .parent()
            .expect("the crate sits two levels under the repository root")
            .to_path_buf();
    }
    root.join(relative)
}
