//! The seam's laws, proved rather than asserted in prose
//! (`docs/src/test-scaffold.md`, "The in-memory message-fed layer").
//!
//! Every scenario here drives the seam and nothing else: the seam is
//! `tests/seam/mod.rs`, the one harness the protocol-state expectations
//! and the re-homed signatures are written against.

#[path = "seam/mod.rs"]
mod seam;

use seam::{
    Emission, Expect, Inbound, Observable, Refusal, Restart, Seam, code_name, emission,
    emission_named, pair_of,
};

/// The genesis member pair of a two-member roster.
const ONE: &str = "1:1";
const TWO: &str = "2:1";

/// The primary timeout every scenario in this file plays by.
const TIMEOUT: u64 = 50;

/// The proposal exchange's named multiset: every frame a settled pair
/// produced for one opaque client proposal, byte for byte. An emission
/// beyond this set fails the assertion, by name.
fn named_proposal_exchange() -> Vec<Emission> {
    vec![
        emission_named(
            ONE,
            TWO,
            "0000000400000001000000000000000000000002040000000000000002",
        ),
        emission_named(
            ONE,
            TWO,
            "000000020000000100000000000000000000000302000000000000000300000001010000000076656374000000000000000100000005616c7068610000000000000002",
        ),
        emission_named(TWO, ONE, "000000030000000100000000000000000000000303"),
        emission_named(
            ONE,
            TWO,
            "0000000400000001000000000000000000000003040000000000000003",
        ),
    ]
}

/// A two-member cluster, settled. The smallest roster whose client
/// proposals round-trip the wire: the leader's phase-2 needs the
/// follower's acknowledgement.
fn settled_pair() -> Seam {
    let mut pair = Seam::boot(2, TIMEOUT).expect("the pair boots");
    pair.settle().expect("the pair settles");
    pair
}

/// A settled pair that has also carried one client proposal: the
/// leader's prepare and the follower's acceptance, on the wire. The
/// named multiset below is this exchange.
fn proposing_pair() -> Seam {
    let mut pair = settled_pair();
    pair.propose(Seam::identity(ONE), b"alpha")
        .expect("the leader takes the proposal");
    pair
}

/// One scenario, run start to finish: boot, settle, one client proposal,
/// a frame fed by hand, one timer sweep. Returns what it produced, so a
/// second run of the same function is the determinism control.
fn exchange_scenario() -> (Vec<Emission>, Vec<Refusal>) {
    let mut pair = proposing_pair();
    pair.feed_all(&[Inbound::named(TWO, ONE, "00")])
        .expect("the hand-fed frame is delivered");
    pair.tick_all().expect("the timer sweep runs");
    (pair.emissions().to_vec(), pair.refusals().to_vec())
}

// ----------------------------------------------------------------------
// Boot
// ----------------------------------------------------------------------

#[test]
fn a_boot_is_a_plain_call_and_nothing_is_emitted_by_it() {
    let pair = Seam::boot(2, TIMEOUT).expect("the pair boots");
    assert_eq!(pair.identities(), vec![ONE, TWO], "both seats are taken");
    assert!(pair.is_live(Seam::identity(ONE)));
    assert!(pair.is_live(Seam::identity(TWO)));
    // The clock is a parameter and it has not moved: a boot alone drives
    // nothing, so the wire is empty and the named multiset is empty.
    assert_eq!(pair.clock(), 0);
    pair.assert_emissions(&[]);
    pair.assert_refusals(&[]);
}

/// The proposal exchange's whole multiset, named byte-exactly. Every
/// frame it delivered is in this list; one frame beyond it fails
/// `assert_emissions` (the two tests below prove it).
#[test]
fn a_two_member_proposal_names_every_frame_byte_exactly() {
    let pair = proposing_pair();
    let states = pair.states();
    assert!(
        states
            .iter()
            .all(|state| state.status.as_deref() == Some("Normal")),
        "every member settles Normal: {states:#?}"
    );
    assert!(pair.is_live(Seam::identity(ONE)));
    assert!(pair.is_live(Seam::identity(TWO)));
    pair.assert_state(&Expect {
        deliveries: pair.emissions().to_vec(),
        post: vec![
            Observable {
                node: ONE.into(),
                status: Some("Normal".into()),
                members: Some(vec![ONE.into(), TWO.into()]),
                weights: Some(vec![1, 1]),
                ..Observable::default()
            },
            Observable {
                node: TWO.into(),
                status: Some("Normal".into()),
                members: Some(vec![ONE.into(), TWO.into()]),
                weights: Some(vec![1, 1]),
                ..Observable::default()
            },
        ],
    })
    .unwrap_or_else(|e| panic!("the corpus's own post comparison: {e}"));
    pair.assert_emissions(&named_proposal_exchange());
}

// ----------------------------------------------------------------------
// Determinism
// ----------------------------------------------------------------------

/// The determinism proof: the SAME script run twice produces byte-
/// identical emissions, in the same order, and the same refusals. Two
/// seams, two fresh marker stores, the same calls in the same order.
#[test]
fn the_same_script_twice_emits_byte_identical_frames() {
    let (first_emissions, first_refusals) = exchange_scenario();
    let (second_emissions, second_refusals) = exchange_scenario();

    assert_eq!(
        first_emissions, second_emissions,
        "the same script must emit the same frames, byte for byte"
    );
    assert_eq!(first_refusals, second_refusals, "the refusals too");
    assert!(
        !first_emissions.is_empty(),
        "the scenario produced frames to compare: {first_emissions:#?}"
    );
    // The byte-level statement, not the struct-level one: the whole
    // multiset as one hex string, in order.
    let flatten = |frames: &[Emission]| {
        frames
            .iter()
            .map(|frame| format!("{}->{}:{}\n", frame.from, frame.to, frame.wire))
            .collect::<String>()
    };
    assert_eq!(
        flatten(&first_emissions),
        flatten(&second_emissions),
        "the emitted bytes are identical"
    );
}

// ----------------------------------------------------------------------
// The named multiset fails on anything beyond it
// ----------------------------------------------------------------------

#[test]
#[should_panic(expected = "beyond the named multiset")]
fn an_emission_beyond_the_named_multiset_fails_the_test() {
    let pair = proposing_pair();
    // The names are the exchange's own, less the LAST frame: the frame
    // that is still on the wire must fail the assertion.
    let mut named = pair.emissions().to_vec();
    named.pop();
    assert!(!named.is_empty(), "the exchange emitted frames to withhold");
    pair.assert_emissions(&named);
}

#[test]
#[should_panic(expected = "named but never emitted")]
fn a_named_emission_that_never_left_fails_the_test() {
    let pair = proposing_pair();
    let mut named = pair.emissions().to_vec();
    named.push(emission(
        Seam::identity(ONE),
        Seam::identity(TWO),
        b"\xde\xad\xbe\xef",
    ));
    pair.assert_emissions(&named);
}

#[test]
fn the_named_emissions_can_be_written_from_either_spelling() {
    let pair = proposing_pair();
    let typed = pair
        .emissions()
        .first()
        .expect("the settle emitted a frame");
    let hex = typed.wire.clone();
    assert_eq!(
        emission(
            Seam::identity(&typed.from),
            Seam::identity(&typed.to),
            &seam::hex(&hex)
        ),
        *typed,
        "the typed spelling and the captured record are one shape"
    );
    assert_eq!(
        emission_named(&typed.from, &typed.to, &hex),
        *typed,
        "the named spelling goes through the same renderer"
    );
    // A frame a test writes names itself the same way the seam names what
    // it delivers, so a scenario can state the exchange from both ends.
    // `Inbound::named` takes (to, from) — the corpus's `Deliver` order —
    // and `emission_named` takes (from, to) — the delivery record's own
    // field order. Both state their order; a frame names itself as the
    // sender would see it leave.
    let frame = Inbound::named(TWO, ONE, "00");
    assert_eq!(frame.name(), emission_named(ONE, TWO, "00"));
    assert_eq!(
        frame.name(),
        emission(Seam::identity(ONE), Seam::identity(TWO), &[0]),
        "the typed spelling of a frame is one record"
    );
}

#[test]
fn a_bumped_life_announces_itself_over_the_seam() {
    let mut pair = settled_pair();
    let mark = pair.mark();
    pair.crash(Seam::identity(TWO))
        .expect("the follower is dropped");
    let risen = pair
        .restart(Seam::identity(TWO), Restart::Crashed)
        .expect("the reopen bumps the life");
    assert_eq!(pair_of(risen), "2:2", "the life is bumped, not reused");

    // The announcement the fenced-boot drive owes, then the ticks that
    // let the leader adopt it.
    pair.announce(risen).expect("the announcement is driven");
    drive_for(&mut pair, 16);
    assert!(
        !pair.emissions_since(mark).is_empty(),
        "the incarnation notice went out on the wire: {:?}",
        pair.emissions_since(mark)
    );
}

// ----------------------------------------------------------------------
// The drop discipline: every refusal named
// ----------------------------------------------------------------------

#[test]
fn every_refusal_is_named_by_node_call_and_code() {
    let mut one = Seam::boot(1, TIMEOUT).expect("the member boots");
    one.settle().expect("the member settles");
    let mark = one.mark();
    let refusal_mark = mark;

    // A datagram that names no message at all: dropped, and named.
    let frames = one
        .feed(&Inbound::new(
            Seam::identity(ONE),
            Seam::identity(ONE),
            [0u8; 4],
        ))
        .expect("the hand-fed frame is delivered");
    assert!(
        frames.is_empty(),
        "an undecodable datagram is answered with nothing: {frames:#?}"
    );
    // A client request that names no service verb: refused, and named.
    one.request(Seam::identity(ONE), b"{not json")
        .expect("the request is delivered");

    one.assert_emissions_since(mark, &[]);
    one.assert_refusals_since(
        refusal_mark,
        &[
            Refusal {
                node: ONE.into(),
                call: "receive",
                code: -5,
            },
            Refusal {
                node: ONE.into(),
                call: "request",
                code: -4,
            },
        ],
    );
    let named: Vec<String> = one
        .refusals_since(refusal_mark)
        .iter()
        .map(Refusal::describe)
        .collect();
    assert_eq!(
        named,
        vec![
            format!("{ONE} receive → {} (-5)", code_name(-5)),
            format!("{ONE} request → {} (-4)", code_name(-4)),
        ],
        "a refusal reads the same in a message as in a log line"
    );
}

#[test]
fn a_call_on_a_down_node_names_itself_and_emits_nothing() {
    let mut pair = settled_pair();
    pair.crash(Seam::identity(TWO))
        .expect("the follower is dropped");
    assert!(!pair.is_live(Seam::identity(TWO)), "the crash took it down");
    let mark = pair.mark();
    assert!(
        pair.tick(Seam::identity(TWO)).is_err(),
        "a tick on a down node is refused"
    );
    assert!(
        pair.feed(&Inbound::new(Seam::identity(TWO), Seam::identity(ONE), []))
            .is_err(),
        "a feed to a down node is refused"
    );
    assert!(pair.crash(Seam::identity(TWO)).is_err(), "no second crash");
    pair.assert_emissions_since(mark, &[]);
}

// ----------------------------------------------------------------------
// The lifecycle, each a plain call
// ----------------------------------------------------------------------

#[test]
fn a_shutdown_takes_the_stop_contract_and_lands_its_markers() {
    let mut one = Seam::boot(1, TIMEOUT).expect("the member boots");
    one.settle().expect("the member settles");
    let before = one.state(Seam::identity(ONE));
    one.shutdown(Seam::identity(ONE))
        .expect("the stop contract runs");
    assert!(
        !one.is_live(Seam::identity(ONE)),
        "the shutdown took it down"
    );

    let after = one.state(Seam::identity(ONE));
    assert!(
        after.status.is_none(),
        "a down node reports no status, only its markers: {after:#?}"
    );
    let rounds = after.markers.as_deref().unwrap_or_default();
    assert!(
        rounds.len() > before.markers.as_deref().unwrap_or_default().len(),
        "the halt wrote its marker rounds: {:?} then {rounds:?}",
        before.markers
    );
    assert!(
        rounds.starts_with(before.markers.as_deref().unwrap_or_default()),
        "the boot's own rounds are still there, in write order: {rounds:?}"
    );
    assert!(
        rounds.contains(&"drain".to_string()),
        "the halt drained its sink, and says so: {rounds:?}"
    );
    one.assert_refusals(&[]);
}

#[test]
fn a_crash_is_a_drop_without_the_stop_contract() {
    let mut one = Seam::boot(1, TIMEOUT).expect("the member boots");
    one.settle().expect("the member settles");
    let before = one.state(Seam::identity(ONE));
    one.crash(Seam::identity(ONE)).expect("the node is dropped");
    assert!(!one.is_live(Seam::identity(ONE)), "the crash took it down");

    // The proof that the stop contract did NOT run: a halt writes a
    // `drain` and its marker rounds, a crash writes nothing at all.
    let after = one.state(Seam::identity(ONE));
    assert_eq!(
        after.markers, before.markers,
        "a crash writes no marker: the schedule is exactly what the boot wrote"
    );
    assert!(
        !after
            .markers
            .as_deref()
            .unwrap_or(&[])
            .contains(&"drain".to_string()),
        "no drain landed: a crash never reached the stop path: {after:#?}"
    );
    one.assert_refusals(&[]);
}

#[test]
fn a_restart_over_the_same_markers_continues_the_life_after_a_shutdown() {
    let mut one = Seam::boot(1, TIMEOUT).expect("the member boots");
    one.settle().expect("the member settles");
    one.shutdown(Seam::identity(ONE))
        .expect("the stop contract runs");
    let revived = one
        .restart(Seam::identity(ONE), Restart::Clean)
        .expect("the reopen succeeds");
    assert_eq!(
        pair_of(revived),
        ONE,
        "a clean stop leaves the life to continue"
    );
    assert!(one.is_live(revived));
    one.settle().expect("the revived member settles");
    one.assert_refusals(&[]);
}

#[test]
fn a_restart_over_the_same_markers_bumps_the_life_after_a_crash() {
    let mut one = Seam::boot(1, TIMEOUT).expect("the member boots");
    one.settle().expect("the member settles");
    one.crash(Seam::identity(ONE)).expect("the node is dropped");
    let risen = one
        .restart(Seam::identity(ONE), Restart::Crashed)
        .expect("the reopen succeeds");
    assert_eq!(
        pair_of(risen),
        "1:2",
        "a crash leaves the next life to be claimed"
    );
    assert!(one.is_live(risen));
    assert_eq!(
        one.identities(),
        vec!["1:1", "1:2"],
        "the bumped life is a seat of its own"
    );
    assert_eq!(
        one.state(Seam::identity(ONE)).markers,
        Some(Vec::new()),
        "the superseded identity's marker schedule moved to the bumped life"
    );
    one.assert_refusals(&[]);
}

#[test]
fn a_restart_of_a_live_node_is_refused_by_name() {
    let mut pair = settled_pair();
    assert!(
        pair.restart(Seam::identity(ONE), Restart::Clean).is_err(),
        "a node that is up is not reopened over its markers"
    );
}

// ----------------------------------------------------------------------
// The levers a scenario drives
// ----------------------------------------------------------------------

#[test]
fn the_seam_drives_every_lever_a_scenario_names() {
    let mut pair = settled_pair();

    // The clock as a parameter: time passes without a drive...
    let before = pair.clock();
    pair.advance(4 * pair.primary_timeout());
    assert!(
        pair.clock() > before,
        "the clock moved by name, not by sleeping"
    );

    // ...and the suspicion that follows from it is driven, not waited on.
    pair.leader_timeout(Seam::identity(ONE))
        .expect("the leader timeout runs");
    pair.note_timeout(Seam::identity(ONE), true, pair.clock())
        .expect("the toggle is captured");

    // A host-forced view change at a named ballot.
    let leader = pair.state(Seam::identity(ONE));
    let (era, view) = (leader.era.unwrap_or(0), leader.view.unwrap_or(0));
    pair.force_view(Seam::identity(ONE), era, view + 1)
        .expect("the forced view change runs");
    drive_for(&mut pair, 16);

    // A typed cluster operation over the ordinary pipeline.
    pair.reconfigure(
        Seam::identity(ONE),
        seam::SystemOperation::Nominate {
            from: seam::View(0),
            offset: 1,
        },
    )
    .expect("the nomination is driven");
    drive_for(&mut pair, 16);

    // A client request through the ordinary boundary.
    pair.request(
        Seam::identity(ONE),
        br#"{"op":"acquire","key":"k","mode":"exclusive"}"#,
    )
    .expect("the request is driven");
    drive_for(&mut pair, 16);

    // The outside gossip shape and the drain-only sweep.
    pair.gossip(&[0, 1, 2, 3, 4, 5, 6, 7])
        .expect("the gossip is delivered");
    pair.deliver().expect("the wire is already quiet");

    // The joiner: a fresh identity over its own marker store.
    let joiner = Seam::identity("3:1");
    pair.join(joiner).expect("the joiner boots");
    assert!(pair.is_live(joiner));
    assert_eq!(pair.identities(), vec![ONE, TWO, "3:1"]);

    // The observer: every seat's post record, in seat order.
    let observed: Vec<Observable> = pair.states();
    assert_eq!(observed.len(), 3, "three seats are observable");
    assert!(
        observed.iter().all(|state| state.node.len() > 2),
        "each is named by its identity pair: {observed:#?}"
    );
    let expectation = pair.expectation_since(0);
    assert_eq!(expectation.deliveries, pair.emissions());
    assert_eq!(expectation.post.len(), observed.len());
}

/// The timer sweep, a fixed number of times: the drive that lets a
/// scenario's forced change take effect, without asserting what the
/// protocol makes of it — that is the protocol families' row, not the
/// seam's.
fn drive_for(pair: &mut Seam, ticks: usize) {
    for _ in 0..ticks {
        pair.tick_all().expect("every seat takes the tick");
    }
}

// ----------------------------------------------------------------------
// The seam-only proof, checked against the seam's own source
// ----------------------------------------------------------------------

/// The seam is message-fed and nothing else: no child process, no socket,
/// no port, no wall clock, no thread to lose a race against. The claim is
/// checked against the seam's own bytes rather than trusted, so a later
/// edit that reaches for any of them fails here.
#[test]
fn the_seam_carries_no_process_no_socket_and_no_wall_clock() {
    const FORBIDDEN: &[&str] = &[
        "std::net",
        "TcpStream",
        "UdpSocket",
        "UnixListener",
        "socket2",
        "Command",
        "fork",
        "spawn",
        "std::process",
        "std::thread",
        "thread::sleep",
        "Instant",
        "SystemTime",
    ];
    let source = include_str!("seam/mod.rs");
    for token in FORBIDDEN {
        assert!(
            !source.contains(token),
            "the seam's own source mentions `{token}`: the message-fed layer reaches \
             for nothing outside the adapter and the logical clock \
             (docs/src/test-scaffold.md, the in-memory message-fed layer)"
        );
    }
}
