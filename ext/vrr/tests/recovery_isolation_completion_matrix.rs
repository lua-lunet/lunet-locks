mod support;

use support::{
    assert_complete_cases, assert_replica_unchanged, entry, message, node, receive, request, state,
    ReplicaSnapshot,
};
use vrr::vrr::{Body, Input, LogState, NodeId, Output, Status};

const NONCE: u64 = 7;
const MEMBER_COUNTS: [usize; 2] = [3, 4];

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum LocalInput {
    Request,
    Idle,
    LeaderTimeout,
    Complete,
    Recover,
}

impl LocalInput {
    const ALL: [Self; 5] = [
        Self::Request,
        Self::Idle,
        Self::LeaderTimeout,
        Self::Complete,
        Self::Recover,
    ];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum PeerBody {
    Prepare,
    PrepareOk,
    Commit,
    StartEpochChange,
    DoEpochChange,
    StartEpoch,
    Recovery,
    RecoveryResponse,
}

impl PeerBody {
    const ALL: [Self; 8] = [
        Self::Prepare,
        Self::PrepareOk,
        Self::Commit,
        Self::StartEpochChange,
        Self::DoEpochChange,
        Self::StartEpoch,
        Self::Recovery,
        Self::RecoveryResponse,
    ];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Nonce {
    Match,
    Mismatch,
}

impl Nonce {
    const ALL: [Self; 2] = [Self::Match, Self::Mismatch];

    fn value(self) -> u64 {
        match self {
            Self::Match => NONCE,
            Self::Mismatch => NONCE + 1,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Provenance {
    ExactLeader,
    OtherMember,
    SelfNode,
    NonMember,
}

impl Provenance {
    const ALL: [Self; 4] = [
        Self::ExactLeader,
        Self::OtherMember,
        Self::SelfNode,
        Self::NonMember,
    ];

    fn from(self) -> NodeId {
        match self {
            Self::ExactLeader => 0,
            Self::OtherMember => 1,
            Self::SelfNode => 3,
            Self::NonMember => 4,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Transfer {
    Absent,
    Valid,
    Malformed,
}

impl Transfer {
    const ALL: [Self; 3] = [Self::Absent, Self::Valid, Self::Malformed];

    fn state(self) -> Option<LogState> {
        match self {
            Self::Absent => None,
            Self::Valid => Some(state(vec![entry(1, 40, 1)], 0)),
            Self::Malformed => Some(LogState {
                slot: 2,
                commit: 0,
                log: vec![entry(1, 41, 1)],
            }),
        }
    }
}

fn recovering() -> vrr::vrr::Replica {
    let mut replica = node(4, 3);
    assert_eq!(
        replica.step(Input::Recover { nonce: NONCE }),
        vec![Output::Broadcast(message(
            0,
            0,
            Body::Recovery { nonce: NONCE }
        ))]
    );
    replica
}

#[test]
fn recovering_local_input_matrix_refuses_every_input_except_host_recover() {
    assert_complete_cases("recovering local inputs", 5, LocalInput::ALL);

    for input in LocalInput::ALL {
        let mut replica = recovering();
        let before = ReplicaSnapshot::capture(&replica);
        match input {
            LocalInput::Request => {
                assert!(replica.step(request(1, 1)).is_empty());
                assert_replica_unchanged(input, &before, &replica);
            }
            LocalInput::Idle => {
                assert!(replica.step(Input::Idle).is_empty());
                assert_replica_unchanged(input, &before, &replica);
            }
            LocalInput::LeaderTimeout => {
                assert!(replica.step(Input::LeaderTimeout).is_empty());
                assert_replica_unchanged(input, &before, &replica);
            }
            LocalInput::Complete => {
                assert!(replica
                    .step(Input::Complete {
                        slot: 1,
                        result: b"result".to_vec(),
                    })
                    .is_empty());
                assert_replica_unchanged(input, &before, &replica);
            }
            LocalInput::Recover => assert_eq!(
                replica.step(Input::Recover { nonce: NONCE + 1 }),
                vec![Output::Broadcast(message(
                    0,
                    0,
                    Body::Recovery { nonce: NONCE + 1 },
                ))],
                "host recovery restarts the recovery attempt"
            ),
        }
    }
}

#[test]
fn recovering_peer_body_matrix_refuses_every_non_response_form() {
    assert_complete_cases("recovering peer bodies", 8, PeerBody::ALL);

    for body in PeerBody::ALL {
        let mut replica = recovering();
        let before = ReplicaSnapshot::capture(&replica);
        let message = match body {
            PeerBody::Prepare => message(
                0,
                1,
                Body::Prepare {
                    commit: 0,
                    entry: entry(1, 1, 1),
                },
            ),
            PeerBody::PrepareOk => message(0, 0, Body::PrepareOk),
            PeerBody::Commit => message(0, 0, Body::Commit),
            PeerBody::StartEpochChange => message(1, 0, Body::StartEpochChange),
            PeerBody::DoEpochChange => message(
                1,
                0,
                Body::DoEpochChange {
                    latest_normal: 0,
                    state: state(vec![], 0),
                },
            ),
            PeerBody::StartEpoch => message(
                1,
                0,
                Body::StartEpoch {
                    state: state(vec![], 0),
                },
            ),
            PeerBody::Recovery => message(0, 0, Body::Recovery { nonce: NONCE }),
            PeerBody::RecoveryResponse => message(
                0,
                1,
                Body::RecoveryResponse {
                    nonce: NONCE + 1,
                    state: Some(state(vec![entry(1, 1, 1)], 0)),
                },
            ),
        };
        assert!(receive(&mut replica, 0, message).is_empty(), "{body:?}");
        assert_replica_unchanged(body, &before, &replica);
    }
}

#[test]
fn recovery_response_nonce_provenance_and_transfer_matrix_is_finite() {
    let cases: Vec<_> = Nonce::ALL
        .into_iter()
        .flat_map(|nonce| {
            Provenance::ALL.into_iter().flat_map(move |provenance| {
                Transfer::ALL
                    .into_iter()
                    .map(move |transfer| (nonce, provenance, transfer))
            })
        })
        .collect();
    assert_complete_cases("recovery response admission", 24, cases.iter().copied());

    for (nonce, provenance, transfer) in cases {
        let mut replica = recovering();
        let before = ReplicaSnapshot::capture(&replica);
        let state = transfer.state();
        let slot = state.as_ref().map_or(0, |state| state.slot);
        let outputs = receive(
            &mut replica,
            provenance.from(),
            message(
                0,
                slot,
                Body::RecoveryResponse {
                    nonce: nonce.value(),
                    state,
                },
            ),
        );
        let accepted = nonce == Nonce::Match
            && provenance == Provenance::ExactLeader
            && transfer == Transfer::Valid;
        assert!(outputs.is_empty(), "{nonce:?} {provenance:?} {transfer:?}");
        if accepted {
            assert_eq!(replica.status(), Status::Recovering);
        } else {
            assert_replica_unchanged((nonce, provenance, transfer), &before, &replica);
        }
    }
}

#[test]
fn recovery_quorum_matrix_requires_distinct_responses_and_selects_maximum_epoch() {
    assert_complete_cases("recovery member counts", 2, MEMBER_COUNTS);

    for count in MEMBER_COUNTS {
        let quorum = count / 2 + 1;
        let responders: Vec<NodeId> = (0..count as NodeId)
            .filter(|node| *node != count as NodeId - 1)
            .collect();
        assert_complete_cases(
            format!("K={count} responders"),
            quorum,
            responders.iter().copied(),
        );

        let mut replica = node(count, count - 1);
        replica.step(Input::Recover { nonce: NONCE });
        for from in responders.iter().copied().filter(|from| *from != 0) {
            assert!(receive(
                &mut replica,
                from,
                message(
                    from,
                    1,
                    Body::RecoveryResponse {
                        nonce: NONCE,
                        state: Some(state(vec![entry(1, 10 + from as u64, 1)], 0)),
                    },
                ),
            )
            .is_empty());
            assert_eq!(
                replica.status(),
                Status::Recovering,
                "K={count}, Q-1 distinct"
            );
        }

        let before_duplicate = ReplicaSnapshot::capture(&replica);
        let duplicate = responders[1];
        assert!(receive(
            &mut replica,
            duplicate,
            message(
                duplicate,
                1,
                Body::RecoveryResponse {
                    nonce: NONCE,
                    state: Some(state(vec![entry(1, 10 + duplicate as u64, 1)], 0)),
                },
            ),
        )
        .is_empty());
        assert_replica_unchanged((count, "duplicate responder"), &before_duplicate, &replica);

        let maximum_epoch = count as u32;
        assert_eq!(
            maximum_epoch % count as u32,
            0,
            "K={count} exact maximum leader"
        );
        assert!(receive(
            &mut replica,
            0,
            message(
                maximum_epoch,
                1,
                Body::RecoveryResponse {
                    nonce: NONCE,
                    state: Some(state(vec![entry(1, 90, 1)], 0)),
                },
            ),
        )
        .is_empty());
        assert_eq!(
            replica.status(),
            Status::Normal,
            "K={count} valid quorum activates"
        );
        assert_eq!(
            replica.epoch(),
            maximum_epoch,
            "K={count} selects maximum epoch"
        );
        assert_eq!(
            replica.log(),
            &[entry(1, 90, 1)],
            "K={count} adopts maximum leader state"
        );
    }
}

#[test]
fn recovery_quorum_refuses_completion_when_the_maximum_epoch_lacks_its_leader_state() {
    assert_complete_cases("missing maximum leader K", 2, MEMBER_COUNTS);

    for count in MEMBER_COUNTS {
        let quorum = count / 2 + 1;
        let responders: Vec<NodeId> = (0..count as NodeId)
            .filter(|node| *node != count as NodeId - 1)
            .collect();
        let mut replica = node(count, count - 1);
        replica.step(Input::Recover { nonce: NONCE });
        let before = ReplicaSnapshot::capture(&replica);

        for from in responders {
            let epoch = if from == 0 { count as u32 - 1 } else { from };
            let state =
                (from == epoch % count as u32).then(|| state(vec![entry(1, from as u64, 1)], 0));
            assert!(receive(
                &mut replica,
                from,
                message(
                    epoch,
                    state.as_ref().map_or(0, |state| state.slot),
                    Body::RecoveryResponse {
                        nonce: NONCE,
                        state,
                    }
                ),
            )
            .is_empty());
        }
        assert_eq!(
            quorum,
            count - 1,
            "K={count} all other members are exactly Q responders"
        );
        assert_eq!(
            replica.status(),
            Status::Recovering,
            "K={count} maximum leader is absent"
        );
        assert_replica_unchanged((count, "missing maximum leader state"), &before, &replica);
    }
}
