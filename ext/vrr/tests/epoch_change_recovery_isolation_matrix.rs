mod support;

use support::{
    assert_complete_cases, assert_replica_unchanged, entry, message, node, receive, state,
    Relation, ReplicaSnapshot, EPOCH,
};
use vrr::vrr::{Body, Input, LogState, NodeId};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum EpochChangeMessage {
    StartEpochChange,
    DoEpochChange,
    StartEpoch,
}

impl EpochChangeMessage {
    const ALL: [Self; 3] = [
        Self::StartEpochChange,
        Self::DoEpochChange,
        Self::StartEpoch,
    ];

    fn carries_state(self) -> bool {
        !matches!(self, Self::StartEpochChange)
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum SenderEligibility {
    Eligible,
    Ineligible,
}

impl SenderEligibility {
    const ALL: [Self; 2] = [Self::Eligible, Self::Ineligible];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum StateValidity {
    Valid,
    Invalid,
}

impl StateValidity {
    const ALL: [Self; 2] = [Self::Valid, Self::Invalid];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct Case {
    message: EpochChangeMessage,
    epoch: Relation,
    sender: SenderEligibility,
    state: Option<StateValidity>,
}

fn sender(case: Case, epoch: u32) -> NodeId {
    let leader = epoch % 4;
    match (case.message, case.sender) {
        (EpochChangeMessage::StartEpochChange, SenderEligibility::Eligible) => 0,
        (EpochChangeMessage::StartEpochChange, SenderEligibility::Ineligible) => 4,
        (_, SenderEligibility::Eligible) => leader,
        (_, SenderEligibility::Ineligible) => (0..4)
            .find(|node| *node != leader && *node != 3)
            .expect("four-node replica has a non-leader peer"),
    }
}

fn transferred_state(validity: StateValidity) -> LogState {
    match validity {
        StateValidity::Valid => state(vec![entry(1, 1, 1)], 0),
        StateValidity::Invalid => LogState {
            slot: 2,
            commit: 0,
            log: vec![entry(1, 1, 1)],
        },
    }
}

fn body(case: Case) -> Body {
    match case.message {
        EpochChangeMessage::StartEpochChange => Body::StartEpochChange,
        EpochChangeMessage::DoEpochChange => Body::DoEpochChange {
            latest_normal: 0,
            state: transferred_state(case.state.expect("DO_EPOCH_CHANGE carries state")),
        },
        EpochChangeMessage::StartEpoch => Body::StartEpoch {
            state: transferred_state(case.state.expect("START_EPOCH carries state")),
        },
    }
}

#[test]
fn recovering_replica_isolates_the_complete_epoch_change_matrix() {
    let cases: Vec<_> = EpochChangeMessage::ALL
        .into_iter()
        .flat_map(|message| {
            Relation::ALL.into_iter().flat_map(move |epoch| {
                SenderEligibility::ALL.into_iter().flat_map(move |sender| {
                    if message.carries_state() {
                        StateValidity::ALL
                            .into_iter()
                            .map(move |state| Case {
                                message,
                                epoch,
                                sender,
                                state: Some(state),
                            })
                            .collect::<Vec<_>>()
                    } else {
                        vec![Case {
                            message,
                            epoch,
                            sender,
                            state: None,
                        }]
                    }
                })
            })
        })
        .collect();
    assert_complete_cases(
        "recovering epoch-change isolation",
        30,
        cases.iter().copied(),
    );

    for case in cases {
        let mut replica = node(4, 3);
        assert!(receive(
            &mut replica,
            1,
            message(
                EPOCH,
                0,
                Body::StartEpoch {
                    state: state(vec![], 0)
                }
            ),
        )
        .is_empty());
        assert_eq!(replica.epoch(), EPOCH);
        assert_eq!(replica.step(Input::Recover { nonce: 7 }).len(), 1);
        let before = ReplicaSnapshot::capture(&replica);

        let epoch = case.epoch.epoch();
        let body = body(case);
        let slot = match &body {
            Body::DoEpochChange { state, .. } | Body::StartEpoch { state } => state.slot,
            Body::StartEpochChange => 0,
            _ => unreachable!("matrix contains only epoch-change messages"),
        };
        let outputs = receive(
            &mut replica,
            sender(case, epoch),
            message(epoch, slot, body),
        );

        assert!(
            outputs.is_empty(),
            "recovering case {case:?} produced output"
        );
        assert_replica_unchanged(case, &before, &replica);
    }
}
