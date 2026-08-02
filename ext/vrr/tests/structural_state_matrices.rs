mod support;

use support::{
    assert_complete_cases, assert_replica_unchanged, entry, message, node, receive, state,
    ReplicaSnapshot,
};
use vrr::vrr::{Body, Input, LogState, Output, Replica, Status};

const NONCE: u64 = 71;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum TransferPath {
    DoEpochChange,
    StartEpoch,
    RecoveryResponse,
}

impl TransferPath {
    const ALL: [Self; 3] = [
        Self::DoEpochChange,
        Self::StartEpoch,
        Self::RecoveryResponse,
    ];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum HeaderRelation {
    Less,
    Equal,
    Greater,
}

impl HeaderRelation {
    const ALL: [Self; 3] = [Self::Less, Self::Equal, Self::Greater];

    fn slot(self, state_slot: u64) -> u64 {
        match self {
            Self::Less => state_slot.checked_sub(1).expect("state slot is positive"),
            Self::Equal => state_slot,
            Self::Greater => state_slot.checked_add(1).expect("state slot has headroom"),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum InvalidStateRule {
    LogLengthSlot,
    CommitSlot,
    ContiguousEntrySlots,
    DuplicateMessageIds,
    ClientRequestOrdering,
    ExecutedPrefixEquality,
    CommitExecuted,
}

impl InvalidStateRule {
    const ALL: [Self; 7] = [
        Self::LogLengthSlot,
        Self::CommitSlot,
        Self::ContiguousEntrySlots,
        Self::DuplicateMessageIds,
        Self::ClientRequestOrdering,
        Self::ExecutedPrefixEquality,
        Self::CommitExecuted,
    ];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum InvalidCase {
    Header(HeaderRelation),
    State(InvalidStateRule),
}

fn committed_entry() -> vrr::vrr::LogEntry {
    entry(1, 7, 2)
}

fn valid_state() -> LogState {
    state(vec![committed_entry(), entry(2, 8, 1)], 1)
}

fn invalid_state(rule: InvalidStateRule) -> LogState {
    let mut state = valid_state();
    match rule {
        InvalidStateRule::LogLengthSlot => state.slot = 3,
        InvalidStateRule::CommitSlot => state.commit = 3,
        InvalidStateRule::ContiguousEntrySlots => state.log[1].slot = 3,
        InvalidStateRule::DuplicateMessageIds => state.log[1].message_id = state.log[0].message_id,
        InvalidStateRule::ClientRequestOrdering => state.log[1] = entry(2, 7, 1),
        InvalidStateRule::ExecutedPrefixEquality => state.log[0].payload = b"different".to_vec(),
        InvalidStateRule::CommitExecuted => state.commit = 0,
    }
    state
}

fn executed_replica(index: usize) -> Replica {
    let mut replica = node(4, index);
    assert!(matches!(
        receive(
            &mut replica,
            0,
            message(
                0,
                1,
                Body::Prepare {
                    commit: 1,
                    entry: committed_entry(),
                },
            ),
        )
        .as_slice(),
        [Output::Execute { slot: 1, .. }, Output::To(0, _)]
            | [Output::To(0, _), Output::Execute { slot: 1, .. }]
    ));
    assert!(replica
        .step(Input::Complete {
            slot: 1,
            result: b"committed".to_vec(),
        })
        .is_empty());
    assert_eq!(replica.commit(), 1);
    assert_eq!(replica.executed(), 1);
    replica
}

fn do_epoch_change_recipient() -> Replica {
    let mut replica = executed_replica(1);
    replica.step(Input::LeaderTimeout);
    assert!(receive(&mut replica, 2, message(1, 1, Body::StartEpochChange)).is_empty());
    assert!(receive(&mut replica, 3, message(1, 1, Body::StartEpochChange)).is_empty());
    assert_eq!(replica.status(), Status::EpochChange);
    replica
}

fn refuse(path: TransferPath, header_slot: u64, state: LogState) {
    let mut replica = match path {
        TransferPath::DoEpochChange => do_epoch_change_recipient(),
        TransferPath::StartEpoch | TransferPath::RecoveryResponse => executed_replica(3),
    };
    if path == TransferPath::RecoveryResponse {
        assert_eq!(
            replica.step(Input::Recover { nonce: NONCE }),
            vec![Output::Broadcast(message(
                0,
                1,
                Body::Recovery { nonce: NONCE }
            ))]
        );
    }
    let before = ReplicaSnapshot::capture(&replica);
    let output = match path {
        TransferPath::DoEpochChange => receive(
            &mut replica,
            0,
            message(
                1,
                header_slot,
                Body::DoEpochChange {
                    latest_normal: 1,
                    state,
                },
            ),
        ),
        TransferPath::StartEpoch => receive(
            &mut replica,
            1,
            message(1, header_slot, Body::StartEpoch { state }),
        ),
        TransferPath::RecoveryResponse => receive(
            &mut replica,
            0,
            message(
                0,
                header_slot,
                Body::RecoveryResponse {
                    nonce: NONCE,
                    state: Some(state),
                },
            ),
        ),
    };
    assert!(output.is_empty(), "{path:?} refused state produced output");
    assert_replica_unchanged(path, &before, &replica);
}

#[test]
fn every_transfer_path_refuses_each_independent_invalid_state_rule() {
    assert_complete_cases("transfer paths", 3, TransferPath::ALL);
    assert_complete_cases("header slot relations", 3, HeaderRelation::ALL);
    assert_complete_cases("structural invalid rules", 7, InvalidStateRule::ALL);

    let invalid_cases: Vec<_> = HeaderRelation::ALL
        .into_iter()
        .filter(|relation| *relation != HeaderRelation::Equal)
        .map(InvalidCase::Header)
        .chain(InvalidStateRule::ALL.into_iter().map(InvalidCase::State))
        .collect();
    assert_complete_cases("one-rule-invalid states", 9, invalid_cases.iter().copied());

    let cases: Vec<_> = TransferPath::ALL
        .into_iter()
        .flat_map(|path| {
            invalid_cases
                .iter()
                .copied()
                .map(move |invalid| (path, invalid))
        })
        .collect();
    assert_complete_cases("invalid transfer-state matrix", 27, cases.iter().copied());

    for (path, invalid) in cases {
        let state = match invalid {
            InvalidCase::Header(_) => valid_state(),
            InvalidCase::State(rule) => invalid_state(rule),
        };
        let header_slot = match invalid {
            InvalidCase::Header(relation) => relation.slot(state.slot),
            InvalidCase::State(_) => state.slot,
        };
        refuse(path, header_slot, state);
    }
}

fn assert_installed(replica: &Replica) {
    assert_eq!(replica.status(), Status::Normal);
    assert_eq!(replica.slot(), 2);
    assert_eq!(replica.commit(), 1);
    assert_eq!(replica.executed(), 1);
    assert_eq!(replica.log(), valid_state().log.as_slice());
}

#[test]
fn valid_state_installations_preserve_committed_and_executed_prefixes_on_every_path() {
    let mut do_change = do_epoch_change_recipient();
    assert!(receive(
        &mut do_change,
        0,
        message(
            1,
            2,
            Body::DoEpochChange {
                latest_normal: 1,
                state: valid_state(),
            },
        ),
    )
    .is_empty());
    assert!(matches!(
        receive(
            &mut do_change,
            2,
            message(
                1,
                2,
                Body::DoEpochChange {
                    latest_normal: 1,
                    state: valid_state(),
                },
            ),
        )
        .as_slice(),
        [Output::Broadcast(_)]
    ));
    assert_installed(&do_change);

    let mut start_epoch = executed_replica(3);
    assert!(matches!(
        receive(
            &mut start_epoch,
            1,
            message(
                1,
                2,
                Body::StartEpoch {
                    state: valid_state()
                }
            ),
        )
        .as_slice(),
        [Output::To(1, _)]
    ));
    assert_installed(&start_epoch);

    let mut recovery = executed_replica(3);
    recovery.step(Input::Recover { nonce: NONCE });
    assert!(receive(
        &mut recovery,
        0,
        message(
            0,
            2,
            Body::RecoveryResponse {
                nonce: NONCE,
                state: Some(valid_state()),
            },
        ),
    )
    .is_empty());
    assert!(receive(
        &mut recovery,
        1,
        message(
            0,
            0,
            Body::RecoveryResponse {
                nonce: NONCE,
                state: None,
            },
        ),
    )
    .is_empty());
    assert!(receive(
        &mut recovery,
        2,
        message(
            0,
            0,
            Body::RecoveryResponse {
                nonce: NONCE,
                state: None,
            },
        ),
    )
    .is_empty());
    assert_installed(&recovery);
}

#[test]
fn executed_prefix_length_is_implied_by_structural_and_commit_cardinality() {
    // A reachable replica has executed <= commit. Thus valid length/slot and commit/slot
    // relations imply log length >= executed; there is no one-rule-invalid state for this guard.
    let prefix_lengths = [1_u64, 2, 3];
    assert_complete_cases("executed-prefix length cardinality", 3, prefix_lengths);

    for length in prefix_lengths {
        let mut replica = executed_replica(3);
        let mut log = vec![committed_entry()];
        for slot in 2..=length {
            log.push(entry(slot, slot + 10, 1));
        }
        let transferred = state(log, 1);
        assert!(
            transferred.log.len() >= replica.executed() as usize,
            "length {length} must cover the executed prefix"
        );
        receive(
            &mut replica,
            1,
            message(
                1,
                transferred.slot,
                Body::StartEpoch {
                    state: transferred.clone(),
                },
            ),
        );
        assert_eq!(replica.status(), Status::Normal);
        assert_eq!(replica.commit(), 1);
        assert_eq!(replica.executed(), 1);
        assert_eq!(replica.log(), transferred.log.as_slice());
    }
}
