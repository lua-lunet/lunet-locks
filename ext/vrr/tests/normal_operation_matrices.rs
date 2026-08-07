mod support;

use support::{
    assert_complete_cases, assert_replica_unchanged, entry, id, message, node, receive, request,
    state, Relation, ReplicaSnapshot, EPOCH, EXECUTION_TIME,
};
use vrr::vrr::{Body, Input, LogEntry, Message, Output, Replica, Status};

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Provenance {
    Leader,
    Backup,
    SelfNode,
    NonMember,
}

impl Provenance {
    const ALL: [Self; 4] = [Self::Leader, Self::Backup, Self::SelfNode, Self::NonMember];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum AckCount {
    First,
    Duplicate,
    Quorum,
}

impl AckCount {
    const ALL: [Self; 3] = [Self::First, Self::Duplicate, Self::Quorum];
}

fn exact_prepare(epoch: u32, slot: u64, request_num: u64, message_id: uuid::Uuid) -> Output {
    Output::Broadcast(Message {
        epoch,
        slot,
        body: Body::Prepare {
            commit: 0,
            entry: LogEntry {
                slot,
                client_id: 1,
                request_num,
                message_id,
                execution_time: EXECUTION_TIME,
                payload: b"request".to_vec(),
            },
        },
    })
}

fn leader_with_request() -> Replica {
    let mut replica = node(4, 0);
    assert_eq!(
        replica.step(request(1, 1)),
        vec![exact_prepare(0, 1, 1, id(1))]
    );
    replica
}

fn epoch_leader_with_request() -> Replica {
    let mut replica = node(4, 1);
    for _ in 0..EPOCH {
        replica.step(Input::LeaderTimeout);
    }
    for from in [0, 2] {
        receive(
            &mut replica,
            from,
            message(EPOCH, 0, Body::StartEpochChange),
        );
    }
    for from in [0, 2] {
        receive(
            &mut replica,
            from,
            message(
                EPOCH,
                0,
                Body::DoEpochChange {
                    latest_normal: 0,
                    state: state(vec![], 0),
                },
            ),
        );
    }
    assert_eq!(replica.epoch(), EPOCH);
    assert_eq!(replica.status(), Status::Normal);
    assert_eq!(
        replica.step(request(1, 1)),
        vec![exact_prepare(EPOCH, 1, 1, id(1))]
    );
    replica
}

fn epoch_backup_with_prepare() -> Replica {
    let mut replica = node(4, 2);
    assert!(receive(
        &mut replica,
        1,
        message(
            1,
            0,
            Body::StartEpoch {
                state: state(vec![], 0)
            }
        ),
    )
    .is_empty());
    assert_eq!(replica.epoch(), 1);
    assert_eq!(
        receive(
            &mut replica,
            1,
            message(
                1,
                1,
                Body::Prepare {
                    commit: 0,
                    entry: entry(1, 23, 29)
                }
            ),
        ),
        vec![Output::To(1, message(1, 1, Body::PrepareOk))]
    );
    replica
}

fn backup_with_prepare() -> Replica {
    let mut replica = node(4, 1);
    let first = entry(1, 23, 29);
    assert_eq!(
        receive(
            &mut replica,
            0,
            message(
                0,
                1,
                Body::Prepare {
                    commit: 0,
                    entry: first,
                },
            ),
        ),
        vec![Output::To(0, message(0, 1, Body::PrepareOk))]
    );
    replica
}

#[test]
fn client_request_ordering_and_message_replay_matrix() {
    assert_complete_cases("client request number relation", 3, Relation::ALL);

    for request_relation in Relation::ALL {
        let mut replica = node(3, 0);
        assert_eq!(
            replica.step(request(1, 29)),
            vec![exact_prepare(0, 1, 29, id(1))]
        );
        let before = ReplicaSnapshot::capture(&replica);
        let outputs = replica.step(request(2, request_relation.request_num()));
        match request_relation {
            Relation::Less | Relation::Equal => {
                assert!(outputs.is_empty(), "request relation {request_relation:?}");
                assert_replica_unchanged(request_relation, &before, &replica);
            }
            Relation::Greater => assert_eq!(outputs, vec![exact_prepare(0, 2, 30, id(2))]),
        }
    }

    let mut replica = node(3, 0);
    assert_eq!(
        replica.step(request(7, 1)),
        vec![exact_prepare(0, 1, 1, id(7))]
    );
    let before = ReplicaSnapshot::capture(&replica);
    assert!(replica.step(request(7, 2)).is_empty());
    assert_replica_unchanged("duplicate message ID", &before, &replica);
}

#[test]
fn prepare_continuity_duplicate_conflict_and_gap_matrix() {
    assert_complete_cases("prepare continuity relation", 3, Relation::ALL);

    for slot_relation in Relation::ALL {
        let mut replica = backup_with_prepare();
        let slot = match slot_relation {
            Relation::Less => 0,
            Relation::Equal => 1,
            Relation::Greater => 2,
        };
        let prepare = message(
            0,
            slot,
            Body::Prepare {
                commit: 0,
                entry: if slot_relation == Relation::Equal {
                    entry(1, 23, 29)
                } else {
                    entry(slot, 24, 30)
                },
            },
        );
        let before = ReplicaSnapshot::capture(&replica);
        let outputs = receive(&mut replica, 0, prepare);
        match slot_relation {
            Relation::Equal => {
                assert_eq!(outputs, vec![Output::To(0, message(0, 1, Body::PrepareOk))])
            }
            Relation::Greater => {
                assert_eq!(outputs, vec![Output::To(0, message(0, 2, Body::PrepareOk))]);
                assert_eq!(replica.slot(), 2);
            }
            Relation::Less => {
                assert!(
                    outputs.is_empty(),
                    "prepare slot relation {slot_relation:?}"
                );
                assert_replica_unchanged(slot_relation, &before, &replica);
            }
        }
    }

    let mut replica = backup_with_prepare();
    let mut conflict = entry(1, 23, 29);
    conflict.payload = b"conflict".to_vec();
    let before = ReplicaSnapshot::capture(&replica);
    assert!(receive(
        &mut replica,
        0,
        message(
            0,
            1,
            Body::Prepare {
                commit: 0,
                entry: conflict
            }
        ),
    )
    .is_empty());
    assert_replica_unchanged("conflicting duplicate prepare", &before, &replica);

    let before = ReplicaSnapshot::capture(&replica);
    assert!(receive(
        &mut replica,
        0,
        message(
            0,
            3,
            Body::Prepare {
                commit: 0,
                entry: entry(3, 24, 30)
            }
        ),
    )
    .is_empty());
    assert_replica_unchanged("gapped prepare", &before, &replica);
}

#[test]
fn prepare_ok_provenance_slot_epoch_and_quorum_matrix() {
    assert_complete_cases("prepare-ok provenance", 4, Provenance::ALL);
    assert_complete_cases("prepare-ok epoch relation", 3, Relation::ALL);
    assert_complete_cases("prepare-ok slot relation", 3, Relation::ALL);
    assert_complete_cases("prepare-ok count", 3, AckCount::ALL);

    for provenance in Provenance::ALL {
        for epoch_relation in Relation::ALL {
            for slot_relation in Relation::ALL {
                let mut replica = epoch_leader_with_request();
                let from = match provenance {
                    Provenance::Leader | Provenance::SelfNode => 1,
                    Provenance::Backup => 0,
                    Provenance::NonMember => 4,
                };
                let before = ReplicaSnapshot::capture(&replica);
                let slot = slot_relation.slot() - 18;
                let outputs = receive(
                    &mut replica,
                    from,
                    message(epoch_relation.epoch(), slot, Body::PrepareOk),
                );
                // Admission per the core's guard (`src/vrr.rs`): a member
                // PrepareOk in the replica's epoch at or below the frontier
                // is counted in the quorum accumulator. A below-frontier
                // (slot 0) vote names no log entry and can never advance
                // commit (`commit.max(0)` is a no-op), so admission is
                // observable only in the accumulator; a single vote is below
                // the K=4 quorum of two external votes.
                let accepted = provenance == Provenance::Backup
                    && epoch_relation == Relation::Equal
                    && slot_relation != Relation::Greater;
                if accepted {
                    assert!(outputs.is_empty());
                    assert_eq!(replica.commit(), 0);
                    let mut expected = before.diagnostic().clone();
                    expected.prepare_oks.entry(slot).or_default().insert(from);
                    let after = ReplicaSnapshot::capture(&replica);
                    assert_eq!(
                        after.diagnostic(),
                        &expected,
                        "accepted prepare-ok {provenance:?} {epoch_relation:?} \
                         {slot_relation:?} adds exactly the quorum vote"
                    );
                } else {
                    assert!(
                        outputs.is_empty(),
                        "prepare-ok {provenance:?} {epoch_relation:?} {slot_relation:?}"
                    );
                    assert_replica_unchanged(
                        (provenance, epoch_relation, slot_relation),
                        &before,
                        &replica,
                    );
                }
            }
        }
    }

    let mut replica = leader_with_request();
    assert!(receive(&mut replica, 1, message(0, 1, Body::PrepareOk)).is_empty());
    assert!(receive(&mut replica, 1, message(0, 1, Body::PrepareOk)).is_empty());
    assert_eq!(
        receive(&mut replica, 2, message(0, 1, Body::PrepareOk)),
        vec![Output::Execute {
            slot: 1,
            client_id: 1,
            request_num: 1,
            message_id: id(1),
            execution_time: EXECUTION_TIME,
            payload: b"request".to_vec(),
        }]
    );
    assert_eq!(replica.commit(), 1);
}

#[test]
fn commit_provenance_slot_and_epoch_matrix() {
    assert_complete_cases("commit provenance", 4, Provenance::ALL);
    assert_complete_cases("commit epoch relation", 3, Relation::ALL);
    assert_complete_cases("commit slot relation", 3, Relation::ALL);

    for provenance in Provenance::ALL {
        for epoch_relation in Relation::ALL {
            for slot_relation in Relation::ALL {
                let mut replica = epoch_backup_with_prepare();
                let from = match provenance {
                    Provenance::Leader => 1,
                    Provenance::Backup => 3,
                    Provenance::SelfNode => 2,
                    Provenance::NonMember => 4,
                };
                let slot = match slot_relation {
                    Relation::Less => 0,
                    Relation::Equal => 1,
                    Relation::Greater => 2,
                };
                let before = ReplicaSnapshot::capture(&replica);
                let epoch = match epoch_relation {
                    Relation::Less => 0,
                    Relation::Equal => 1,
                    Relation::Greater => 2,
                };
                let outputs = receive(&mut replica, from, message(epoch, slot, Body::Commit));
                let accepted = provenance == Provenance::Leader
                    && epoch_relation == Relation::Equal
                    && slot_relation != Relation::Greater;
                if accepted {
                    assert_eq!(replica.commit(), slot);
                    if slot_relation == Relation::Equal {
                        assert_eq!(
                            outputs,
                            vec![Output::Execute {
                                slot: 1,
                                client_id: 23,
                                request_num: 29,
                                message_id: entry(1, 23, 29).message_id,
                                execution_time: EXECUTION_TIME + 1,
                                payload: b"request-1".to_vec(),
                            }]
                        );
                    } else {
                        assert!(outputs.is_empty());
                    }
                } else {
                    assert!(
                        outputs.is_empty(),
                        "commit {provenance:?} {epoch_relation:?} {slot_relation:?}"
                    );
                    assert_replica_unchanged(
                        (provenance, epoch_relation, slot_relation),
                        &before,
                        &replica,
                    );
                }
            }
        }
    }
}

#[test]
fn execution_ordering_and_exact_result_replay_matrix() {
    assert_complete_cases("complete slot relation", 3, Relation::ALL);

    let mut replica = leader_with_request();
    assert!(receive(&mut replica, 1, message(0, 1, Body::PrepareOk)).is_empty());
    assert_eq!(
        receive(&mut replica, 2, message(0, 1, Body::PrepareOk)),
        vec![Output::Execute {
            slot: 1,
            client_id: 1,
            request_num: 1,
            message_id: id(1),
            execution_time: EXECUTION_TIME,
            payload: b"request".to_vec(),
        }]
    );
    assert_eq!(
        replica.step(Input::Request {
            client_id: 2,
            request_num: 1,
            message_id: id(2),
            execution_time: EXECUTION_TIME,
            payload: b"request".to_vec(),
        }),
        vec![Output::Broadcast(Message {
            epoch: 0,
            slot: 2,
            body: Body::Prepare {
                commit: 1,
                entry: LogEntry {
                    slot: 2,
                    client_id: 2,
                    request_num: 1,
                    message_id: id(2),
                    execution_time: EXECUTION_TIME,
                    payload: b"request".to_vec(),
                },
            },
        })]
    );
    assert!(receive(&mut replica, 1, message(0, 2, Body::PrepareOk)).is_empty());
    assert!(receive(&mut replica, 2, message(0, 2, Body::PrepareOk)).is_empty());

    for slot_relation in [Relation::Less, Relation::Greater] {
        let slot = if slot_relation == Relation::Less {
            0
        } else {
            2
        };
        let before = ReplicaSnapshot::capture(&replica);
        assert!(replica
            .step(Input::Complete {
                slot,
                result: vec![slot as u8]
            })
            .is_empty());
        assert_replica_unchanged(slot_relation, &before, &replica);
    }
    let result = b"exact-result".to_vec();
    assert_eq!(
        replica.step(Input::Complete { slot: 1, result }),
        vec![
            Output::Reply(b"exact-result".to_vec()),
            Output::Execute {
                slot: 2,
                client_id: 2,
                request_num: 1,
                message_id: id(2),
                execution_time: EXECUTION_TIME,
                payload: b"request".to_vec(),
            }
        ]
    );
    assert_eq!(
        replica.step(request(9, 1)),
        vec![Output::Reply(b"exact-result".to_vec())]
    );
}

#[test]
fn checked_epoch_overflow_fail_stop_matrix() {
    assert_complete_cases("epoch overflow boundary", 3, [u32::MIN, 1, u32::MAX]);

    let mut replica = node(3, 1);
    receive(
        &mut replica,
        0,
        message(
            u32::MAX,
            0,
            Body::StartEpoch {
                state: state(vec![], 0),
            },
        ),
    );
    assert_eq!(replica.epoch(), u32::MAX);
    let before = ReplicaSnapshot::capture(&replica);
    assert!(replica.step(Input::LeaderTimeout).is_empty());
    assert_replica_unchanged("u32::MAX epoch timeout", &before, &replica);
}
