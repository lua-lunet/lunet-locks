mod support;

use std::collections::HashSet;

use support::{
    assert_complete_cases, assert_replica_unchanged, entry, members, message, node, receive, state,
    ReplicaSnapshot,
};
use vrr::vrr::{Body, Input, NodeId, Output, Replica, Status};

const MEMBER_COUNTS: [usize; 2] = [3, 4];

fn quorum(count: usize) -> usize {
    count / 2 + 1
}

fn members_except(count: usize, excluded: NodeId) -> Vec<NodeId> {
    (0..count as NodeId)
        .filter(|node| *node != excluded)
        .collect()
}

fn assert_refused(
    replica: &mut Replica,
    context: impl std::fmt::Debug,
    from: NodeId,
    message: vrr::vrr::Message,
) {
    let before = ReplicaSnapshot::capture(replica);
    assert!(receive(replica, from, message).is_empty(), "{context:?}");
    assert_replica_unchanged(context, &before, replica);
}

#[test]
fn constructor_membership_and_leader_rotation_matrices_are_finite_and_deterministic() {
    assert_complete_cases("membership counts", 2, MEMBER_COUNTS);

    for count in MEMBER_COUNTS {
        let membership = members(count);
        assert_eq!(membership.len(), count, "K={count}");
        assert_eq!(
            membership.iter().collect::<HashSet<_>>().len(),
            count,
            "K={count} membership is unique"
        );

        for (index, own) in membership.iter().enumerate() {
            let replica = Replica::new(membership.clone(), own).expect("sorted member is valid");
            let epochs: Vec<_> = (0..(count * 2) as u32).collect();
            assert_complete_cases(
                format!("K={count} leader epochs"),
                count * 2,
                epochs.clone(),
            );
            let leaders: Vec<_> = epochs
                .iter()
                .map(|epoch| replica.leader_of(*epoch))
                .collect();
            assert_eq!(leaders.len(), count * 2, "K={count} leader cardinality");
            for leader in 0..count as NodeId {
                assert_eq!(
                    leaders.iter().filter(|actual| **actual == leader).count(),
                    2,
                    "K={count} leader {leader} appears once per rotation"
                );
            }
            assert_eq!(replica.is_leader(), index == 0, "K={count}, node={index}");
        }

        assert!(Replica::new(membership[..2].to_vec(), &membership[0]).is_err());
        let mut unsorted = membership.clone();
        unsorted.swap(0, 1);
        assert!(Replica::new(unsorted, &membership[0]).is_err(), "K={count}");
        let mut duplicate = membership.clone();
        duplicate[1] = duplicate[0].clone();
        assert!(
            Replica::new(duplicate, &membership[0]).is_err(),
            "K={count}"
        );
        assert!(
            Replica::new(membership, "not-a-member").is_err(),
            "K={count}"
        );
    }
}

#[test]
fn normal_commit_quorum_and_sender_provenance_matrices() {
    for count in MEMBER_COUNTS {
        let q = quorum(count);
        assert_eq!(q, if count == 3 { 2 } else { 3 }, "K={count} Q");
        let external_counts = [q - 2, q - 1, q];
        assert_complete_cases(
            format!("K={count} normal external ACK counts"),
            3,
            external_counts,
        );

        for external_count in external_counts {
            let mut leader = node(count, 0);
            assert_eq!(leader.step(support::request(1, 1)).len(), 1, "K={count}");
            let backups = members_except(count, 0);
            for from in backups.iter().copied().take(external_count) {
                let outputs = receive(&mut leader, from, message(0, 1, Body::PrepareOk));
                let expected = usize::from(from == backups[q - 2]);
                assert_eq!(
                    outputs.len(),
                    expected,
                    "K={count}, external ACKs={external_count}"
                );
            }
            assert_eq!(
                leader.commit(),
                u64::from(external_count >= q - 1),
                "K={count}"
            );
        }

        let mut leader = node(count, 0);
        leader.step(support::request(1, 1));
        let ack = message(0, 1, Body::PrepareOk);
        let first_backup = 1;
        receive(&mut leader, first_backup, ack.clone());
        let before_duplicate = ReplicaSnapshot::capture(&leader);
        assert!(
            receive(&mut leader, first_backup, ack.clone()).is_empty(),
            "K={count}"
        );
        assert_replica_unchanged("duplicate PREPARE_OK", &before_duplicate, &leader);
        assert_refused(&mut leader, "self PREPARE_OK", 0, ack.clone());
        assert_refused(&mut leader, "nonmember PREPARE_OK", count as NodeId, ack);

        let mut backup = node(count, 1);
        let prepare = message(
            0,
            1,
            Body::Prepare {
                commit: 0,
                entry: entry(1, 1, 1),
            },
        );
        assert_refused(&mut backup, "self PREPARE", 1, prepare.clone());
        assert_refused(
            &mut backup,
            "nonmember PREPARE",
            count as NodeId,
            prepare.clone(),
        );
        assert!(matches!(
            receive(&mut backup, 0, prepare).as_slice(),
            [Output::To(0, _)]
        ));
    }
}

#[test]
fn epoch_change_qualification_quorum_and_sender_provenance_matrices() {
    for count in MEMBER_COUNTS {
        let q = quorum(count);
        let vote_counts = [q - 2, q - 1, q];
        assert_complete_cases(
            format!("K={count} epoch-change vote counts"),
            3,
            vote_counts,
        );

        for vote_count in vote_counts {
            let mut leader = node(count, 1);
            assert_eq!(leader.step(Input::LeaderTimeout).len(), 1, "K={count}");
            let voters = members_except(count, 1);
            for from in voters.iter().copied().take(vote_count) {
                assert!(
                    receive(&mut leader, from, message(1, 0, Body::StartEpochChange)).is_empty()
                );
            }

            for from in voters.iter().copied().take(q - 1) {
                let outputs = receive(
                    &mut leader,
                    from,
                    message(
                        1,
                        0,
                        Body::DoEpochChange {
                            latest_normal: 0,
                            state: state(Vec::new(), 0),
                        },
                    ),
                );
                let completed = vote_count >= q - 1 && from == voters[q - 2];
                assert_eq!(
                    outputs.len(),
                    usize::from(completed),
                    "K={count}, votes={vote_count}"
                );
            }
            assert_eq!(
                leader.status(),
                if vote_count >= q - 1 {
                    Status::Normal
                } else {
                    Status::EpochChange
                },
                "K={count}, epoch-change votes={vote_count}"
            );
        }

        let mut leader = node(count, 1);
        leader.step(Input::LeaderTimeout);
        assert_refused(
            &mut leader,
            "self START_EPOCH_CHANGE",
            1,
            message(1, 0, Body::StartEpochChange),
        );
        assert_refused(
            &mut leader,
            "nonmember START_EPOCH_CHANGE",
            count as NodeId,
            message(1, 0, Body::StartEpochChange),
        );
        assert_refused(
            &mut leader,
            "self DO_EPOCH_CHANGE",
            1,
            message(
                1,
                0,
                Body::DoEpochChange {
                    latest_normal: 0,
                    state: state(Vec::new(), 0),
                },
            ),
        );
        assert_refused(
            &mut leader,
            "nonmember DO_EPOCH_CHANGE",
            count as NodeId,
            message(
                1,
                0,
                Body::DoEpochChange {
                    latest_normal: 0,
                    state: state(Vec::new(), 0),
                },
            ),
        );
    }
}

#[test]
fn recovery_quorum_counts_distinct_other_members_and_refuses_bad_provenance() {
    for count in MEMBER_COUNTS {
        let q = quorum(count);
        let mut recovering = node(count, count - 1);
        assert_eq!(
            recovering.step(Input::Recover { nonce: 7 }).len(),
            1,
            "K={count}"
        );
        let responders = members_except(count, (count - 1) as NodeId);
        assert_eq!(responders.len(), q, "K={count} has exactly Q other members");
        assert_complete_cases(
            format!("K={count} recovery responders"),
            q,
            responders.clone(),
        );

        for from in responders.iter().copied().take(q - 1) {
            let slot = if from == 0 { 1 } else { 0 };
            let body = if from == 0 {
                Body::RecoveryResponse {
                    nonce: 7,
                    state: Some(state(vec![entry(1, 1, 1)], 0)),
                }
            } else {
                Body::RecoveryResponse {
                    nonce: 7,
                    state: None,
                }
            };
            assert!(
                receive(&mut recovering, from, message(0, slot, body)).is_empty(),
                "K={count}"
            );
            assert_eq!(recovering.status(), Status::Recovering, "K={count}, Q-1");
        }

        let duplicate_from = responders[0];
        let duplicate_slot = if duplicate_from == 0 { 1 } else { 0 };
        let before_duplicate = ReplicaSnapshot::capture(&recovering);
        let duplicate = if duplicate_from == 0 {
            Body::RecoveryResponse {
                nonce: 7,
                state: Some(state(vec![entry(1, 1, 1)], 0)),
            }
        } else {
            Body::RecoveryResponse {
                nonce: 7,
                state: None,
            }
        };
        assert!(receive(
            &mut recovering,
            duplicate_from,
            message(0, duplicate_slot, duplicate),
        )
        .is_empty());
        assert_replica_unchanged(
            "duplicate recovery response",
            &before_duplicate,
            &recovering,
        );

        let last = responders[q - 1];
        let last_slot = if last == 0 { 1 } else { 0 };
        let last_body = if last == 0 {
            Body::RecoveryResponse {
                nonce: 7,
                state: Some(state(vec![entry(1, 1, 1)], 0)),
            }
        } else {
            Body::RecoveryResponse {
                nonce: 7,
                state: None,
            }
        };
        let outputs = receive(&mut recovering, last, message(0, last_slot, last_body));
        assert_eq!(outputs.len(), 0, "K={count}, Q recovery responses");
        assert_eq!(recovering.status(), Status::Normal, "K={count}, Q");
        assert_eq!(recovering.slot(), 1, "K={count} leader state adopted");

        assert_refused(
            &mut recovering,
            "self recovery response after Q",
            (count - 1) as NodeId,
            message(
                0,
                0,
                Body::RecoveryResponse {
                    nonce: 7,
                    state: None,
                },
            ),
        );
        assert_refused(
            &mut recovering,
            "nonmember recovery response after Q",
            count as NodeId,
            message(
                0,
                0,
                Body::RecoveryResponse {
                    nonce: 7,
                    state: None,
                },
            ),
        );
    }
}
