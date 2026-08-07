mod support;

use std::collections::HashSet;

use support::{
    assert_complete_cases, assert_replica_unchanged, baseline_replica, baseline_state, message,
    node, receive, request, Boundary, InputValidity, Relation, ReplicaSnapshot, Sender,
};
use vrr::vrr::{Body, Input};

#[test]
fn relation_vocabulary_has_safe_total_concrete_mappings() {
    for relation in Relation::ALL {
        assert!(
            relation.epoch() > 0,
            "relation {relation:?} has a concrete epoch"
        );
        assert!(
            relation.slot() > 0,
            "relation {relation:?} has a concrete slot"
        );
        assert!(
            relation.request_num() > 0,
            "relation {relation:?} has a concrete request number"
        );
        assert!(
            relation.nonce() > 0,
            "relation {relation:?} has a concrete nonce"
        );
        assert!(
            relation.lease_expiry() > 0,
            "relation {relation:?} has a concrete lease expiry"
        );
    }
    assert!(Relation::Less.epoch() < Relation::Equal.epoch());
    assert!(Relation::Equal.epoch() < Relation::Greater.epoch());
    assert!(Relation::Less.slot() < Relation::Equal.slot());
    assert!(Relation::Equal.slot() < Relation::Greater.slot());
    assert!(Relation::Less.node_id() < Relation::Equal.node_id());
    assert!(Relation::Equal.node_id() < Relation::Greater.node_id());
    assert!(Relation::Less.request_num() < Relation::Equal.request_num());
    assert!(Relation::Equal.request_num() < Relation::Greater.request_num());
    assert!(Relation::Less.nonce() < Relation::Equal.nonce());
    assert!(Relation::Equal.nonce() < Relation::Greater.nonce());
    assert!(Relation::Less.lease_expiry() < Relation::Equal.lease_expiry());
    assert!(Relation::Equal.lease_expiry() < Relation::Greater.lease_expiry());
    assert_eq!(Relation::Equal.lease_expiry(), support::EXECUTION_TIME);

    assert_eq!(Boundary::Minimum.u32(), u32::MIN);
    assert_eq!(Boundary::Maximum.u32(), u32::MAX);
    assert_eq!(Boundary::Minimum.u64(), u64::MIN);
    assert_eq!(Boundary::Maximum.u64(), u64::MAX);
    assert_complete_cases("input validity", 3, InputValidity::ALL);
    assert_complete_cases("boundaries", 3, Boundary::ALL);

    let senders: HashSet<_> = Sender::ALL
        .into_iter()
        .map(|sender| sender.node_id(Relation::Equal.epoch()))
        .collect();
    assert_eq!(
        senders.len(),
        Sender::ALL.len(),
        "sender mappings must be distinct"
    );
}

#[test]
fn baseline_fixtures_are_valid_and_snapshot_refusal_state() {
    let replica = baseline_replica();
    let snapshot = ReplicaSnapshot::capture(&replica);
    assert_replica_unchanged("fresh baseline", &snapshot, &replica);

    let state = baseline_state();
    assert_eq!(state.slot, 1);
    assert_eq!(state.commit, 0);
    assert_eq!(state.log.len(), 1);
}

/// The snapshot wraps the complete diagnostic boundary, so a refusal
/// assertion detects mutation of every protocol-relevant state category —
/// not just the public scalars. Each arm drives one accepted transition and
/// asserts both that the snapshot changed and that the category's field is
/// the one that moved, pinning the field's presence in the boundary.
#[test]
fn snapshot_detects_each_protocol_state_category() {
    // Recovery nonce and evidence map: a host recovery attempt, then an
    // admissible matching nonleader `state: None` response.
    let mut replica = node(3, 0);
    let before = ReplicaSnapshot::capture(&replica);
    replica.step(Input::Recover { nonce: 7 });
    let recovering = ReplicaSnapshot::capture(&replica);
    assert_ne!(before, recovering, "snapshot detects the recovery attempt");
    assert_eq!(recovering.diagnostic().recovery_nonce, Some(7));
    assert!(receive(
        &mut replica,
        1,
        message(
            0,
            0,
            Body::RecoveryResponse {
                nonce: 7,
                state: None
            }
        ),
    )
    .is_empty());
    let after = ReplicaSnapshot::capture(&replica);
    assert_ne!(
        recovering, after,
        "snapshot detects recovery evidence admission"
    );
    assert_eq!(after.diagnostic().recovery.len(), 1);

    // Prepare-acknowledgement quorum accumulator: one external PrepareOk
    // below the K=4 quorum changes nothing the public scalars can see.
    let mut leader = node(4, 0);
    leader.step(request(1, 1));
    let before = ReplicaSnapshot::capture(&leader);
    assert!(receive(&mut leader, 1, message(0, 1, Body::PrepareOk)).is_empty());
    let after = ReplicaSnapshot::capture(&leader);
    assert_ne!(before, after, "snapshot detects the prepare-ok vote");
    assert_eq!(after.diagnostic().prepare_oks.len(), 1);
    assert_eq!(after.diagnostic().commit, before.diagnostic().commit);

    // Epoch-change evidence: start-change votes, the sent marker, and the
    // do-change reports all move before activation sets latest_normal.
    let mut replica = node(4, 1);
    replica.step(Input::LeaderTimeout);
    let before = ReplicaSnapshot::capture(&replica);
    assert!(receive(&mut replica, 0, message(1, 0, Body::StartEpochChange)).is_empty());
    let after = ReplicaSnapshot::capture(&replica);
    assert_ne!(before, after, "snapshot detects the start-change vote");
    assert_eq!(after.diagnostic().start_changes.len(), 1);
    assert!(!after.diagnostic().sent_do_change);
    assert!(receive(&mut replica, 2, message(1, 0, Body::StartEpochChange)).is_empty());
    let qualified = ReplicaSnapshot::capture(&replica);
    assert_ne!(after, qualified, "snapshot detects qualification");
    assert!(qualified.diagnostic().sent_do_change);
    assert_eq!(qualified.diagnostic().do_changes.len(), 1);
    assert!(receive(
        &mut replica,
        0,
        message(
            1,
            0,
            Body::DoEpochChange {
                latest_normal: 0,
                state: support::state(Vec::new(), 0),
            },
        ),
    )
    .is_empty());
    let reported = ReplicaSnapshot::capture(&replica);
    assert_ne!(qualified, reported, "snapshot detects the do-change report");
    assert_eq!(reported.diagnostic().do_changes.len(), 2);
    assert_eq!(reported.diagnostic().latest_normal, 0);
    assert_eq!(
        receive(
            &mut replica,
            2,
            message(
                1,
                0,
                Body::DoEpochChange {
                    latest_normal: 0,
                    state: support::state(Vec::new(), 0),
                },
            ),
        )
        .len(),
        1
    );
    let activated = ReplicaSnapshot::capture(&replica);
    assert_ne!(reported, activated, "snapshot detects epoch activation");
    assert_eq!(activated.diagnostic().latest_normal, 1);

    // Client table, result history, and the in-flight execution marker.
    let mut leader = node(4, 0);
    let before = ReplicaSnapshot::capture(&leader);
    leader.step(request(1, 1));
    let requested = ReplicaSnapshot::capture(&leader);
    assert_ne!(before, requested, "snapshot detects the client-table entry");
    assert_eq!(requested.diagnostic().clients.len(), 1);
    assert!(receive(&mut leader, 1, message(0, 1, Body::PrepareOk)).is_empty());
    assert!(receive(&mut leader, 2, message(0, 1, Body::PrepareOk)).len() == 1);
    let executing = ReplicaSnapshot::capture(&leader);
    assert_ne!(
        requested, executing,
        "snapshot detects the execution marker"
    );
    assert_eq!(executing.diagnostic().executing, Some(1));
    assert_eq!(executing.diagnostic().executed, 0);
    assert_eq!(
        leader.step(Input::Complete {
            slot: 1,
            result: b"result".to_vec(),
        }),
        vec![vrr::vrr::Output::Reply(b"result".to_vec())]
    );
    let completed = ReplicaSnapshot::capture(&leader);
    assert_ne!(executing, completed, "snapshot detects the recorded result");
    assert_eq!(completed.diagnostic().results.len(), 1);
    assert_eq!(completed.diagnostic().executing, None);
    assert_eq!(
        completed.diagnostic().clients[&1].result,
        Some(b"result".to_vec())
    );
}
