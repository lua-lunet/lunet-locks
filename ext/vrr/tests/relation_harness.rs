mod support;

use std::collections::HashSet;

use support::{
    assert_complete_cases, assert_replica_unchanged, baseline_replica, baseline_state, Boundary,
    InputValidity, Relation, ReplicaSnapshot, Sender,
};

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
