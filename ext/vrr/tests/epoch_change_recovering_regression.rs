mod support;

use support::{assert_replica_unchanged, message, node, receive, state, ReplicaSnapshot};
use vrr::vrr::{Body, Input};

#[test]
fn recovering_replica_refuses_future_do_epoch_change_without_mutation() {
    let mut replica = node(4, 1);
    replica.step(Input::Recover { nonce: 7 });
    let before = ReplicaSnapshot::capture(&replica);

    assert!(receive(
        &mut replica,
        0,
        message(
            1,
            0,
            Body::DoEpochChange {
                latest_normal: 0,
                state: state(vec![], 0),
            },
        ),
    )
    .is_empty());
    assert_replica_unchanged("recovering future DO_EPOCH_CHANGE", &before, &replica);
}
