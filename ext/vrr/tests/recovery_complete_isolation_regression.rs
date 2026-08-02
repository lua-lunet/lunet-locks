mod support;

use support::{assert_replica_unchanged, message, node, receive, request, ReplicaSnapshot};
use vrr::vrr::{Body, Input, Status};

#[test]
fn recovering_replica_refuses_completion_without_mutation() {
    let mut replica = node(3, 0);
    assert_eq!(replica.step(request(1, 1)).len(), 1);
    assert_eq!(
        receive(&mut replica, 1, message(0, 1, Body::PrepareOk)).len(),
        1
    );
    assert_eq!(replica.status(), Status::Normal);

    assert_eq!(replica.step(Input::Recover { nonce: 7 }).len(), 1);
    let before = ReplicaSnapshot::capture(&replica);

    assert!(replica
        .step(Input::Complete {
            slot: 1,
            result: b"result".to_vec(),
        })
        .is_empty());
    assert_replica_unchanged("recovering COMPLETE", &before, &replica);
}
