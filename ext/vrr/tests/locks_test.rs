use uuid::Uuid;
use vrr::locks::{Lease, Request, Response, Service};

fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn set(message: u8, client: u64, request: u64, holder: u8, expiry: u64) -> Request {
    Request::Set {
        message_id: id(message),
        client_id: client,
        request_num: request,
        lock_id: 7,
        lease: Lease {
            lease_id: 9,
            holder: id(holder),
            expiry,
        },
    }
}

#[test]
fn client_json_round_trips() {
    let request = set(1, 2, 3, 4, 500);
    let bytes = serde_json::to_vec(&request).unwrap();
    assert_eq!(Service::decode(&bytes).unwrap(), request);
}

#[test]
fn set_obeys_the_lease_rules() {
    let mut service = Service::default();
    let first: Response = serde_json::from_slice(
        &service
            .execute(
                id(1),
                1,
                1,
                100,
                &serde_json::to_vec(&set(1, 1, 1, 1, 500)).unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let blocked: Response = serde_json::from_slice(
        &service
            .execute(
                id(2),
                2,
                1,
                100,
                &serde_json::to_vec(&set(2, 2, 1, 2, 600)).unwrap(),
            )
            .unwrap(),
    )
    .unwrap();
    let expired: Response = serde_json::from_slice(
        &service
            .execute(
                id(3),
                2,
                2,
                500,
                &serde_json::to_vec(&set(3, 2, 2, 2, 900)).unwrap(),
            )
            .unwrap(),
    )
    .unwrap();

    assert!(matches!(first, Response::Set { granted: true, .. }));
    assert!(matches!(blocked, Response::Set { granted: false, .. }));
    assert!(matches!(expired, Response::Set { granted: true, .. }));
}

#[test]
fn execution_rejects_an_envelope_payload_mismatch() {
    let mut service = Service::default();
    let request = set(1, 1, 1, 1, 500);
    let payload = serde_json::to_vec(&request).unwrap();
    assert!(Service::validate(id(1), 1, 1, &payload));
    assert!(!Service::validate(id(2), 1, 1, &payload));
    assert!(service.execute(id(2), 1, 1, 100, &payload).is_err());
}
