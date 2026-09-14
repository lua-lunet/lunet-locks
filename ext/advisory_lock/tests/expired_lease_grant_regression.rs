// Confirmation test for F12 under the duration shape: the lock service
// rejects a non-positive lease window. A SET with lease_ms == 0 returns
// granted: false on a free lock or over an expired incumbent, and does not
// mutate lock state. (The old absolute-expiry refusal — expiry <=
// execution_time — has no client-expressible successor: the client cannot
// name an expiry at all.)
use lunet_advisory_lock::locks::{LeaseCandidate, Request, Response, Service};
use uuid::Uuid;

fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn execute(service: &mut Service, request: &Request, execution_time: u64) -> Vec<u8> {
    let (message_id, client_id, request_num) = request.ids();
    service
        .execute(
            message_id,
            client_id,
            request_num,
            execution_time,
            &serde_json::to_vec(request).unwrap(),
        )
        .expect("execution succeeds")
        .0
}

fn set_request(
    message: u8,
    client: u64,
    request_num: u64,
    lock_id: u64,
    holder: Uuid,
    lease_ms: u64,
) -> Request {
    Request::Set {
        message_id: id(message),
        client_id: client,
        request_num,
        lock_id,
        lease: LeaseCandidate {
            lease_id: 13,
            holder,
            lease_ms,
        },
        name: None,
        labels: None,
        sent_at_ms: None,
    }
}

fn get_request(message: u8, client: u64, request_num: u64, lock_id: u64) -> Request {
    Request::Get {
        message_id: id(message),
        client_id: client,
        request_num,
        lock_id,
    }
}

#[test]
fn sets_with_zero_duration_rejected_on_free_lock() {
    const EXECUTION_TIME: u64 = 100;
    const LOCK_ID: u64 = 7;
    let holder = id(1);

    let mut service = Service::default();

    let request = set_request(1, 1, 1, LOCK_ID, holder, 0);
    let response_bytes = execute(&mut service, &request, EXECUTION_TIME);
    let response: Response = serde_json::from_slice(&response_bytes).unwrap();

    assert!(
        matches!(
            response,
            Response::Set {
                granted: false,
                lock_id: LOCK_ID,
                lease: None,
                ..
            }
        ),
        "SET with lease_ms=0 at execution_time={EXECUTION_TIME} must be rejected; got {response:?}"
    );

    let get_bytes = execute(&mut service, &get_request(2, 1, 2, LOCK_ID), EXECUTION_TIME);
    let get_response: Response = serde_json::from_slice(&get_bytes).unwrap();

    assert!(
        matches!(
            get_response,
            Response::Get {
                lease: None,
                lock_id: LOCK_ID,
                ..
            }
        ),
        "GET after rejected SET must report no live lease; got {get_response:?}"
    );
}

#[test]
fn sets_with_zero_duration_rejected_over_expired_incumbent() {
    const EXECUTION_TIME: u64 = 100;
    const LOCK_ID: u64 = 7;
    let old_holder = id(1);
    let new_holder = id(2);

    let mut service = Service::default();

    // An incumbent whose window lapsed: taken at t=0 with 10 ms to live,
    // so its stamped expiry (10) sits before the execution clock (100).
    let install = set_request(1, 1, 1, LOCK_ID, old_holder, 10);
    execute(&mut service, &install, 0);

    let request = set_request(2, 2, 1, LOCK_ID, new_holder, 0);
    let response_bytes = execute(&mut service, &request, EXECUTION_TIME);
    let response: Response = serde_json::from_slice(&response_bytes).unwrap();

    assert!(
        matches!(
            response,
            Response::Set {
                granted: false,
                lock_id: LOCK_ID,
                lease: None,
                ..
            }
        ),
        "SET with lease_ms=0 over an expired incumbent must be rejected; got {response:?}"
    );

    let get_bytes = execute(&mut service, &get_request(3, 2, 2, LOCK_ID), EXECUTION_TIME);
    let get_response: Response = serde_json::from_slice(&get_bytes).unwrap();

    assert!(
        matches!(
            get_response,
            Response::Get {
                lease: None,
                lock_id: LOCK_ID,
                ..
            }
        ),
        "GET after rejected SET over expired incumbent must report no live lease; got {get_response:?}"
    );
}
