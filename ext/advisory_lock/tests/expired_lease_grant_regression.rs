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
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn sets_with_zero_duration_rejected_over_expired_incumbent() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
