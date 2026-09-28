use lunet_advisory_lock::locks::{LeaseCandidate, Request, Response, Service};
use uuid::Uuid;

fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn set(message: u8, client: u64, request_num: u64, holder: u8, lease_ms: u64) -> Request {
    Request::Set {
        message_id: id(message),
        client_id: client,
        request_num,
        lock_id: 7,
        name: None,
        labels: None,
        lease: LeaseCandidate {
            lease_id: 9,
            holder: id(holder),
            lease_ms,
        },
        sent_at_ms: None,
    }
}

fn baseline_service() -> Service {
    Service::default()
}

#[test]
fn client_json_round_trips() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn set_obeys_the_lease_rules() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn execution_rejects_an_envelope_payload_mismatch() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
