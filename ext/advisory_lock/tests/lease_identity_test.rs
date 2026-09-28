//! The issue-#9 lease-identity surface: the lock display name, operator
//! labels, the state-machine-tracked lease-age counters, and the privileged
//! break op, driven through `Service::execute` over the client JSON exactly
//! as the protocol decodes it.

use lunet_advisory_lock::locks::{Response, Service, Transition, taken_at_ms_on_holder_change};
use serde_json::{Value, json};
use uuid::Uuid;

const LOCK_ID: u64 = 9001;
const HOLDER_A: &str = "11111111-1111-1111-1111-111111111111";
const HOLDER_B: &str = "22222222-2222-2222-2222-222222222222";

fn uuid_text(byte: u8) -> String {
    Uuid::from_bytes([byte; 16]).to_string()
}

fn set_json(id: u8, client: u64, num: u64, holder: &str, lease_ms: u64) -> Value {
    json!({
        "op": "set",
        "message_id": uuid_text(id),
        "client_id": client,
        "request_num": num,
        "lock_id": LOCK_ID,
        "lease": {"lease_id": 9, "holder": holder, "lease_ms": lease_ms},
    })
}

fn break_json(id: u8, client: u64, num: u64) -> Value {
    json!({
        "op": "break",
        "message_id": uuid_text(id),
        "client_id": client,
        "request_num": num,
        "lock_id": LOCK_ID,
    })
}

fn get_json(id: u8, client: u64, num: u64) -> Value {
    json!({
        "op": "get",
        "message_id": uuid_text(id),
        "client_id": client,
        "request_num": num,
        "lock_id": LOCK_ID,
    })
}

fn release_json(id: u8, client: u64, num: u64, holder: &str, lease_id: u64) -> Value {
    json!({
        "op": "release",
        "message_id": uuid_text(id),
        "client_id": client,
        "request_num": num,
        "lock_id": LOCK_ID,
        "holder": holder,
        "lease_id": lease_id,
    })
}

fn run(service: &mut Service, id: u8, client: u64, num: u64, at: u64, payload: Value) -> Response {
    let (bytes, _) = service
        .execute(
            Uuid::from_bytes([id; 16]),
            client,
            num,
            at,
            payload.to_string().as_bytes(),
        )
        .unwrap();
    serde_json::from_slice(&bytes).unwrap()
}

fn run_transition(
    service: &mut Service,
    id: u8,
    client: u64,
    num: u64,
    at: u64,
    payload: Value,
) -> Option<Transition> {
    let (_, transition) = service
        .execute(
            Uuid::from_bytes([id; 16]),
            client,
            num,
            at,
            payload.to_string().as_bytes(),
        )
        .unwrap();
    transition
}

fn labels_are(actual: &Option<Vec<String>>, expected: &[&str]) -> bool {
    actual.as_deref().is_some_and(|labels| {
        labels
            .iter()
            .map(String::as_str)
            .eq(expected.iter().copied())
    })
}

fn granted_lease(response: Response) -> (String, Option<String>, Option<Vec<String>>, u64, u32) {
    match response {
        Response::Set {
            granted: true,
            lease: Some(lease),
            ..
        } => (
            lease.holder.to_string(),
            lease.name,
            lease.labels,
            lease.taken_at_ms,
            lease.renew_count,
        ),
        other => panic!("expected a granted SET reply, got {other:?}"),
    }
}

fn get_lease(response: Response) -> (u64, u32, String, Option<String>, Option<Vec<String>>) {
    match response {
        Response::Get {
            lease: Some(lease), ..
        } => (
            lease.taken_at_ms,
            lease.renew_count,
            lease.holder.to_string(),
            lease.name,
            lease.labels,
        ),
        other => panic!("expected a GET reply with a lease, got {other:?}"),
    }
}

#[test]
fn set_with_name_and_labels_stores_canonical_identity() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn get_reply_carries_the_extended_lease() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn same_holder_renew_increments_renew_count_and_keeps_taken_at() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn renew_supplies_identity_replacements() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn holder_change_after_expiry_resets_the_counters() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn refused_release_reply_carries_the_incumbent_counters() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn taken_at_ms_bumps_on_a_same_ms_collision() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn break_reply_reports_the_post_break_record() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn break_of_an_unknown_lock_reports_nothing_broken() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn break_journals_a_distinct_event() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn request_validation_refuses_malformed_identity() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn nine_labels_are_refused_even_with_duplicates() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn forged_lease_fields_are_refused() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn request_round_trip_the_bare_candidate_decodes() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
