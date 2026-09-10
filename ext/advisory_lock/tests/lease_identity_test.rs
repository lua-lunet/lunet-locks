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

fn set_json(id: u8, client: u64, num: u64, holder: &str, expiry: u64) -> Value {
    json!({
        "op": "set",
        "message_id": uuid_text(id),
        "client_id": client,
        "request_num": num,
        "lock_id": LOCK_ID,
        "lease": {"lease_id": 9, "holder": holder, "expiry": expiry},
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
    let mut service = Service::default();
    let mut request = set_json(1, 1, 1, HOLDER_A, 500);
    request["name"] = json!("/cluster/members/0000001");
    request["labels"] = json!(["prod", "csv", "prod"]);
    let (holder, name, labels, taken_at, renew_count) =
        granted_lease(run(&mut service, 1, 1, 1, 100, request));
    assert_eq!(holder, HOLDER_A);
    assert_eq!(name.as_deref(), Some("/cluster/members/0000001"));
    assert!(labels_are(&labels, &["csv", "prod"]));
    assert_eq!(taken_at, 100);
    assert_eq!(renew_count, 0);
}

#[test]
fn get_reply_carries_the_extended_lease() {
    let mut service = Service::default();
    let mut request = set_json(1, 1, 1, HOLDER_A, 500);
    request["name"] = json!("/cluster/members/0000001");
    request["labels"] = json!(["csv"]);
    let granted = run(&mut service, 1, 1, 1, 100, request);
    let (_, _, _, _, _) = granted_lease(granted);
    let (taken_at, renew_count, holder, name, labels) =
        get_lease(run(&mut service, 2, 1, 2, 110, get_json(2, 1, 2)));
    assert_eq!(taken_at, 100);
    assert_eq!(renew_count, 0);
    assert_eq!(holder, HOLDER_A);
    assert_eq!(name.as_deref(), Some("/cluster/members/0000001"));
    assert!(labels_are(&labels, &["csv"]));
}

#[test]
fn same_holder_renew_increments_renew_count_and_keeps_taken_at() {
    let mut service = Service::default();
    let mut first = set_json(1, 1, 1, HOLDER_A, 500);
    first["name"] = json!("/cluster/members/0000001");
    let (holder, name, labels, taken_at, renew_count) =
        granted_lease(run(&mut service, 1, 1, 1, 100, first));
    assert_eq!(holder, HOLDER_A);
    assert_eq!(name.as_deref(), Some("/cluster/members/0000001"));
    assert_eq!(labels, None);
    assert_eq!(taken_at, 100);
    assert_eq!(renew_count, 0);

    let second = set_json(2, 1, 2, HOLDER_A, 600);
    let (holder, name, labels, taken_at, renew_count) =
        granted_lease(run(&mut service, 2, 1, 2, 110, second));
    assert_eq!(holder, HOLDER_A);
    assert_eq!(name.as_deref(), Some("/cluster/members/0000001"));
    assert_eq!(labels, None);
    assert_eq!(taken_at, 100);
    assert_eq!(renew_count, 1);

    let third = set_json(3, 1, 3, HOLDER_A, 700);
    let (_, _, _, taken_at, renew_count) = granted_lease(run(&mut service, 3, 1, 3, 120, third));
    assert_eq!(taken_at, 100);
    assert_eq!(renew_count, 2);
}

#[test]
fn renew_supplies_identity_replacements() {
    let mut service = Service::default();
    let mut first = set_json(1, 1, 1, HOLDER_A, 500);
    first["name"] = json!("/cluster/members/0000001");
    first["labels"] = json!(["csv"]);
    let _ = granted_lease(run(&mut service, 1, 1, 1, 100, first));

    let mut second = set_json(2, 1, 2, HOLDER_A, 600);
    second["labels"] = json!(["prod"]);
    let (_, name, labels, _, _) = granted_lease(run(&mut service, 2, 1, 2, 110, second));
    assert_eq!(name.as_deref(), Some("/cluster/members/0000001"));
    assert!(labels_are(&labels, &["prod"]));
}

#[test]
fn holder_change_after_expiry_resets_the_counters() {
    let mut service = Service::default();
    let first = set_json(1, 1, 1, HOLDER_A, 150);
    let (_, _, _, taken_at, _) = granted_lease(run(&mut service, 1, 1, 1, 100, first));
    assert_eq!(taken_at, 100);

    let second = set_json(2, 2, 1, HOLDER_B, 900);
    let (holder, _, _, taken_at, renew_count) =
        granted_lease(run(&mut service, 2, 2, 1, 200, second));
    assert_eq!(holder, HOLDER_B);
    assert_eq!(taken_at, 200);
    assert_eq!(renew_count, 0);
}

#[test]
fn refused_release_reply_carries_the_incumbent_counters() {
    let mut service = Service::default();
    let mut request = set_json(1, 1, 1, HOLDER_A, 500);
    request["name"] = json!("/cluster/members/0000001");
    let granted = run(&mut service, 1, 1, 1, 100, request);
    let (_, _, _, _, _) = granted_lease(granted);

    let refused = run(
        &mut service,
        2,
        2,
        1,
        110,
        release_json(2, 2, 1, HOLDER_B, 9),
    );
    match refused {
        Response::Release {
            released: false,
            lease: Some(lease),
            ..
        } => {
            assert_eq!(lease.holder.to_string(), HOLDER_A);
            assert_eq!(lease.taken_at_ms, 100);
            assert_eq!(lease.renew_count, 0);
        }
        other => panic!("expected a refused RELEASE reply, got {other:?}"),
    }
}

#[test]
fn taken_at_ms_bumps_on_a_same_ms_collision() {
    assert_eq!(taken_at_ms_on_holder_change(100, 100), 101);
    assert_eq!(taken_at_ms_on_holder_change(100, 200), 200);
    assert_eq!(taken_at_ms_on_holder_change(0, 0), 1);
}

#[test]
fn break_reply_reports_the_post_break_record() {
    let mut service = Service::default();
    let mut request = set_json(1, 1, 1, HOLDER_A, 500);
    request["name"] = json!("/cluster/members/0000001");
    request["labels"] = json!(["csv"]);
    let granted = run(&mut service, 1, 1, 1, 100, request);
    let (_, _, _, _, _) = granted_lease(granted);

    let response = run(&mut service, 2, 2, 1, 110, break_json(2, 2, 1));
    match response {
        Response::Break {
            broken: true,
            lease: Some(lease),
            ..
        } => {
            assert_eq!(lease.lease_id, 10, "the stored record's lease_id bumps");
            assert_eq!(lease.holder, Uuid::nil(), "the holder clears");
            assert_eq!(lease.expiry, 0, "the expiry clears");
            assert_eq!(lease.taken_at_ms, 0, "the take counter clears");
            assert_eq!(lease.renew_count, 0, "the renew counter clears");
            assert_eq!(lease.name.as_deref(), Some("/cluster/members/0000001"));
            assert!(labels_are(&lease.labels, &["csv"]));
        }
        other => panic!("expected a successful BREAK reply, got {other:?}"),
    }

    // The post-break record is never live: a GET reports no lease.
    match run(&mut service, 3, 2, 2, 120, get_json(3, 2, 2)) {
        Response::Get { lease: None, .. } => {}
        other => panic!("expected a GET reply without a lease, got {other:?}"),
    }

    // The next grant keeps the identity unless the SET supplies its own.
    let retake = set_json(4, 3, 1, HOLDER_B, 900);
    let (holder, name, labels, taken_at, renew_count) =
        granted_lease(run(&mut service, 4, 3, 1, 130, retake));
    assert_eq!(holder, HOLDER_B);
    assert_eq!(name.as_deref(), Some("/cluster/members/0000001"));
    assert!(labels_are(&labels, &["csv"]));
    assert_eq!(taken_at, 130);
    assert_eq!(renew_count, 0);
}

#[test]
fn break_of_an_unknown_lock_reports_nothing_broken() {
    let mut service = Service::default();
    match run(&mut service, 1, 1, 1, 100, break_json(1, 1, 1)) {
        Response::Break {
            broken: false,
            lease: None,
            ..
        } => {}
        other => panic!("expected an unsuccessful BREAK reply, got {other:?}"),
    }
}

#[test]
fn break_journals_a_distinct_event() {
    let mut service = Service::default();
    let request = set_json(1, 1, 1, HOLDER_A, 500);
    let transition = run_transition(&mut service, 1, 1, 1, 100, request);
    assert!(transition.is_some());

    let transition = run_transition(&mut service, 2, 2, 1, 110, break_json(2, 2, 1));
    match transition {
        Some(Transition::Break {
            lock_id,
            lease_id,
            holder,
            expiry,
        }) => {
            assert_eq!(lock_id, LOCK_ID);
            assert_eq!(lease_id, 9);
            assert_eq!(holder, *Uuid::parse_str(HOLDER_A).unwrap().as_bytes());
            assert_eq!(expiry, 500);
        }
        other => panic!("expected a break transition, got {other:?}"),
    }

    // Breaking an unknown lock journals nothing.
    let mut fresh = Service::default();
    assert!(run_transition(&mut fresh, 3, 3, 1, 100, break_json(3, 3, 1)).is_none());
}

#[test]
fn request_validation_refuses_malformed_identity() {
    let mut service = Service::default();
    let too_long = format!("/{}", "a".repeat(128));
    let cases = vec![
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"name":"cluster/x","lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"name":"/a//b","lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"name":"/a b","lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"name":too_long,"lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"labels":["ok","-bad"],"lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"labels":["UPPER"],"lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"labels":["a".repeat(33)],"lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
        json!({"op":"set","message_id":uuid_text(1),"client_id":1,"request_num":1,"lock_id":LOCK_ID,"labels":["1","2","3","4","5","6","7","8","9"],"lease":{"lease_id":1,"holder":HOLDER_A,"expiry":500}}),
    ];
    for payload in &cases {
        assert!(
            Service::decode(payload.to_string().as_bytes()).is_err(),
            "expected a decode refusal for {payload}"
        );
        // And the state machine refuses the same payload at execute time.
        assert!(
            service
                .execute(
                    Uuid::from_bytes([1; 16]),
                    1,
                    1,
                    100,
                    payload.to_string().as_bytes()
                )
                .is_err()
        );
    }
}

#[test]
fn nine_labels_are_refused_even_with_duplicates() {
    let payload = json!({
        "op": "set",
        "message_id": uuid_text(1),
        "client_id": 1,
        "request_num": 1,
        "lock_id": LOCK_ID,
        "labels": ["a", "a", "b", "c", "d", "e", "f", "g", "h"],
        "lease": {"lease_id": 1, "holder": HOLDER_A, "expiry": 500},
    });
    assert!(Service::decode(payload.to_string().as_bytes()).is_err());
}

#[test]
fn counters_are_never_client_writable() {
    let mut service = Service::default();
    let payload = json!({
        "op": "set",
        "message_id": uuid_text(1),
        "client_id": 1,
        "request_num": 1,
        "lock_id": LOCK_ID,
        "lease": {"lease_id": 9, "holder": HOLDER_A, "expiry": 500,
                  "taken_at_ms": 555, "renew_count": 77, "name": "/forged"},
    });
    let (_, _, _, taken_at, renew_count) = granted_lease(run(&mut service, 1, 1, 1, 100, payload));
    assert_eq!(taken_at, 100, "the state machine sets taken_at_ms");
    assert_eq!(renew_count, 0, "the state machine sets renew_count");
}

#[test]
fn request_round_trip_keeps_old_shapes_decoding() {
    let mut service = Service::default();
    let old = set_json(1, 1, 1, HOLDER_A, 500);
    let granted = run(&mut service, 1, 1, 1, 100, old);
    let (holder, name, labels, _, _) = granted_lease(granted);
    assert_eq!(holder, HOLDER_A);
    assert_eq!(name, None);
    assert_eq!(labels, None);
}
