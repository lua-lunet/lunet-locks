//! Rich lock table: name/labels validation, lease-age counters, SET event
//! classification, and the privileged break op.
use lunet_advisory_lock::locks::{Event, Lease, Request, Response, Service};
use uuid::Uuid;

const LOCK_ID: u64 = 7;

fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn set(
    message: u8,
    holder: u8,
    lease_id: u64,
    expiry: u64,
    name: Option<&str>,
    labels: Option<Vec<&str>>,
) -> Request {
    Request::Set {
        message_id: id(message),
        client_id: u64::from(message),
        request_num: u64::from(message),
        lock_id: LOCK_ID,
        lease: Lease {
            lease_id,
            holder: id(holder),
            expiry,
        },
        name: name.map(str::to_owned),
        labels: labels.map(|labels| labels.into_iter().map(str::to_owned).collect()),
    }
}

fn get(message: u8) -> Request {
    Request::Get {
        message_id: id(message),
        client_id: u64::from(message),
        request_num: u64::from(message),
        lock_id: LOCK_ID,
    }
}

fn release(message: u8, holder: u8, lease_id: u64) -> Request {
    Request::Release {
        message_id: id(message),
        client_id: u64::from(message),
        request_num: u64::from(message),
        lock_id: LOCK_ID,
        holder: id(holder),
        lease_id,
    }
}

fn break_lock(message: u8) -> Request {
    Request::Break {
        message_id: id(message),
        client_id: u64::from(message),
        request_num: u64::from(message),
        lock_id: LOCK_ID,
    }
}

fn execute(service: &mut Service, request: &Request, execution_time: u64) -> Response {
    let (message_id, client_id, request_num) = request.ids();
    serde_json::from_slice(
        &service
            .execute(
                message_id,
                client_id,
                request_num,
                execution_time,
                &serde_json::to_vec(request).unwrap(),
            )
            .unwrap(),
    )
    .unwrap()
}

fn set_response(
    service: &mut Service,
    request: &Request,
    execution_time: u64,
) -> (
    bool,
    Event,
    Option<lunet_advisory_lock::locks::ExtendedLease>,
) {
    match execute(service, request, execution_time) {
        Response::Set {
            granted,
            event,
            lease,
            ..
        } => (granted, event, lease),
        response => panic!("SET must produce a SET response: {response:?}"),
    }
}

#[test]
fn invalid_names_are_denied_without_touching_the_table() {
    let too_long = format!("/{}", "a".repeat(128));
    let invalid = [
        "no-leading-slash",
        "/",
        "/double//slash",
        "/trailing/",
        "/bad char/x",
        "/non-ascii-é",
        too_long.as_str(),
    ];
    for name in invalid {
        let mut service = Service::default();
        let (granted, event, lease) =
            set_response(&mut service, &set(1, 1, 1, 500, Some(name), None), 100);
        assert!(!granted, "invalid name {name:?} must be denied");
        assert_eq!(event, Event::Deny);
        assert_eq!(lease, None, "invalid name {name:?} must not create a row");
        assert!(
            matches!(
                execute(&mut service, &get(2), 100),
                Response::Get { lease: None, .. }
            ),
            "invalid name {name:?} must not mutate the lock table"
        );
    }
}

#[test]
fn invalid_labels_are_denied_without_touching_the_table() {
    let too_long = "a".repeat(33);
    let too_many: Vec<&str> = ["a", "b", "c", "d", "e", "f", "g", "h", "i"].into();
    let invalid: Vec<Option<Vec<&str>>> = vec![
        Some(vec![""]),
        Some(vec!["-leading-hyphen"]),
        Some(vec!["trailing-hyphen-"]),
        Some(vec!["Uppercase"]),
        Some(vec!["under_score"]),
        Some(vec![too_long.as_str()]),
        Some(too_many),
    ];
    for labels in invalid {
        let mut service = Service::default();
        let (granted, event, lease) =
            set_response(&mut service, &set(1, 1, 1, 500, None, labels.clone()), 100);
        assert!(!granted, "invalid labels {labels:?} must be denied");
        assert_eq!(event, Event::Deny);
        assert_eq!(
            lease, None,
            "invalid labels {labels:?} must not create a row"
        );
    }
}

#[test]
fn labels_are_deduplicated_and_sorted_and_update_only_when_carried() {
    let mut service = Service::default();
    let (granted, event, lease) = set_response(
        &mut service,
        &set(
            1,
            1,
            1,
            500,
            Some("/cluster/members/0000001"),
            Some(vec!["zeta", "alpha", "zeta", "m-1"]),
        ),
        100,
    );
    assert!(granted && event == Event::Acquire);
    let lease = lease.expect("acquire returns the lease");
    assert_eq!(lease.name.as_deref(), Some("/cluster/members/0000001"));
    assert_eq!(lease.labels, vec!["alpha", "m-1", "zeta"]);

    // A granted SET without name/labels leaves the stored values untouched.
    let (granted, event, lease) = set_response(&mut service, &set(2, 1, 2, 900, None, None), 200);
    assert!(granted && event == Event::Renew);
    let lease = lease.expect("renew returns the lease");
    assert_eq!(lease.name.as_deref(), Some("/cluster/members/0000001"));
    assert_eq!(lease.labels, vec!["alpha", "m-1", "zeta"]);

    // A granted SET carrying new values replaces them.
    let (granted, _, lease) = set_response(
        &mut service,
        &set(3, 1, 3, 1000, Some("/cluster/other"), Some(vec!["solo"])),
        300,
    );
    assert!(granted);
    let lease = lease.unwrap();
    assert_eq!(lease.name.as_deref(), Some("/cluster/other"));
    assert_eq!(lease.labels, vec!["solo"]);
}

#[test]
fn set_events_cover_acquire_renew_cas_and_deny() {
    let mut service = Service::default();

    // acquire: previously missing.
    let (granted, event, lease) = set_response(&mut service, &set(1, 1, 10, 500, None, None), 100);
    assert!(granted && event == Event::Acquire);
    let acquired = lease.unwrap();
    assert_eq!(acquired.taken_at_ms, Some(100));
    assert_eq!(acquired.renew_count, 0);

    // deny: live foreign incumbent; table unchanged.
    let (granted, event, lease) = set_response(&mut service, &set(2, 2, 11, 600, None, None), 200);
    assert!(!granted && event == Event::Deny);
    let incumbent = lease.expect("deny returns the incumbent");
    assert_eq!(incumbent.holder, Some(id(1)));
    assert_eq!(incumbent.lease_id, 10);

    // renew: same holder bumps renew_count, keeps taken_at_ms.
    let (granted, event, lease) = set_response(&mut service, &set(3, 1, 12, 700, None, None), 300);
    assert!(granted && event == Event::Renew);
    let renewed = lease.unwrap();
    assert_eq!(renewed.lease_id, 12);
    assert_eq!(renewed.taken_at_ms, Some(100));
    assert_eq!(renewed.renew_count, 1);

    // cas: holder change over the expired incumbent resets the counters.
    let (granted, event, lease) = set_response(&mut service, &set(4, 2, 13, 900, None, None), 701);
    assert!(granted && event == Event::Cas);
    let overtaken = lease.unwrap();
    assert_eq!(overtaken.holder, Some(id(2)));
    assert_eq!(overtaken.taken_at_ms, Some(701));
    assert_eq!(overtaken.renew_count, 0);
}

#[test]
fn taken_at_ms_bumps_one_millisecond_on_a_same_ms_holder_change() {
    let mut service = Service::default();
    let (granted, _, lease) = set_response(&mut service, &set(1, 1, 10, 500, None, None), 100);
    assert!(granted);
    assert_eq!(lease.unwrap().taken_at_ms, Some(100));

    // Release and re-acquire by another holder inside the same millisecond.
    assert!(matches!(
        execute(&mut service, &release(2, 1, 10), 100),
        Response::Release {
            released: true,
            event: Some(Event::Release),
            ..
        }
    ));
    let (granted, event, lease) = set_response(&mut service, &set(3, 2, 11, 600, None, None), 100);
    assert!(granted && event == Event::Acquire);
    assert_eq!(
        lease.unwrap().taken_at_ms,
        Some(101),
        "same-ms holder change bumps taken_at_ms by 1 ms"
    );
}

#[test]
fn renew_count_resets_on_release_and_expiry_observation() {
    let mut service = Service::default();
    set_response(&mut service, &set(1, 1, 10, 500, None, None), 100);
    set_response(&mut service, &set(2, 1, 11, 600, None, None), 200);
    set_response(&mut service, &set(3, 1, 12, 700, None, None), 300);

    // Expiry observation frees the lock and zeroes renew_count; the same
    // holder re-acquiring afterwards is a renew that starts from zero.
    let (granted, event, lease) = set_response(&mut service, &set(4, 1, 13, 900, None, None), 800);
    assert!(granted && event == Event::Renew);
    assert_eq!(lease.unwrap().renew_count, 1);

    // Release zeroes renew_count; a fresh acquire starts at zero.
    assert!(matches!(
        execute(&mut service, &release(5, 1, 13), 810),
        Response::Release { released: true, .. }
    ));
    let (granted, event, lease) = set_response(&mut service, &set(6, 1, 14, 950, None, None), 820);
    assert!(granted && event == Event::Acquire);
    let lease = lease.unwrap();
    assert_eq!(lease.renew_count, 0);
    assert_eq!(lease.taken_at_ms, Some(820));
}

#[test]
fn get_returns_the_extended_lease_shape() {
    let mut service = Service::default();
    set_response(
        &mut service,
        &set(1, 1, 10, 500, Some("/a/b"), Some(vec!["tag-1"])),
        100,
    );
    let bytes = {
        let request = get(2);
        let (message_id, client_id, request_num) = request.ids();
        service
            .execute(
                message_id,
                client_id,
                request_num,
                100,
                &serde_json::to_vec(&request).unwrap(),
            )
            .unwrap()
    };
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(
        json["lease"],
        serde_json::json!({
            "lease_id": 10,
            "holder": id(1),
            "expiry": 500,
            "name": "/a/b",
            "labels": ["tag-1"],
            "taken_at_ms": 100,
            "renew_count": 0,
        })
    );
}

#[test]
fn break_is_unconditional_and_idempotent_and_keeps_metadata() {
    let mut service = Service::default();

    // Breaking a missing lock: idempotent success, no lease.
    assert_eq!(
        execute(&mut service, &break_lock(1), 100),
        Response::Break {
            message_id: id(1),
            request_num: 1,
            lock_id: LOCK_ID,
            broken: false,
            event: Event::Break,
            lease: None,
        }
    );

    set_response(
        &mut service,
        &set(2, 1, 10, 500, Some("/a"), Some(vec!["t"])),
        100,
    );

    // Breaking a held lock: fence token bumped, lease cleared, metadata kept.
    match execute(&mut service, &break_lock(3), 200) {
        Response::Break {
            broken,
            event,
            lease,
            ..
        } => {
            assert!(broken && event == Event::Break);
            let lease = lease.expect("break echoes the cleared lease");
            assert_eq!(lease.lease_id, 11, "break bumps the fencing token");
            assert_eq!(lease.holder, None);
            assert_eq!(lease.expiry, None);
            assert_eq!(lease.taken_at_ms, None);
            assert_eq!(lease.renew_count, 0);
            assert_eq!(lease.name.as_deref(), Some("/a"));
            assert_eq!(lease.labels, vec!["t"]);
        }
        response => panic!("BREAK must produce a BREAK response: {response:?}"),
    }

    // Breaking the now-free lock: idempotent success.
    assert!(matches!(
        execute(&mut service, &break_lock(4), 300),
        Response::Break {
            broken: false,
            event: Event::Break,
            lease: None,
            ..
        }
    ));

    // The lock is free afterwards; re-acquiring keeps the stored metadata.
    let (granted, event, lease) = set_response(&mut service, &set(5, 2, 20, 900, None, None), 400);
    assert!(granted && event == Event::Acquire);
    let lease = lease.unwrap();
    assert_eq!(lease.lease_id, 20);
    assert_eq!(lease.name.as_deref(), Some("/a"));
    assert_eq!(lease.labels, vec!["t"]);
}

#[test]
fn set_reply_serializes_the_event_and_extended_lease_fields() {
    let mut service = Service::default();
    let request = set(1, 1, 10, 500, None, None);
    let (message_id, client_id, request_num) = request.ids();
    let bytes = service
        .execute(
            message_id,
            client_id,
            request_num,
            100,
            &serde_json::to_vec(&request).unwrap(),
        )
        .unwrap();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(json["op"], "set");
    assert_eq!(json["event"], "acquire");
    assert_eq!(json["granted"], true);
    for field in [
        "lease_id",
        "holder",
        "expiry",
        "name",
        "labels",
        "taken_at_ms",
        "renew_count",
    ] {
        assert!(
            json["lease"].get(field).is_some(),
            "extended lease must carry {field}: {json}"
        );
    }
}
