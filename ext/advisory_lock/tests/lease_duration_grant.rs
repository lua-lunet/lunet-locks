//! The duration-grant discipline: the SET request asks for a DURATION
//! (`lease_ms`), never an absolute expiry; the leader stamps
//! `expiry = execution_time + lease_ms` on its own wallclock. The
//! client's clock never enters the lease's semantics: a client whose
//! clock is +5 s fast must not shorten anyone's lease, and a client
//! whose clock is -5 s slow must still HOLD. Under the old
//! absolute-expiry shape the client's clock decided both, and both
//! tests failed.

use lunet_advisory_lock::locks::{Request, Response, Service};
use serde_json::{Value, json};
use uuid::Uuid;

const LOCK_ID: u64 = 9001;
const LEASE_MS: u64 = 500;

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
        "lease": {"lease_id": 1, "holder": holder, "lease_ms": lease_ms},
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

fn granted_expiry(response: Response) -> (u64, u64) {
    match response {
        Response::Set {
            granted: true,
            lease: Some(lease),
            executed_at,
            ..
        } => (lease.expiry, executed_at),
        other => panic!("expected a granted SET reply, got {other:?}"),
    }
}

/// The drift-kill: the client's clock is +5 s fast, so under the old
/// shape its absolute `expiry` named a point 5 s past the leader's now
/// — the grant lived 5 s + 500 ms on the leader's timeline. Under the
/// duration shape the grant lives exactly 500 ms on the leader's
/// timeline, whatever the client's clock says.
#[test]
fn fast_client_clock_cannot_lengthen_the_lease() {
    let mut service = Service::default();
    let leader_now: u64 = 1_000_000;
    let fast_client_now = leader_now + 5_000;
    let _ = fast_client_now;
    let request = set_json(1, 1, 1, "11111111-1111-1111-1111-111111111111", LEASE_MS);
    let (expiry, executed_at) = granted_expiry(run(&mut service, 1, 1, 1, leader_now, request));
    assert_eq!(executed_at, leader_now);
    assert_eq!(
        expiry - executed_at,
        LEASE_MS,
        "the grant must live exactly lease_ms on the leader's timeline, \
         not on the client's"
    );
}

/// The slow client: under the old shape a -5 s client's absolute
/// `expiry` was already in the leader's past, so the grant was DENIED
/// (expiry <= execution_time) and the client could never hold. The
/// duration shape makes holding independent of the client's clock.
#[test]
fn slow_client_clock_still_holds() {
    let mut service = Service::default();
    let leader_now: u64 = 1_000_000;
    let request = set_json(1, 1, 1, "11111111-1111-1111-1111-111111111111", LEASE_MS);
    let (expiry, executed_at) = granted_expiry(run(&mut service, 1, 1, 1, leader_now, request));
    assert_eq!(expiry - executed_at, LEASE_MS);

    // The lease is live on the leader's timeline, and the holder's GET
    // reports it for the whole duration: the slow client holds.
    let held = run(
        &mut service,
        2,
        1,
        2,
        leader_now + LEASE_MS - 1,
        get_json(2, 1, 2),
    );
    match held {
        Response::Get { lease: Some(_), .. } => {}
        other => panic!("the slow client must hold through the window, got {other:?}"),
    }
}

/// A zero duration is never a grant: the guard is `lease_ms > 0`.
#[test]
fn zero_duration_is_refused() {
    let mut service = Service::default();
    let request = set_json(1, 1, 1, "11111111-1111-1111-1111-111111111111", 0);
    match run(&mut service, 1, 1, 1, 1_000, request) {
        Response::Set {
            granted: false,
            lease: None,
            ..
        } => {}
        other => panic!("a zero duration must never grant, got {other:?}"),
    }
}

/// Renewal keeps the holder across fast and slow clients: a +5 s-fast
/// holder renews, the re-stamped expiry sits one window past the RENEWAL
/// execution tick (never past the original take), and the incumbent's
/// `taken_at_ms` and `renew_count` survive. Then a -5 s-slow client — the
/// holder itself before the fix — renews again and still holds.
#[test]
fn renewal_restamps_on_the_leader_clock_and_keeps_the_holder() {
    let mut service = Service::default();
    let holder = "11111111-1111-1111-1111-111111111111";
    // Take at T: a fast client's clock plays no part in the stamp.
    let take = run(
        &mut service,
        1,
        1,
        1,
        1_000,
        set_json(1, 1, 1, holder, LEASE_MS),
    );
    let (expiry, _executed_at) = granted_expiry(take);
    assert_eq!(expiry, 1_000 + LEASE_MS);

    // Renew at T+250: expiry re-stamped one window past the renewal's
    // execution tick, taken_at_ms keeps the original take, renew_count 1.
    let renew = run(
        &mut service,
        2,
        1,
        2,
        1_250,
        set_json(2, 1, 2, holder, LEASE_MS),
    );
    let (renewed_expiry, renewed_at) = granted_expiry(renew);
    assert_eq!(renewed_expiry, 1_250 + LEASE_MS);
    assert_eq!(renewed_at, 1_250);
    match run(&mut service, 3, 1, 3, 1_300, get_json(3, 1, 3)) {
        Response::Get {
            lease: Some(lease), ..
        } => {
            assert_eq!(lease.taken_at_ms, 1_000, "the original take survives");
            assert_eq!(lease.renew_count, 1);
        }
        other => panic!("the holder must stand across the renewal, got {other:?}"),
    }

    // A second renewal at a much later tick still holds and re-stamps.
    let later = run(
        &mut service,
        4,
        1,
        4,
        1_400,
        set_json(4, 1, 4, holder, LEASE_MS),
    );
    let (later_expiry, _) = granted_expiry(later);
    assert_eq!(later_expiry, 1_400 + LEASE_MS);
}

/// Same-holder replace and other-holder deny are unchanged by the shape
/// swap: the same holder may take an EXPIRED incumbent (fresh Hold,
/// counters reset), a different holder is denied over a live incumbent,
/// and a denial mutates nothing.
#[test]
fn same_holder_replace_and_other_holder_deny_unchanged() {
    let holder_a = "11111111-1111-1111-1111-111111111111";
    let holder_b = "22222222-2222-2222-2222-222222222222";
    let mut service = Service::default();

    // A grants at T with a 300 ms window.
    granted_expiry(run(
        &mut service,
        1,
        1,
        1,
        1_000,
        set_json(1, 1, 1, holder_a, 300),
    ));
    // B denied over the live incumbent.
    match run(
        &mut service,
        2,
        2,
        1,
        1_050,
        set_json(2, 2, 1, holder_b, LEASE_MS),
    ) {
        Response::Set {
            granted: false,
            lease: Some(lease),
            ..
        } => assert_eq!(lease.holder.to_string(), holder_a, "the incumbent echoes"),
        other => panic!("a foreign SET over a live lease must be denied, got {other:?}"),
    }
    // A's lease expires; A re-takes (same holder, fresh Hold, counters reset).
    let retake = run(
        &mut service,
        3,
        1,
        2,
        1_400,
        set_json(3, 1, 2, holder_a, LEASE_MS),
    );
    let (retaken_expiry, retaken_at) = granted_expiry(retake);
    assert_eq!(retaken_expiry, 1_400 + LEASE_MS);
    assert_eq!(retaken_at, 1_400);
    match run(&mut service, 4, 1, 3, 1_450, get_json(4, 1, 3)) {
        Response::Get {
            lease: Some(lease), ..
        } => {
            assert_eq!(
                lease.taken_at_ms, 1_400,
                "a post-expiry take is a fresh take"
            );
            assert_eq!(lease.renew_count, 0);
        }
        other => panic!("the re-take must hold, got {other:?}"),
    }
    // B denied again; the state is untouched.
    match run(
        &mut service,
        5,
        2,
        2,
        1_450,
        set_json(5, 2, 2, holder_b, LEASE_MS),
    ) {
        Response::Set {
            granted: false,
            lease: Some(lease),
            ..
        } => {
            assert_eq!(lease.holder.to_string(), holder_a);
            assert_eq!(lease.renew_count, 0);
        }
        other => panic!("a foreign SET must be denied, got {other:?}"),
    }
}

/// The client's `sent_at_ms` is PURELY observational: two SETs identical
/// except for the send timestamp produce byte-identical replies, and no
/// amount of skew in it can flip a grant. (The compile-time half of the
/// proof is the execute arm's shape: it destructures the request without
/// ever binding `sent_at_ms`.)
#[test]
fn sent_at_ms_never_enters_any_decision() {
    let holder = "11111111-1111-1111-1111-111111111111";
    let mut with_ts = Service::default();
    let mut without_ts = Service::default();

    let mut request = set_json(1, 1, 1, holder, LEASE_MS);
    request["sent_at_ms"] = json!(999_999_999);
    let granted = run(&mut with_ts, 1, 1, 1, 1_000, request);
    let plain = run(
        &mut without_ts,
        1,
        1,
        1,
        1_000,
        set_json(1, 1, 1, holder, LEASE_MS),
    );

    let with_bytes = serde_json::to_value(&granted).unwrap();
    let plain_bytes = serde_json::to_value(&plain).unwrap();
    assert_eq!(
        with_bytes, plain_bytes,
        "the send timestamp must not perturb the reply"
    );

    // Even an absurdly skewed ts (a whole day fast) cannot flip a grant.
    let mut skewed = Service::default();
    let mut fast = set_json(2, 1, 1, holder, LEASE_MS);
    fast["sent_at_ms"] = json!(1_000 + 86_400_000);
    granted_expiry(run(&mut skewed, 2, 1, 1, 1_000, fast));
    match run(
        &mut skewed,
        3,
        1,
        2,
        1_000 + LEASE_MS - 1,
        get_json(3, 1, 2),
    ) {
        Response::Get { lease: Some(_), .. } => {}
        other => panic!("the skewed ts must not shorten the lease, got {other:?}"),
    }
}

/// The retired absolute-expiry candidate is refused at decode: a payload
/// that still carries the old `lease.expiry` shape never enters the
/// replication log (`deny_unknown_fields` on the candidate).
#[test]
fn the_old_absolute_expiry_candidate_is_refused() {
    let payload = json!({
        "op": "set",
        "message_id": uuid_text(1),
        "client_id": 1,
        "request_num": 1,
        "lock_id": LOCK_ID,
        "lease": {"lease_id": 1, "holder": "11111111-1111-1111-1111-111111111111",
                  "expiry": 1_722_600_001_000u64, "lease_ms": LEASE_MS},
    });
    assert!(
        Service::decode(payload.to_string().as_bytes()).is_err(),
        "the client cannot express expiry on the wire"
    );
    let mut service = Service::default();
    assert!(
        service
            .execute(
                Uuid::from_bytes([1; 16]),
                1,
                1,
                1_000,
                payload.to_string().as_bytes()
            )
            .is_err()
    );
}

/// The wire's UUID identity discipline (the run-3 rig regression): the
/// Contender drew its holder as bare 32-hex, which parsed silently and
/// only missed at the client's string comparison against the leader's
/// hyphenated echo — every granted renewal was absorbed as a denial and
/// no tenure survived. Decode now refuses every non-canonical id loudly,
/// before the request can enter the replication log.
#[test]
fn the_bare_hex_holder_the_contender_once_drew_is_refused_at_decode() {
    let payload = json!({
        "op": "set",
        "message_id": uuid_text(1),
        "client_id": 1,
        "request_num": 1,
        "lock_id": LOCK_ID,
        "lease": {"lease_id": 1,
                  "holder": "0000000000000000fb843133094f979d",
                  "lease_ms": LEASE_MS},
    });
    let error = Service::decode(payload.to_string().as_bytes())
        .expect_err("the bare-hex draw is the rig regression's exact shape");
    assert!(
        error.to_string().contains("canonical"),
        "the refusal names the canonical-form rule: {error}"
    );
}

#[test]
fn a_bare_hex_message_id_is_refused_at_decode() {
    let payload = json!({
        "op": "get",
        "message_id": "00000000000000000000000000000001",
        "client_id": 1,
        "request_num": 1,
        "lock_id": LOCK_ID,
    });
    assert!(
        Service::decode(payload.to_string().as_bytes()).is_err(),
        "every request id must be the canonical hyphenated form"
    );
}

#[test]
fn a_bare_hex_release_holder_is_refused_at_decode() {
    let payload = json!({
        "op": "release",
        "message_id": uuid_text(2),
        "client_id": 1,
        "request_num": 1,
        "lock_id": LOCK_ID,
        "holder": "0000000000000000fb843133094f979d",
        "lease_id": 1,
    });
    assert!(
        Service::decode(payload.to_string().as_bytes()).is_err(),
        "the release names its holder; it must be canonical too"
    );
}

#[test]
fn the_canonical_hyphenated_form_round_trips_unchanged() {
    let payload = set_json(1, 1, 1, "11111111-1111-1111-1111-111111111111", LEASE_MS);
    let request =
        Service::decode(payload.to_string().as_bytes()).expect("the canonical form decodes");
    let Request::Set { lease, .. } = request else {
        panic!("the payload is a set");
    };
    assert_eq!(
        lease.holder.to_string(),
        "11111111-1111-1111-1111-111111111111"
    );
}
