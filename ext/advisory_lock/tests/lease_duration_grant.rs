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
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The slow client: under the old shape a -5 s client's absolute
/// `expiry` was already in the leader's past, so the grant was DENIED
/// (expiry <= execution_time) and the client could never hold. The
/// duration shape makes holding independent of the client's clock.
#[test]
fn slow_client_clock_still_holds() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// A zero duration is never a grant: the guard is `lease_ms > 0`.
#[test]
fn zero_duration_is_refused() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// Renewal keeps the holder across fast and slow clients: a +5 s-fast
/// holder renews, the re-stamped expiry sits one window past the RENEWAL
/// execution tick (never past the original take), and the incumbent's
/// `taken_at_ms` and `renew_count` survive. Then a -5 s-slow client — the
/// holder itself before the fix — renews again and still holds.
#[test]
fn renewal_restamps_on_the_leader_clock_and_keeps_the_holder() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// Same-holder replace and other-holder deny are unchanged by the shape
/// swap: the same holder may take an EXPIRED incumbent (fresh Hold,
/// counters reset), a different holder is denied over a live incumbent,
/// and a denial mutates nothing.
#[test]
fn same_holder_replace_and_other_holder_deny_unchanged() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The client's `sent_at_ms` is PURELY observational: two SETs identical
/// except for the send timestamp produce byte-identical replies, and no
/// amount of skew in it can flip a grant. (The compile-time half of the
/// proof is the execute arm's shape: it destructures the request without
/// ever binding `sent_at_ms`.)
#[test]
fn sent_at_ms_never_enters_any_decision() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The retired absolute-expiry candidate is refused at decode: a payload
/// that still carries the old `lease.expiry` shape never enters the
/// replication log (`deny_unknown_fields` on the candidate).
#[test]
fn the_old_absolute_expiry_candidate_is_refused() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

/// The wire's UUID identity discipline (the run-3 rig regression): the
/// Contender drew its holder as bare 32-hex, which parsed silently and
/// only missed at the client's string comparison against the leader's
/// hyphenated echo — every granted renewal was absorbed as a denial and
/// no tenure survived. Decode now refuses every non-canonical id loudly,
/// before the request can enter the replication log.
#[test]
fn the_bare_hex_holder_the_contender_once_drew_is_refused_at_decode() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn a_bare_hex_message_id_is_refused_at_decode() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn a_bare_hex_release_holder_is_refused_at_decode() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn the_canonical_hyphenated_form_round_trips_unchanged() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
