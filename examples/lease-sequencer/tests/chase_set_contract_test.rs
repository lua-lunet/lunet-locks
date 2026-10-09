//! The chase driver's SET payload against the engine's lease contract.
//! The binary's `submit_hold`/`submit_renew` send
//! [`lease_sequencer::chase_set_payload`]'s exact shape, so this test
//! executes THAT payload against the engine's `Service` and pins three
//! facts: the payload decodes, a hold is GRANTED (with the
//! leader-stamped `expiry = execution_time + lease_ms`), and the
//! same-holder renewal renews (`Transition::Renew`, `renew_count` bump).
//! The negative leg refuses the retired absolute-`expiry` candidate —
//! a payload drifted back to the retired shape decodes to an error, and
//! on a live cluster it spins grantless (zero grants, zero renews): the
//! v0.14.0 standby-lane failure this test pins.

use lunet_advisory_lock::locks::{Request, Service, Transition};
use serde_json::Value;
use uuid::Uuid;

const HOLDER: &str = "0f0e0d0c-0b0a-0908-0706-050403020100";
const LOCK_ID: u64 = 0x0DDBA11;

fn set_payload(message_id: &str, client_id: u64, request_num: u64, lease_id: u64) -> String {
    lease_sequencer::chase_set_payload(
        message_id,
        LOCK_ID,
        client_id,
        request_num,
        lease_id,
        HOLDER,
        500,
    )
}

fn message_id_of(request: &Request) -> Uuid {
    match request {
        Request::Set { message_id, .. } => *message_id,
        other => panic!("a set request, got {other:?}"),
    }
}

#[test]
fn the_chase_set_payload_is_granted_and_renews() {
    let payload = set_payload("3f2a9b1c-5d6e-4f70-8a91-2b3c4d5e6f70", 1, 7, 42);
    let request = Service::decode(payload.as_bytes()).expect("the chase payload decodes");
    let message_id = message_id_of(&request);

    // The hold: execution tick 1000, a 500 ms window -> expiry 1500.
    let mut service = Service::default();
    let (bytes, transition) = service
        .execute(message_id, 1, 7, 1000, payload.as_bytes())
        .expect("the hold executes");
    let reply: Value = serde_json::from_slice(&bytes).expect("the reply is JSON");
    assert_eq!(reply["granted"], Value::Bool(true), "the hold is granted");
    assert_eq!(reply["lease"]["expiry"], Value::from(1500));
    assert_eq!(reply["lease"]["lease_ms"], Value::from(500));
    assert_eq!(reply["lease"]["renew_count"], Value::from(0));
    assert!(
        matches!(
            transition,
            Some(Transition::Hold {
                lock_id: 0x0DDBA11,
                lease_id: 42,
                ..
            })
        ),
        "the hold transition, got {transition:?}"
    );

    // The renewal: the same holder asks again with a fresh request number.
    let payload = set_payload("4a3b8c2d-6e7f-4a80-9b02-3c4d5e6f7081", 1, 8, 43);
    let request = Service::decode(payload.as_bytes()).expect("the renewal decodes");
    let message_id = message_id_of(&request);
    let (bytes, transition) = service
        .execute(message_id, 1, 8, 1100, payload.as_bytes())
        .expect("the renewal executes");
    let reply: Value = serde_json::from_slice(&bytes).expect("the reply is JSON");
    assert_eq!(
        reply["granted"],
        Value::Bool(true),
        "the renewal is granted"
    );
    assert_eq!(reply["lease"]["expiry"], Value::from(1600));
    assert_eq!(reply["lease"]["renew_count"], Value::from(1));
    assert!(
        matches!(
            transition,
            Some(Transition::Renew { lock_id: 0x0DDBA11, lease_id: 43, holder, .. })
                if holder == [0x0f, 0x0e, 0x0d, 0x0c, 0x0b, 0x0a, 0x09, 0x08, 0x07, 0x06, 0x05, 0x04, 0x03, 0x02, 0x01, 0x00]
        ),
        "the renew transition, got {transition:?}"
    );
}

#[test]
fn the_retired_absolute_expiry_candidate_is_refused() {
    // The shape the driver sent before the duration contract: the
    // candidate carries the retired `expiry` member and omits `lease_ms`.
    let payload = format!(
        "{{\"op\":\"set\",\"message_id\":\"3f2a9b1c-5d6e-4f70-8a91-2b3c4d5e6f70\",\
         \"client_id\":1,\"request_num\":7,\"lock_id\":{LOCK_ID},\
         \"lease\":{{\"lease_id\":42,\"holder\":\"{HOLDER}\",\"expiry\":1500}}}}"
    );
    assert!(
        Service::decode(payload.as_bytes()).is_err(),
        "the retired absolute-expiry candidate is refused at decode"
    );
}
