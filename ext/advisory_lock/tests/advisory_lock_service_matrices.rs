use std::collections::HashSet;

use lunet_advisory_lock::locks::{Lease, LeaseCandidate, Request, Response, Service};
use uuid::Uuid;

const EXECUTION_TIME: u64 = 100;
const LOCK_ID: u64 = 7;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Operation {
    Get,
    Set,
}

impl Operation {
    const ALL: [Self; 2] = [Self::Get, Self::Set];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Incumbent {
    Absent,
    Live,
    Expired,
}

impl Incumbent {
    const ALL: [Self; 3] = [Self::Absent, Self::Live, Self::Expired];

    /// The incumbent's stored lease and the instant it is installed at,
    /// so the stamp lands relative to the matrix's execution clock:
    /// `Live` (installed at EXECUTION_TIME) lives past it, `Expired`
    /// (installed one tick earlier) dies exactly at it. `Absent` leases
    /// nothing.
    fn lease(self, holder: Uuid) -> (Lease, u64) {
        match self {
            Self::Absent => (
                Lease {
                    lease_id: 11,
                    holder,
                    expiry: EXECUTION_TIME + 1,
                    lease_ms: 0,
                    name: None,
                    labels: None,
                    taken_at_ms: EXECUTION_TIME,
                    renew_count: 0,
                },
                EXECUTION_TIME,
            ),
            Self::Live => (
                Lease {
                    lease_id: 11,
                    holder,
                    expiry: EXECUTION_TIME + 1,
                    lease_ms: 1,
                    name: None,
                    labels: None,
                    taken_at_ms: EXECUTION_TIME,
                    renew_count: 0,
                },
                EXECUTION_TIME,
            ),
            Self::Expired => (
                Lease {
                    lease_id: 11,
                    holder,
                    expiry: EXECUTION_TIME,
                    lease_ms: 1,
                    name: None,
                    labels: None,
                    taken_at_ms: EXECUTION_TIME - 1,
                    renew_count: 0,
                },
                EXECUTION_TIME - 1,
            ),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum DurationClass {
    Zero,
    Positive,
}

impl DurationClass {
    const ALL: [Self; 2] = [Self::Zero, Self::Positive];

    fn duration(self) -> u64 {
        match self {
            Self::Zero => 0,
            Self::Positive => 1,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Holder {
    Same,
    Different,
}

impl Holder {
    const ALL: [Self; 2] = [Self::Same, Self::Different];

    fn candidate(self, incumbent: Uuid) -> Uuid {
        match self {
            Self::Same => incumbent,
            Self::Different => id(2),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum Envelope {
    Exact,
    MessageMismatch,
    ClientMismatch,
    RequestMismatch,
}

impl Envelope {
    const ALL: [Self; 4] = [
        Self::Exact,
        Self::MessageMismatch,
        Self::ClientMismatch,
        Self::RequestMismatch,
    ];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
struct Case {
    operation: Operation,
    incumbent: Incumbent,
    duration: DurationClass,
    holder: Holder,
    envelope: Envelope,
}

fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn request(operation: Operation, holder: Uuid, duration: u64) -> Request {
    match operation {
        Operation::Get => Request::Get {
            message_id: id(3),
            client_id: 5,
            request_num: 7,
            lock_id: LOCK_ID,
        },
        Operation::Set => Request::Set {
            message_id: id(3),
            client_id: 5,
            request_num: 7,
            lock_id: LOCK_ID,
            lease: LeaseCandidate {
                lease_id: 13,
                holder,
                lease_ms: duration,
            },
            name: None,
            labels: None,
            sent_at_ms: None,
        },
    }
}

fn install(service: &mut Service, lease: Lease, at: u64) {
    let request = Request::Set {
        message_id: id(9),
        client_id: 9,
        request_num: 9,
        lock_id: LOCK_ID,
        lease: LeaseCandidate {
            lease_id: lease.lease_id,
            holder: lease.holder,
            lease_ms: lease.lease_ms,
        },
        name: None,
        labels: None,
        sent_at_ms: None,
    };
    let (message_id, client_id, request_num) = request.ids();
    let executed = service
        .execute(
            message_id,
            client_id,
            request_num,
            at,
            &serde_json::to_vec(&request).expect("request serializes"),
        )
        .map(|(bytes, _)| bytes)
        .map_err(|error| error.to_string());
    assert!(executed.is_ok(), "incumbent installs at {at}");
    let response: Response = serde_json::from_slice(&executed.unwrap()).expect("stored reply");
    match response {
        Response::Set {
            granted: true,
            lease: Some(stored),
            ..
        } => assert_eq!(
            stored.expiry,
            at + stored.lease_ms,
            "the install must stamp expiry at its own execution tick"
        ),
        other => panic!("incumbent install did not grant: {other:?}"),
    }
}

fn observed_lease(service: &mut Service, lock_id: u64) -> Option<Lease> {
    observed_lease_at(service, lock_id, EXECUTION_TIME)
}

fn observed_lease_at(service: &mut Service, lock_id: u64, execution_time: u64) -> Option<Lease> {
    let request = Request::Get {
        message_id: id(10),
        client_id: 10,
        request_num: 10,
        lock_id,
    };
    let (message_id, client_id, request_num) = request.ids();
    let response: Response = serde_json::from_slice(
        &service
            .execute(
                message_id,
                client_id,
                request_num,
                execution_time,
                &serde_json::to_vec(&request).unwrap(),
            )
            .unwrap()
            .0,
    )
    .unwrap();
    match response {
        Response::Get { lease, .. } => lease,
        Response::Set { .. } | Response::Release { .. } | Response::Break { .. } => {
            unreachable!("GET must produce a GET response")
        }
    }
}

#[test]
fn release_requires_the_exact_live_lease_and_is_idempotent_after_expiry() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

fn service_with(incumbent: Incumbent, holder: Uuid) -> Service {
    let mut service = Service::default();
    if incumbent != Incumbent::Absent {
        let (incumbent, at) = incumbent.lease(holder);
        install(&mut service, incumbent, at);
    }
    service
}

#[test]
fn service_matrix_is_complete_correlated_and_deterministic() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}

#[test]
fn lock_isolation_and_u64_extrema_are_preserved() {
    panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
}
