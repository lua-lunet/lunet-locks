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
    let holder = id(1);
    let incumbent = Lease {
        lease_id: 13,
        holder,
        expiry: 200,
        lease_ms: 100,
        name: None,
        labels: None,
        taken_at_ms: EXECUTION_TIME,
        renew_count: 0,
    };
    let mut service = Service::default();
    let install_lease = Lease {
        lease_id: incumbent.lease_id,
        holder: incumbent.holder,
        expiry: incumbent.expiry,
        lease_ms: incumbent.lease_ms,
        name: None,
        labels: None,
        taken_at_ms: incumbent.taken_at_ms,
        renew_count: incumbent.renew_count,
    };
    install(&mut service, install_lease, EXECUTION_TIME);

    let mismatch = Request::Release {
        message_id: id(2),
        client_id: 2,
        request_num: 1,
        lock_id: LOCK_ID,
        holder,
        lease_id: 14,
    };
    let exact = Request::Release {
        message_id: id(3),
        client_id: 2,
        request_num: 2,
        lock_id: LOCK_ID,
        holder,
        lease_id: 13,
    };
    let expired = Request::Release {
        message_id: id(4),
        client_id: 2,
        request_num: 3,
        lock_id: LOCK_ID,
        holder: id(9),
        lease_id: 99,
    };

    let response = |service: &mut Service, request: &Request, time| -> Response {
        serde_json::from_slice(
            &service
                .execute(
                    request.ids().0,
                    request.ids().1,
                    request.ids().2,
                    time,
                    &serde_json::to_vec(request).unwrap(),
                )
                .unwrap()
                .0,
        )
        .unwrap()
    };
    assert_eq!(
        response(&mut service, &mismatch, EXECUTION_TIME),
        Response::Release {
            message_id: id(2),
            request_num: 1,
            lock_id: LOCK_ID,
            released: false,
            lease: Some(incumbent),
            executed_at: EXECUTION_TIME,
        }
    );
    assert_eq!(
        response(&mut service, &exact, EXECUTION_TIME),
        Response::Release {
            message_id: id(3),
            request_num: 2,
            lock_id: LOCK_ID,
            released: true,
            lease: None,
            executed_at: EXECUTION_TIME,
        }
    );
    assert_eq!(
        response(&mut service, &expired, 200),
        Response::Release {
            message_id: id(4),
            request_num: 3,
            lock_id: LOCK_ID,
            released: true,
            lease: None,
            executed_at: 200,
        }
    );
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
    let mut cases = HashSet::new();
    let incumbent_holder = id(1);

    for operation in Operation::ALL {
        for incumbent in Incumbent::ALL {
            for duration in DurationClass::ALL {
                for holder in Holder::ALL {
                    for envelope in Envelope::ALL {
                        let case = Case {
                            operation,
                            incumbent,
                            duration,
                            holder,
                            envelope,
                        };
                        assert!(cases.insert(case), "duplicate matrix case: {case:?}");

                        let candidate_holder = holder.candidate(incumbent_holder);
                        let request = request(operation, candidate_holder, duration.duration());
                        let candidate = match &request {
                            Request::Set { lease, .. } => Some(lease.clone()),
                            Request::Get { .. } => None,
                            Request::Release { .. } | Request::Break { .. } => {
                                unreachable!("matrix only creates GET and SET")
                            }
                        };
                        let (candidate_lease, _) = incumbent.lease(incumbent_holder);
                        let expected_live = (incumbent != Incumbent::Absent)
                            .then_some(candidate_lease)
                            .filter(|lease| lease.expiry > EXECUTION_TIME);
                        let mut first = service_with(incumbent, incumbent_holder);
                        let mut second = service_with(incumbent, incumbent_holder);
                        let payload = serde_json::to_vec(&request).unwrap();
                        let (message_id, client_id, request_num) = request.ids();
                        let (actual_id, actual_client, actual_request_num) = match envelope {
                            Envelope::Exact => (message_id, client_id, request_num),
                            Envelope::MessageMismatch => (id(4), client_id, request_num),
                            Envelope::ClientMismatch => (message_id, client_id + 1, request_num),
                            Envelope::RequestMismatch => (message_id, client_id, request_num + 1),
                        };
                        let first_result = first
                            .execute(
                                actual_id,
                                actual_client,
                                actual_request_num,
                                EXECUTION_TIME,
                                &payload,
                            )
                            .map_err(|error| error.to_string());
                        let second_result = second
                            .execute(
                                actual_id,
                                actual_client,
                                actual_request_num,
                                EXECUTION_TIME,
                                &payload,
                            )
                            .map_err(|error| error.to_string());
                        assert_eq!(
                            first_result, second_result,
                            "replicas diverged for {case:?}"
                        );

                        if envelope != Envelope::Exact {
                            assert!(first_result.is_err(), "mismatch accepted for {case:?}");
                            assert_eq!(
                                observed_lease(&mut first, LOCK_ID),
                                expected_live,
                                "mismatch mutated lock state for {case:?}"
                            );
                            continue;
                        }

                        let response: Response =
                            serde_json::from_slice(&first_result.unwrap().0).unwrap();
                        match (operation, response) {
                            (
                                Operation::Get,
                                Response::Get {
                                    message_id: response_id,
                                    request_num: response_num,
                                    lock_id,
                                    lease,
                                    ..
                                },
                            ) => {
                                assert_eq!(
                                    (response_id, response_num, lock_id),
                                    (message_id, request_num, LOCK_ID),
                                    "GET correlation failed for {case:?}"
                                );
                                assert_eq!(
                                    lease, expected_live,
                                    "GET liveness failed for {case:?}"
                                );
                            }
                            (
                                Operation::Set,
                                Response::Set {
                                    message_id: response_id,
                                    request_num: response_num,
                                    lock_id,
                                    granted,
                                    lease,
                                    ..
                                },
                            ) => {
                                let granted_expected = expected_live
                                    .as_ref()
                                    .is_none_or(|current| current.holder == candidate_holder)
                                    && duration.duration() > 0;
                                assert_eq!(
                                    (response_id, response_num, lock_id),
                                    (message_id, request_num, LOCK_ID),
                                    "SET correlation failed for {case:?}"
                                );
                                assert_eq!(
                                    granted, granted_expected,
                                    "SET grant failed for {case:?}"
                                );
                                // A granted SET stores the state machine's
                                // record: the expiry is stamped at the
                                // leader's execution tick and the counters
                                // are tracked, never echoed from the
                                // request.
                                let granted_reply_lease = candidate.map(|candidate| Lease {
                                    lease_id: candidate.lease_id,
                                    holder: candidate.holder,
                                    expiry: EXECUTION_TIME + candidate.lease_ms,
                                    lease_ms: candidate.lease_ms,
                                    name: None,
                                    labels: None,
                                    taken_at_ms: EXECUTION_TIME,
                                    renew_count: if expected_live
                                        .as_ref()
                                        .is_some_and(|live| live.holder == candidate_holder)
                                    {
                                        1
                                    } else {
                                        0
                                    },
                                });
                                assert_eq!(
                                    lease,
                                    if granted {
                                        granted_reply_lease.clone()
                                    } else {
                                        expected_live.clone()
                                    },
                                    "SET response lease failed for {case:?}"
                                );
                                assert_eq!(
                                    observed_lease(&mut first, LOCK_ID),
                                    if granted {
                                        granted_reply_lease
                                            .filter(|lease| lease.expiry > EXECUTION_TIME)
                                    } else {
                                        expected_live.clone()
                                    },
                                    "SET state failed for {case:?}"
                                );
                            }
                            (_, response) => {
                                panic!("wrong response variant for {case:?}: {response:?}")
                            }
                        }
                    }
                }
            }
        }
    }

    assert_eq!(cases.len(), 96, "matrix cardinality changed");
}

#[test]
fn lock_isolation_and_u64_extrema_are_preserved() {
    let cases = [u64::MIN, u64::MAX];
    assert_eq!(
        cases.iter().copied().collect::<HashSet<_>>().len(),
        2,
        "duplicate boundary case"
    );

    for value in cases {
        let mut service = Service::default();
        let duration: u64 = 1;
        let set = Request::Set {
            message_id: id(13),
            client_id: value,
            request_num: value,
            lock_id: value,
            lease: LeaseCandidate {
                lease_id: value,
                holder: id(12),
                lease_ms: duration,
            },
            name: None,
            labels: None,
            sent_at_ms: None,
        };
        let (message_id, client_id, request_num) = set.ids();
        let response: Response = serde_json::from_slice(
            &service
                .execute(
                    message_id,
                    client_id,
                    request_num,
                    value,
                    &serde_json::to_vec(&set).unwrap(),
                )
                .unwrap()
                .0,
        )
        .unwrap();
        let stamped = Lease {
            lease_id: value,
            holder: id(12),
            expiry: value.saturating_add(duration),
            lease_ms: duration,
            name: None,
            labels: None,
            taken_at_ms: value,
            renew_count: 0,
        };
        let live = stamped.expiry > value;
        assert_eq!(
            response,
            Response::Set {
                message_id: id(13),
                request_num: value,
                lock_id: value,
                granted: true,
                lease: Some(stamped.clone()),
                executed_at: value,
            }
        );
        assert_eq!(
            observed_lease_at(&mut service, value, value),
            live.then_some(stamped.clone())
        );
        assert_eq!(
            observed_lease_at(&mut service, value ^ 1, value),
            None,
            "lock IDs must be isolated at {value}"
        );
    }
}
