use std::collections::HashSet;

use uuid::Uuid;
use vrr::locks::{Lease, Request, Response, Service};

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

    fn lease(self, holder: Uuid) -> Option<Lease> {
        match self {
            Self::Absent => None,
            Self::Live => Some(Lease {
                lease_id: 11,
                holder,
                expiry: EXECUTION_TIME + 1,
            }),
            Self::Expired => Some(Lease {
                lease_id: 11,
                holder,
                expiry: EXECUTION_TIME,
            }),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
enum ExpiryRelation {
    Less,
    Equal,
    Greater,
}

impl ExpiryRelation {
    const ALL: [Self; 3] = [Self::Less, Self::Equal, Self::Greater];

    fn expiry(self) -> u64 {
        match self {
            Self::Less => EXECUTION_TIME - 1,
            Self::Equal => EXECUTION_TIME,
            Self::Greater => EXECUTION_TIME + 1,
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
    expiry: ExpiryRelation,
    holder: Holder,
    envelope: Envelope,
}

fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

fn request(operation: Operation, holder: Uuid, expiry: u64) -> Request {
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
            lease: Lease {
                lease_id: 13,
                holder,
                expiry,
            },
        },
    }
}

fn execute(service: &mut Service, request: &Request) -> Result<Vec<u8>, String> {
    let (message_id, client_id, request_num) = request.ids();
    service
        .execute(
            message_id,
            client_id,
            request_num,
            EXECUTION_TIME,
            &serde_json::to_vec(request).expect("request serializes"),
        )
        .map_err(|error| error.to_string())
}

fn install(service: &mut Service, lease: Lease) {
    let request = Request::Set {
        message_id: id(9),
        client_id: 9,
        request_num: 9,
        lock_id: LOCK_ID,
        lease,
    };
    assert!(execute(service, &request).is_ok(), "incumbent installs");
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
            .unwrap(),
    )
    .unwrap();
    match response {
        Response::Get { lease, .. } => lease,
        Response::Set { .. } => unreachable!("GET must produce a GET response"),
    }
}

fn service_with(incumbent: Incumbent, holder: Uuid) -> Service {
    let mut service = Service::default();
    if let Some(lease) = incumbent.lease(holder) {
        install(&mut service, lease);
    }
    service
}

#[test]
fn service_matrix_is_complete_correlated_and_deterministic() {
    let mut cases = HashSet::new();
    let incumbent_holder = id(1);

    for operation in Operation::ALL {
        for incumbent in Incumbent::ALL {
            for expiry in ExpiryRelation::ALL {
                for holder in Holder::ALL {
                    for envelope in Envelope::ALL {
                        let case = Case {
                            operation,
                            incumbent,
                            expiry,
                            holder,
                            envelope,
                        };
                        assert!(cases.insert(case), "duplicate matrix case: {case:?}");

                        let candidate_holder = holder.candidate(incumbent_holder);
                        let request = request(operation, candidate_holder, expiry.expiry());
                        let candidate_lease = match request {
                            Request::Set { lease, .. } => Some(lease),
                            Request::Get { .. } => None,
                        };
                        let incumbent_lease = incumbent.lease(incumbent_holder);
                        let expected_live =
                            incumbent_lease.filter(|lease| lease.expiry > EXECUTION_TIME);
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
                            serde_json::from_slice(&first_result.unwrap()).unwrap();
                        match (operation, response) {
                            (
                                Operation::Get,
                                Response::Get {
                                    message_id: response_id,
                                    request_num: response_num,
                                    lock_id,
                                    lease,
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
                                },
                            ) => {
                                let granted_expected = expected_live
                                    .is_none_or(|current| current.holder == candidate_holder);
                                assert_eq!(
                                    (response_id, response_num, lock_id),
                                    (message_id, request_num, LOCK_ID),
                                    "SET correlation failed for {case:?}"
                                );
                                assert_eq!(
                                    granted, granted_expected,
                                    "SET grant failed for {case:?}"
                                );
                                assert_eq!(
                                    lease,
                                    if granted {
                                        candidate_lease
                                    } else {
                                        expected_live
                                    },
                                    "SET response lease failed for {case:?}"
                                );
                                assert_eq!(
                                    observed_lease(&mut first, LOCK_ID),
                                    if granted {
                                        candidate_lease
                                            .filter(|lease| lease.expiry > EXECUTION_TIME)
                                    } else {
                                        expected_live
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

    assert_eq!(cases.len(), 144, "matrix cardinality changed");
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
        let lease = Lease {
            lease_id: value,
            holder: id(12),
            expiry: if value == u64::MIN { 1 } else { u64::MAX },
        };
        let set = Request::Set {
            message_id: id(13),
            client_id: value,
            request_num: value,
            lock_id: value,
            lease,
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
                .unwrap(),
        )
        .unwrap();
        assert_eq!(
            response,
            Response::Set {
                message_id: id(13),
                request_num: value,
                lock_id: value,
                granted: true,
                lease: Some(lease)
            }
        );
        assert_eq!(
            observed_lease_at(&mut service, value, value),
            if value == u64::MIN { Some(lease) } else { None }
        );
        assert_eq!(
            observed_lease_at(&mut service, value ^ 1, value),
            None,
            "lock IDs must be isolated at {value}"
        );
    }
}
