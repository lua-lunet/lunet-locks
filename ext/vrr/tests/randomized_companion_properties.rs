//! Bounded randomized companions to the finite matrices; these samples are not exhaustive.

use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use uuid::Uuid;
use vrr::vrr::{Body, Header, Input, LogEntry, Message, Replica, Tag, MAX_DATAGRAM};

const CASES: u32 = 128;
const MAX_PAYLOAD: usize = 1024;
const MAX_SEQUENCE: usize = 16;

fn replica() -> Replica {
    Replica::new(vec!["0:1".into(), "1:1".into(), "2:1".into()], "0:1")
        .expect("fixed test membership is valid")
}

fn request(client_id: u64, request_num: u64, execution_time: u64, payload: Vec<u8>) -> Input {
    Input::Request {
        client_id,
        request_num,
        message_id: Uuid::from_u128((client_id as u128) << 64 | request_num as u128),
        execution_time,
        payload,
    }
}

#[derive(Clone, Debug)]
enum SequenceInput {
    Request {
        client_id: u8,
        request_num: u64,
        execution_time: u64,
        payload: Vec<u8>,
    },
    Idle,
    LeaderTimeout,
}

fn sequence_input() -> impl Strategy<Value = SequenceInput> {
    prop_oneof![
        (
            0u8..8,
            any::<u64>(),
            any::<u64>(),
            prop::collection::vec(any::<u8>(), 0..=MAX_PAYLOAD)
        )
            .prop_map(|(client_id, request_num, execution_time, payload)| {
                SequenceInput::Request {
                    client_id,
                    request_num,
                    execution_time,
                    payload,
                }
            }),
        Just(SequenceInput::Idle),
        Just(SequenceInput::LeaderTimeout),
    ]
}

fn assert_replica_structure(replica: &Replica) {
    assert_eq!(replica.log().len() as u64, replica.slot());
    assert!(replica.executed() <= replica.commit());
    assert!(replica.commit() <= replica.slot());

    let mut requests = BTreeMap::new();
    for (index, entry) in replica.log().iter().enumerate() {
        assert_eq!(entry.slot, index as u64 + 1);
        assert!(requests
            .insert(entry.client_id, entry.request_num)
            .is_none_or(|previous| entry.request_num > previous));
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: CASES,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel("proptest-regressions"))),
        .. ProptestConfig::default()
    })]

    // `ffi_wire_matrices` covers finite canonical tags and boundary slots. Slots are
    // u64 values, so this samples the intentionally non-finite interior domain.
    #[test]
    fn wide_headers_round_trip(tag in prop_oneof![
        Just(Tag::Prepare), Just(Tag::PrepareOk), Just(Tag::Commit),
        Just(Tag::StartEpochChange), Just(Tag::DoEpochChange), Just(Tag::StartEpoch),
        Just(Tag::Recovery), Just(Tag::RecoveryResponse),
    ], epoch in any::<u32>(), slot in any::<u64>()) {
        let header = Header { tag, epoch, slot };
        prop_assert_eq!(Header::decode(&header.encode()), Some(header));
    }

    // `normal_operation_matrices` covers request ordering relations. Payload bytes,
    // request numbers, and timestamps have intentionally unbounded value domains.
    #[test]
    fn leader_request_preserves_wide_fields(
        client_id in any::<u64>(),
        request_num in any::<u64>(),
        execution_time in any::<u64>(),
        payload in prop::collection::vec(any::<u8>(), 0..=MAX_PAYLOAD),
    ) {
        let message_id = Uuid::from_u128((client_id as u128) << 64 | request_num as u128);
        let mut replica = replica();
        let outputs = replica.step(request(client_id, request_num, execution_time, payload.clone()));

        prop_assert_eq!(replica.slot(), 1);
        prop_assert_eq!(replica.log(), &[LogEntry {
            slot: 1,
            client_id,
            request_num,
            message_id,
            execution_time,
            payload: payload.clone(),
        }]);
        prop_assert_eq!(outputs, vec![vrr::vrr::Output::Broadcast(Message {
            epoch: 0,
            slot: 1,
            body: Body::Prepare { commit: 0, entry: replica.log()[0].clone() },
        })]);
    }

    // `ffi_wire_matrices` covers chosen malformed forms. Arbitrary byte strings are
    // genuinely unbounded, bounded here by the datagram limit for test resources.
    #[test]
    fn arbitrary_datagrams_are_rejected_or_canonically_round_trip(
        bytes in prop::collection::vec(any::<u8>(), 0..=MAX_DATAGRAM),
    ) {
        if let Some(message) = Message::decode(&bytes) {
            let encoded = message.encode().expect("decoded message serializes");
            prop_assert_eq!(Message::decode(&encoded), Some(message));
        }
    }

    // `normal_operation_matrices` covers individual request, idle, and timeout
    // transitions. This bounds interleavings rather than claiming sequence coverage.
    #[test]
    fn bounded_local_input_sequences_preserve_replica_structure(
        sequence in prop::collection::vec(sequence_input(), 0..=MAX_SEQUENCE),
    ) {
        let mut replica = replica();
        for input in sequence {
            match input {
                SequenceInput::Request { client_id, request_num, execution_time, payload } => {
                    replica.step(request(client_id.into(), request_num, execution_time, payload));
                }
                SequenceInput::Idle => { replica.step(Input::Idle); }
                SequenceInput::LeaderTimeout => { replica.step(Input::LeaderTimeout); }
            }
            assert_replica_structure(&replica);
        }
    }
}
