use uuid::Uuid;
use vrr::vrr::{Body, Header, Input, LogEntry, LogState, Message, Output, Replica, Status, Tag};

fn members(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("{index}:1")).collect()
}

fn node(count: usize, index: usize) -> Replica {
    Replica::new(members(count), &format!("{index}:1")).unwrap()
}

fn entry(slot: u64, client_id: u64, request_num: u64) -> LogEntry {
    LogEntry {
        slot,
        client_id,
        request_num,
        message_id: Uuid::from_u128((client_id as u128) << 64 | request_num as u128),
        execution_time: 100 + slot,
        payload: format!("request-{slot}").into_bytes(),
    }
}

fn state(log: Vec<LogEntry>, commit: u64) -> LogState {
    LogState {
        slot: log.len() as u64,
        commit,
        log,
    }
}

fn message(epoch: u32, slot: u64, body: Body) -> Message {
    Message { epoch, slot, body }
}

fn receive(replica: &mut Replica, from: u32, message: Message) -> Vec<Output> {
    replica.step(Input::Message { from, message })
}

fn request(message: u8, request_num: u64) -> Input {
    Input::Request {
        client_id: 1,
        request_num,
        message_id: Uuid::from_bytes([message; 16]),
        execution_time: 100,
        payload: b"request".to_vec(),
    }
}

#[test]
fn configuration_and_canonical_tags_are_unambiguous() {
    assert!(Replica::new(members(2), "0:1").is_err());
    assert!(Replica::new(vec!["b".into(), "a".into(), "c".into()], "a").is_err());
    assert!(Replica::new(members(4), "0:1").is_ok());

    let tags = [
        (Tag::Prepare, 0x10),
        (Tag::PrepareOk, 0x11),
        (Tag::Commit, 0x12),
        (Tag::StartEpochChange, 0x20),
        (Tag::DoEpochChange, 0x21),
        (Tag::StartEpoch, 0x22),
        (Tag::Recovery, 0x30),
        (Tag::RecoveryResponse, 0x31),
    ];
    for (tag, number) in tags {
        let header = Header {
            tag,
            epoch: 0x0102_0304,
            slot: 0x0102_0304_0506_0708,
        };
        let bytes = header.encode();
        assert_eq!(u32::from_be_bytes(bytes[..4].try_into().unwrap()), number);
        assert_eq!(Header::decode(&bytes), Some(header));
    }

    let encoded = message(1, 0, Body::StartEpochChange).encode().unwrap();
    assert!(std::str::from_utf8(&encoded[16..])
        .unwrap()
        .contains("StartEpochChange"));
}

#[test]
fn four_node_normal_commit_needs_two_distinct_backups() {
    let mut leader = node(4, 0);
    let prepare = match leader.step(request(1, 1)).pop().unwrap() {
        Output::Broadcast(message) => message,
        output => panic!("unexpected {output:?}"),
    };
    let ack = message(0, 1, Body::PrepareOk);

    assert!(receive(&mut leader, 1, ack.clone()).is_empty());
    assert!(receive(&mut leader, 1, ack.clone()).is_empty());
    assert!(matches!(
        receive(&mut leader, 2, ack).as_slice(),
        [Output::Execute { slot: 1, .. }]
    ));
    assert_eq!(leader.commit(), 1);
    assert_eq!(leader.executed(), 0);

    let result = b"exact-result".to_vec();
    assert_eq!(
        leader.step(Input::Complete {
            slot: 1,
            result: result.clone(),
        }),
        vec![Output::Reply(result.clone())]
    );
    assert_eq!(leader.executed(), 1);
    assert_eq!(leader.step(request(9, 1)), vec![Output::Reply(result)]);
    assert!(matches!(prepare.body, Body::Prepare { .. }));
}

#[test]
fn duplicate_prepare_resends_ack_but_conflict_and_gap_are_refused() {
    let mut backup = node(3, 1);
    let first = entry(1, 1, 1);
    let prepare = message(
        0,
        1,
        Body::Prepare {
            commit: 0,
            entry: first.clone(),
        },
    );
    assert!(matches!(
        receive(&mut backup, 0, prepare.clone()).as_slice(),
        [Output::To(
            0,
            Message {
                body: Body::PrepareOk,
                ..
            }
        )]
    ));
    assert!(matches!(
        receive(&mut backup, 0, prepare).as_slice(),
        [Output::To(
            0,
            Message {
                body: Body::PrepareOk,
                ..
            }
        )]
    ));

    let mut conflict = first;
    conflict.payload = b"different".to_vec();
    assert!(receive(
        &mut backup,
        0,
        message(
            0,
            1,
            Body::Prepare {
                commit: 0,
                entry: conflict
            }
        )
    )
    .is_empty());
    assert!(receive(
        &mut backup,
        0,
        message(
            0,
            3,
            Body::Prepare {
                commit: 0,
                entry: entry(3, 2, 1),
            },
        ),
    )
    .is_empty());
    assert_eq!(backup.slot(), 1);
}

#[test]
fn four_node_epoch_change_uses_two_votes_and_three_reports() {
    let mut leader = node(4, 1);
    leader.step(Input::LeaderTimeout);
    let report = |from_entry: LogEntry| {
        message(
            1,
            1,
            Body::DoEpochChange {
                latest_normal: 0,
                state: state(vec![from_entry], 0),
            },
        )
    };
    assert!(receive(&mut leader, 0, report(entry(1, 1, 1))).is_empty());
    assert!(receive(&mut leader, 2, message(1, 0, Body::StartEpochChange)).is_empty());
    assert_eq!(leader.status(), Status::EpochChange);
    assert!(receive(&mut leader, 2, message(1, 0, Body::StartEpochChange)).is_empty());
    assert!(receive(&mut leader, 3, message(1, 0, Body::StartEpochChange)).is_empty());
    assert_eq!(leader.status(), Status::EpochChange);

    let outputs = receive(&mut leader, 2, report(entry(1, 1, 1)));
    assert_eq!(leader.status(), Status::Normal);
    assert!(outputs.iter().any(|output| matches!(
        output,
        Output::Broadcast(Message {
            body: Body::StartEpoch { .. },
            ..
        })
    )));
}

#[test]
fn four_node_recovery_requires_three_other_responders() {
    let mut replica = node(4, 3);
    replica.step(Input::Recover { nonce: 7 });
    let leader_state = state(vec![entry(1, 1, 1)], 1);
    receive(
        &mut replica,
        0,
        message(
            0,
            1,
            Body::RecoveryResponse {
                nonce: 7,
                state: Some(leader_state),
            },
        ),
    );
    receive(
        &mut replica,
        1,
        message(
            0,
            0,
            Body::RecoveryResponse {
                nonce: 7,
                state: None,
            },
        ),
    );
    assert_eq!(replica.status(), Status::Recovering);
    assert!(receive(
        &mut replica,
        1,
        message(
            0,
            0,
            Body::RecoveryResponse {
                nonce: 7,
                state: None,
            },
        ),
    )
    .is_empty());
    let outputs = receive(
        &mut replica,
        2,
        message(
            0,
            0,
            Body::RecoveryResponse {
                nonce: 7,
                state: None,
            },
        ),
    );
    assert_eq!(replica.status(), Status::Normal);
    assert_eq!(replica.commit(), 1);
    assert!(matches!(
        outputs.as_slice(),
        [Output::Execute { slot: 1, .. }]
    ));
}

#[test]
fn malformed_transferred_states_are_rejected_without_mutation() {
    let malformed = vec![
        LogState {
            slot: 2,
            commit: 0,
            log: vec![entry(1, 1, 1)],
        },
        LogState {
            slot: 1,
            commit: 2,
            log: vec![entry(1, 1, 1)],
        },
        state(vec![entry(2, 1, 1)], 0),
        state(vec![entry(1, 1, 2), entry(2, 1, 1)], 0),
    ];
    let mut duplicate_id = state(vec![entry(1, 1, 1), entry(2, 2, 1)], 0);
    duplicate_id.log[1].message_id = duplicate_id.log[0].message_id;

    for bad in malformed.into_iter().chain([duplicate_id]) {
        let mut replica = node(3, 2);
        let outputs = receive(
            &mut replica,
            1,
            message(1, bad.slot, Body::StartEpoch { state: bad }),
        );
        assert!(outputs.is_empty());
        assert_eq!(replica.epoch(), 0);
        assert_eq!(replica.slot(), 0);
        assert_eq!(replica.status(), Status::Normal);
    }

    let mut replica = node(3, 2);
    assert!(receive(
        &mut replica,
        1,
        message(
            1,
            0,
            Body::StartEpoch {
                state: state(vec![entry(1, 1, 1)], 0),
            },
        ),
    )
    .is_empty());
    assert_eq!(replica.epoch(), 0);
}

#[test]
fn epoch_change_selects_one_whole_log_and_rejects_uncovered_commit() {
    let mut leader = node(3, 1);
    leader.step(Input::LeaderTimeout);
    let short = state(vec![entry(1, 1, 1)], 0);
    receive(
        &mut leader,
        0,
        message(
            1,
            1,
            Body::DoEpochChange {
                latest_normal: 1,
                state: short,
            },
        ),
    );
    let outputs = receive(&mut leader, 2, message(1, 0, Body::StartEpochChange));
    assert_eq!(leader.status(), Status::Normal);
    let installed = outputs
        .iter()
        .find_map(|output| match output {
            Output::Broadcast(Message {
                body: Body::StartEpoch { state },
                ..
            }) => Some(state),
            _ => None,
        })
        .unwrap();
    assert_eq!(installed.log, vec![entry(1, 1, 1)]);

    let mut rejected = node(3, 1);
    rejected.step(Input::LeaderTimeout);
    receive(
        &mut rejected,
        0,
        message(
            1,
            1,
            Body::DoEpochChange {
                latest_normal: 1,
                state: state(vec![entry(1, 1, 1)], 0),
            },
        ),
    );
    let mut own_long = state(vec![entry(1, 1, 1), entry(2, 2, 1)], 0);
    own_long.commit = 2;
    receive(
        &mut rejected,
        2,
        message(
            1,
            2,
            Body::DoEpochChange {
                latest_normal: 0,
                state: own_long,
            },
        ),
    );
    let outputs = receive(&mut rejected, 2, message(1, 0, Body::StartEpochChange));
    assert!(outputs.is_empty());
    assert_eq!(rejected.status(), Status::EpochChange);
}

#[test]
fn delayed_same_epoch_install_and_report_cannot_reinstall_stale_state() {
    let mut backup = node(3, 2);
    receive(
        &mut backup,
        1,
        message(
            1,
            0,
            Body::StartEpoch {
                state: state(vec![], 0),
            },
        ),
    );
    let current = entry(1, 1, 1);
    receive(
        &mut backup,
        1,
        message(
            1,
            1,
            Body::Prepare {
                commit: 0,
                entry: current.clone(),
            },
        ),
    );
    assert!(receive(
        &mut backup,
        1,
        message(
            1,
            0,
            Body::StartEpoch {
                state: state(vec![], 0)
            }
        ),
    )
    .is_empty());
    assert_eq!(backup.log(), &[current]);

    let delayed = message(
        1,
        0,
        Body::DoEpochChange {
            latest_normal: 0,
            state: state(vec![], 0),
        },
    );
    assert!(receive(&mut backup, 0, delayed).is_empty());
    assert_eq!(backup.status(), Status::Normal);
}

#[test]
fn reconstructed_client_table_does_not_mark_newer_suffix_executed() {
    let mut leader = node(3, 1);
    leader.step(Input::LeaderTimeout);
    let transferred = state(vec![entry(1, 1, 1), entry(2, 1, 2)], 1);
    receive(
        &mut leader,
        0,
        message(
            1,
            2,
            Body::DoEpochChange {
                latest_normal: 0,
                state: transferred,
            },
        ),
    );
    let outputs = receive(&mut leader, 2, message(1, 0, Body::StartEpochChange));
    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::Execute { slot: 1, .. })));
    assert!(leader
        .step(Input::Complete {
            slot: 1,
            result: b"older".to_vec(),
        })
        .is_empty());
    assert!(leader.step(request(2, 2)).is_empty());
    assert_eq!(leader.executed(), 1);
    assert_eq!(leader.commit(), 1);
}

#[test]
fn future_install_must_preserve_locally_executed_prefix_and_epoch_exhaustion_fails_stop() {
    let mut backup = node(3, 1);
    let first = entry(1, 1, 1);
    let outputs = receive(
        &mut backup,
        0,
        message(
            0,
            1,
            Body::Prepare {
                commit: 1,
                entry: first.clone(),
            },
        ),
    );
    assert!(outputs
        .iter()
        .any(|output| matches!(output, Output::Execute { .. })));
    backup.step(Input::Complete {
        slot: 1,
        result: b"result".to_vec(),
    });

    let conflicting = state(vec![entry(1, 2, 1)], 1);
    assert!(receive(
        &mut backup,
        2,
        message(2, 1, Body::StartEpoch { state: conflicting }),
    )
    .is_empty());
    assert_eq!(backup.epoch(), 0);
    assert_eq!(backup.log(), &[first]);

    receive(
        &mut backup,
        0,
        message(
            u32::MAX,
            1,
            Body::StartEpoch {
                state: state(vec![entry(1, 1, 1)], 1),
            },
        ),
    );
    assert_eq!(backup.epoch(), u32::MAX);
    assert!(backup.step(Input::LeaderTimeout).is_empty());
    assert_eq!(backup.epoch(), u32::MAX);
}
