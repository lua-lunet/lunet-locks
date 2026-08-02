#![allow(dead_code)] // Each integration target compiles this shared module independently.

use std::collections::HashSet;

use uuid::Uuid;
use vrr::locks::{Lease, Request, Service};
use vrr::vrr::{Body, Input, LogEntry, LogState, Message, NodeId, Output, Replica, Status};

pub const MEMBER_COUNT: usize = 4;
pub const EPOCH: u32 = 17;
pub const SLOT: u64 = 19;
pub const NODE_ID: NodeId = 2;
pub const CLIENT_ID: u64 = 23;
pub const REQUEST_NUM: u64 = 29;
pub const NONCE: u64 = 31;
pub const EXECUTION_TIME: u64 = 100;
pub const LEASE_EXPIRY: u64 = EXECUTION_TIME;

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Relation {
    Less,
    Equal,
    Greater,
}

impl Relation {
    pub const ALL: [Self; 3] = [Self::Less, Self::Equal, Self::Greater];

    pub fn epoch(self) -> u32 {
        match self {
            Self::Less => EPOCH
                .checked_sub(1)
                .expect("epoch baseline has a predecessor"),
            Self::Equal => EPOCH,
            Self::Greater => EPOCH
                .checked_add(1)
                .expect("epoch baseline has a successor"),
        }
    }

    pub fn slot(self) -> u64 {
        match self {
            Self::Less => SLOT
                .checked_sub(1)
                .expect("slot baseline has a predecessor"),
            Self::Equal => SLOT,
            Self::Greater => SLOT.checked_add(1).expect("slot baseline has a successor"),
        }
    }

    pub fn node_id(self) -> NodeId {
        match self {
            Self::Less => NODE_ID
                .checked_sub(1)
                .expect("node ID baseline has a predecessor"),
            Self::Equal => NODE_ID,
            Self::Greater => NODE_ID
                .checked_add(1)
                .expect("node ID baseline has a successor"),
        }
    }

    pub fn request_num(self) -> u64 {
        match self {
            Self::Less => REQUEST_NUM
                .checked_sub(1)
                .expect("request baseline has a predecessor"),
            Self::Equal => REQUEST_NUM,
            Self::Greater => REQUEST_NUM
                .checked_add(1)
                .expect("request baseline has a successor"),
        }
    }

    pub fn nonce(self) -> u64 {
        match self {
            Self::Less => NONCE
                .checked_sub(1)
                .expect("nonce baseline has a predecessor"),
            Self::Equal => NONCE,
            Self::Greater => NONCE
                .checked_add(1)
                .expect("nonce baseline has a successor"),
        }
    }

    pub fn lease_expiry(self) -> u64 {
        match self {
            Self::Less => LEASE_EXPIRY
                .checked_sub(1)
                .expect("lease baseline has a predecessor"),
            Self::Equal => LEASE_EXPIRY,
            Self::Greater => LEASE_EXPIRY
                .checked_add(1)
                .expect("lease baseline has a successor"),
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum InputValidity {
    Valid,
    Null,
    Invalid,
}

impl InputValidity {
    pub const ALL: [Self; 3] = [Self::Valid, Self::Null, Self::Invalid];
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Boundary {
    Minimum,
    Baseline,
    Maximum,
}

impl Boundary {
    pub const ALL: [Self; 3] = [Self::Minimum, Self::Baseline, Self::Maximum];

    pub fn u32(self) -> u32 {
        match self {
            Self::Minimum => u32::MIN,
            Self::Baseline => EPOCH,
            Self::Maximum => u32::MAX,
        }
    }

    pub fn u64(self) -> u64 {
        match self {
            Self::Minimum => u64::MIN,
            Self::Baseline => SLOT,
            Self::Maximum => u64::MAX,
        }
    }
}

#[derive(Copy, Clone, Debug, Eq, Hash, PartialEq)]
pub enum Sender {
    Leader,
    BackupOne,
    BackupTwo,
    SelfNode,
    NonMember,
}

impl Sender {
    pub const ALL: [Self; 5] = [
        Self::Leader,
        Self::BackupOne,
        Self::BackupTwo,
        Self::SelfNode,
        Self::NonMember,
    ];

    pub fn node_id(self, epoch: u32) -> NodeId {
        let leader = epoch % MEMBER_COUNT as u32;
        match self {
            Self::Leader => leader,
            Self::BackupOne => (leader + 1) % MEMBER_COUNT as u32,
            Self::BackupTwo => (leader + 2) % MEMBER_COUNT as u32,
            Self::SelfNode => (leader + 3) % MEMBER_COUNT as u32,
            Self::NonMember => MEMBER_COUNT as u32,
        }
    }
}

pub fn members(count: usize) -> Vec<String> {
    (0..count).map(|index| format!("{index}:1")).collect()
}

pub fn node(count: usize, index: usize) -> Replica {
    Replica::new(members(count), &format!("{index}:1")).expect("valid test membership")
}

pub fn baseline_replica() -> Replica {
    node(MEMBER_COUNT, 3)
}

pub fn id(byte: u8) -> Uuid {
    Uuid::from_bytes([byte; 16])
}

pub fn entry(slot: u64, client_id: u64, request_num: u64) -> LogEntry {
    LogEntry {
        slot,
        client_id,
        request_num,
        message_id: Uuid::from_u128((client_id as u128) << 64 | request_num as u128),
        execution_time: EXECUTION_TIME.saturating_add(slot),
        payload: format!("request-{slot}").into_bytes(),
    }
}

pub fn baseline_entry() -> LogEntry {
    entry(1, CLIENT_ID, REQUEST_NUM)
}

pub fn state(log: Vec<LogEntry>, commit: u64) -> LogState {
    LogState {
        slot: u64::try_from(log.len()).expect("test log length fits in a slot"),
        commit,
        log,
    }
}

pub fn baseline_state() -> LogState {
    state(vec![baseline_entry()], 0)
}

pub fn message(epoch: u32, slot: u64, body: Body) -> Message {
    Message { epoch, slot, body }
}

pub fn receive(replica: &mut Replica, from: NodeId, message: Message) -> Vec<Output> {
    replica.step(Input::Message { from, message })
}

pub fn request(message: u8, request_num: u64) -> Input {
    Input::Request {
        client_id: 1,
        request_num,
        message_id: id(message),
        execution_time: EXECUTION_TIME,
        payload: b"request".to_vec(),
    }
}

pub fn set(message: u8, client: u64, request: u64, holder: u8, expiry: u64) -> Request {
    Request::Set {
        message_id: id(message),
        client_id: client,
        request_num: request,
        lock_id: 7,
        lease: Lease {
            lease_id: 9,
            holder: id(holder),
            expiry,
        },
    }
}

pub fn baseline_service() -> Service {
    Service::default()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicaSnapshot {
    epoch: u32,
    status: Status,
    slot: u64,
    commit: u64,
    executed: u64,
    log: Vec<LogEntry>,
}

impl ReplicaSnapshot {
    pub fn capture(replica: &Replica) -> Self {
        Self {
            epoch: replica.epoch(),
            status: replica.status(),
            slot: replica.slot(),
            commit: replica.commit(),
            executed: replica.executed(),
            log: replica.log().to_vec(),
        }
    }
}

pub fn assert_replica_unchanged(
    context: impl std::fmt::Debug,
    before: &ReplicaSnapshot,
    replica: &Replica,
) {
    let after = ReplicaSnapshot::capture(replica);
    assert_eq!(
        before, &after,
        "refused case {context:?} mutated replica state"
    );
}

pub fn assert_complete_cases<T>(
    context: impl std::fmt::Debug,
    expected_count: usize,
    cases: impl IntoIterator<Item = T>,
) where
    T: Copy + std::fmt::Debug + Eq + std::hash::Hash,
{
    let cases: Vec<_> = cases.into_iter().collect();
    assert_eq!(
        cases.len(),
        expected_count,
        "wrong case count for {context:?}; complete cases: {cases:?}"
    );

    let unique: HashSet<_> = cases.iter().copied().collect();
    assert_eq!(
        unique.len(),
        expected_count,
        "duplicate case(s) for {context:?}; complete cases: {cases:?}"
    );
}
