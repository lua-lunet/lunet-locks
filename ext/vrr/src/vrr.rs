use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use uuid::Uuid;

pub const MAX_DATAGRAM: usize = 65_507;
pub type NodeId = u32;
pub type Epoch = u32;
pub type Slot = u64;

#[repr(u32)]
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Tag {
    Prepare = 0x10,
    PrepareOk = 0x11,
    Commit = 0x12,
    StartEpochChange = 0x20,
    DoEpochChange = 0x21,
    StartEpoch = 0x22,
    Recovery = 0x30,
    RecoveryResponse = 0x31,
}

impl Tag {
    fn from_u32(value: u32) -> Option<Self> {
        Some(match value {
            0x10 => Self::Prepare,
            0x11 => Self::PrepareOk,
            0x12 => Self::Commit,
            0x20 => Self::StartEpochChange,
            0x21 => Self::DoEpochChange,
            0x22 => Self::StartEpoch,
            0x30 => Self::Recovery,
            0x31 => Self::RecoveryResponse,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Header {
    pub tag: Tag,
    pub epoch: Epoch,
    pub slot: Slot,
}

impl Header {
    pub fn encode(self) -> [u8; 16] {
        let mut bytes = [0; 16];
        bytes[..4].copy_from_slice(&(self.tag as u32).to_be_bytes());
        bytes[4..8].copy_from_slice(&self.epoch.to_be_bytes());
        bytes[8..].copy_from_slice(&self.slot.to_be_bytes());
        bytes
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 16 {
            return None;
        }
        Some(Self {
            tag: Tag::from_u32(u32::from_be_bytes(bytes[..4].try_into().ok()?))?,
            epoch: u32::from_be_bytes(bytes[4..8].try_into().ok()?),
            slot: u64::from_be_bytes(bytes[8..16].try_into().ok()?),
        })
    }
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LogEntry {
    pub slot: Slot,
    pub client_id: u64,
    pub request_num: u64,
    pub message_id: Uuid,
    pub execution_time: u64,
    pub payload: Vec<u8>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub struct LogState {
    pub slot: Slot,
    pub commit: Slot,
    pub log: Vec<LogEntry>,
}

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
pub enum Body {
    Prepare {
        commit: Slot,
        entry: LogEntry,
    },
    PrepareOk,
    Commit,
    StartEpochChange,
    DoEpochChange {
        latest_normal: Epoch,
        state: LogState,
    },
    StartEpoch {
        state: LogState,
    },
    Recovery {
        nonce: u64,
    },
    RecoveryResponse {
        nonce: u64,
        state: Option<LogState>,
    },
}

impl Body {
    fn tag(&self) -> Tag {
        match self {
            Self::Prepare { .. } => Tag::Prepare,
            Self::PrepareOk => Tag::PrepareOk,
            Self::Commit => Tag::Commit,
            Self::StartEpochChange => Tag::StartEpochChange,
            Self::DoEpochChange { .. } => Tag::DoEpochChange,
            Self::StartEpoch { .. } => Tag::StartEpoch,
            Self::Recovery { .. } => Tag::Recovery,
            Self::RecoveryResponse { .. } => Tag::RecoveryResponse,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Message {
    pub epoch: Epoch,
    pub slot: Slot,
    pub body: Body,
}

impl Message {
    pub fn header(&self) -> Header {
        Header {
            tag: self.body.tag(),
            epoch: self.epoch,
            slot: self.slot,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        let mut bytes = self.header().encode().to_vec();
        bytes.extend(serde_json::to_vec(&self.body)?);
        Ok(bytes)
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let header = Header::decode(bytes)?;
        let body: Body = serde_json::from_slice(&bytes[16..]).ok()?;
        (header.tag == body.tag()).then_some(Self {
            epoch: header.epoch,
            slot: header.slot,
            body,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Input {
    Request {
        client_id: u64,
        request_num: u64,
        message_id: Uuid,
        execution_time: u64,
        payload: Vec<u8>,
    },
    Message {
        from: NodeId,
        message: Message,
    },
    Idle,
    LeaderTimeout,
    Complete {
        slot: Slot,
        result: Vec<u8>,
    },
    Recover {
        nonce: u64,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output {
    Broadcast(Message),
    To(NodeId, Message),
    Execute {
        slot: Slot,
        client_id: u64,
        request_num: u64,
        message_id: Uuid,
        execution_time: u64,
        payload: Vec<u8>,
    },
    Reply(Vec<u8>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Status {
    Normal,
    EpochChange,
    Recovering,
}

#[derive(Clone)]
struct ClientEntry {
    request_num: u64,
    message_id: Uuid,
    result: Option<Vec<u8>>,
}

pub struct Replica {
    members: Vec<String>,
    node: NodeId,
    epoch: Epoch,
    status: Status,
    slot: Slot,
    commit_slot: Slot,
    executed_slot: Slot,
    executing: Option<Slot>,
    log: Vec<LogEntry>,
    clients: BTreeMap<u64, ClientEntry>,
    prepare_oks: BTreeMap<Slot, BTreeSet<NodeId>>,
    latest_normal: Epoch,
    start_changes: BTreeSet<NodeId>,
    sent_do_change: bool,
    do_changes: BTreeMap<NodeId, (Epoch, LogState)>,
    recovery_nonce: Option<u64>,
    recovery: BTreeMap<NodeId, (Epoch, Option<LogState>)>,
}

impl Replica {
    pub fn new(members: Vec<String>, own: &str) -> Result<Self, &'static str> {
        if members.len() < 3 || members.len() > u32::MAX as usize {
            return Err("membership must contain at least three representable nodes");
        }
        if members.windows(2).any(|pair| pair[0] >= pair[1]) {
            return Err("membership must be strictly sorted");
        }
        let node = members
            .iter()
            .position(|member| member == own)
            .ok_or("own address is not in membership")? as NodeId;
        Ok(Self {
            members,
            node,
            epoch: 0,
            status: Status::Normal,
            slot: 0,
            commit_slot: 0,
            executed_slot: 0,
            executing: None,
            log: Vec::new(),
            clients: BTreeMap::new(),
            prepare_oks: BTreeMap::new(),
            latest_normal: 0,
            start_changes: BTreeSet::new(),
            sent_do_change: false,
            do_changes: BTreeMap::new(),
            recovery_nonce: None,
            recovery: BTreeMap::new(),
        })
    }

    pub fn epoch(&self) -> Epoch {
        self.epoch
    }
    pub fn status(&self) -> Status {
        self.status
    }
    pub fn slot(&self) -> Slot {
        self.slot
    }
    pub fn commit(&self) -> Slot {
        self.commit_slot
    }
    pub fn executed(&self) -> Slot {
        self.executed_slot
    }
    pub fn log(&self) -> &[LogEntry] {
        &self.log
    }
    pub fn leader_of(&self, epoch: Epoch) -> NodeId {
        epoch % self.members.len() as u32
    }
    pub fn is_leader(&self) -> bool {
        self.leader_of(self.epoch) == self.node
    }

    pub fn request_datagram_size(
        &self,
        client_id: u64,
        request_num: u64,
        message_id: Uuid,
        execution_time: u64,
        payload: &[u8],
    ) -> usize {
        if self.status != Status::Normal
            || !self.is_leader()
            || self
                .clients
                .get(&client_id)
                .is_some_and(|client| request_num <= client.request_num)
        {
            return 0;
        }
        let Some(slot) = self.slot.checked_add(1) else {
            return MAX_DATAGRAM + 1;
        };
        self.message(
            slot,
            Body::Prepare {
                commit: self.commit_slot,
                entry: LogEntry {
                    slot,
                    client_id,
                    request_num,
                    message_id,
                    execution_time,
                    payload: payload.to_vec(),
                },
            },
        )
        .encode()
        .map_or(MAX_DATAGRAM + 1, |bytes| bytes.len())
    }

    fn quorum(&self) -> usize {
        self.members.len() / 2 + 1
    }
    fn member(&self, node: NodeId) -> bool {
        (node as usize) < self.members.len()
    }
    fn message(&self, slot: Slot, body: Body) -> Message {
        Message {
            epoch: self.epoch,
            slot,
            body,
        }
    }
    fn state(&self) -> LogState {
        LogState {
            slot: self.slot,
            commit: self.commit_slot,
            log: self.log.clone(),
        }
    }

    pub fn step(&mut self, input: Input) -> Vec<Output> {
        match input {
            Input::Request {
                client_id,
                request_num,
                message_id,
                execution_time,
                payload,
            } => self.request(client_id, request_num, message_id, execution_time, payload),
            Input::Message { from, message } => self.receive(from, message),
            Input::Idle if self.status == Status::Normal && self.is_leader() => {
                vec![Output::Broadcast(
                    self.message(self.commit_slot, Body::Commit),
                )]
            }
            Input::LeaderTimeout if self.status != Status::Recovering => self
                .epoch
                .checked_add(1)
                .map_or_else(Vec::new, |epoch| self.enter_change(epoch)),
            Input::Complete { slot, result } if self.status != Status::Recovering => {
                self.complete(slot, result)
            }
            Input::Recover { nonce } => {
                self.status = Status::Recovering;
                self.recovery_nonce = Some(nonce);
                self.recovery.clear();
                vec![Output::Broadcast(
                    self.message(self.slot, Body::Recovery { nonce }),
                )]
            }
            _ => Vec::new(),
        }
    }

    fn request(
        &mut self,
        client_id: u64,
        request_num: u64,
        message_id: Uuid,
        execution_time: u64,
        payload: Vec<u8>,
    ) -> Vec<Output> {
        if self.status != Status::Normal || !self.is_leader() {
            return Vec::new();
        }
        if let Some(client) = self.clients.get(&client_id) {
            if request_num < client.request_num {
                return Vec::new();
            }
            if request_num == client.request_num {
                return client
                    .result
                    .clone()
                    .map(Output::Reply)
                    .into_iter()
                    .collect();
            }
        }
        let Some(slot) = self.slot.checked_add(1) else {
            return Vec::new();
        };
        if self.log.iter().any(|entry| entry.message_id == message_id) {
            return Vec::new();
        }
        self.slot = slot;
        let entry = LogEntry {
            slot: self.slot,
            client_id,
            request_num,
            message_id,
            execution_time,
            payload,
        };
        self.log.push(entry.clone());
        self.clients.insert(
            client_id,
            ClientEntry {
                request_num,
                message_id,
                result: None,
            },
        );
        vec![Output::Broadcast(self.message(
            self.slot,
            Body::Prepare {
                commit: self.commit_slot,
                entry,
            },
        ))]
    }

    fn receive(&mut self, from: NodeId, message: Message) -> Vec<Output> {
        if !self.member(from) || from == self.node {
            return Vec::new();
        }
        let epoch = message.epoch;
        let slot = message.slot;
        match message.body {
            Body::Prepare { commit, entry }
                if self.status == Status::Normal
                    && epoch == self.epoch
                    && from == self.leader_of(epoch)
                    && !self.is_leader() =>
            {
                if slot <= self.slot {
                    let installed = usize::try_from(slot)
                        .ok()
                        .and_then(|slot| slot.checked_sub(1))
                        .and_then(|index| self.log.get(index));
                    if installed != Some(&entry) || commit > self.slot {
                        return Vec::new();
                    }
                    self.commit_slot = self.commit_slot.max(commit);
                    let mut out = self.pending_execution();
                    out.push(Output::To(from, self.message(slot, Body::PrepareOk)));
                    return out;
                }
                if self.slot.checked_add(1) != Some(slot)
                    || entry.slot != slot
                    || commit > slot
                    || self
                        .log
                        .iter()
                        .any(|old| old.message_id == entry.message_id)
                    || self
                        .clients
                        .get(&entry.client_id)
                        .is_some_and(|client| entry.request_num <= client.request_num)
                {
                    return Vec::new();
                }
                self.slot = slot;
                self.log.push(entry.clone());
                self.clients.insert(
                    entry.client_id,
                    ClientEntry {
                        request_num: entry.request_num,
                        message_id: entry.message_id,
                        result: None,
                    },
                );
                self.commit_slot = self.commit_slot.max(commit);
                let mut out = self.pending_execution();
                out.push(Output::To(from, self.message(self.slot, Body::PrepareOk)));
                out
            }
            Body::PrepareOk
                if self.status == Status::Normal
                    && epoch == self.epoch
                    && self.is_leader()
                    && slot <= self.slot =>
            {
                self.prepare_oks.entry(slot).or_default().insert(from);
                if self.prepare_oks[&slot].len() < self.quorum() - 1 {
                    Vec::new()
                } else {
                    self.commit_slot = self.commit_slot.max(slot);
                    self.pending_execution()
                }
            }
            Body::Commit
                if self.status == Status::Normal
                    && epoch == self.epoch
                    && from == self.leader_of(epoch)
                    && slot <= self.slot =>
            {
                self.commit_slot = self.commit_slot.max(slot);
                self.pending_execution()
            }
            Body::StartEpochChange if self.status != Status::Recovering && epoch >= self.epoch => {
                let mut out = if epoch > self.epoch {
                    self.enter_change(epoch)
                } else {
                    Vec::new()
                };
                if self.status == Status::EpochChange {
                    self.start_changes.insert(from);
                    out.extend(self.qualify_change());
                }
                out
            }
            Body::DoEpochChange {
                latest_normal,
                state,
            } if self.status != Status::Recovering
                && epoch >= self.epoch
                && self.leader_of(epoch) == self.node =>
            {
                if !self.valid_state_message(slot, &state) {
                    return Vec::new();
                }
                let mut out = if epoch > self.epoch {
                    self.enter_change(epoch)
                } else {
                    Vec::new()
                };
                if self.status != Status::EpochChange {
                    return out;
                }
                self.do_changes.insert(from, (latest_normal, state));
                out.extend(self.finish_change());
                out
            }
            Body::StartEpoch { state }
                if self.status != Status::Recovering
                    && epoch >= self.epoch
                    && from == self.leader_of(epoch) =>
            {
                if (epoch == self.epoch && self.status != Status::EpochChange)
                    || !self.valid_state_message(slot, &state)
                {
                    return Vec::new();
                }
                self.epoch = epoch;
                let retained_suffix = state.slot > state.commit;
                self.adopt(state);
                self.activate_epoch(epoch);
                let mut out = if retained_suffix {
                    vec![Output::To(from, self.message(self.slot, Body::PrepareOk))]
                } else {
                    Vec::new()
                };
                debug_assert_eq!(retained_suffix, self.slot > self.commit_slot);
                out.extend(self.pending_execution());
                out
            }
            Body::Recovery { nonce } if self.status == Status::Normal => vec![Output::To(
                from,
                self.message(
                    self.slot,
                    Body::RecoveryResponse {
                        nonce,
                        state: self.is_leader().then(|| self.state()),
                    },
                ),
            )],
            Body::RecoveryResponse { nonce, state }
                if self.status == Status::Recovering && self.recovery_nonce == Some(nonce) =>
            {
                if state.is_some() != (from == self.leader_of(epoch)) {
                    return Vec::new();
                }
                if state
                    .as_ref()
                    .is_some_and(|state| !self.valid_state_message(slot, state))
                {
                    return Vec::new();
                }
                self.recovery.insert(from, (epoch, state));
                self.finish_recovery()
            }
            _ => Vec::new(),
        }
    }

    fn pending_execution(&mut self) -> Vec<Output> {
        if self.executing.is_some() || self.executed_slot >= self.commit_slot {
            return Vec::new();
        }
        let entry = self.log[self.executed_slot as usize].clone();
        self.executing = Some(entry.slot);
        vec![Output::Execute {
            slot: entry.slot,
            client_id: entry.client_id,
            request_num: entry.request_num,
            message_id: entry.message_id,
            execution_time: entry.execution_time,
            payload: entry.payload.clone(),
        }]
    }

    fn complete(&mut self, slot: Slot, result: Vec<u8>) -> Vec<Output> {
        if self.executing != Some(slot)
            || self.executed_slot.checked_add(1) != Some(slot)
            || slot > self.commit_slot
        {
            return Vec::new();
        }
        let entry = &self.log[(slot - 1) as usize];
        let mut out = Vec::new();
        if let Some(client) = self.clients.get_mut(&entry.client_id) {
            if client.request_num == entry.request_num && client.message_id == entry.message_id {
                client.result = Some(result.clone());
                if self.status == Status::Normal && self.is_leader() {
                    out.push(Output::Reply(result));
                }
            }
        }
        self.executed_slot = slot;
        self.executing = None;
        out.extend(self.pending_execution());
        out
    }

    fn enter_change(&mut self, epoch: Epoch) -> Vec<Output> {
        self.epoch = epoch;
        self.status = Status::EpochChange;
        self.start_changes.clear();
        self.do_changes.clear();
        self.prepare_oks.clear();
        self.sent_do_change = false;
        vec![Output::Broadcast(
            self.message(self.slot, Body::StartEpochChange),
        )]
    }

    fn qualify_change(&mut self) -> Vec<Output> {
        if self.sent_do_change || self.start_changes.len() < self.quorum() - 1 {
            return Vec::new();
        }
        self.sent_do_change = true;
        let message = self.message(
            self.slot,
            Body::DoEpochChange {
                latest_normal: self.latest_normal,
                state: self.state(),
            },
        );
        if self.is_leader() {
            self.do_changes
                .insert(self.node, (self.latest_normal, self.state()));
            self.finish_change()
        } else {
            vec![Output::To(self.leader_of(self.epoch), message)]
        }
    }

    fn finish_change(&mut self) -> Vec<Output> {
        if !self.sent_do_change
            || !self.do_changes.contains_key(&self.node)
            || self.do_changes.len() < self.quorum()
        {
            return Vec::new();
        }
        let best = self
            .do_changes
            .values()
            .max_by_key(|(normal, state)| (*normal, state.slot))
            .expect("quorum")
            .1
            .clone();
        let commit_slot = self
            .do_changes
            .values()
            .map(|(_, state)| state.commit)
            .max()
            .expect("quorum");
        if best.slot < commit_slot {
            return Vec::new();
        }
        let epoch = self.epoch;
        self.adopt(LogState {
            slot: best.slot,
            commit: commit_slot,
            log: best.log,
        });
        self.activate_epoch(epoch);
        let mut out = self.pending_execution();
        out.push(Output::Broadcast(self.message(
            self.slot,
            Body::StartEpoch {
                state: self.state(),
            },
        )));
        out
    }

    fn finish_recovery(&mut self) -> Vec<Output> {
        if self.recovery.len() < self.quorum() {
            return Vec::new();
        }
        let epoch = self
            .recovery
            .values()
            .map(|(epoch, _)| *epoch)
            .max()
            .expect("quorum");
        let leader = self.leader_of(epoch);
        let Some((state_epoch, Some(state))) = self.recovery.get(&leader).cloned() else {
            return Vec::new();
        };
        if state_epoch != epoch {
            return Vec::new();
        }
        if !self.valid_state_message(state.slot, &state) {
            return Vec::new();
        }
        self.epoch = epoch;
        self.adopt(state);
        self.activate_epoch(epoch);
        self.recovery_nonce = None;
        self.recovery.clear();
        self.pending_execution()
    }

    fn structurally_valid(state: &LogState) -> bool {
        if u64::try_from(state.log.len()).ok() != Some(state.slot) || state.commit > state.slot {
            return false;
        }
        let mut requests = BTreeMap::new();
        let mut messages = BTreeSet::new();
        state.log.iter().enumerate().all(|(index, entry)| {
            let slot = u64::try_from(index)
                .ok()
                .and_then(|index| index.checked_add(1));
            slot == Some(entry.slot)
                && messages.insert(entry.message_id)
                && requests
                    .insert(entry.client_id, entry.request_num)
                    .is_none_or(|previous| entry.request_num > previous)
        })
    }

    fn valid_state_message(&self, header_slot: Slot, state: &LogState) -> bool {
        header_slot == state.slot
            && state.commit >= self.executed_slot
            && Self::structurally_valid(state)
            && state.log.len() >= self.executed_slot as usize
            && state.log[..self.executed_slot as usize] == self.log[..self.executed_slot as usize]
    }

    fn adopt(&mut self, state: LogState) {
        let old_clients = std::mem::take(&mut self.clients);
        self.log = state.log;
        self.slot = state.slot;
        self.commit_slot = state.commit;
        self.executing = None;
        self.clients.clear();
        self.prepare_oks.clear();
        for entry in &self.log {
            self.clients.insert(
                entry.client_id,
                ClientEntry {
                    request_num: entry.request_num,
                    message_id: entry.message_id,
                    result: None,
                },
            );
        }
        for (client_id, old) in old_clients {
            if let Some(client) = self.clients.get_mut(&client_id) {
                if client.request_num == old.request_num && client.message_id == old.message_id {
                    client.result = old.result;
                }
            }
        }
    }

    fn activate_epoch(&mut self, epoch: Epoch) {
        self.status = Status::Normal;
        self.latest_normal = epoch;
        self.start_changes.clear();
        self.do_changes.clear();
        self.prepare_oks.clear();
        self.sent_do_change = false;
    }
}
