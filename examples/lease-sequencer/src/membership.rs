use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};

pub const SNAPSHOT_REQUEST: u8 = 1;
pub const SNAPSHOT_RESPONSE: u8 = 2;
const MAX_MEMBERS: usize = 16;
const QUEUE_CAP: usize = 64;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SnapshotMember {
    pub id: u32,
    pub weight: u32,
    pub endpoint: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Snapshot {
    pub era: u32,
    pub slot: u64,
    pub members: Vec<SnapshotMember>,
}

fn valid_endpoint(endpoint: &str) -> bool {
    let Some((host, port)) = endpoint.rsplit_once(':') else {
        return false;
    };
    let Ok(port) = port.parse::<u16>() else {
        return false;
    };
    if port == 0 {
        return false;
    }
    if let Some(v6) = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')) {
        return !v6.is_empty() && v6.split(':').count() >= 3;
    }
    let octets: Vec<&str> = host.split('.').collect();
    octets.len() == 4
        && octets.iter().all(|octet| {
            let value = octet.parse::<u16>().unwrap_or(u16::MAX);
            !octet.is_empty() && octet.len() <= 3 && value <= 255
        })
}

pub fn encode_request() -> Vec<u8> {
    vec![SNAPSHOT_REQUEST]
}

pub fn decode_request(payload: &[u8]) -> bool {
    payload == [SNAPSHOT_REQUEST]
}

pub fn encode_response(snapshot: &Snapshot) -> Vec<u8> {
    assert!(
        !snapshot.members.is_empty() && snapshot.members.len() <= MAX_MEMBERS,
        "snapshot: membership must carry 1..=16 members"
    );
    assert!(
        snapshot
            .members
            .windows(2)
            .all(|pair| pair[0].id < pair[1].id),
        "snapshot: members must ascend by id"
    );
    let mut out = Vec::with_capacity(15 + 10 * snapshot.members.len());
    out.push(SNAPSHOT_RESPONSE);
    out.extend_from_slice(&snapshot.era.to_be_bytes());
    out.extend_from_slice(&snapshot.slot.to_be_bytes());
    out.extend_from_slice(&(snapshot.members.len() as u16).to_be_bytes());
    for member in &snapshot.members {
        assert!(valid_endpoint(&member.endpoint), "snapshot: bad endpoint");
        out.extend_from_slice(&member.id.to_be_bytes());
        out.extend_from_slice(&member.weight.to_be_bytes());
        let endpoint = member.endpoint.as_bytes();
        out.extend_from_slice(&(endpoint.len() as u16).to_be_bytes());
        out.extend_from_slice(endpoint);
    }
    out
}

pub fn decode_response(payload: &[u8]) -> Option<Snapshot> {
    if payload.len() < 15 || payload[0] != SNAPSHOT_RESPONSE {
        return None;
    }
    let era = u32::from_be_bytes(payload[1..5].try_into().ok()?);
    let slot = u64::from_be_bytes(payload[5..13].try_into().ok()?);
    let count = u16::from_be_bytes(payload[13..15].try_into().ok()?) as usize;
    if !(1..=MAX_MEMBERS).contains(&count) {
        return None;
    }
    let mut members = Vec::with_capacity(count);
    let mut cursor = 15usize;
    let mut previous: u32 = 0;
    for index in 0..count {
        if payload.len() < cursor + 10 {
            return None;
        }
        let id = u32::from_be_bytes(payload[cursor..cursor + 4].try_into().ok()?);
        let weight = u32::from_be_bytes(payload[cursor + 4..cursor + 8].try_into().ok()?);
        let endpoint_len =
            u16::from_be_bytes(payload[cursor + 8..cursor + 10].try_into().ok()?) as usize;
        if payload.len() < cursor + 10 + endpoint_len {
            return None;
        }
        let Ok(endpoint) = std::str::from_utf8(&payload[cursor + 10..cursor + 10 + endpoint_len])
        else {
            return None;
        };
        cursor += 10 + endpoint_len;
        if index > 0 && id <= previous {
            return None;
        }
        if !valid_endpoint(endpoint) {
            return None;
        }
        previous = id;
        members.push(SnapshotMember {
            id,
            weight,
            endpoint: endpoint.to_string(),
        });
    }
    if cursor != payload.len() {
        return None;
    }
    Some(Snapshot { era, slot, members })
}

pub fn newer(era: u32, slot: u64, own_era: u32, own_slot: u64) -> bool {
    era > own_era || (era == own_era && slot > own_slot)
}

pub fn member_weight(members: &[SnapshotMember], id: u32) -> Option<u64> {
    members
        .iter()
        .find(|member| member.id == id)
        .map(|member| u64::from(member.weight))
}

pub fn quorum_reached(members: &[SnapshotMember], responded: &HashSet<u32>) -> bool {
    let total: u64 = members.iter().map(|member| u64::from(member.weight)).sum();
    let threshold = total / 2 + 1;
    let sum: u64 = members
        .iter()
        .filter(|member| responded.contains(&member.id))
        .map(|member| u64::from(member.weight))
        .sum();
    sum >= threshold
}

pub fn descriptor_model(nodes: &[(u32, String, u16, bool)]) -> Vec<SnapshotMember> {
    let mut members: Vec<SnapshotMember> = nodes
        .iter()
        .filter(|(.., genesis)| *genesis)
        .map(|(id, host, port, _)| SnapshotMember {
            id: *id,
            weight: 1,
            endpoint: format!("{host}:{port}"),
        })
        .collect();
    members.sort_by_key(|member| member.id);
    members
}

pub struct Model {
    pub era: u32,
    pub slot: u64,
    pub members: Vec<SnapshotMember>,
}

impl Model {
    pub fn adopt(&mut self, snapshot: Snapshot) -> bool {
        if !newer(snapshot.era, snapshot.slot, self.era, self.slot) {
            return false;
        }
        self.era = snapshot.era;
        self.slot = snapshot.slot;
        self.members = snapshot.members;
        true
    }

    pub fn apply_change(&mut self, action: &str, id: u32, endpoint: &str) -> bool {
        let exists = self.members.iter().any(|member| member.id == id);
        let mut updated: Vec<SnapshotMember> = self
            .members
            .iter()
            .filter(|member| member.id != id)
            .cloned()
            .collect();
        match action {
            "join" => {
                if exists || !valid_endpoint(endpoint) {
                    return false;
                }
                updated.push(SnapshotMember {
                    id,
                    weight: 0,
                    endpoint: endpoint.to_string(),
                });
            }
            "increment" | "decrement" => {
                let Some(existing) = self.members.iter().find(|m| m.id == id) else {
                    return false;
                };
                updated.push(SnapshotMember {
                    id,
                    weight: if action == "increment" { 1 } else { 0 },
                    endpoint: existing.endpoint.clone(),
                });
            }
            "leave" => {
                if !exists {
                    return false;
                }
            }
            _ => return false,
        }
        updated.sort_by_key(|member| member.id);
        self.members = updated;
        true
    }

    pub fn advance(&mut self, slot: u64) {
        self.era += 1;
        self.slot = slot;
    }

    pub fn snapshot(&self) -> Snapshot {
        Snapshot {
            era: self.era,
            slot: self.slot,
            members: self.members.clone(),
        }
    }
}

pub fn sidecar_path(state: &str) -> PathBuf {
    PathBuf::from(format!("{state}.membership"))
}

pub fn encode_sidecar(snapshot: &Snapshot) -> String {
    let mut text = format!(
        "{{\"format\":\"membership-sidecar/v1\",\"era\":{},\"slot\":{}}}\n",
        snapshot.era, snapshot.slot
    );
    for member in &snapshot.members {
        text.push_str(&format!(
            "{{\"id\":{},\"weight\":{},\"endpoint\":\"{}\"}}\n",
            member.id, member.weight, member.endpoint
        ));
    }
    text
}

pub fn decode_sidecar(text: &str) -> Option<Snapshot> {
    let mut lines = text.lines().map(str::trim).filter(|line| !line.is_empty());
    let header = lines.next()?;
    if !header.contains("\"format\":\"membership-sidecar/v1\"") {
        return None;
    }
    let era: u32 = header
        .split("\"era\":")
        .nth(1)?
        .split(',')
        .next()?
        .parse()
        .ok()?;
    let slot: u64 = header
        .split("\"slot\":")
        .nth(1)?
        .split('}')
        .next()?
        .parse()
        .ok()?;
    let mut members: Vec<SnapshotMember> = Vec::new();
    for line in lines {
        let id: u32 = line
            .split("\"id\":")
            .nth(1)?
            .split(',')
            .next()?
            .parse()
            .ok()?;
        let weight: u32 = line
            .split("\"weight\":")
            .nth(1)?
            .split(',')
            .next()?
            .parse()
            .ok()?;
        let endpoint = line
            .split("\"endpoint\":\"")
            .nth(1)?
            .trim_end_matches("\"}");
        if !valid_endpoint(endpoint) {
            return None;
        }
        if let Some(previous) = members.last()
            && id <= previous.id
        {
            return None;
        }
        members.push(SnapshotMember {
            id,
            weight,
            endpoint: endpoint.to_string(),
        });
    }
    if members.is_empty() || members.len() > MAX_MEMBERS {
        return None;
    }
    Some(Snapshot { era, slot, members })
}

pub struct SidecarWriter {
    tx: SyncSender<Snapshot>,
    drops: Arc<std::sync::atomic::AtomicU64>,
}

impl SidecarWriter {
    pub fn open(state: &str) -> std::io::Result<Self> {
        let path = sidecar_path(state);
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let (tx, rx) = sync_channel::<Snapshot>(QUEUE_CAP);
        let drops = Arc::new(AtomicU64::new(0));
        let thread_drops = Arc::clone(&drops);
        std::thread::Builder::new()
            .name("membership-sidecar".to_string())
            .spawn(move || {
                while let Ok(snapshot) = rx.recv() {
                    let text = encode_sidecar(&snapshot);
                    let temporary = path.with_extension("membership.new");
                    if let Err(error) = fs::File::create(&temporary)
                        .and_then(|mut file| file.write_all(text.as_bytes()))
                        .and_then(|()| fs::rename(&temporary, &path))
                    {
                        eprintln!(
                            "lease-sequencer: membership sidecar write failed \
                             ({error}); write-behind disabled for this process"
                        );
                        thread_drops.fetch_add(1, Ordering::Relaxed);
                        return;
                    }
                }
            })
            .map_err(std::io::Error::other)?;
        Ok(Self { tx, drops })
    }

    pub fn enqueue(&self, snapshot: &Snapshot) {
        match self.tx.try_send(snapshot.clone()) {
            Ok(()) => {}
            Err(_) => {
                self.drops.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    pub fn drops(&self) -> u64 {
        self.drops.load(Ordering::Relaxed)
    }
}

pub fn load_sidecar(state: &str) -> Option<Snapshot> {
    let text = fs::read_to_string(sidecar_path(state)).ok()?;
    decode_sidecar(&text)
}

pub struct Tally {
    pub ids: HashSet<u32>,
    pub weight: u64,
}

impl Tally {
    pub fn new() -> Self {
        Self {
            ids: HashSet::new(),
            weight: 0,
        }
    }

    pub fn record(&mut self, id: u32, weight: Option<u64>) {
        if weight.is_none() || self.ids.contains(&id) {
            return;
        }
        self.ids.insert(id);
        self.weight += weight.unwrap_or(0);
    }

    pub fn agrees_with(&self, members: &[SnapshotMember]) -> bool {
        quorum_reached(members, &self.ids)
    }
}

impl Default for Tally {
    fn default() -> Self {
        Self::new()
    }
}

pub fn agreement_key(snapshot: &Snapshot) -> String {
    let mut key = format!("{}:{}", snapshot.era, snapshot.slot);
    for member in &snapshot.members {
        key.push_str(&format!(
            "|{}:{}:{}",
            member.id, member.weight, member.endpoint
        ));
    }
    key
}
