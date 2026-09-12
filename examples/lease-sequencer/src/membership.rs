//! The cluster membership snapshot affordances (`src/snapshot.tl`, mirrored
//! byte for byte): the era-qualified request/response payloads, the era+slot
//! total comparison, the weighted quorum over agreeing snapshots, the
//! leader-side application of committed admin verbs, and the membership
//! sidecar — the lazy write-behind copy of the adopted facts next to the
//! incarnation marker.
//!
//! Snapshots are ADVISORY evidence, never a consensus mechanism: safety
//! stays with the replicated configuration commands, a discovered
//! configuration is adopted in memory and feeds the existing fenced-boot
//! path, and nothing here bypasses a gate.

use std::collections::HashSet;
use std::fs;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{SyncSender, sync_channel};

/// The request payload: one tag byte. The requester needs no state of its
/// own; every responder answers from its current membership model.
pub const SNAPSHOT_REQUEST: u8 = 1;
/// The response payload tag: era + slot + members.
pub const SNAPSHOT_RESPONSE: u8 = 2;
/// The core bounds a configuration's membership at 16; a snapshot carries
/// at most that.
const MAX_MEMBERS: usize = 16;
/// The bounded write-behind queue's capacity: producers never block, a
/// full queue drops the fact and counts it.
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

/// An endpoint is literal `IPv4:port` — the same rule `config.tl` enforces
/// for every addressing row.
/// An endpoint is an IPv4 `dotted.dotted.dotted:port` or a bracketed IPv6
/// `[addr]:port` literal — the bracket form is how the rig's IPv6-only
/// benchmark nodes appear in descriptors and snapshots.
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

/// The response is strictly shaped and small: era (u32), the choosing slot
/// (u64), the member count (u16), then per member its id (u32), weight
/// (u32), and length-delimited endpoint — members in canonical strictly
/// ascending id order, one shape per membership.
pub fn encode_response(snapshot: &Snapshot) -> Vec<u8> {
    let _ = u32::MAX; // era is a u32 by construction; the assert was vacuous
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

/// The up-to-dateness comparison: era plus slot, lexicographic — every
/// configuration knows the slot at which it was chosen (the genesis
/// configuration is slot 0), so the pair is total. True when the first
/// names a configuration the second does not cover yet; an equal or older
/// snapshot is a no-op everywhere.
pub fn newer(era: u32, slot: u64, own_era: u32, own_slot: u64) -> bool {
    era > own_era || (era == own_era && slot > own_slot)
}

/// The responder's weight inside a snapshot's membership, or `None` when
/// the snapshot does not name it (inconsistent evidence; the response is
/// dropped).
pub fn member_weight(members: &[SnapshotMember], id: u32) -> Option<u64> {
    members
        .iter()
        .find(|member| member.id == id)
        .map(|member| u64::from(member.weight))
}

/// The weighted majority the deployment's configurations vote with
/// (`WeightedMajority`): the threshold is half the total weight plus one,
/// the sum runs over the agreeing responders, and a weight-0 learner's
/// agreement is collected but contributes nothing — learners never satisfy
/// a quorum.
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

/// The boot membership model the descriptor gives a node: era 1 (the
/// founding configuration the core folds, chosen at slot 0), the
/// descriptor's GENESIS lines in line order at weight 1. A post-genesis
/// descriptor line is an addressing row, not a membership fact: the node
/// can talk to the member, and snapshots and dissemination teach whether
/// it is still in the live configuration and at what weight. `nodes`
/// carries `(id, host, port, genesis)` per line.
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

/// The membership model this process carries: era 0 over the descriptor
/// (or over the parsed sidecar), advancing only forward — a newer (era,
/// slot) is adopted in memory, an equal or older one is a no-op.
pub struct Model {
    pub era: u32,
    pub slot: u64,
    pub members: Vec<SnapshotMember>,
}

impl Model {
    /// Adopt a newer (era, slot) snapshot wholesale. The caller adds the
    /// addressing rows the adopted membership names.
    pub fn adopt(&mut self, snapshot: Snapshot) -> bool {
        if !newer(snapshot.era, snapshot.slot, self.era, self.slot) {
            return false;
        }
        self.era = snapshot.era;
        self.slot = snapshot.slot;
        self.members = snapshot.members;
        true
    }

    /// The leader-side application of one committed admin verb: a join
    /// appends the member at weight 0 (additive and idempotent), an
    /// increment promotes it to a voter, a decrement returns it to weight
    /// 0, a leave removes it. A refused change touches nothing.
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

    /// The next generation after this node's own committed verb: the
    /// model's counter advances exactly once per reconfiguration commit it
    /// drives, at the establishing operation's choosing slot.
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

// ---------------------------------------------------------------------------
// The membership sidecar
// ---------------------------------------------------------------------------

/// The sidecar's on-disk series, next to the incarnation marker (whose bump
/// semantics are untouched): line 1 is the header
/// `{"format":"membership-sidecar/v1","era":N,"slot":N}`, then one line per
/// member `{"id":N,"weight":N,"endpoint":"host:port"}` in canonical
/// ascending-id order. Written atomically (temporary file, rename) with no
/// fsync — the same documented loss window the AOF writer carries, and
/// nothing depends on the file for safety. A sidecar that does not parse is
/// ignored entirely at boot: the model falls back to the descriptor and
/// discovery re-learns.
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

/// A handle to the sidecar writer thread. Producers enqueue complete
/// membership facts and never wait on disk: the queue is bounded, and an
/// enqueue onto a full queue drops the fact and increments the drop
/// counter — the same drop-on-overflow contract the AOF writer follows.
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
                // The writer thread drains its bounded queue and rewrites
                // the sidecar atomically per accepted fact: temporary
                // file, rename, no fsync — the documented loss window.
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

    /// Enqueue one membership fact. Never blocks, never fails: a full
    /// queue drops the fact and increments the drop counter.
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

/// The boot-time read of the sidecar: strict, and a sidecar that does not
/// parse falls back to the descriptor.
pub fn load_sidecar(state: &str) -> Option<Snapshot> {
    let text = fs::read_to_string(sidecar_path(state)).ok()?;
    decode_sidecar(&text)
}

/// The discovery tally for one agreement key: the agreeing responders'
/// ids and their summed weight.
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

    /// Records one responder's agreement: an unattributed weight (the
    /// snapshot does not name the responder) and a repeat response are
    /// dropped.
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

/// The agreement key over a snapshot: era, choosing slot, and the member
/// set. Snapshots that agree share the key.
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    fn member(id: u32, weight: u32, endpoint: &str) -> SnapshotMember {
        SnapshotMember {
            id,
            weight,
            endpoint: endpoint.to_string(),
        }
    }

    fn three() -> Vec<SnapshotMember> {
        vec![
            member(101, 1, "127.0.0.1:27101"),
            member(202, 1, "127.0.0.1:27102"),
            member(303, 1, "127.0.0.1:27103"),
        ]
    }

    #[test]
    fn request_is_one_tag_byte() {
        assert_eq!(encode_request(), vec![SNAPSHOT_REQUEST]);
        assert!(decode_request(&encode_request()));
        assert!(!decode_request(&[]));
        assert!(!decode_request(&[SNAPSHOT_REQUEST, 0]));
    }

    #[test]
    fn response_round_trips() {
        let snapshot = Snapshot {
            era: 5,
            slot: 42,
            members: three(),
        };
        let decoded = decode_response(&encode_response(&snapshot)).expect("decode");
        assert_eq!(decoded, snapshot);
    }

    #[test]
    fn response_decode_refuses_malformed_shapes() {
        let good = encode_response(&Snapshot {
            era: 1,
            slot: 2,
            members: three(),
        });
        assert!(decode_response(&good).is_some());
        assert!(decode_response(&[]).is_none(), "empty");
        assert!(decode_response(&good[..4]).is_none(), "truncated header");
        let mut mistagged = good.clone();
        mistagged[0] = 4;
        assert!(decode_response(&mistagged).is_none(), "wrong tag");
        let mut overcount = good.clone();
        overcount[13] = 17;
        assert!(decode_response(&overcount).is_none(), "over member cap");
        assert!(
            decode_response(&good[..good.len() - 1]).is_none(),
            "short tail"
        );
        let mut trailing = good.clone();
        trailing.push(0);
        assert!(decode_response(&trailing).is_none(), "trailing byte");
        // The first member's endpoint-length field is bytes 23..25; a
        // zeroed length decodes to an empty endpoint (not IPv4:port) and
        // an inflated one overruns the payload.
        let mut empty_endpoint = good.clone();
        empty_endpoint[24] = 0;
        assert!(decode_response(&empty_endpoint).is_none(), "empty endpoint");
        let mut overrun = good.clone();
        overrun[24] = 200;
        assert!(
            decode_response(&overrun).is_none(),
            "overrun endpoint length"
        );
        // A descending id pair (byte-level edit: the id 101's low byte)
        let mut descending = good.clone();
        descending[18] = 202;
        assert!(decode_response(&descending).is_none(), "descending ids");
    }

    #[test]
    fn comparison_is_total_era_then_slot() {
        assert!(newer(2, 0, 1, 9), "era dominates");
        assert!(newer(1, 9, 1, 3), "slot breaks ties");
        assert!(!newer(3, 7, 3, 7), "equal is a no-op");
        assert!(!newer(1, 9, 3, 0), "older is ignored");
    }

    #[test]
    fn quorum_is_the_weighted_majority_learners_never_satisfy() {
        let members = vec![
            member(1, 1, "127.0.0.1:7001"),
            member(2, 1, "127.0.0.1:7002"),
            member(3, 1, "127.0.0.1:7003"),
            member(4, 1, "127.0.0.1:7004"),
            member(6, 0, "127.0.0.1:7006"),
        ];
        let mut two = HashSet::new();
        two.insert(1);
        two.insert(2);
        let mut three_voters = two.clone();
        three_voters.insert(3);
        let mut learner = HashSet::new();
        learner.insert(6);
        assert!(!quorum_reached(&members, &two));
        assert!(quorum_reached(&members, &three_voters));
        assert!(!quorum_reached(&members, &learner));
    }

    #[test]
    fn apply_change_moves_the_model_exactly() {
        let mut model = Model {
            era: 0,
            slot: 0,
            members: three(),
        };
        assert!(model.apply_change("join", 404, "127.0.0.1:27104"));
        assert_eq!(model.members.len(), 4);
        assert_eq!(member_weight(&model.members, 404), Some(0));
        assert!(!model.apply_change("join", 404, "127.0.0.1:9"), "dup join");
        assert!(model.apply_change("increment", 404, ""));
        assert_eq!(member_weight(&model.members, 404), Some(1));
        assert!(model.apply_change("decrement", 404, ""));
        assert_eq!(member_weight(&model.members, 404), Some(0));
        assert!(model.apply_change("leave", 404, ""));
        assert_eq!(member_weight(&model.members, 404), None);
        assert!(!model.apply_change("leave", 404, ""), "absent leave");
        assert!(!model.apply_change("resign", 404, ""), "unknown action");
        assert!(model.members.windows(2).all(|pair| pair[0].id < pair[1].id));
    }

    #[test]
    fn sidecar_round_trips_and_refuses_malformed_series() {
        let snapshot = Snapshot {
            era: 5,
            slot: 42,
            members: three(),
        };
        let text = encode_sidecar(&snapshot);
        assert_eq!(text.lines().count(), 4, "header plus three members");
        let decoded = decode_sidecar(&text).expect("parse");
        assert_eq!(decoded, snapshot);
        assert!(decode_sidecar("").is_none());
        assert!(decode_sidecar("{\"format\":\"other/v1\",\"era\":1,\"slot\":0}\n").is_none());
        assert!(
            decode_sidecar(
                "{\"format\":\"membership-sidecar/v1\",\"era\":5,\"slot\":42}\n\
                 {\"id\":41,\"weight\":1,\"endpoint\":\"127.0.0.1:1\"}\n\
                 {\"id\":40,\"weight\":1,\"endpoint\":\"127.0.0.1:2\"}\n"
            )
            .is_none(),
            "descending ids"
        );
    }

    #[test]
    fn writer_enqueue_drops_on_overflow_never_blocks() {
        let dir = std::env::temp_dir().join(format!(
            "membership-sidecar-test-{}-{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        fs::create_dir_all(&dir).unwrap();
        let state = dir.join("node.state");
        let writer = SidecarWriter::open(state.to_str().unwrap()).unwrap();
        let snapshot = Snapshot {
            era: 9,
            slot: 3,
            members: three(),
        };
        for _ in 0..QUEUE_CAP * 4 {
            writer.enqueue(&snapshot);
        }
        assert!(writer.drops() > 0, "overflow must drop, never block");
        // The accepted facts eventually land in the sidecar.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let parsed = fs::read_to_string(sidecar_path(state.to_str().unwrap()))
                .ok()
                .and_then(|text| decode_sidecar(&text));
            if parsed.as_ref().is_some_and(|snap| snap.era == 9) {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "sidecar never landed");
            std::thread::sleep(Duration::from_millis(20));
        }
        drop(writer);
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn tally_records_one_responder_once() {
        let mut tally = Tally::new();
        tally.record(101, member_weight(&three(), 101));
        tally.record(101, member_weight(&three(), 101));
        assert_eq!(tally.weight, 1, "a repeated response adds nothing");
        tally.record(999, None);
        assert_eq!(tally.ids.len(), 1, "unattributed weight is dropped");
    }
}
