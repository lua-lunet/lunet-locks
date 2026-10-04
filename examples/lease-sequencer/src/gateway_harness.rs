//! The in-process committing cluster the gateway loop drives: three real
//! adapter `Node`s over scratch marker stores, the wire captured in
//! memory, and the executor's logical clock (`set_compliance_clock`,
//! never a wall reading) — the same harness shape the compliance
//! executor and the relay tests play. The gateway rides the last seat,
//! a backup, so every command actually crosses the loop's forward step
//! to the leader.
//!
//! The committed half of the loop comes off the journals: an operation
//! entry carries the identity its proposer gave it (the command's
//! `uuid`) and the payload as committed, and every seat holds the same
//! slot once it has learned the commit.

use crate::gateway::{ForwardOutcome, Forwarder};
use lunet_advisory_lock::Node;
use std::collections::{HashSet, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use vrr::ids::OperationId;
use vrr::journal::Payload;

/// The roster: the smallest cluster whose commits cross a real
/// quorum. The gateway's seat is the last member, a genesis backup.
const MEMBERS: usize = 3;
/// The primary timeout every node is opened with, in the logical
/// clock's own units. Every scenario's gateway knobs stay well inside
/// it, so no timer sweep turns into a view change mid-test.
const PRIMARY_TIMEOUT: u64 = 50;
/// The settle and drain bounds: a provisioned cluster converges and a
/// post-proposal cascade quiets long before these, so a loop that does
/// not quiet inside one is a defect.
const SETTLE_BOUND: usize = 1_000;

/// One member's packed identity: the first life of the `index + 1`-th
/// system, packed the core's own way (MSB system, LSB crash counter).
fn identity(index: usize) -> u32 {
    (((index + 1) as u32) << 16) | 1
}

/// One seat of the roster: the live node and its own packed identity.
struct Seat {
    id: u32,
    node: Node,
}

/// One committed operation entry, read off a journal: the slot it
/// occupies (the join's slot IS the session_id), the identity the
/// proposing host gave it (the command's `uuid`), and the payload as
/// committed at the node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Committed {
    pub slot: u64,
    pub uuid: [u8; 16],
    pub payload: Vec<u8>,
}

/// The in-memory abstract socket the tests assert on: the handle the
/// nexus holds is a [`MemHandle`]; the test's `MemSink` is a reader on
/// the same cell, so the written bytes and the drop of the held handle
/// are both observable from outside.
#[derive(Clone)]
pub struct MemSink {
    shared: Arc<SinkCell>,
}

struct SinkCell {
    written: Mutex<Vec<Vec<u8>>>,
    dropped: AtomicBool,
}

impl MemSink {
    /// A fresh socket cell, empty and un-released.
    pub fn new() -> MemSink {
        MemSink {
            shared: Arc::new(SinkCell {
                written: Mutex::new(Vec::new()),
                dropped: AtomicBool::new(false),
            }),
        }
    }

    /// The opaque handle the nexus takes ownership of. Dropping it is
    /// the release the sweeper and the shutdown owe (the drop flag
    /// lights; for a TCP handle the drop closes the socket).
    pub fn handle(&self) -> MemHandle {
        MemHandle {
            shared: Arc::clone(&self.shared),
        }
    }

    /// Every response the socket has received, in write order.
    pub fn bytes(&self) -> Vec<Vec<u8>> {
        self.shared.written.lock().unwrap().clone()
    }

    /// Whether the handle the nexus held has been released.
    pub fn dropped(&self) -> bool {
        self.shared.dropped.load(Ordering::SeqCst)
    }
}

impl Default for MemSink {
    fn default() -> Self {
        Self::new()
    }
}

/// The handle side of a [`MemSink`]: what the nexus holds as its
/// `Box<dyn AppSink>`.
pub struct MemHandle {
    shared: Arc<SinkCell>,
}

impl Drop for MemHandle {
    fn drop(&mut self) {
        self.shared.dropped.store(true, Ordering::SeqCst);
    }
}

impl crate::gateway::AppSink for MemHandle {
    fn write_response(&mut self, response: &[u8]) {
        self.shared.written.lock().unwrap().push(response.to_vec());
    }
}

/// A handle whose write dies — the crash-between-take-and-write shape:
/// the nexus has already taken the entry when the write unwinds.
pub struct CrashHandle {
    shared: Arc<SinkCell>,
}

impl Drop for CrashHandle {
    fn drop(&mut self) {
        self.shared.dropped.store(true, Ordering::SeqCst);
    }
}

impl crate::gateway::AppSink for CrashHandle {
    fn write_response(&mut self, _response: &[u8]) {
        panic!("the socket dies before the response is written");
    }
}

impl MemSink {
    /// The crashing variant of this socket's handle: same cell, so the
    /// test reads nothing written and the release flag on drop.
    pub fn crash_handle(&self) -> CrashHandle {
        CrashHandle {
            shared: Arc::clone(&self.shared),
        }
    }
}

/// A scenario's scratch directory under the repo's `.tmp`, named for
/// the scenario, so two scenarios never share a marker store and a
/// stale directory from an earlier run never boots over.
fn scratch(name: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../../.tmp/gateway-tests")
        .join(name);
    if dir.exists() {
        std::fs::remove_dir_all(&dir).expect("a stale scenario directory clears");
    }
    std::fs::create_dir_all(&dir).expect("the scenario directory creates");
    dir
}

/// The provisioned cluster: three real nodes, the wire in memory, the
/// logical clock the harness owns.
pub struct Cluster {
    seats: Vec<Seat>,
    queue: VecDeque<(u32, u32, Vec<u8>)>,
    clock: u64,
    /// The seat the gateway rides.
    gateway_seat: usize,
    /// The slots already reported as committed: each slot commits once
    /// cluster-wide, and the loop's committed half reports it once.
    reported: HashSet<u64>,
}

impl Cluster {
    /// Boot the roster over fresh marker stores and settle it: sweep
    /// the timer until every seat is normal and the wire is quiet.
    pub fn boot(name: &str) -> Cluster {
        let dir = scratch(name);
        let genesis = (0..MEMBERS)
            .map(|index| format!("{}:n{}", identity(index), index + 1))
            .collect::<Vec<_>>()
            .join("\0");
        let mut seats = Vec::with_capacity(MEMBERS);
        for index in 0..MEMBERS {
            let node = Node::open_compliance(
                &genesis,
                &format!("n{}", index + 1),
                &dir.join(format!("n{}.state", index + 1)).to_string_lossy(),
                PRIMARY_TIMEOUT,
            )
            .expect("the member opens over its own marker store");
            seats.push(Seat {
                id: identity(index),
                node,
            });
        }
        let mut cluster = Cluster {
            seats,
            queue: VecDeque::new(),
            clock: 0,
            gateway_seat: MEMBERS - 1,
            reported: HashSet::new(),
        };
        cluster.settle();
        cluster
    }

    /// The logical clock's reading — the driver's `now`.
    pub fn now(&self) -> u64 {
        self.clock
    }

    /// Pure logical time for the gateway's own deadlines: the clock
    /// moves, the nodes are not driven, the wire stays held. A command
    /// proposed but not yet exchanged stays uncommitted while its
    /// gateway deadline passes over it.
    pub fn elapse(&mut self, ticks: u64) {
        self.clock += ticks;
    }

    /// The timer sweep: the clock moves one tick, every seat takes it,
    /// and the wire drains to quiet.
    pub fn advance(&mut self, ticks: u64) {
        for _ in 0..ticks {
            self.clock += 1;
            for index in 0..self.seats.len() {
                self.arm(index);
                let _ = self.seats[index].node.idle();
            }
            self.drain();
        }
    }

    /// Arms one seat with the harness's clock: every drive carries the
    /// logical tick, never a wall reading.
    fn arm(&mut self, index: usize) {
        let at = self.clock;
        self.seats[index].node.set_compliance_clock(at);
    }

    /// Hand one datagram to its addressee, attributed to its sender.
    fn deliver(&mut self, from: u32, to: u32, bytes: &[u8]) {
        let index = self
            .seats
            .iter()
            .position(|seat| seat.id == to)
            .expect("every addressee is a seat of this roster");
        self.arm(index);
        let _ = self.seats[index].node.receive(from, bytes);
    }

    /// Move every released datagram to its addressee and collect what
    /// that releases, until the wire is quiet. The clock does not move.
    fn drain(&mut self) {
        for _ in 0..SETTLE_BOUND {
            let mut queue: Vec<(u32, u32, Vec<u8>)> = Vec::new();
            for index in 0..self.seats.len() {
                let from = self.seats[index].id;
                while let Some(out) = self.seats[index].node.next_output() {
                    // Kind 2 is a client reply: under the compliance
                    // rules the application boundary is opaque, so no
                    // reply is ever queued. A send rides the wire.
                    assert_eq!(out.kind, lunet_advisory_lock::OUTPUT_SEND);
                    queue.push((from, out.to, out.bytes));
                }
            }
            if queue.is_empty() {
                return;
            }
            for (from, to, bytes) in queue {
                self.deliver(from, to, &bytes);
            }
        }
        panic!("the wire did not quiet inside {SETTLE_BOUND} deliveries");
    }

    /// Every seat's replication state is normal and the wire is quiet.
    fn quiet_and_normal(&self) -> bool {
        self.queue.is_empty()
            && self.seats.iter().all(|seat| {
                seat.node
                    .status()
                    .state_name()
                    .eq_ignore_ascii_case("normal")
            })
    }

    /// Settle the cluster after boot: sweep the timer until every seat
    /// is normal and the wire is quiet.
    fn settle(&mut self) {
        for _ in 0..SETTLE_BOUND {
            self.advance(1);
            if self.quiet_and_normal() {
                return;
            }
        }
        panic!(
            "the cluster did not settle inside {SETTLE_BOUND} ticks: {:?}",
            self.seats
                .iter()
                .map(|seat| seat.node.status().state_name())
                .collect::<Vec<_>>()
        );
    }

    /// The seat the cluster's current view names as its primary.
    fn leader(&self) -> usize {
        let status = self.seats[self.gateway_seat].node.status();
        self.seats
            .iter()
            .position(|seat| seat.id == status.leader)
            .expect("the leader is a seat of this roster")
    }

    /// Propose one operation at a seat, armed with the harness's clock,
    /// and move what it releases into the wire.
    fn propose_at(&mut self, index: usize, id: OperationId, payload: &[u8]) -> i32 {
        self.arm(index);
        let rc = self.seats[index].node.propose_opaque(id, payload);
        for _ in 0..SETTLE_BOUND {
            let Some(out) = self.seats[index].node.next_output() else {
                break;
            };
            assert_eq!(out.kind, lunet_advisory_lock::OUTPUT_SEND);
            self.queue
                .push_back((self.seats[index].id, out.to, out.bytes));
        }
        rc
    }

    /// Every new committed operation entry, across every seat's
    /// journal, in slot order — each slot reported exactly once. This
    /// is the committed half of the loop's input: the uuid of the
    /// command and the command result as committed at the node.
    pub fn committed_since(&mut self) -> Vec<Committed> {
        let mut fresh: Vec<Committed> = Vec::new();
        for seat in &self.seats {
            for entry in seat.node.journal_entries() {
                let Payload::Operation { id, payload } = entry.payload else {
                    continue;
                };
                if !self.reported.insert(entry.slot.0) {
                    continue;
                }
                fresh.push(Committed {
                    slot: entry.slot.0,
                    uuid: operation_id_bytes(id),
                    payload: payload.to_vec(),
                });
            }
        }
        fresh.sort_by_key(|committed| committed.slot);
        fresh
    }

    /// One committed slot's entry as a seat holds it (the audit read
    /// back the obligation 7 proof runs over).
    pub fn committed_entry(&self, seat: usize, slot: u64) -> Committed {
        let entry = self.seats[seat]
            .node
            .journal_entries()
            .into_iter()
            .find(|entry| entry.slot.0 == slot)
            .expect("the slot is inside the journal");
        let Payload::Operation { id, payload } = entry.payload else {
            panic!("slot {slot} is not an operation entry");
        };
        Committed {
            slot: entry.slot.0,
            uuid: operation_id_bytes(id),
            payload: payload.to_vec(),
        }
    }
}

impl Forwarder for Cluster {
    /// The loop's forward step: hand the command to the replication
    /// protocol. A refusal names itself; the gateway rolls the command
    /// back by name. The ordinary shape re-drives at the leader when
    /// the gateway's own seat is a backup — that re-drive is the
    /// forward to the leader.
    fn forward(&mut self, uuid: [u8; 16], payload: &[u8]) -> ForwardOutcome {
        let id = operation_id(uuid);
        let mut rc = self.propose_at(self.gateway_seat, id, payload);
        if rc == lunet_advisory_lock::NOT_LEADER {
            let leader = self.leader();
            rc = self.propose_at(leader, id, payload);
        }
        if rc == lunet_advisory_lock::OK {
            ForwardOutcome::Accepted
        } else {
            ForwardOutcome::Refused("forward_refused")
        }
    }
}

/// `OperationId` from the command's 16-byte uuid: first 8 bytes are
/// `msb`, last 8 `lsb`, both big-endian — the core's wire order, the
/// same channel the entry identity crosses the journal by.
fn operation_id(uuid: [u8; 16]) -> OperationId {
    OperationId {
        msb: u64::from_be_bytes(uuid[..8].try_into().expect("8 bytes")),
        lsb: u64::from_be_bytes(uuid[8..].try_into().expect("8 bytes")),
    }
}

/// The 16-byte uuid an entry identity carries, back out of the core's
/// `OperationId` halves.
fn operation_id_bytes(id: OperationId) -> [u8; 16] {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&id.msb.to_be_bytes());
    bytes[8..].copy_from_slice(&id.lsb.to_be_bytes());
    bytes
}
