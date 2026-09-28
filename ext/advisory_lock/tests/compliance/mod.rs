//! The compliance suite's executor over OUR host
//! (`ext/uvrr-core/docs/uvrr-host-compliance.md`): the abstract host
//! interface driven through the adapter's [`Node`] over the real marker
//! store. A case is a setup script, one input, and the expected
//! post-state; the executor replays the script, applies the input,
//! drains the network, and captures the delivery sequence and the post
//! records. The corpus JSON is read from the vendored submodule at its
//! pinned tag — it is upstream data, never copied here.
//!
//! Nodes are addressed by the explicit pair `system:counter`
//! (`docs/uvrr-io-obligations.md`, the boot-gate chapter §5), one-indexed
//! in both halves: the roster members of a provisioned cluster are `1:1`
//! through `nodes:1`, a bumped life is `system:2` and onward, and the
//! gossip sender is `nodes+1:1`. The pair's halves are the packed
//! identity's — the adapter's own peer address space.

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use serde::Deserialize;

use lunet_advisory_lock::Node;
use vrr::configuration::SystemOperation;
use vrr::ids::{CrashCounter, NodeId, OperationId, SystemId, View};
use vrr::journal::{LogEntry, Payload};
use vrr::progress::Status;

/// The settle bound: the bootstrap converges long before this, and a
/// script that cannot settle inside it is a corpus defect, not a slow
/// machine, so the executor errors rather than spinning.
const SETTLE_BOUND: usize = 1_000;

/// The drain bound: the post-input cascade is finite and deterministic.
const DRAIN_BOUND: usize = 1_000;

/// The vector operations' fixed identity half: part of the fixture
/// (`docs/uvrr-host-compliance.md` §4), never zero, never privileged.
const OP_MSB: u64 = 0x7665_6374;

/// The marker-store root for every case's node state: inside the repo,
/// under the scratch tree.
pub fn compliance_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/compliance")
}

/// Where the vendored corpus lives (the submodule at its pinned tag).
pub fn corpus_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../uvrr-core/tests/compliance/corpus")
}

// ----------------------------------------------------------------------
// The case record (the corpus's schema, mirrored as data)
// ----------------------------------------------------------------------

/// One typed cluster operation, the `reconfigure` operation's argument,
/// in the corpus's abstract operation names (`docs/uvrr-host-compliance.md`
/// §3).
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SystemOp {
    Increment { node: String },
    Decrement { node: String },
    Double,
    Halve,
    Join { node: String, position: u32 },
    Leave { node: String },
    Nominate { from: u32, offset: u32 },
    Batch { ops: Vec<SystemOp> },
}

/// One abstract host operation, the sans-I/O boundary mirrored as data.
#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum Op {
    Provision {
        nodes: usize,
        timeout: u64,
    },
    Settle,
    Propose {
        node: String,
        payload: String,
    },
    Reconfigure {
        node: String,
        system: SystemOp,
    },
    Tick {
        node: String,
    },
    TickAll,
    DeliverAll,
    Crash {
        node: String,
    },
    Restart {
        node: String,
        kind: String,
    },
    Halt {
        node: String,
    },
    Boot {
        node: String,
    },
    Announce {
        node: String,
        old: String,
    },
    Deliver {
        to: String,
        from: String,
        wire: String,
    },
    Gossip {
        wire: String,
    },
}

/// One expected delivery in the post-input drain.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ExpectedDelivery {
    pub from: String,
    pub to: String,
    pub wire: String,
}

/// One node's post-state. Absent fields are unconstrained.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct PostNode {
    pub node: String,
    pub status: Option<String>,
    pub era: Option<u32>,
    pub view: Option<u32>,
    pub accepted: Option<u64>,
    pub committed: Option<u64>,
    pub applied: Option<u64>,
    pub journal: Option<Vec<String>>,
    pub members: Option<Vec<String>>,
    pub weights: Option<Vec<u64>>,
    pub markers: Option<Vec<String>>,
    pub witnesses: Option<Vec<String>>,
}

/// The expectation.
#[derive(Clone, Debug, Default, PartialEq, Eq, Deserialize)]
pub struct Expect {
    #[serde(default)]
    pub deliveries: Vec<ExpectedDelivery>,
    #[serde(default)]
    pub post: Vec<PostNode>,
}

/// One compliance case.
#[derive(Clone, Debug, Deserialize)]
pub struct Case {
    pub id: String,
    pub family: String,
    pub clause: String,
    pub setup: Vec<Op>,
    pub input: Op,
    pub expect: Expect,
}

// ----------------------------------------------------------------------
// Identities, hex, and wire
// ----------------------------------------------------------------------

/// The identity half's parse failure: the pair is one-indexed in both
/// halves, so a zero half is no identity (`docs/uvrr-io-obligations.md`,
/// the boot-gate chapter §5).
fn half(text: &str) -> Result<u16, String> {
    text.parse::<u16>()
        .map_err(|e| format!("the identity half {text:?} is not a u16: {e}"))
        .and_then(|value| {
            if value == 0 {
                Err(format!("the identity half {text:?} is zero: no identity"))
            } else {
                Ok(value)
            }
        })
}

/// The identity an explicit pair names.
pub fn identity(pair: &str) -> Result<NodeId, String> {
    let (system, counter) = pair
        .split_once(':')
        .ok_or_else(|| format!("the identity {pair:?} is not the pair system:counter"))?;
    let system = SystemId::new(half(system)?)
        .ok_or_else(|| format!("the system half {system:?} is refused"))?;
    let counter = CrashCounter::new(half(counter)?)
        .ok_or_else(|| format!("the counter half {counter:?} is refused"))?;
    Ok(NodeId::new(system, counter))
}

/// The explicit pair an identity names.
pub fn pair_of(id: NodeId) -> String {
    let system = id.system_id().map_or(0, SystemId::get);
    let counter = id.crash_counter().map_or(0, CrashCounter::get);
    format!("{system}:{counter}")
}

const HEX: &[u8; 16] = b"0123456789abcdef";

fn hex_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0xf) as usize] as char);
    }
    out
}

fn hex_decode(text: &str) -> Result<Vec<u8>, String> {
    let bytes = text.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err("odd hex length".into());
    }
    let value = |b: u8| -> Result<u8, String> {
        match b {
            b'0'..=b'9' => Ok(b - b'0'),
            b'a'..=b'f' => Ok(b - b'a' + 10),
            b'A'..=b'F' => Ok(b - b'A' + 10),
            _ => Err("not hex".into()),
        }
    };
    (0..bytes.len() / 2)
        .map(|i| Ok((value(bytes[i * 2])? << 4) | value(bytes[i * 2 + 1])?))
        .collect()
}

// ----------------------------------------------------------------------
// Abstract names
// ----------------------------------------------------------------------

/// The corpus's status vocabulary, stated once: the protocol's own
/// status names, never a Rust debug rendering.
fn status_name(status: Status) -> &'static str {
    match status {
        Status::Normal => "Normal",
        Status::ViewChange => "ViewChange",
        Status::Restarting => "Restarting",
        Status::Replaying => "Replaying",
        Status::Joining => "Joining",
    }
}

/// The abstract operation name of a typed cluster operation
/// (`docs/uvrr-host-compliance.md` §3): the corpus's vocabulary, never
/// Rust debug formatting.
fn system_op_name(op: &SystemOperation) -> String {
    match op {
        SystemOperation::Void => "void".into(),
        SystemOperation::Init { order } => format!(
            "init order={}",
            order
                .iter()
                .map(|id| pair_of(*id))
                .collect::<Vec<_>>()
                .join(",")
        ),
        SystemOperation::Increment(node) => format!("increment {}", pair_of(*node)),
        SystemOperation::Decrement(node) => format!("decrement {}", pair_of(*node)),
        SystemOperation::Double => "double".into(),
        SystemOperation::Halve => "halve".into(),
        SystemOperation::Join { node, position } => {
            format!("join {} at {position}", pair_of(*node))
        }
        SystemOperation::Leave(node) => format!("leave {}", pair_of(*node)),
        SystemOperation::Nominate { from, offset } => {
            format!("nominate from={} offset={offset}", from.0)
        }
        SystemOperation::Batch(ops) => format!(
            "batch [{}]",
            ops.iter()
                .map(system_op_name)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The typed operation an abstract name names, with the argument
/// identities resolved.
fn system_op_of(op: &SystemOp) -> Result<SystemOperation, String> {
    Ok(match op {
        SystemOp::Increment { node } => SystemOperation::Increment(identity(node)?),
        SystemOp::Decrement { node } => SystemOperation::Decrement(identity(node)?),
        SystemOp::Double => SystemOperation::Double,
        SystemOp::Halve => SystemOperation::Halve,
        SystemOp::Join { node, position } => SystemOperation::Join {
            node: identity(node)?,
            position: *position,
        },
        SystemOp::Leave { node } => SystemOperation::Leave(identity(node)?),
        SystemOp::Nominate { from, offset } => SystemOperation::Nominate {
            from: View(*from),
            offset: *offset,
        },
        SystemOp::Batch { ops } => {
            SystemOperation::Batch(ops.iter().map(system_op_of).collect::<Result<_, _>>()?)
        }
    })
}

/// Renders a journal entry's payload as corpus text.
fn render_payload(entry: &LogEntry) -> String {
    match &entry.payload {
        Payload::Operation { payload, .. } => String::from_utf8_lossy(payload).into_owned(),
        Payload::System(op) => system_op_name(op),
    }
}

// ----------------------------------------------------------------------
// The executor
// ----------------------------------------------------------------------

/// One slot of the executor's cluster: the identity occupying it, the
/// live node (or the down state), the descriptor pieces a reopen needs,
/// and the retained boot-gate marker schedule. `markers` is `None`
/// exactly when a restart moved the slot's schedule to another identity
/// (the bumped life's boot owns the new schedule; the superseded
/// identity's record is empty — the corpus's own per-slot semantics).
struct NodeSlot {
    id: NodeId,
    node: Option<Node>,
    name: String,
    state_path: PathBuf,
    members: String,
    markers: Option<Arc<Mutex<Vec<String>>>>,
}

/// The abstract host interface over one provisioned cluster of adapter
/// nodes.
pub struct Executor {
    slots: Vec<NodeSlot>,
    /// The wire: datagrams queued for delivery, FIFO, exactly as the
    /// nodes emitted them.
    queue: VecDeque<(NodeId, NodeId, Vec<u8>)>,
    /// The executor's logical clock, advanced once per tick/tick_all and
    /// carried by every drive between those advances (the corpus's
    /// determinism: no wall clock anywhere).
    clock: u64,
    /// The roster size; the one-past-the-end identity names the outside
    /// gossip sender.
    nodes: usize,
    /// The genesis members buffer every provisioned node's descriptor
    /// carries.
    genesis: String,
    /// The vector operation sequence, one past the last proposal.
    op_seq: u64,
    /// The case's private marker-store directory.
    dir: PathBuf,
    /// The primary-timeout knob every provision and reopen plays by.
    provision_timeout: u64,
}

impl Executor {
    /// [`Op::Provision`], the setup's mandatory first operation: one
    /// [`Node::open_compliance`] per roster member over a fresh marker
    /// store, the packed-vector identities
    /// (`NodeId::new(SystemId(pos+1), CrashCounter(1))`), the case's
    /// primary-timeout knob, and the unbounded suffix budget the
    /// compliance rules arm.
    pub fn provision(nodes: usize, timeout: u64) -> Result<Executor, String> {
        static CASE_SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = compliance_root().join(format!(
            "case-{}-{}",
            std::process::id(),
            CASE_SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
        ));
        std::fs::create_dir_all(&dir).map_err(|e| format!("the case directory creates: {e}"))?;
        let genesis = (0..nodes)
            .map(|index| {
                let id = identity(&format!("{}:1", index + 1))?;
                Ok(format!("{}:n{}", id.0, index + 1))
            })
            .collect::<Result<Vec<_>, String>>()?
            .join("\0");
        let mut slots = Vec::with_capacity(nodes);
        for index in 0..nodes {
            let id = identity(&format!("{}:1", index + 1))?;
            let state_path = dir.join(format!("{}.state", id.0));
            let node = Node::open_compliance(
                &genesis,
                &format!("n{}", index + 1),
                &state_path.to_string_lossy(),
                timeout,
            )
            .map_err(|code| format!("node {}: the provision boots: code {code}", pair_of(id)))?;
            slots.push(NodeSlot {
                id,
                node: Some(node),
                name: format!("n{}", index + 1),
                state_path,
                members: genesis.clone(),
                markers: None,
            });
        }
        // The live nodes' marker schedules ride the store handles.
        for slot in &mut slots {
            if let Some(node) = &slot.node {
                slot.markers = Some(node.marker_log());
            }
        }
        Ok(Executor {
            slots,
            queue: VecDeque::new(),
            clock: 0,
            nodes,
            genesis,
            op_seq: 1,
            dir,
            provision_timeout: timeout,
        })
    }

    fn slot_of(&self, id: NodeId) -> Option<usize> {
        self.slots.iter().position(|slot| slot.id == id)
    }

    /// Sets the executor's logical clock on the node about to be driven.
    fn arm(node: &mut Node, clock: u64) {
        node.set_compliance_clock(clock);
    }

    /// Moves the node's queued outputs into the wire. Any output beyond
    /// the send kind is an adapter bug under the compliance rules (the
    /// opaque acknowledge answers no proposal).
    fn drain_outputs(&mut self, index: usize) {
        let own = self.slots[index].id;
        while let Some(out) = self.slots[index]
            .node
            .as_mut()
            .and_then(|node| node.next_output())
        {
            assert!(
                out.kind == lunet_advisory_lock::OUTPUT_SEND,
                "the compliance executor queues only unicast datagrams, got kind {}",
                out.kind
            );
            self.queue.push_back((own, NodeId(out.to), out.bytes));
        }
    }

    /// Applies one operation.
    pub fn apply(&mut self, op: &Op) -> Result<(), String> {
        match op {
            Op::Provision { nodes, timeout } => {
                let _ = (nodes, timeout);
                Err("provision is the setup's first operation, built by the executor".into())
            }
            Op::Settle => {
                for _ in 0..SETTLE_BOUND {
                    if self.quiet_and_normal() {
                        return Ok(());
                    }
                    self.tick_all()?;
                    self.deliver_all(None)?;
                }
                Err("settle did not converge inside the bound".into())
            }
            Op::Propose { node, payload } => {
                let id = identity(node)?;
                let op_id = OperationId {
                    msb: OP_MSB,
                    lsb: self.op_seq,
                };
                self.op_seq += 1;
                let index = self
                    .slot_of(id)
                    .ok_or_else(|| format!("propose: no slot for {node}"))?;
                let node_ref = self.slots[index]
                    .node
                    .as_mut()
                    .ok_or_else(|| format!("propose: {node} is down"))?;
                Self::arm(node_ref, self.clock);
                let _ = node_ref.propose_opaque(op_id, payload.as_bytes());
                self.drain_outputs(index);
                Ok(())
            }
            Op::Reconfigure { node, system } => {
                let id = identity(node)?;
                let system_op = system_op_of(system)?;
                let index = self
                    .slot_of(id)
                    .ok_or_else(|| format!("reconfigure: no slot for {node}"))?;
                let node_ref = self.slots[index]
                    .node
                    .as_mut()
                    .ok_or_else(|| format!("reconfigure: {node} is down"))?;
                Self::arm(node_ref, self.clock);
                let _ = node_ref.reconfigure_opaque(system_op);
                self.drain_outputs(index);
                Ok(())
            }
            Op::Tick { node } => {
                let id = identity(node)?;
                self.clock += 1;
                let index = self
                    .slot_of(id)
                    .ok_or_else(|| format!("tick: no slot for {node}"))?;
                let node_ref = self.slots[index]
                    .node
                    .as_mut()
                    .ok_or_else(|| format!("tick: {node} is down"))?;
                Self::arm(node_ref, self.clock);
                let _ = node_ref.idle();
                self.drain_outputs(index);
                Ok(())
            }
            Op::TickAll => self.tick_all(),
            Op::DeliverAll => {
                self.deliver_all(None)?;
                Ok(())
            }
            Op::Crash { node } => {
                let id = identity(node)?;
                let index = self
                    .slot_of(id)
                    .ok_or_else(|| format!("crash: no slot for {node}"))?;
                let slot = &mut self.slots[index];
                if slot.node.take().is_none() {
                    return Err(format!("crash: {node} is already down"));
                }
                Ok(())
            }
            Op::Restart { node, kind } => {
                let id = identity(node)?;
                let index = self
                    .slot_of(id)
                    .ok_or_else(|| format!("restart: no slot for {node}"))?;
                if self.slots[index].node.is_some() {
                    return Err(format!("restart: {node} is up; halt or crash it first"));
                }
                let (name, state_path, members) = {
                    let slot = &self.slots[index];
                    (
                        slot.name.clone(),
                        slot.state_path.clone(),
                        slot.members.clone(),
                    )
                };
                match kind.as_str() {
                    "clean" => {
                        let node = Node::open_compliance(
                            &members,
                            &name,
                            &state_path.to_string_lossy(),
                            self.provision_timeout,
                        )
                        .map_err(|code| format!("the clean restart boots: code {code}"))?;
                        if node.own_id() != id.0 {
                            return Err(format!(
                                "the clean restart continued a different identity: {} != {}",
                                node.own_id(),
                                id.0
                            ));
                        }
                        let markers = node.marker_log();
                        let slot = &mut self.slots[index];
                        slot.node = Some(node);
                        slot.markers = Some(markers);
                        self.drain_outputs(index);
                        Ok(())
                    }
                    "crashed" => {
                        let next = id.next_life().ok_or("the identity space is spent")?;
                        let node = Node::open_compliance(
                            &members,
                            &name,
                            &state_path.to_string_lossy(),
                            self.provision_timeout,
                        )
                        .map_err(|code| format!("the crashed restart boots: code {code}"))?;
                        if node.own_id() != next.0 {
                            return Err(format!(
                                "the crashed restart bumped to a different identity: {} != {}",
                                node.own_id(),
                                next.0
                            ));
                        }
                        let markers = node.marker_log();
                        let new_slot = NodeSlot {
                            id: next,
                            node: Some(node),
                            name,
                            state_path: state_path.clone(),
                            members: members.clone(),
                            markers: Some(markers),
                        };
                        self.slots.push(new_slot);
                        let new_index = self.slots.len() - 1;
                        // The bump's boot owns the new schedule; the
                        // superseded identity's record is empty.
                        self.slots[index].markers = None;
                        self.drain_outputs(new_index);
                        Ok(())
                    }
                    other => Err(format!("no such restart kind: {other}")),
                }
            }
            Op::Halt { node } => {
                let id = identity(node)?;
                let index = self
                    .slot_of(id)
                    .ok_or_else(|| format!("halt: no slot for {node}"))?;
                let Some(node_ref) = self.slots[index].node.as_mut() else {
                    return Err(format!("halt: {node} is down"));
                };
                Self::arm(node_ref, self.clock);
                let code = node_ref.stop();
                if code != lunet_advisory_lock::OK {
                    return Err(format!("halt: {node} refused: code {code}"));
                }
                self.slots[index].node = None;
                Ok(())
            }
            Op::Boot { node } => {
                let id = identity(node)?;
                if self.slot_of(id).is_some() {
                    return Err(format!("boot: {node} already has a slot"));
                }
                let system = id.system_id().map_or(0, SystemId::get);
                let name = format!("n{system}");
                let members = format!("{}\0{}:{name}:j", self.genesis, id.0);
                let state_path = self.dir.join(format!("{}.state", id.0));
                let node = Node::open_compliance(
                    &members,
                    &name,
                    &state_path.to_string_lossy(),
                    self.provision_timeout,
                )
                .map_err(|code| format!("the fresh joiner boots: code {code}"))?;
                let markers = node.marker_log();
                self.slots.push(NodeSlot {
                    id,
                    node: Some(node),
                    name,
                    state_path,
                    members,
                    markers: Some(markers),
                });
                let index = self.slots.len() - 1;
                self.drain_outputs(index);
                Ok(())
            }
            Op::Announce { node, old } => {
                let id = identity(node)?;
                let _previous = identity(old)?;
                let index = self
                    .slot_of(id)
                    .ok_or_else(|| format!("announce: no slot for {node}"))?;
                let node_ref = self.slots[index]
                    .node
                    .as_mut()
                    .ok_or_else(|| format!("announce: {node} is down"))?;
                Self::arm(node_ref, self.clock);
                let _ = node_ref.recover();
                self.drain_outputs(index);
                Ok(())
            }
            Op::Deliver { to, from, wire } => {
                let bytes = hex_decode(wire)?;
                let to_id = identity(to)?;
                let from_id = identity(from)?;
                let index = self
                    .slot_of(to_id)
                    .ok_or_else(|| format!("deliver: no slot for {to}"))?;
                if self.slots[index].node.is_none() {
                    return Err("the receiver is down".into());
                }
                let node_ref = self.slots[index].node.as_mut().expect("checked above");
                Self::arm(node_ref, self.clock);
                let _ = node_ref.receive(from_id.0, &bytes);
                self.drain_outputs(index);
                Ok(())
            }
            Op::Gossip { wire } => {
                let bytes = hex_decode(wire)?;
                let sender = identity(&format!("{}:1", self.nodes + 1))?;
                let live: Vec<usize> = (0..self.slots.len())
                    .filter(|index| self.slots[*index].node.is_some())
                    .collect();
                for index in live {
                    let node_ref = self.slots[index].node.as_mut().expect("checked above");
                    Self::arm(node_ref, self.clock);
                    let _ = node_ref.receive(sender.0, &bytes);
                }
                for index in 0..self.slots.len() {
                    self.drain_outputs(index);
                }
                Ok(())
            }
        }
    }

    /// Whether the network is quiet and every live node is `Normal`.
    fn quiet_and_normal(&self) -> bool {
        if !self.queue.is_empty() {
            return false;
        }
        self.slots.iter().all(|slot| {
            let Some(node) = slot.node.as_ref() else {
                return true;
            };
            Status::from_word(node.status().state) == Some(Status::Normal)
        })
    }

    /// One timer event to every live node, in slot order, the clock
    /// advanced once for the sweep (the reference host's `tick_all`).
    fn tick_all(&mut self) -> Result<(), String> {
        self.clock += 1;
        for index in 0..self.slots.len() {
            let Some(node_ref) = self.slots[index].node.as_mut() else {
                continue;
            };
            Self::arm(node_ref, self.clock);
            let _ = node_ref.idle();
        }
        for index in 0..self.slots.len() {
            self.drain_outputs(index);
        }
        Ok(())
    }

    /// Delivers every queued datagram once, in queue order. A delivery
    /// to a down node is popped and skipped (recorded undeliverable —
    /// it stays named in the drain's list).
    fn deliver_all(
        &mut self,
        mut record: Option<&mut Vec<ExpectedDelivery>>,
    ) -> Result<(), String> {
        let bound = DRAIN_BOUND;
        let mut steps = 0;
        while let Some((from, to, bytes)) = self.queue.pop_front() {
            steps += 1;
            if steps > bound {
                return Err("deliver_all did not quiet inside the bound".into());
            }
            if let Some(recorded) = record.as_deref_mut() {
                recorded.push(ExpectedDelivery {
                    from: pair_of(from),
                    to: pair_of(to),
                    wire: hex_encode(&bytes),
                });
            }
            let index = self.slot_of(to);
            let up = index.map(|i| self.slots[i].node.is_some()).unwrap_or(false);
            if up {
                let i = index.expect("checked above");
                let node_ref = self.slots[i].node.as_mut().expect("checked above");
                Self::arm(node_ref, self.clock);
                let _ = node_ref.receive(from.0, &bytes);
                self.drain_outputs(i);
            }
        }
        Ok(())
    }

    /// The post-input drain: delivers until quiet, recording the exact
    /// delivery sequence. No timer event fires in the drain.
    pub fn drain(&mut self) -> Result<Vec<ExpectedDelivery>, String> {
        let mut out = Vec::new();
        for _ in 0..DRAIN_BOUND {
            if self.queue.is_empty() {
                return Ok(out);
            }
            self.deliver_all(Some(&mut out))?;
        }
        Err("the drain did not quiet inside the bound".into())
    }

    /// The boot-gate marker schedule a known identity's post record
    /// carries: the machine rounds of the slot's most recent gate, as
    /// `<Marker>@<system>:<counter>` with the `drain` between the halt's
    /// rounds. A slot whose schedule a restart moved away carries none.
    fn marker_states(markers: &Option<Arc<Mutex<Vec<String>>>>) -> Vec<String> {
        let Some(log) = markers else {
            return Vec::new();
        };
        log.lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .filter_map(|line| {
                if line == "drain" {
                    return Some("drain".to_string());
                }
                let rest = line.strip_prefix("commit:")?;
                let (name, packed) = rest.split_once('@')?;
                let identity = NodeId(packed.parse().ok()?);
                let system = identity.system_id()?.get();
                let counter = identity.crash_counter()?.get();
                Some(format!("{name}@{system}:{counter}"))
            })
            .collect()
    }

    /// One node's full post record, every field captured.
    #[must_use]
    pub fn post_of(&self, id: NodeId) -> PostNode {
        let Some(index) = self.slot_of(id) else {
            return PostNode::default();
        };
        let slot = &self.slots[index];
        let Some(node) = slot.node.as_ref() else {
            return PostNode {
                node: pair_of(id),
                markers: Some(Self::marker_states(&slot.markers)),
                ..PostNode::default()
            };
        };
        let status = node.status();
        let (accepted, committed, applied) = node.frontiers();
        let (members, weights) = node
            .membership()
            .map(|(order, weights)| {
                (
                    order.iter().map(|m| pair_of(*m)).collect::<Vec<_>>(),
                    weights,
                )
            })
            .unwrap_or((Vec::new(), Vec::new()));
        PostNode {
            node: pair_of(id),
            status: Status::from_word(status.state)
                .map(status_name)
                .map(str::to_string),
            era: Some(status.era),
            view: Some(status.view),
            accepted: Some(accepted),
            committed: Some(committed),
            applied: Some(applied),
            journal: Some(node.journal_entries().iter().map(render_payload).collect()),
            members: Some(members),
            weights: Some(weights),
            markers: Some(Self::marker_states(&slot.markers)),
            witnesses: Some(node.witnesses().iter().map(|w| pair_of(*w)).collect()),
        }
    }
}

/// Runs a case's setup and input, then drains and captures the full
/// expectation.
pub fn run_case(case: &Case) -> Result<Expect, String> {
    let Some(Op::Provision { nodes, timeout }) = case.setup.first() else {
        return Err("the setup must open with provision".into());
    };
    let mut executor = Executor::provision(*nodes, *timeout)?;
    for op in &case.setup[1..] {
        executor
            .apply(op)
            .map_err(|e| format!("the setup's {op:?} refused: {e}"))?;
    }
    executor
        .apply(&case.input)
        .map_err(|e| format!("the input {:?} refused: {e}", case.input))?;
    let deliveries = executor.drain()?;
    let post = executor
        .slots
        .iter()
        .map(|slot| executor.post_of(slot.id))
        .collect();
    Ok(Expect { deliveries, post })
}

/// One named field's mismatch, or `None` when the field passes or is
/// unnamed.
fn field<T: PartialEq + std::fmt::Debug>(
    captured: &Option<T>,
    named: &Option<T>,
    name: &str,
) -> Option<String> {
    captured
        .as_ref()
        .zip(named.as_ref())
        .filter(|(x, y)| x != y)
        .map(|(x, y)| format!("{name}: expected {y:?}, captured {x:?}"))
}

/// Asserts a captured expectation against a case's named expectation:
/// the delivery sequence exactly, the named post fields exactly.
pub fn assert_expectation(case: &Case, captured: &Expect) -> Result<(), String> {
    if captured.deliveries != case.expect.deliveries {
        return Err(format!(
            "the delivery sequence differs: expected {:?}, captured {:?}",
            case.expect.deliveries, captured.deliveries
        ));
    }
    for named in &case.expect.post {
        let Some(captured_post) = captured.post.iter().find(|p| p.node == named.node) else {
            return Err(format!("no post record for node {}", named.node));
        };
        if let Some(mismatch) = field(&captured_post.status, &named.status, "status")
            .or_else(|| field(&captured_post.era, &named.era, "era"))
            .or_else(|| field(&captured_post.view, &named.view, "view"))
            .or_else(|| field(&captured_post.accepted, &named.accepted, "accepted"))
            .or_else(|| field(&captured_post.committed, &named.committed, "committed"))
            .or_else(|| field(&captured_post.applied, &named.applied, "applied"))
            .or_else(|| field(&captured_post.journal, &named.journal, "journal"))
            .or_else(|| field(&captured_post.members, &named.members, "members"))
            .or_else(|| field(&captured_post.weights, &named.weights, "weights"))
            .or_else(|| field(&captured_post.markers, &named.markers, "markers"))
            .or_else(|| field(&captured_post.witnesses, &named.witnesses, "witnesses"))
        {
            return Err(mismatch);
        }
    }
    Ok(())
}
