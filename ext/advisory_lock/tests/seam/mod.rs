//! The in-memory message-fed seam — layer 1 of the test scaffold
//! (`docs/src/test-scaffold.md`, "The in-memory message-fed layer"), and
//! the ONE harness the protocol-state expectations are written against.
//!
//! The node under test is the adapter's `Node`. Outbound frames are
//! captured in memory, inbound frames are fed by the test, and the clock
//! is this module's logical tick — a parameter on every call, never a
//! wall reading. The storage is the real marker store over a scratch
//! directory, so a boot, a stop and a restart exercise the durable
//! obligations directly; the only things not real are the wire and the
//! clock.
//!
//! The laws the seam holds, each proved in `tests/seam_test.rs`:
//!
//! - no child process, no sockets, no ports, never a wall clock — the
//!   gate in that test reads THIS file and fails on any spelling of them,
//!   so the claim is checked, not asserted in prose;
//! - the emission multiset is asserted EXACTLY: an emission beyond the
//!   named multiset fails the test and names the excess one;
//! - boot, shutdown, crash (a drop with no stop contract taken) and
//!   restart (a reopen over the same markers) are each a plain call.
//!
//! Its vocabulary is NOT a second one. Every name this module renders —
//! the `system:counter` identity pairs, the lower-case hex wire bytes,
//! the `<Marker>@<system>:<counter>` marker rounds, the post-state
//! record, the delivery record — is the compliance executor's own,
//! borrowed from `tests/compliance/mod.rs`, the same module the 73-case
//! corpus replays through. There is one definition of each; this module
//! carries none of its own.

// Including this module: `#[path = "seam/mod.rs"] mod seam;`, the shape
// `tests/compliance.rs:7` already uses. A binary that exercises only the
// calls its own scenarios need will see the rest reported as dead — the
// lint has no notion of a module whose callers live in another binary, so
// THAT binary states `#[allow(dead_code)]` at its own `mod seam;` line
// when it needs to. Nothing is silenced here: `tests/seam_test.rs`
// exercises every call below, and the build is silent without any allow.

#[path = "../compliance/mod.rs"]
// The corpus's case-and-executor surface. This seam borrows the naming
// vocabulary out of it — the identity pairs, the hex codec, the marker
// and post-state recorders, the expectation comparison — and drives NONE
// of its corpus-only machinery: the case records, the corpus reader and
// the runner are the gate's, not this layer's. They therefore read as
// dead inside this binary, which is exactly what this allow pays for;
// the seam's own items carry no allow, and `tests/seam_test.rs`
// exercises every one of them.
#[allow(dead_code)]
mod compliance;

use std::collections::VecDeque;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use lunet_advisory_lock::{Node, OK};
use vrr::ids::{OperationId, SystemId};

use compliance::OP_MSB;
use compliance::post_record;

/// The cluster operation a scenario drives, the core's own typed
/// enumeration: the same value `Node::reconfigure_opaque` takes.
pub use vrr::configuration::SystemOperation;
/// The identity a scenario addresses: the packed `system:counter` pair,
/// the adapter's own peer address space.
pub use vrr::ids::NodeId;
/// The view number a typed cluster operation names.
pub use vrr::ids::View;

/// One outbound frame, named. The compliance executor's own delivery
/// record, re-exported under this layer's word for it: a frame's sender,
/// its addressee, and the wire bytes as lower-case hex. There is one
/// shape and one rendering — the corpus asserts against the same one.
pub use compliance::ExpectedDelivery as Emission;

/// One node's observable post-state, captured by the compliance
/// executor's own recorder: status, era, view, the three frontiers, the
/// journal, the membership with its weights, the boot-gate marker
/// schedule, and the witness list. Absent fields are unconstrained.
pub use compliance::PostNode as Observable;

/// A named expectation in the corpus's own grammar: the delivery sequence
/// a scenario expects, and the post fields it constrains. The seam
/// compares against it with the compliance executor's own comparison.
pub use compliance::Expect;

/// The `system:counter` pair an identity is named by, the corpus's own
/// rendering of one.
pub use compliance::pair_of;

/// The settle bound: a provisioned cluster converges long before this, so
/// a script that cannot settle inside it is a scenario defect, not a slow
/// machine — the seam errors rather than spins.
pub const SETTLE_BOUND: usize = 1_000;

/// The drain bound: the cascade after one feed is finite and
/// deterministic, so a script that outruns it is a scenario defect.
pub const DRAIN_BOUND: usize = 1_000;

/// One queued frame: sender, addressee, wire bytes.
type Frame = (NodeId, NodeId, Vec<u8>);

// ----------------------------------------------------------------------
// Names
// ----------------------------------------------------------------------

/// A refusal code's name, the adapter's own vocabulary stated once
/// (`src/advisory_lock.tl:54`, the `errors` table the Lua boundary
/// raises) so a refusal reads the same in a log line, a status note and a
/// seam assertion. `OK` names itself `ok`; a code no table entry names
/// renders `unknown code` rather than a bare integer.
pub fn code_name(code: i32) -> &'static str {
    match code {
        0 => "ok",
        -1 => "invalid argument",
        -2 => "invalid configuration or recovery state",
        -4 => "invalid client JSON",
        -5 => "invalid replication message",
        -6 => "message exceeds one UDP packet",
        -7 => "lock service execution failed",
        -8 => "not the leader",
        -9 => "replica faulted",
        -10 => "node stopped",
        -127 => "native panic",
        _ => "unknown code",
    }
}

/// The wire bytes a lower-case hex string names, through the compliance
/// executor's own decoder.
///
/// # Panics
///
/// A string that is not hex. Authoring input, not a runtime condition.
#[must_use]
pub fn hex(text: &str) -> Vec<u8> {
    compliance::hex_decode(text).unwrap_or_else(|e| panic!("the wire {text:?}: {e}"))
}

/// The named emission a test writes, from typed identities and wire
/// bytes: exactly the record the seam captures, so a scenario's names and
/// its capture can never drift apart.
#[must_use]
pub fn emission(from: NodeId, to: NodeId, wire: &[u8]) -> Emission {
    Emission {
        from: pair_of(from),
        to: pair_of(to),
        wire: compliance::hex_encode(wire),
    }
}

/// The named emission a test writes from the corpus's own spelling, in
/// the delivery record's OWN field order — `(from, to, wire)`, sender
/// first, as [`Emission`] states it. (The corpus's `Deliver` operation
/// takes the opposite order, `(to, from, wire)`, and [`Inbound::named`]
/// follows that one; this is the asymmetry the corpus already has.)
///
/// The named form is how a hand-written expectation reads; both spellings
/// go through the one renderer. A pair or a hex string that names nothing
/// panics at authoring time, never at assertion time.
#[must_use]
pub fn emission_named(from: &str, to: &str, wire: &str) -> Emission {
    for half in [to, from] {
        let _ =
            compliance::identity(half).unwrap_or_else(|e| panic!("the emission's `{half}`: {e}"));
    }
    let bytes = compliance::hex_decode(wire).unwrap_or_else(|e| panic!("the emission's wire: {e}"));
    Emission {
        from: from.to_string(),
        to: to.to_string(),
        wire: compliance::hex_encode(&bytes),
    }
}

/// One refused obligation, by name: which node refused, which call was
/// made, and the code it answered. A drive that reports anything but
/// `OK` lands here — including a dropped peer frame and a malformed
/// one, so the drop discipline is enumerable rather than inferred.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Refusal {
    /// The refusing node, as the corpus names an identity.
    pub node: String,
    /// The seam call the test made, in the harness's own vocabulary.
    pub call: &'static str,
    /// The code the drive reported.
    pub code: i32,
}

impl Refusal {
    /// One refused obligation's full name: `1:1 reconfigure → not the
    /// leader (-8)`.
    #[must_use]
    pub fn describe(&self) -> String {
        format!(
            "{} {} → {} ({})",
            self.node,
            self.call,
            code_name(self.code),
            self.code
        )
    }
}

/// How a down node comes back: over the same markers, continuing its
/// life, or with its life bumped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Restart {
    /// A clean stop ran first: the boot continues the same identity.
    Clean,
    /// No stop contract ran: the boot classifies the death a crash and
    /// bumps the life.
    Crashed,
}

// ----------------------------------------------------------------------
// The seam
// ----------------------------------------------------------------------

/// One seat of the harness: the identity occupying it, the live node (or
/// the down state), the descriptor pieces a reopen needs, and the retained
/// boot-gate marker schedule. `markers` is `None` exactly when a restart
/// moved that schedule onto another identity.
struct Seat {
    id: NodeId,
    node: Option<Node>,
    name: String,
    state_path: PathBuf,
    members: String,
    markers: Option<Arc<Mutex<Vec<String>>>>,
}

/// The seam: one in-memory, message-fed cluster of adapter `Node`s.
pub struct Seam {
    seats: Vec<Seat>,
    /// The wire: frames queued for delivery, FIFO, in the order the
    /// nodes emitted them. Nothing else carries traffic.
    wire: VecDeque<Frame>,
    /// The logical clock. Every drive carries the value this field holds
    /// (`Node::set_compliance_clock`), and only this module ever moves
    /// it, so no reading of a wall clock can enter a replay.
    clock: u64,
    /// The roster size; the one-past-the-end identity names the outside
    /// gossip sender.
    nodes: usize,
    /// The genesis member buffer every provisioned seat's descriptor
    /// carries.
    genesis: String,
    /// The opaque proposal sequence, one past the last proposal.
    op_seq: u64,
    /// The scenario's private marker-store directory.
    dir: PathBuf,
    /// The primary-timeout knob every boot and reopen plays by.
    primary_timeout: u64,
    /// Every frame this seam delivered, in order, since boot.
    emissions: Vec<Emission>,
    /// Every refusal this seam's drives collected, in order, since boot.
    refusals: Vec<Refusal>,
}

impl Seam {
    /// BOOT: one provisioned cluster over a fresh marker store — one
    /// `Node::open_compliance` per roster member, the packed identities
    /// `NodeId::new(SystemId(index+1), CrashCounter(1))`, the primary
    /// timeout the scenario plays by, and the compliance rules that make
    /// the clock a parameter and the payloads opaque.
    pub fn boot(nodes: usize, primary_timeout: u64) -> Result<Seam, String> {
        if nodes == 0 {
            return Err("a seam provisions at least one member".into());
        }
        let dir = fresh_dir("cluster");
        let genesis = genesis_of(nodes)?;
        let mut seats = Vec::with_capacity(nodes);
        for index in 0..nodes {
            let id = provisioned_id(index)?;
            let state_path = dir.join(format!("{}.state", id.0));
            let node = Node::open_compliance(
                &genesis,
                &member_name(index),
                &state_path.to_string_lossy(),
                primary_timeout,
            )
            .map_err(|code| {
                format!(
                    "boot {}: the member does not open: {} ({})",
                    pair_of(id),
                    code_name(code),
                    code
                )
            })?;
            let markers = node.marker_log();
            seats.push(Seat {
                id,
                node: Some(node),
                name: member_name(index),
                state_path,
                members: genesis.clone(),
                markers: Some(markers),
            });
        }
        Ok(Seam {
            seats,
            wire: VecDeque::new(),
            clock: 0,
            nodes,
            genesis,
            op_seq: 1,
            dir,
            primary_timeout,
            emissions: Vec::new(),
            refusals: Vec::new(),
        })
    }

    // ------------------------------------------------------------------
    // The clock
    // ------------------------------------------------------------------

    /// The logical clock's current value: the tick every drive from here
    /// carries. A test names the time it forces a transition at by
    /// naming this number.
    #[must_use]
    pub fn clock(&self) -> u64 {
        self.clock
    }

    /// Move the logical clock forward WITHOUT driving. Time passes for a
    /// node only when it is driven, so a scenario separates "the timeout
    /// has elapsed" from "the node has noticed".
    pub fn advance(&mut self, ticks: u64) {
        self.clock += ticks;
    }

    /// The timer event, to one node: the clock moves one tick and that
    /// node takes it (`Node::idle`). Returns the frames the tick's whole
    /// exchange delivered.
    pub fn tick(&mut self, id: NodeId) -> Result<Vec<Emission>, String> {
        self.clock += 1;
        let mark = self.emissions.len();
        let index = self.seat_index(id)?;
        self.drive_one(index, "tick", |node| node.idle())
            .ok_or_else(|| format!("tick: {} is down", pair_of(id)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// The timer sweep, to every live seat in seat order: the clock moves
    /// one tick for the sweep and each node takes it.
    pub fn tick_all(&mut self) -> Result<Vec<Emission>, String> {
        self.clock += 1;
        let mark = self.emissions.len();
        for index in 0..self.seats.len() {
            let _ = self.drive_one(index, "tick_all", |node| node.idle());
        }
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// Drive the cluster until it is quiet and every live seat reports
    /// `Normal`, inside [`SETTLE_BOUND`]. Returns every frame the settle
    /// delivered — a scenario that settles names that whole multiset.
    pub fn settle(&mut self) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        for _ in 0..SETTLE_BOUND {
            self.tick_all()?;
            if self.quiet_and_normal() {
                return Ok(self.emissions_since(mark).to_vec());
            }
        }
        Err(format!(
            "settle did not converge inside {SETTLE_BOUND} ticks: {:?}",
            self.states()
        ))
    }

    // ------------------------------------------------------------------
    // The force-fed exchange
    // ------------------------------------------------------------------

    /// Feed one inbound frame — the plain function call the layer's whole
    /// point is: the addressed node is handed the bytes, attributed to
    /// the sending identity, and the exchange it causes is delivered to
    /// quiet. Returns every frame that exchange delivered.
    pub fn feed(&mut self, frame: &Inbound) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let index = self.seat_index(frame.to)?;
        let from = frame.from;
        let wire = frame.wire.clone();
        self.drive_one(index, "receive", |node| node.receive(from.0, &wire))
            .ok_or_else(|| format!("feed: {} is down", pair_of(frame.to)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// Feed an explicit LIST of inbound frames, in order, each with its
    /// own exchange: the scenario's message sequence, written down.
    pub fn feed_all(&mut self, frames: &[Inbound]) -> Result<Vec<Emission>, String> {
        let mut out = Vec::new();
        for frame in frames {
            out.extend(self.feed(frame)?);
        }
        Ok(out)
    }

    /// The outside gossip shape: one frame from the one-past-the-end
    /// identity, handed to every live node.
    pub fn gossip(&mut self, wire: &[u8]) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let sender = outside_sender(self.nodes)?;
        let bytes = wire.to_vec();
        for index in 0..self.seats.len() {
            let _ = self.drive_one(index, "gossip", |node| node.receive(sender.0, &bytes));
        }
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// Deliver the queued wire to quiet without moving the clock: the
    /// frames already on the wire arrive and nothing else happens.
    pub fn deliver(&mut self) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    // ------------------------------------------------------------------
    // The other drives
    // ------------------------------------------------------------------

    /// One opaque client proposal: the payload is raw bytes the core
    /// carries opaque, under the fixture's fixed identity half.
    pub fn propose(&mut self, id: NodeId, payload: &[u8]) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let op_id = OperationId {
            msb: OP_MSB,
            lsb: self.op_seq,
        };
        self.op_seq += 1;
        let index = self.seat_index(id)?;
        let bytes = payload.to_vec();
        self.drive_one(index, "propose", |node| node.propose_opaque(op_id, &bytes))
            .ok_or_else(|| format!("propose: {} is down", pair_of(id)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// One typed cluster operation, over the ordinary consensus pipeline.
    pub fn reconfigure(
        &mut self,
        id: NodeId,
        op: SystemOperation,
    ) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let index = self.seat_index(id)?;
        self.drive_one(index, "reconfigure", |node| node.reconfigure_opaque(op))
            .ok_or_else(|| format!("reconfigure: {} is down", pair_of(id)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// One client request through the ordinary (non-opaque) boundary: the
    /// lock-verb JSON a caller sends, with the correlated reply leaving as
    /// a kind-2 output rather than as wire traffic.
    pub fn request(&mut self, id: NodeId, json: &[u8]) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let index = self.seat_index(id)?;
        let bytes = json.to_vec();
        self.drive_one(index, "request", |node| node.request(&bytes))
            .ok_or_else(|| format!("request: {} is down", pair_of(id)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// The host-forced view change: the leader timeout's conclusion that a
    /// primary is dead, at the named (era, view).
    pub fn force_view(&mut self, id: NodeId, era: u32, view: u32) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let index = self.seat_index(id)?;
        self.drive_one(index, "force_view", |node| node.force_view(era, view))
            .ok_or_else(|| format!("force_view: {} is down", pair_of(id)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// The election tick: the same liveness input as [`Seam::tick`], taken
    /// through the boundary a leader timeout fires on.
    pub fn leader_timeout(&mut self, id: NodeId) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let index = self.seat_index(id)?;
        self.drive_one(index, "leader_timeout", |node| node.leader_timeout())
            .ok_or_else(|| format!("leader_timeout: {} is down", pair_of(id)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    /// One timeout toggle's event capture: the host's timeout plane's
    /// record of a flip of its `timedout` state. Capture only — no
    /// protocol path — so the seam records it and drives nothing.
    pub fn note_timeout(&mut self, id: NodeId, timedout: bool, at_ms: u64) -> Result<(), String> {
        let index = self.seat_index(id)?;
        let Some(node) = self.seats[index].node.as_mut() else {
            return Err(format!("note_timeout: {} is down", pair_of(id)));
        };
        node.note_timeout_toggle(timedout, at_ms, None);
        Ok(())
    }

    /// The fenced-boot drive: the announcement a restarted replica owes,
    /// then the tick that lets the primary adopt it.
    pub fn announce(&mut self, id: NodeId) -> Result<Vec<Emission>, String> {
        let mark = self.emissions.len();
        let index = self.seat_index(id)?;
        self.drive_one(index, "announce", |node| node.recover())
            .ok_or_else(|| format!("announce: {} is down", pair_of(id)))?;
        self.exchange()?;
        Ok(self.emissions_since(mark).to_vec())
    }

    // ------------------------------------------------------------------
    // The lifecycle, each a plain call
    // ------------------------------------------------------------------

    /// SHUTDOWN: the stop contract, taken on the node that is up. The wire
    /// closes, the marker rounds are written, the durable sink drains, and
    /// only then does the seat come down — which is what makes a later
    /// reopen over the same markers a restart rather than a crash.
    pub fn shutdown(&mut self, id: NodeId) -> Result<(), String> {
        let index = self.seat_index(id)?;
        let code = self
            .drive_one(index, "shutdown", |node| node.stop())
            .ok_or_else(|| format!("shutdown: {} is already down", pair_of(id)))?;
        if code != OK {
            return Err(format!(
                "shutdown {}: {} ({code})",
                pair_of(id),
                code_name(code)
            ));
        }
        self.seats[index].node = None;
        Ok(())
    }

    /// CRASH: the node is dropped WITHOUT the stop contract — no drain
    /// point, no marker round, no sink drain. A drop, never a teardown:
    /// what is left behind is exactly what a power cut leaves, and the
    /// next boot over the same markers is what classifies it.
    pub fn crash(&mut self, id: NodeId) -> Result<(), String> {
        let index = self.seat_index(id)?;
        if self.seats[index].node.take().is_none() {
            return Err(format!("crash: {} is already down", pair_of(id)));
        }
        Ok(())
    }

    /// RESTART: a reopen over the SAME markers. `Restart::Clean` continues
    /// the identity a shutdown ended; `Restart::Crashed` reads the death a
    /// crash left and bumps the life, seating the bumped identity as a
    /// seat of its own. Returns the identity now live, so the caller
    /// addresses the bumped one from here on.
    pub fn restart(&mut self, id: NodeId, kind: Restart) -> Result<NodeId, String> {
        let index = self.seat_index(id)?;
        if self.seats[index].node.is_some() {
            return Err(format!(
                "restart: {} is up; shutdown or crash it first",
                pair_of(id)
            ));
        }
        let (name, state_path, members) = {
            let seat = &self.seats[index];
            (
                seat.name.clone(),
                seat.state_path.clone(),
                seat.members.clone(),
            )
        };
        let booted = Node::open_compliance(
            &members,
            &name,
            &state_path.to_string_lossy(),
            self.primary_timeout,
        )
        .map_err(|code| {
            format!(
                "restart {}: the node does not reopen: {} ({})",
                pair_of(id),
                code_name(code),
                code
            )
        })?;
        let expected = match kind {
            Restart::Clean => id,
            Restart::Crashed => id
                .next_life()
                .ok_or_else(|| format!("restart {}: the identity space is spent", pair_of(id)))?,
        };
        if booted.own_id() != expected.0 {
            return Err(format!(
                "restart {}: the reopen came up as {}, not {}",
                pair_of(id),
                pair_of(NodeId(booted.own_id())),
                pair_of(expected)
            ));
        }
        let markers = booted.marker_log();
        match kind {
            Restart::Clean => {
                let seat = &mut self.seats[index];
                seat.node = Some(booted);
                seat.markers = Some(markers);
                self.exchange()?;
                Ok(expected)
            }
            Restart::Crashed => {
                // The bumped life owns the new schedule; the superseded
                // identity's record is empty.
                self.seats[index].markers = None;
                self.seats.push(Seat {
                    id: expected,
                    node: Some(booted),
                    name,
                    state_path,
                    members,
                    markers: Some(markers),
                });
                self.exchange()?;
                Ok(expected)
            }
        }
    }

    /// BOOT a joiner: a fresh identity over its own marker store, with the
    /// roster's genesis and a post-genesis `:j` entry for itself. Returns
    /// the identity it came up as.
    pub fn join(&mut self, id: NodeId) -> Result<NodeId, String> {
        if self.seats.iter().any(|seat| seat.id == id) {
            return Err(format!("join: {} already has a seat", pair_of(id)));
        }
        let system = id.system_id().map_or(0, SystemId::get);
        let name = format!("n{system}");
        let members = format!("{}\0{}:{name}:j", self.genesis, id.0);
        let state_path = self.dir.join(format!("{}.state", id.0));
        let node = Node::open_compliance(
            &members,
            &name,
            &state_path.to_string_lossy(),
            self.primary_timeout,
        )
        .map_err(|code| {
            format!(
                "join {}: the joiner does not open: {} ({})",
                pair_of(id),
                code_name(code),
                code
            )
        })?;
        if node.own_id() != id.0 {
            return Err(format!(
                "join {}: the joiner came up as {}, not {}",
                pair_of(id),
                pair_of(NodeId(node.own_id())),
                pair_of(id)
            ));
        }
        let markers = node.marker_log();
        self.seats.push(Seat {
            id,
            node: Some(node),
            name,
            state_path,
            members,
            markers: Some(markers),
        });
        self.exchange()?;
        Ok(id)
    }

    // ------------------------------------------------------------------
    // What the seam saw
    // ------------------------------------------------------------------

    /// The identity an explicit `system:counter` pair names — the corpus's
    /// own spelling, the same one every pair in a named expectation uses.
    ///
    /// # Panics
    ///
    /// A pair that names no identity. Authoring input, not a runtime
    /// condition.
    #[must_use]
    pub fn identity(pair: &str) -> NodeId {
        compliance::identity(pair).unwrap_or_else(|e| panic!("{e}"))
    }

    /// The primary timeout this cluster plays by, in the clock's units:
    /// the interval a scenario advances past to force a suspicion.
    #[must_use]
    pub fn primary_timeout(&self) -> u64 {
        self.primary_timeout
    }

    /// A mark in the emission tape: the count to hand back to
    /// [`Seam::assert_emissions_since`], so a scenario names one step's
    /// multiset instead of everything the script has produced so far.
    #[must_use]
    pub fn mark(&self) -> usize {
        self.emissions.len()
    }

    /// Every frame delivered since boot, in order.
    #[must_use]
    pub fn emissions(&self) -> &[Emission] {
        &self.emissions
    }

    /// Every frame delivered after `mark`, in order.
    #[must_use]
    pub fn emissions_since(&self, mark: usize) -> &[Emission] {
        &self.emissions[mark.min(self.emissions.len())..]
    }

    /// Every refusal collected since boot, in order.
    #[must_use]
    pub fn refusals(&self) -> &[Refusal] {
        &self.refusals
    }

    /// Every refusal collected after `mark`, in order.
    #[must_use]
    pub fn refusals_since(&self, mark: usize) -> &[Refusal] {
        &self.refusals[mark.min(self.refusals.len())..]
    }

    /// One identity's observable state: the compliance recorder's own post
    /// record, taken now. A down seat reads its marker schedule and
    /// nothing else; an unseated identity reads the empty record.
    #[must_use]
    pub fn state(&self, id: NodeId) -> Observable {
        let Some(index) = self.seat_index(id).ok() else {
            return post_record(None, &None, id);
        };
        let seat = &self.seats[index];
        post_record(seat.node.as_ref(), &seat.markers, id)
    }

    /// Every seat's observable state, in seat order.
    #[must_use]
    pub fn states(&self) -> Vec<Observable> {
        self.seats
            .iter()
            .map(|seat| post_record(seat.node.as_ref(), &seat.markers, seat.id))
            .collect()
    }

    /// Every seated identity, in seat order, as the corpus names one.
    #[must_use]
    pub fn identities(&self) -> Vec<String> {
        self.seats.iter().map(|seat| pair_of(seat.id)).collect()
    }

    /// Whether a seat is live.
    #[must_use]
    pub fn is_live(&self, id: NodeId) -> bool {
        self.seat_index(id)
            .is_ok_and(|index| self.seats[index].node.is_some())
    }

    /// The whole post-input expectation in the corpus's own shape: every
    /// frame delivered since `mark`, and every seat's post record.
    #[must_use]
    pub fn expectation_since(&self, mark: usize) -> compliance::Expect {
        compliance::Expect {
            deliveries: self.emissions_since(mark).to_vec(),
            post: self.states(),
        }
    }

    // ------------------------------------------------------------------
    // The named-multiset assertions
    // ------------------------------------------------------------------

    /// The emission multiset, asserted EXACTLY against everything the
    /// seam has produced since boot. An emission beyond the named
    /// multiset FAILS the test, and the failure names that emission and
    /// every named one that never arrived — nothing is ever allowed
    /// through unnamed.
    #[track_caller]
    pub fn assert_emissions(&self, named: &[Emission]) {
        self.assert_emissions_since(0, named);
    }

    /// The emission multiset, asserted EXACTLY against what the seam has
    /// produced after `mark`.
    #[track_caller]
    pub fn assert_emissions_since(&self, mark: usize, named: &[Emission]) {
        let captured: Vec<&Emission> = self.emissions_since(mark).iter().collect();
        let expected: Vec<&Emission> = named.iter().collect();
        if captured == expected {
            return;
        }
        let excess = beyond(&captured, &expected);
        let missing = beyond(&expected, &captured);
        panic!(
            "the emission multiset differs: {} captured, {} named\n\
             beyond the named multiset (each one FAILS the test): {}\n\
             named but never emitted: {}\n\
             captured: {:?}\nnamed: {:?}",
            captured.len(),
            expected.len(),
            describe_all(&excess),
            describe_all(&missing),
            captured,
            expected
        );
    }

    /// The refusal multiset, asserted EXACTLY against everything the seam
    /// has collected since boot: every refusal named, and no refusal
    /// unnamed.
    #[track_caller]
    pub fn assert_refusals(&self, named: &[Refusal]) {
        self.assert_refusals_since(0, named);
    }

    /// The refusal multiset, asserted EXACTLY against what the seam has
    /// collected after `mark`.
    #[track_caller]
    pub fn assert_refusals_since(&self, mark: usize, named: &[Refusal]) {
        let captured: Vec<&Refusal> = self.refusals_since(mark).iter().collect();
        let expected: Vec<&Refusal> = named.iter().collect();
        if captured == expected {
            return;
        }
        panic!(
            "the refusal multiset differs: {} captured, {} named\n\
             beyond the named multiset: {:?}\nnamed but never refused: {:?}",
            captured.len(),
            expected.len(),
            beyond(&captured, &expected),
            beyond(&expected, &captured)
        );
    }

    /// The observable state against the corpus's own `post` expectation,
    /// by that executor's own comparison (the named fields exactly, the
    /// unconstrained ones free).
    ///
    /// # Errors
    ///
    /// The mismatch the corpus's comparison names, or `Err` when no seat
    /// carries the named node.
    pub fn assert_state(&self, named: &compliance::Expect) -> Result<(), String> {
        let captured = self.expectation_since(0);
        compliance::assert_named(named, &captured)
    }

    // ------------------------------------------------------------------
    // The wire and the records
    // ------------------------------------------------------------------

    /// The seat an identity holds.
    fn seat_index(&self, id: NodeId) -> Result<usize, String> {
        self.seats
            .iter()
            .position(|seat| seat.id == id)
            .ok_or_else(|| format!("no seat for {}", pair_of(id)))
    }

    /// The refusal discipline: a drive that answers anything but `OK` is
    /// recorded by node, by call and by code before the code goes back to
    /// the caller. Nothing is ever dropped without a name.
    fn record(&mut self, id: NodeId, call: &'static str, code: i32) {
        if code != OK {
            self.refusals.push(Refusal {
                node: pair_of(id),
                call,
                code,
            });
        }
    }

    /// Drives one seat at the current logical tick and records what it
    /// answered: `None` when the seat is down (a call on a down node is
    /// the seam's own refusal, not a node's), the code otherwise.
    fn drive_one(
        &mut self,
        index: usize,
        call: &'static str,
        act: impl FnOnce(&mut Node) -> i32,
    ) -> Option<i32> {
        let clock = self.clock;
        let node = self.seats[index].node.as_mut()?;
        node.set_compliance_clock(clock);
        let code = act(node);
        self.record(self.seats[index].id, call, code);
        Some(code)
    }

    /// Moves every seat's queued outputs onto the wire. An output beyond
    /// the unicast send kind is an adapter bug under these rules.
    fn collect_outputs(&mut self) {
        for index in 0..self.seats.len() {
            let own = self.seats[index].id;
            let Some(node) = self.seats[index].node.as_mut() else {
                continue;
            };
            while let Some(out) = node.next_output() {
                assert!(
                    out.kind == lunet_advisory_lock::OUTPUT_SEND,
                    "the wire carries only unicast datagrams, got kind {} ({})",
                    out.kind,
                    lunet_advisory_lock::output_kind_name(out.kind)
                );
                self.wire.push_back((own, NodeId(out.to), out.bytes));
            }
        }
    }

    /// One whole exchange: the seats' queued outputs move onto the wire,
    /// then every queued frame is delivered, in queue order, to quiet.
    /// Each delivery may make its addressee emit, so the sweep repeats
    /// inside [`DRAIN_BOUND`]. A frame addressed to a down seat — or to an
    /// identity the scenario never seated — is recorded and skipped: it
    /// stays named in the multiset, which is how a scenario sees that a
    /// peer was gone. This is the compliance executor's own rule
    /// (`tests/compliance/mod.rs`, `deliver_all`), stated once.
    fn exchange(&mut self) -> Result<Vec<Emission>, String> {
        self.collect_outputs();
        let mut steps = 0usize;
        while let Some((from, to, bytes)) = self.wire.pop_front() {
            steps += 1;
            if steps > DRAIN_BOUND {
                return Err(format!(
                    "the drain did not quiet inside {DRAIN_BOUND} deliveries"
                ));
            }
            self.emissions.push(Emission {
                from: pair_of(from),
                to: pair_of(to),
                wire: compliance::hex_encode(&bytes),
            });
            if let Ok(index) = self.seat_index(to) {
                let _ = self.drive_one(index, "receive", |node| node.receive(from.0, &bytes));
            }
            self.collect_outputs();
        }
        Ok(Vec::new())
    }

    /// Whether the wire is quiet and every live seat reports `Normal`.
    fn quiet_and_normal(&self) -> bool {
        self.wire.is_empty()
            && self.seats.iter().all(|seat| {
                seat.node
                    .as_ref()
                    .is_none_or(|node| node.status().state == 0)
            })
    }
}

// ----------------------------------------------------------------------
// One inbound frame
// ----------------------------------------------------------------------

/// One inbound frame, fed to the seam by the test: the addressee, the
/// identity the bytes are attributed to, and the wire bytes exactly as
/// the sender emitted them.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Inbound {
    pub to: NodeId,
    pub from: NodeId,
    pub wire: Vec<u8>,
}

impl Inbound {
    /// One frame, from typed identities and wire bytes.
    #[must_use]
    pub fn new(to: NodeId, from: NodeId, wire: impl Into<Vec<u8>>) -> Inbound {
        Inbound {
            to,
            from,
            wire: wire.into(),
        }
    }

    /// One frame, from the corpus's own spelling, in the corpus's `Deliver`
    /// operation order — `(to, from, wire)`, addressee first. A pair or a
    /// hex string that names nothing panics at authoring time, never at
    /// assertion time.
    #[must_use]
    pub fn named(to: &str, from: &str, wire: &str) -> Inbound {
        let addressee =
            compliance::identity(to).unwrap_or_else(|e| panic!("the frame's `to`: {e}"));
        let sender =
            compliance::identity(from).unwrap_or_else(|e| panic!("the frame's `from`: {e}"));
        let bytes =
            compliance::hex_decode(wire).unwrap_or_else(|e| panic!("the frame's wire: {e}"));
        Inbound {
            to: addressee,
            from: sender,
            wire: bytes,
        }
    }

    /// The frame named the way an emission is, in the emission's own field
    /// order: sender, addressee, wire hex.
    #[must_use]
    pub fn name(&self) -> Emission {
        emission(self.from, self.to, &self.wire)
    }
}

// ----------------------------------------------------------------------
// Identities, naming, and the scratch tree
// ----------------------------------------------------------------------

/// The identity a provisioned roster member occupies: the packed pair
/// `system:counter`, one-indexed in both halves, the counter the first
/// life.
fn provisioned_id(index: usize) -> Result<NodeId, String> {
    compliance::identity(&format!("{}:1", index + 1))
}

/// A provisioned member's descriptor name.
fn member_name(index: usize) -> String {
    format!("n{}", index + 1)
}

/// The genesis member buffer a provisioned roster's descriptor carries:
/// `<u32-id>:<name>` per member, NUL-separated, in succession order.
fn genesis_of(nodes: usize) -> Result<String, String> {
    (0..nodes)
        .map(|index| {
            let id = provisioned_id(index)?;
            Ok(format!("{}:{}", id.0, member_name(index)))
        })
        .collect::<Result<Vec<_>, String>>()
        .map(|members| members.join("\0"))
}

/// The one-past-the-end identity that names the outside gossip sender.
fn outside_sender(nodes: usize) -> Result<NodeId, String> {
    compliance::identity(&format!("{}:1", nodes + 1))
}

/// This seam's scratch tree, inside the repo (`.tmp` is scratch).
fn scratch_root() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/seam")
}

/// A scenario's private directory, created empty. The name carries the
/// including test binary and a process-wide counter, so two scenarios in
/// one binary and two binaries in one `cargo test` run never share a
/// marker store; a directory left by an earlier run is cleared first, so
/// a scenario never boots over its own predecessor's state.
fn fresh_dir(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = scratch_root();
    fs::create_dir_all(&root).expect("the seam's scratch root creates");
    let owner = module_path!().replace("::", "-");
    let dir = root.join(format!(
        "{name}-{owner}-{}-{}",
        std::env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0".into()),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    if dir.exists() {
        fs::remove_dir_all(&dir).expect("a stale scenario directory clears");
    }
    fs::create_dir_all(&dir).expect("the scenario directory creates");
    dir
}

/// The items of `captured` that the named multiset does not account for,
/// counted: the same name twice is only beyond the multiset once it
/// appears a second time.
fn beyond<'a, T: PartialEq>(captured: &[&'a T], named: &[&'a T]) -> Vec<&'a T> {
    let mut unaccounted: Vec<&T> = named.to_vec();
    let mut extra: Vec<&T> = Vec::new();
    for item in captured.iter().copied() {
        match unaccounted
            .iter()
            .position(|candidate| **candidate == *item)
        {
            Some(at) => {
                unaccounted.remove(at);
            }
            None => extra.push(item),
        }
    }
    extra
}

/// Names to put in a failure message.
fn describe_all(emissions: &[&Emission]) -> String {
    if emissions.is_empty() {
        return "none".into();
    }
    emissions
        .iter()
        .map(|e| format!("{}→{} {}", e.from, e.to, e.wire))
        .collect::<Vec<_>>()
        .join(", ")
}
