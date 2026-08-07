//! Item 10: deterministic in-process multi-replica cluster safety harness.
//!
//! A cluster of public-API `Replica`s talks through a controllable bus. The
//! bus supports delivery, drop, duplication, reorder, and a simple
//! split-point partition (cross-partition traffic is withheld until healed,
//! and may be dropped while partitioned). Host execution completions are
//! deterministic functions of the executed entry. All scheduling is seeded
//! (SplitMix64), so every schedule replays bit-identically.
//!
//! Safety invariant, checked after EVERY harness step (every message
//! delivery, every host input, every fault action — never only at the end):
//! committed-slot agreement — no two replicas execute different entries at
//! the same slot. Since a replica executes its log in slot order, this is
//! pairwise equality of the executed log prefixes, plus the per-replica
//! sanity that only committed entries ever execute (`executed <= commit`).
//!
//! Progress is checked in a fair (lossless, immediately completing)
//! schedule for K=3 and K=4; safety is checked under finite seeded
//! loss/reorder/duplicate/partition schedules for both cluster sizes.

mod support;

use std::collections::BTreeMap;

use proptest::prelude::*;
use proptest::test_runner::FileFailurePersistence;
use support::{node, ReplicaSnapshot, EXECUTION_TIME};
use uuid::Uuid;
use vrr::vrr::{Input, Message, NodeId, Output, Replica, Slot, Status};

const FAIR_REQUESTS: u64 = 8;
const FAIR_ROUND_CAP: usize = 100;
const SEEDED_SEEDS: u64 = 24;
const SEEDED_STEPS: usize = 1_500;
const SEEDED_MAX_REQUESTS: u64 = 12;
const REPLAY_SEEDS: u64 = 4;
const STABILIZE_ROUNDS: usize = 200;
const DRAIN_CAP: usize = 10_000;

/// SplitMix64: small, platform-independent, fully seeded.
struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: usize) -> usize {
        assert!(bound > 0, "Rng::below requires a positive bound");
        (self.next() % bound as u64) as usize
    }
}

#[derive(Clone, Debug)]
struct Envelope {
    from: NodeId,
    to: NodeId,
    message: Message,
}

/// Observable run record used for the determinism replay comparison and for
/// progress assertions: completed executions in completion order (with the
/// deterministic result bytes the host computed) and every client reply.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Transcript {
    executions: Vec<(NodeId, Slot, Vec<u8>)>,
    replies: Vec<(NodeId, Vec<u8>)>,
}

/// The host computes results as a pure function of the committed entry, so
/// two replicas executing the same entry compute the same result and a
/// replayed schedule reproduces the same bytes.
fn deterministic_result(slot: Slot, payload: &[u8]) -> Vec<u8> {
    let mut result = format!("result-{slot}:").into_bytes();
    result.extend_from_slice(payload);
    result
}

struct Cluster {
    replicas: Vec<Replica>,
    bus: Vec<Envelope>,
    /// Simple partition: `Some(split)` withholds every envelope whose
    /// endpoints lie on different sides of `split` until healed.
    partition: Option<NodeId>,
    /// In-flight host executions: node -> (slot, payload) of the latest
    /// `Output::Execute`. A suffix rollback clears the replica's marker and
    /// may re-issue the same slot with a different entry; only the latest
    /// issued execution of a slot is ever completed (the host abandons the
    /// stale one), mirroring the core's single in-flight marker.
    pending: BTreeMap<NodeId, (Slot, Vec<u8>)>,
    transcript: Transcript,
    requests_sent: u64,
    nonce: u64,
    steps: u64,
    actions: BTreeMap<&'static str, u64>,
}

impl Cluster {
    fn new(k: usize) -> Self {
        assert!(k >= 3, "cluster size must be at least three");
        Self {
            replicas: (0..k).map(|index| node(k, index)).collect(),
            bus: Vec::new(),
            partition: None,
            pending: BTreeMap::new(),
            transcript: Transcript::default(),
            requests_sent: 0,
            nonce: 0,
            steps: 0,
            actions: BTreeMap::new(),
        }
    }

    fn count(&mut self, action: &'static str) {
        *self.actions.entry(action).or_insert(0) += 1;
    }

    fn eligible(&self, envelope: &Envelope) -> bool {
        match self.partition {
            None => true,
            Some(split) => (envelope.from < split) == (envelope.to < split),
        }
    }

    /// Routes one replica's outputs: peer messages onto the bus, executions
    /// to the host pending slot, replies to the transcript.
    fn route(&mut self, from: NodeId, outputs: Vec<Output>) {
        for output in outputs {
            match output {
                Output::Broadcast(message) => {
                    for to in 0..self.replicas.len() as NodeId {
                        if to != from {
                            self.bus.push(Envelope {
                                from,
                                to,
                                message: message.clone(),
                            });
                        }
                    }
                }
                Output::To(to, message) => self.bus.push(Envelope { from, to, message }),
                Output::Execute { slot, payload, .. } => {
                    self.pending.insert(from, (slot, payload));
                }
                Output::Reply(bytes) => self.transcript.replies.push((from, bytes)),
            }
        }
    }

    /// Drops a pending execution the replica no longer tracks (completed
    /// elsewhere, or abandoned by an adoption that cleared the marker; the
    /// replica re-emits `Output::Execute` if the slot still needs running).
    fn reconcile(&mut self, node: NodeId) {
        let marker = self.replicas[node as usize].diagnostic().executing;
        if let Some((slot, _)) = self.pending.get(&node) {
            if marker != Some(*slot) {
                self.pending.remove(&node);
            }
        }
    }

    /// Every harness step funnels here: the safety invariant is re-checked
    /// after each single message delivery, host input, or fault action.
    fn after_step(&mut self) {
        self.steps += 1;
        self.check_safety();
    }

    /// Committed-slot agreement: no two replicas may execute different
    /// entries at the same slot. Executed slots are a prefix of the log
    /// (the core executes strictly in order), so pairwise prefix equality
    /// over `min(executed)` is exactly the invariant. Also asserts the
    /// per-replica sanity that only committed entries ever execute.
    fn check_safety(&self) {
        for (index, replica) in self.replicas.iter().enumerate() {
            assert!(
                replica.executed() <= replica.commit(),
                "step {}: node {index} executed {} beyond its commit {}",
                self.steps,
                replica.executed(),
                replica.commit()
            );
            assert!(
                replica.executed() as usize <= replica.log().len(),
                "step {}: node {index} executed {} outside its log of {} entries",
                self.steps,
                replica.executed(),
                replica.log().len()
            );
        }
        for left in 0..self.replicas.len() {
            for right in (left + 1)..self.replicas.len() {
                let (a, b) = (&self.replicas[left], &self.replicas[right]);
                let common = a.executed().min(b.executed()) as usize;
                for (index, (entry_a, entry_b)) in
                    a.log().iter().zip(b.log().iter()).take(common).enumerate()
                {
                    let slot = index as u64 + 1;
                    assert_eq!(
                        entry_a, entry_b,
                        "step {}: committed-slot disagreement: node {left} and node {right} \
                         executed different entries at slot {slot}",
                        self.steps
                    );
                }
            }
        }
    }

    /// Steps one replica, routes its outputs, reconciles the host pending
    /// slot, and re-checks safety. The single funnel for every step.
    fn step_replica(&mut self, node: NodeId, input: Input) {
        let outputs = self.replicas[node as usize].step(input);
        self.route(node, outputs);
        self.reconcile(node);
        self.after_step();
    }

    /// Delivers one eligible in-flight envelope, chosen by the schedule.
    fn deliver(&mut self, rng: &mut Rng) {
        self.count("deliver");
        let eligible: Vec<usize> = (0..self.bus.len())
            .filter(|&index| self.eligible(&self.bus[index]))
            .collect();
        if eligible.is_empty() {
            self.after_step();
            return;
        }
        let pick = eligible[rng.below(eligible.len())];
        let envelope = self.bus.remove(pick);
        self.step_replica(
            envelope.to,
            Input::Message {
                from: envelope.from,
                message: envelope.message,
            },
        );
    }

    /// Drops one in-flight envelope (any, including cross-partition traffic
    /// lost while partitioned).
    fn drop_envelope(&mut self, rng: &mut Rng) {
        self.count("drop");
        if !self.bus.is_empty() {
            let index = rng.below(self.bus.len());
            self.bus.remove(index);
        }
        self.after_step();
    }

    /// Duplicates one in-flight envelope; the copy is delivered independently.
    fn duplicate_envelope(&mut self, rng: &mut Rng) {
        self.count("duplicate");
        if !self.bus.is_empty() {
            let envelope = self.bus[rng.below(self.bus.len())].clone();
            self.bus.push(envelope);
        }
        self.after_step();
    }

    /// Reorders the bus by swapping two in-flight envelopes.
    fn reorder_bus(&mut self, rng: &mut Rng) {
        self.count("reorder");
        if self.bus.len() >= 2 {
            let first = rng.below(self.bus.len());
            let mut second = rng.below(self.bus.len() - 1);
            if second >= first {
                second += 1;
            }
            self.bus.swap(first, second);
        }
        self.after_step();
    }

    /// Injects the next client request at the leader of the highest epoch
    /// any replica currently reports (a client that keeps asking finds the
    /// leader; refusals when the cluster disagrees are protocol-legal and
    /// simply waste the request number, which stays globally unique).
    fn request(&mut self, rng: &mut Rng, max_requests: u64) {
        if self.requests_sent >= max_requests {
            self.deliver(rng);
            return;
        }
        self.count("request");
        self.requests_sent += 1;
        let n = self.requests_sent;
        let max_epoch = self
            .replicas
            .iter()
            .map(|replica| replica.epoch())
            .max()
            .expect("nonempty cluster");
        let target = max_epoch % self.replicas.len() as NodeId;
        self.step_replica(
            target,
            Input::Request {
                client_id: 1,
                request_num: n,
                message_id: Uuid::from_u128(u128::from(n)),
                execution_time: EXECUTION_TIME,
                payload: format!("seeded-request-{n}").into_bytes(),
            },
        );
    }

    /// Nodes with an in-flight execution whose host may legally complete it
    /// (completions are refused outright while `Status::Recovering`).
    fn completable(&self) -> Vec<NodeId> {
        self.pending
            .keys()
            .copied()
            .filter(|&node| self.replicas[node as usize].status() != Status::Recovering)
            .collect()
    }

    /// Completes the in-flight execution on `node` with the deterministic
    /// result for that entry, records it in the transcript, and asserts the
    /// completion was accepted (a pending entry on a completable node always
    /// satisfies the core's completion guard: marker set, `executed + 1 ==
    /// slot`, `slot <= commit`, status not `Recovering`).
    fn complete_one(&mut self, node: NodeId) {
        let (slot, payload) = self.pending[&node].clone();
        let result = deterministic_result(slot, &payload);
        self.step_replica(
            node,
            Input::Complete {
                slot,
                result: result.clone(),
            },
        );
        let replica = &self.replicas[node as usize];
        assert!(
            replica.diagnostic().executing != Some(slot),
            "step {}: completion of slot {slot} on node {node} refused while its marker persists",
            self.steps
        );
        assert_eq!(
            replica.executed(),
            slot,
            "step {}: accepted completion did not advance node {node} to slot {slot}",
            self.steps
        );
        self.transcript.executions.push((node, slot, result));
    }

    /// Completes one schedule-chosen in-flight execution.
    fn complete(&mut self, rng: &mut Rng) {
        self.count("complete");
        let candidates = self.completable();
        if candidates.is_empty() {
            self.after_step();
            return;
        }
        let node = candidates[rng.below(candidates.len())];
        self.complete_one(node);
    }

    /// Steps `Input::Idle` on a schedule-chosen node (the leader heartbeat;
    /// a no-op everywhere else).
    fn idle(&mut self, rng: &mut Rng) {
        self.count("idle");
        let node = rng.below(self.replicas.len()) as NodeId;
        self.step_replica(node, Input::Idle);
    }

    /// Steps `Input::LeaderTimeout` on a schedule-chosen node.
    fn timeout(&mut self, rng: &mut Rng) {
        self.count("timeout");
        let node = rng.below(self.replicas.len()) as NodeId;
        self.step_replica(node, Input::LeaderTimeout);
    }

    /// Starts a host recovery attempt on a schedule-chosen node with a
    /// globally unique nonce, under a sane rolling-restart host policy: the
    /// core provably requires a response from every other replica
    /// (`four_node_recovery_requires_three_other_responders`) and only
    /// answers probes while `Status::Normal`, so the host never starts an
    /// attempt while another replica is still recovering — that would be
    /// rebooting the whole cluster at once, a terminal liveness state of
    /// the host's own making, not protocol behaviour under test.
    fn recover(&mut self, rng: &mut Rng) {
        self.count("recover");
        let node = rng.below(self.replicas.len()) as NodeId;
        let another_recovering = self.replicas.iter().enumerate().any(|(index, replica)| {
            index != node as usize && replica.status() == Status::Recovering
        });
        if another_recovering {
            self.after_step();
            return;
        }
        self.nonce += 1;
        self.step_replica(node, Input::Recover { nonce: self.nonce });
    }

    /// Cuts a simple split-point partition, or heals the existing one.
    fn partition_or_heal(&mut self, rng: &mut Rng) {
        if self.partition.is_some() {
            self.count("heal");
            self.partition = None;
        } else {
            self.count("partition");
            let split = 1 + rng.below(self.replicas.len() - 1) as NodeId;
            self.partition = Some(split);
        }
        self.after_step();
    }

    /// Delivers eligible envelopes until none remain (used by the fair
    /// schedule and the post-chaos convergence tail, where everything is
    /// eligible). Capped so a non-converging protocol would fail loudly.
    fn drain_bus(&mut self) {
        let mut deliveries = 0;
        loop {
            let next = (0..self.bus.len()).find(|&index| self.eligible(&self.bus[index]));
            let Some(index) = next else { break };
            let envelope = self.bus.remove(index);
            self.step_replica(
                envelope.to,
                Input::Message {
                    from: envelope.from,
                    message: envelope.message,
                },
            );
            deliveries += 1;
            assert!(
                deliveries <= DRAIN_CAP,
                "step {}: bus drain did not converge",
                self.steps
            );
        }
    }

    /// Completes in-flight executions until none remain completable.
    fn drain_executions(&mut self) {
        loop {
            let next = self.completable().first().copied();
            let Some(node) = next else { break };
            self.complete_one(node);
        }
    }

    /// Fair schedule: no faults, immediate completion, leader heartbeat
    /// every round. Drives `total` requests through the epoch-0 leader and
    /// returns once every replica has executed every slot. Asserts the
    /// schedule actually converges (progress).
    fn run_fair(&mut self, total: u64) {
        let leader: NodeId = 0;
        let mut sent = 0;
        for round in 0..FAIR_ROUND_CAP {
            if sent < total {
                sent += 1;
                self.step_replica(
                    leader,
                    Input::Request {
                        client_id: 1,
                        request_num: sent,
                        message_id: Uuid::from_u128(u128::from(sent)),
                        execution_time: EXECUTION_TIME,
                        payload: format!("fair-request-{sent}").into_bytes(),
                    },
                );
            }
            self.drain_bus();
            self.drain_executions();
            self.step_replica(leader, Input::Idle);
            self.drain_bus();
            self.drain_executions();
            if sent == total
                && self
                    .replicas
                    .iter()
                    .all(|replica| replica.executed() == total)
            {
                self.requests_sent = sent;
                return;
            }
            assert!(
                round + 1 < FAIR_ROUND_CAP,
                "fair schedule failed to converge; replica frontiers: {:?}",
                self.replicas
                    .iter()
                    .map(|replica| (
                        replica.epoch(),
                        replica.status(),
                        replica.slot(),
                        replica.commit(),
                        replica.executed()
                    ))
                    .collect::<Vec<_>>()
            );
        }
    }

    /// Every replica is `Status::Normal` at one shared epoch.
    fn converged(&self) -> bool {
        let epoch = self.replicas[0].epoch();
        self.replicas
            .iter()
            .all(|replica| replica.status() == Status::Normal && replica.epoch() == epoch)
    }

    /// Fully settled: converged, nothing in flight, and identical log,
    /// slot, commit, and executed frontiers on every replica. (With the bus
    /// empty and no execution pending, equal `commit` implies equal
    /// `executed`, checked explicitly anyway.)
    fn quiesced(&self) -> bool {
        if !self.converged() || !self.bus.is_empty() || !self.pending.is_empty() {
            return false;
        }
        let first = &self.replicas[0];
        self.replicas.iter().all(|replica| {
            replica.slot() == first.slot()
                && replica.commit() == first.commit()
                && replica.executed() == first.executed()
                && replica.log() == first.log()
        })
    }

    /// Post-chaos stabilization tail: heal, then drive the cluster to one
    /// quiesced epoch — drain everything, complete executions, re-drive
    /// stuck host recovery attempts with fresh nonces (a legitimate host
    /// retry), and ratchet the cluster epoch forward with a leader timeout
    /// on the lagging non-recovering replica. Returns `true` once quiesced.
    /// Returns `false` if every replica ended up in `Status::Recovering`
    /// (unreachable under the driver's rolling-restart host policy, kept as
    /// a defensive early-out) so no schedule of host inputs can progress.
    /// Safety was still checked after every step on the way there.
    fn stabilize(&mut self) -> bool {
        self.partition = None;
        for _round in 0..STABILIZE_ROUNDS {
            self.drain_bus();
            self.drain_executions();
            for node in 0..self.replicas.len() as NodeId {
                if self.replicas[node as usize].status() == Status::Recovering {
                    self.nonce += 1;
                    let nonce = self.nonce;
                    self.step_replica(node, Input::Recover { nonce });
                }
            }
            // Heartbeat every round: the current leader's Commit broadcast
            // is the only way backups learn a suffix commit the leader
            // reached from their own post-StartEpoch PrepareOks.
            for node in 0..self.replicas.len() as NodeId {
                self.step_replica(node, Input::Idle);
            }
            self.drain_bus();
            self.drain_executions();
            if self.quiesced() {
                return true;
            }
            let Some(laggard) = (0..self.replicas.len() as NodeId)
                .filter(|&node| {
                    !matches!(
                        self.replicas[node as usize].status(),
                        Status::Recovering | Status::Replaying
                    )
                })
                .min_by_key(|&node| (self.replicas[node as usize].epoch(), node))
            else {
                return false; // every replica is recovering: terminal stall
            };
            self.step_replica(laggard, Input::LeaderTimeout);
        }
        panic!(
            "cluster did not stabilize; frontiers: {:?}",
            self.replicas
                .iter()
                .map(|replica| (
                    replica.epoch(),
                    replica.status(),
                    replica.slot(),
                    replica.commit(),
                    replica.executed()
                ))
                .collect::<Vec<_>>()
        );
    }

    /// One finite seeded schedule: `steps` weighted random actions mixing
    /// delivery with drop/duplicate/reorder/partition faults, host
    /// completions, client requests, leader timeouts, and recoveries; then
    /// a stabilization tail. Safety is asserted after every action inside
    /// `step_replica`/`after_step`. Returns whether the cluster stabilized.
    fn run_seeded(&mut self, seed: u64, steps: usize, max_requests: u64) -> bool {
        let mut rng = Rng::new(
            seed.wrapping_mul(0xA24B_AED4_963E_E407)
                .wrapping_add(self.replicas.len() as u64),
        );
        for _ in 0..steps {
            match rng.below(100) {
                0..=44 => self.deliver(&mut rng),
                45..=59 => self.complete(&mut rng),
                60..=67 => self.request(&mut rng, max_requests),
                68..=72 => self.idle(&mut rng),
                73..=76 => self.drop_envelope(&mut rng),
                77..=80 => self.duplicate_envelope(&mut rng),
                81..=84 => self.reorder_bus(&mut rng),
                85..=88 => self.timeout(&mut rng),
                89..=90 => self.recover(&mut rng),
                91..=96 => self.partition_or_heal(&mut rng),
                _ => self.deliver(&mut rng),
            }
        }
        self.stabilize()
    }
}

/// Progress in a fair schedule: every replica commits and executes every
/// request, all logs are identical, executions happen in slot order with
/// deterministic results, and the leader replies once per request.
fn fair_case(k: usize) {
    let mut cluster = Cluster::new(k);
    cluster.run_fair(FAIR_REQUESTS);

    let reference = cluster.replicas[0].log().to_vec();
    assert_eq!(reference.len() as u64, FAIR_REQUESTS);
    for (index, replica) in cluster.replicas.iter().enumerate() {
        assert_eq!(replica.commit(), FAIR_REQUESTS, "node {index} commit");
        assert_eq!(replica.executed(), FAIR_REQUESTS, "node {index} executed");
        assert_eq!(replica.log(), reference.as_slice(), "node {index} log");
    }

    for node in 0..k as NodeId {
        let slots: Vec<Slot> = cluster
            .transcript
            .executions
            .iter()
            .filter(|(executor, _, _)| *executor == node)
            .map(|(_, slot, _)| *slot)
            .collect();
        assert_eq!(
            slots,
            (1..=FAIR_REQUESTS).collect::<Vec<_>>(),
            "node {node} execution order"
        );
        for slot in 1..=FAIR_REQUESTS {
            let entry = &reference[(slot - 1) as usize];
            let expected = deterministic_result(slot, &entry.payload);
            assert!(
                cluster
                    .transcript
                    .executions
                    .contains(&(node, slot, expected.clone())),
                "node {node} missing deterministic execution of slot {slot}"
            );
        }
    }

    let replies: Vec<&Vec<u8>> = cluster
        .transcript
        .replies
        .iter()
        .filter(|(responder, _)| *responder == 0)
        .map(|(_, bytes)| bytes)
        .collect();
    let expected: Vec<Vec<u8>> = (1..=FAIR_REQUESTS)
        .map(|n| deterministic_result(n, format!("fair-request-{n}").as_bytes()))
        .collect();
    assert_eq!(
        replies.len(),
        expected.len(),
        "leader reply count must equal the request count"
    );
    for (reply, expected) in replies.iter().zip(expected.iter()) {
        assert_eq!(*reply, expected, "leader reply bytes");
    }
}

/// Safety under finite seeded loss/reorder/duplicate/partition schedules:
/// the per-step invariant inside the driver is the assertion; this campaign
/// additionally proves the schedules are non-vacuous (work executed) and
/// that every fault kind was actually exercised.
fn seeded_campaign_case(k: usize) {
    let mut totals: BTreeMap<&'static str, u64> = BTreeMap::new();
    let mut executions = 0;
    let mut stabilized = 0;
    let mut seeds_with_progress = 0;
    for seed in 0..SEEDED_SEEDS {
        let mut cluster = Cluster::new(k);
        if cluster.run_seeded(seed, SEEDED_STEPS, SEEDED_MAX_REQUESTS) {
            stabilized += 1;
        }
        executions += cluster.transcript.executions.len();
        if !cluster.transcript.executions.is_empty() {
            seeds_with_progress += 1;
        }
        for (action, count) in cluster.actions {
            *totals.entry(action).or_insert(0) += count;
        }
    }
    assert_eq!(
        stabilized, SEEDED_SEEDS,
        "every seeded schedule must stabilize after healing"
    );
    assert!(
        seeds_with_progress >= SEEDED_SEEDS * 3 / 4,
        "only {seeds_with_progress}/{SEEDED_SEEDS} seeded schedules executed any committed entry"
    );
    assert!(
        executions >= SEEDED_SEEDS as usize * 2,
        "seeded campaign was nearly vacuous: only {executions} executions across {SEEDED_SEEDS} seeds"
    );
    for action in [
        "deliver",
        "drop",
        "duplicate",
        "reorder",
        "partition",
        "heal",
        "complete",
        "request",
        "idle",
        "timeout",
        "recover",
    ] {
        assert!(
            totals.get(action).copied().unwrap_or(0) > 0,
            "seeded campaign never exercised action kind {action}"
        );
    }
}

fn replay_once(k: usize, seed: u64) -> (bool, Transcript, Vec<ReplicaSnapshot>) {
    let mut cluster = Cluster::new(k);
    let stabilized = cluster.run_seeded(seed, SEEDED_STEPS, SEEDED_MAX_REQUESTS);
    let snapshots = cluster
        .replicas
        .iter()
        .map(ReplicaSnapshot::capture)
        .collect();
    (stabilized, cluster.transcript, snapshots)
}

#[test]
fn fair_schedule_delivers_commits_executes_and_replies_every_request_k3() {
    fair_case(3);
}

#[test]
fn fair_schedule_delivers_commits_executes_and_replies_every_request_k4() {
    fair_case(4);
}

#[test]
fn seeded_loss_reorder_duplicate_partition_schedules_preserve_slot_agreement_k3() {
    seeded_campaign_case(3);
}

#[test]
fn seeded_loss_reorder_duplicate_partition_schedules_preserve_slot_agreement_k4() {
    seeded_campaign_case(4);
}

/// Determinism: the same seed replays bit-identically — same execution
/// transcript, same reply transcript, and the same complete diagnostic
/// state on every replica.
#[test]
fn seeded_schedules_replay_bit_identically() {
    for k in [3usize, 4] {
        for seed in 0..REPLAY_SEEDS {
            let first = replay_once(k, seed);
            let second = replay_once(k, seed);
            assert_eq!(first, second, "seed {seed} K={k} replay diverged");
        }
    }
}

// ── Bounded randomized cluster schedule companions (Item 14) ──────────

const PROPTEST_CASES: u32 = 64;
const PROPTEST_MAX_STEPS: usize = 500;

#[derive(Clone, Copy, Debug)]
enum ScheduleAction {
    Deliver,
    Complete,
    Request,
    Idle,
    Drop,
    Duplicate,
    Reorder,
    Timeout,
    Recover,
    PartitionOrHeal,
}

fn schedule_action() -> impl Strategy<Value = ScheduleAction> {
    prop_oneof![
        35 => Just(ScheduleAction::Deliver),
        13 => Just(ScheduleAction::Complete),
        7  => Just(ScheduleAction::Request),
        4  => Just(ScheduleAction::Idle),
        3  => Just(ScheduleAction::Drop),
        3  => Just(ScheduleAction::Duplicate),
        3  => Just(ScheduleAction::Reorder),
        3  => Just(ScheduleAction::Timeout),
        2  => Just(ScheduleAction::Recover),
        5  => Just(ScheduleAction::PartitionOrHeal),
    ]
}

fn schedule_sequence() -> impl Strategy<Value = Vec<ScheduleAction>> {
    prop::collection::vec(schedule_action(), 0..=PROPTEST_MAX_STEPS)
}

fn run_proptest_schedule(cluster: &mut Cluster, actions: &[ScheduleAction], rng: &mut Rng) {
    for &action in actions {
        match action {
            ScheduleAction::Deliver => cluster.deliver(rng),
            ScheduleAction::Complete => cluster.complete(rng),
            ScheduleAction::Request => cluster.request(rng, PROPTEST_MAX_STEPS as u64),
            ScheduleAction::Idle => cluster.idle(rng),
            ScheduleAction::Drop => cluster.drop_envelope(rng),
            ScheduleAction::Duplicate => cluster.duplicate_envelope(rng),
            ScheduleAction::Reorder => cluster.reorder_bus(rng),
            ScheduleAction::Timeout => cluster.timeout(rng),
            ScheduleAction::Recover => cluster.recover(rng),
            ScheduleAction::PartitionOrHeal => cluster.partition_or_heal(rng),
        }
    }
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: PROPTEST_CASES,
        failure_persistence: Some(Box::new(FileFailurePersistence::SourceParallel(
            "proptest-regressions",
        ))),
        .. ProptestConfig::default()
    })]
    #[test]
    fn bounded_cluster_schedules_preserve_slot_agreement_and_stabilize(
        k in (3usize..=4usize),
        seed in any::<u64>(),
        actions in schedule_sequence(),
    ) {
        let mut cluster = Cluster::new(k);
        let mut rng = Rng::new(seed);
        run_proptest_schedule(&mut cluster, &actions, &mut rng);
        assert!(
            cluster.stabilize(),
            "K={k} seed={seed} did not stabilize after {actions_len} actions; frontiers: {frontiers:?}",
            actions_len = actions.len(),
            frontiers = cluster.replicas.iter().map(|replica| (
                replica.epoch(), replica.status(), replica.slot(),
                replica.commit(), replica.executed()
            )).collect::<Vec<_>>()
        );
    }
}
