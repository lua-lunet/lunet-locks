//! The host's relay: the ledger of un-acknowledged traffic and the
//! flavoured-timeout opinions drawn from it (`docs/src/timeout-policy.md`).
//!
//! The core releases a message once and never re-releases it: a repeated
//! send is a relay of what the core already put on the wire, never a
//! message the host composes (upstream `uvrr-core/docs/architecture.md`,
//! "Liveness: the resend, the heartbeat, and the retransmit"). This module
//! is that host half, and nothing else:
//!
//! - [`Ledger`] remembers every datagram this node released that the
//!   protocol gives a response to, and drops the ones a response answered.
//!   The relay is a transport replay of those exact bytes to the same
//!   peers; the ledger never mints a message.
//! - [`cluster_timeout`] and [`poll_opinion`] ask the matcher
//!   (`vrr::timeout::matcher`) what a node may do when a wait expires,
//!   and the host obeys the answer. No host constant restates a
//!   protocol opinion.
//!
//! Two waits draw on the ledger. The cluster wait (the quorum response a
//! seated member awaits) relays everything it has not received a response
//! on: phase 1's `Prepare` and phase 2's `Commit` from a leader, and a
//! view change's `StartViewChange` fence votes and `DoViewChange`
//! evidence from any member. The steady quiet of a leader with nothing
//! outstanding takes the heartbeat option, which is the leader's LAST
//! COMMIT re-announced — the frontier announcement and the proof of life
//! in one, and no new slot.

use vrr::timeout::{Opinion, State, Timeout, matcher};
use vrr::wire::Tag;

/// One datagram the core released, kept for the relay: the peer it was
/// addressed to, the ballot that authorised it, the slot its header
/// carries, the tag, and the exact bytes the network carried. The relay
/// re-sends these and nothing else, so a relayed datagram is
/// byte-identical to the datagram it repeats.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Datagram {
    /// The member the datagram was addressed to.
    pub to: u32,
    /// The era the header was authorised at.
    pub era: u32,
    /// The view the header was authorised at.
    pub view: u32,
    /// The slot the header speaks at.
    pub slot: u64,
    /// The header's tag, decoded through the core's own table.
    pub tag: Tag,
    /// The datagram, exactly as the core released it.
    pub bytes: Vec<u8>,
}

impl Datagram {
    /// The relay's own name for one datagram, for a failure message: the
    /// peer, the ballot and the slot, never a bare count.
    #[must_use]
    pub fn describe(&self) -> String {
        format!(
            "{} {}:{} slot {} to {}",
            self.tag.name(),
            self.era,
            self.view,
            self.slot,
            self.to
        )
    }
}

/// The tags whose repeats the relay carries, and the evidence each one is
/// answered by. The four classes are exactly the ones the resend policy
/// names: phase 1 (`Prepare`), phase 2 (`Commit`), the ordinary fence
/// (`StartViewChange`) and the primary's evidence request
/// (`DoViewChange`). Everything else the core releases is either already
/// final (a `StartView` the cluster has installed) or answered by the
/// core's own re-release, and relaying it would put a stale ballot on
/// the wire for no gain.
fn relayable(tag: Tag) -> bool {
    matches!(
        tag,
        Tag::Prepare | Tag::Commit | Tag::StartViewChange | Tag::DoViewChange
    )
}

/// Whether one inbound datagram is the response that retires a ledger
/// entry. The evidence is the protocol's own, never an inference from
/// silence:
///
/// - a `Prepare` is answered by its recipient's `PrepareOk` at the same
///   slot or later (a replica acknowledges a slot only once it holds
///   every slot before it);
/// - a `Commit` carries no acknowledgement of its own, and the
///   recipient's `PrepareOk` at a LATER slot is the proof it applied
///   through this one;
/// - a `StartViewChange` fence vote is answered by the recipient's own
///   fence vote, which the receiver counts once however often it
///   repeats;
/// - `DoViewChange` evidence is answered by the designated primary
///   installing a view of this era, which retires the attempt whatever
///   became of the evidence itself.
///
/// A response from a different peer, or at a ballot the entry was not
/// authorised at, answers nothing.
fn answers(entry: &Datagram, from: u32, era: u32, view: u32, slot: u64, tag: Tag) -> bool {
    if entry.to != from || entry.era != era {
        return false;
    }
    match entry.tag {
        Tag::Prepare => tag == Tag::PrepareOk && view == entry.view && slot >= entry.slot,
        Tag::Commit => tag == Tag::PrepareOk && view == entry.view && slot > entry.slot,
        Tag::StartViewChange => tag == Tag::StartViewChange && view == entry.view,
        Tag::DoViewChange => tag == Tag::StartView && view >= entry.view,
        _ => false,
    }
}

/// The ledger of un-acknowledged traffic: every relayable datagram this
/// node released and no response has retired, bounded so a long partition
/// cannot grow it without limit. The bound drops the OLDEST entries —
/// the relay covers the recent window, and a datagram nobody answered
/// after that many successors is traffic the cluster has moved past.
pub struct Ledger {
    unacked: Vec<Datagram>,
    /// The leader's last commit, kept whether or not a response has
    /// retired it: the heartbeat re-announces the frontier, and a
    /// frontier every replica already holds is still the frontier. One
    /// ballot's worth of peers, replaced whole on each new commit.
    last_commit: Vec<Datagram>,
    bound: usize,
    /// How many entries the bound has dropped, for the operator's line.
    dropped: u64,
}

/// The default bound: a window far wider than any quorum response the
/// host's own timeouts allow, so a healthy cluster never reaches it.
const DEFAULT_BOUND: usize = 256;

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

impl Ledger {
    /// An empty ledger with the default bound.
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(DEFAULT_BOUND)
    }

    /// An empty ledger that keeps at most `bound` entries.
    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            unacked: Vec::new(),
            last_commit: Vec::new(),
            bound: bound.max(1),
            dropped: 0,
        }
    }

    /// Remember one datagram the node released. A tag outside the four
    /// relayable classes is not the relay's business and is not kept. A
    /// repeat of a datagram already standing (the core released the same
    /// peer, ballot, slot and tag again) replaces it rather than
    /// doubling it, so one outstanding request is one relay.
    pub fn record(&mut self, datagram: Datagram) {
        if !relayable(datagram.tag) {
            return;
        }
        if datagram.tag == Tag::Commit {
            self.remember_commit(datagram.clone());
        }
        if let Some(standing) = self.unacked.iter_mut().find(|entry| {
            entry.to == datagram.to
                && entry.era == datagram.era
                && entry.view == datagram.view
                && entry.slot == datagram.slot
                && entry.tag == datagram.tag
        }) {
            *standing = datagram;
            return;
        }
        if self.unacked.len() >= self.bound {
            self.unacked.remove(0);
            self.dropped += 1;
        }
        self.unacked.push(datagram);
    }

    /// The leader's frontier: the newest commit released per peer. A
    /// commit at a newer ballot replaces the whole frontier — the
    /// previous ballot's commit is history, not this leader's frontier.
    fn remember_commit(&mut self, datagram: Datagram) {
        if let Some(standing) = self.last_commit.first()
            && (standing.era, standing.view) != (datagram.era, datagram.view)
        {
            self.last_commit.clear();
        }
        match self
            .last_commit
            .iter_mut()
            .find(|entry| entry.to == datagram.to)
        {
            Some(standing) => *standing = datagram,
            None => self.last_commit.push(datagram),
        }
    }

    /// One inbound datagram arrived: retire every entry its tag answers.
    pub fn answered(&mut self, from: u32, era: u32, view: u32, slot: u64, tag: Tag) {
        if !relayable(tag) && !matches!(tag, Tag::PrepareOk | Tag::StartView) {
            return;
        }
        self.unacked
            .retain(|entry| !answers(entry, from, era, view, slot, tag));
    }

    /// The relay set for the ballot this node is in: every un-answered
    /// datagram authorised at exactly `(era, view)`, in send order. An
    /// entry from an earlier ballot is dead — the attempt it belonged to
    /// is over, and a receiver would discard the repeat — so it is never
    /// relayed.
    #[must_use]
    pub fn relay(&self, era: u32, view: u32) -> Vec<Datagram> {
        self.unacked
            .iter()
            .filter(|entry| entry.era == era && entry.view == view)
            .cloned()
            .collect()
    }

    /// The leader's last commit, the datagrams the heartbeat re-announces:
    /// the newest commit this node released per peer, and only at the
    /// ballot asked for. Empty when this node has released no commit at
    /// this ballot, and a leader with nothing to re-announce beats with
    /// nothing.
    #[must_use]
    pub fn last_commit(&self, era: u32, view: u32) -> Vec<Datagram> {
        self.last_commit
            .iter()
            .filter(|entry| entry.era == era && entry.view == view)
            .cloned()
            .collect()
    }

    /// How many datagrams are standing un-answered.
    #[must_use]
    pub fn unacknowledged(&self) -> usize {
        self.unacked.len()
    }

    /// How many entries the bound has dropped since boot.
    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

/// The replication state a host's own status word is, in the matcher's
/// vocabulary. A seated member of the current view — settled or inside a
/// view change — is an agent the cluster wait and the steady quiet are
/// protocol for; a node walking the boot machine's own path is not, and a
/// clock event on it is not protocol (`docs/src/timeout-policy.md`).
#[must_use]
pub fn matcher_state(replication_state: u32) -> State {
    match replication_state {
        // `normal` and `view_change`: seated in the cluster, the view
        // settled or in the attempt's limbo.
        0 | 1 => State::InTheCluster,
        // `recovering`, `replaying` and `joining`: the boot machine owns
        // its own progress.
        _ => State::Booted,
    }
}

/// What a due cluster wait may do, decided by the matcher and nothing
/// else: the relay of the un-acknowledged traffic, or nothing at all
/// because the node's condition is not protocol for that wait.
#[must_use]
pub fn cluster_timeout(
    replication_state: u32,
    ledger: &mut Ledger,
    era: u32,
    view: u32,
) -> Vec<Datagram> {
    if !matches!(
        matcher(matcher_state(replication_state), Timeout::Cluster),
        Opinion::Retransmit
    ) {
        return Vec::new();
    }
    ledger.relay(era, view)
}

/// What a due viewchange poll may do. The poll's own wait is the cluster
/// wait — the quorum response the armed attempt awaits — and the
/// matcher's answer for it on a seated member is the retransmit of that
/// attempt. The same opinion answers `(steady, cluster)`, so a node that
/// names either seated state gets the retransmit; nothing else does.
#[must_use]
pub fn poll_opinion(replication_state: u32) -> Opinion {
    matcher(matcher_state(replication_state), Timeout::Cluster)
}

/// What the leader's steady quiet may do: the heartbeat option, the
/// leader's last commit re-announced. `None` when this node does not
/// lead, when it leads with a view change in the air, or when it has
/// committed nothing at this ballot to re-announce.
#[must_use]
pub fn heartbeat(last_commit: Vec<Datagram>) -> Option<Vec<Datagram>> {
    if last_commit.is_empty() {
        None
    } else {
        Some(last_commit)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare(to: u32, slot: u64) -> Datagram {
        Datagram {
            to,
            era: 4,
            view: 42,
            slot,
            tag: Tag::Prepare,
            bytes: vec![Tag::Prepare as u8, slot as u8],
        }
    }

    fn commit(to: u32, slot: u64) -> Datagram {
        Datagram {
            to,
            era: 4,
            view: 42,
            slot,
            tag: Tag::Commit,
            bytes: vec![Tag::Commit as u8, slot as u8],
        }
    }

    #[test]
    fn the_relay_returns_every_unanswered_datagram_byte_for_byte() {
        let mut ledger = Ledger::new();
        ledger.record(prepare(2, 10));
        ledger.record(commit(2, 11));
        ledger.record(prepare(3, 10));
        assert_eq!(ledger.unacknowledged(), 3);
        let relayed = ledger.relay(4, 42);
        assert_eq!(relayed.len(), 3, "nothing was answered: {relayed:?}");
        assert_eq!(relayed[0], prepare(2, 10));
        assert_eq!(relayed[0].bytes, prepare(2, 10).bytes);
    }

    #[test]
    fn a_prepare_ok_from_one_peer_answers_only_that_peers_entry() {
        let mut ledger = Ledger::new();
        ledger.record(prepare(2, 10));
        ledger.record(prepare(3, 10));
        ledger.answered(2, 4, 42, 10, Tag::PrepareOk);
        let relayed = ledger.relay(4, 42);
        assert_eq!(relayed.len(), 1);
        assert_eq!(relayed[0].to, 3, "the other peer's prepare is unanswered");
    }

    #[test]
    fn a_later_prepare_ok_answers_the_commit_below_it() {
        let mut ledger = Ledger::new();
        ledger.record(commit(2, 10));
        ledger.answered(2, 4, 42, 10, Tag::PrepareOk);
        assert_eq!(
            ledger.unacknowledged(),
            1,
            "an ack at the commit's own slot does not prove the commit was applied"
        );
        ledger.answered(2, 4, 42, 11, Tag::PrepareOk);
        assert_eq!(ledger.unacknowledged(), 0);
    }

    #[test]
    fn an_older_ballots_entry_is_never_relayed() {
        let mut ledger = Ledger::new();
        ledger.record(prepare(2, 10));
        assert!(
            ledger.relay(4, 43).is_empty(),
            "the view advanced: the attempt is dead"
        );
        assert_eq!(ledger.unacknowledged(), 1, "and it is still counted");
    }

    #[test]
    fn a_tag_outside_the_four_classes_is_not_the_relays_business() {
        let mut ledger = Ledger::new();
        ledger.record(Datagram {
            to: 2,
            era: 4,
            view: 42,
            slot: 10,
            tag: Tag::Reincarnation,
            bytes: vec![Tag::Reincarnation as u8],
        });
        assert_eq!(ledger.unacknowledged(), 0);
    }

    #[test]
    fn the_bound_drops_the_oldest_and_counts_it() {
        let mut ledger = Ledger::with_bound(3);
        for slot in 1..=6 {
            ledger.record(prepare(2, slot));
        }
        assert_eq!(ledger.unacknowledged(), 3);
        assert_eq!(ledger.dropped(), 3);
        let relayed = ledger.relay(4, 42);
        assert_eq!(relayed[0].slot, 4, "the oldest went first");
        assert_eq!(relayed[2].slot, 6);
    }

    #[test]
    fn a_released_repeat_is_one_relay_not_two() {
        let mut ledger = Ledger::new();
        ledger.record(prepare(2, 10));
        ledger.record(prepare(2, 10));
        assert_eq!(ledger.unacknowledged(), 1);
    }

    #[test]
    fn the_heartbeat_is_the_newest_commit_per_peer_answered_or_not() {
        let mut ledger = Ledger::new();
        ledger.record(commit(2, 10));
        ledger.record(commit(3, 10));
        ledger.record(commit(2, 11));
        ledger.record(prepare(2, 12));
        // The frontier every replica already holds is still the frontier.
        ledger.answered(2, 4, 42, 11, Tag::PrepareOk);
        ledger.answered(3, 4, 42, 10, Tag::PrepareOk);
        let beat = heartbeat(ledger.last_commit(4, 42)).expect("a commit stands");
        assert_eq!(beat.len(), 2, "one per peer the frontier went to");
        assert!(beat.iter().all(|d| d.tag == Tag::Commit));
        let slots: Vec<u64> = beat.iter().map(|d| d.slot).collect();
        assert!(slots.contains(&11), "the newest commit per peer: {beat:?}");
        assert!(
            heartbeat(ledger.last_commit(4, 43)).is_none(),
            "another ballot: nothing"
        );
        assert!(
            heartbeat(Vec::new()).is_none(),
            "no commit: nothing to beat with"
        );
    }

    #[test]
    fn a_commit_at_a_newer_ballot_replaces_the_whole_frontier() {
        let mut ledger = Ledger::new();
        ledger.record(commit(2, 10));
        let moved = Datagram {
            to: 2,
            era: 4,
            view: 43,
            slot: 20,
            tag: Tag::Commit,
            bytes: vec![Tag::Commit as u8, 20],
        };
        ledger.record(moved.clone());
        assert_eq!(ledger.last_commit(4, 43), vec![moved]);
        assert!(
            ledger.last_commit(4, 42).is_empty(),
            "the previous ballot's commit is history"
        );
    }
}
