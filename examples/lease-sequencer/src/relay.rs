use vrr::timeout::{Opinion, State, Timeout, matcher};
use vrr::wire::Tag;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Datagram {
    pub to: u32,
    pub era: u32,
    pub view: u32,
    pub slot: u64,
    pub tag: Tag,
    pub bytes: Vec<u8>,
}

impl Datagram {
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

fn relayable(tag: Tag) -> bool {
    matches!(
        tag,
        Tag::Prepare | Tag::Commit | Tag::StartViewChange | Tag::DoViewChange
    )
}

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

pub struct Ledger {
    unacked: Vec<Datagram>,
    last_commit: Vec<Datagram>,
    bound: usize,
    dropped: u64,
}

const DEFAULT_BOUND: usize = 256;

impl Default for Ledger {
    fn default() -> Self {
        Self::new()
    }
}

impl Ledger {
    #[must_use]
    pub fn new() -> Self {
        Self::with_bound(DEFAULT_BOUND)
    }

    #[must_use]
    pub fn with_bound(bound: usize) -> Self {
        Self {
            unacked: Vec::new(),
            last_commit: Vec::new(),
            bound: bound.max(1),
            dropped: 0,
        }
    }

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

    pub fn answered(&mut self, from: u32, era: u32, view: u32, slot: u64, tag: Tag) {
        if !relayable(tag) && !matches!(tag, Tag::PrepareOk | Tag::StartView) {
            return;
        }
        self.unacked
            .retain(|entry| !answers(entry, from, era, view, slot, tag));
    }

    #[must_use]
    pub fn relay(&self, era: u32, view: u32) -> Vec<Datagram> {
        self.unacked
            .iter()
            .filter(|entry| entry.era == era && entry.view == view)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn last_commit(&self, era: u32, view: u32) -> Vec<Datagram> {
        self.last_commit
            .iter()
            .filter(|entry| entry.era == era && entry.view == view)
            .cloned()
            .collect()
    }

    #[must_use]
    pub fn unacknowledged(&self) -> usize {
        self.unacked.len()
    }

    #[must_use]
    pub fn dropped(&self) -> u64 {
        self.dropped
    }
}

#[must_use]
pub fn matcher_state(replication_state: u32) -> State {
    match replication_state {
        0 | 1 => State::InTheCluster,
        _ => State::Booted,
    }
}

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

#[must_use]
pub fn poll_opinion(replication_state: u32) -> Opinion {
    matcher(matcher_state(replication_state), Timeout::Cluster)
}

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
