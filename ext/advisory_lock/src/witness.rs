//! The witness: the deployment's state holder.
//!
//! Every deployment registers at least one witness per data centre. A
//! witness is outside the roster, never votes, and receives the full
//! phase-2 and commit stream from every leader in turn. Its append-only
//! series is the durable copy of the stream: the witness records what it
//! receives and tracks two high-watermarks — the highest frontier it
//! holds in its buffer and the highest frontier it has flushed (the
//! witness ruling in `docs/src/decisions.md`). The series is no longer
//! observability only; it is the copy a full-cluster cold start recovers
//! from, on the operator's driven replay — never an automatic startup
//! path.
//!
//! This module is the watermarks and the three sync-point rules, pure:
//! the flush effect (the tape's fsync) is the driver's, so the rules are
//! provable red-green with no disk, no thread, and no clock. The file
//! tape and the wire mount ride separately.

/// The committed frontier a sync point names: the stream's progress
/// identity, ordered by era, then view, then slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Frontier {
    pub era: u32,
    pub view: u32,
    pub slot: u64,
}

/// The sync-point rule's outcome for a received point.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SyncOutcome {
    /// The flushed watermark already covers the frontier: acknowledged.
    Acked,
    /// The frontier is in the buffer: the witness forces the flush of
    /// the batch already in hand (a small amount of bookkeeping and an
    /// early flush, never a change to the streaming protocol), and the
    /// point acknowledges with that flush.
    ForceFlush,
    /// The frontier is beyond the buffer: the witness holds the point
    /// open, flushes the moment the stream covers it, and acknowledges
    /// then.
    Held,
}

/// The two high-watermarks and the held-open points. Every mutation is
/// monotonic: a watermark never moves backwards, and a held point never
/// unholds — the witness records what it receives and never forgets
/// progress.
#[derive(Debug, Default)]
pub struct WitnessWatermarks {
    buffered: Option<Frontier>,
    flushed: Option<Frontier>,
    held: Vec<Frontier>,
}

impl WitnessWatermarks {
    pub fn new() -> Self {
        Self::default()
    }

    /// The highest frontier held in the buffer (received, not yet flushed).
    pub fn buffered(&self) -> Option<Frontier> {
        self.buffered
    }

    /// The highest frontier flushed.
    pub fn flushed(&self) -> Option<Frontier> {
        self.flushed
    }

    /// The held-open sync points, in arrival order.
    pub fn held(&self) -> &[Frontier] {
        &self.held
    }

    /// A stream record landed in the buffer. The buffered watermark
    /// moves to the record's frontier; the flush effect is the driver's.
    pub fn observe(&mut self, frontier: Frontier) {
        if self.buffered.is_none_or(|held| held < frontier) {
            self.buffered = Some(frontier);
        }
    }

    /// The buffer's contents are durable: the flushed watermark covers
    /// everything the buffer held. Returns the held-open points the new
    /// flushed watermark covers — their acknowledgements are owed now.
    pub fn flushed_through_buffer(&mut self) -> Vec<Frontier> {
        if let Some(buffered) = self.buffered
            && self.flushed.is_none_or(|flushed| flushed < buffered)
        {
            self.flushed = Some(buffered);
        }
        self.resolve_held()
    }

    /// The three sync-point rules, applied in order:
    ///
    /// 1. the flushed watermark already covers the frontier — acknowledge
    ///    immediately;
    /// 2. the frontier is in the buffer — force the flush and acknowledge
    ///    with it;
    /// 3. the frontier has not yet been seen — hold the point open,
    ///    flush the moment the stream covers it, and acknowledge then.
    ///
    /// No reply is awaited on the wire: the witnesses' series are the
    /// backup certificate. The outcome tells the driver which flush (if
    /// any) the point owes; the acknowledgements are the driver's to
    /// log.
    pub fn sync_point(&mut self, frontier: Frontier) -> SyncOutcome {
        if self.flushed.is_some_and(|flushed| flushed >= frontier) {
            return SyncOutcome::Acked;
        }
        if self.buffered.is_some_and(|buffered| buffered >= frontier) {
            return SyncOutcome::ForceFlush;
        }
        if !self.held.contains(&frontier) {
            self.held.push(frontier);
        }
        SyncOutcome::Held
    }

    /// The held points the current flushed watermark covers, removed
    /// from the hold set in arrival order.
    fn resolve_held(&mut self) -> Vec<Frontier> {
        let Some(flushed) = self.flushed else {
            return Vec::new();
        };
        let (covered, remaining): (Vec<Frontier>, Vec<Frontier>) =
            self.held.drain(..).partition(|point| *point <= flushed);
        self.held = remaining;
        covered
    }
}

#[cfg(test)]
mod tests;
