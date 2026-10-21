//! The witness: the deployment's state holder — the watermarks and the
//! three sync-point rules, pure: the flush effect (the tape's fsync) is
//! the driver's, so the rules are provable red-green with no disk, no
//! thread, and no clock.

/// The committed frontier a sync point names: ordered by era, then
/// view, then slot.
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
    /// The frontier is in the buffer: force the flush of the batch
    /// already in hand and acknowledge with it.
    ForceFlush,
    /// The frontier is beyond the buffer: hold the point open and
    /// acknowledge when the stream covers it.
    Held,
}

/// The two high-watermarks and the held-open points. Every mutation is
/// monotonic: a watermark never moves backwards, a held point never
/// unholds.
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

    pub fn buffered(&self) -> Option<Frontier> {
        self.buffered
    }

    pub fn flushed(&self) -> Option<Frontier> {
        self.flushed
    }

    pub fn held(&self) -> &[Frontier] {
        &self.held
    }

    /// A stream record landed in the buffer; the buffered watermark
    /// moves to its frontier.
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

    /// The three sync-point rules, in order: covered by the flushed
    /// watermark — acknowledge immediately; in the buffer — force the
    /// flush and acknowledge with it; unseen — hold the point open and
    /// acknowledge when the stream covers it.
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
