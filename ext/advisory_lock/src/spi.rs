//! The contract seam: the three traits the machinery is written against —
//! [`Disk`], [`StateStore`], [`CommitHook`] — plus the boot fence
//! ([`LifecycleStore`]) riding beside them as the fourth mounting point.
//! Nothing else crosses the boundary: no ambient filesystem, no global, no
//! verdict read back out of a callback.

use vrr::ids::Slot;

pub use crate::disk::{Disk, DiskDirEntry, DiskFile, EngineFile, StdDisk, std_disk};
pub use crate::locks::StateSnapshot;
pub use crate::state::{FileStateStore, StateStore, StoreStatus, StreamCounters};
pub use vrr::lifecycle::LifecycleStore;

/// A trigger fired at each applied commit, synchronously on the apply
/// path in commit order. Returns nothing — a hook cannot fail or refuse a
/// commit; what it does is its own affair. `&mut self` because a hook may
/// hold a position across commits.
pub trait CommitHook: Send + Sync {
    /// One applied commit; `slot` names it.
    fn on_commit(&mut self, slot: Slot);
}

/// The hook that does nothing: the wiring this tree ships.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoOpHook;

impl CommitHook for NoOpHook {
    fn on_commit(&mut self, _slot: Slot) {}
}
