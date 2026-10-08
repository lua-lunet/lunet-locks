//! The contract seam: the three traits the machinery is written against,
//! and the division of labour they draw.
//!
//! One body of code, two environments, never conflated. The replication
//! machinery must prove itself in a minimal experimental harness and must
//! then survive real disks, real crashes and real operators. Those are two
//! environments around one protocol, and the whole distance between them is
//! three contracts:
//!
//! - [`Disk`] — every byte of disk access. Trivial files on one side, an
//!   industrial engine on the other.
//! - [`StateStore`] — the lock table's persistence, under a law of its own
//!   (below).
//! - [`CommitHook`] — a callback at each applied commit. A trigger, nothing
//!   more: the machinery does not know what the callback does and does not
//!   care.
//!
//! Nothing else crosses the boundary. There is no ambient filesystem, no
//! global, no environment reach-around, and no verdict the machinery reads
//! back out of a callback. An embedder that wants the third environment —
//! this repository — mounts its implementations of these three traits and
//! changes nothing above them.
//!
//! # The upstream side
//!
//! The advisory-locks demo in `uvrr-core` ships the machinery with the
//! least environment it can get away with:
//!
//! - trivial [`Disk`] and [`StateStore`] implementations — plain files, no
//!   AOF, no real superblock;
//! - induced timeouts, driven by the harness — no failure-detection
//!   machinery in the loop;
//! - crash-at-a-named-commit through [`CommitHook`], and crash-reload, in a
//!   plain harness, run distributed across a few nodes to take timings;
//! - maelstrom-style verification of the machinery itself.
//!
//! # This repository's side
//!
//! The industrial deployment mounts the heavy environment onto the same
//! machinery through the same traits:
//!
//! - the tbio-core storage engine — already behind `marker_store`'s FFI
//!   boundary — and the industrial [`Disk`] and [`StateStore`]
//!   implementations built on it;
//! - real timeouts and real failure detection;
//! - amortized flushing and logging driven by [`CommitHook`]: a snapshot
//!   flush spread over new commits so the eager shutdown flush rarely
//!   carries a large backlog;
//! - the AOF, the lock-event journal, the web console, the bridge and the
//!   launch tooling.
//!
//! No compliance suite, no corpus machinery, no FFI or cdylib surface and no
//! industrial durability crosses back the other way: the experiment carries
//! no industrial weight, and the industry carries no experimental
//! scaffolding.
//!
//! # The persistence law
//!
//! The two store operations are two disciplines, and [`StateStore`] is the
//! contract that binds both. Quoted from [`crate::state`], the law is:
//!
//! - **FLUSH is EAGER**: on the shutdown path the whole lock-table state is
//!   written in the foreground before the stop completes. A stop that cannot
//!   flush is a FAILED stop — it surfaces the failure, it never silently
//!   skips.
//! - **LOAD is LAZY**: the regular path never loads eagerly at boot. The
//!   table starts empty and state materialises only on demand (the
//!   cold-start fallback).
//! - **A CRASHED boot (the marker gate's verdict) DISTRUSTS the state
//!   file**: load returns nothing and the node rebuilds from the replica
//!   stream. Only a clean-stop file is loadable.
//!
//! The amendable half is stated once, on the trait itself in
//! [`crate::state::StateStore`], and a hook rides beside it without touching
//! it: an amortized flush is maintenance, not a second copy of the law.
//!
//! # The commit hook
//!
//! [`CommitHook::on_commit`] is a trigger, not a decision point. It returns
//! nothing because there is nothing for the machinery to do with an answer:
//! a commit has happened, the machinery applies it, and what happens around
//! that commit is the consumer's affair. The experimental harness kills a
//! node at a named commit — by whatever means it chooses, which is its own
//! affair — so a crash-reload is exercised at a chosen point instead of
//! after a timeout. The industrial side uses the same callback for
//! amortized maintenance: every N commits, TigerBeetle-style, flush the
//! state snapshot; and for logging. Production wiring is [`NoOpHook`].

use vrr::ids::Slot;

pub use crate::disk::{Disk, DiskDirEntry, DiskFile, StdDisk, std_disk};
pub use crate::locks::StateSnapshot;
pub use crate::state::{FileStateStore, StateStore};

/// A trigger fired at each applied commit.
///
/// Called synchronously on the apply path, once per applied commit,
/// immediately before the state machine is asked to execute against the
/// lock table — so a hook observes commits in the order the replica
/// committed them, and never asynchronously with respect to them.
///
/// The method returns nothing, and that is the whole contract. The callback
/// carries no verdict: a hook cannot fail a commit, cannot refuse one and
/// cannot tell the machinery anything, because the machinery has no use for
/// the answer. Whatever a hook does — crash the process at a named commit,
/// flush a snapshot every N commits, log the commit — is the hook's own
/// affair and the machinery's ignorance is what keeps the seam a seam.
///
/// `&mut self` because a hook may hold a position across commits: an
/// amortized flush counts them, and a counter is state. `Send + Sync`
/// because a hook is mounted once and lives as long as the node does.
pub trait CommitHook: Send + Sync {
    /// One applied commit. `slot` is the slot the entry committed at, so a
    /// hook can name the commit it acts on.
    fn on_commit(&mut self, slot: Slot);
}

/// The hook that does nothing: the wiring this tree ships.
///
/// Every node built by the public entry points holds this, so the default
/// path pays one dynamic dispatch and one uncontended lock per commit and
/// nothing else. An embedder mounting its own hook replaces it wholesale.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct NoOpHook;

impl CommitHook for NoOpHook {
    fn on_commit(&mut self, _slot: Slot) {}
}
