//! Advisory locks over the external Viewstamped Replication core.
//!
//! The machinery is written against three contracts — [`spi::Disk`],
//! [`spi::StateStore`] and [`spi::CommitHook`] — and nothing else crosses
//! the boundary between the protocol and whatever environment it runs in.
//! [`spi`] is the one named surface both sides are written against, and its
//! module doc is the normative division of labour.

pub mod aof;
/// The disk seam: the one contract every byte this crate reads or writes
/// is named against, so the crate compiles identically over the local
/// filesystem ([`disk::StdDisk`]) and over whatever industrial engine an
/// embedder mounts behind [`disk::Disk`] instead. Nothing above the seam
/// knows which one it holds.
pub mod disk;
mod ffi;
#[cfg(feature = "flight-recorder")]
pub mod flight;
pub mod info;
pub mod journal;
pub mod locks;
mod marker_store;
pub mod recovery_flush;
/// The contract seam: the three traits the machinery is written against —
/// [`spi::Disk`], [`spi::StateStore`] and [`spi::CommitHook`] — re-exported
/// under one name. This module IS the boundary between the experimental
/// harness upstream runs the machinery in and the industrial deployment
/// this repository runs it in, and its doc comment states that division
/// normatively.
pub mod spi;
/// The state seam: the lock table's persistence contract, one trait
/// ([`state::StateStore`]). Flush is EAGER — on the shutdown path the
/// whole lock-table state is written in the foreground before the stop
/// completes, and a stop that cannot flush is a failed stop, never a
/// silent skip. Load is LAZY — the regular path never loads eagerly at
/// boot; the table starts empty and state materialises only on demand, at
/// the apply path's first committed entry. A crashed boot (the marker
/// gate's verdict) distrusts the state file and rebuilds from the replica
/// stream: only a clean-stop file is loadable.
pub mod state;

pub use ffi::{
    Node, NodeOutput, NodeStatus, OUTPUT_REPLY, OUTPUT_SEND, POSITION_APPEND, PRIMARY_TIMEOUT_MS,
    RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT, RECONFIGURE_JOIN, RECONFIGURE_LEAVE,
    output_kind_name, replication_state_name,
};

pub use recovery_flush::{FlushOutcome, RecoveryFlush};

#[cfg(any(test, debug_assertions))]
#[doc(hidden)]
pub use ffi::{census_paths, census_push};

/// The wall-clock millisecond every log line carries. `#[doc(hidden)]`
/// because no embedder calls it: `maybe_invariant!` reaches it through
/// `$crate`, and the crate's own sites call it directly.
#[doc(hidden)]
pub use ffi::log_millis;

pub use ffi::{
    CLIENT_JSON, CONFIG, FAULTED, INVALID, NOT_LEADER, OK, PANIC, SERVICE, TOO_LARGE, VRR_MESSAGE,
};
