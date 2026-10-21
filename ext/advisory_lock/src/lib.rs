//! Advisory locks over the external Viewstamped Replication core.
//!
//! The machinery is written against three contracts — [`spi::Disk`],
//! [`spi::StateStore`] and [`spi::CommitHook`] — with the boot fence's
//! [`spi::LifecycleStore`] re-exported beside them as the fourth mounting
//! point. Nothing else crosses the boundary between the protocol and
//! whatever environment it runs in; [`spi`] is the one named surface both
//! sides are written against.

pub mod aof;
pub mod disk;
mod ffi;
#[cfg(feature = "flight-recorder")]
pub mod flight;
pub mod info;
pub mod journal;
pub mod locks;
mod marker_store;
pub mod recovery_flush;
pub mod spi;
pub mod state;
#[cfg(not(target_os = "windows"))]
pub mod wal_snapshot;
pub mod witness;

pub use ffi::{
    Node, NodeOutput, NodeStatus, OUTPUT_REPLY, OUTPUT_SEND, POSITION_APPEND, PRIMARY_TIMEOUT_MS,
    RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT, RECONFIGURE_JOIN, RECONFIGURE_LEAVE,
    StateStatusRaw, output_kind_name, replication_state_name,
};

pub use recovery_flush::{FlushOutcome, RecoveryFlush};

#[cfg(any(test, debug_assertions))]
#[doc(hidden)]
pub use ffi::{census_paths, census_push};

/// The wall-clock millisecond every log line carries. `#[doc(hidden)]`
/// because no embedder calls it: the crate's own sites call it directly,
/// and a log line's ts is the host's clock, nothing more.
#[doc(hidden)]
pub use ffi::log_millis;

pub use ffi::{
    CLIENT_JSON, CONFIG, FAULTED, INVALID, NOT_LEADER, OK, PANIC, SERVICE, TOO_LARGE, VRR_MESSAGE,
};
