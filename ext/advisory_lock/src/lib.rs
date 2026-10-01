//! Advisory locks over the external Viewstamped Replication core.

pub mod aof;
mod ffi;
#[cfg(feature = "flight-recorder")]
pub mod flight;
pub mod journal;
pub mod locks;
mod marker_store;
pub mod recovery_flush;

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
