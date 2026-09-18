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
    Node, NodeOutput, NodeStatus, OUTPUT_REPLY, OUTPUT_SEND, POSITION_APPEND,
    RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT, RECONFIGURE_JOIN, RECONFIGURE_LEAVE,
    output_kind_name, replication_state_name,
};

pub use recovery_flush::{FlushOutcome, RecoveryFlush};

pub use ffi::{
    CLIENT_JSON, CONFIG, FAULTED, INVALID, NOT_LEADER, OK, PANIC, SERVICE, TOO_LARGE, VRR_MESSAGE,
};
