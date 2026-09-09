//! Advisory locks over the external Viewstamped Replication core.

pub mod aof;
mod ffi;
pub mod journal;
pub mod locks;

pub use ffi::{
    Node, NodeOutput, NodeStatus, POSITION_APPEND, RECONFIGURE_DECREMENT, RECONFIGURE_INCREMENT,
    RECONFIGURE_JOIN, RECONFIGURE_LEAVE,
};

pub use ffi::{
    CLIENT_JSON, CONFIG, FAULTED, INVALID, NOT_LEADER, OK, PANIC, SERVICE, TOO_LARGE, VRR_MESSAGE,
};
