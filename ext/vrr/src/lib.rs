//! Advisory locks over Viewstamped Replication.
//!
//! `locks` is the client protocol and the replicated service. `vrr` is the
//! replication core and treats client payloads as opaque bytes.

mod ffi;
pub mod locks;
pub mod vrr;
