//! The library facade the integration tests link: the pure phi module.
//! The binary keeps its own `mod` tree; this facade re-exports only what
//! tests and external embedders need.

pub mod bridge;
pub mod phi;
pub mod telemetry;
