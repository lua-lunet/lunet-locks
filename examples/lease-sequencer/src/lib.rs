//! The library facade the integration tests link: the pure
//! leader-failure detection and timeout modules. The binary keeps its
//! own `mod` tree; this facade re-exports only what tests and external
//! embedders need.

pub mod bench_oracle;
pub mod bench_store;
pub mod bridge;
pub mod client_gate;
pub mod embedded_client;
pub mod flight_tape;
pub mod rejoin;
pub mod shutdown_check;
pub mod tape;
pub mod telemetry;
pub mod timeouts;
pub mod uds_harness;
