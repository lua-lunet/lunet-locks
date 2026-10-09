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
pub mod gateway;
pub mod rejoin;
pub mod relay;
pub mod shutdown_check;
pub mod tape;
pub mod telemetry;
pub mod timeouts;
pub mod uds_harness;

/// The chase driver's SET payload (the binary's `submit_hold` and
/// `submit_renew` send this exact shape — one payload serves hold, steal,
/// and renew). The candidate names the DURATION (`lease_ms`), never an
/// absolute expiry: the engine's `LeaseCandidate` decodes strictly and
/// refuses the retired `expiry` member outright, so a payload that drifts
/// back to the retired shape spins grantless on a live cluster — zero
/// grants, zero renews. The contract test
/// (`tests/chase_set_contract_test.rs`) decodes and executes THIS builder's
/// payload against the engine's `Service`, so any drift fails the gates
/// instead of the standby lane.
pub fn chase_set_payload(
    message_id: &str,
    lock_id: u64,
    client_id: u64,
    request_num: u64,
    lease_id: u64,
    holder: &str,
    lease_ms: u64,
) -> String {
    format!(
        "{{\"op\":\"set\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\
         \"request_num\":{request_num},\"lock_id\":{lock_id},\
         \"lease\":{{\"lease_id\":{lease_id},\"holder\":\"{holder}\",\"lease_ms\":{lease_ms}}}}}"
    )
}
