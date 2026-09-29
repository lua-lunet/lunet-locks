//! The embedded lock client: the sequencer host's contender
//! loops driven in-process against a two-node localhost harness — the
//! gap-test's direct-routing pump (no TCP, no sockets: node outputs are
//! delivered to the addressed peer inside the process). The scenario is
//! the exp1/exp2 client-discipline story, asserted through
//! Service-visible lock state (GET probes through the leader), never
//! stdout:
//!
//! 1. both embedded clients boot OFF — no op is ever built, the lock
//!    stays free;
//! 2. SIGUSR2 starts them: the SET race lands deterministically (the
//!    leader-side client's op is submitted and committed first) and the
//!    holder renews on the leader-echoed cadence;
//! 3. SIGUSR1 silences the HOLDER's host: the lease lapses unrenewed
//!    and the OTHER client takes over within TTL+slack with a fresh
//!    take (a new `taken_at_ms` past the silence, never a renewal of the
//!    silenced client's lease);
//! 4. SIGUSR2 restarts the silenced client: it re-enters as a
//!    NON-holder — it probes, sees the incumbent, and polls; the holder
//!    keeps holdership across the window.

#[test]
fn embedded_clients_gate_takeover_and_rejoin_as_non_holders() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
