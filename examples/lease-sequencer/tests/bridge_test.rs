//! The AOF console bridge: red/green tests for the decode and
//! replay surface. The fixture is a real AOF series written through the
//! vendored TigerBeetle writer (AofFile) with envelope records whose Wire
//! payloads are packed uVRR messages carrying Service request payloads —
//! the same bytes the standby learner records. The bridge must decode them
//! through the existing surfaces only: `Record::decode`,
//! `Message::unpack_from`, `Service::decode`, and the `Service::execute`
//! state machine (the exact committed-work path the adapter's
//! `Effect::Apply` arm runs).

// ------------------------------------------------------------ decode ----

/// The committed lock work decodes into the console's event kinds, in ns
/// order, with the record's ns clock converted to epoch ms at the boundary
/// (integer floor division by 1_000_000 — never widened back).
#[test]
fn decode_produces_events_with_ms_times() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// /locks state is the replayed Service state: the broken lock is free with
/// the bumped keeper lease_id, the name/labels are sticky.
#[test]
fn replayed_lock_state_matches() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// An unknown envelope marker is rejected — never guessed. The record is
/// counted undecodable and produces no event.
#[test]
fn unknown_markers_are_rejected() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// A Wire payload that is not a uVRR message (garbage) is counted as an
/// undecodable wire message, never an event.
#[test]
fn garbage_wire_payload_is_counted_not_decoded() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The series reader is read-only: replaying twice yields identical state
/// and never mutates the series (same file set, same sizes).
#[test]
fn replay_is_read_only_and_deterministic() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

// -------------------------------------------------------------- HTTP ----

/// The HTTP surface: the endpoints answer with the console's shapes.
#[test]
fn http_endpoints_serve_console_shapes() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// Segmented requests under parallel load: several servers, several
/// client threads, every request written one segment at a time. The
/// server drains the full request head before answering, so every
/// request must complete without a connection reset and parse to its
/// intended route — a reset or a mis-parsed path (the empty path of a
/// half-read request line) is a failure. Bounded: a few hundred
/// loopback connections.
#[test]
fn segmented_requests_under_parallel_load_never_reset() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The live WebSocket push: with follow on, an appended record arrives as a
/// text frame on /api/v1/live.
#[test]
fn follow_pushes_new_events_over_websocket() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The wire alphabet the bridge counts is total over the core's tags.
#[test]
fn wire_alphabet_is_total() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}

/// The bulk telemetry endpoint: the phi samples (marker 5) and the timeout
/// decisions (marker 2) as arrays, plus the span — the ECharts tab's data
/// source.
#[test]
fn telemetry_phi_endpoint_serves_samples_and_decisions() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
