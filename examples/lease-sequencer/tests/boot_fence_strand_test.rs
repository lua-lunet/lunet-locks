//! The boot-fence strand (local-softball5/6-2026-09-18, `.tmp/telemetry/
//! local-softball5-2026-09-18/REPORT.md` and `local-softball6-2026-09-18/
//! REPORT.md`): a provisioned voter — a fresh First boot, a clean
//! superblock, a full genesis member — strands boot-fenced at
//! `state=4 (Joining) view=0` forever while the other two voters walk the
//! genesis views past it and serve as a 2-of-3 quorum. The stranded node
//! drops every inbound message as `ViewMismatch { got: N, current: 0 }`,
//! never adopts, never panics, and sends nothing the cluster can act on —
//! the recorded runs predate the join-gossip drive. On the current tree
//! the host loop's resend timer emits the entry ticket
//! (`rejoin::gossip_datagram`, the joiner half of
//! `lease_sequencer::rejoin`) at the node's current view, and this test
//! pins that emission.
//!
//! The recorded race, from the run logs: the stranded node's FIRST-EVER
//! inbound datagram was already at the walk's final view (run 5:
//! `got: View(12)` in the boot millisecond; run 6: 84,979 drops, every
//! one `got: View(6)`, ZERO datagrams at views 0–5). The whole genesis
//! walk completed before the node processed anything, and the settled
//! serving cluster holds no further view changes — so the only inbound
//! the fence ever sees is the leader's higher-view Prepare/Commit stream.
//!
//! The contract pinned here (AGENTS.md's bug-provenance law): a node at
//! rest must time out and send. The boot gate's uninitialised/dirty path
//! says the node gossips, catches up, rejoins — so the fenced node must
//! either ADOPT a view (reach Normal at the cluster's view) or PRODUCE
//! outbound evidence the cluster can act on: a join gossip / GossipRequest
//! at any view, or a state-transfer request the leader accepts.
//!
//! The join gossip is the rejoin gossip's joiner half — a HOST obligation
//! (`lease_sequencer::rejoin`): rejoining is a gossip protocol OUTSIDE the
//! main uVRR protocol, and the joiner keeps its own
//! resend timer. The core emits no `GossipRequest` — a `Joining` node's
//! tick drives only an already-open fetch (`ext/uvrr-core/src/replica/
//! mod.rs` `plan_tick`), and the fetch opens only through paths a fenced
//! fresh boot never reaches — while the core HANDLES the message on
//! receive (`plan_gossip_request`: every node that hears it records the
//! sender as a gossip-witness; the leader answers with the missed-range
//! push above the sender's frontier plus a fresh commit, and the echo of
//! the request's own view is what qualifies that push at the boot fence
//! — `plan_new_state`). The datagram therefore carries the node's
//! CURRENT view, never a view it has merely heard of: an entry ticket
//! naming a foreign view would draw an answer the fence drops as
//! `StaleTransfer` — evidence dressed up, not evidence. This harness
//! drives the host's resend timer exactly as `main.rs::timers` does:
//! delete the drive and the strand returns.
//!
//! Why the strand is deterministic (the drop rules, cited):
//! - `ext/uvrr-core/src/replica/normal.rs` (`plan_prepare`, the
//!   higher-view branch): a Prepare from the legitimate primary of a
//!   HIGHER view hits `plan_higher_view_signal` only for a node NOT at
//!   its boot fence (`if !boot_fence`); a boot-fenced `Joining` member
//!   (`current == retained`) falls through to
//!   `header.view != current` → `Diagnostic::ViewMismatch { got, current
//!   }`. Its adoption window accepts messages AT its current view only.
//! - The same shape in `plan_commit` (the `if !boot_fence` branch, then
//!   the `ViewMismatch` drop).
//! - `ext/uvrr-core/src/replica/mod.rs` (`plan_tick`): a `Joining` node
//!   is excluded from the suspicion gate (`matches!(status, Normal |
//!   Restarting)`), is not promotable unless it IS the genesis primary,
//!   and with no stalled offer and no open fetch its tick produces the
//!   smallest honest transition — no outbound, ever. The §10 acquisition
//!   re-run re-issues only an already-open fetch, and the fetch opens
//!   only through `plan_higher_view_signal`, which the boot fence skips.
//!
//! Why the fabric must SETTLE the cluster before releasing the held
//! node: a view change whose designated primary is the held node jams the
//! walkers into the limbo, and the poll's forced advance keeps
//! broadcasting `StartViewChange` fence votes (`enter_view_change` sends
//! to every backup) — a boot-fenced node ADMITS a higher-view fence vote
//! (`view_change.rs::plan_start_view_change`: `header.view > target` →
//! joins the attempt) and a jammed attempt at one of its primary views
//! would seat it. That rescue is exactly what the live runs' cluster
//! never offered: it had already settled at its final view. So the fabric
//! walks past, then quiesces the churn behind a serving leader (the
//! fresh-commit arrival re-arms the follower's watch, the live host's
//! calm-profile behavior) before the held node's first datagram — the
//! recorded race, deterministically.
//!
//! The test then gives the fenced node a generous bounded budget of its
//! own timeout drives and demands the contract: adopt, or emit a join
//! gossip / GossipRequest at any view — the entry ticket the leader acts
//! on whatever view it names.

/// The boot-fence strand: a provisioned voter whose first-ever inbound
/// datagram arrives at the settled cluster's view must still reach the
/// cluster — adopt, or emit outbound evidence the cluster can act on.
/// Every inbound datagram at the fence drops as
/// `ViewMismatch { got: N, current: 0 }` and the node's own timeout
/// machinery produces nothing (the drop rules above), so the pinned
/// evidence is the join gossip: the host loop's resend timer
/// (`main.rs::timers`) emits `rejoin::gossip_datagram` at the node's
/// CURRENT view every `rejoin::GOSSIP_RESEND_MS`, and the heal window
/// counts those resends. The node stays fenced at `Joining` view 0 in
/// this fabric — adoption is the cluster's answering half, exercised by
/// the live crash-family re-run lane.
#[test]
fn a_boot_fenced_voter_must_adopt_or_emit_actionable_evidence() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
