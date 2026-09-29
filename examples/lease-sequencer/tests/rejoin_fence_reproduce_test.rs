//! The fenced crash-restart reproduction: the two rejoin families the
//! local-softball9-2026-09-25 run recorded (its `progress.log` declares both
//! BLOCKED), force-fed to the fenced node in-process — no spawn, no fork,
//! no sockets; the storage is the one-line incarnation marker the boot gate
//! actually read, and the network is the committed capture
//! `docs/src/fenced-crash-restart/messages.jsonl` (every frame traceable to
//! the run's flight recorders by file/line/seq/ts).
//!
//! The recorded wire truth, both cases: the crashed voter reopens under the
//! bumped identity (`old 3 -> new 16777219`, incarnation 1), announces
//! `Reincarnation(3, 16777219)` at its boot view (era 1, view 0) once per
//! second per the host's fenced-boot drive (`main.rs::timers` ->
//! `Node::recover`, upstream §8's re-announce), and never seats.
//!
//! - Shallow case (the view-churn fence): the cluster churned views
//!   235..1238 at era 5 through the whole window and never addressed one
//!   datagram to the bumped identity; the only inbound class is the churn's
//!   `StartViewChange` fence vote addressed to the past-life id 3 (the
//!   transport's descriptor row; the peers' bumped rows died with the
//!   crashed process and were never re-learned — the peers announce only
//!   while fenced). The restarted node's era-1 table cannot evaluate era 5:
//!   every vote drops `UnevaluableEra` (`ext/uvrr-core/src/replica/
//!   view_change.rs::plan_start_view_change` — the era record lookup
//!   precedes the view comparison). Its journal for the window: 1979
//!   `UnevaluableEra`, 152 `ReincarnationRefused` (its own announcement
//!   looped back through the self row), and the leaders' journals refuse
//!   every announcement `ReincarnationRefused` — the armed-leader
//!   precondition (`Status::Normal` and primary of the current view,
//!   `ext/uvrr-core/src/replica/reincarnation.rs::plan_reincarnation`)
//!   never held in the churn. End state at the fence: `state=restarting
//!   era=1 view=0 config_era=1 voting=0`.
//! - Deep case (the install that never lands): the leader armed and the
//!   forced sequence committed the node's promotion (its journal reaches
//!   `config_era=5 voting=1`), but the restarted node's transport never
//!   re-learned the leader's bump, so the era-5 `StartView` and the memo
//!   stream arrive under the descriptor id 1, which the folded era-5
//!   configuration does not name: the journal drops them
//!   `StartViewNotFromPrimary` and `UnknownSender`. The fold's own wire
//!   path rode segments the flight recorder's keep-2 rotation pruned, so
//!   this test force-feeds the fence phase's surviving frames against the
//!   same minimum boot state; at the era-1 table the era gate fires first
//!   and the node never seats either way.
//!
//! The contract under test (the host's own "voting and serving" signal,
//! `main.rs::timers`): an announced crashed restart must be walked back to
//! voting weight by the leader's forced sequence. Both tests are RED: the
//! fence holds.

/// The shallow family (the view-churn fence): the restarted node's only
/// inbound for 151 s is the era-5 churn's fence votes addressed to its
/// past-life id; every one drops `UnevaluableEra` at the era-1 table and
/// no answer to the announcement ever arrives. The contract says the
/// leader's forced sequence walks the announced node back to voting
/// weight; the recorded exchange never seats it.
#[test]
fn reincarnating_into_the_view_churn_must_seat() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
/// The deep family (the install that never lands): the forced sequence
/// committed the node's promotion, then the era-5 view-126 install and the
/// memo stream arrived under the descriptor id 1 the restarted node's
/// transport still named the leader by. The capture's surviving frames for
/// the fence phase (the era-4 fence vote, the era-5 fence vote, the
/// `StartView`, the memo stream's first prepare and commit) are force-fed
/// here; at the minimum boot state the era gate fires first, exactly as it
/// did for every unevaluable frame the live node dropped. The contract is
/// the same: the announced restart must seat.
#[test]
fn reincarnating_under_the_stale_sender_attribution_must_seat() {
    panic!(
        "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
    )
}
