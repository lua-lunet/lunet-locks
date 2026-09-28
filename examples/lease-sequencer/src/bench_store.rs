//! The bench store service: the driver side of the lifecycle marker
//! store RPC (`docs/src/bench-harness.md`).
//!
//! The driver holds each node identity's durable state in memory and
//! answers the node's `read_copies` / `commit` / `drain` calls over a
//! per-node unix socket. It is not a passive file: the scenario signals
//! the identity's transitions (clean stop, crash), and the discipline
//! automaton holds every store call to the signalled expectation. A
//! call that arrives unsignalled or out of order is a hard harness
//! failure — the reply refuses, the violation is latched, and the run
//! stops. The bench never tolerates a store surprise: a surprise is the
//! bug being hunted.
//!
//! # The call grammar the engine speaks
//!
//! - **First life** — `read` (answered none), then the latch `commit`
//!   with the running sentinel.
//! - **Clean stop** (SIGUSR1 cycle, SIGTERM) — `commit(stopping)`,
//!   `drain`, `commit(stopped)`, strictly in that order.
//! - **Clean boot** — `read` answered with the drain-proven `stopped`,
//!   then the latch `commit` with the sentinel.
//! - **Dirty boot** (SIGUSR2 cycle, SIGKILL) — `read` answered with the
//!   recorded sentinel; the emission gate's bump `commit` (the next
//!   life at the running sentinel) lands at boot, before the first
//!   announcement, and the engine's seated latch lands the same round
//!   again when the witness mints — the same identity and marker,
//!   expected at most twice.
//!
//! Everything else is a violation.

use std::io::{BufRead, BufReader, Write};
use std::os::unix::net::UnixListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};

/// The engine marker words on the wire (the node's `marker_word`).
const STOPPING: &str = "stopping";
const STOPPED: &str = "stopped";
const RESTARTING: &str = "restarting";
const JOINING: &str = "joining";

fn is_sentinel(marker: &str) -> bool {
    marker == RESTARTING || marker == JOINING
}

/// The discipline phase one node identity is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// The first life's boot read is expected.
    FirstBoot,
    /// A boot read was answered; the latch commit is expected.
    Latch,
    /// Latched and serving: no store calls until a signalled transition.
    Running,
    /// A clean stop was signalled: `commit(stopping)` is expected.
    ExpectStopping,
    /// The halt's first round landed: the `drain` is expected.
    ExpectDrain,
    /// The drain landed: `commit(stopped)` is expected.
    ExpectStopped,
    /// The clean stop completed: the next boot read answers `stopped`.
    CleanBoot,
    /// A crash/dirty cycle was signalled: the next boot read answers
    /// the running sentinel.
    DirtyBoot,
    /// The dirty boot read was answered: the deferred bump commit lands
    /// exactly once when the witness seats.
    Unseated,
    /// A clean stop was signalled inside the deferred window: the store
    /// has already seen the emission gate's bump round (it landed at
    /// boot), so the store is silent until the witness seats — then the
    /// engine's latch round lands and the full halt rounds follow.
    StopFromUnseated,
}

/// One node identity's memory-resident durable state plus its
/// discipline automaton.
pub struct NodeDiscipline {
    incarnation: u64,
    marker: Option<String>,
    phase: Phase,
    /// The bump rounds landed in the current life: the emission gate's
    /// boot round is the first, the engine's seated latch round the
    /// second. Reset by every boot read (a new life begins).
    bump_rounds: u8,
    /// The latched violation, if any: the first surprise, kept whole.
    violation: Option<String>,
}

impl Default for NodeDiscipline {
    fn default() -> NodeDiscipline {
        NodeDiscipline::new()
    }
}

impl NodeDiscipline {
    /// A fresh identity awaiting its first life's boot.
    pub fn new() -> NodeDiscipline {
        NodeDiscipline {
            incarnation: 0,
            marker: None,
            phase: Phase::FirstBoot,
            bump_rounds: 0,
            violation: None,
        }
    }

    /// The scenario's clean-stop signal (SIGUSR1 cycle or SIGTERM):
    /// lawful from a serving state only. Inside the deferred window the
    /// store sees nothing at all — no marker round is lawful and the
    /// sink drain rides outside the store.
    pub fn signal_clean_stop(&mut self) {
        match self.phase {
            Phase::Running => self.phase = Phase::ExpectStopping,
            Phase::Unseated => self.phase = Phase::StopFromUnseated,
            phase => self.violate(format!(
                "a clean stop was signalled while the identity was in {phase:?}"
            )),
        }
    }

    /// The scenario's crash signal (SIGUSR2 cycle or SIGKILL): lawful
    /// from a serving state only.
    pub fn signal_crash(&mut self) {
        match self.phase {
            Phase::Running | Phase::Unseated => self.phase = Phase::DirtyBoot,
            phase => self.violate(format!(
                "a crash was signalled while the identity was in {phase:?}"
            )),
        }
    }

    /// The latched violation, if the identity ever surprised the driver.
    pub fn violation(&self) -> Option<&str> {
        self.violation.as_deref()
    }

    /// The terminal audit: the run ends with every identity drained and
    /// flushed (the teardown's clean stops) — or mid-life only where the
    /// scenario left it deliberately. `true` when the automaton rests in
    /// a lawful resting phase with no violation.
    pub fn at_rest(&self) -> bool {
        self.violation.is_none()
            && matches!(
                self.phase,
                Phase::Running
                    | Phase::Unseated
                    | Phase::CleanBoot
                    | Phase::DirtyBoot
                    | Phase::StopFromUnseated
            )
    }

    /// The identity is latched and serving (or in the unseated window,
    /// which serves through recovery) with no latched violation.
    pub fn is_running(&self) -> bool {
        self.violation.is_none() && matches!(self.phase, Phase::Running | Phase::Unseated)
    }

    fn violate(&mut self, what: String) {
        if self.violation.is_none() {
            self.violation = Some(what);
        }
    }

    fn refusal(&mut self, what: String) -> serde_json::Value {
        self.violate(what.clone());
        serde_json::json!({"ok": false, "error": what})
    }

    /// A `read_copies` call: the reply is the recorded verdict.
    pub fn on_read(&mut self) -> serde_json::Value {
        self.bump_rounds = 0;
        match self.phase {
            Phase::FirstBoot => {
                self.phase = Phase::Latch;
                serde_json::json!({"verdict": "none"})
            }
            Phase::CleanBoot => {
                self.phase = Phase::Latch;
                serde_json::json!({"verdict": STOPPED, "incarnation": self.incarnation})
            }
            Phase::DirtyBoot | Phase::StopFromUnseated => {
                self.phase = Phase::Unseated;
                // The sentinels collapse to `joining` on the read,
                // exactly as the durable path's classification does. The
                // deferred window's stop re-classifies crashed through
                // the same answer.
                serde_json::json!({"verdict": JOINING, "incarnation": self.incarnation})
            }
            phase => self.refusal(format!(
                "an unsignalled boot read arrived while the identity was in {phase:?}"
            )),
        }
    }

    /// A `commit` call: the reply is the driver's acknowledgement or its
    /// refusal.
    pub fn on_commit(&mut self, incarnation: u64, marker: &str) -> serde_json::Value {
        match self.phase {
            Phase::Latch if is_sentinel(marker) => {
                self.incarnation = incarnation;
                self.marker = Some(marker.to_string());
                self.phase = Phase::Running;
                serde_json::json!({"ok": true})
            }
            Phase::ExpectStopping if marker == STOPPING => {
                self.marker = Some(marker.to_string());
                self.phase = Phase::ExpectDrain;
                serde_json::json!({"ok": true})
            }
            // The EMISSION GATE lands the bump round at boot (the next
            // life at the running sentinel, before the first
            // announcement), and the engine's seated latch lands the
            // same round again when the witness mints: two identical
            // sentinel rounds at incarnation + 1, the first recorded
            // here (the identity stays unseated), the second latches
            // Running.
            Phase::Unseated
                if is_sentinel(marker)
                    && self.bump_rounds == 0
                    && self.incarnation.checked_add(1) == Some(incarnation) =>
            {
                self.incarnation = incarnation;
                self.marker = Some(marker.to_string());
                self.bump_rounds = 1;
                serde_json::json!({"ok": true})
            }
            Phase::Unseated
                if is_sentinel(marker)
                    && self.bump_rounds >= 1
                    && incarnation == self.incarnation =>
            {
                self.marker = Some(marker.to_string());
                self.bump_rounds += 1;
                self.phase = Phase::Running;
                serde_json::json!({"ok": true})
            }
            Phase::ExpectStopped if marker == STOPPED => {
                self.marker = Some(marker.to_string());
                self.phase = Phase::CleanBoot;
                serde_json::json!({"ok": true})
            }
            // The stop was signalled inside the deferred window, but the
            // witness seated before the stop ran: the engine's latch
            // round lands lawfully (the emission gate's round already
            // stands), and the stop then owes the full halt rounds.
            Phase::StopFromUnseated if is_sentinel(marker) && incarnation == self.incarnation => {
                self.marker = Some(marker.to_string());
                self.phase = Phase::ExpectStopping;
                serde_json::json!({"ok": true})
            }
            // A crash was signalled and the node lived long enough for
            // the seated latch to land its round: the same round the
            // emission gate already made durable, recorded, and the
            // boot read that follows answers from it.
            Phase::DirtyBoot if is_sentinel(marker) && incarnation == self.incarnation => {
                self.marker = Some(marker.to_string());
                serde_json::json!({"ok": true})
            }
            phase => self.refusal(format!(
                "an unexpected commit (incarnation {incarnation}, marker {marker}) arrived \
                 while the identity was in {phase:?}"
            )),
        }
    }

    /// A `drain` call: lawful strictly between the halt's two rounds.
    /// The deferred window's stop owes nothing to the store — its sink
    /// drain rides the host directly.
    pub fn on_drain(&mut self) -> serde_json::Value {
        match self.phase {
            Phase::ExpectDrain => {
                self.phase = Phase::ExpectStopped;
                serde_json::json!({"ok": true})
            }
            phase => self.refusal(format!(
                "an unexpected drain arrived while the identity was in {phase:?}"
            )),
        }
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Handle one wire line: the reply JSON and whether the run's failure
/// flag must latch (a refusal or any latched violation). This is the
/// whole wire layer — `serve_socket` only moves bytes, so the wire's
/// own surprises (an unparseable call, an unknown op) are unit-tested
/// here, not just observed on a live socket.
fn handle_line(discipline: &mut NodeDiscipline, line: &str) -> (serde_json::Value, bool) {
    let request: serde_json::Value = match serde_json::from_str(line.trim_end()) {
        Ok(request) => request,
        Err(error) => {
            let reply = discipline.refusal(format!("an unparseable store call: {error}"));
            return (reply, true);
        }
    };
    let reply = match request.get("op").and_then(|op| op.as_str()) {
        Some("read") => discipline.on_read(),
        Some("commit") => {
            let Some(incarnation) = request.get("incarnation").and_then(|i| i.as_u64()) else {
                let reply =
                    discipline.refusal("a commit carried no integer incarnation field".to_string());
                let latch = reply.get("error").is_some() || discipline.violation().is_some();
                return (reply, latch);
            };
            let marker = request
                .get("marker")
                .and_then(|m| m.as_str())
                .unwrap_or("")
                .to_string();
            discipline.on_commit(incarnation, &marker)
        }
        Some("drain") => discipline.on_drain(),
        other => discipline.refusal(format!("an unknown store call op {other:?}")),
    };
    // The latch discriminator is the `error` field: every refusal
    // carries one (a refused call or a wire surprise), and no lawful
    // reply does — the read's verdict and the commit's bare `ok` alike.
    let latch = reply.get("error").is_some() || discipline.violation().is_some();
    (reply, latch)
}

/// Serve one node identity's store socket until the listener errors or
/// the run's failure flag latches. Each accepted connection is one node
/// life: the bench's in-process cycles and the driver's respawns
/// reconnect per boot. Protocol: one NDJSON request, one NDJSON reply.
/// A violation refuses the request, latches the run's failure flag,
/// and keeps serving — the loudest possible trail.
pub fn serve_socket(
    listener: UnixListener,
    discipline: Arc<Mutex<NodeDiscipline>>,
    failure: Arc<AtomicBool>,
) {
    for stream in listener.incoming() {
        if failure.load(Ordering::Relaxed) {
            return;
        }
        let Ok(mut stream) = stream else {
            return;
        };
        let Ok(clone) = stream.try_clone() else {
            return;
        };
        let mut reader = BufReader::new(clone);
        loop {
            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {}
                Err(_) => break,
            }
            let (reply, latch) = {
                let mut guard = lock(&discipline);
                handle_line(&mut guard, &line)
            };
            if latch {
                failure.store(true, Ordering::Relaxed);
            }
            let mut out = reply.to_string();
            out.push('\n');
            if stream.write_all(out.as_bytes()).is_err() {
                break;
            }
            let _ = stream.flush();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn boot_clean(discipline: &mut NodeDiscipline) {
        assert_eq!(discipline.on_read(), serde_json::json!({"verdict": "none"}));
        assert_eq!(
            discipline.on_commit(1, JOINING),
            serde_json::json!({"ok": true})
        );
    }

    #[test]
    fn the_first_life_boots_and_latches() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_clean_cycle_is_the_ordered_sequence() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_dirty_cycle_bumps_through_the_unseated_window() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn an_unsignalled_commit_is_a_violation() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn an_unsignalled_read_is_a_violation() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn a_drain_outside_the_halt_is_a_violation() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_late_bump_may_race_a_signalled_stop() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_stop_inside_the_deferred_window_is_silent_to_the_store() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn a_drain_inside_the_deferred_windows_stop_is_a_violation() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn a_bump_off_a_max_incarnation_is_a_non_match_never_an_overflow() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_wire_refuses_a_commit_without_an_incarnation_field() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn a_signalled_stop_from_a_non_serving_phase_is_a_violation() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_wire_round_trips_a_lawful_read() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_wire_refuses_an_unparseable_call_and_latches() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_wire_refuses_an_unknown_op_and_latches() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_wire_latches_a_discipline_refusal_with_its_reason() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn the_default_discipline_is_a_fresh_identity() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }
}
