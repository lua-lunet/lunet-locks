//! The client gate: the SIGUSR1 silence / SIGUSR2 start switch every
//! lease-load worker polls at its sleep/every-iteration boundary. The gate
//! is pure state so the semantics are unit-testable: boot is OFF, silence
//! clears the schedule and every trace of holdership (including the
//! lease-id/request-num bookkeeping, so a late reply can never finish a
//! renewal), and a start re-enters the chase as a probe — never a blind
//! BUMP or SET of a lease the silenced client no longer holds.

/// The gate mode: OFF does nothing, ON chases the lock.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Mode {
    Off,
    On,
}

/// The op the worker should issue under the current gate state.
#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum Op {
    Get,
    Set,
    Extend,
}

/// The gate state one worker carries: the mode, the wall-ms instant of
/// its next scheduled action (`None` probes), its holdership identity,
/// and the lease-id/request-num bookkeeping.
#[derive(Debug)]
pub struct Gate {
    pub mode: Mode,
    pub schedule: Option<u64>,
    pub holder: Option<String>,
    pub lease_id: u64,
    pub request_num: u64,
}

/// Boot the gate: the process (re)starts silent and does nothing until
/// the first SIGUSR2.
pub fn boot() -> Gate {
    Gate {
        mode: Mode::Off,
        schedule: None,
        holder: None,
        lease_id: 0,
        request_num: 0,
    }
}

/// The silence transition (SIGUSR1): stop all outbound operations,
/// forget holdership, drop any scheduled action, and reset the
/// lease-id/request-num bookkeeping so an in-flight SET cannot
/// double-renew. The current wall-ms reading is consumed so the caller's
/// log line and the internal reset share one clock reading.
pub fn stop(gate: &mut Gate, _wall_ms: u64) {
    gate.mode = Mode::Off;
    gate.schedule = None;
    gate.holder = None;
    reset_bookkeeping(gate);
}

/// The start transition (SIGUSR2): leave OFF and begin the chase from
/// the probe state — the first action is a GET probe (no schedule, no
/// holdership to resume). Idempotent while ON.
pub fn start(gate: &mut Gate, _wall_ms: u64) {
    gate.mode = Mode::On;
}

/// The op the worker issues under the gate's current state: nothing while
/// OFF; a GET probe when ON with no schedule; the scheduled BUMP only
/// while the gate still holds the lock it would renew (a schedule that
/// survived without holdership probes instead — it can never renew a
/// lease the gate no longer holds).
pub fn next_op(gate: &Gate, _now_ms: u64) -> Option<Op> {
    if gate.mode == Mode::Off {
        return None;
    }
    match gate.schedule {
        None => Some(Op::Get),
        Some(_) if gate.holder.is_some() => Some(Op::Extend),
        Some(_) => Some(Op::Get),
    }
}

/// The bookkeeping reset helper: lease-id and request-num go to zero so
/// a late SET's identity can never be re-used across a silence window.
pub fn reset_bookkeeping(gate: &mut Gate) {
    gate.lease_id = 0;
    gate.request_num = 0;
}

#[cfg(test)]
mod tests {
    #[test]
    fn boot_is_off() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[test]
    fn usr1_clears_holdership_and_schedule() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[test]
    fn usr1_reset_bookkeeping_helper_zeroes_only_bookkeeping() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[test]
    fn usr2_from_off_first_action_is_a_probe() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[test]
    fn usr1_mid_holdership_then_usr2_never_extends_blindly() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[test]
    fn off_gate_issues_nothing() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }

    #[test]
    fn on_gate_with_schedule_is_an_extension_only_while_its_holder_stands() {
        panic!(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    }
}
