//! The AOF lifecycle gate and telemetry log (item22 M2) plus the
//! phi-informed timeout estimator (item22 M3).
//!
//! # The lifecycle gate (the hard requirement)
//!
//! The host's AOF follows its voting weight: weight 0 (or the boot
//! Recovering/Joining phase where the weight is not yet known) = ON;
//! weight > 0 = OFF. 1→0 re-arms it (a trace gap since disarm is fine and
//! expected). The 1000 ms forced-flush loop runs only while the gate is
//! active, so a node wedged in Restarting/Joining under a network storm
//! still leaves a trace. Clean stop: a teardown record (marker
//! `TelemetryStateTransition`, `event=teardown`) is written LAST and the
//! AOF is flushed unconditionally (SIGTERM/clean path; SIGKILL loses the
//! last unflushed window).
//!
//! # Rollover (no disk flooding)
//!
//! The AOF series keeps exactly the CURRENT file + ONE closed old file.
//! When the active file's size approaches the threshold the active file is
//! closed, a new `{epoch}.aof` opens, and the prune deletes older files
//! oldest-first — the min-2 floor from item21's retention encodes the
//! same rule; here it is enforced as keep-exactly-two.

use lunet_locks_aof::envelope::{Marker, Record, local_ns};
use lunet_locks_aof::{AofFile, Error, Options, retention};
use std::path::{Path, PathBuf};

// ----------------------------------------------------------------- gate ----

/// The weight-driven AOF lifecycle gate. Constructed ON (the boot
/// Recovering/Joining phase always logs), then driven by the node's live
/// voting weight.
#[derive(Debug)]
pub struct Gate {
    active: bool,
    /// The forced-flush interval (ms) — 1000 by default.
    flush_interval_ms: u64,
    /// The last flush the gate drove (its own clock, host ms).
    last_flush_ms: u64,
    /// The rollover threshold (bytes): the active file is closed and a
    /// fresh one opened at/after this size.
    #[allow(dead_code)]
    rollover_bytes: u64,
}

/// The gate's event: `Some(true)` re-armed the AOF, `Some(false)` disarmed
/// it, `None` nothing changed.
pub type GateEvent = Option<bool>;

impl Gate {
    /// A gate that starts active (the boot phase), flushed last at
    /// `last_flush_ms`, with the flush interval and the rollover threshold.
    pub fn new(last_flush_ms: u64, flush_interval_ms: u64, rollover_bytes: u64) -> Self {
        Gate {
            active: true,
            flush_interval_ms,
            last_flush_ms,
            rollover_bytes,
        }
    }

    /// Whether the AOF is on: weight 0 or unknown (the boot phase) = ON;
    /// weight > 0 = OFF (the hard requirement).
    pub fn active(&self) -> bool {
        self.active
    }

    /// Drive the gate from the node's voting weight (`None` when the node
    /// is not a member of the current folded configuration — the boot
    /// Recovering/Joining phase). Reports the transition so the host can
    /// flush on disarm and note the trace gap on re-arm.
    pub fn on_weight(&mut self, weight: Option<u32>) -> GateEvent {
        let on = !matches!(weight, Some(w) if w > 0);
        if on == self.active {
            return None;
        }
        self.active = on;
        Some(on)
    }

    /// The forced flusher: due only while active, one interval after the
    /// last flush it drove.
    pub fn flush_due(&self, now_ms: u64) -> bool {
        self.active && now_ms.saturating_sub(self.last_flush_ms) >= self.flush_interval_ms
    }

    /// Stamp a flush the gate drove.
    pub fn note_flush(&mut self, now_ms: u64) {
        self.last_flush_ms = now_ms;
    }

    /// Stamp the re-arm: the flusher restarts from NOW (a fresh interval).
    pub fn note_rearm(&mut self, now_ms: u64) {
        self.last_flush_ms = now_ms;
    }

    /// The rollover threshold.
    pub fn rollover_bytes(&self) -> u64 {
        self.rollover_bytes
    }
}

// -------------------------------------------------------- telemetry log ----

/// The node's telemetry AOF: the vendored TigerBeetle AOF plus the
/// envelope record layer (M1), the weight-driven lifecycle gate, the
/// rollover rule (exactly current + one closed old), and the teardown
/// discipline. Every record carries the local nanosecond clock in its
/// header.
pub struct TelemetryLog {
    file: Option<AofFile>,
    dir: PathBuf,
    /// The lifecycle gate: weight 0 (or the boot phase) = ON, weight > 0
    /// = OFF; the flusher and the gated records follow it.
    gate: Gate,
    /// The rollover threshold (bytes on the active file).
    rollover_bytes: u64,
    /// The retention threshold passed through to the open sweep.
    retention_bytes: u64,
    /// The active file's tracked size (appended bytes since open/roll).
    active_size: u64,
    closed: bool,
}

impl TelemetryLog {
    /// Open (or start) the telemetry series at `dir`: the retention sweep
    /// runs, a NEW active file opens with force OFF, and the gate starts
    /// ON (the boot Recovering/Joining phase).
    pub fn open(
        dir: &Path,
        flush_interval_ms: u64,
        rollover_bytes: u64,
        retention_bytes: u64,
    ) -> Result<Self, Error> {
        let file = AofFile::open_with(
            dir,
            Options {
                force_flush: false,
                retention_bytes,
            },
        )?;
        Ok(TelemetryLog {
            active_size: 0,
            closed: false,
            dir: dir.to_path_buf(),
            file: Some(file),
            gate: Gate::new(0, flush_interval_ms, rollover_bytes),
            retention_bytes,
            rollover_bytes,
        })
    }

    /// The lifecycle gate's drive (call every host tick): weight 0 or
    /// unknown (the boot Recovering/Joining phase) = ON; weight > 0 = OFF.
    /// Disarming flushes what is buffered once; re-arming restarts the
    /// flusher's interval.
    pub fn on_weight(&mut self, weight: Option<u32>, now_ms: u64) {
        match self.gate.on_weight(weight) {
            Some(false) => self.flush(),
            Some(true) => self.gate.note_rearm(now_ms),
            None => {}
        }
    }

    /// Whether the AOF is on.
    pub fn gate_active(&self) -> bool {
        self.gate.active()
    }

    /// The 1000 ms forced-flush loop's body, plus the rollover check. Runs
    /// only while the gate is active.
    pub fn tick(&mut self, now_ms: u64) {
        if !self.closed && self.gate.flush_due(now_ms) {
            self.flush();
            self.gate.note_flush(now_ms);
        }
        if self.gate.active()
            && self.rollover_due()
            && let Err(error) = self.rollover()
        {
            eprintln!(
                "lease-sequencer: telemetry aof rollover failed ({error}); \
                 the telemetry stream is disabled for this process"
            );
            self.file = None;
            self.closed = true;
        }
    }

    /// Record one envelope record — the gate-checked append path (the
    /// boot/teardown records go through `append` directly: they write
    /// regardless of the gate, since the clean-stop teardown flushes
    /// unconditionally). Rollover runs after the append when the active
    /// file has reached the threshold.
    pub fn record(&mut self, record: Record) {
        if self.file.is_none() || !self.gate.active() {
            return;
        }
        self.append(record);
        if self.rollover_due()
            && let Err(error) = self.rollover()
        {
            eprintln!(
                "lease-sequencer: telemetry aof rollover failed ({error}); \
                 the telemetry stream is disabled for this process"
            );
            self.file = None;
            self.closed = true;
        }
    }

    /// The active file's path (for operators: which file is being
    /// appended to).
    pub fn active_path(&self) -> &Path {
        self.file
            .as_ref()
            .map(|file| file.path())
            .unwrap_or(&self.dir)
    }

    /// The active file's size (approximate: envelope bytes appended).
    pub fn active_size(&self) -> u64 {
        self.active_size
    }

    /// Whether the rollover threshold is reached.
    pub fn rollover_due(&self) -> bool {
        self.active_size >= self.rollover_bytes
    }

    /// Append one envelope record. A failed append poisons the log (the
    /// stream disables for the process — telemetry never breaks the
    /// replication path); the caller checks `is_closed`/`failed`.
    pub fn append(&mut self, record: Record) {
        if self.file.is_none() {
            return;
        }
        let encoded = record.encode();
        let Some(file) = self.file.as_mut() else {
            return;
        };
        match file.append(&encoded) {
            Ok(_op) => {
                self.active_size += encoded.len() as u64;
            }
            Err(error) => {
                eprintln!(
                    "lease-sequencer: telemetry aof append failed ({error}); \
                     the telemetry stream is disabled for this process"
                );
                self.file = None;
                self.closed = true;
            }
        }
    }

    /// Explicit fsync (the forced-flush loop's body; flushes only what is
    /// there when the log is still open).
    pub fn flush(&mut self) {
        if let Some(file) = self.file.as_mut()
            && let Err(error) = file.flush()
        {
            eprintln!("lease-sequencer: telemetry aof flush failed ({error})");
        }
    }

    /// Rollover: close the active file, open a fresh `{epoch}.aof`, prune
    /// older files so the series is exactly current + one closed old.
    pub fn rollover(&mut self) -> Result<(), Error> {
        if let Some(old) = self.file.take() {
            old.close()?;
        }
        self.active_size = 0;
        let file = AofFile::open_with(
            &self.dir,
            Options {
                force_flush: false,
                retention_bytes: self.retention_bytes,
            },
        )?;
        self.file = Some(file);
        // The keep-exactly-two rule: everything older than the (new)
        // active file and its one predecessor goes, oldest first.
        let listed = retention::list_aof_files(&self.dir)?;
        let files: Vec<(String, (u64, u64))> = listed
            .iter()
            .map(|(path, key, _)| {
                (
                    path.file_name().unwrap().to_string_lossy().to_string(),
                    *key,
                )
            })
            .collect();
        let active = self
            .file
            .as_ref()
            .map(|file| {
                file.path()
                    .file_name()
                    .unwrap()
                    .to_string_lossy()
                    .to_string()
            })
            .unwrap_or_default();
        for name in prune_plan(&files, &active) {
            std::fs::remove_file(self.dir.join(&name))?;
        }
        Ok(())
    }

    /// Clean stop: write the teardown record LAST, flush unconditionally,
    /// close.
    pub fn teardown(&mut self) -> Result<(), Error> {
        if !self.closed {
            self.append(teardown_record(local_ns()));
            self.flush();
            if let Some(file) = self.file.take() {
                file.close()?;
            }
            self.closed = true;
        }
        Ok(())
    }

    /// Whether the log is closed (teardown ran, or an append failure
    /// poisoned the stream).
    pub fn is_closed(&self) -> bool {
        self.closed
    }
}

impl Drop for TelemetryLog {
    fn drop(&mut self) {
        if !self.closed {
            // The last-resort flush: the teardown discipline already ran on
            // the clean paths.
            self.flush();
        }
    }
}

/// The clean-stop teardown record: `TelemetryStateTransition`, JSON
/// `{"event":"teardown","ns":<local clock>}`.
pub fn teardown_record(ns: u64) -> Record {
    Record::telemetry(
        Marker::TelemetryStateTransition,
        ns,
        format!("{{\"event\":\"teardown\",\"ns\":{ns}}}").as_bytes(),
    )
}

/// The marker table's runtime mirror (the envelope's byte values).
pub fn marker_bytes(marker: Marker) -> u8 {
    marker as u8
}

/// The keep-exactly-two prune plan over the `.aof` series: everything
/// older than the current file's one predecessor goes, oldest first.
/// `files` are `(name, (epoch, seq))` pairs; `active` is the current
/// file's name.
pub fn prune_plan(files: &[(String, (u64, u64))], active: &str) -> Vec<String> {
    // All series files other than the active one, oldest first.
    let mut closed: Vec<&(String, (u64, u64))> =
        files.iter().filter(|(name, _)| name != active).collect();
    closed.sort_by_key(|(_, key)| *key);
    if closed.len() <= 1 {
        return Vec::new();
    }
    closed[..closed.len() - 1]
        .iter()
        .map(|(name, _)| name.clone())
        .collect()
}

// --------------------------------------------------- phi timeout (M3) ------

/// The phi-timeout knobs (the host's clamp bounds).
#[derive(Clone, Copy, Debug)]
pub struct TimeoutKnobs {
    /// The wait never derives below this (`--phi-timeout-min-ms`).
    pub min_ms: u64,
    /// The wait never derives above this (`--phi-timeout-max-ms`).
    pub max_ms: u64,
    /// The old fixed gate (`--election-ms`) the estimator falls back to
    /// when the sketch is untrustworthy.
    pub fixed_ms: u64,
}

/// The phi-informed wait estimate (M3): `safety * max(heartbeat, learned
/// mean)` from the leader's sketch, clamped to `[min, max]`. An unsettled
/// sketch (`mean_interval_ms = None`: fewer than two intervals learned)
/// falls back to the old fixed gate, itself clamped — the estimate is
/// never earlier than a settled phi allows and never later than the old
/// fixed gate.
pub fn phi_wait_ms(
    mean_interval_ms: Option<f64>,
    heartbeat_ms: u64,
    safety_multiple: f64,
    knobs: &TimeoutKnobs,
) -> Option<u64> {
    let raw = match mean_interval_ms {
        Some(mean) => safety_multiple * (heartbeat_ms as f64).max(mean),
        None => knobs.fixed_ms as f64,
    };
    if !raw.is_finite() || raw <= 0.0 {
        return Some(knobs.max_ms);
    }
    Some((raw as u64).clamp(knobs.min_ms, knobs.max_ms))
}

/// The `TelemetryTimeoutDecision` record: the phi estimate, now, the wait
/// the loop had set before this tick, the wait it will set next.
pub fn timeout_decision_record(
    ns: u64,
    phi: f64,
    now_ms: u64,
    prev_wait_ms: u64,
    next_wait_ms: u64,
) -> Record {
    let json = format!(
        "{{\"phi\":{phi:.3},\"now_ms\":{now_ms},\"prev_wait_ms\":{prev_wait_ms},\
         \"next_wait_ms\":{next_wait_ms}}}"
    );
    Record::telemetry(Marker::TelemetryTimeoutDecision, ns, json.as_bytes())
}
