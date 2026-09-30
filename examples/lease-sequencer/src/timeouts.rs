//! The host's timeout plane: the `timedout` toggle, the leader
//! timeout, the cluster viewchange timeout, and the randomized delay law
//! both schedules arm with.
//!
//! Failure detection here is the flavoured-timeout model and nothing
//! else: a node watches its leader over a randomised deadline and, when
//! the deadline passes, acts — the §14.2 host-forced view change, the
//! `suspect` toggle, the output flush. A node that observes a real
//! failure is therefore guaranteed to act on its own schedule. The
//! steady-state contract — the toggle, the viewchange poll, the
//! detection latch's arm points — is stated in
//! `docs/src/failure-detection.md`; the types here implement it and the
//! doc governs.

/// The textual form of a UDP endpoint for the JSON sample log (v4 or v6).
pub fn addr_text(addr: std::net::SocketAddr) -> String {
    addr.to_string()
}

// --------------------------------------------------------------- toggle ----

/// One state change of the `timedout` toggle: everything the toggle
/// logging carries (`docs/src/failure-detection.md`) — the new state,
/// the toggle's local-clock ts, and the ts of the LAST toggle (kept in
/// memory). It lands in the regular log and, as one `timeout-toggle`
/// event, in the Flight Recorder.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ToggleRecord {
    /// The state after the toggle: `true` = the node has timed out on
    /// its leader and issued or joined a view change; `false` = a fresh
    /// commit arrived and the next tick resumes.
    pub timedout: bool,
    /// This toggle's ts (the local clock, ms).
    pub at_ms: u64,
    /// The ts of the previous toggle, when one exists.
    pub previous_ms: Option<u64>,
}

/// The `timedout` toggle (`docs/src/failure-detection.md`): the host's
/// one-bit record of whether it has timed out on its leader and issued
/// or joined a view change. While it holds, the leader timeout is
/// NEITHER re-armed NOR checked — it is a steady-state leader-failure
/// timeout and is never fed leader-election costs — and the cluster
/// viewchange timeout polls instead. A fresh commit flips it false; the
/// next tick of the leader timer resumes.
pub struct TimeoutToggle {
    timedout: bool,
    last_toggle_ms: Option<u64>,
}

impl Default for TimeoutToggle {
    fn default() -> Self {
        Self::new()
    }
}

impl TimeoutToggle {
    pub fn new() -> Self {
        Self {
            timedout: false,
            last_toggle_ms: None,
        }
    }

    /// Whether the node is timed out on its leader. The leader timer
    /// checks this before doing anything (`if not timedout`); the
    /// cluster viewchange timer polls while it holds.
    pub fn timed_out(&self) -> bool {
        self.timedout
    }

    /// The ts of the LAST toggle, when one has happened.
    pub fn last_toggle_ms(&self) -> Option<u64> {
        self.last_toggle_ms
    }

    /// The node timed out on its leader (its suspicion drive issued a
    /// view change, or it joined one a peer started): toggle
    /// `timedout=true`. `Some` only on a state CHANGE — a steady `true`
    /// logs nothing, so a view change in progress never re-logs per
    /// poll.
    pub fn on_suspicion(&mut self, now_ms: u64) -> Option<ToggleRecord> {
        self.set(true, now_ms)
    }

    /// A fresh commit arrived: toggle `timedout=false`; the next tick
    /// of the leader timer resumes. `Some` only on the state change.
    pub fn on_commit(&mut self, now_ms: u64) -> Option<ToggleRecord> {
        self.set(false, now_ms)
    }

    fn set(&mut self, value: bool, now_ms: u64) -> Option<ToggleRecord> {
        if self.timedout == value {
            return None;
        }
        let previous_ms = self.last_toggle_ms;
        self.timedout = value;
        self.last_toggle_ms = Some(now_ms);
        Some(ToggleRecord {
            timedout: value,
            at_ms: now_ms,
            previous_ms,
        })
    }
}

// ------------------------------------------------------ random delay law ----

/// The randomized delay law both the leader timeout and the cluster
/// viewchange schedule arm with: `min + unit * (max - min)`
/// (`docs/src/failure-detection.md`). The unit sample is clamped into
/// [0, 1], so a hostile RNG sample can never escape the validated
/// bounds; a degenerate `max <= min` schedule is the fixed wait.
pub fn random_wait_ms(min_ms: u64, max_ms: u64, unit: f64) -> u64 {
    if max_ms <= min_ms {
        return min_ms;
    }
    let unit = unit.clamp(0.0, 1.0);
    min_ms + (unit * (max_ms - min_ms) as f64) as u64
}

/// The host's xorshift RNG: the single randomness source the host loop
/// feeds every randomised wait (the leader timeout's deadline, the
/// cluster viewchange schedule, the lease driver's jitter).
pub struct Rng(u64);

impl Rng {
    pub fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    pub fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
    /// One unit sample in [0, 1): the injected randomness every
    /// `random_wait_ms` consumer passes back in, so a test can drive a
    /// wait deterministically.
    pub fn unit(&mut self) -> f64 {
        // 53 random bits into an f64 fraction: the full double mantissa,
        // no bias toward either bound.
        (self.next() >> 11) as f64 / (1u64 << 53) as f64
    }
}

// ------------------------------------------------------- leader timeout ----

/// The leader timeout — the host's leader-failure detector, and the
/// service's only failure-detection mechanism. Per watched (era, leader)
/// key the deadline is `now + uniform_random(min, max)` over the
/// `--leader-timeout-min-ms/max-ms` knobs, re-armed on leader evidence:
/// the key's birth (a leader or era change), the fresh-commit resume,
/// and each heartbeat Commit arriving from the current leader. A due
/// deadline fires the §14.2 host-forced view change.
///
/// The timing law is Raft's (Ongaro 2014): broadcastTime ≪
/// electionTimeout ≪ MTBF, the wait RANDOMISED in a generous fixed
/// interval — the randomisation is the liveness mechanism (a
/// synchronised fleet must never time out in lockstep), the generosity
/// is the stability mechanism.
pub struct LeaderTimeout {
    min_ms: u64,
    max_ms: u64,
    watched: Option<(u32, u32)>,
    deadline_ms: u64,
    /// The ts of the last leader evidence: the key's birth, or the last
    /// re-arm. The detection note's silence measures against it.
    last_evidence_ms: u64,
}

impl LeaderTimeout {
    pub fn new(min_ms: u64, max_ms: u64) -> Self {
        Self {
            min_ms,
            max_ms,
            watched: None,
            deadline_ms: 0,
            last_evidence_ms: 0,
        }
    }

    /// The schedule's lower bound, in ms.
    pub fn min_ms(&self) -> u64 {
        self.min_ms
    }

    /// The schedule's upper bound, in ms.
    pub fn max_ms(&self) -> u64 {
        self.max_ms
    }

    /// Watches (era, leader): a NEW key (a leader change, a re-keying
    /// era) re-arms the deadline at its birth; the same key is stable.
    pub fn watch(&mut self, key: (u32, u32), now_ms: u64, unit: f64) {
        if self.watched == Some(key) {
            return;
        }
        self.watched = Some(key);
        self.rearm(now_ms, unit);
    }

    /// Re-arms the deadline: `now + uniform_random(min, max)`. Every
    /// leader-evidencing arrival runs this.
    pub fn rearm(&mut self, now_ms: u64, unit: f64) {
        self.deadline_ms = now_ms + random_wait_ms(self.min_ms, self.max_ms, unit);
        self.last_evidence_ms = now_ms;
    }

    /// Whether the deadline has passed while a key is watched.
    pub fn due(&self, now_ms: u64) -> bool {
        self.watched.is_some() && now_ms >= self.deadline_ms
    }

    /// The watched (era, leader) key, when one is watched.
    pub fn watched(&self) -> Option<(u32, u32)> {
        self.watched
    }

    /// The armed deadline (meaningful while a key is watched).
    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }

    /// The ts of the last leader evidence (birth or re-arm).
    pub fn last_evidence_ms(&self) -> u64 {
        self.last_evidence_ms
    }
}

// ---------------------------------------------------- viewchange timer ----

/// The cluster viewchange timeout's range error: the validated config
/// pair refused `min > max`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ViewChangeRange {
    pub min_ms: u64,
    pub max_ms: u64,
}

impl std::fmt::Display for ViewChangeRange {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "viewchange timeout min {} ms exceeds max {} ms",
            self.min_ms, self.max_ms
        )
    }
}

impl std::error::Error for ViewChangeRange {}

/// The due poll's actuation: what the due poll drives.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PollActuation {
    /// Nothing: the poll is not due or the node is not timed out.
    None,
    /// The ordinary suspicion tick (`leader_timeout`, `Input::Tick`).
    LeaderTimeout,
    /// The §14.2 host-forced view into `view + 1`: the limbo's drive.
    ForceView,
}

/// The due poll's actuation decision (`docs/src/failure-detection.md`):
/// while `timedout` holds the viewchange timer takes over from the
/// leader timeout. A node inside a view change
/// (`state == STATE_VIEW_CHANGE_HOST`) whose attempt's designated
/// primary may never arrive cannot be advanced by an ordinary tick —
/// the core's tick suspicion gate admits only `Normal` nodes
/// (`ext/uvrr-core/src/replica/mod.rs:1546-1547`) — so the limbo's poll
/// carries the §14.2 forced view instead: a NEW attempt re-broadcasts
/// its fence, the peers join and vote, and a live primary installs. A
/// fresh commit disarms the poll and the leader timeout resumes.
pub fn poll_actuation(timed_out: bool, state: u32, due: bool) -> PollActuation {
    if !timed_out || !due {
        return PollActuation::None;
    }
    if state == STATE_VIEW_CHANGE_HOST {
        return PollActuation::ForceView;
    }
    PollActuation::LeaderTimeout
}

/// The host's replication-state constants (`NodeStatus.state`): the
/// view-change limbo the poll's forced view carries.
pub const STATE_VIEW_CHANGE_HOST: u32 = 1;

/// The cluster viewchange timeout (`docs/src/failure-detection.md`): a
/// node that has SENT a view change / view votes is BY DEFINITION not
/// talking to the leader it suspects dead — it POLLS on that with its
/// own fixed, RANDOMISED timeout `min + rand() * (max - min)` so it
/// does not get stuck when the network drops its messages. This is a
/// DIFFERENT timer from the leader timeout (which checks
/// `if not timedout` and stands down); a fresh commit disarms the
/// poll. Too-low values cause view-change storms — hence the
/// randomisation — and the minimum must stay above 4x RTT.
pub struct ViewChangeTimer {
    min_ms: u64,
    max_ms: u64,
    deadline_ms: u64,
    armed: bool,
}

impl ViewChangeTimer {
    /// A validated poll timer. `min > max` refuses: the randomized
    /// schedule needs a real range.
    pub fn new(min_ms: u64, max_ms: u64) -> Result<Self, ViewChangeRange> {
        if min_ms > max_ms {
            return Err(ViewChangeRange { min_ms, max_ms });
        }
        Ok(Self {
            min_ms,
            max_ms,
            deadline_ms: 0,
            armed: false,
        })
    }

    /// Arms (or re-arms) the next poll: it lands at
    /// `now + min + unit * (max - min)`, `unit` the injected RNG's unit
    /// sample. Re-arming after every poll is the randomisation — a
    /// synchronised fleet must never re-poll in lockstep.
    pub fn arm(&mut self, now_ms: u64, unit: f64) {
        self.deadline_ms = now_ms + random_wait_ms(self.min_ms, self.max_ms, unit);
        self.armed = true;
    }

    /// Whether the poll is due: armed and the schedule has elapsed.
    pub fn due(&self, now_ms: u64) -> bool {
        self.armed && now_ms >= self.deadline_ms
    }

    /// Disarms the poll: a fresh commit released the node from the
    /// view change.
    pub fn disarm(&mut self) {
        self.armed = false;
    }

    /// Whether the timer is armed.
    pub fn armed(&self) -> bool {
        self.armed
    }

    /// The armed poll's deadline (meaningful while armed).
    pub fn deadline_ms(&self) -> u64 {
        self.deadline_ms
    }
}
