//! The contender decision machinery, extracted from the lease-load
//! binary so the sequencer host can drive the identical chase machine
//! in-process (item04): no client→cluster TCP noise, the same cadence
//! math, the same SIGUSR1 silence / SIGUSR2 start gate. The module is a
//! pure object on the wall clock — every decision fn takes the
//! caller's `now` — fed replies by whoever transported the request: the
//! binary's TCP link or the host's own `Node` output drain. The state
//! machine is the design's §1.2: GET probe → free/expired ⇒ SET race;
//! live foreign incumbent ⇒ poll past the leader-echoed expiry with
//! jitter; holder ⇒ BUMP renewal one renewal-margin inside the deadline;
//! error/deny ⇒ backoff.

use crate::client_gate::{self, Gate, Mode, Op};
use serde_json::Value;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

/// One contender's knobs: the lock it chases, its client identity (the
/// base; client `i` uses `client_id + i`), the lease window, and the
/// renewal point as a fraction of that window.
#[derive(Clone)]
pub struct Config {
    pub lock_id: u64,
    pub client_id: u64,
    pub lease_ms: u64,
    pub renew_fraction: f64,
}

/// The action a contender decided on: the op label (`get`, or `set` for
/// both the acquire race and the same-holder renewal), the message id
/// the submitter correlates the reply by, and the request JSON —
/// byte-for-byte the line the wire clients send.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Action {
    pub op: &'static str,
    pub message_id: [u8; 16],
    pub request: String,
}

/// Whether the reply carries no error (a completed round trip, granted
/// or not).
pub fn reply_ok(reply: &Value) -> bool {
    reply.get("error").is_none()
}

/// The lease's remaining window in the leader's timeline
/// (`expiry - executed_at`), when the reply carried a live lease.
pub fn reply_remaining_ms(reply: &Value) -> Option<u64> {
    let lease = reply.get("lease")?;
    let expiry = lease.get("expiry")?.as_u64()?;
    let executed_at = reply.get("executed_at")?.as_u64()?;
    Some(expiry.saturating_sub(executed_at))
}

/// Whether the reply's lease names this contender as its holder.
fn reply_holds(reply: &Value, holder: &str) -> bool {
    reply
        .get("lease")
        .and_then(|lease| lease.get("holder"))
        .and_then(|lease_holder| lease_holder.as_str())
        .is_some_and(|lease_holder| lease_holder == holder)
        && reply
            .get("granted")
            .and_then(|granted| granted.as_bool())
            .unwrap_or(true)
}

struct Rng(u64);

impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed | 1)
    }
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }
}

/// One lock client's chase state: the client gate (mode, schedule,
/// holdership, lease-id/request-num bookkeeping) plus the identity and
/// cadence knobs the gate's decisions are made against.
pub struct Contender {
    config: Config,
    gate: Gate,
    holder: String,
    renew_margin: u64,
    rng: Rng,
}

impl Contender {
    /// Boot the contender: the gate starts OFF (silent until the first
    /// SIGUSR2) and the holder identity is drawn from the seed exactly
    /// as the lease-load worker draws it.
    pub fn new(config: Config, seed: u64) -> Contender {
        let renew_margin = (config.lease_ms as f64 * (1.0 - config.renew_fraction)) as u64;
        let mut rng = Rng::new(seed);
        let holder = format!("{:032x}", rng.next());
        Contender {
            config,
            gate: client_gate::boot(),
            holder,
            renew_margin,
            rng,
        }
    }

    /// The client gate — the signal drain's handle.
    pub fn gate(&mut self) -> &mut Gate {
        &mut self.gate
    }

    /// The gate mode (the loop's sleep discipline reads it).
    pub fn mode(&self) -> Mode {
        self.gate.mode
    }

    /// The contender's holder identity (its lease's holder string).
    pub fn holder(&self) -> &str {
        &self.holder
    }

    /// Whether the gate currently holds the lock it chases.
    pub fn holds(&self) -> bool {
        self.gate.holder.is_some()
    }

    /// The wall-ms instant of the next scheduled action (`None` =
    /// probe when due).
    pub fn scheduled_at(&self) -> Option<u64> {
        self.gate.schedule
    }

    /// The op due at `now`: `None` while OFF or before the schedule; a
    /// GET probe or a BUMP renewal otherwise (the gate schedules probes
    /// and renewals only).
    pub fn next_action(&mut self, now_ms: u64) -> Option<Action> {
        if self.gate.mode == Mode::Off {
            return None;
        }
        if let Some(at) = self.gate.schedule
            && now_ms < at
        {
            return None;
        }
        match client_gate::next_op(&self.gate, now_ms)? {
            Op::Get => Some(self.build_get()),
            Op::Bump => Some(self.build_set(now_ms, "bump")),
            Op::Set => unreachable!("the gate schedules probes and renewals only"),
        }
    }

    /// Absorb one action's reply into the chase state — the
    /// leader-echoed expiry interpretation, holdership, and the next
    /// schedule — and return the immediate follow-up action a free
    /// probe races with (the SET). `None` reply is an error or an
    /// unparseable reply: back off and re-probe.
    pub fn absorb(
        &mut self,
        now_ms: u64,
        action: &Action,
        reply: Option<&Value>,
    ) -> Option<Action> {
        let ok = reply.is_some_and(|reply| reply_ok(reply));
        let remaining = reply.and_then(|reply| reply_remaining_ms(reply));
        let holds = reply.is_some_and(|reply| reply_holds(reply, &self.holder));
        if holds {
            self.gate.holder = Some(self.holder.clone());
        }
        match (action.op, ok, remaining, holds) {
            ("get", true, Some(remaining), false) => {
                // A live foreign incumbent: poll past its leader-echoed
                // expiry with jitter; it may renew out from under us.
                self.gate.schedule = Some(now_ms + remaining + self.rng.below(100));
                None
            }
            ("get", true, Some(_), _) => {
                // The probe found our own lease live again: re-adopt at
                // the tight race window the free path races in.
                self.gate.schedule = Some(now_ms + self.rng.below(50));
                None
            }
            ("get", true, None, _) => {
                // Free or expired: race to SET immediately — the
                // follow-up the caller submits on the spot.
                self.gate.schedule = Some(now_ms);
                Some(self.build_set(now_ms, "set"))
            }
            ("bump", true, Some(remaining), _) => {
                // A renewal (BUMP): the same-holder regrant extends the
                // lease by one window from now; the next renewal sits one
                // renewal-margin inside the leader-echoed deadline.
                self.gate.schedule =
                    Some(now_ms + remaining.saturating_sub(self.renew_margin));
                None
            }
            ("set", _, _, _) => {
                // The SET race's outcome: the machine stakes the
                // holdership the race claimed and schedules its renewal
                // — a lost race corrects on the next renewal's denial
                // (the backoff re-probes).
                self.gate.holder = Some(self.holder.clone());
                self.gate.schedule = Some(now_ms + self.config.lease_ms - self.renew_margin);
                None
            }
            _ => {
                // Error, rejection, or an unparseable reply: back off and
                // probe again.
                self.gate.schedule = Some(now_ms + 100 + self.rng.below(200));
                None
            }
        }
    }

    /// The GET probe request (the chase's eyes; the wire client's
    /// read-only verb).
    fn build_get(&mut self) -> Action {
        self.gate.request_num += 1;
        let message_id = *uuid::Uuid::new_v4().as_bytes();
        let request = format!(
            "{{\"op\":\"get\",\"message_id\":\"{}\",\"client_id\":{},\"request_num\":{},\
             \"lock_id\":{}}}",
            uuid::Uuid::from_bytes(message_id),
            self.config.client_id,
            self.gate.request_num,
            self.config.lock_id
        );
        Action {
            op: "get",
            message_id,
            request,
        }
    }

    /// The SET request both the acquire race and the same-holder
    /// renewal (BUMP) send: one lease window from `now`, this
    /// contender's holder identity, and the gate's lease-id /
    /// request-num bookkeeping advanced.
    fn build_set(&mut self, now_ms: u64, op: &'static str) -> Action {
        self.gate.request_num += 1;
        self.gate.lease_id += 1;
        let message_id = *uuid::Uuid::new_v4().as_bytes();
        let expiry = now_ms + self.config.lease_ms;
        let request = format!(
            "{{\"op\":\"set\",\"message_id\":\"{}\",\"client_id\":{},\"request_num\":{},\
             \"lock_id\":{},\"lease\":{{\"lease_id\":{},\"holder\":\"{}\",\"expiry\":{expiry}}}}}",
            uuid::Uuid::from_bytes(message_id),
            self.config.client_id,
            self.gate.request_num,
            self.config.lock_id,
            self.gate.lease_id,
            self.holder
        );
        Action {
            op,
            message_id,
            request,
        }
    }
}

/// The process-level signal gate: SIGUSR1 (silence) and SIGUSR2 (start)
/// arrive to the process, not to a worker. The async handler's only
/// move is the signal stream's self-pipe write; a dedicated arbiter
/// reads the stream and latches the process mode (OFF / ON), logging
/// each real transition exactly once on the client's normal stdout
/// stream. Every worker — the load binary's contender and getter
/// loops, the host runner's embedded clients — re-reads the latched
/// mode at its own sleep/every-iteration boundary and moves its own
/// gate to match: a signal is a process transition, never an edge one
/// worker's apply can consume ahead of another's, and a worker that
/// slept through a signal still follows it on wake. No request or
/// reply encoding changes — the gate sits entirely outside the wire.
#[derive(Clone)]
pub struct Signals {
    on: Arc<AtomicBool>,
}

impl Signals {
    /// Register the SIGUSR1/SIGUSR2 stream and start the arbiter that
    /// latches the process mode.
    pub fn register() -> Signals {
        let on = Arc::new(AtomicBool::new(false));
        let mut signals = signal_hook::iterator::Signals::new(&[
            signal_hook::consts::SIGUSR1,
            signal_hook::consts::SIGUSR2,
        ])
        .expect("SIGUSR1/SIGUSR2 registration");
        let latch = Arc::clone(&on);
        std::thread::spawn(move || {
            for signal in signals.forever() {
                if signal == signal_hook::consts::SIGUSR1 {
                    if latch.swap(false, Ordering::Relaxed) {
                        println!("client stop (SIGUSR1) at wall={}", wall_ms());
                    }
                } else if signal == signal_hook::consts::SIGUSR2 {
                    if !latch.swap(true, Ordering::Relaxed) {
                        println!("client start (SIGUSR2) at wall={}", wall_ms());
                    }
                }
            }
        });
        Signals { on }
    }

    /// Latch silence exactly as the arbiter does on SIGUSR1 (the
    /// in-process test seam; the real path is the OS signal).
    pub fn signal_silence(&self) {
        self.on.store(false, Ordering::Relaxed);
    }

    /// Latch start exactly as the arbiter does on SIGUSR2.
    pub fn signal_start(&self) {
        self.on.store(true, Ordering::Relaxed);
    }

    /// The latched process mode.
    pub fn on(&self) -> bool {
        self.on.load(Ordering::Relaxed)
    }

    /// The per-worker form the lease-load binary uses: move this
    /// worker's gate to the latched process mode. Idempotent — a gate
    /// already in the process mode is untouched, so repeated signals in
    /// the same mode are no-ops and a worker that slept through a
    /// signal still follows it on wake.
    pub fn apply(&self, gate: &mut Gate) {
        let on = self.on.load(Ordering::Relaxed);
        if on == (gate.mode == Mode::On) {
            return;
        }
        let now = wall_ms();
        if on {
            client_gate::start(gate, now);
        } else {
            client_gate::stop(gate, now);
        }
    }
}

/// One embedded client's in-flight op: the submitted action and the
/// wall-ms deadline after which the op is abandoned and the chase backs
/// off.
struct Pending {
    action: Action,
    deadline: u64,
}

/// One embedded client: a contender plus its in-flight op.
struct Client {
    contender: Contender,
    pending: Option<Pending>,
}

impl Client {
    /// One pump step under the gate: at most one op is in flight at a
    /// time (the wire loop blocks on its round trip; the runner mirrors
    /// it with the pending discipline) — an overdue pending expires
    /// into the backoff, and only an idle client submits its next due
    /// action through the caller's transport.
    fn step(&mut self, now_ms: u64, deadline_ms: u64, submit: &mut dyn FnMut(&Action) -> bool) {
        if let Some(pending) = &self.pending {
            if now_ms >= pending.deadline {
                let pending = self.pending.take().expect("the deadline check just matched");
                let _ = self.contender.absorb(now_ms, &pending.action, None);
            } else {
                return;
            }
        }
        let Some(action) = self.contender.next_action(now_ms) else {
            return;
        };
        if submit(&action) {
            self.pending = Some(Pending {
                action,
                deadline: now_ms + deadline_ms,
            });
        } else {
            // Refused (not the leader, no route): absorb it as an error
            // and the chase backs off.
            let _ = self.contender.absorb(now_ms, &action, None);
        }
    }

    /// Absorb one pending op's reply and submit the immediate follow-up
    /// (the free-probe SET race) the absorb decides on.
    fn complete(
        &mut self,
        now_ms: u64,
        deadline_ms: u64,
        pending: Pending,
        reply: Option<Value>,
        submit: &mut dyn FnMut(&Action) -> bool,
    ) {
        let follow = self
            .contender
            .absorb(now_ms, &pending.action, reply.as_ref());
        if let Some(action) = follow {
            if submit(&action) {
                self.pending = Some(Pending {
                    action,
                    deadline: now_ms + deadline_ms,
                });
            } else {
                let _ = self.contender.absorb(now_ms, &action, None);
            }
        }
    }
}

/// The host-side runner: N embedded contender loops behind one process
/// signal gate, ticked from the host loop (no threads). The transport
/// facing side is the caller's: `tick` submits each due action through
/// the submit closure (whatever routes it to the leader), and the host
/// feeds back every reply it sees by message id — `absorb` claims the
/// ones the runner owns.
pub struct Runner {
    clients: Vec<Client>,
    signals: Signals,
    deadline_ms: u64,
    on: bool,
}

impl Runner {
    /// Boot the runner: every client's gate starts OFF, and the process
    /// gate mirrors that (the first SIGUSR2 logs the start transition).
    /// `seed_base` feeds each client's holder-identity draw
    /// (`seed_base ^ (index + 1)`) — one process passes its own
    /// `wall ^ pid` mix so client identities never collide across
    /// hosts.
    pub fn new(
        count: usize,
        config: Config,
        signals: Signals,
        deadline_ms: u64,
        seed_base: u64,
    ) -> Runner {
        let clients = (0..count)
            .map(|index| {
                let client_config = Config {
                    client_id: config.client_id + index as u64,
                    ..config.clone()
                };
                let seed = seed_base ^ (index as u64 + 1);
                Client {
                    contender: Contender::new(client_config, seed),
                    pending: None,
                }
            })
            .collect();
        Runner {
            clients,
            signals,
            deadline_ms,
            on: false,
        }
    }

    /// The process signal gate (the flags the OS handler lights).
    pub fn signals(&self) -> &Signals {
        &self.signals
    }

    /// Client `index`'s holder identity.
    pub fn holder(&self, index: usize) -> Option<&str> {
        self.clients
            .get(index)
            .map(|client| client.contender.holder())
    }

    /// Whether client `index` currently holds the lock it chases.
    pub fn holds(&self, index: usize) -> bool {
        self.clients
            .get(index)
            .is_some_and(|client| client.contender.holds())
    }

    /// Client `index`'s gate (the test surface for mode and
    /// bookkeeping assertions).
    pub fn gate(&self, index: usize) -> Option<&Gate> {
        self.clients.get(index).map(|client| &client.contender.gate)
    }

    /// Client `index`'s in-flight message id, if any.
    pub fn pending_id(&self, index: usize) -> Option<[u8; 16]> {
        self.clients
            .get(index)
            .and_then(|client| client.pending.as_ref().map(|pending| pending.action.message_id))
    }

    /// One host-loop tick: move every embedded client's gate to the
    /// latched process signal mode — one process, one transition, one
    /// log line — then step every client. The submit closure routes one
    /// action to the leader and reports whether it was accepted for
    /// proposal (or forwarded); a refusal is absorbed as an error and
    /// the chase backs off.
    pub fn tick(&mut self, now_ms: u64, submit: &mut dyn FnMut(&Action) -> bool) {
        // Silence also abandons any in-flight op (a reply that arrives
        // late is ignored, not absorbed).
        let on = self.signals.on();
        if on != self.on {
            if on {
                for client in &mut self.clients {
                    client_gate::start(client.contender.gate(), now_ms);
                }
                println!("client start (SIGUSR2) at wall={now_ms}");
            } else {
                for client in &mut self.clients {
                    client_gate::stop(client.contender.gate(), now_ms);
                    client.pending = None;
                }
                println!("client stop (SIGUSR1) at wall={now_ms}");
            }
            self.on = on;
        }
        let deadline_ms = self.deadline_ms;
        for client in &mut self.clients {
            client.step(now_ms, deadline_ms, submit);
        }
    }

    /// Feed one reply to the runner: the client whose pending op carries
    /// this message id absorbs it (and submits the follow-up the
    /// absorption decides on); an unknown message id is not claimed.
    pub fn absorb(
        &mut self,
        now_ms: u64,
        message_id: &[u8; 16],
        reply: &[u8],
        submit: &mut dyn FnMut(&Action) -> bool,
    ) -> bool {
        let Some(index) = self.clients.iter().position(|client| {
            client
                .pending
                .as_ref()
                .is_some_and(|pending| &pending.action.message_id == message_id)
        }) else {
            return false;
        };
        let deadline_ms = self.deadline_ms;
        let client = &mut self.clients[index];
        let pending = client.pending.take().expect("the position check matched");
        let parsed: Option<Value> = serde_json::from_slice(reply).ok();
        client.complete(now_ms, deadline_ms, pending, parsed, submit);
        true
    }

    /// A forwarded op's not-leader refusal: the pending op is dropped
    /// and the chase backs off (the next action is a probe).
    pub fn not_leader(&mut self, now_ms: u64, message_id: &[u8; 16]) -> bool {
        let Some(client) = self.clients.iter_mut().find(|client| {
            client
                .pending
                .as_ref()
                .is_some_and(|pending| &pending.action.message_id == message_id)
        }) else {
            return false;
        };
        let pending = client.pending.take().expect("the find matched");
        let _ = client.contender.absorb(now_ms, &pending.action, None);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn config() -> Config {
        Config {
            lock_id: 0x0DDBA12,
            client_id: 800_000,
            lease_ms: 500,
            renew_fraction: 0.5,
        }
    }

    fn contender() -> Contender {
        Contender::new(config(), 0xDEAD_BEEF)
    }

    fn started(contender: &mut Contender) {
        client_gate::start(contender.gate(), 1000);
    }

    fn probe_action(contender: &mut Contender) -> Action {
        started(contender);
        contender
            .next_action(1000)
            .expect("a started contender probes")
    }

    /// A GET reply naming `holder` with `remaining` ms left on the
    /// leader's timeline.
    fn get_reply(remaining: u64, holder: &str) -> Value {
        json!({
            "op": "get",
            "lease": {"lease_id": 5, "holder": holder, "expiry": 10_000 + remaining},
            "executed_at": 10_000,
        })
    }

    /// A granted SET reply (the race or a renewal) for `holder`.
    fn set_reply(granted: bool, holder: &str) -> Value {
        json!({
            "op": "set",
            "granted": granted,
            "lease": {"lease_id": 6, "holder": holder, "expiry": 10_500},
            "executed_at": 10_000,
        })
    }

    // ------------------------------------------------------------ Contender ----

    #[test]
    fn contender_boots_silent_and_schedules_nothing() {
        let mut contender = contender();
        assert_eq!(contender.mode(), Mode::Off);
        assert_eq!(contender.next_action(2000), None);
        assert_eq!(contender.scheduled_at(), None);
        assert!(!contender.holds());
    }

    #[test]
    fn started_contender_probes_with_a_get_request() {
        let mut contender = contender();
        let action = probe_action(&mut contender);
        assert_eq!(action.op, "get");
        let value: Value = serde_json::from_str(&action.request).expect("valid json");
        assert_eq!(value["op"], "get");
        assert_eq!(value["client_id"], 800_000);
        assert_eq!(value["request_num"], 1);
        assert_eq!(value["lock_id"], 0x0DDBA12 as u64);
        let message_id = uuid::Uuid::parse_str(value["message_id"].as_str().expect("uuid"))
            .expect("parseable message id");
        assert_eq!(action.message_id, *message_id.as_bytes());
    }

    #[test]
    fn free_probe_races_inline_with_a_set() {
        let mut contender = contender();
        let holder = contender.holder().to_string();
        let probe = probe_action(&mut contender);
        let free = json!({"op": "get", "lease": null, "executed_at": 10_000});
        let race = contender
            .absorb(10_010, &probe, Some(&free))
            .expect("a free probe races to SET");
        assert_eq!(race.op, "set");
        let value: Value = serde_json::from_str(&race.request).expect("valid json");
        assert_eq!(value["op"], "set");
        assert_eq!(value["client_id"], 800_000);
        assert_eq!(value["request_num"], 2);
        assert_eq!(value["lock_id"], 0x0DDBA12 as u64);
        assert_eq!(value["lease"]["holder"], holder.as_str());
        assert_eq!(value["lease"]["expiry"], 10_010 + 500);
        assert_eq!(value["lease"]["lease_id"], 1);
        // The race stakes holdership; the gate adopts it only once the
        // race's own reply lands.
        assert!(!contender.holds());
    }

    #[test]
    fn set_reply_adopts_holdership_and_schedules_the_renewal() {
        let mut contender = contender();
        let holder = contender.holder().to_string();
        let probe = probe_action(&mut contender);
        let free = json!({"op": "get", "lease": null, "executed_at": 10_000});
        let race = contender
            .absorb(10_010, &probe, Some(&free))
            .expect("the free-probe SET race");
        let granted = set_reply(true, &holder);
        assert_eq!(
            contender.absorb(10_020, &race, Some(&granted)),
            None,
            "the race's reply schedules no follow-up"
        );
        assert!(contender.holds());
        // The renewal is one renewal-margin inside the window the race
        // asked for: 500 - 250 = 250 ms past the absorb's now.
        assert_eq!(contender.scheduled_at(), Some(10_020 + 250));
        let renewal = contender
            .next_action(10_270)
            .expect("the renewal is due at the schedule");
        assert_eq!(renewal.op, "bump");
    }

    #[test]
    fn foreign_incumbent_polls_past_expiry_with_jitter() {
        let mut contender = contender();
        let probe = probe_action(&mut contender);
        let foreign = get_reply(300, "22222222-2222-2222-2222-222222222222");
        assert_eq!(contender.absorb(1000, &probe, Some(&foreign)), None);
        let at = contender.scheduled_at().expect("the poll is scheduled");
        assert!(
            (1300..1400).contains(&at),
            "poll past the leader-echoed expiry with <100 ms jitter, got {at}"
        );
        assert!(!contender.holds());
        // The poll is a probe, and the probe is against the same lock.
        let poll = contender.next_action(at).expect("the poll fires");
        assert_eq!(poll.op, "get");
        let value: Value = serde_json::from_str(&poll.request).expect("valid json");
        assert_eq!(value["lock_id"], 0x0DDBA12 as u64);
    }

    #[test]
    fn own_live_lease_probe_reschedules_a_tight_renewal() {
        let mut contender = contender();
        let holder = contender.holder().to_string();
        let probe = probe_action(&mut contender);
        let own = get_reply(300, &holder);
        assert_eq!(contender.absorb(1000, &probe, Some(&own)), None);
        assert!(contender.holds(), "the probe found our own lease live");
        let at = contender.scheduled_at().expect("the re-probe is scheduled");
        assert!(
            (1000..1050).contains(&at),
            "the re-adopt is inside the tight race window, got {at}"
        );
        // The rescheduled op is a renewal, not another probe.
        assert_eq!(contender.next_action(at).expect("due").op, "bump");
    }

    #[test]
    fn renewal_reply_schedules_the_next_renewal_inside_the_window() {
        let mut contender = contender();
        let holder = contender.holder().to_string();
        let probe = probe_action(&mut contender);
        let free = json!({"op": "get", "lease": null, "executed_at": 10_000});
        let race = contender
            .absorb(10_010, &probe, Some(&free))
            .expect("the race");
        let granted = set_reply(true, &holder);
        contender.absorb(10_020, &race, Some(&granted));
        let renewal = contender
            .next_action(10_270)
            .expect("the renewal is due");
        assert_eq!(renewal.op, "bump");
        // The leader echoed 300 ms remaining: the next renewal is
        // scheduled one renewal-margin inside that (300 - 250 = 50 ms).
        let renewed = get_reply(300, &holder);
        assert_eq!(contender.absorb(10_280, &renewal, Some(&renewed)), None);
        assert_eq!(contender.scheduled_at(), Some(10_280 + 50));
    }

    #[test]
    fn error_or_unparseable_reply_backs_off_and_reprobes() {
        let mut contender = contender();
        let probe = probe_action(&mut contender);
        let not_leader = json!({"error": "not_leader"});
        assert_eq!(contender.absorb(1000, &probe, Some(&not_leader)), None);
        let at = contender.scheduled_at().expect("the backoff is scheduled");
        assert!(
            (1100..1300).contains(&at),
            "the backoff is 100-300 ms out, got {at}"
        );
        assert!(!contender.holds());
        assert_eq!(contender.next_action(at).expect("due").op, "get");
        // An unparseable reply (a dropped connection) backs off too.
        let probe = contender.next_action(at).expect("due");
        assert_eq!(contender.absorb(5000, &probe, None), None);
        let at = contender.scheduled_at().expect("the backoff is scheduled");
        assert!(
            (5100..5300).contains(&at),
            "the second backoff is 100-300 ms out, got {at}"
        );
    }

    #[test]
    fn silence_mid_holdership_forgets_and_restarts_with_a_probe() {
        let mut contender = contender();
        let holder = contender.holder().to_string();
        let probe = probe_action(&mut contender);
        let free = json!({"op": "get", "lease": null, "executed_at": 10_000});
        let race = contender
            .absorb(10_010, &probe, Some(&free))
            .expect("the race");
        let granted = set_reply(true, &holder);
        contender.absorb(10_020, &race, Some(&granted));
        assert!(contender.holds());
        client_gate::stop(contender.gate(), 2000);
        assert_eq!(contender.mode(), Mode::Off);
        assert!(!contender.holds(), "silence forgets holdership");
        assert_eq!(contender.scheduled_at(), None);
        client_gate::start(contender.gate(), 3000);
        let restart = contender.next_action(3000).expect("a restart probes");
        assert_eq!(restart.op, "get", "restart must probe, never re-BUMP");
    }

    // -------------------------------------------------------------- Runner ----

    fn runner(count: usize) -> Runner {
        Runner::new(count, config(), Signals::register(), 1000, 0x5EED_BA5E)
    }

    #[test]
    fn runner_boots_every_client_silent() {
        let runner = runner(2);
        assert_eq!(runner.gate(0).expect("client 0").mode, Mode::Off);
        assert_eq!(runner.gate(1).expect("client 1").mode, Mode::Off);
        let mut runner = runner;
        runner.tick(1000, &mut |_| panic!("a silent runner must not submit"));
    }

    #[test]
    fn runner_start_probes_for_every_client() {
        let mut runner = runner(2);
        runner.signals().signal_start();
        let mut submitted: Vec<Action> = Vec::new();
        runner.tick(1000, &mut |action| {
            submitted.push(action.clone());
            true
        });
        assert_eq!(submitted.len(), 2, "one probe per client");
        assert!(submitted.iter().all(|action| action.op == "get"));
        let first: Value = serde_json::from_str(&submitted[0].request).unwrap();
        let second: Value = serde_json::from_str(&submitted[1].request).unwrap();
        assert_eq!(first["client_id"], 800_000);
        assert_eq!(second["client_id"], 800_001);
        assert_ne!(
            runner.pending_id(0).expect("client 0 pending"),
            runner.pending_id(1).expect("client 1 pending")
        );
    }

    #[test]
    fn runner_absorb_routes_the_free_probe_race_inline() {
        let mut runner = runner(1);
        runner.signals().signal_start();
        let mut submitted: Vec<Action> = Vec::new();
        runner.tick(1000, &mut |action| {
            submitted.push(action.clone());
            true
        });
        let pending = runner.pending_id(0).expect("the probe is pending");
        let free = json!({"op": "get", "lease": null, "executed_at": 10_000})
            .to_string()
            .into_bytes();
        let mut race: Option<Action> = None;
        assert!(runner.absorb(1010, &pending, &free, &mut |action| {
            race = Some(action.clone());
            true
        }));
        let race = race.expect("the free probe races to SET inline");
        assert_eq!(race.op, "set");
        assert_eq!(
            runner.pending_id(0).expect("the race is pending"),
            race.message_id,
            "the follow-up is the new in-flight op"
        );
    }

    #[test]
    fn runner_reply_for_an_unknown_message_id_is_not_claimed() {
        let mut runner = runner(1);
        runner.signals().signal_start();
        runner.tick(1000, &mut |_| true);
        let unknown = [0xAA; 16];
        assert!(!runner.absorb(1010, &unknown, b"{}", &mut |_| true));
    }

    #[test]
    fn runner_pending_deadline_expires_into_a_backoff() {
        let mut runner = runner(1);
        runner.signals().signal_start();
        let mut submitted: Vec<Action> = Vec::new();
        runner.tick(1000, &mut |action| {
            submitted.push(action.clone());
            true
        });
        // The 1000 ms op deadline passes with no reply: the op is
        // abandoned and the chase backs off; the next probe is due at
        // 100-300 ms past the expiry tick.
        runner.tick(2100, &mut |action| {
            submitted.push(action.clone());
            true
        });
        assert!(
            submitted.iter().filter(|a| a.op == "get").count() == 1,
            "the backoff has not re-probed yet at the expiry tick"
        );
        runner.tick(2500, &mut |action| {
            submitted.push(action.clone());
            true
        });
        assert_eq!(
            submitted.iter().filter(|a| a.op == "get").count(),
            2,
            "the backoff elapsed and the chase re-probed"
        );
    }

    #[test]
    fn runner_holds_one_op_in_flight_at_a_time() {
        let mut runner = runner(1);
        runner.signals().signal_start();
        let mut submitted: Vec<Action> = Vec::new();
        runner.tick(1000, &mut |action| {
            submitted.push(action.clone());
            true
        });
        // The probe is still in flight (no reply): the next ticks must
        // not build or submit anything — the wire loop blocks on its
        // round trip; the runner mirrors it with the pending discipline.
        runner.tick(1005, &mut |action| {
            submitted.push(action.clone());
            true
        });
        runner.tick(1010, &mut |action| {
            submitted.push(action.clone());
            true
        });
        assert_eq!(submitted.len(), 1, "one op in flight at a time");
        assert_eq!(runner.pending_id(0), Some(submitted[0].message_id));
    }

    #[test]
    fn runner_not_leader_drops_the_pending_and_backs_off() {
        let mut runner = runner(1);
        runner.signals().signal_start();
        runner.tick(1000, &mut |_| true);
        let pending = runner.pending_id(0).expect("the probe is pending");
        assert!(runner.not_leader(1000, &pending));
        assert_eq!(runner.pending_id(0), None);
        let at = runner
            .gate(0)
            .expect("client 0")
            .schedule
            .expect("the backoff is scheduled");
        assert!(
            (1100..1300).contains(&at),
            "the backoff is 100-300 ms out, got {at}"
        );
        assert!(!runner.not_leader(1000, &[0xBB; 16]));
    }

    #[test]
    fn runner_silence_stops_every_client_and_abandons_in_flight_ops() {
        let mut runner = runner(2);
        runner.signals().signal_start();
        runner.tick(1000, &mut |_| true);
        let pending = runner.pending_id(0).expect("the probe is in flight");
        runner.signals().signal_silence();
        runner.tick(1100, &mut |_| panic!("a silenced runner must not submit"));
        assert_eq!(runner.gate(0).expect("client 0").mode, Mode::Off);
        assert_eq!(runner.gate(1).expect("client 1").mode, Mode::Off);
        assert_eq!(runner.pending_id(0), None, "the in-flight op is abandoned");
        assert_eq!(runner.pending_id(1), None);
        // The abandoned op's late reply is ignored, not absorbed.
        let free = json!({"op": "get", "lease": null, "executed_at": 10_000})
            .to_string()
            .into_bytes();
        assert!(!runner.absorb(1200, &pending, &free, &mut |_| {
            panic!("an abandoned op's reply must not race")
        }));
        assert_eq!(runner.gate(0).expect("client 0").schedule, None);
    }

    // ------------------------------------------------------------- Signals ----

    /// One SIGUSR1 is a process transition: every worker's gate follows
    /// it, whichever worker's apply ran first — the drainer awake at
    /// the signal, the sleeper only on wake. The regression is the
    /// lost-flag race: an edge consumed by the first apply must never
    /// starve the second gate.
    #[test]
    fn one_silence_signal_reaches_every_gate_not_just_the_first_drainer() {
        let signals = Signals::register();
        let mut drainer = client_gate::boot();
        let mut sleeper = client_gate::boot();
        for gate in [&mut drainer, &mut sleeper] {
            client_gate::start(gate, 1000);
            gate.holder = Some("holder-identity".to_string());
            gate.schedule = Some(2000);
        }
        signals.signal_silence();
        // The drainer applies first; the sleeper applies only on wake.
        signals.apply(&mut drainer);
        signals.apply(&mut sleeper);
        assert_eq!(drainer.mode, Mode::Off);
        assert_eq!(
            sleeper.mode,
            Mode::Off,
            "the sleeper follows the process silence on wake"
        );
        assert!(
            sleeper.holder.is_none(),
            "the late-applied silence still forgets holdership"
        );
        assert_eq!(sleeper.schedule, None);
        // The start direction is a process transition too.
        signals.signal_start();
        signals.apply(&mut drainer);
        signals.apply(&mut sleeper);
        assert_eq!(drainer.mode, Mode::On);
        assert_eq!(sleeper.mode, Mode::On);
        // A gate that followed the silence late restarts as a probe,
        // never a blind re-BUMP (the item01 discipline holds).
        assert_eq!(
            client_gate::next_op(&sleeper, 3000),
            Some(client_gate::Op::Get)
        );
    }

    /// Repeated signals in the same mode are level no-ops: a gate
    /// already in the process mode is untouched by a later apply.
    #[test]
    fn repeated_signals_in_the_same_mode_leave_a_matching_gate_untouched() {
        let signals = Signals::register();
        let mut gate = client_gate::boot();
        signals.signal_start();
        signals.apply(&mut gate);
        assert_eq!(gate.mode, Mode::On);
        gate.request_num = 7;
        signals.signal_start();
        signals.apply(&mut gate);
        assert_eq!(gate.mode, Mode::On);
        assert_eq!(gate.request_num, 7, "an already-on gate is untouched");
        signals.signal_silence();
        signals.apply(&mut gate);
        assert_eq!(gate.mode, Mode::Off);
        signals.signal_silence();
        signals.apply(&mut gate);
        assert_eq!(gate.mode, Mode::Off);
    }
}
