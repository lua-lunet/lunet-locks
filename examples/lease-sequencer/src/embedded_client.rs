use crate::client_gate::{self, Gate, Mode, Op};
use serde_json::Value;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};
use uuid::Uuid;

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

#[derive(Clone)]
pub struct Config {
    pub lock_id: u64,
    pub client_id: u64,
    pub lease_ms: u64,
    pub renew_fraction: f64,
    pub probe_floor_ms: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Action {
    pub op: &'static str,
    pub message_id: [u8; 16],
    pub request: String,
}

pub fn reply_ok(reply: &Value) -> bool {
    reply.get("error").is_none()
}

pub fn reply_granted(reply: &Value, op: &str) -> bool {
    if !reply_ok(reply) {
        return false;
    }
    match op {
        "set" | "extend" => reply.get("granted").and_then(|g| g.as_bool()) == Some(true),
        _ => true,
    }
}

pub fn reply_remaining_ms(reply: &Value) -> Option<u64> {
    let lease = reply.get("lease")?;
    let expiry = lease.get("expiry")?.as_u64()?;
    let executed_at = reply.get("executed_at")?.as_u64()?;
    Some(expiry.saturating_sub(executed_at))
}

fn is_wire_uuid(text: &str) -> bool {
    Uuid::parse_str(text).is_ok_and(|parsed| parsed.to_string() == text)
}

fn reply_holds(reply: &Value, holder: &str) -> bool {
    assert!(
        is_wire_uuid(holder),
        "the contender's identity must be the wire's canonical UUID form: {holder}"
    );
    reply
        .get("lease")
        .and_then(|lease| lease.get("holder"))
        .and_then(|lease_holder| lease_holder.as_str())
        .is_some_and(|lease_holder| {
            assert!(
                is_wire_uuid(lease_holder),
                "the leader's echoed holder must be the wire's canonical UUID form: {lease_holder}"
            );
            lease_holder == holder
        })
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

pub struct Contender {
    config: Config,
    gate: Gate,
    holder: String,
    renew_margin: u64,
    rng: Rng,
}

impl Contender {
    pub fn new(config: Config, seed: u64) -> Contender {
        let renew_margin = (config.lease_ms as f64 * (1.0 - config.renew_fraction)) as u64;
        let mut rng = Rng::new(seed);
        let holder = Uuid::from_u64_pair(0, rng.next()).to_string();
        assert!(
            is_wire_uuid(&holder),
            "the drawn identity must be the wire's canonical UUID form: {holder}"
        );
        Contender {
            config,
            gate: client_gate::boot(),
            holder,
            renew_margin,
            rng,
        }
    }

    pub fn gate(&mut self) -> &mut Gate {
        &mut self.gate
    }

    pub fn mode(&self) -> Mode {
        self.gate.mode
    }

    pub fn holder(&self) -> &str {
        &self.holder
    }

    pub fn holds(&self) -> bool {
        self.gate.holder.is_some()
    }

    pub fn scheduled_at(&self) -> Option<u64> {
        self.gate.schedule
    }

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
            Op::Extend => Some(self.build_set(now_ms, "extend")),
            Op::Set => unreachable!("the gate schedules probes and renewals only"),
        }
    }

    pub fn absorb(
        &mut self,
        now_ms: u64,
        action: &Action,
        reply: Option<&Value>,
    ) -> Option<Action> {
        let ok = reply.is_some_and(reply_ok);
        let remaining = reply.and_then(reply_remaining_ms);
        let holds = reply.is_some_and(|reply| reply_holds(reply, &self.holder));
        if holds {
            self.gate.holder = Some(self.holder.clone());
        }
        match (action.op, ok, remaining, holds) {
            ("get", true, Some(remaining), false) => {
                self.gate.schedule = Some(
                    now_ms
                        + self
                            .config
                            .probe_floor_ms
                            .max(remaining + self.rng.below(100)),
                );
                None
            }
            ("get", true, Some(_), _) => {
                self.gate.schedule = Some(now_ms + self.rng.below(50));
                None
            }
            ("get", true, None, _) => {
                self.gate.schedule = Some(now_ms);
                Some(self.build_set(now_ms, "set"))
            }
            ("extend", true, Some(remaining), true) => {
                self.gate.schedule = Some(now_ms + remaining.saturating_sub(self.renew_margin));
                None
            }
            ("extend", true, Some(_), false) => {
                self.gate.holder = None;
                self.gate.schedule = Some(now_ms + 100 + self.rng.below(200));
                None
            }
            ("set", _, _, _) => {
                self.gate.holder = Some(self.holder.clone());
                self.gate.schedule = Some(now_ms + self.config.lease_ms - self.renew_margin);
                None
            }
            _ => {
                self.gate.schedule = Some(now_ms + 100 + self.rng.below(200));
                None
            }
        }
    }

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

    fn build_set(&mut self, now_ms: u64, op: &'static str) -> Action {
        self.gate.request_num += 1;
        self.gate.lease_id += 1;
        let message_id = *uuid::Uuid::new_v4().as_bytes();
        let request = format!(
            "{{\"op\":\"set\",\"message_id\":\"{}\",\"client_id\":{},\"request_num\":{},\
             \"lock_id\":{},\"lease\":{{\"lease_id\":{},\"holder\":\"{}\",\"lease_ms\":{}}},\
             \"sent_at_ms\":{now_ms}}}",
            uuid::Uuid::from_bytes(message_id),
            self.config.client_id,
            self.gate.request_num,
            self.config.lock_id,
            self.gate.lease_id,
            self.holder,
            self.config.lease_ms
        );
        Action {
            op,
            message_id,
            request,
        }
    }
}

#[derive(Clone)]
pub struct Signals {
    on: Arc<AtomicBool>,
}

impl Signals {
    pub fn register() -> Signals {
        let on = Arc::new(AtomicBool::new(false));
        let mut signals = signal_hook::iterator::Signals::new([
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
                } else if signal == signal_hook::consts::SIGUSR2
                    && !latch.swap(true, Ordering::Relaxed)
                {
                    println!("client start (SIGUSR2) at wall={}", wall_ms());
                }
            }
        });
        Signals { on }
    }

    pub fn signal_silence(&self) {
        self.on.store(false, Ordering::Relaxed);
    }

    pub fn signal_start(&self) {
        self.on.store(true, Ordering::Relaxed);
    }

    pub fn on(&self) -> bool {
        self.on.load(Ordering::Relaxed)
    }

    pub fn always_on() -> Signals {
        Signals {
            on: Arc::new(AtomicBool::new(true)),
        }
    }

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

struct Pending {
    action: Action,
    deadline: u64,
}

struct Client {
    contender: Contender,
    pending: Option<Pending>,
}

impl Client {
    fn step(
        &mut self,
        now_ms: u64,
        deadline_ms: u64,
        submit: &mut dyn FnMut(&Action) -> bool,
    ) -> u32 {
        let mut expired = 0u32;
        if let Some(pending) = &self.pending {
            if now_ms >= pending.deadline {
                let pending = self
                    .pending
                    .take()
                    .expect("the deadline check just matched");
                let _ = self.contender.absorb(now_ms, &pending.action, None);
                expired += 1;
            } else {
                return expired;
            }
        }
        let Some(action) = self.contender.next_action(now_ms) else {
            return expired;
        };
        if submit(&action) {
            self.pending = Some(Pending {
                action,
                deadline: now_ms + deadline_ms,
            });
        } else {
            let _ = self.contender.absorb(now_ms, &action, None);
        }
        expired
    }

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

pub struct Runner {
    clients: Vec<Client>,
    signals: Signals,
    deadline_ms: u64,
    on: bool,
}

impl Runner {
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

    pub fn signals(&self) -> &Signals {
        &self.signals
    }

    pub fn holder(&self, index: usize) -> Option<&str> {
        self.clients
            .get(index)
            .map(|client| client.contender.holder())
    }

    pub fn holds(&self, index: usize) -> bool {
        self.clients
            .get(index)
            .is_some_and(|client| client.contender.holds())
    }

    pub fn gate(&self, index: usize) -> Option<&Gate> {
        self.clients.get(index).map(|client| &client.contender.gate)
    }

    pub fn pending_id(&self, index: usize) -> Option<[u8; 16]> {
        self.clients.get(index).and_then(|client| {
            client
                .pending
                .as_ref()
                .map(|pending| pending.action.message_id)
        })
    }

    pub fn tick(&mut self, now_ms: u64, submit: &mut dyn FnMut(&Action) -> bool) -> u32 {
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
        let mut expired = 0u32;
        for client in &mut self.clients {
            expired += client.step(now_ms, deadline_ms, submit);
        }
        expired
    }

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
