//! The experiment load client: continuous SET/BUMP/GET traffic at the
//! configured lease cadence (the design's §1.2) with per-operation
//! round-trip timing (§7) and client-side stats.
//!
//! Round trips are measured on the client's monotonic clock: one reading
//! immediately before the request line is written and one immediately
//! after the response line is read, one sample per operation. Expiry is
//! interpreted against the leader's timeline through the echoed execution
//! tick (`executed_at`) each reply carries: the client computes the lease's
//! remaining window as `expiry - executed_at` and schedules its renewal or
//! next poll from that, never assuming synchronized clocks.
//!
//! Stats: one JSON line per window (window percentiles plus cumulative
//! percentiles, so the last line is always the summary) and, on a clean
//! exit, a final JSON object. The bounded reporting interval is the
//! proof-of-life discipline: the clients show what they see without
//! taxing the cluster.
//!
//! The lease cadence knobs mirror the design's numbers: `--lease-ms 100
//! --renew-fraction 0.8` runs the design's cadence exactly; the committed
//! demo's aggressive timing (500 ms lease, renewal at half the window) is
//! the default.
//!
//! The client gate boots every worker OFF: a (re)started client is silent
//! until the first SIGUSR2 (start) and returns to OFF on SIGUSR1
//! (silence). Silence also forgets holdership and resets the lease-id /
//! request-num bookkeeping, so a restarted worker re-enters as a
//! NON-holder — its first action is a GET probe, never a blind BUMP or
//! SET renewal. Transitions are logged on the client's stdout stream.

use lease_sequencer::client_gate::{self, Gate, Mode, Op};
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

struct Options {
    servers: Vec<String>,
    seconds: u64,
    clients: usize,
    getters: usize,
    high_rate: bool,
    lease_ms: u64,
    renew_fraction: f64,
    window_ms: u64,
    stats_out: String,
    lock_id: u64,
    id_base: u64,
}

fn parse_options() -> Options {
    let mut options = Options {
        servers: Vec::new(),
        seconds: 0,
        clients: 1,
        getters: 1,
        high_rate: false,
        lease_ms: 500,
        renew_fraction: 0.5,
        window_ms: 2000,
        stats_out: String::new(),
        lock_id: 0x0DDBA12,
        id_base: 800000,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        let (flag, value) = match argv.get(index + 1) {
            Some(value) => (argv[index].clone(), value.clone()),
            None => {
                eprintln!("lease-load: missing value for {}", argv[index]);
                std::process::exit(2);
            }
        };
        match flag.as_str() {
            "--server" => options.servers.push(value),
            "--seconds" => options.seconds = value.parse().expect("--seconds number"),
            "--clients" => options.clients = value.parse().expect("--clients number"),
            "--getters" => options.getters = value.parse().expect("--getters number"),
            "--rate" => match value.as_str() {
                "low" => options.high_rate = false,
                "high" => options.high_rate = true,
                other => {
                    eprintln!("lease-load: --rate must be low|high, got {other}");
                    std::process::exit(2);
                }
            },
            "--lease-ms" => options.lease_ms = value.parse().expect("--lease-ms number"),
            "--renew-fraction" => {
                options.renew_fraction = value.parse().expect("--renew-fraction number")
            }
            "--window-ms" => options.window_ms = value.parse().expect("--window-ms number"),
            "--stats-out" => options.stats_out = value,
            "--lock-id" => options.lock_id = value.parse().expect("--lock-id number"),
            "--id-base" => options.id_base = value.parse().expect("--id-base number"),
            other => {
                eprintln!("lease-load: unknown argument {other}");
                std::process::exit(2);
            }
        }
        index += 2;
    }
    if options.servers.is_empty() {
        eprintln!(
            "usage: lease-load --server IPv4:PORT [--server IPv4:PORT ...] \
             [--seconds N] [--clients N] [--getters N] [--rate low|high] \
             [--lease-ms N] [--renew-fraction F] [--window-ms N] \
             [--stats-out PATH] [--lock-id N] [--id-base N]"
        );
        std::process::exit(2);
    }
    options
}

fn wall_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

/// The process-level signal flags: SIGUSR1 (silence) and SIGUSR2 (start)
/// arrive asynchronously and light `Arc<AtomicBool>`s; every worker drains
/// both at its sleep/every-iteration boundary into its own client gate.
/// No request or reply encoding changes — the gate sits entirely outside
/// the wire.
#[derive(Clone)]
struct Signals {
    silence: Arc<AtomicBool>,
    start: Arc<AtomicBool>,
}

impl Signals {
    fn register() -> Signals {
        let silence = Arc::new(AtomicBool::new(false));
        let start = Arc::new(AtomicBool::new(false));
        signal_hook::flag::register(signal_hook::consts::SIGUSR1, Arc::clone(&silence))
            .expect("SIGUSR1 registration");
        signal_hook::flag::register(signal_hook::consts::SIGUSR2, Arc::clone(&start))
            .expect("SIGUSR2 registration");
        Signals { silence, start }
    }

    /// Drain both flags into the worker's gate. Transitions are logged on
    /// the client's normal stdout stream; repeated signals in the same
    /// mode are no-ops (the gate was already there).
    fn apply(&self, gate: &mut Gate) {
        let now = wall_ms();
        if self.silence.swap(false, Ordering::Relaxed) {
            let was_on = gate.mode == Mode::On;
            client_gate::stop(gate, now);
            if was_on {
                println!("client stop (SIGUSR1) at wall={now}");
            }
        }
        if self.start.swap(false, Ordering::Relaxed) {
            let was_off = gate.mode == Mode::Off;
            client_gate::start(gate, now);
            if was_off {
                println!("client start (SIGUSR2) at wall={now}");
            }
        }
    }
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

/// One timed operation: the round trip in microseconds from the client's
/// monotonic clock, plus the leader-echoed expiry interpretation.
struct Sample {
    op: &'static str,
    ok: bool,
    rt_us: u64,
    /// Remaining lease in the leader's timeline at execution
    /// (`expiry - executed_at`), when the reply carried a lease.
    remaining_ms: Option<u64>,
}

#[derive(Default)]
struct Stats {
    count: u64,
    ok: u64,
    errors: u64,
    rt_us: Vec<u64>,
    by_op_ok: [u64; 3],
    by_op_err: [u64; 3],
}

fn op_index(op: &str) -> usize {
    match op {
        "get" => 0,
        "set" => 1,
        "bump" => 2,
        _ => unreachable!("known op"),
    }
}

fn absorb(stats: &mut Stats, sample: &Sample) {
    stats.count += 1;
    let index = op_index(sample.op);
    if sample.ok {
        stats.ok += 1;
        stats.by_op_ok[index] += 1;
    } else {
        stats.errors += 1;
        stats.by_op_err[index] += 1;
    }
    stats.rt_us.push(sample.rt_us);
}

fn percentile(sorted: &[u64], fraction: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let rank = ((fraction * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    sorted[rank - 1]
}

fn summarize(stats: &Stats) -> Value {
    let mut sorted = stats.rt_us.clone();
    sorted.sort_unstable();
    let mean = if sorted.is_empty() {
        0.0
    } else {
        sorted.iter().map(|rt| *rt as f64).sum::<f64>() / sorted.len() as f64
    };
    json!({
        "count": stats.count,
        "ok": stats.ok,
        "errors": stats.errors,
        "p50_us": percentile(&sorted, 0.50),
        "p90_us": percentile(&sorted, 0.90),
        "p99_us": percentile(&sorted, 0.99),
        "max_us": sorted.last().copied().unwrap_or(0),
        "mean_us": mean.round() as u64,
        "by_op": {
            "get": {"ok": stats.by_op_ok[0], "err": stats.by_op_err[0]},
            "set": {"ok": stats.by_op_ok[1], "err": stats.by_op_err[1]},
            "bump": {"ok": stats.by_op_ok[2], "err": stats.by_op_err[2]},
        }
    })
}

/// One NDJSON connection to a node's client port, with reconnect across the
/// server list (a killed node's port drops; the load continues at quorum).
struct Link {
    servers: Vec<String>,
    stream: BufReader<TcpStream>,
    server: String,
}

impl Link {
    fn connect(servers: &[String], start: usize) -> Link {
        let mut attempt = start;
        loop {
            let server = servers[attempt % servers.len()].clone();
            match TcpStream::connect(server.as_str()) {
                Ok(stream) => {
                    let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
                    return Link {
                        servers: servers.to_vec(),
                        stream: BufReader::new(stream),
                        server,
                    };
                }
                Err(_) => {
                    std::thread::sleep(Duration::from_millis(200));
                    attempt += 1;
                }
            }
        }
    }

    /// One request line out, one reply line back, both timed on the
    /// monotonic clock (§7). A dropped connection or a timed-out op counts
    /// as an error sample and reconnects.
    fn round_trip(&mut self, request: &str, op: &'static str) -> (Sample, Option<Value>) {
        let start = Instant::now();
        let write = self
            .stream
            .get_mut()
            .write_all(request.as_bytes())
            .and_then(|_| self.stream.get_mut().write_all(b"\n"))
            .and_then(|_| self.stream.get_mut().flush());
        if write.is_err() {
            let rt = start.elapsed().as_micros() as u64;
            self.reconnect();
            return (
                Sample {
                    op,
                    ok: false,
                    rt_us: rt,
                    remaining_ms: None,
                },
                None,
            );
        }
        let mut line = String::new();
        match self.stream.read_line(&mut line) {
            Ok(0) | Err(_) => {
                let rt = start.elapsed().as_micros() as u64;
                self.reconnect();
                return (
                    Sample {
                        op,
                        ok: false,
                        rt_us: rt,
                        remaining_ms: None,
                    },
                    None,
                );
            }
            Ok(_) => {}
        }
        let rt = start.elapsed().as_micros() as u64;
        let reply: Option<Value> = serde_json::from_str(line.trim_end()).ok();
        let ok = reply
            .as_ref()
            .is_some_and(|reply| reply.get("error").is_none());
        // The client-side TCP port answers a non-leader `not_leader` and
        // does not forward: the op is a completed (failed) round trip, and
        // the connection rotates to the next node so the stream follows
        // the leader.
        let not_leader = reply.as_ref().is_some_and(|reply| {
            reply.get("error").and_then(|error| error.as_str()) == Some("not_leader")
        });
        if not_leader {
            self.reconnect();
        }
        let remaining_ms = reply.as_ref().and_then(|reply| {
            let lease = reply.get("lease")?;
            let expiry = lease.get("expiry")?.as_u64()?;
            let executed_at = reply.get("executed_at")?.as_u64()?;
            Some(expiry.saturating_sub(executed_at))
        });
        (
            Sample {
                op,
                ok,
                rt_us: rt,
                remaining_ms,
            },
            reply,
        )
    }

    fn reconnect(&mut self) {
        let index = self
            .servers
            .iter()
            .position(|server| *server == self.server)
            .unwrap_or(0);
        *self = Link::connect(&self.servers, index + 1);
    }
}

/// The contender loop: hold the lock under a lease (SET), renew it one
/// window ahead of the leader-echoed deadline (BUMP as the same-holder
/// regrant), poll as GET while a live incumbent stands, and race to SET
/// when the lock is free or expired — the design's §1.2 cadence.
fn contender(options: &Options, shared: &Arc<Mutex<Vec<Sample>>>, index: usize, signals: &Signals) {
    let mut rng = Rng::new(wall_ms() ^ (index as u64 + 1) ^ (std::process::id() as u64));
    let client_id = options.id_base + index as u64;
    let holder = format!("{:032x}", rng.next());
    let mut link = Link::connect(&options.servers, index);
    // The gate boots OFF (silence until the first SIGUSR2) and is the
    // single owner of the chase state: schedule, holdership identity, and
    // the lease-id/request-num bookkeeping (silence resets it all).
    let mut gate = client_gate::boot();
    // The renewal schedule lives a renewal-margin inside the leader-echoed
    // deadline.
    let renew_margin = (options.lease_ms as f64 * (1.0 - options.renew_fraction)) as u64;
    loop {
        signals.apply(&mut gate);
        if gate.mode == Mode::Off {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        let now = wall_ms();
        if let Some(at) = gate.schedule
            && now < at
        {
            std::thread::sleep(Duration::from_millis((at - now).clamp(1, 20)));
            continue;
        }
        let (op, request) = match client_gate::next_op(&gate, now).expect("gate is On") {
            Op::Get => {
                gate.request_num += 1;
                let message_id = uuid::Uuid::new_v4();
                (
                    "get",
                    format!(
                        "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{},\"lock_id\":{}}}",
                        gate.request_num, options.lock_id
                    ),
                )
            }
            Op::Bump => {
                // A renewal (BUMP): the same-holder regrant extends the
                // lease by one window from now. Reachable only while the
                // gate still holds the lock.
                gate.request_num += 1;
                gate.lease_id += 1;
                let message_id = uuid::Uuid::new_v4();
                let expiry = wall_ms() + options.lease_ms;
                (
                    "bump",
                    format!(
                        "{{\"op\":\"set\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{},\"lock_id\":{},\"lease\":{{\"lease_id\":{},\"holder\":\"{holder}\",\"expiry\":{expiry}}}}}",
                        gate.request_num, options.lock_id, gate.lease_id
                    ),
                )
            }
            Op::Set => unreachable!("the gate schedules probes and renewals only"),
        };
        let (sample, reply) = link.round_trip(&request, op);
        let holds = reply.as_ref().is_some_and(|reply| {
            reply
                .get("lease")
                .and_then(|lease| lease.get("holder"))
                .and_then(|lease_holder| lease_holder.as_str())
                .is_some_and(|lease_holder| lease_holder == holder)
                && reply
                    .get("granted")
                    .and_then(|granted| granted.as_bool())
                    .unwrap_or(true)
        });
        if holds {
            gate.holder = Some(holder.clone());
        }
        gate.schedule = match (op, sample.ok, sample.remaining_ms, holds) {
            ("get", true, Some(remaining), false) => {
                // A live foreign incumbent: poll past its leader-echoed
                // expiry with jitter; it may renew out from under us.
                Some(wall_ms() + remaining + rng.below(100))
            }
            ("get", true, remaining, _) => {
                // Free or expired: race to SET immediately.
                if remaining.is_some() {
                    Some(wall_ms() + rng.below(50))
                } else {
                    let expiry = wall_ms() + options.lease_ms;
                    gate.request_num += 1;
                    gate.lease_id += 1;
                    let message_id = uuid::Uuid::new_v4();
                    let request = format!(
                        "{{\"op\":\"set\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{},\"lock_id\":{},\"lease\":{{\"lease_id\":{},\"holder\":\"{holder}\",\"expiry\":{expiry}}}}}",
                        gate.request_num, options.lock_id, gate.lease_id
                    );
                    let (sample, _) = link.round_trip(&request, "set");
                    shared.lock().unwrap().push(sample);
                    gate.holder = Some(holder.clone());
                    Some(wall_ms() + options.lease_ms - renew_margin)
                }
            }
            ("bump", true, Some(remaining), _) => {
                Some(wall_ms() + remaining.saturating_sub(renew_margin))
            }
            _ => {
                // Error, rejection, or an unparseable reply: back off and
                // probe again.
                Some(wall_ms() + 100 + rng.below(200))
            }
        };
        shared.lock().unwrap().push(sample);
    }
}

/// The getter loop: fixed-interval GET probes at the configured rate,
/// behind the same client gate — booted silent, started on SIGUSR2.
fn getter(options: &Options, shared: &Arc<Mutex<Vec<Sample>>>, index: usize, signals: &Signals) {
    let interval = if options.high_rate { 50 } else { 250 };
    let mut rng = Rng::new(wall_ms() ^ (index as u64 + 1) ^ (std::process::id() as u64));
    let client_id = options.id_base + 1000 + index as u64;
    let mut link = Link::connect(&options.servers, index);
    let mut request_num: u64 = 0;
    let mut gate = client_gate::boot();
    loop {
        signals.apply(&mut gate);
        if gate.mode == Mode::Off {
            std::thread::sleep(Duration::from_millis(20));
            continue;
        }
        request_num += 1;
        let message_id = uuid::Uuid::new_v4();
        let request = format!(
            "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{request_num},\"lock_id\":{}}}",
            options.lock_id
        );
        let (sample, _) = link.round_trip(&request, "get");
        shared.lock().unwrap().push(sample);
        std::thread::sleep(Duration::from_millis(
            interval + rng.below(interval / 4 + 1),
        ));
    }
}

fn main() {
    let options = parse_options();
    let signals = Signals::register();
    let shared = Arc::new(Mutex::new(Vec::<Sample>::new()));
    let mut handles = Vec::new();
    for index in 0..options.clients {
        let options = clone_options(&options);
        let shared = Arc::clone(&shared);
        let signals = signals.clone();
        handles.push(std::thread::spawn(move || {
            contender(&options, &shared, index, &signals)
        }));
    }
    for index in 0..options.getters {
        let options = clone_options(&options);
        let shared = Arc::clone(&shared);
        let signals = signals.clone();
        handles.push(std::thread::spawn(move || {
            getter(&options, &shared, index, &signals)
        }));
    }
    let started = Instant::now();
    let mut stats_out = (!options.stats_out.is_empty())
        .then(|| std::fs::File::create(&options.stats_out).expect("stats-out file"));
    let mut cumulative = Stats::default();
    loop {
        std::thread::sleep(Duration::from_millis(options.window_ms));
        let window: Vec<Sample> = std::mem::take(&mut *shared.lock().unwrap());
        for sample in &window {
            absorb(&mut cumulative, sample);
        }
        let line = json!({
            "kind": "window",
            "ts_ms": wall_ms(),
            "elapsed_s": started.elapsed().as_secs(),
            "window": summarize(&(Stats {
                count: window.len() as u64,
                rt_us: window.iter().map(|s| s.rt_us).collect(),
                ..Stats::default()
            })),
            "cumulative": summarize(&cumulative),
        })
        .to_string();
        if let Some(out) = stats_out.as_mut() {
            let _ = writeln!(out, "{line}");
            let _ = out.flush();
        }
        if options.seconds > 0 && started.elapsed().as_secs() >= options.seconds {
            let final_line = json!({
                "kind": "final",
                "ts_ms": wall_ms(),
                "elapsed_s": started.elapsed().as_secs(),
                "cumulative": summarize(&cumulative),
            });
            println!("{}", serde_json::to_string_pretty(&final_line).unwrap());
            if let Some(out) = stats_out.as_mut() {
                let _ = writeln!(out, "{final_line}");
            }
            std::process::exit(0);
        }
    }
}

fn clone_options(options: &Options) -> Options {
    Options {
        servers: options.servers.clone(),
        seconds: options.seconds,
        clients: options.clients,
        getters: options.getters,
        high_rate: options.high_rate,
        lease_ms: options.lease_ms,
        renew_fraction: options.renew_fraction,
        window_ms: options.window_ms,
        stats_out: options.stats_out.clone(),
        lock_id: options.lock_id,
        id_base: options.id_base,
    }
}
