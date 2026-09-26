//! The bench harness driver (`docs/src/bench-harness.md`).
//!
//! One process owns the run: it writes the three-node descriptor, binds
//! each node identity's store socket, spawns the node processes (real
//! UDP between them, real TCP client ports), serves every marker-store
//! call from memory under the signalled discipline, loads the cluster
//! with the nodes' embedded CAS clients, reads the lock through a TCP
//! client per node, walks the lifecycle scenario ladder, and ends with
//! the oracle verdict: the three CAS chains identical, the client
//! histories consistent, no maybe-invariant voice, the store discipline
//! held. Any failure stops the run; nothing is tolerated and nothing is
//! left running.

use lease_sequencer::bench_oracle as oracle;
use lease_sequencer::bench_store::{NodeDiscipline, serve_socket};
use std::io::{Read, Write};
use std::net::TcpStream;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The lock the embedded clients chase (the rig's `EMBEDDED_LOCK_ID`):
/// the CAS chain the poller observes.
const BENCH_LOCK_ID: u64 = 0x0DDBA12;
/// The poller client ids (disjoint from the embedded clients' base).
const POLLER_CLIENT_BASE: u64 = 900_000;
/// The descriptor id of a live identity: the same system half at the
/// genesis life — (system half << 16) | 1.
fn descriptor_id(live: u32) -> u32 {
    ((live >> 16) << 16) | 1
}

fn millis() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .as_millis() as u64
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

struct Options {
    node_bin: String,
    run_dir: PathBuf,
    duration: u64,
    base_port: u16,
    clients: usize,
    lease_ms: u64,
}

fn parse_options() -> Options {
    let mut options = Options {
        node_bin: String::new(),
        run_dir: PathBuf::from(format!(".tmp/bench-{}", millis())),
        duration: 180,
        base_port: 41100,
        clients: 1,
        lease_ms: 50,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        let flag = &argv[index];
        let Some(value) = argv.get(index + 1) else {
            eprintln!("skaffold_bench_driver: missing value for {flag}");
            std::process::exit(2);
        };
        match flag.as_str() {
            "--node-bin" => options.node_bin = value.clone(),
            "--run-dir" => options.run_dir = PathBuf::from(value),
            "--duration" => options.duration = value.parse().unwrap_or(180),
            "--base-port" => options.base_port = value.parse().unwrap_or(41100),
            "--clients" => options.clients = value.parse().unwrap_or(1),
            "--lease-ms" => options.lease_ms = value.parse().unwrap_or(50),
            other => {
                eprintln!("skaffold_bench_driver: unknown option {other}");
                std::process::exit(2);
            }
        }
        index += 2;
    }
    if options.node_bin.is_empty() {
        eprintln!(
            "usage: skaffold_bench_driver --node-bin PATH [--run-dir PATH] [--duration SECS] \
             [--base-port N] [--clients N] [--lease-ms N]"
        );
        std::process::exit(2);
    }
    options
}

/// One spawned node life. Dropping it SIGKILLs the process (the
/// no-dangle backstop; the scenario's own stops are deliberate).
struct NodeProc {
    child: Child,
}

impl NodeProc {
    fn pid(&self) -> u32 {
        self.child.id()
    }
}

impl Drop for NodeProc {
    fn drop(&mut self) {
        // The in-process kill: no shell, no leaked stderr on the
        // deliberate stops it backstops.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

struct Node {
    name: String,
    id: u32,
    tcp: u16,
    store_sock: PathBuf,
    state: PathBuf,
    log: PathBuf,
    journal: PathBuf,
    flight: PathBuf,
    discipline: Arc<Mutex<NodeDiscipline>>,
    proc: Option<NodeProc>,
}

/// One blocking TCP NDJSON exchange: connect, one line out, one line in.
fn tcp_call(addr: &str, line: &str, timeout: Duration) -> Result<String, String> {
    let stream = TcpStream::connect(addr).map_err(|error| format!("connect {addr}: {error}"))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|error| error.to_string())?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|error| error.to_string())?;
    let mut stream = stream;
    let mut out = line.to_string();
    out.push('\n');
    stream
        .write_all(out.as_bytes())
        .map_err(|error| format!("write {addr}: {error}"))?;
    stream.flush().map_err(|error| error.to_string())?;
    let mut bytes = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        match stream.read(&mut byte) {
            Ok(0) => return Err(format!("{addr} closed the connection")),
            Ok(_) => {
                if byte[0] == b'\n' {
                    break;
                }
                bytes.push(byte[0]);
                if bytes.len() > 65_000 {
                    return Err(format!("{addr} reply overran the line budget"));
                }
            }
            Err(error) => return Err(format!("read {addr}: {error}")),
        }
    }
    String::from_utf8(bytes).map_err(|error| error.to_string())
}

/// A node's rolling log file: `<name>.<date>.log` (the daily rotation
/// puts the date between stem and suffix).
fn is_node_log(file: &str, node: &str) -> bool {
    file.starts_with(&format!("{node}.")) && file.ends_with(".log")
}

/// One poller's observation log: every granted lease it read through a
/// node's service, as (holder, lease_id) pairs.
type ClientHistory = Arc<Mutex<Vec<([u8; 16], u64)>>>;

struct Poller {
    stop: Arc<AtomicBool>,
    history: ClientHistory,
    request_num: u64,
}

impl Poller {
    /// One GET round against the node's client port; a granted lease is
    /// recorded as (holder, lease_id). Errors and non-leader replies
    /// record nothing — the observation is the granted leases only.
    fn step(&mut self, addr: &str) {
        self.request_num += 1;
        let message_id = uuid::Uuid::new_v4();
        let line = format!(
            "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":{},\"request_num\":{},\"lock_id\":{BENCH_LOCK_ID}}}",
            POLLER_CLIENT_BASE + self.request_num % 1000,
            self.request_num
        );
        let Ok(reply) = tcp_call(addr, &line, Duration::from_millis(500)) else {
            return;
        };
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&reply) else {
            return;
        };
        let Some(lease) = value.get("lease") else {
            return;
        };
        if lease.is_null() {
            return;
        }
        let (Some(holder), Some(lease_id)) = (
            lease.get("holder").and_then(|h| h.as_str()),
            lease.get("lease_id").and_then(|i| i.as_u64()),
        ) else {
            return;
        };
        let Ok(holder) = uuid::Uuid::parse_str(holder) else {
            return;
        };
        lock(&self.history).push((*holder.as_bytes(), lease_id));
    }
}

struct Driver {
    options: Options,
    nodes: Vec<Node>,
    failure: Arc<AtomicBool>,
    histories: Vec<ClientHistory>,
    poller_stop: Arc<AtomicBool>,
}

impl Driver {
    fn node_addr(&self, index: usize) -> String {
        format!("127.0.0.1:{}", self.nodes[index].tcp)
    }

    fn violation(&self) -> Option<String> {
        if !self.failure.load(Ordering::Relaxed) {
            return None;
        }
        let mut reasons = Vec::new();
        for node in &self.nodes {
            if let Some(violation) = lock(&node.discipline).violation() {
                reasons.push(format!("{}: {violation}", node.name));
            }
        }
        Some(if reasons.is_empty() {
            "the store service latched a failure".to_string()
        } else {
            reasons.join("; ")
        })
    }

    /// The last few lines of a node's stderr, bounded: the evidence a
    /// failed wait prints (a node that died on boot — the stale-binary
    /// class, a bad option, a refused socket — says why here, and a
    /// bare timeout would bury it in the run directory).
    fn err_tail(&self, index: usize, lines: usize) -> String {
        let node = &self.nodes[index];
        let Ok(text) =
            std::fs::read_to_string(self.options.run_dir.join(format!("{}.err", node.name)))
        else {
            return format!("{}: (no stderr yet)", node.name);
        };
        let mut tail: Vec<&str> = text.lines().rev().take(lines).collect();
        tail.reverse();
        if tail.is_empty() {
            format!("{}: (stderr empty)", node.name)
        } else {
            format!("{}: {}", node.name, tail.join(" | "))
        }
    }

    /// Every live node's stderr tail, one line each — the wait-failure
    /// evidence block.
    fn err_tails(&self) -> String {
        (0..self.nodes.len())
            .filter(|index| self.nodes[*index].proc.is_some())
            .map(|index| self.err_tail(index, 4))
            .collect::<Vec<_>>()
            .join("\n")
    }

    /// The poll-loop wait every step rides: the predicate inside the
    /// budget, the store violation as the instant abort. A timeout
    /// carries the live nodes' stderr tails — the cause of a dead wait
    /// (a node that never booted) is in them, not in the clock.
    fn wait_for(
        &self,
        what: &str,
        budget: Duration,
        mut predicate: impl FnMut(&Self) -> bool,
    ) -> Result<(), String> {
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            if let Some(violation) = self.violation() {
                return Err(format!(
                    "the store discipline failed during {what}: {violation}"
                ));
            }
            if predicate(self) {
                return Ok(());
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        Err(format!(
            "{what} did not land within {} ms\n{}",
            budget.as_millis(),
            self.err_tails()
        ))
    }

    /// The current leader's node index, from the newest `leader` note
    /// across the node logs (the descriptor id is the id's low band).
    /// The rolling files are `<name>.<date>.log` under the run dir.
    fn leader_index(&self) -> Option<usize> {
        let mut newest: Option<(u64, u32)> = None;
        for node in &self.nodes {
            let Ok(entries) = std::fs::read_dir(&self.options.run_dir) else {
                continue;
            };
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                if !is_node_log(name, &node.name) {
                    continue;
                }
                let Ok(text) = std::fs::read_to_string(entry.path()) else {
                    continue;
                };
                for line in text.lines() {
                    let Some(at) = line.rfind("leader leader=") else {
                        continue;
                    };
                    let id: u32 = line[at + "leader leader=".len()..]
                        .split_whitespace()
                        .next()
                        .and_then(|token| token.parse().ok())
                        .unwrap_or(u32::MAX);
                    if id == u32::MAX {
                        continue;
                    }
                    let ts: u64 = line
                        .split_whitespace()
                        .find_map(|token| token.strip_prefix("ts="))
                        .and_then(|token| token.parse().ok())
                        .unwrap_or(0);
                    if newest.is_none_or(|(best, _)| ts >= best) {
                        newest = Some((ts, descriptor_id(id)));
                    }
                }
            }
        }
        let (_, desc) = newest?;
        self.nodes.iter().position(|node| node.id == desc)
    }

    fn serving(&self) -> bool {
        (0..self.nodes.len()).any(|index| {
            self.nodes[index].proc.is_some() && self.get_lease_observation(index).is_some()
        })
    }

    /// One GET through the driver's own exchange (not the poller): the
    /// serving probe and the failover measurement.
    fn get_lease_observation(&self, index: usize) -> Option<()> {
        let message_id = uuid::Uuid::new_v4();
        let line = format!(
            "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":{POLLER_CLIENT_BASE},\"request_num\":1,\"lock_id\":{BENCH_LOCK_ID}}}"
        );
        let reply = tcp_call(&self.node_addr(index), &line, Duration::from_millis(500)).ok()?;
        let value = serde_json::from_str::<serde_json::Value>(&reply).ok()?;
        let lease = value.get("lease")?;
        (!lease.is_null()).then_some(())
    }

    fn abdicate(&self, index: usize) -> Result<(), String> {
        let reply = tcp_call(
            &self.node_addr(index),
            "{\"action\":\"abdicate\"}",
            Duration::from_secs(5),
        )?;
        if reply.contains("\"accepted\":true") {
            Ok(())
        } else {
            Err(format!(
                "{} refused the abdication: {reply}",
                self.nodes[index].name
            ))
        }
    }

    /// Abdicate whoever currently leads: the log-derived leader can be
    /// stale the moment after a failover, and a `not_leader` refusal is
    /// side-effect-free, so the driver simply offers the verb to every
    /// live node in turn until one accepts.
    fn abdicate_leader(&self) -> Result<usize, String> {
        let deadline = Instant::now() + Duration::from_secs(30);
        while Instant::now() < deadline {
            if let Some(violation) = self.violation() {
                return Err(format!(
                    "the store discipline failed during the abdication: {violation}"
                ));
            }
            for index in 0..self.nodes.len() {
                if self.nodes[index].proc.is_none() {
                    continue;
                }
                if self.abdicate(index).is_ok() {
                    return Ok(index);
                }
            }
            std::thread::sleep(Duration::from_millis(100));
        }
        Err(format!(
            "every abdication attempt was refused within 30 s\n{}",
            self.err_tails()
        ))
    }

    fn signal(&self, index: usize, signal: &str) -> Result<(), String> {
        let node = &self.nodes[index];
        let Some(proc) = &node.proc else {
            return Err(format!("{} has no live process to signal", node.name));
        };
        // The deliberate lifecycle signals (TERM, USR1, USR2): std has no
        // signal API, so this is a direct `execvp` of the `kill` system
        // utility — no shell, typed arguments, checked exit status. The
        // repo's "no shell orchestration" rule targets shell scripts;
        // this is not one. The in-process kill backstop (`NodeProc::drop`)
        // is the std call it correctly is.
        let status = Command::new("kill")
            .arg(signal)
            .arg(proc.pid().to_string())
            .status()
            .map_err(|error| format!("kill {signal} {}: {error}", node.name))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("kill {signal} {} exited {status}", node.name))
        }
    }

    fn wait_exit(&mut self, index: usize, budget: Duration) -> Result<(), String> {
        let deadline = Instant::now() + budget;
        let name = self.nodes[index].name.clone();
        loop {
            // try_wait reaps: a zombie reads alive to kill -0, so the
            // child's own status is the only truthful exit check.
            match self.nodes[index]
                .proc
                .as_mut()
                .map(|proc| proc.child.try_wait())
            {
                None | Some(Ok(Some(_))) => return Ok(()),
                Some(Ok(None)) => {}
                Some(Err(error)) => return Err(format!("wait {name}: {error}")),
            }
            if Instant::now() >= deadline {
                return Err(format!(
                    "{name} did not exit within {} ms\n{}",
                    budget.as_millis(),
                    self.err_tail(index, 4)
                ));
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }

    fn spawn(&mut self, index: usize) -> Result<(), String> {
        let node = &self.nodes[index];
        let stdout = std::fs::File::create(self.options.run_dir.join(format!("{}.out", node.name)))
            .map_err(|error| error.to_string())?;
        let stderr = std::fs::File::create(self.options.run_dir.join(format!("{}.err", node.name)))
            .map_err(|error| error.to_string())?;
        let child = Command::new(&self.options.node_bin)
            .arg("--name")
            .arg(&node.name)
            .arg("--config")
            .arg(self.options.run_dir.join("cluster.jsonl"))
            .arg("--client")
            .arg(format!("127.0.0.1:{}", node.tcp))
            .arg("--state")
            .arg(&node.state)
            .arg("--log")
            .arg(&node.log)
            .arg("--bench-store")
            .arg(&node.store_sock)
            .arg("--journal-dir")
            .arg(&node.journal)
            .arg("--embedded-client")
            .arg(self.options.clients.to_string())
            .arg("--client-ttl-ms")
            .arg(self.options.lease_ms.to_string())
            .arg("--heartbeat-ms")
            .arg("10")
            .arg("--election-ms")
            .arg("500")
            .arg("--recovery-ms")
            .arg("500")
            .arg("--phi-timeout-min-ms")
            .arg("100")
            .arg("--phi-timeout-max-ms")
            .arg("500")
            .arg("--viewchange-timeout-min-ms")
            .arg("200")
            .arg("--viewchange-timeout-max-ms")
            .arg("400")
            // The detection knobs: aggressive but sane — phi 8 is the
            // standard accrual threshold; a twitchier detector churns
            // views faster than the rejoin walk completes (the wedge
            // class), which is a bench-config artifact, not evidence.
            .arg("--phi-threshold")
            .arg("8.0")
            .arg("--phi-safety")
            .arg("2.0")
            .env("LUNET_FLIGHT_RECORDER_DIR", &node.flight)
            // The rolling log's env filter (the runners' discipline):
            // the operator's RUST_LOG wins, else info.
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string()),
            )
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .map_err(|error| format!("spawn {}: {error}", node.name))?;
        self.nodes[index].proc = Some(NodeProc { child });
        Ok(())
    }

    fn non_leader_index(&self) -> Result<usize, String> {
        let leader = self
            .leader_index()
            .ok_or_else(|| "no leader is known".to_string())?;
        (0..self.nodes.len())
            .find(|index| *index != leader && self.nodes[*index].proc.is_some())
            .ok_or_else(|| "no live non-leader".to_string())
    }

    fn clean_stop(&mut self, index: usize) -> Result<(), String> {
        lock(&self.nodes[index].discipline).signal_clean_stop();
        self.signal(index, "-TERM")?;
        self.wait_exit(index, Duration::from_secs(10))?;
        self.nodes[index].proc = None;
        Ok(())
    }

    fn crash(&mut self, index: usize) -> Result<(), String> {
        lock(&self.nodes[index].discipline).signal_crash();
        self.signal(index, "-9")?;
        self.wait_exit(index, Duration::from_secs(10))?;
        self.nodes[index].proc = None;
        Ok(())
    }

    fn cycle(&mut self, index: usize, signal: &str) -> Result<(), String> {
        {
            let mut discipline = lock(&self.nodes[index].discipline);
            if signal == "-USR1" {
                discipline.signal_clean_stop();
            } else {
                discipline.signal_crash();
            }
        }
        self.signal(index, signal)
    }

    fn wait_running(&self, index: usize, what: &str) -> Result<(), String> {
        self.wait_for(what, Duration::from_secs(30), move |driver| {
            let discipline = lock(&driver.nodes[index].discipline);
            discipline.is_running()
        })
    }

    fn wait_serving(&self, what: &str) -> Result<(), String> {
        self.wait_for(what, Duration::from_secs(30), Driver::serving)
    }

    /// The scenario ladder (the doc's §The scenario ladder).
    fn ladder(&mut self) -> Result<(), String> {
        // 1. Settle: three nodes boot, elect, serve.
        self.wait_serving("settle: the cluster serves")?;
        let leader = self
            .wait_for(
                "settle: a leader is known",
                Duration::from_secs(30),
                |driver| driver.leader_index().is_some(),
            )
            .map(|_| self.leader_index().expect("the wait just landed"))?;
        println!("bench: settled; leader {}", self.nodes[leader].name);

        // 2. Abdicate + clean cycle: the detection-free failover, then
        // the old leader's SIGUSR1 cycle.
        let t0 = Instant::now();
        let abdicated = self.abdicate_leader()?;
        self.wait_serving("abdication: the successor serves")?;
        println!("bench: abdication failover_ms={}", t0.elapsed().as_millis());
        self.cycle(abdicated, "-USR1")?;
        self.wait_running(abdicated, "the SIGUSR1 clean cycle re-boots")?;
        self.wait_serving("the cluster serves after the clean cycle")?;
        println!("bench: clean cycle on {}", self.nodes[abdicated].name);

        // 3. Clean swap: a non-leader SIGTERMs out and respawns.
        let index = self.non_leader_index()?;
        self.clean_stop(index)?;
        println!("bench: clean stop of {}", self.nodes[index].name);
        self.spawn(index)?;
        self.wait_running(index, "the clean swap resumes")?;
        self.wait_serving("the cluster serves after the clean swap")?;
        println!("bench: clean swap of {} resumed", self.nodes[index].name);

        // 4. Crash swap: a non-leader is SIGKILLed and respawns.
        let index = self.non_leader_index()?;
        self.crash(index)?;
        println!("bench: crash of {}", self.nodes[index].name);
        self.spawn(index)?;
        self.wait_running(index, "the crash swap reincarnates")?;
        self.wait_serving("the cluster serves after the crash swap")?;
        println!(
            "bench: crash swap of {} reincarnated",
            self.nodes[index].name
        );

        // 5. Abdicate + leader swap: the leader abdicates, is confirmed
        // serving, then SIGTERMs out and respawns.
        let t0 = Instant::now();
        let abdicated = self.abdicate_leader()?;
        self.wait_serving("the abdicated leader swap's successor serves")?;
        println!("bench: abdication failover_ms={}", t0.elapsed().as_millis());
        self.clean_stop(abdicated)?;
        self.spawn(abdicated)?;
        self.wait_running(abdicated, "the leader swap resumes")?;
        self.wait_serving("the cluster serves after the leader swap")?;
        println!(
            "bench: leader swap of {} resumed",
            self.nodes[abdicated].name
        );

        // 6. Leader crash: the phi detector drives the takeover.
        let leader = self
            .wait_for(
                "a leader before the leader crash",
                Duration::from_secs(30),
                |driver| driver.leader_index().is_some(),
            )
            .map(|_| self.leader_index().expect("the wait just landed"))?;
        let t0 = Instant::now();
        self.crash(leader)?;
        self.wait_serving("the detector-driven takeover serves")?;
        println!("bench: detector takeover_ms={}", t0.elapsed().as_millis());
        self.spawn(leader)?;
        self.wait_running(leader, "the crashed leader reincarnates")?;
        self.wait_serving("the cluster serves after the leader crash")?;
        println!(
            "bench: crashed leader {} reincarnated",
            self.nodes[leader].name
        );

        // 7. Dirty cycle: a node takes a SIGUSR2 mid-load.
        let index = self.non_leader_index()?;
        self.cycle(index, "-USR2")?;
        self.wait_running(index, "the SIGUSR2 dirty cycle re-boots")?;
        self.wait_serving("the cluster serves after the dirty cycle")?;
        println!("bench: dirty cycle on {}", self.nodes[index].name);
        Ok(())
    }

    fn teardown(&mut self) {
        self.poller_stop.store(true, Ordering::Relaxed);
        for index in 0..self.nodes.len() {
            if self.nodes[index].proc.is_none() {
                continue;
            }
            // Only a latched-and-serving identity takes a fresh clean-stop
            // signal: a node already mid-stop from a failed ladder step
            // keeps its single expectation (no double-signal noise), and
            // the process drop below backstops whatever survives.
            let serving = lock(&self.nodes[index].discipline).is_running();
            if serving {
                lock(&self.nodes[index].discipline).signal_clean_stop();
            }
            if self.signal(index, "-TERM").is_ok() {
                let _ = self.wait_exit(index, Duration::from_secs(10));
            }
            self.nodes[index].proc = None;
        }
    }

    /// The oracle: the run's verdict, printed per check.
    fn oracle(&self) -> Result<(), String> {
        let mut failures: Vec<String> = Vec::new();
        let mut chains = Vec::new();
        for node in &self.nodes {
            match oracle::node_chain(&node.journal) {
                Ok(chain) => chains.push((node.name.clone(), chain)),
                Err(error) => failures.push(error),
            }
        }
        if chains.len() == self.nodes.len() {
            let refs: Vec<(&str, Vec<oracle::Transition>)> = chains
                .iter()
                .map(|(name, chain)| (name.as_str(), chain.clone()))
                .collect();
            if let Err(error) = oracle::chains_agree(&refs) {
                failures.push(format!("chain equality: {error}"));
            } else {
                println!(
                    "bench oracle: the {} chains agree ({} transitions)",
                    refs.len(),
                    refs.first().map(|(_, chain)| chain.len()).unwrap_or(0)
                );
            }
            // The client-history reference is the longest chain: a node
            // that was down for part of the ladder legitimately holds
            // fewer transitions, and the fullest chain is the most
            // complete record of what clients could have observed. The
            // coupling is deliberate: this check's verdict is meaningful
            // only when `chains_agree` above passed — on a
            // content-diverged world the longest chain is still a
            // wrong-world record, and the client check may pass or fail
            // against it for the wrong reasons. `chains_agree` fails the
            // run independently; the two failures are reported together
            // and either one is conclusive.
            let reference = &chains
                .iter()
                .max_by_key(|(_, chain)| chain.len())
                .expect("chains is nonempty")
                .1;
            let pre = failures.len();
            for (index, history) in self.histories.iter().enumerate() {
                let history = lock(history);
                if let Err(error) = oracle::client_history_consistent(reference, &history) {
                    failures.push(format!(
                        "{} client history: {error}",
                        self.nodes[index].name
                    ));
                }
            }
            if failures.len() == pre {
                println!("bench oracle: the client histories are consistent");
            }
        }
        let mut logs = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.options.run_dir) {
            for entry in entries.flatten() {
                let name = entry.file_name();
                let Some(name) = name.to_str() else { continue };
                let is_log = self.nodes.iter().any(|node| {
                    is_node_log(name, &node.name) || name == format!("{}.err", node.name)
                });
                if is_log && let Ok(text) = std::fs::read_to_string(entry.path()) {
                    logs.push((name.to_string(), text));
                }
            }
        }
        let log_refs: Vec<(&str, String)> = logs
            .iter()
            .map(|(name, text)| (name.as_str(), text.clone()))
            .collect();
        if let Err(error) = oracle::logs_without_maybe_violations(&log_refs) {
            failures.push(error);
        } else {
            println!("bench oracle: no maybe-invariant voice");
        }
        for node in &self.nodes {
            let discipline = lock(&node.discipline);
            if let Some(violation) = discipline.violation() {
                failures.push(format!("{} store discipline: {violation}", node.name));
            } else if !discipline.at_rest() {
                failures.push(format!(
                    "{} store discipline: the automaton did not rest",
                    node.name
                ));
            }
        }
        if failures.is_empty() {
            println!("bench oracle: the store discipline held");
            Ok(())
        } else {
            Err(failures.join("\n"))
        }
    }
}

fn main() {
    let options = parse_options();
    let run_dir = options.run_dir.clone();
    if let Err(error) = std::fs::create_dir_all(&run_dir) {
        eprintln!(
            "skaffold_bench_driver: cannot create {}: {error}",
            run_dir.display()
        );
        std::process::exit(2);
    }
    let mut descriptor = String::new();
    for id in 1..=3u32 {
        descriptor.push_str(&format!(
            "{{\"id\":{id},\"name\":\"n{id}\",\"host\":\"127.0.0.1\",\"port\":{},\"genesis\":true}}\n",
            options.base_port + id as u16
        ));
    }
    if let Err(error) = std::fs::write(run_dir.join("cluster.jsonl"), descriptor) {
        eprintln!("skaffold_bench_driver: cannot write the descriptor: {error}");
        std::process::exit(2);
    }

    let failure = Arc::new(AtomicBool::new(false));
    let mut nodes = Vec::new();
    for id in 1..=3u32 {
        let name = format!("n{id}");
        let store_sock = run_dir.join(format!("{name}.store.sock"));
        let _ = std::fs::remove_file(&store_sock);
        let listener = UnixListener::bind(&store_sock).unwrap_or_else(|error| {
            eprintln!(
                "skaffold_bench_driver: cannot bind {}: {error}",
                store_sock.display()
            );
            std::process::exit(2);
        });
        let discipline = Arc::new(Mutex::new(NodeDiscipline::new()));
        {
            let discipline = Arc::clone(&discipline);
            let failure = Arc::clone(&failure);
            std::thread::spawn(move || serve_socket(listener, discipline, failure));
        }
        let journal = run_dir.join(format!("{name}-journal"));
        let flight = run_dir.join(format!("{name}-flight"));
        for dir in [&journal, &flight] {
            if let Err(error) = std::fs::create_dir_all(dir) {
                eprintln!(
                    "skaffold_bench_driver: cannot create {}: {error}",
                    dir.display()
                );
                std::process::exit(2);
            }
        }
        nodes.push(Node {
            name: name.clone(),
            id,
            tcp: options.base_port + 100 + id as u16,
            store_sock,
            state: run_dir.join(format!("{name}.state")),
            log: run_dir.join(format!("{name}.log")),
            journal,
            flight,
            discipline,
            proc: None,
        });
    }

    let poller_stop = Arc::new(AtomicBool::new(false));
    let mut histories = Vec::new();
    for (index, node) in nodes.iter().enumerate() {
        let history: ClientHistory = Arc::new(Mutex::new(Vec::new()));
        histories.push(Arc::clone(&history));
        let addr = format!("127.0.0.1:{}", node.tcp);
        let stop = Arc::clone(&poller_stop);
        std::thread::spawn(move || {
            let mut poller = Poller {
                stop,
                history,
                request_num: (index as u64) * 1_000_000,
            };
            while !poller.stop.load(Ordering::Relaxed) {
                poller.step(&addr);
                std::thread::sleep(Duration::from_millis(25));
            }
        });
    }

    let mut driver = Driver {
        options,
        nodes,
        failure,
        histories,
        poller_stop,
    };

    let started = Instant::now();
    let budget = Duration::from_secs(driver.options.duration);
    let mut outcome: Result<(), String> = Ok(());
    for index in 0..driver.nodes.len() {
        if let Err(error) = driver.spawn(index) {
            outcome = Err(error);
            break;
        }
    }
    if outcome.is_ok() {
        outcome = driver.ladder();
    }
    if started.elapsed() > budget && outcome.is_ok() {
        outcome = Err(format!(
            "the ladder overran the {} s run budget",
            driver.options.duration
        ));
    }
    driver.teardown();
    match driver.oracle() {
        Ok(()) => {}
        Err(error) => {
            outcome = match outcome {
                Ok(()) => Err(error),
                Err(prior) => Err(format!("{prior}\n{error}")),
            };
        }
    }
    match outcome {
        Ok(()) => {
            println!(
                "bench: PASS run={} elapsed_ms={}",
                run_dir.display(),
                started.elapsed().as_millis()
            );
        }
        Err(error) => {
            eprintln!(
                "bench: FAIL run={} elapsed_ms={}\n{error}",
                run_dir.display(),
                started.elapsed().as_millis()
            );
            std::process::exit(1);
        }
    }
}
