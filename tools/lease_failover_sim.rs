//! Local, dependency-free lease failover demonstration.
//!
//! This is intentionally a small `std`-only control loop. It talks to the
//! service over TCP NDJSON exactly as an external client would; it does not
//! link the lock implementation or simulate replication.

use std::env;
use std::fs::{self, File};
use std::io::{self, Read, Write};
use std::net::{Shutdown, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SENTINEL_LOCK: u64 = 0x0DDBA11;
const LEASE_MS: u64 = 1_000;
const RENEW_EVERY: Duration = Duration::from_millis(900);
const REPLACEMENT_DELAY: Duration = Duration::from_millis(1_100);
const KILL_EVERY: Duration = Duration::from_secs(3);
const TAKEOVER_DEADLINE: Duration = Duration::from_secs(5);

struct Logger {
    file: File,
}

impl Logger {
    fn line(&mut self, message: impl AsRef<str>) {
        let line = message.as_ref();
        println!("{line}");
        let _ = writeln!(self.file, "{line}");
        let _ = self.file.flush();
    }
}

struct Cluster {
    children: Vec<Child>,
}

impl Cluster {
    fn stop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
        }
        for child in &mut self.children {
            let _ = child.wait();
        }
        self.children.clear();
    }
}

impl Drop for Cluster {
    fn drop(&mut self) {
        self.stop();
    }
}

struct Worker {
    dc: u64,
    serial: u64,
    client_id: u64,
    holder: String,
    request_num: u64,
    lease_id: u64,
    active: bool,
    owns: bool,
    last_renewal: Instant,
    connection: Option<TcpStream>,
}

impl Worker {
    fn new(dc: u64, serial: u64) -> Self {
        let client_id = dc * 10_000 + serial;
        Self {
            dc,
            serial,
            client_id,
            holder: format!("00000000-0000-4000-8000-{client_id:012x}"),
            request_num: 0,
            lease_id: 1,
            active: true,
            owns: false,
            last_renewal: Instant::now() - RENEW_EVERY,
            connection: None,
        }
    }

    fn name(&self) -> String {
        format!("DC{}-{:04}", self.dc, self.serial)
    }

    fn next_envelope(&mut self) -> (u64, String) {
        self.request_num += 1;
        let request_num = self.request_num;
        let tail = (self.client_id << 32) | request_num;
        (
            request_num,
            format!("00000000-{:04x}-4000-8000-{tail:012x}", self.client_id >> 16),
        )
    }

    fn port(&self) -> u16 {
        29_100 + self.dc as u16
    }

    fn stop(&mut self) {
        if let Some(stream) = self.connection.take() {
            let _ = stream.shutdown(Shutdown::Both);
        }
        self.active = false;
        self.owns = false;
    }
}

struct PendingReplacement {
    dc: u64,
    serial: u64,
    due: Instant,
    killed_client_id: u64,
    deadline: Instant,
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("wall clock precedes UNIX epoch")
        .as_millis() as u64
}

fn exchange(worker: &mut Worker, port: u16, json: &str) -> io::Result<String> {
    let mut last_error = None;
    // A retry deliberately writes the identical JSON envelope. The service's
    // `(client_id, request_num)` duplicate suppression makes this safe.
    for _ in 0..3 {
        match exchange_once(worker, port, json) {
            Ok(reply) => return Ok(reply),
            Err(error) => {
                worker.connection = None;
                last_error = Some(error);
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
    Err(last_error.expect("retry loop records an error"))
}

fn exchange_once(worker: &mut Worker, port: u16, json: &str) -> io::Result<String> {
    if worker.connection.is_none() {
        let address = format!("127.0.0.1:{port}");
        let stream = TcpStream::connect_timeout(&address.parse().unwrap(), Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(4)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        worker.connection = Some(stream);
    }
    let stream = worker.connection.as_mut().expect("connection was initialized");
    stream.write_all(json.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let mut reply = String::new();
    let mut byte = [0_u8; 1];
    loop {
        let read = stream.read(&mut byte)?;
        if read == 0 || byte[0] == b'\n' {
            break;
        }
        reply.push(byte[0] as char);
    }
    if reply.is_empty() {
        worker.connection = None;
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "server closed before reply"));
    }
    Ok(reply)
}

fn request_get(worker: &mut Worker) -> String {
    let (number, message_id) = worker.next_envelope();
    format!(
        r#"{{"op":"get","message_id":"{message_id}","client_id":{},"request_num":{number},"lock_id":{SENTINEL_LOCK}}}"#,
        worker.client_id
    )
}

fn request_set(worker: &mut Worker) -> String {
    let (number, message_id) = worker.next_envelope();
    let expiry = now_ms() + LEASE_MS;
    format!(
        r#"{{"op":"set","message_id":"{message_id}","client_id":{},"request_num":{number},"lock_id":{SENTINEL_LOCK},"lease":{{"lease_id":{},"holder":"{}","expiry":{expiry}}}}}"#,
        worker.client_id, worker.lease_id, worker.holder
    )
}

fn holder_in(reply: &str, known_holders: &[String]) -> Option<usize> {
    let mut found = None;
    for (index, holder) in known_holders.iter().enumerate() {
        if reply.contains(&format!(r#""holder":"{holder}""#)) {
            if found.replace(index).is_some() {
                return None;
            }
        }
    }
    found
}

fn start_cluster(root: &Path, runtime: &Path, work: &Path) -> io::Result<Cluster> {
    let mut children = Vec::new();
    for (name, client_port, peer_port) in [("n1", 29101, 29111), ("n2", 29102, 29112), ("n3", 29103, 29113)] {
        let stdout = File::create(work.join(format!("{name}.out")))?;
        let stderr = File::create(work.join(format!("{name}.err")))?;
        let child = Command::new(runtime)
            .current_dir(root)
            .arg("build/server.lua")
            .args(["--node", name, "--client", &format!("127.0.0.1:{client_port}")])
            .args(["--state", &work.join(format!("{name}.nonce")).display().to_string()])
            .args(["--member", "n1=127.0.0.1:29111"])
            .args(["--member", "n2=127.0.0.1:29112"])
            .args(["--member", "n3=127.0.0.1:29113"])
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        let _ = peer_port; // The explicit tuple documents the client/peer topology.
        children.push(child);
    }
    Ok(Cluster { children })
}

fn usage() -> ! {
    eprintln!("usage: lease_failover_sim [--duration SECONDS]");
    std::process::exit(2)
}

fn main() -> io::Result<()> {
    let mut duration = 30_u64;
    let args: Vec<String> = env::args().skip(1).collect();
    if !args.is_empty() {
        if args.len() != 2 || args[0] != "--duration" {
            usage();
        }
        duration = args[1].parse().unwrap_or_else(|_| usage());
        if duration == 0 || duration > 30 {
            usage();
        }
    }
    let root = PathBuf::from(env::var("SIM_ROOT").expect("SIM_ROOT is required"));
    let runtime = PathBuf::from(env::var("LUNET_RUN").expect("LUNET_RUN is required"));
    if !runtime.is_file() {
        return Err(io::Error::new(io::ErrorKind::NotFound, "project-local lunet-run missing"));
    }
    let work = root.join(".tmp").join(format!("lease-failover-{}", now_ms()));
    fs::create_dir_all(&work)?;
    let mut log = Logger { file: File::create(work.join("simulation.log"))? };
    log.line(format!("simulation start duration={duration}s work={}", work.display()));
    let mut cluster = start_cluster(&root, &runtime, &work)?;
    log.line("cluster start n1,n2,n3; client endpoints 29101,29102,29103");
    thread::sleep(Duration::from_millis(2_600));

    let mut workers = vec![Worker::new(1, 1), Worker::new(2, 1), Worker::new(3, 1)];
    for worker in &workers {
        log.line(format!("client start {} logical_id={}", worker.name(), worker.client_id));
    }
    let started = Instant::now();
    let end = started + Duration::from_secs(duration);
    let mut next_kill = started + KILL_EVERY;
    let mut replacement: Option<PendingReplacement> = None;
    let mut observed_holder: Option<usize> = None;
    let mut failure: Option<String> = None;

    while Instant::now() < end && failure.is_none() {
        let holder_names: Vec<String> = workers.iter().map(|worker| worker.holder.clone()).collect();
        for index in 0..workers.len() {
            if !workers[index].active {
                continue;
            }
            let get = request_get(&mut workers[index]);
            let port = workers[index].port();
            match exchange(&mut workers[index], port, &get) {
                Ok(reply) => {
                    let seen = holder_in(&reply, &holder_names);
                    if reply.matches(r#""holder":"#).count() > 1 {
                        failure = Some(format!("conflicting live holders in one observation: {reply}"));
                        break;
                    }
                    if let Some(holder) = seen {
                        observed_holder = Some(holder);
                    } else if reply.contains("\"lease\":null") {
                        observed_holder = None;
                    }
                    let should_set = seen != Some(index) || workers[index].last_renewal.elapsed() >= RENEW_EVERY;
                    if should_set {
                        let was_owner = workers[index].owns;
                        let set = request_set(&mut workers[index]);
                        match exchange(&mut workers[index], port, &set) {
                            Ok(set_reply) if set_reply.contains(r#""granted":true"#) => {
                                workers[index].owns = true;
                                workers[index].last_renewal = Instant::now();
                                observed_holder = Some(index);
                                if was_owner {
                                    log.line(format!("renew {} lease_id={}", workers[index].name(), workers[index].lease_id));
                                } else {
                                    log.line(format!("acquire {} lease_id={}", workers[index].name(), workers[index].lease_id));
                                }
                            }
                            Ok(_) => workers[index].owns = false,
                            Err(error) => log.line(format!("no-leader/unavailable {}: {error}", workers[index].name())),
                        }
                    }
                }
                Err(error) => log.line(format!("no-leader/unavailable {}: {error}", workers[index].name())),
            }
        }

        if let Some(pending) = &replacement {
            let due = pending.due;
            let dc = pending.dc;
            let serial = pending.serial;
            let deadline = pending.deadline;
            let killed_client_id = pending.killed_client_id;
            if Instant::now() >= due {
                let worker = Worker::new(dc, serial);
                log.line(format!("client start {} logical_id={} after lease expiry", worker.name(), worker.client_id));
                workers.push(worker);
                replacement.as_mut().unwrap().due = end + Duration::from_secs(1);
            }
            if Instant::now() > deadline {
                failure = Some(format!("no replacement holder appeared within {:?}", TAKEOVER_DEADLINE));
            }
            if let Some(holder) = observed_holder {
                if workers[holder].client_id != killed_client_id && workers[holder].active {
                    log.line(format!("lease expiry/takeover {}", workers[holder].name()));
                    replacement = None;
                }
            }
        }

        if replacement.is_none() && Instant::now() >= next_kill {
            if let Some(holder) = observed_holder {
                if workers[holder].active {
                    let killed = &mut workers[holder];
                    killed.stop();
                    log.line(format!("client kill {} logical_id={}", killed.name(), killed.client_id));
                    replacement = Some(PendingReplacement {
                        dc: killed.dc,
                        serial: killed.serial + 1,
                        due: Instant::now() + REPLACEMENT_DELAY,
                        killed_client_id: killed.client_id,
                        deadline: Instant::now() + TAKEOVER_DEADLINE,
                    });
                }
            }
            next_kill += KILL_EVERY;
        }
        thread::sleep(Duration::from_millis(80));
    }

    cluster.stop();
    if let Some(message) = failure {
        log.line(format!("simulation FAILED: {message}"));
        return Err(io::Error::other(message));
    }
    log.line("simulation passed: no conflicting live holder observed; nodes cleaned up");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_identity_is_durable_and_names_are_human_readable() {
        let worker = Worker::new(2, 17);
        assert_eq!(worker.client_id, 20_017);
        assert_eq!(worker.name(), "DC2-0017");
    }

    #[test]
    fn holder_parser_recognizes_one_known_holder() {
        let holders = vec!["first".to_owned(), "second".to_owned()];
        assert_eq!(holder_in(r#"{"lease":{"holder":"second"}}"#, &holders), Some(1));
    }
}
