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

/// One JSON line on the simulator's own log: the observability
/// contract's shape, `ts` (the wall clock), `event` (the line's stable
/// name) and the line's own fields. Echoed to stdout in the same shape,
/// so a lane that tees the stream captures JSON lines too.
struct Logger {
    file: File,
}

impl Logger {
    fn event(&mut self, event: &str, fields: &[(&str, String)]) {
        let mut line = format!(
            "{{\"ts\":{},\"level\":\"INFO\",\"event\":{}{}",
            now_ms(),
            json_string(event),
            fields
                .iter()
                .map(|(key, value)| format!(",{}:{value}", json_string(key)))
                .collect::<String>()
        );
        line.push('}');
        println!("{line}");
        let _ = writeln!(self.file, "{line}");
        let _ = self.file.flush();
    }

    fn detail(&mut self, event: &str, detail: &str) {
        self.event(event, &[("detail", json_string(detail))]);
    }
}

/// One string as a JSON literal, quotes and control characters escaped.
fn json_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for c in text.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

/// One number as a JSON literal.
fn json_number(value: u64) -> String {
    value.to_string()
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
    port: u16,
}

impl Worker {
    fn new(dc: u64, serial: u64, port: u16) -> Self {
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
            port,
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
            format!(
                "00000000-{:04x}-4000-8000-{tail:012x}",
                self.client_id >> 16
            ),
        )
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

/// Read one NDJSON reply line from a socket the simulator owns.
fn read_reply(stream: &mut TcpStream) -> io::Result<String> {
    let mut reply = String::new();
    let mut byte = [0_u8; 1];
    loop {
        let read = stream.read(&mut byte)?;
        if read == 0 || byte[0] == b'\n' {
            break;
        }
        reply.push(byte[0] as char);
    }
    Ok(reply)
}

fn exchange_once(worker: &mut Worker, port: u16, json: &str) -> io::Result<String> {
    if worker.connection.is_none() {
        let address = format!("127.0.0.1:{port}");
        let stream = TcpStream::connect_timeout(&address.parse().unwrap(), Duration::from_secs(2))?;
        stream.set_read_timeout(Some(Duration::from_secs(4)))?;
        stream.set_write_timeout(Some(Duration::from_secs(2)))?;
        worker.connection = Some(stream);
    }
    let stream = worker
        .connection
        .as_mut()
        .expect("connection was initialized");
    stream.write_all(json.as_bytes())?;
    stream.write_all(b"\n")?;
    stream.flush()?;
    let reply = read_reply(stream)?;
    if reply.is_empty() {
        worker.connection = None;
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "server closed before reply",
        ));
    }
    Ok(reply)
}

/// The client circuit over a real socket: one connection, the nexus holding
/// each command's reply against the socket it arrived on, and the
/// one-outstanding rule enforced by name. Three commands are written down the
/// SAME socket before any reply is read:
///
/// 1. the first is the connection's one outstanding command — it is executed
///    and its reply is serialised onto the socket the command arrived on, the
///    nexus dropping the entry as it writes;
/// 2. the second and third both arrived while the first was outstanding, so
///    both are refused by name and neither is queued behind it or executed.
///
/// Then, with the replies read, a fourth command is written and answered: the
/// refusal cost the connection nothing, and the next read is served normally.
fn one_outstanding_stage(port: u16) -> io::Result<()> {
    let address = format!("127.0.0.1:{port}");
    let mut stream =
        TcpStream::connect_timeout(&address.parse().unwrap(), Duration::from_secs(2))?;
    stream.set_read_timeout(Some(Duration::from_secs(10)))?;
    stream.set_write_timeout(Some(Duration::from_secs(2)))?;
    fn send(stream: &mut TcpStream, json: &str) -> io::Result<()> {
        stream.write_all(json.as_bytes())?;
        stream.write_all(b"\n")?;
        stream.flush()
    }
    // One client identity, increasing request numbers, distinct message ids —
    // the shape a pipelining client produces.
    let first = "04040404-0404-0404-0404-040404040401";
    let second = "04040404-0404-0404-0404-040404040402";
    let third = "04040404-0404-0404-0404-040404040403";
    let fourth = "04040404-0404-0404-0404-040404040404";
    let command = |message_id: &str, request_num: u64| {
        format!(
            r#"{{"op":"get","message_id":"{message_id}","client_id":404,"request_num":{request_num},"lock_id":{SENTINEL_LOCK}}}"#
        )
    };
    send(&mut stream, &command(first, 1))?;
    send(&mut stream, &command(second, 2))?;
    send(&mut stream, &command(third, 3))?;
    let one = read_reply(&mut stream)?;
    let two = read_reply(&mut stream)?;
    let three = read_reply(&mut stream)?;
    if one.contains("request_outstanding") {
        return Err(io::Error::other(format!(
            "the first command on a free connection was refused: {one}"
        )));
    }
    if !one.contains(&format!(r#""message_id":"{first}""#)) {
        return Err(io::Error::other(format!(
            "the first command's reply does not carry its own message_id: {one}"
        )));
    }
    let refusal = |message_id: &str| {
        format!(r#"{{"error":"request_outstanding","message_id":"{message_id}"}}"#)
    };
    if two != refusal(second) {
        return Err(io::Error::other(format!(
            "the second command on an outstanding connection was not refused by name: {two}"
        )));
    }
    if three != refusal(third) {
        return Err(io::Error::other(format!(
            "the third command was not refused by name: {three}"
        )));
    }
    send(&mut stream, &command(fourth, 4))?;
    let after = read_reply(&mut stream)?;
    if after.contains("request_outstanding") || !after.contains(&format!(r#""message_id":"{fourth}""#))
    {
        return Err(io::Error::other(format!(
            "the connection did not serve the next command after the refusal: {after}"
        )));
    }
    Ok(())
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
    // The client rents the lock for a duration; the leader stamps the
    // absolute expiry off its own execution clock.
    format!(
        r#"{{"op":"set","message_id":"{message_id}","client_id":{},"request_num":{number},"lock_id":{SENTINEL_LOCK},"lease":{{"lease_id":{},"holder":"{}","lease_ms":{LEASE_MS}}}}}"#,
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
    // The deployment descriptor: sparse, admin-assigned, never-recycled
    // NodeIds; line order is the genesis succession sequence (n1 is the
    // genesis primary). Each id is the packed pair (system half, crash
    // counter 1) — the sysadmin-assigned system identifier in the high
    // sixteen bits, the genesis life's crash counter in the low sixteen
    // (docs/src/architecture.md, the deployment descriptor).
    fs::write(
        work.join("cluster.jsonl"),
        concat!(
            "{\"id\":6619137,\"name\":\"n1\",\"host\":\"127.0.0.1\",\"port\":29111,\"genesis\":true}\n",
            "{\"id\":13238273,\"name\":\"n2\",\"host\":\"127.0.0.1\",\"port\":29112,\"genesis\":true}\n",
            "{\"id\":19857409,\"name\":\"n3\",\"host\":\"127.0.0.1\",\"port\":29113,\"genesis\":true}\n",
        ),
    )?;
    let mut children = Vec::new();
    for (name, client_port, peer_port) in [
        ("n1", 29101, 29111),
        ("n2", 29102, 29112),
        ("n3", 29103, 29113),
    ] {
        let stdout = File::create(work.join(format!("{name}.out")))?;
        let stderr = File::create(work.join(format!("{name}.err")))?;
        let child = Command::new(runtime)
            .current_dir(root)
            .arg("build/server.lua")
            .args([
                "--node",
                name,
                "--client",
                &format!("127.0.0.1:{client_port}"),
            ])
            .args([
                "--state",
                &work.join(format!("{name}.nonce")).display().to_string(),
            ])
            .arg("--cluster")
            .arg(work.join("cluster.jsonl"))
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()?;
        let _ = peer_port; // The explicit tuple documents the client/peer topology.
        children.push(child);
    }
    Ok(Cluster { children })
}

fn usage() -> ! {
    eprintln!("usage: lease_failover_sim [--duration SECONDS] [--external-ports P1,P2,P3]");
    std::process::exit(2)
}

fn main() -> io::Result<()> {
    let mut duration = 30_u64;
    let mut external_ports: Option<[u16; 3]> = None;
    let args: Vec<String> = env::args().skip(1).collect();
    let mut index = 0;
    while index < args.len() {
        match args[index].as_str() {
            "--duration" => {
                index += 1;
                duration = args
                    .get(index)
                    .and_then(|value| value.parse().ok())
                    .unwrap_or_else(|| usage());
                if duration == 0 || duration > 30 {
                    usage();
                }
            }
            "--external-ports" => {
                index += 1;
                let values: Vec<u16> = args
                    .get(index)
                    .map(String::as_str)
                    .unwrap_or("")
                    .split(',')
                    .map(str::parse)
                    .collect::<Result<_, _>>()
                    .unwrap_or_else(|_| usage());
                external_ports = values.try_into().ok();
                if external_ports.is_none() || external_ports == Some([0, 0, 0]) {
                    usage();
                }
            }
            _ => usage(),
        }
        index += 1;
    }
    let root = PathBuf::from(env::var("SIM_ROOT").expect("SIM_ROOT is required"));
    let runtime = external_ports
        .is_none()
        .then(|| PathBuf::from(env::var("LUNET_RUN").expect("LUNET_RUN is required")));
    if let Some(runtime) = &runtime {
        if !runtime.is_file() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                "project-local lunet-run missing",
            ));
        }
    }
    let work = root
        .join(".tmp")
        .join(format!("lease-failover-{}", now_ms()));
    fs::create_dir_all(&work)?;
    let mut log = Logger {
        file: File::create(work.join("simulation.log"))?,
    };
    log.event(
        "simulation-start",
        &[
            ("duration_s", json_number(duration)),
            ("work", json_string(&work.display().to_string())),
        ],
    );
    let mut cluster = if let Some(runtime) = &runtime {
        Some(start_cluster(&root, runtime, &work)?)
    } else {
        None
    };
    let ports = external_ports.unwrap_or([29_101, 29_102, 29_103]);
    if cluster.is_some() {
        log.event(
            "cluster-start",
            &[
                ("cluster", json_string("n1,n2,n3")),
                (
                    "client_endpoints",
                    json_string("29101,29102,29103"),
                ),
            ],
        );
        thread::sleep(Duration::from_millis(2_600));
    } else {
        log.event(
            "cluster-start",
            &[
                ("cluster", json_string("external-stable")),
                (
                    "client_endpoints",
                    json_string(&format!("{},{},{}", ports[0], ports[1], ports[2])),
                ),
            ],
        );
    }

    let mut workers = vec![
        Worker::new(1, 1, ports[0]),
        Worker::new(2, 1, ports[1]),
        Worker::new(3, 1, ports[2]),
    ];
    // The client circuit over a real socket, before the chaos: the
    // command-to-socket nexus and the one-outstanding refusal, on a
    // connection that has nothing else on it.
    if cluster.is_some() {
        match one_outstanding_stage(ports[0]) {
            Ok(()) => log.event(
                "one-outstanding-stage",
                &[("verdict", json_string("passed"))],
            ),
            Err(error) => {
                log.detail("one-outstanding-stage", &error.to_string());
                if let Some(cluster) = &mut cluster {
                    cluster.stop();
                }
                return Err(error);
            }
        }
    }
    for worker in &workers {
        log.event(
            "client-start",
            &[
                ("node", json_string(&worker.name())),
                ("logical_id", json_number(worker.client_id)),
            ],
        );
    }
    let started = Instant::now();
    let end = started + Duration::from_secs(duration);
    let mut next_kill = started + KILL_EVERY;
    let mut replacement: Option<PendingReplacement> = None;
    let mut observed_holder: Option<usize> = None;
    let mut observed_live_holder = false;
    let mut failure: Option<String> = None;

    while Instant::now() < end && failure.is_none() {
        let holder_names: Vec<String> =
            workers.iter().map(|worker| worker.holder.clone()).collect();
        for index in 0..workers.len() {
            if !workers[index].active {
                continue;
            }
            let get = request_get(&mut workers[index]);
            let port = workers[index].port;
            match exchange(&mut workers[index], port, &get) {
                Ok(reply) => {
                    let seen = holder_in(&reply, &holder_names);
                    if reply.matches(r#""holder":"#).count() > 1 {
                        failure = Some(format!(
                            "conflicting live holders in one observation: {reply}"
                        ));
                        break;
                    }
                    if let Some(holder) = seen {
                        observed_holder = Some(holder);
                        observed_live_holder = true;
                    } else if reply.contains("\"lease\":null") {
                        observed_holder = None;
                    }
                    let should_set =
                        seen != Some(index) || workers[index].last_renewal.elapsed() >= RENEW_EVERY;
                    if should_set {
                        let was_owner = workers[index].owns;
                        let set = request_set(&mut workers[index]);
                        match exchange(&mut workers[index], port, &set) {
                            Ok(set_reply) if set_reply.contains(r#""granted":true"#) => {
                                workers[index].owns = true;
                                workers[index].last_renewal = Instant::now();
                                observed_holder = Some(index);
                                observed_live_holder = true;
                                log.event(
                                    if was_owner { "client-renew" } else { "client-acquire" },
                                    &[
                                        ("node", json_string(&workers[index].name())),
                                        ("lease_id", json_number(workers[index].lease_id)),
                                    ],
                                );
                            }
                            Ok(_) => workers[index].owns = false,
                            Err(error) => log.detail(
                                "client-unavailable",
                                &format!("{}: {error}", workers[index].name()),
                            ),
                        }
                    }
                }
                Err(error) => log.detail(
                    "client-unavailable",
                    &format!("{}: {error}", workers[index].name()),
                ),
            }
        }

        if let Some(pending) = &replacement {
            let due = pending.due;
            let dc = pending.dc;
            let serial = pending.serial;
            let deadline = pending.deadline;
            let killed_client_id = pending.killed_client_id;
            if Instant::now() >= due {
                let worker = Worker::new(dc, serial, ports[(dc - 1) as usize]);
                log.event(
                    "client-restart",
                    &[
                        ("node", json_string(&worker.name())),
                        ("logical_id", json_number(worker.client_id)),
                    ],
                );
                workers.push(worker);
                replacement.as_mut().unwrap().due = end + Duration::from_secs(1);
            }
            if Instant::now() > deadline {
                failure = Some(format!(
                    "no replacement holder appeared within {:?}",
                    TAKEOVER_DEADLINE
                ));
            }
            if let Some(holder) = observed_holder {
                if workers[holder].client_id != killed_client_id && workers[holder].active {
                    log.event(
                        "lease-takeover",
                        &[("node", json_string(&workers[holder].name()))],
                    );
                    replacement = None;
                }
            }
        }

        if replacement.is_none() && Instant::now() >= next_kill {
            if let Some(holder) = observed_holder {
                if workers[holder].active {
                    let killed = &mut workers[holder];
                    killed.stop();
                    log.event(
                        "client-kill",
                        &[
                            ("node", json_string(&killed.name())),
                            ("logical_id", json_number(killed.client_id)),
                        ],
                    );
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

    if !observed_live_holder && failure.is_none() {
        failure = Some("no live holder was observed before simulation deadline".to_owned());
    }
    if let Some(cluster) = &mut cluster {
        cluster.stop();
    }
    if let Some(message) = failure {
        log.detail("simulation-failed", &message);
        return Err(io::Error::other(message));
    }
    log.detail(
        "simulation-passed",
        "no conflicting live holder observed; nodes cleaned up",
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_identity_is_durable_and_names_are_human_readable() {
        let worker = Worker::new(2, 17, 29_102);
        assert_eq!(worker.client_id, 20_017);
        assert_eq!(worker.name(), "DC2-0017");
    }

    #[test]
    fn holder_parser_recognizes_one_known_holder() {
        let holders = vec!["first".to_owned(), "second".to_owned()];
        assert_eq!(
            holder_in(r#"{"lease":{"holder":"second"}}"#, &holders),
            Some(1)
        );
    }
}
