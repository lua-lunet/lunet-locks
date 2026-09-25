//! The live-process signal gate: every catchable termination signal
//! (SIGTERM, SIGINT, SIGQUIT) drives the same clean shutdown — the serve
//! loop's drain point closes the wire, the `Stopped`→`flushed` marker
//! rounds run across the four superblock copies, and the next boot
//! continues under the SAME incarnation — while SIGKILL is the negative
//! (the running sentinel stands, the next boot bumps into the high band)
//! and SIGHUP is a logged no-op that never stops the process. Each test
//! spawns real `lease-sequencer` serve processes on loopback over a
//! three-genesis-member descriptor and speaks the real client wire.
//!
//! Scratch discipline: every run directory lives under the repo's
//! `.tmp/` (resolved from `CARGO_MANIFEST_DIR`), never the OS temp dir.

use lunet_locks_aof::marker::{self, MarkerState};
use std::fs;
use std::io::{Read as _, Write as _};
use std::net::TcpStream;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// The stop-path log records each clean stop must leave behind (the
/// stop-begin record names the caught signal; the drain and the
/// drain-proven records come from the adapter's stop contract).
const DRAIN_RECORD: &str = "stop: the wire is closed, the in-memory state is final";
const FLUSHED_RECORD: &str = "stop: drained and the drain proven";

/// The SIGHUP no-op record the serve loop writes (config reload is not
/// wired; the wiring is a scheduled item).
const HUP_NOOP_RECORD: &str = "sighup: config reload not wired; noop";

/// Serialize the process-heavy signal cases: each spawns a full
/// three-node cluster; concurrent clusters would contend for the
/// scheduler and stretch every readiness poll.
static GATE_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn gate_lock() -> std::sync::MutexGuard<'static, ()> {
    GATE_LOCK
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// One unique scratch directory under the repo's `.tmp/`, resolved from
/// the manifest path (the write boundary), never `std::env::temp_dir`.
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let ns = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let base = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/signal-gate");
    fs::create_dir_all(&base).expect("scratch base");
    let dir = base.join(format!(
        "{name}-p{}-n{}-{}",
        std::process::id(),
        SEQ.fetch_add(1, Ordering::Relaxed),
        ns % 1_000_000_000
    ));
    fs::create_dir_all(&dir).expect("scratch dir");
    dir
}

/// A port the OS is not using right now (probe-and-release; the child
/// binds it moments later).
fn free_udp_port() -> u16 {
    std::net::UdpSocket::bind(("127.0.0.1", 0))
        .expect("udp probe")
        .local_addr()
        .expect("udp probe addr")
        .port()
}

fn free_tcp_port() -> u16 {
    std::net::TcpListener::bind(("127.0.0.1", 0))
        .expect("tcp probe")
        .local_addr()
        .expect("tcp probe addr")
        .port()
}

/// One spawned serve process with the reaping guarantee: on every drop
/// path — assert success, assert failure, or timeout — the process is
/// killed if still alive and reaped, never left behind.
struct ServeProcess {
    child: Child,
    dir: PathBuf,
    name: String,
    client_port: u16,
}

impl Drop for ServeProcess {
    fn drop(&mut self) {
        // The deliberate-stop paths reaped the child already; a still-
        // running child here is an assert-failure path: kill and reap.
        if self.child.try_wait().expect("child status").is_none() {
            let _ = Command::new("kill").arg("-9").arg(self.pid_text()).output();
            let _ = self.child.wait();
        }
        let _ = &self.dir;
    }
}

impl ServeProcess {
    fn pid_text(&self) -> String {
        self.child.id().to_string()
    }

    /// True while the process has not exited.
    fn alive(&mut self) -> bool {
        self.child.try_wait().expect("child status").is_none()
    }

    /// Wait until the process exits or the deadline passes (then kill it
    /// — a hung stop is a failure, not a hang).
    fn wait_exit(&mut self, what: &str, deadline: Duration) -> std::process::ExitStatus {
        let started = Instant::now();
        loop {
            if let Some(status) = self.child.try_wait().expect("child status") {
                return status;
            }
            assert!(
                started.elapsed() < deadline,
                "{what}: timed out waiting for exit (pid {})",
                self.pid_text()
            );
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

/// The descriptor + spawn for one three-genesis-member cluster on
/// loopback; returns the three live processes. The cluster config is
/// minimal genesis: three voters, nothing else.
fn spawn_cluster(dir: &Path) -> Vec<ServeProcess> {
    let bin = env!("CARGO_BIN_EXE_lease-sequencer");
    let mut descriptor = String::new();
    let mut ports = Vec::new();
    for id in 1..=3 {
        let peer = free_udp_port();
        let client = free_tcp_port();
        ports.push((peer, client));
        descriptor.push_str(&format!(
            "{{\"id\":{id},\"name\":\"n{id}\",\"host\":\"127.0.0.1\",\"port\":{peer},\
             \"genesis\":true}}\n"
        ));
    }
    let config_path = dir.join("cluster.jsonl");
    fs::write(&config_path, descriptor).expect("descriptor");
    let config_path = config_path.to_str().expect("descriptor path").to_string();
    let mut nodes = Vec::new();
    for (index, (peer, client)) in ports.iter().enumerate() {
        let _ = peer;
        let name = format!("n{}", index + 1);
        let stdout = fs::File::create(dir.join(format!("{name}.stdout")))
            .expect("stdout capture");
        let stderr = fs::File::create(dir.join(format!("{name}.stderr")))
            .expect("stderr capture");
        let child = Command::new(bin)
            .args([
                "--name",
                &name,
                "--config",
                &config_path,
                "--client",
                &format!("127.0.0.1:{client}"),
                "--state",
                dir.join(format!("{name}.state")).to_str().expect("state path"),
                "--log",
                dir.join(format!("{name}.log")).to_str().expect("log path"),
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("lease-sequencer spawns");
        nodes.push(ServeProcess {
            child,
            dir: dir.to_path_buf(),
            name,
            client_port: *client,
        });
    }
    nodes
}

/// One lock-verb op over the real client wire: connect, send a `get`,
/// read one NDJSON reply line. The reply is the "serving" proof — the
/// cluster committed the op.
fn try_get_op(client_port: u16) -> Option<String> {
    let mut stream = TcpStream::connect(("127.0.0.1", client_port)).ok()?;
    stream
        .set_read_timeout(Some(Duration::from_secs(3)))
        .expect("read timeout");
    let message_id = uuid::Uuid::new_v4().to_string();
    let request = format!(
        "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":900001,\
         \"request_num\":1,\"lock_id\":7}}\n"
    );
    stream.write_all(request.as_bytes()).ok()?;
    stream.flush().ok()?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        if buf.contains(&b'\n') {
            break;
        }
        let read = stream.read(&mut chunk).ok()?;
        if read == 0 {
            break;
        }
        buf.extend_from_slice(&chunk[..read]);
    }
    if buf.is_empty() {
        return None;
    }
    Some(String::from_utf8_lossy(&buf).into_owned())
}

/// Drive a node to serving: a committed `get` reply inside the deadline
/// (the poll loop retries with fresh connections until the cluster's
/// election settles and the op commits).
fn wait_serving(node: &ServeProcess, what: &str) -> String {
    let deadline = Instant::now() + Duration::from_secs(60);
    loop {
        if let Some(reply) = try_get_op(node.client_port) {
            return reply;
        }
        assert!(
            Instant::now() < deadline,
            "{what}: timed out waiting for a committed get reply"
        );
        std::thread::sleep(Duration::from_millis(300));
    }
}

/// Send a signal to the process by `kill(1)` (the test process must not
/// raise signals into itself).
fn send_signal(node: &ServeProcess, signal_name: &str) {
    let output = Command::new("kill")
        .args(["-s", signal_name, &node.pid_text()])
        .output()
        .expect("kill runs");
    assert!(
        output.status.success(),
        "kill -s {signal_name} failed: {:?}",
        output
    );
}

/// The node's tracing log file (the rolling appender's
/// `<prefix>.<date>.log` under the run dir).
fn log_file(node: &ServeProcess) -> PathBuf {
    let entries = fs::read_dir(&node.dir).expect("run dir");
    let prefix = format!("{}.", node.name);
    for entry in entries {
        let entry = entry.expect("run dir entry");
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with(&prefix) && name.ends_with(".log") {
            return entry.path();
        }
    }
    panic!("no log file for {} under {}", node.name, node.dir.display());
}

/// Wait until the log contains the needle (the non-blocking writer may
/// lag a live process; a stopped process has flushed on the guard).
fn wait_log_contains(path: &Path, needle: &str, deadline: Duration) {
    let started = Instant::now();
    loop {
        if let Ok(text) = fs::read_to_string(path)
            && text.contains(needle)
        {
            return;
        }
        assert!(
            started.elapsed() < deadline,
            "timed out waiting for {needle:?} in {}",
            path.display()
        );
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn log_contains(path: &Path, needle: &str) -> bool {
    fs::read_to_string(path)
        .map(|text| text.contains(needle))
        .unwrap_or(false)
}

/// The clean-stop marker end-shape: the `Stopped` round then the
/// `flushed` quorum across the four superblock copies (every copy
/// readable at the final state, the hash chain two or more rounds deep),
/// and the single-file projection agreeing.
fn assert_flushed_marker(state_path: &Path) {
    let superblock = superblock_path(state_path);
    let classified = marker::classify(&superblock)
        .unwrap_or_else(|code| panic!("marker classify failed with code {code}"));
    assert_eq!(
        classified.state,
        MarkerState::Flushed,
        "the final marker state must be flushed"
    );
    let identity = classified.identity;
    let copies = marker::inspect(&superblock).expect("marker inspect");
    assert_eq!(copies.len(), 4, "the marker zone holds four copies");
    for (index, copy) in copies.iter().enumerate() {
        assert_eq!(copy.readable, 1, "copy {index} must be readable");
        assert_eq!(copy.valid_checksum, 1, "copy {index} must checksum");
        assert_eq!(
            copy.state,
            MarkerState::Flushed.code(),
            "copy {index} must sit at flushed"
        );
        assert_eq!(
            copy.node,
            identity.packed(),
            "copy {index} must hold the marker's identity pair"
        );
        assert!(
            copy.sequence >= 2,
            "copy {index} sequence {} must show at least the Stopped round \
             then the flushed round",
            copy.sequence
        );
    }
    let projection = fs::read_to_string(state_path).expect("single-file projection");
    assert!(
        projection.trim().starts_with(&format!(
            "{} {} flushed",
            identity.system_identifier(),
            identity.crash_counter()
        )),
        "the single-file projection must read the identity pair and \
         `flushed`, got {projection:?}"
    );
}

/// The running sentinel: the marker stands at `unflushed` (the crash
/// shape — the stop path never ran).
fn assert_unflushed_marker(state_path: &Path) {
    let superblock = superblock_path(state_path);
    let classified = marker::classify(&superblock)
        .unwrap_or_else(|code| panic!("marker classify failed with code {code}"));
    assert_eq!(
        classified.state,
        MarkerState::Unflushed,
        "the running sentinel must stand"
    );
    let projection = fs::read_to_string(state_path).expect("single-file projection");
    assert!(
        projection.trim().ends_with("unflushed"),
        "the single-file projection must read the running sentinel, got \
         {projection:?}"
    );
}

fn superblock_path(state_path: &Path) -> PathBuf {
    let mut os = state_path.as_os_str().to_os_string();
    os.push(".superblock");
    PathBuf::from(os)
}

/// The restart record's bumped identity: parse `new=` from the later-life
/// record and demand the marker pair's next life.
fn assert_bumped_identity(log: &Path) -> u32 {
    let text = fs::read_to_string(log).expect("boot log");
    for line in text.lines() {
        if let Some(new_text) = line
            .split_whitespace()
            .find_map(|field| field.strip_prefix("new="))
            .filter(|_| line.contains("later life of the same system"))
        {
            let bumped: u32 = new_text
                .parse()
                .unwrap_or_else(|_| panic!("later-life record's new={new_text}"));
            // The announced identity is the marker pair's next life: a
            // lawful packed id whose crash counter is a life past the
            // genesis one.
            assert!(
                (bumped >> 16) != 0 && (bumped & 0xffff) >= 2,
                "the later life {bumped} is a lawful identity past the genesis counter"
            );
            return bumped;
        }
    }
    panic!("no later-life record in {}", log.display());
}

/// Cleanly stop every node (TERM → the same stop contract the gate
/// proves) and reap; the `ServeProcess` drop is the backstop.
fn stop_cluster(mut nodes: Vec<ServeProcess>) {
    for node in &mut nodes {
        if node.alive() {
            send_signal(node, "TERM");
        }
    }
    for node in &mut nodes {
        let status = node.wait_exit("cluster teardown", Duration::from_secs(60));
        assert_eq!(
            status.code(),
            Some(0),
            "teardown of {} must exit cleanly",
            node.name
        );
    }
}

/// One clean-stop signal case, shared by TERM/INT/QUIT: spawn the
/// cluster, drive the first node to serving, signal it, and assert the
/// full clean-stop shape — clean exit (not a signal death), the named
/// stop-begin record for THAT signal, the drain and drain-proven
/// records, the flushed marker quorum at the same incarnation, and the
/// re-boot under the SAME identity (no bump, no resurrection).
fn clean_stop_case(signal_name: &str, stop_record: &str) {
    let _gate = gate_lock();
    let dir = scratch(&format!("{signal_name}-clean-stop"));
    let state_path = dir.join("n1.state");
    let mut nodes = spawn_cluster(&dir);
    let mut node = nodes.remove(0);
    let _peers = nodes;
    wait_serving(&node, &format!("{signal_name}: the node reaches serving"));

    send_signal(&node, signal_name);
    let status = node.wait_exit(signal_name, Duration::from_secs(60));
    assert_eq!(
        status.code(),
        Some(0),
        "{signal_name} must exit with the recorded clean-stop code 0, not a \
         signal death"
    );

    let log = log_file(&node);
    wait_log_contains(&log, stop_record, Duration::from_secs(10));
    wait_log_contains(&log, DRAIN_RECORD, Duration::from_secs(10));
    wait_log_contains(&log, FLUSHED_RECORD, Duration::from_secs(10));
    assert_flushed_marker(&state_path);

    // The re-boot: same incarnation, no identity bump, no resurrection —
    // the durable state continues and the node serves again.
    let bin = env!("CARGO_BIN_EXE_lease-sequencer");
    let config_path = dir.join("cluster.jsonl");
    let stdout = fs::File::create(dir.join("n1.boot2.stdout")).expect("stdout capture");
    let stderr = fs::File::create(dir.join("n1.boot2.stderr")).expect("stderr capture");
    let mut rebooted = ServeProcess {
        child: Command::new(bin)
            .args([
                "--name",
                "n1",
                "--config",
                config_path.to_str().expect("config path"),
                "--client",
                &format!("127.0.0.1:{}", node.client_port),
                "--state",
                state_path.to_str().expect("state path"),
                "--log",
                dir.join("n1.log").to_str().expect("log path"),
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("re-boot spawns"),
        dir: dir.clone(),
        name: "n1".to_string(),
        client_port: node.client_port,
    };
    wait_serving(&rebooted, &format!("{signal_name}: the re-boot serves"));
    let boot_log = log_file(&rebooted);
    wait_log_contains(
        &boot_log,
        "node provisioned own=1 incarnation=0",
        Duration::from_secs(10),
    );
    assert!(
        !log_contains(&boot_log, "later life in the high band"),
        "a clean-stop re-boot must NOT bump the identity"
    );
    send_signal(&rebooted, "TERM");
    let status = rebooted.wait_exit("re-boot teardown", Duration::from_secs(60));
    assert_eq!(status.code(), Some(0), "the re-boot must stop cleanly");
    assert_flushed_marker(&state_path);
}

#[test]
fn sigterm_drives_the_clean_stop_and_same_incarnation_reboot() {
    clean_stop_case("TERM", "sigterm: clean stop");
}

#[test]
fn sigint_drives_the_clean_stop_and_same_incarnation_reboot() {
    clean_stop_case("INT", "sigint: clean stop");
}

#[test]
fn sigquit_drives_the_clean_stop_and_same_incarnation_reboot() {
    clean_stop_case("QUIT", "sigquit: clean stop");
}

/// The negative: SIGKILL skips the stop path entirely — the running
/// sentinel stands and the next boot bumps the identity into the
/// `>= 2^24` band (error-on-crashed), the documented crash shape.
#[test]
fn sigkill_leaves_the_running_sentinel_and_next_boot_bumps() {
    let _gate = gate_lock();
    let dir = scratch("sigkill-negative");
    let state_path = dir.join("n1.state");
    let mut nodes = spawn_cluster(&dir);
    let mut node = nodes.remove(0);
    let mut peers = nodes;
    wait_serving(&node, "sigkill: the node reaches serving");

    send_signal(&node, "KILL");
    let status = node.wait_exit("sigkill", Duration::from_secs(60));
    assert!(
        status.code().is_none(),
        "SIGKILL must not exit through the stop path"
    );
    assert_unflushed_marker(&state_path);

    // The peers do not carry evidence this case needs; stop them so the
    // re-boot runs alone.
    stop_cluster(peers.drain(..).collect());

    let bin = env!("CARGO_BIN_EXE_lease-sequencer");
    let config_path = dir.join("cluster.jsonl");
    let stdout = fs::File::create(dir.join("n1.boot2.stdout")).expect("stdout capture");
    let stderr = fs::File::create(dir.join("n1.boot2.stderr")).expect("stderr capture");
    let mut rebooted = ServeProcess {
        child: Command::new(bin)
            .args([
                "--name",
                "n1",
                "--config",
                config_path.to_str().expect("config path"),
                "--client",
                &format!("127.0.0.1:{}", node.client_port),
                "--state",
                state_path.to_str().expect("state path"),
                "--log",
                dir.join("n1.log").to_str().expect("log path"),
            ])
            .env("RUST_LOG", "info")
            .stdout(Stdio::from(stdout))
            .stderr(Stdio::from(stderr))
            .spawn()
            .expect("re-boot spawns"),
        dir: dir.clone(),
        name: "n1".to_string(),
        client_port: node.client_port,
    };
    let boot_log = log_file(&rebooted);
    wait_log_contains(
        &boot_log,
        "later life in the high band",
        Duration::from_secs(30),
    );
    let bumped = assert_bumped_identity(&boot_log);
    // The bumped life boots under its new identity; the marker round for
    // the bump defers to the seated witness (the boot gate's
    // crashed classification), so the identity evidence is the boot's
    // own provisioned record.
    wait_log_contains(
        &boot_log,
        &format!("node provisioned own={bumped} incarnation=1"),
        Duration::from_secs(30),
    );
    send_signal(&rebooted, "TERM");
    let status = rebooted.wait_exit("bumped-life teardown", Duration::from_secs(60));
    assert_eq!(status.code(), Some(0), "the bumped life must stop cleanly");
}

/// SIGHUP is a logged no-op: the process survives, the noop record
/// appears, and a following ops attempt succeeds — the node keeps
/// serving.
#[test]
fn sighup_is_a_logged_noop_and_the_node_keeps_serving() {
    let _gate = gate_lock();
    let dir = scratch("sighup-noop");
    let state_path = dir.join("n1.state");
    let mut nodes = spawn_cluster(&dir);
    let mut node = nodes.remove(0);
    let peers = nodes;
    wait_serving(&node, "sighup: the node reaches serving");

    send_signal(&node, "HUP");
    let log = log_file(&node);
    wait_log_contains(&log, HUP_NOOP_RECORD, Duration::from_secs(30));
    assert!(node.alive(), "SIGHUP must never stop the process");
    let reply = wait_serving(&node, "sighup: the node keeps serving");
    assert!(
        reply.contains("\"op\":\"get\""),
        "the following ops attempt must commit, got {reply:?}"
    );

    send_signal(&node, "TERM");
    let status = node.wait_exit("sighup teardown", Duration::from_secs(60));
    assert_eq!(status.code(), Some(0), "the post-HUP stop must be clean");
    assert_flushed_marker(&state_path);
    stop_cluster(peers);
}
