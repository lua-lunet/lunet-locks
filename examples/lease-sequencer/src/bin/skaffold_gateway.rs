//! The gateway's skaffold driver: the RFC's session loop
//! (`docs/src/rfc-application-protocol.md`) run end to end against a
//! real committing core in-process — three adapter `Node`s, the wire in
//! memory, the logical clock — with the application on a loopback TCP
//! socket. The nexus holds a `TcpStream` as the opaque handle; the
//! driver reads messages from the socket, forwards each to the leader,
//! and writes each committed result back through the taken nexus entry.
//!
//! The scenario: one application joins with its audit metadata (the
//! reply is the bearer), sends a command and its re-send back to back
//! (the re-send attaches to the command already in flight and is
//! answered once), then a second command. Every step prints a
//! `[pass]`/`[fail]` line; the exit is 0 only when the loop closed.

use lease_sequencer::gateway::{AcceptOutcome, Gateway, GatewayConfig};
use lease_sequencer::gateway_harness::Cluster;
use serde_json::{Value, json};
use std::io::{BufRead, BufReader, Write};
use std::net::{TcpListener, TcpStream};
use std::time::Duration;

fn main() {
    if let Err(failure) = run() {
        eprintln!("[fail] {failure}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), String> {
    let mut cluster = Cluster::boot("skaffold-gateway");
    let mut gateway = Gateway::new(GatewayConfig {
        key: [0x5A; 32],
        command_deadline: 30,
        session_ttl: 1_000,
    });

    // The application side: a loopback connection. The driver's end is
    // the abstract socket the messages are read from and the opaque
    // handle the nexus holds.
    let listener = TcpListener::bind("127.0.0.1:0")
        .map_err(|error| format!("the loopback listener binds: {error}"))?;
    let address = listener.local_addr().unwrap();
    let application = std::thread::spawn(move || application_session(address));
    let (socket, _) = listener
        .accept()
        .map_err(|error| format!("the application connects: {error}"))?;

    let mut reader = BufReader::new(socket.try_clone().expect("the socket clones"));
    let mut messages = 0usize;
    while let Some(line) = read_message(&mut reader, &socket, None)? {
        // Everything the application already sent behind the read (a
        // re-send read before the commit keeps its attach).
        let mut batch = vec![line];
        while let Some(line) = read_message(&mut reader, &socket, Some(50))? {
            batch.push(line);
        }
        messages += batch.len();
        for message in &batch {
            read_step(&mut gateway, &mut cluster, message, &socket)?;
        }
        // The committed half: pump the cluster, upcall every new
        // committed entry — the uuid of the command and the command
        // result as committed at the node — and write each response
        // through the nexus entry it takes.
        committed_step(&mut gateway, &mut cluster)?;
    }

    let verdict = application
        .join()
        .map_err(|_| "the application thread failed".to_string())??;
    println!("[pass] {verdict}");
    println!("[pass] the session loop closed: {messages} messages read, every reply written");
    Ok(())
}

/// One message from the abstract socket, into the gateway: the join
/// establishes the session, the commands ride it. The forward step is
/// the cluster's own (`forward` to the leader).
fn read_step(
    gateway: &mut Gateway,
    cluster: &mut Cluster,
    line: &str,
    socket: &TcpStream,
) -> Result<(), String> {
    let message: Value = serde_json::from_str(line.trim())
        .map_err(|error| format!("the message is not JSON: {error}"))?;
    let now = cluster.now();
    let outcome = if message.get("op").and_then(Value::as_str) == Some("join") {
        let audit = message["audit"].clone();
        let outcome = gateway.join(&audit, Box::new(socket.try_clone().unwrap()), now, cluster);
        println!("[loop] join read from the socket, forwarded: {outcome:?}");
        outcome
    } else {
        let bearer = message["bearer"]
            .as_str()
            .ok_or("a command carries its bearer")?
            .to_string();
        let request_num = message["request_num"]
            .as_u64()
            .ok_or("a command carries its request number")?;
        let body = message["body"].clone();
        let outcome = gateway.command(
            &bearer,
            request_num,
            &body,
            Box::new(socket.try_clone().unwrap()),
            now,
            cluster,
        );
        println!("[loop] command {request_num} read from the socket: {outcome:?}");
        outcome
    };
    if outcome == AcceptOutcome::Refused("forward_refused") {
        return Err("the forward step was refused".to_string());
    }
    Ok(())
}

/// The committed half of the loop.
fn committed_step(gateway: &mut Gateway, cluster: &mut Cluster) -> Result<(), String> {
    cluster.advance(1);
    for record in cluster.committed_since() {
        println!(
            "[loop] committed slot={} uuid={:02x?}..: upcalled with its result as committed",
            record.slot,
            &record.uuid[..4]
        );
        gateway.committed(cluster.now(), record.slot, record.uuid, &record.payload);
    }
    Ok(())
}

/// One message, or the batch tail: a blocking read when `timeout` is
/// `None`, a bounded one when the driver is draining what the
/// application already sent.
fn read_message(
    reader: &mut BufReader<TcpStream>,
    socket: &TcpStream,
    timeout: Option<u64>,
) -> Result<Option<String>, String> {
    socket
        .set_read_timeout(timeout.map(Duration::from_millis))
        .map_err(|error| format!("the read timeout sets: {error}"))?;
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) => Ok(None),
        Ok(_) => Ok(Some(line)),
        Err(error)
            if error.kind() == std::io::ErrorKind::WouldBlock
                || error.kind() == std::io::ErrorKind::TimedOut =>
        {
            Ok(None)
        }
        Err(error) => Err(format!("the socket read: {error}")),
    }
}

/// The application: one connection, one session — join, a command with
/// its re-send, a second command; every reply read back and checked on
/// the application-visible surface.
fn application_session(address: std::net::SocketAddr) -> Result<String, String> {
    let mut socket = TcpStream::connect(address)
        .map_err(|error| format!("the application connects: {error}"))?;
    let mut reader = BufReader::new(socket.try_clone().expect("the socket clones"));

    // The join, with the application's audit metadata.
    let join_line = json!({"op": "join", "audit": {"app": "console", "user": "u-1"}});
    writeln!(socket, "{join_line}").map_err(|error| format!("the join writes: {error}"))?;
    let reply = read_reply(&mut reader)?;
    let bearer = reply["bearer"]
        .as_str()
        .ok_or("the join reply carries the bearer")?
        .to_string();
    println!("[app] joined; the reply carried the bearer");

    // A command and its re-send, back to back: one reply, on the
    // attached handle.
    let command = json!({"bearer": bearer, "request_num": 1, "body": {"op": "put", "key": "k"}});
    writeln!(socket, "{command}").map_err(|error| format!("the command writes: {error}"))?;
    writeln!(socket, "{command}").map_err(|error| format!("the re-send writes: {error}"))?;
    let reply = read_reply(&mut reader)?;
    if reply.get("committed").is_none() {
        return Err("the command's reply carries the committed result".to_string());
    }
    println!("[app] the command and its re-send were answered once");

    // The next command: read the next message, repeat.
    let next = json!({"bearer": bearer, "request_num": 2, "body": {"op": "get", "key": "k"}});
    writeln!(socket, "{next}").map_err(|error| format!("the second command writes: {error}"))?;
    let reply = read_reply(&mut reader)?;
    if reply.get("committed").is_none() {
        return Err("the second command's reply carries the committed result".to_string());
    }
    println!("[app] the next command was answered");

    Ok("the application held its session end to end".to_string())
}

/// One reply line from the application's socket.
fn read_reply(reader: &mut BufReader<TcpStream>) -> Result<Value, String> {
    let mut line = String::new();
    reader
        .read_line(&mut line)
        .map_err(|error| format!("the reply reads: {error}"))?;
    if line.is_empty() {
        return Err("the socket closed before the reply".to_string());
    }
    serde_json::from_str(line.trim()).map_err(|error| format!("the reply is not JSON: {error}"))
}
