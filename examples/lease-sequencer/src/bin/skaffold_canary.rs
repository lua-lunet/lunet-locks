//! The rig canary: "can we find our arse with both hands on the cloud
//! servers? Y/N" (`papers/AGENTS.md` testing methodology). It proves the
//! environment — address binding, resolution, and reachability — OUTSIDE
//! any service logic, binding the exact shapes `lease-sequencer` binds
//! (a v6 UDP socket on the descriptor endpoint and a TCP client listener
//! on the same address with the +1000 port offset) and round-tripping
//! both a UDP datagram exchange and a newline-JSON TCP exchange with the
//! peer. Exit 0 only when every check passes; one `Y`/`N` line per check.
//!
//! The canary is SYMMETRIC: run it on both ends of a pair (start them
//! within a few seconds of each other; fire-and-forget works). Each side
//! sends marked datagrams to the peer, echoes back every marked datagram
//! that is not its own (a per-process run id in the payload prevents
//! self-reflection ping-pong), retries its sends and its TCP dial until
//! a deadline, and then lingers a few seconds as a pure echo responder
//! so a slightly later peer still gets its echoes. This is what makes
//! "run on both ends, nearly together" converge regardless of which
//! side binds first.
//!
//! usage: skaffold_canary BIND PEER [N]
//!   BIND  "host:port" — the UDP/peer shape this process binds (the same
//!         string shape the descriptor's `host` + `port` produce).
//!   PEER  "host:port" — the peer's UDP endpoint (its TCP client port is
//!         PEER port + 1000, the service's client-port pattern).
//!   N     canary datagram count (default 3).

use std::io::{BufRead, BufReader, Write};
use std::net::{TcpStream, ToSocketAddrs, UdpSocket};
use std::time::{Duration, Instant};

const UDP_POLL: Duration = Duration::from_millis(500);
// Rig-start budget: the peer's process is launched over a separate ssh
// session, so "both ends nearly together" can mean tens of seconds of
// skew. The exchange phases wait THIS long before declaring N; the
// unit tests pass much shorter budgets for fast feedback.
const UDP_ROUND_BUDGET: Duration = Duration::from_secs(20);
const TCP_TIMEOUT: Duration = Duration::from_secs(3);
const TCP_DIAL_BUDGET: Duration = Duration::from_secs(20);
const LINGER: Duration = Duration::from_secs(3);

/// Resolve an endpoint string the way the service does
/// (`to_socket_addrs` over the `host:port` text), first as given —
/// which handles the bracketed-literal v6 form `[..]:port` — then,
/// on failure, with the brackets stripped so a bare literal also
/// resolves. Returns the first candidate address.
fn resolve(endpoint: &str) -> Option<std::net::SocketAddr> {
    if let Ok(mut addrs) = endpoint.to_socket_addrs()
        && let Some(addr) = addrs.next()
    {
        return Some(addr);
    }
    let bare = endpoint.trim_start_matches('[').replace("]:", ":");
    if let Ok(mut addrs) = bare.to_socket_addrs()
        && let Some(addr) = addrs.next()
    {
        return Some(addr);
    }
    None
}

/// The canary payload marker: every datagram and every TCP line echoes
/// it, so a foreign packet can never satisfy a check.
const MARKER: &str = "lunet-canary";

fn main() {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.len() < 2 {
        eprintln!("usage: skaffold_canary BIND PEER [N]");
        std::process::exit(2);
    }
    let bind = argv[0].clone();
    let peer = argv[1].clone();
    let count: usize = argv
        .get(2)
        .and_then(|n| n.parse().ok())
        .filter(|n| *n > 0)
        .unwrap_or(3);
    let runid: String = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| format!("{:x}", d.subsec_nanos() as u64 | (d.as_secs() << 32)))
        .unwrap_or_else(|_| "0".into());

    let mut passed = 0usize;
    let mut failed = 0usize;
    let mut line = |ok: bool, name: &str, detail: &str| {
        println!("{} {} {detail}", if ok { "Y" } else { "N" }, name);
        if ok {
            passed += 1;
        } else {
            failed += 1;
        }
    };

    // Check 1: bind the exact shapes the service binds — the UDP peer
    // port on the BIND string and the TCP client listener on port+1000.
    let (bind_host, bind_port_text) = match bind.rsplit_once(':') {
        Some(pair) => pair,
        None => {
            eprintln!("skaffold_canary: BIND {bind} is not host:port");
            std::process::exit(2);
        }
    };
    let bind_port: u16 = bind_port_text.parse().unwrap_or_else(|_| {
        eprintln!("skaffold_canary: BIND port {bind_port_text} is not a number");
        std::process::exit(2);
    });
    let bare_host = bind_host.trim_start_matches('[').trim_end_matches(']');
    let client_endpoint = format!("[{bare_host}]:{}", bind_port + 1000);
    let udp = UdpSocket::bind(bind.as_str());
    let udp_ok = udp.is_ok();
    line(udp_ok, "udp-bind", &bind);
    let tcp = std::net::TcpListener::bind(&client_endpoint);
    let tcp_bind_ok = tcp.is_ok();
    line(tcp_bind_ok, "tcp-bind", &client_endpoint);

    let (Some(peer_addr), Some(peer_client)) = (resolve(&peer), client_addr(&peer)) else {
        line(false, "peer-resolve", &peer);
        finish(passed, failed);
        return;
    };
    line(true, "peer-resolve", &format!("{peer} -> {peer_addr}"));

    // Short-circuit the exchange checks when a bind failed: the rounds
    // cannot run without both sockets.
    if let (Ok(sock), Ok(listener)) = (udp, tcp) {
        let udp_rounds = udp_canary_round(&sock, &runid, peer_addr, count, UDP_ROUND_BUDGET);
        line(
            udp_rounds == count,
            "udp-echo",
            &format!("{udp_rounds}/{count} to {peer_addr}"),
        );
        let echo_thread = std::thread::spawn(move || tcp_echo_server(listener, TCP_DIAL_BUDGET));
        let tcp_ok_round = tcp_echo_client(peer_client, TCP_DIAL_BUDGET);
        line(tcp_ok_round, "tcp-echo", &peer_client.to_string());
        let _ = echo_thread.join();
        // Linger as a pure echo responder so a peer that started just
        // after us still collects its echoes even though our checks
        // are done.
        linger_echo(&sock, &runid);
    } else {
        line(false, "udp-echo", "skipped: no local udp socket");
        line(false, "tcp-echo", "skipped: no local tcp listener");
    }

    finish(passed, failed);
}

/// The TCP client port of a peer UDP endpoint: same host, port + 1000
/// (the service's client-port pattern). Parsed textually first (works
/// for bracketed and bare forms alike), resolved numerically as the
/// fallback.
fn client_addr(peer: &str) -> Option<std::net::SocketAddr> {
    if let Some((host, port)) = peer.rsplit_once(':')
        && let Ok(port) = port.parse::<u16>()
    {
        let bare = host.trim_start_matches('[').trim_end_matches(']');
        let candidate = format!("[{bare}]:{}", port + 1000);
        if let Ok(mut addrs) = candidate.to_socket_addrs()
            && let Some(addr) = addrs.next()
        {
            return Some(addr);
        }
    }
    let addr = resolve(peer)?;
    Some(std::net::SocketAddr::new(addr.ip(), addr.port() + 1000))
}

fn own_payload(runid: &str, i: usize) -> String {
    format!("{MARKER}:{runid}:{i}")
}

/// One marked datagram: does it carry the canary marker at all, and is
/// it OURS (carries our run id)?
fn classify(buf: &[u8], runid: &str) -> Option<(bool, String)> {
    let text = std::str::from_utf8(buf).ok()?;
    if !text.starts_with(MARKER) {
        return None;
    }
    let ours = text.starts_with(&format!("{MARKER}:{runid}:"));
    Some((ours, text.to_string()))
}

/// N echo datagrams, symmetric-peer style: keep sending the un-acked
/// marked datagrams (re-sent every poll so a late-starting peer is
/// still caught), echo back every foreign marked datagram, count an
/// ack only when the exact own payload comes back from the peer's
/// address. Returns the verified echo count.
fn udp_canary_round(
    sock: &UdpSocket,
    runid: &str,
    peer: std::net::SocketAddr,
    count: usize,
    budget: Duration,
) -> usize {
    sock.set_read_timeout(Some(UDP_POLL)).ok();
    let deadline = Instant::now() + budget;
    let mut acked = vec![false; count];
    let mut buf = [0u8; 512];
    while !acked.iter().all(|a| *a) && Instant::now() < deadline {
        for (i, acked) in acked.iter().enumerate() {
            if !acked {
                let payload = own_payload(runid, i);
                let _ = sock.send_to(payload.as_bytes(), peer);
            }
        }
        // Drain the socket for one poll window: ack own echoes, bounce
        // foreign marked datagrams back to their sender.
        let poll_end = Instant::now() + UDP_POLL;
        while Instant::now() < poll_end {
            match sock.recv_from(&mut buf) {
                Ok((n, from)) => match classify(&buf[..n], runid) {
                    Some((true, text)) => {
                        if from.ip() == peer.ip()
                            && let Some(i) = (0..count).find(|i| text == own_payload(runid, *i))
                        {
                            acked[i] = true;
                        }
                    }
                    Some((false, text)) => {
                        let _ = sock.send_to(text.as_bytes(), from);
                    }
                    None => {}
                },
                Err(_) => break,
            }
        }
    }
    acked.iter().filter(|a| **a).count()
}

/// Echo-responder-only phase: bounce every foreign marked datagram for
/// the linger window, then return (the process exits).
fn linger_echo(sock: &UdpSocket, runid: &str) {
    sock.set_read_timeout(Some(Duration::from_millis(200))).ok();
    let deadline = Instant::now() + LINGER;
    let mut buf = [0u8; 512];
    while Instant::now() < deadline {
        if let Ok((n, from)) = sock.recv_from(&mut buf)
            && let Some((false, text)) = classify(&buf[..n], runid)
        {
            let _ = sock.send_to(text.as_bytes(), from);
        }
    }
}

/// The bounded TCP echo-server role: accept one connection within the
/// deadline (non-blocking poll, never a bare blocking accept — a dead
/// peer must time the whole canary out, not hang it), read one line,
/// reply `{"canary":<line>}`.
fn tcp_echo_server(listener: std::net::TcpListener, accept_budget: Duration) {
    listener.set_nonblocking(true).ok();
    let deadline = Instant::now() + accept_budget;
    let mut stream = loop {
        if Instant::now() > deadline {
            return;
        }
        match listener.accept() {
            Ok((stream, _)) => break stream,
            Err(_) => std::thread::sleep(Duration::from_millis(10)),
        }
    };
    stream.set_nonblocking(false).ok();
    stream.set_read_timeout(Some(TCP_TIMEOUT)).ok();
    let mut reader = BufReader::new(&mut stream);
    let mut line = String::new();
    if reader.read_line(&mut line).is_ok() && !line.is_empty() {
        let reply = format!("{{\"canary\":{}}}", line.trim());
        let _ = stream.write_all(reply.as_bytes());
        let _ = stream.write_all(b"\n");
    }
}

/// The TCP newline-JSON round trip client role: dial the peer's client
/// port (retrying until the deadline — the peer's listener may come up
/// a beat after ours), send one JSON line, read one reply line. The
/// echo server replies with the line it received, so the check is the
/// marker's presence in the reply.
fn tcp_echo_client(peer_client: std::net::SocketAddr, dial_budget: Duration) -> bool {
    let deadline = Instant::now() + dial_budget;
    let mut stream = loop {
        if Instant::now() > deadline {
            return false;
        }
        match TcpStream::connect_timeout(&peer_client, TCP_TIMEOUT) {
            Ok(stream) => break stream,
            Err(_) => std::thread::sleep(Duration::from_millis(300)),
        }
    };
    stream.set_read_timeout(Some(TCP_TIMEOUT)).ok();
    if stream
        .write_all(format!("{{\"{MARKER}\":1}}\n").as_bytes())
        .is_err()
    {
        return false;
    }
    let mut reader = BufReader::new(stream);
    let mut reply = String::new();
    reader.read_line(&mut reply).is_ok() && reply.contains(MARKER)
}

fn finish(passed: usize, failed: usize) {
    eprintln!("skaffold_canary: {passed} passed, {failed} failed");
    if failed > 0 {
        std::process::exit(1);
    }
    std::process::exit(0);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_handles_bracketed_and_bare_v6() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn client_addr_is_port_plus_thousand() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn classify_never_claims_foreign_packets() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn two_symmetric_canaries_both_pass_on_localhost() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn tcp_both_directions_round_trip() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }

    #[test]
    fn dead_peer_times_out_without_hanging() {
        panic!("EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration")
    }
}
