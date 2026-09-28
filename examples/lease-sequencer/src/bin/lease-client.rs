//! The demo's control client: drives one NDJSON verb (lock or admin) over a
//! node's TCP client port and prints the reply line, or prints the epoch
//! millisecond clock (`now`) for the shell orchestrator.

use std::io::{BufRead, BufReader, Write};
use std::net::TcpStream;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct Options {
    server: Option<String>,
    verb: String,
    id: Option<u64>,
    name: Option<String>,
    endpoint: Option<String>,
    lock: u64,
    lease_ms: u64,
}

fn parse_options() -> Options {
    let mut options = Options {
        server: None,
        verb: String::new(),
        id: None,
        name: None,
        endpoint: None,
        lock: 0x0DDBA11,
        lease_ms: 500,
    };
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let mut index = 0;
    while index < argv.len() {
        let flag = argv[index].clone();
        match flag.as_str() {
            "now" => {
                let ms = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .expect("clock before epoch")
                    .as_millis();
                println!("{ms}");
                std::process::exit(0);
            }
            "--server" => {
                options.server = Some(argv[index + 1].clone());
                index += 2;
            }
            "--verb" => {
                options.verb = argv[index + 1].clone();
                index += 2;
            }
            "--id" => {
                options.id = Some(argv[index + 1].parse().expect("--id number"));
                index += 2;
            }
            "--name" => {
                options.name = Some(argv[index + 1].clone());
                index += 2;
            }
            "--endpoint" => {
                options.endpoint = Some(argv[index + 1].clone());
                index += 2;
            }
            "--lock" => {
                options.lock = argv[index + 1].parse().expect("--lock number");
                index += 2;
            }
            "--lease-ms" => {
                options.lease_ms = argv[index + 1].parse().expect("--lease-ms number");
                index += 2;
            }
            other => {
                eprintln!("lease-client: unknown argument {other}");
                std::process::exit(2);
            }
        }
    }
    if options.verb.is_empty() {
        eprintln!(
            "usage: lease-client now | lease-client --server IPv4:PORT --verb \
             get|set|release|join|increment|decrement|leave [--lock N] [--lease-ms N] \
             [--id N] [--name NAME] [--endpoint IPv4:PORT]"
        );
        std::process::exit(2);
    }
    options
}

fn main() {
    let options = parse_options();
    let client_id: u64 = 900000 + (std::process::id() as u64);
    let message_id = uuid::Uuid::new_v4();
    let request_num: u64 = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock before epoch")
        .subsec_nanos() as u64;
    let json = match options.verb.as_str() {
        "get" => format!(
            "{{\"op\":\"get\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{request_num},\"lock_id\":{}}}",
            options.lock
        ),
        "set" => {
            let expiry = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .expect("clock before epoch")
                .as_millis() as u64
                + options.lease_ms;
            format!(
                "{{\"op\":\"set\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{request_num},\"lock_id\":{},\"lease\":{{\"lease_id\":1,\"holder\":\"{}\",\"expiry\":{expiry}}}}}",
                options.lock,
                uuid::Uuid::new_v4()
            )
        }
        "release" => format!(
            "{{\"op\":\"release\",\"message_id\":\"{message_id}\",\"client_id\":{client_id},\"request_num\":{request_num},\"lock_id\":{},\"holder\":\"{}\",\"lease_id\":1}}",
            options.lock,
            uuid::Uuid::new_v4()
        ),
        "join" => format!(
            "{{\"action\":\"join\",\"message_id\":\"{message_id}\",\"id\":{},\"name\":\"{}\",\"endpoint\":\"{}\"}}",
            options.id.unwrap_or_else(|| die("--id required for join")),
            options.name.clone().unwrap_or_default(),
            options.endpoint.clone().unwrap_or_default()
        ),
        "increment" | "decrement" | "leave" => format!(
            "{{\"action\":\"{}\",\"message_id\":\"{message_id}\",\"id\":{}}}",
            options.verb,
            options.id.unwrap_or_else(|| die("--id required"))
        ),
        other => {
            eprintln!("lease-client: unknown verb {other}");
            std::process::exit(2);
        }
    };
    let server = options.server.unwrap_or_else(|| die("--server required"));
    let stream = TcpStream::connect(server.as_str()).unwrap_or_else(|e| {
        eprintln!("lease-client: connect {server} failed: {e}");
        std::process::exit(1);
    });
    stream
        .set_read_timeout(Some(Duration::from_secs(20)))
        .expect("read timeout");
    let mut stream = stream;
    stream
        .write_all(json.as_bytes())
        .and_then(|_| stream.write_all(b"\n"))
        .and_then(|_| stream.flush())
        .unwrap_or_else(|e| {
            eprintln!("lease-client: write failed: {e}");
            std::process::exit(1);
        });
    let mut reader = BufReader::new(stream);
    let mut line = String::new();
    match reader.read_line(&mut line) {
        Ok(0) | Err(_) => {
            eprintln!("lease-client: no reply");
            std::process::exit(1);
        }
        Ok(_) => {
            print!("{}", line.trim_end());
            println!();
        }
    }
}

fn die(message: &str) -> ! {
    eprintln!("lease-client: {message}");
    std::process::exit(2);
}
