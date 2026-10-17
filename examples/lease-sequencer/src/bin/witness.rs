//! The witness: the deployment's state holder, as a standalone
//! out-of-roster receiver.
//!
//! ```text
//! witness --listen ADDR:PORT --aof-dir PATH [--flush-ms N] [--rollover-mib N]
//! ```
//!
//! The witness is outside the roster, never votes, and receives the full
//! phase-2 and commit stream from every leader in turn (the static
//! witness list every member loads from its own host config — the
//! members' push, not this process's pull). Its append-only series is
//! the durable copy of the stream: the `{epoch}.aof` envelope tape, one
//! Wire record per received datagram, byte-identical to what the network
//! carried — huge by design, everything that happened, for debugging and
//! for the operator's driven cold-start replay.
//!
//! The two high-watermarks (the highest frontier held in the buffer and
//! the highest flushed) observe every datagram's wire header, and the
//! flush cadence is the AOF shape: deferred, batched, force-flushed on
//! the timer rather than on the receive path, so the witness's I/O
//! jitter stays its own. The sync-point rule is the watermarks': flushed
//! covers the point — ack at once; in the buffer — force the flush of
//! the batch in hand; unseen — hold it open, flush the moment the stream
//! covers it, ack then. No reply is ever awaited: the series is the
//! backup certificate.
//!
//! The witness is never on any startup path: a cluster member never
//! reads this directory. The lock-state lift off the tape (the feed and
//! the console's series) and the operator's replay tool land beside this
//! receiver.

use std::net::UdpSocket;
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use tbio::envelope::Record;

use lease_sequencer::bridge::{RawTransition, replay_transitions};
use lease_sequencer::telemetry::TelemetryLog;
use lunet_advisory_lock::journal::{
    Journal, JournalEvent, KIND_BREAK, KIND_HOLD, KIND_RELEASE, KIND_RENEW,
};
use lunet_advisory_lock::locks::Transition;
use lunet_advisory_lock::witness::{Frontier, WitnessWatermarks};

fn main() {
    let mut listen = String::new();
    let mut aof_dir: Option<PathBuf> = None;
    let mut flush_ms: u64 = 1000;
    let mut rollover_mib: u64 = 4;
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--listen" => {
                listen = argv
                    .next()
                    .unwrap_or_else(|| die("--listen needs ADDR:PORT"));
            }
            "--aof-dir" => {
                aof_dir = Some(PathBuf::from(
                    argv.next().unwrap_or_else(|| die("--aof-dir needs a path")),
                ));
            }
            "--flush-ms" => {
                flush_ms = argv
                    .next()
                    .unwrap_or_else(|| die("--flush-ms needs a number"))
                    .parse()
                    .unwrap_or_else(|_| die("--flush-ms must be a number"));
            }
            "--rollover-mib" => {
                rollover_mib = argv
                    .next()
                    .unwrap_or_else(|| die("--rollover-mib needs a number"))
                    .parse()
                    .unwrap_or_else(|_| die("--rollover-mib must be a number"));
            }
            "--help" | "-h" => {
                print_usage();
                std::process::exit(0);
            }
            other => die(&format!("unknown option {other}")),
        }
    }
    let Some(dir) = aof_dir else {
        print_usage();
        std::process::exit(2);
    };
    let socket =
        UdpSocket::bind(&listen).unwrap_or_else(|e| die(&format!("cannot bind {listen}: {e}")));
    // The receive waits in short slices so the flush cadence stays the
    // timer's, not the traffic's: an idle witness still flushes.
    socket
        .set_read_timeout(Some(std::time::Duration::from_millis(100)))
        .expect("the read timeout sets");
    let mut tape = TelemetryLog::open(
        &dir,
        flush_ms,
        rollover_mib * 1024 * 1024,
        u64::MAX / 2, // the retention prune is W5's; keep-everything at open
    )
    .unwrap_or_else(|e| die(&format!("cannot open the tape at {}: {e}", dir.display())));

    println!(
        "witness: receiving on udp {listen}, series {}, flush-ms {flush_ms}",
        dir.display()
    );

    let mut watermarks = WitnessWatermarks::new();
    let mut last_flush = now_ms();
    // The lock-event series: the lift's own write seat, the same
    // directory the wire tape occupies — one directory, two series (the
    // feed globs the ev-* names, the tape's envelopes are the epoch
    // names; neither collides).
    let mut journal = Journal::open(&dir, 8 * 1024 * 1024)
        .unwrap_or_else(|e| die(&format!("cannot open the event series: {e}")));
    let mut derived: usize = 0;

    loop {
        let mut buf = [0u8; 65_536];
        match socket.recv_from(&mut buf) {
            Ok((size, _peer)) => {
                // The wire tape: the datagram whole, byte-identical, and
                // the buffered watermark observes the frontier its wire
                // header carries.
                let datagram = &buf[..size];
                tape.record(Record::wire(local_ns(), datagram));
                if let Some(frontier) = header_frontier(datagram) {
                    watermarks.observe(frontier);
                }
            }
            Err(error)
                if error.kind() == std::io::ErrorKind::WouldBlock
                    || error.kind() == std::io::ErrorKind::TimedOut => {}
            Err(error) => die(&format!("the receive loop failed: {error}")),
        }

        // The flush cadence: the timer, never the receive path. The
        // flushed watermark moves with the tape's flush, the held sync
        // points it covers are acknowledged — logged, never awaited —
        // and the lift writes the newly derived lock events to the
        // series: the committed stream replayed through the real state
        // machine, the transitions' own shapes (the lease id, the holder
        // bytes, the absolute expiry) encoded whole.
        let now = now_ms();
        tape.tick(now);
        if flush_ms > 0 && now.saturating_sub(last_flush) >= flush_ms {
            let acked = watermarks.flushed_through_buffer();
            for point in acked {
                eprintln!("witness: sync point {point:?} acknowledged");
            }
            eprintln!(
                "witness: the tape flushed (buffered {:?}, held {})",
                watermarks.buffered(),
                watermarks.held().len()
            );
            let transitions = replay_transitions(&dir);
            for raw in transitions.iter().skip(derived) {
                let RawTransition {
                    ts_ms, transition, ..
                } = raw;
                let event = transition_event(transition, *ts_ms);
                if let Err(error) = journal.append(&event) {
                    eprintln!("witness: the event series refused an append ({error})");
                    break;
                }
                derived += 1;
            }
            if let Err(error) = journal.flush() {
                eprintln!("witness: the event series' flush failed ({error})");
            }
            last_flush = now;
        }
    }
}

/// The LKE1 record one transition encodes: the fields are the state
/// machine's own — the lease id, the holder bytes, the absolute expiry.
fn transition_event(transition: &Transition, ts_ms: u64) -> JournalEvent {
    let (lock_id, lease_id, holder, expiry) = match transition {
        Transition::Hold {
            lock_id,
            lease_id,
            holder,
            expiry,
        }
        | Transition::Renew {
            lock_id,
            lease_id,
            holder,
            expiry,
        }
        | Transition::Release {
            lock_id,
            lease_id,
            holder,
            expiry,
        }
        | Transition::Break {
            lock_id,
            lease_id,
            holder,
            expiry,
        } => (*lock_id, *lease_id, *holder, *expiry),
    };
    let kind = match transition {
        Transition::Hold { .. } => KIND_HOLD,
        Transition::Renew { .. } => KIND_RENEW,
        Transition::Release { .. } => KIND_RELEASE,
        Transition::Break { .. } => KIND_BREAK,
    };
    JournalEvent {
        kind,
        ts: ts_ms,
        lock_id,
        lease_id,
        holder,
        expiry,
    }
}

/// The datagram's wire header's identity: (era, view, slot). `None` for
/// anything too short to carry one.
fn header_frontier(datagram: &[u8]) -> Option<Frontier> {
    if datagram.len() < 24 {
        return None;
    }
    Some(Frontier {
        era: u32::from_be_bytes([datagram[4], datagram[5], datagram[6], datagram[7]]),
        view: u32::from_be_bytes([datagram[8], datagram[9], datagram[10], datagram[11]]),
        slot: u64::from_be_bytes([
            datagram[16],
            datagram[17],
            datagram[18],
            datagram[19],
            datagram[20],
            datagram[21],
            datagram[22],
            datagram[23],
        ]),
    })
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

fn local_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos() as u64
}

fn print_usage() {
    eprintln!("usage: witness --listen ADDR:PORT --aof-dir PATH [--flush-ms N] [--rollover-mib N]");
}

fn die(message: &str) -> ! {
    eprintln!("witness: {message}");
    std::process::exit(2);
}
