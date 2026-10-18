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
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use tbio::envelope::Record;

use lease_sequencer::bridge::{RawTransition, Server, lift_transitions, replay_transitions};
use lease_sequencer::telemetry::TelemetryLog;
use lunet_advisory_lock::journal::{
    Journal, JournalEvent, KIND_BREAK, KIND_HOLD, KIND_RELEASE, KIND_RENEW, Meta,
};
use lunet_advisory_lock::locks::Transition;
use lunet_advisory_lock::witness::{Frontier, SyncOutcome, WitnessWatermarks};

fn main() {
    let mut listen = String::new();
    let mut aof_dir: Option<PathBuf> = None;
    let mut flush_ms: u64 = 1000;
    let mut rollover_mib: u64 = 4;
    let mut retention_mib: u64 = 64;
    let mut bind: Option<String> = None;
    let mut follow = false;
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
            "--bind" => {
                bind = Some(argv.next().unwrap_or_else(|| die("--bind needs ADDR:PORT")));
            }
            "--retention-mib" => {
                retention_mib = argv
                    .next()
                    .unwrap_or_else(|| die("--retention-mib needs a number"))
                    .parse()
                    .unwrap_or_else(|_| die("--retention-mib must be a number"));
            }
            "--follow" => follow = true,
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
        retention_mib * 1024 * 1024,
    )
    .unwrap_or_else(|e| die(&format!("cannot open the tape at {}: {e}", dir.display())));

    println!(
        "witness: receiving on udp {listen}, series {}, flush-ms {}",
        dir.display(),
        flush_ms
    );
    // The served cache: the witness IS the console's decoupled backend —
    // the bridge's read-only OpenAPI server on this witness's own
    // directory, the locks lifted off the tape here and served live
    // (the --follow WS push rides the tape's own writes). The running
    // nodes are decoupled from it: the console's reads never touch a
    // member.
    if let Some(bind) = &bind {
        let server = Server::spawn(&dir, None, bind, follow)
            .unwrap_or_else(|e| die(&format!("cannot serve on {bind}: {e}")));
        println!(
            "witness: serving the console on http://{} (series {}, follow {follow})",
            server.port(),
            dir.display()
        );
    }

    let mut watermarks = WitnessWatermarks::new();
    let mut last_flush = now_ms();
    // The lock-event series: the lift's own write seat, the same
    // directory the wire tape occupies — one directory, two series (the
    // feed globs the ev-* names, the tape's envelopes are the epoch
    // names; neither collides).
    let mut journal = Journal::open(&dir, 8 * 1024 * 1024)
        .unwrap_or_else(|e| die(&format!("cannot open the event series: {e}")));
    // The lift's cursor: the frontier reached — the last written
    // transition's (era, view, slot) — never a count. The tape's
    // retention drops the oldest files and the replay window re-bases
    // underneath the lift each tick; whatever the window still carries
    // at or below the frontier is already in the series, and the lift
    // resumes exactly past it.
    let mut lifted: Option<Frontier> = None;

    loop {
        let mut buf = [0u8; 65_536];
        match socket.recv_from(&mut buf) {
            Ok((size, _peer)) => {
                let datagram = &buf[..size];
                // The peer envelope the members forward: the magic, the
                // kind byte, the fingerprint, then the payload. The
                // kinds: 1 = a VRR datagram (taped whole, the buffered
                // watermark observing the frontier its wire header
                // carries); 4 = the stop's sync point (the ceremony's
                // step 1 — the three rules apply, and rule 2 forces the
                // flush of the batch in hand).
                match classify(datagram) {
                    Kind::Vrr { frontier, payload } => {
                        // The wire tape: the VRR payload whole — the
                        // envelope is the transport's, never the stream's.
                        tape.record(Record::wire(local_ns(), payload));
                        if let Some(frontier) = frontier {
                            watermarks.observe(frontier);
                        }
                    }
                    Kind::SyncPoint { frontier } => {
                        eprintln!("witness: sync point {frontier:?} received");
                        match watermarks.sync_point(frontier) {
                            SyncOutcome::Acked => {
                                eprintln!("witness: sync point {frontier:?} acknowledged")
                            }
                            SyncOutcome::ForceFlush => {
                                tape.force_flush(now_ms());
                                for point in watermarks.flushed_through_buffer() {
                                    eprintln!("witness: sync point {point:?} acknowledged");
                                }
                            }
                            SyncOutcome::Held => {
                                eprintln!("witness: sync point {frontier:?} held open")
                            }
                        }
                    }
                    Kind::Other => {}
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
        // bytes, the absolute expiry) encoded whole. The lift's cursor
        // is the frontier reached — the last written transition's
        // (era, view, slot) — so the retention's dropped tape head
        // re-bases the replay window without moving the lift's seat:
        // work already in the series is never re-written, new work is
        // never skipped.
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
            for raw in lift_transitions(&transitions, lifted) {
                let RawTransition {
                    ts_ms,
                    frontier,
                    transition,
                    ..
                } = raw;
                let event = transition_event(&transition, ts_ms);
                if let Err(error) = journal.append(&event) {
                    eprintln!("witness: the event series refused an append ({error})");
                    break;
                }
                lifted = Some(frontier);
            }
            if let Err(error) = journal.flush() {
                eprintln!("witness: the event series' flush failed ({error})");
            }
            // The retention, on the maintenance cadence (never the
            // receive path): the wire tape keeps its newest files within
            // the byte budget (the active file never on the list), and
            // the lock-event series drops the rolled files whose every
            // lease has expired — the live set is small and the dead
            // record is bulk.
            sweep_tape(&dir, &tape, retention_mib * 1024 * 1024);
            sweep_expired_events(&dir, now);
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
/// The peer envelope the members speak: the magic (`\0` + the name + `\0`,
/// 26 bytes), the kind byte, the 16-hex fingerprint, then the payload.
/// The payload kinds this witness reads: 1 = a VRR datagram (the wire
/// tape's record; its header is tag|era|view|slot, 20 bytes, big-endian),
/// 4 = the stop's sync point (the payload is the frontier triple, 16
/// bytes). Anything else is nothing: the members forward two kinds.
const PEER_MAGIC: &[u8; 26] = b"\0LUNET_ADVISORY_LOCK_PEER\0";
const PEER_HEADER_BYTES: usize = 26 + 1 + 16;

enum Kind<'a> {
    /// A VRR datagram: the frontier its wire header carries, and the raw
    /// VRR payload the tape records — the envelope is the transport's,
    /// never the stream's.
    Vrr {
        frontier: Option<Frontier>,
        payload: &'a [u8],
    },
    /// The stop's sync point: the frontier triple, 16 bytes.
    SyncPoint {
        frontier: Frontier,
    },
    Other,
}

fn classify(datagram: &[u8]) -> Kind<'_> {
    if datagram.len() < PEER_HEADER_BYTES || &datagram[..26] != PEER_MAGIC {
        return Kind::Other;
    }
    let payload = &datagram[PEER_HEADER_BYTES..];
    match datagram[26] {
        1 => {
            if payload.len() < 20 {
                return Kind::Other;
            }
            let era = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            let view = u32::from_be_bytes([payload[8], payload[9], payload[10], payload[11]]);
            let slot = u64::from_be_bytes([
                payload[12],
                payload[13],
                payload[14],
                payload[15],
                payload[16],
                payload[17],
                payload[18],
                payload[19],
            ]);
            let frontier = Frontier { era, view, slot };
            Kind::Vrr {
                frontier: Some(frontier),
                payload,
            }
        }
        4 => {
            if payload.len() < 16 {
                return Kind::Other;
            }
            let era = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let view = u32::from_be_bytes([payload[4], payload[5], payload[6], payload[7]]);
            let slot = u64::from_be_bytes([
                payload[8],
                payload[9],
                payload[10],
                payload[11],
                payload[12],
                payload[13],
                payload[14],
                payload[15],
            ]);
            Kind::SyncPoint {
                frontier: Frontier { era, view, slot },
            }
        }
        _ => Kind::Other,
    }
}

/// The wire tape's retention: the newest files within the byte budget,
/// the active file never on the list (the plan's own floor: the active
/// plus one older).
fn sweep_tape(dir: &Path, tape: &TelemetryLog, retention_bytes: u64) {
    use tbio::retention::{AofSeriesFile, list_aof_files, retention_plan};
    let active = tape.active_path();
    let Ok(listed) = list_aof_files(dir) else {
        return;
    };
    let files: Vec<AofSeriesFile> = listed
        .into_iter()
        .filter(|(path, _, _)| path != active)
        .map(|(path, (epoch, seq), size)| AofSeriesFile {
            epoch,
            seq,
            name: path
                .file_name()
                .map(|n| n.to_string_lossy().to_string())
                .unwrap_or_default(),
            size,
        })
        .collect();
    for name in retention_plan(&files, 0, retention_bytes) {
        if let Err(error) = std::fs::remove_file(dir.join(&name)) {
            eprintln!("witness: the tape's prune left {name} ({error})");
        }
    }
}

/// The lock-event series' lease horizon: a rolled file whose EVERY lease
/// has expired is droppable — the metafile's expiry window is the bound,
/// and the reader-valid-prefix rule plus the console's IndexedDB history
/// tolerate the deletion. The live set is small; the dead record is bulk.
fn sweep_expired_events(dir: &Path, now_ms: u64) {
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for entry in entries.flatten() {
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.starts_with("ev-") || !name.ends_with(".bin") {
            continue;
        }
        let meta_path = entry.path().with_extension("meta");
        let Ok(bytes) = std::fs::read(&meta_path) else {
            continue;
        };
        let Some(meta) = Meta::decode(&bytes) else {
            continue;
        };
        if now_ms > meta.expiry_max {
            let _ = std::fs::remove_file(entry.path());
            let _ = std::fs::remove_file(meta_path);
        }
    }
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
