//! The process signal gate under real OS signals. SIGUSR1/SIGUSR2 are
//! process-global; the gates are per-worker — the discipline under test
//! is that every worker follows every process transition, whichever
//! worker's apply() ran first (one signal, two workers: the first drain
//! must not consume it ahead of the other's). These tests raise real
//! signals in this test process, and a raise latches every Signals
//! registered in the process — so they are `#[ignore]`d from the normal
//! suite and must run SOLO, serialized:
//!
//!     cargo test --release --test signal_gate_test -- --ignored --test-threads=1

use lease_sequencer::client_gate::{self, Mode};
use lease_sequencer::embedded_client::Signals;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

/// A worker's gate mode mirrored out of the worker thread (0 = OFF,
/// 1 = ON) — the test's eyes on a gate the worker owns.
type Mirror = Arc<AtomicU8>;

/// One worker: its own gate drained through the shared signal gate, the
/// loop shape the lease-load binary runs.
struct Worker {
    handle: JoinHandle<()>,
    mirror: Mirror,
    run: Arc<AtomicBool>,
}

impl Worker {
    fn mode(&self) -> u8 {
        self.mirror.load(Ordering::Relaxed)
    }

    fn stop(self) {
        self.run.store(false, Ordering::Relaxed);
        self.handle.join().expect("the worker thread joins");
    }
}

/// Spawn one worker: its own client gate, driven through the shared
/// process signal gate at the given drain cadence (the loop's
/// sleep/every-iteration boundary). `start_on` boots the gate ON
/// without a signal — a worker already mid-chase when the signal
/// lands, the rig's contender shape.
fn spawn_worker(signals: &Signals, sleep_ms: u64, start_on: bool) -> Worker {
    let mirror = Arc::new(AtomicU8::new(0));
    let run = Arc::new(AtomicBool::new(true));
    let signals = signals.clone();
    let worker_mirror = Arc::clone(&mirror);
    let worker_run = Arc::clone(&run);
    let handle = std::thread::spawn(move || {
        let mut gate = client_gate::boot();
        if start_on {
            client_gate::start(&mut gate, 0);
            worker_mirror.store(1, Ordering::Relaxed);
        }
        while worker_run.load(Ordering::Relaxed) {
            signals.apply(&mut gate);
            worker_mirror.store(u8::from(gate.mode == Mode::On), Ordering::Relaxed);
            std::thread::sleep(Duration::from_millis(sleep_ms));
        }
    });
    Worker {
        handle,
        mirror,
        run,
    }
}

/// Wait until every worker's mirrored mode is `want`, or name the
/// workers that never followed the transition.
fn wait_all_mode(workers: &[&Worker], want: u8, what: &str) {
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if workers.iter().all(|worker| worker.mode() == want) {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for {what}; modes: [{}]",
            workers
                .iter()
                .map(|worker| worker.mode().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        );
        std::thread::sleep(Duration::from_millis(2));
    }
}

/// Two workers both awake and draining: one SIGUSR2, one SIGUSR1, and
/// both gates must follow both transitions.
#[test]
#[ignore = "raises real SIGUSR1/SIGUSR2 in this process; run solo (see module docs)"]
fn a_real_signal_is_a_process_transition_every_worker_follows() {
    let signals = Signals::register();
    let first = spawn_worker(&signals, 1, false);
    let second = spawn_worker(&signals, 1, false);
    signal_hook::low_level::raise(signal_hook::consts::SIGUSR2).expect("SIGUSR2 raised");
    wait_all_mode(&[&first, &second], 1, "both workers to start (SIGUSR2)");
    signal_hook::low_level::raise(signal_hook::consts::SIGUSR1).expect("SIGUSR1 raised");
    wait_all_mode(&[&first, &second], 0, "both workers to silence (SIGUSR1)");
    first.stop();
    second.stop();
}

/// The rig shape: one fast drainer and one asleep inside its 200 ms
/// renew-sleep window when the SIGUSR1 lands. The sleeper cannot drain
/// the signal while asleep — its next apply, after the wake, must still
/// follow the process silence. (The chase is entered through the seam
/// so both workers are provably mid-chase before the real signal; the
/// silence is the real-signal path under test.)
#[test]
#[ignore = "raises a real SIGUSR1 in this process; run solo (see module docs)"]
fn a_worker_sleeping_through_the_silence_follows_it_on_wake() {
    let signals = Signals::register();
    signals.signal_start();
    let fast = spawn_worker(&signals, 1, true);
    let sleepy = spawn_worker(&signals, 200, true);
    wait_all_mode(&[&fast, &sleepy], 1, "both workers chasing");
    // The sleeper is mid-window: it cannot apply for another ~150 ms.
    std::thread::sleep(Duration::from_millis(50));
    signal_hook::low_level::raise(signal_hook::consts::SIGUSR1).expect("SIGUSR1 raised");
    wait_all_mode(&[&fast], 0, "the fast worker to silence");
    wait_all_mode(
        &[&sleepy],
        0,
        "the sleeping worker to follow the silence on wake",
    );
    fast.stop();
    sleepy.stop();
}
