//! The phi-accrual leader-failure detector (item19): trailer codec,
//! per-(era, leader, addr, monitor) sketches, and the FFI C-ABI wrapper
//! embedding the `phi-accrual-detector` crate behind the `phi` feature.
//! The core's wire contract (W3: exact lengths, trailing-byte rejection)
//! is untouched — the trailer lives entirely at the adapter framing
//! layer: the host strips it before `node.receive()` and appends it after
//! the send is queued.

#[cfg(feature = "phi")]
use std::sync::OnceLock;
#[cfg(feature = "phi")]
use tokio::runtime::Runtime;

/// The trailer's 2-byte magic, inside the reserved high-band byte space
/// so a Commit body's trailing slot bytes cannot collide with it.
pub const TRAILER_MAGIC: [u8; 2] = [0xC0, 0x0B];

/// One leader heartbeat's piggybacked phi metadata, appended at the BACK
/// of the leader's heartbeat Commit datagrams (the messages followers
/// already receive under load). The leader endpoint and the monitoring
/// node id are NOT on the wire: the receiver derives both (the socket the
/// datagram answered from; itself).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Trailer {
    pub era: u32,
    pub leader: u32,
    pub seq: u32,
    pub sent_at_ms: u64,
}

impl Trailer {
    /// magic(2) | era(4) | leader(4) | seq(4) | sent_at(8), little-endian.
    pub const WIRE_BYTES: usize = 2 + 4 + 4 + 4 + 8;

    pub fn append_to(&self, packet: &mut Vec<u8>) {
        packet.extend_from_slice(&TRAILER_MAGIC);
        packet.extend_from_slice(&self.era.to_le_bytes());
        packet.extend_from_slice(&self.leader.to_le_bytes());
        packet.extend_from_slice(&self.seq.to_le_bytes());
        packet.extend_from_slice(&self.sent_at_ms.to_le_bytes());
    }

    /// Splits one packet into (front = the intact VRR message, trailer).
    /// `None` when the packet carries no trailer — the ordinary case for
    /// every non-heartbeat datagram.
    pub fn strip_from(packet: &[u8]) -> Option<(&[u8], Trailer)> {
        if packet.len() < Self::WIRE_BYTES {
            return None;
        }
        let split = packet.len() - Self::WIRE_BYTES;
        let (front, tail) = packet.split_at(split);
        if tail[0..2] != TRAILER_MAGIC {
            return None;
        }
        let mut cursor = 2;
        let read_u32 = |c: &mut usize| {
            let value = u32::from_le_bytes(tail[*c..*c + 4].try_into().expect("4 bytes"));
            *c += 4;
            value
        };
        let read_u64 = |c: &mut usize| {
            let value = u64::from_le_bytes(tail[*c..*c + 8].try_into().expect("8 bytes"));
            *c += 8;
            value
        };
        let era = read_u32(&mut cursor);
        let leader = read_u32(&mut cursor);
        let seq = read_u32(&mut cursor);
        let sent_at_ms = read_u64(&mut cursor);
        Some((
            front,
            Trailer {
                era,
                leader,
                seq,
                sent_at_ms,
            },
        ))
    }
}

/// The textual form of a UDP endpoint for the JSON sample log (v4 or v6).
pub fn addr_text(addr: std::net::SocketAddr) -> String {
    addr.to_string()
}

/// The detector's policy knobs.
#[derive(Clone, Debug)]
pub struct PhiConfig {
    /// The phi value a sketch must reach before its monitor acts.
    pub phi_threshold: f64,
    /// The heartbeat interval the leader is configured with (ms); the
    /// safety floor is a multiple of it.
    pub heartbeat_ms: u64,
    /// A view change may not fire before `safety_multiple *
    /// heartbeat_ms` of leader silence, whatever phi says.
    pub safety_multiple: f64,
    /// Arrival intervals kept per sketch (the crate's window length).
    pub window: u32,
}

impl Default for PhiConfig {
    fn default() -> Self {
        Self {
            phi_threshold: 1.0,
            heartbeat_ms: 10,
            safety_multiple: 2.0,
            window: 100,
        }
    }
}

/// One sketch's identity: the monitored view and who is watching.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SketchKey {
    pub era: u32,
    pub leader: u32,
    /// The leader's endpoint as the receiver saw it (v4 or v6).
    pub leader_addr: String,
    pub monitor: u32,
}

/// One (era, leader, addr, monitor) sketch: the embedded crate's Detector
/// (`phi` feature) or a disabled stub, plus the host's own mirror of the
/// arrival intervals — the mirror feeds the JSON sample log and the
/// learned-mean query, neither of which the crate exposes. A sketch with
/// fewer than two learned intervals reports phi 0: there is no spread to
/// deviate from, and the crate's single-interval case is degenerate.
pub struct Sketch {
    cfg: PhiConfig,
    intervals_ms: Vec<u64>,
    last_arrival_ms: u64,
    seed: bool,
    #[cfg(feature = "phi")]
    detector: ffi::DetectorHandle,
}

// The host loop is single-threaded; the raw handle never crosses threads.
#[cfg(feature = "phi")]
unsafe impl Send for Sketch {}

impl Sketch {
    pub fn new(cfg: PhiConfig) -> Self {
        #[cfg(feature = "phi")]
        let detector = ffi::new(cfg.window);
        Self {
            cfg,
            intervals_ms: Vec::new(),
            last_arrival_ms: 0,
            seed: false,
            #[cfg(feature = "phi")]
            detector,
        }
    }

    /// The arrival the last observation ended at (0 for a fresh sketch).
    pub fn last_arrival(&self) -> u64 {
        self.last_arrival_ms
    }

    /// One heartbeat arrival. The first seeds the clock and learns
    /// nothing; every later one contributes its interval.
    pub fn observe(&mut self, at_ms: u64) {
        if !self.seed {
            // First arrival: nothing to measure yet — seed the clock and
            // hand the crate its baseline.
            self.last_arrival_ms = at_ms;
            self.seed = true;
            #[cfg(feature = "phi")]
            unsafe {
                ffi::observe(self.detector, at_ms as i64)
            };
            return;
        }
        let interval = at_ms.saturating_sub(self.last_arrival_ms);
        self.intervals_ms.push(interval);
        let window = self.cfg.window as usize;
        if self.intervals_ms.len() > window {
            self.intervals_ms.remove(0);
        }
        self.last_arrival_ms = at_ms;
        #[cfg(feature = "phi")]
        unsafe {
            ffi::observe(self.detector, at_ms as i64)
        };
    }

    /// The count of learned intervals (0 for a fresh sketch).
    pub fn sample_count(&self) -> u32 {
        self.intervals_ms.len() as u32
    }

    pub fn mean_interval_ms(&self) -> f64 {
        if self.intervals_ms.is_empty() {
            return 0.0;
        }
        let total: u64 = self.intervals_ms.iter().sum();
        total as f64 / self.intervals_ms.len() as f64
    }

    /// The phi value at `now_ms` from the embedded detector. 0 with no
    /// learned interval (the sketch is empty or the feature is off — no
    /// evidence, no suspicion; the core's own timer remains the
    /// detector).
    pub fn phi(&self, now_ms: u64) -> f64 {
        if self.intervals_ms.len() < 2 {
            return 0.0;
        }
        #[cfg(feature = "phi")]
        return unsafe { ffi::value(self.detector, now_ms as i64) };
        #[cfg(not(feature = "phi"))]
        {
            let _ = now_ms;
            0.0
        }
    }
}

/// The detection decision: phi over the threshold AND the elapsed silence
/// past the hard safety floor. The floor is `safety_multiple` over the
/// LARGER of the configured heartbeat and the learned mean interval —
/// the leader's real cadence rides the host tick plus a full Prepare
/// round, so the configured value alone can sit inside the normal
/// distribution and fire on ordinary jitter.
pub fn decide(sketch: &Sketch, now_ms: u64, cfg: &PhiConfig) -> bool {
    if sketch.intervals_ms.len() < 2 {
        return false;
    }
    let silence = now_ms.saturating_sub(sketch.last_arrival_ms);
    let floor = cfg.safety_multiple * (cfg.heartbeat_ms as f64).max(sketch.mean_interval_ms());
    if (silence as f64) < floor {
        return false;
    }
    sketch.phi(now_ms) >= cfg.phi_threshold
}

/// The hard safety floor the decision respects: `safety_multiple` over the
/// larger of the configured heartbeat and the learned mean interval.
pub fn floor_ms(sketch: &Sketch, cfg: &PhiConfig) -> f64 {
    cfg.safety_multiple * (cfg.heartbeat_ms as f64).max(sketch.mean_interval_ms())
}

/// The per-node sketch table, keyed per (era, leader, addr, monitor). An
/// observation for a key that is not the live one REPLACES the table
/// (era/config change = fresh sketch); there is at most one live leader
/// being monitored at a time, so the table holds one sketch.
pub struct Table {
    cfg: PhiConfig,
    live: Option<(SketchKey, Sketch)>,
}

impl Table {
    pub fn new(cfg: PhiConfig) -> Self {
        Self { cfg, live: None }
    }

    pub fn get(&self, key: &SketchKey) -> Option<&Sketch> {
        self.live
            .as_ref()
            .filter(|(k, _)| *k == *key)
            .map(|(_, s)| s)
    }

    /// The table's live sketch and its key, whatever era it was learned
    /// under. The consumer (the host's detector step) matches on the
    /// LEADER id — a lagging node's folded configuration era can trail the
    /// leader's trailer era, and demanding era equality would make every
    /// sketch look never-observed.
    pub fn live(&self) -> Option<(&SketchKey, &Sketch)> {
        self.live.as_ref().map(|(key, sketch)| (key, sketch))
    }

    pub fn observe(&mut self, key: &SketchKey, at_ms: u64) -> Option<u64> {
        match &mut self.live {
            Some((k, _)) if *k == *key => {}
            _ => {
                self.live = Some((key.clone(), Sketch::new(self.cfg.clone())));
            }
        }
        let (_, sketch) = self.live.as_mut().expect("just set");
        let previous = if sketch.seed {
            Some(sketch.last_arrival_ms)
        } else {
            None
        };
        sketch.observe(at_ms);
        previous.map(|previous| at_ms.saturating_sub(previous))
    }

    pub fn config(&self) -> &PhiConfig {
        &self.cfg
    }
}

// ------------------------------------------------------------------ ffi ----

/// The C ABI surface (`phi` feature only): a non-Rust host drives the
/// embedded `phi-accrual-detector` crate through raw handles. Every
/// function is `#[no_mangle] extern "C"`, returns 0 on success / 1 on a
/// null argument, and never panics across the boundary. The crate's async
/// surface (tokio RwLock) is bridged with a shared current-thread
/// runtime; every call blocks, which is exactly the contract a C caller
/// expects.
#[cfg(feature = "phi")]
pub mod ffi {
    use super::*;
    use chrono::{Local, TimeZone};
    use phi_accrual_detector::{Detector, PhiInteraction};
    use std::ffi::c_void;
    use std::sync::Mutex;

    /// The pointee of a [`PhiHandle`]: the crate's Detector plus the
    /// interval mirror that backs the query surface.
    pub struct Repr {
        detector: Detector,
        window: u32,
        intervals_ms: Vec<u64>,
        last_arrival_ms: u64,
        seeded: bool,
    }

    /// Opaque handle: a boxed `Repr`, returned by `phi_detector_new`.
    /// The pointee is private; hosts pass it back opaquely.
    pub type PhiHandle = *mut Mutex<Repr>;

    /// One shared current-thread runtime per process. The Detector's
    /// async methods only await its own uncontended RwLock, so blocking
    /// on them from a sync thread is safe and prompt.
    fn runtime() -> &'static Runtime {
        static RUNTIME: OnceLock<Runtime> = OnceLock::new();
        RUNTIME.get_or_init(|| {
            tokio::runtime::Builder::new_current_thread()
                .build()
                .expect("phi runtime")
        })
    }

    fn local(unix_ms: i64) -> chrono::DateTime<Local> {
        Local
            .timestamp_millis_opt(unix_ms)
            .single()
            .unwrap_or_else(Local::now)
    }

    /// Rust-side constructor for the host loop (the same allocator the
    /// FFI uses, so `phi_detector_free` frees both).
    pub fn new(window: u32) -> DetectorHandle {
        phi_detector_new(window)
    }

    /// # Safety
    ///
    /// `handle` must be a live `phi_detector_new` handle.
    pub unsafe fn observe(handle: DetectorHandle, at_ms: i64) -> i32 {
        unsafe { phi_observe(handle, at_ms) }
    }

    /// # Safety
    ///
    /// `handle` must be a live `phi_detector_new` handle.
    pub unsafe fn value(handle: DetectorHandle, now_ms: i64) -> f64 {
        let mut out = f64::NAN;
        let code = unsafe { phi_value(handle, now_ms, &mut out) };
        if code == 0 { out } else { 0.0 }
    }

    /// The learned mean interval and sample count (the mirror's view).
    /// # Safety
    ///
    /// `handle` must be a live `phi_detector_new` handle; both out
    /// pointers must be writable.
    pub unsafe fn query(
        handle: DetectorHandle,
        out_mean_ms: &mut f64,
        out_samples: &mut u32,
    ) -> i32 {
        unsafe { phi_query(handle, out_mean_ms, out_samples) }
    }

    pub type DetectorHandle = PhiHandle;

    /// A new detector sketch with `window` sample capacity. Null never
    /// returns.
    #[unsafe(no_mangle)]
    pub extern "C" fn phi_detector_new(window: u32) -> PhiHandle {
        let window = window.clamp(1, 10_000);
        Box::into_raw(Box::new(Mutex::new(Repr {
            detector: Detector::new(window),
            window,
            intervals_ms: Vec::new(),
            last_arrival_ms: 0,
            seeded: false,
        })))
    }

    /// One heartbeat arrival at epoch-ms `at_ms`.
    ///
    /// Returns 0 on success, 1 on a null handle or a negative stamp.
    ///
    /// # Safety
    ///
    /// `handle` must be a live `phi_detector_new` handle.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn phi_observe(handle: PhiHandle, at_ms: i64) -> i32 {
        let Some(repr) = (unsafe { handle.as_ref() }) else {
            return 1;
        };
        if at_ms < 0 {
            return 1;
        }
        if let Ok(mut repr) = repr.lock() {
            if repr.seeded {
                let interval = (at_ms as u64).saturating_sub(repr.last_arrival_ms);
                repr.intervals_ms.push(interval);
                let window = repr.window as usize;
                if repr.intervals_ms.len() > window {
                    repr.intervals_ms.remove(0);
                }
            }
            repr.seeded = true;
            repr.last_arrival_ms = at_ms as u64;
            let _ = runtime().block_on(repr.detector.insert(local(at_ms)));
            return 0;
        }
        1
    }

    /// The learned mean arrival interval and the interval-sample count.
    ///
    /// Returns 0 on success, 1 on a null argument.
    ///
    /// # Safety
    ///
    /// `handle` must be a live `phi_detector_new` handle; both out
    /// pointers must be writable.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn phi_query(
        handle: PhiHandle,
        out_mean_ms: *mut f64,
        out_samples: *mut u32,
    ) -> i32 {
        let Some(repr) = (unsafe { handle.as_ref() }) else {
            return 1;
        };
        if out_mean_ms.is_null() || out_samples.is_null() {
            return 1;
        }
        let Ok(repr) = repr.lock() else {
            return 1;
        };
        let mean = if repr.intervals_ms.is_empty() {
            0.0
        } else {
            let total: u64 = repr.intervals_ms.iter().sum();
            total as f64 / repr.intervals_ms.len() as f64
        };
        unsafe {
            *out_mean_ms = mean;
            *out_samples = repr.intervals_ms.len() as u32;
        }
        0
    }

    /// The phi value at `now_ms` into `out_phi`.
    ///
    /// Returns 0 on success, 1 on a null argument or a negative stamp.
    ///
    /// # Safety
    ///
    /// `handle` must be a live `phi_detector_new` handle; `out_phi` must
    /// be writable.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn phi_value(handle: PhiHandle, now_ms: i64, out_phi: *mut f64) -> i32 {
        let Some(repr) = (unsafe { handle.as_ref() }) else {
            return 1;
        };
        if out_phi.is_null() || now_ms < 0 {
            return 1;
        }
        let Ok(repr) = repr.lock() else {
            return 1;
        };
        let value = runtime()
            .block_on(repr.detector.phi(local(now_ms)))
            .unwrap_or(0.0);
        // A NaN phi (the crate's empty-window arithmetic) is no evidence.
        let value = if value.is_nan() { 0.0 } else { value };
        unsafe { *out_phi = value };
        0
    }

    /// Frees a handle. Null is a no-op.
    ///
    /// # Safety
    ///
    /// `handle` must be a live `phi_detector_new` handle or null.
    #[unsafe(no_mangle)]
    pub unsafe extern "C" fn phi_detector_free(handle: PhiHandle) {
        if !handle.is_null() {
            unsafe { drop(Box::from_raw(handle)) };
        }
    }

    // The c_void alias keeps the C ABI's pointer vocabulary honest for
    // non-Rust headers.
    #[allow(dead_code)]
    type Void = c_void;
}
