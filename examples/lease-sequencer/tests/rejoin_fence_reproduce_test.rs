//! The fenced crash-restart reproduction: the two rejoin families the
//! local-softball9-2026-09-25 run recorded (its `progress.log` declares both
//! BLOCKED), force-fed to the fenced node in-process — no spawn, no fork,
//! no sockets; the storage is the one-line incarnation marker the boot gate
//! actually read, and the network is the committed capture
//! `docs/src/fenced-crash-restart/messages.jsonl` (every frame traceable to
//! the run's flight recorders by file/line/seq/ts).
//!
//! The recorded wire truth, both cases: the crashed voter reopens under the
//! bumped identity (`old 3 -> new 16777219`, incarnation 1), announces
//! `Reincarnation(3, 16777219)` at its boot view (era 1, view 0) once per
//! second per the host's fenced-boot drive (`main.rs::timers` ->
//! `Node::recover`, upstream §8's re-announce), and never seats.
//!
//! - Shallow case (the view-churn fence): the cluster churned views
//!   235..1238 at era 5 through the whole window and never addressed one
//!   datagram to the bumped identity; the only inbound class is the churn's
//!   `StartViewChange` fence vote addressed to the past-life id 3 (the
//!   transport's descriptor row; the peers' bumped rows died with the
//!   crashed process and were never re-learned — the peers announce only
//!   while fenced). The restarted node's era-1 table cannot evaluate era 5:
//!   every vote drops `UnevaluableEra` (`ext/uvrr-core/src/replica/
//!   view_change.rs::plan_start_view_change` — the era record lookup
//!   precedes the view comparison). Its journal for the window: 1979
//!   `UnevaluableEra`, 152 `ReincarnationRefused` (its own announcement
//!   looped back through the self row), and the leaders' journals refuse
//!   every announcement `ReincarnationRefused` — the armed-leader
//!   precondition (`Status::Normal` and primary of the current view,
//!   `ext/uvrr-core/src/replica/reincarnation.rs::plan_reincarnation`)
//!   never held in the churn. End state at the fence: `state=restarting
//!   era=1 view=0 config_era=1 voting=0`.
//! - Deep case (the install that never lands): the leader armed and the
//!   forced sequence committed the node's promotion (its journal reaches
//!   `config_era=5 voting=1`), but the restarted node's transport never
//!   re-learned the leader's bump, so the era-5 `StartView` and the memo
//!   stream arrive under the descriptor id 1, which the folded era-5
//!   configuration does not name: the journal drops them
//!   `StartViewNotFromPrimary` and `UnknownSender`. The fold's own wire
//!   path rode segments the flight recorder's keep-2 rotation pruned, so
//!   this test force-feeds the fence phase's surviving frames against the
//!   same minimum boot state; at the era-1 table the era gate fires first
//!   and the node never seats either way.
//!
//! The contract under test (the host's own "voting and serving" signal,
//! `main.rs::timers`): an announced crashed restart must be walked back to
//! voting weight by the leader's forced sequence. Both tests are RED: the
//! fence holds.

use lunet_advisory_lock::Node;

/// The host loop's tick (`main.rs::TICK_MS`).
const TICK_MS: u64 = 5;
/// The fenced-boot drive's cadence (`main.rs::recovery_ms`'s default): the
/// recorded announcements land one per second per peer.
const RECOVERY_MS: u64 = 1000;

/// `Status::Restarting`'s snapshot word (`vrr::progress::Status::to_word`).
const STATE_RESTARTING: u32 = 2;
/// `Status::Normal`'s snapshot word.
const STATE_NORMAL: u32 = 0;

const ROOT: &str = "/Users/Shared/lua-lunet/lunet-locks/.tmp/rejoin-fence-reproduce-test";

/// The committed capture this test replays.
const CAPTURE: &str = concat!(
    env!("CARGO_MANIFEST_DIR"),
    "/../../docs/src/fenced-crash-restart/messages.jsonl"
);

/// One captured inbound frame: the recorded bytes, the offset from the
/// fenced node's boot, and the sender identity the fenced node's transport
/// attributed (the capture's `received_as_from`: the descriptor row, never
/// the bump — the remap rows die with the crashed process).
struct CapturedFrame {
    from: u32,
    at_ms: u64,
    bytes: Vec<u8>,
}

fn unhex(hex: &str) -> Vec<u8> {
    hex.as_bytes()
        .chunks(2)
        .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).expect("ascii hex"), 16))
        .collect::<Result<Vec<u8>, _>>()
        .expect("the committed capture's hex parses")
}

/// Reads the capture's `in` records for one case, oldest first, with the
/// offsets rebased on the case's boot ts (the summary record's window).
fn load_capture(case: &str) -> (u64, Vec<CapturedFrame>) {
    let text = std::fs::read_to_string(CAPTURE).expect("the committed capture reads");
    let mut boot_ts = None;
    let mut frames = Vec::new();
    for line in text.lines() {
        let value: serde_json::Value = serde_json::from_str(line).expect("jsonl record");
        if value.get("case").and_then(|c| c.as_str()) != Some(case) {
            continue;
        }
        if value.get("kind").and_then(|k| k.as_str()) == Some("summary") {
            boot_ts = Some(
                value["window_ts_ms"][0]
                    .as_u64()
                    .expect("the summary carries the window"),
            );
            continue;
        }
        if value.get("direction").and_then(|d| d.as_str()) != Some("in") {
            continue;
        }
        let from = value
            .get("received_as_from")
            .or_else(|| value.get("from"))
            .and_then(|f| f.as_u64())
            .expect("the record names its sender") as u32;
        frames.push(CapturedFrame {
            from,
            at_ms: value["source"]["ts_ms"].as_u64().expect("ts"),
            bytes: unhex(value["hex"].as_str().expect("hex")),
        });
    }
    let boot_ts = boot_ts.expect("the capture carries the case's summary");
    frames.sort_by_key(|frame| frame.at_ms);
    for frame in &mut frames {
        frame.at_ms -= boot_ts;
    }
    (boot_ts, frames)
}

/// The crashed boot: the marker file the gate read is the running sentinel
/// (`0 unflushed` — a SIGKILLed voter's durable state); no stopped quorum
/// classifies the boot crashed, the identity bumps to incarnation 1
/// (`3 + 2^24`), and the replica reopens clean over the genesis descriptor.
fn open_crashed_n3(dir: &std::path::Path) -> Node {
    std::fs::create_dir_all(dir).expect("scratch dir");
    let state = dir.join("n3.state");
    std::fs::write(&state, "0 unflushed\n").expect("the captured boot state");
    let members = ["1:n1", "2:n2", "3:n3"].join("\0");
    Node::open(&members, "n3", state.to_str().expect("utf8 path"), None, 0)
        .expect("the boot gate classifies the crashed marker and the node opens")
}

/// The fenced node's host loop (`main.rs::timers`, reduced to what a
/// fenced restarting node ever drives): the heartbeat tick, the
/// fenced-boot `recover()` on its resend cadence, the captured inbound
/// frames at their recorded offsets, and the self-loop the transport's
/// remap row produces (a send to the past-life id 3 lands back on the
/// node's own socket — its journal's `ReincarnationRefused` drops).
struct Host {
    node: Node,
    /// The node's outbound `Reincarnation` frames so far.
    announcements: u64,
    /// Frames delivered into the node.
    received: u64,
}

impl Host {
    fn drain(&mut self) {
        while let Some(out) = self.node.next_output() {
            if out.kind != 1 {
                continue;
            }
            if u32::from_be_bytes(out.bytes[0..4].try_into().expect("tag")) == 13 {
                self.announcements += 1;
            }
            if out.to == 3 {
                let _ = self.node.receive(self.node.own_id(), &out.bytes);
            }
        }
    }
}

/// Drives the fenced node through the recorded window and one
/// announcement-cadence margin, then answers whether it seated.
fn drive(case: &str, dir: &std::path::Path) -> (Host, Vec<CapturedFrame>) {
    let (_boot_ts, frames) = load_capture(case);
    let window_ms = frames.last().map(|frame| frame.at_ms).unwrap_or(0);
    let deadline = window_ms + 2 * RECOVERY_MS;
    let mut host = Host {
        node: open_crashed_n3(dir),
        announcements: 0,
        received: 0,
    };
    // The boot's wire phase: the announcement is emitted at boot, ahead of
    // the first drive (the capture's first received announcements land 2-7
    // ms after the boot ts).
    host.drain();
    let mut next_frame = 0;
    let mut now = 0;
    while now <= deadline {
        while next_frame < frames.len() && frames[next_frame].at_ms <= now {
            let frame = &frames[next_frame];
            let _ = host.node.receive(frame.from, &frame.bytes);
            host.received += 1;
            next_frame += 1;
        }
        if now % RECOVERY_MS == 0 {
            let _ = host.node.recover();
        }
        let _ = host.node.idle();
        host.drain();
        now += TICK_MS;
    }
    (host, frames)
}

/// The shallow family (the view-churn fence): the restarted node's only
/// inbound for 151 s is the era-5 churn's fence votes addressed to its
/// past-life id; every one drops `UnevaluableEra` at the era-1 table and
/// no answer to the announcement ever arrives. The contract says the
/// leader's forced sequence walks the announced node back to voting
/// weight; the recorded exchange never seats it.
#[test]
fn reincarnating_into_the_view_churn_must_seat() {
    let dir = std::path::Path::new(ROOT).join("shallow");
    let _ = std::fs::remove_dir_all(&dir);
    let (host, frames) = drive("shallow", &dir);
    let status = host.node.status();
    assert_eq!(host.node.own_id(), 16777219, "the crashed boot bumps");
    assert_eq!(
        status.state, STATE_RESTARTING,
        "the node never left the fence"
    );
    assert!(host.announcements > 0, "the entry ticket fired");
    assert_eq!(host.received, frames.len() as u64, "the capture fed");
    let weight = host.node.voting_weight();
    assert!(
        weight.unwrap_or(0) > 0,
        "the recorded exchange never seated the restarted node: \
         state={} era={} view={} config_era={} voting_weight={weight:?} after \
         {} announcements and {} captured inbound frames (the run's window: \
         304 Reincarnation(3, 16777219) out, 1979 era-5 fence votes in, every \
         one dropped UnevaluableEra, no frame ever addressed to the bumped \
         identity — docs/src/fenced-crash-restart/messages.jsonl)",
        status.state_name(),
        status.era,
        status.view,
        status.config_era,
        host.announcements,
        host.received,
    );
}

/// The deep family (the install that never lands): the forced sequence
/// committed the node's promotion, then the era-5 view-126 install and the
/// memo stream arrived under the descriptor id 1 the restarted node's
/// transport still named the leader by. The capture's surviving frames for
/// the fence phase (the era-4 fence vote, the era-5 fence vote, the
/// `StartView`, the memo stream's first prepare and commit) are force-fed
/// here; at the minimum boot state the era gate fires first, exactly as it
/// did for every unevaluable frame the live node dropped. The contract is
/// the same: the announced restart must seat.
#[test]
fn reincarnating_under_the_stale_sender_attribution_must_seat() {
    let dir = std::path::Path::new(ROOT).join("deep");
    let _ = std::fs::remove_dir_all(&dir);
    let (host, frames) = drive("deep", &dir);
    let status = host.node.status();
    assert_eq!(host.node.own_id(), 16777219, "the crashed boot bumps");
    assert!(host.announcements > 0, "the entry ticket fired");
    assert_eq!(host.received, frames.len() as u64, "the capture fed");
    let weight = host.node.voting_weight();
    assert!(
        weight.unwrap_or(0) > 0 && status.state == STATE_NORMAL,
        "the recorded stream never seated the restarted node: state={} \
         era={} view={} config_era={} voting_weight={weight:?} after {} \
         announcements and {} captured inbound frames (the live node folded \
         the era-5 configuration and held voting weight 1, yet the view-126 \
         install kept dropping StartViewNotFromPrimary under the stale \
         sender attribution — docs/src/fenced-crash-restart/messages.jsonl)",
        status.state_name(),
        status.era,
        status.view,
        status.config_era,
        host.announcements,
        host.received,
    );
}
