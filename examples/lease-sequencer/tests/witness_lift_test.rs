//! The witness lift's cursor over the tape replay: retention re-bases
//! the window, the lift must not skip or re-write a single event. The
//! witness re-reads the whole tape each flush tick and writes the
//! transitions past its cursor; the tape's retention drops the oldest
//! files, the replay window re-bases underneath, and a COUNT cursor
//! then names the wrong transitions — real work is skipped into
//! nowhere, or written work rides again. The cursor is the frontier
//! reached — the last written transition's (era, view, slot) — and the
//! lift resumes exactly past it.

use lease_sequencer::bridge::{RawTransition, lift_transitions, replay_transitions};
use lunet_advisory_lock::witness::Frontier;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use tbio::AofFile;
use tbio::envelope::Record;
use tbio::retention::list_aof_files;
use uuid::Uuid;
use vrr::ids::{Ballot, Era, OperationId, Slot, View};
use vrr::journal::{LogEntry, Payload};
use vrr::message::{Body, Message};
use vrr::wire::{Header, Pack, Tag};

const ERA: u32 = 7;
const VIEW: u32 = 3;
const HOLDER: &str = "0f0e0d0c-0b0a-0908-0706-050403020100";
const LOCK_ID: u64 = 0x0DDBA11;

/// The scratch tree, inside the repo (`.tmp` is scratch).
fn scratch(name: &str) -> PathBuf {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../.tmp/witness-lift");
    std::fs::create_dir_all(&root).expect("the scratch root creates");
    let dir = root.join(format!(
        "{}-{}-{}",
        name,
        std::process::id(),
        SEQ.fetch_add(1, Ordering::SeqCst)
    ));
    std::fs::create_dir_all(&dir).expect("the case directory creates");
    dir
}

/// One committed lock work taped as the wire Prepare the leader
/// broadcast: the payload is a Set by one holder (the chase payload's
/// exact shape), the header carries the frontier triple. The ns stamp
/// is slot-scaled so the replay's ns order is the slot order and the
/// lifted ts_ms names the slot.
fn tape_slot_work(aof: &mut AofFile, slot: u64) {
    let message_id: Uuid = format!("3f2a9b1c-5d6e-4f70-8a91-{slot:012x}")
        .parse()
        .expect("the fixture uuid parses");
    let payload = lease_sequencer::chase_set_payload(
        &message_id.to_string(),
        LOCK_ID,
        1,
        slot,
        slot,
        HOLDER,
        500_000,
    );
    let id = OperationId {
        msb: u64::from_be_bytes(message_id.as_bytes()[..8].try_into().unwrap()),
        lsb: u64::from_be_bytes(message_id.as_bytes()[8..].try_into().unwrap()),
    };
    let message = Message {
        header: Header {
            tag: Tag::Prepare,
            view: Ballot {
                era: Era(ERA),
                view: View(VIEW),
            },
            slot: Slot(slot),
        },
        body: Body::Prepare {
            entry: LogEntry {
                slot: Slot(slot),
                era: Era(ERA),
                payload: Payload::Operation {
                    id,
                    payload: payload.as_bytes().into(),
                },
            },
            committed: Slot(slot),
        },
    };
    let mut buf = vec![0u8; message.packed_len()];
    let written = message.pack_into(&mut buf).expect("the prepare packs");
    let ns = slot * 1_000_000;
    aof.append(&Record::wire(ns, &buf[..written]).encode())
        .expect("the wire record appends");
}

/// The dropped-oldest sweep's effect: the tape's oldest file removed
/// whole (the plan keeps the newest full plus the active).
fn drop_oldest_tape_file(dir: &Path) {
    let mut listed = list_aof_files(dir).expect("the tape lists");
    let (oldest, _, _) = listed.remove(0);
    std::fs::remove_file(&oldest).expect("the oldest tape file drops");
}

/// The lifted slots (the replay's ts_ms is the fixture's slot stamp).
fn lifted_slots(lifted: &[RawTransition]) -> Vec<u64> {
    lifted.iter().map(|raw| raw.ts_ms).collect()
}

/// The lifted frontiers: every one in the fixture's own (era, view,
/// slot) shape.
fn lifted_frontiers(lifted: &[RawTransition]) -> Vec<Frontier> {
    lifted
        .iter()
        .map(|raw| raw.frontier)
        .inspect(|frontier| {
            assert_eq!(frontier.era, ERA, "the frontier carries the era");
            assert_eq!(frontier.view, VIEW, "the frontier carries the view");
        })
        .collect()
}

#[test]
fn the_lift_resumes_from_the_frontier_after_retention_rebases_the_tape() {
    let case = scratch("frontier-cursor");
    let dir = case.join("aof");

    // The tape: the first file carries slots 1-2, the second carries
    // slot 3.
    {
        let mut aof = AofFile::open(&dir).expect("the first tape file opens");
        tape_slot_work(&mut aof, 1);
        tape_slot_work(&mut aof, 2);
    }
    {
        let mut aof = AofFile::open(&dir).expect("the second tape file opens");
        tape_slot_work(&mut aof, 3);
    }

    // Tick 1: the lift over the whole tape — three transitions, all
    // written, the cursor reaching the last.
    let tick1 = replay_transitions(&dir);
    assert_eq!(
        lifted_slots(&tick1),
        vec![1, 2, 3],
        "the whole tape lifts on the first tick"
    );
    let reached1 = tick1.last().map(|raw| raw.frontier);
    assert_eq!(
        lifted_frontiers(&tick1),
        vec![
            Frontier {
                era: ERA,
                view: VIEW,
                slot: 1
            },
            Frontier {
                era: ERA,
                view: VIEW,
                slot: 2
            },
            Frontier {
                era: ERA,
                view: VIEW,
                slot: 3
            },
        ],
        "every transition carries its Prepare's frontier"
    );

    // The retention: the oldest tape file drops.
    drop_oldest_tape_file(&dir);

    // The stream advances: slot 4 rides a new active file.
    {
        let mut aof = AofFile::open(&dir).expect("the third tape file opens");
        tape_slot_work(&mut aof, 4);
    }

    // Tick 2: the re-based window. The slot-3 work is already in the
    // series; the lift must write ONLY the slot-4 event — never skip
    // it, never re-write the slot-3 event.
    let tick2 = replay_transitions(&dir);
    assert_eq!(
        lifted_slots(&tick2),
        vec![3, 4],
        "the re-based window carries the surviving work plus the new"
    );

    // The lift: the cursor the first tick reached, the fresh tail it
    // writes now.
    let lifted = lift_transitions(&tick2, reached1);
    assert_eq!(
        lifted_slots(&lifted),
        vec![4],
        "the lift resumes past the frontier: the slot-4 event is in the series"
    );

    // The cursor's idempotence: re-lifting the same window at the
    // frontier now reached writes nothing — the series never rides
    // again.
    let reached2 = lifted.last().map(|raw| raw.frontier);
    assert_eq!(
        lift_transitions(&tick2, reached2).len(),
        0,
        "the frontier cursor re-lifts nothing"
    );
}
