//! The in-test telemetry recorder: the record-once-then-replay half of
//! the corpus tests. The corpora those tests replayed were gitignored run
//! pulls — dead for any fresh clone — so the tests now record their own
//! corpus into the repository's `.tmp/` scratch and replay it in the
//! same run. The wire frames pack with the core's own wire encoder and
//! the AOF appends through the vendored TB record layer, so a recorded
//! corpus is the same artifact class the rig's telemetry AOFs are.

use lease_sequencer::phi::Trailer;
use lunet_locks_aof::envelope::Record;
use lunet_locks_aof::ffi::RawFile;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;
use vrr::wire::Pack;

/// Distinct scratch roots per call: the wall clock alone does not
/// separate parallel test threads, so every root carries a process-global
/// sequence.
static SCRATCH_SEQ: AtomicU64 = AtomicU64::new(0);

/// A fresh scratch root under the repository's `.tmp/` directory (the
/// write boundary): this crate sits two levels below the repository root,
/// and the crate directory is baked in at compile time.
pub fn temp_root(label: &str) -> PathBuf {
    let repo_tmp = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join(".tmp");
    let seq = SCRATCH_SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = repo_tmp.join(format!(
        "{label}-{}-{}-{seq}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("temp root");
    dir
}

/// The operation identity for `message_id` (the core derives it the same
/// way from the peer-carried payload).
pub fn operation_id(message_id: Uuid) -> vrr::ids::OperationId {
    let id_bytes = message_id.as_bytes();
    vrr::ids::OperationId {
        msb: u64::from_be_bytes(id_bytes[..8].try_into().expect("8 bytes")),
        lsb: u64::from_be_bytes(id_bytes[8..].try_into().expect("8 bytes")),
    }
}

/// One Prepare datagram proposing `payload` at `slot` (era `era`, view
/// `view`): packed with the core's wire encoder, no trailer — the wire
/// header names no sender, exactly the recorded corpora's Prepare shape.
pub fn prepare_frame(
    era: u32,
    view: u32,
    slot: u64,
    message_id: Uuid,
    payload: &[u8],
    committed: u64,
) -> Vec<u8> {
    let message = vrr::message::Message {
        header: vrr::wire::Header {
            tag: vrr::wire::Tag::Prepare,
            view: vrr::ids::ViewId {
                era: vrr::ids::Era(era),
                view: vrr::ids::View(view),
            },
            slot: vrr::ids::Slot(slot),
        },
        body: vrr::message::Body::Prepare {
            entry: vrr::journal::LogEntry {
                slot: vrr::ids::Slot(slot),
                era: vrr::ids::Era(era),
                payload: vrr::journal::Payload::Operation {
                    id: operation_id(message_id),
                    payload: payload.to_vec().into(),
                },
            },
            committed: vrr::ids::Slot(committed),
        },
    };
    let mut bytes = vec![0u8; message.packed_len()];
    message.pack_into(&mut bytes).expect("the frame packs");
    bytes
}

/// One Commit datagram advancing the frontier to `committed`, with the
/// leader heartbeat's phi trailer appended (the tape derives the sender
/// from the trailer's leader).
pub fn commit_frame(era: u32, view: u32, slot: u64, committed: u64, trailer: &Trailer) -> Vec<u8> {
    let message = vrr::message::Message {
        header: vrr::wire::Header {
            tag: vrr::wire::Tag::Commit,
            view: vrr::ids::ViewId {
                era: vrr::ids::Era(era),
                view: vrr::ids::View(view),
            },
            slot: vrr::ids::Slot(slot),
        },
        body: vrr::message::Body::Commit {
            committed: vrr::ids::Slot(committed),
        },
    };
    let mut bytes = vec![0u8; message.packed_len()];
    message.pack_into(&mut bytes).expect("the frame packs");
    trailer.append_to(&mut bytes);
    bytes
}

/// Writes one `.aof` series file into `dir`: the envelope-encoded records
/// appended through the same TB record layer the hosts append through.
pub fn write_aof(dir: &Path, epoch: u64, records: &[Record]) {
    std::fs::create_dir_all(dir).expect("aof dir");
    let path = dir.join(format!("{epoch}.aof"));
    let mut file = unsafe { RawFile::open(path.to_str().expect("utf-8 path").as_bytes(), false) }
        .expect("aof open");
    for record in records {
        file.append(&record.encode()).expect("aof append");
    }
    file.flush().expect("aof flush");
    file.close().expect("aof close");
}
