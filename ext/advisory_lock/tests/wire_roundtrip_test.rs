//! `header_slot_role` admits. The widths the sums below state: header 20
//! (tag 4 + era 4 + view 4 + slot 8, W1), body discriminant 1, NodeId 4,
//! Era 4, View 4, Slot 8, OperationId 16, opaque length prefix 4.

use vrr::configuration::SystemOperation;
use vrr::ids::{Ballot, Era, NodeId, OperationId, Slot, View};
use vrr::invariant::header_slot_role;
use vrr::journal::{LogEntry, Payload};
use vrr::message::{Body, Message};
use vrr::wire::{Malformed, Pack, Tag, Unpack, UnpackError};

const HEADER: usize = 20;
const DISC: usize = 1;
const NODE: usize = 4;
const SLOT: usize = 8;

/// One System-operation entry at slot 8, era 3: 8 slot + 4 era + 1
/// payload discriminant + the operation's own width.
fn system_entry(op: &SystemOperation) -> LogEntry {
    LogEntry {
        slot: Slot(8),
        era: Era(3),
        payload: Payload::System(op.clone()),
    }
}

fn ballot() -> Ballot {
    Ballot {
        era: Era(3),
        view: View(7),
    }
}

fn operation_entry(payload_len: usize) -> LogEntry {
    LogEntry {
        slot: Slot(7),
        era: Era(3),
        payload: Payload::Operation {
            id: OperationId {
                msb: 0x7665_6374_0000_0001,
                lsb: 0x0000_0000_0000_0002,
            },
            payload: vec![0xAA; payload_len].into_boxed_slice(),
        },
    }
}

fn frame(message: &Message) -> Vec<u8> {
    let mut buf = vec![0u8; message.packed_len()];
    let written = message.pack_into(&mut buf).expect("the frame packs");
    assert_eq!(written, buf.len(), "pack_into writes exactly packed_len");
    buf
}

/// The boundary roundtrip: pack, unpack, every field survives, and the
/// frame's header slot is one its tag's table admits.
fn roundtrips(message: Message) -> Vec<u8> {
    let bytes = frame(&message);
    let decoded = Message::unpack_from(&bytes).expect("the frame unpacks");
    assert_eq!(decoded, message, "every field survives the roundtrip");
    assert!(
        header_slot_role(message.header.tag).admits(message.header.slot),
        "the header slot is one the tag's table admits"
    );
    bytes
}

#[test]
fn every_live_tag_roundtrips_its_number_and_names_itself() {
    const LIVE: [(Tag, u32, &str); 14] = [
        (Tag::Prepare, 2, "prepare"),
        (Tag::PrepareOk, 3, "prepare_ok"),
        (Tag::Commit, 4, "commit"),
        (Tag::StartViewChange, 5, "start_view_change"),
        (Tag::DoViewChange, 6, "do_view_change"),
        (Tag::StartView, 7, "start_view"),
        (Tag::PlannedViewChange, 8, "planned_view_change"),
        (Tag::GetState, 9, "get_state"),
        (Tag::NewState, 10, "new_state"),
        (Tag::Reincarnation, 13, "reincarnation"),
        (Tag::Fuse, 14, "fuse"),
        (Tag::FuseOk, 15, "fuse_ok"),
        (Tag::CommitBatch, 16, "commit_batch"),
        (Tag::GossipRequest, 17, "gossip_request"),
    ];
    for (tag, number, name) in LIVE {
        assert_eq!(tag.as_u32(), number, "the numbering is explicit");
        assert_eq!(Tag::from_u32(number), Some(tag), "the table is total");
        assert_eq!(tag.name(), name, "the name names the number");
        assert!(!tag.name().is_empty());
    }
    for reserved in [0u32, 1, 11, 12, 18, u32::MAX] {
        assert_eq!(Tag::from_u32(reserved), None, "reserved stays reserved");
    }
}

#[test]
fn prepare_roundtrips_at_its_exact_length() {
    let entry = operation_entry(5);
    let message = Message {
        header: vrr::wire::Header {
            tag: Tag::Prepare,
            view: ballot(),
            slot: Slot(7),
        },
        body: Body::Prepare {
            entry,
            committed: Slot(6),
        },
    };
    let bytes = roundtrips(message);
    assert_eq!(
        bytes.len(),
        // header 20 + disc 1 + entry (8 slot + 4 era + 1 disc + 16 id +
        // 4 prefix + 5 payload) + 8 committed frontier.
        HEADER + DISC + (SLOT + NODE + 1 + 16 + 4 + 5) + SLOT,
        "the exact normative length (W3)"
    );
}

#[test]
fn the_empty_bodies_roundtrip_at_21_bytes_and_the_frontiers_at_29() {
    for (tag, body) in [
        (Tag::PrepareOk, Body::PrepareOk {}),
        (Tag::StartViewChange, Body::StartViewChange {}),
        (Tag::PlannedViewChange, Body::PlannedViewChange {}),
    ] {
        let slot = if header_slot_role(tag).admits(Slot::NONE) {
            Slot::NONE
        } else {
            Slot(7)
        };
        let message = Message {
            header: vrr::wire::Header {
                tag,
                view: ballot(),
                slot,
            },
            body,
        };
        let bytes = roundtrips(message);
        assert_eq!(bytes.len(), HEADER + DISC, "an empty body is one byte");
    }
    let commit = Message {
        header: vrr::wire::Header {
            tag: Tag::Commit,
            view: ballot(),
            slot: Slot(11),
        },
        body: Body::Commit {
            committed: Slot(11),
        },
    };
    assert_eq!(roundtrips(commit).len(), HEADER + DISC + SLOT);
    let get_state = Message {
        header: vrr::wire::Header {
            tag: Tag::GetState,
            view: ballot(),
            slot: Slot(9),
        },
        body: Body::GetState { from: Slot(9) },
    };
    assert_eq!(roundtrips(get_state).len(), HEADER + DISC + SLOT);
}

#[test]
fn do_view_change_roundtrips_at_its_exact_length() {
    let suffix = vec![system_entry(&SystemOperation::Double)];
    let message = Message {
        header: vrr::wire::Header {
            tag: Tag::DoViewChange,
            view: ballot(),
            slot: Slot(9),
        },
        body: Body::DoViewChange {
            retained: Ballot {
                era: Era(2),
                view: View(6),
            },
            accepted: Slot(9),
            committed: Slot(6),
            suffix,
            evidence: vrr::message::EvidenceKind::Planned,
            era_proof: vrr::message::EraProof {
                op: SystemOperation::Join {
                    node: NodeId(0x0002_0001),
                    position: 4,
                },
                committed_at: Slot(2),
            },
        },
    };
    let bytes = roundtrips(message);
    assert_eq!(
        bytes.len(),
        // header 20 + disc 1 + retained 8 + accepted 8 + committed 8 +
        // suffix (4 count + 14 entry: 8 + 4 + 1 + 1 Double) + evidence 1
        // + era proof (9 Join + 8 slot).
        HEADER + DISC + SLOT + SLOT + SLOT + (4 + 14) + 1 + (9 + SLOT),
        "the exact normative length (W3)"
    );
}

#[test]
fn start_view_roundtrips_at_its_exact_length() {
    let suffix = vec![system_entry(&SystemOperation::Void)];
    let message = Message {
        header: vrr::wire::Header {
            tag: Tag::StartView,
            view: ballot(),
            slot: Slot(9),
        },
        body: Body::StartView {
            suffix,
            accepted: Slot(9),
            committed: Slot(6),
            era_proof: vrr::message::EraProof {
                op: SystemOperation::Join {
                    node: NodeId(0x0002_0001),
                    position: 4,
                },
                committed_at: Slot(2),
            },
        },
    };
    let bytes = roundtrips(message);
    assert_eq!(
        bytes.len(),
        // header 20 + disc 1 + suffix (4 count + 14 entry: 8 + 4 + 1 + 1
        // Void) + accepted 8 + committed 8 + era proof (9 Join + 8 slot).
        HEADER + DISC + (4 + 14) + SLOT + SLOT + (9 + SLOT),
        "the exact normative length (W3)"
    );
}

#[test]
fn new_state_roundtrips_at_its_exact_length() {
    let message = Message {
        header: vrr::wire::Header {
            tag: Tag::NewState,
            view: ballot(),
            slot: Slot(8),
        },
        body: Body::NewState {
            entries: vec![system_entry(&SystemOperation::Void)],
            through: Slot(8),
            committed: Slot(6),
            more: true,
        },
    };
    let bytes = roundtrips(message);
    assert_eq!(
        bytes.len(),
        // header 20 + disc 1 + entries (4 count + 14 entry) + through 8 +
        // committed 8 + more 1.
        HEADER + DISC + (4 + 14) + SLOT + SLOT + 1,
        "the exact normative length (W3)"
    );
}

#[test]
fn the_reincarnation_frame_is_45_bytes_and_roundtrips() {
    let message = Message {
        header: vrr::wire::Header {
            tag: Tag::Reincarnation,
            view: ballot(),
            slot: Slot::NONE,
        },
        body: Body::Reincarnation {
            old: NodeId(0x0001_0001),
            new: NodeId(0x0001_0002),
            committed: Slot::NONE,
            prepared: Slot::NONE,
        },
    };
    let bytes = roundtrips(message);
    assert_eq!(
        bytes.len(),
        HEADER + DISC + NODE + NODE + SLOT + SLOT,
        "the 45-byte reincarnation frame"
    );
    assert_eq!(bytes.len(), 45);
    let decoded = Message::unpack_from(&bytes).expect("the frame unpacks");
    let Body::Reincarnation {
        old,
        new,
        committed,
        prepared,
    } = decoded.body
    else {
        panic!("the body survives as a Reincarnation");
    };
    assert_eq!(old, NodeId(0x0001_0001));
    assert_eq!(new, NodeId(0x0001_0002));
    assert_eq!(committed, Slot::NONE);
    assert_eq!(prepared, Slot::NONE);
}

#[test]
fn the_fuse_family_roundtrips_and_a_zero_count_refuses() {
    let fuse = Message {
        header: vrr::wire::Header {
            tag: Tag::Fuse,
            view: ballot(),
            slot: Slot(5),
        },
        body: Body::Fuse {
            ops: vec![SystemOperation::Void, SystemOperation::Double],
        },
    };
    assert_eq!(roundtrips(fuse).len(), HEADER + DISC + 4 + 1 + 1);

    let fuse_ok = Message {
        header: vrr::wire::Header {
            tag: Tag::FuseOk,
            view: ballot(),
            slot: Slot(6),
        },
        body: Body::FuseOk {
            acks: vec![Slot(5), Slot(6)],
        },
    };
    assert_eq!(roundtrips(fuse_ok).len(), HEADER + DISC + 4 + SLOT + SLOT);

    let commit_batch = Message {
        header: vrr::wire::Header {
            tag: Tag::CommitBatch,
            view: ballot(),
            slot: Slot(6),
        },
        body: Body::CommitBatch {
            committed: vec![Slot(5), Slot(6)],
        },
    };
    assert_eq!(
        roundtrips(commit_batch).len(),
        HEADER + DISC + 4 + SLOT + SLOT
    );

    // A fuse envelope names its batch, and naming none is not a batch:
    // the decode refuses rather than yielding an empty one.
    let mut zero_count = vec![0u8; HEADER + DISC + 4];
    let header = vrr::wire::Header {
        tag: Tag::Fuse,
        view: ballot(),
        slot: Slot(5),
    };
    header.pack_into(&mut zero_count).expect("the header packs");
    zero_count[HEADER] = Tag::Fuse.as_u32() as u8;
    assert_eq!(
        Message::unpack_from(&zero_count),
        Err(UnpackError::Malformed(Malformed::OutOfDomain)),
        "a zero-count envelope is malformed"
    );
}

#[test]
fn the_gossip_request_frame_is_37_bytes_and_roundtrips() {
    let message = Message {
        header: vrr::wire::Header {
            tag: Tag::GossipRequest,
            view: ballot(),
            slot: Slot::NONE,
        },
        body: Body::GossipRequest {
            prepared: Slot(4),
            committed: Slot(2),
        },
    };
    let bytes = roundtrips(message);
    assert_eq!(
        bytes.len(),
        HEADER + DISC + SLOT + SLOT,
        "the gossip request frame"
    );
    assert_eq!(bytes.len(), 37);
}

#[test]
fn a_header_body_disagreement_and_an_unknown_tag_are_malformed() {
    let mut bytes = roundtrips(Message {
        header: vrr::wire::Header {
            tag: Tag::Prepare,
            view: ballot(),
            slot: Slot(7),
        },
        body: Body::Prepare {
            entry: operation_entry(5),
            committed: Slot(6),
        },
    });
    // The body discriminant lies about the header's tag: the wire carries
    // the kind twice and a disagreement is malformed, never guessed at.
    bytes[HEADER] = Tag::StartViewChange.as_u32() as u8;
    assert!(matches!(
        Message::unpack_from(&bytes),
        Err(UnpackError::Malformed(_))
    ));
    // A tag outside the table is refused with the offending value, and
    // the reserved zero is refused first of all.
    for raw in [0u32, 11, 12, 18] {
        let mut unknown = bytes.clone();
        unknown[0..4].copy_from_slice(&raw.to_be_bytes());
        assert_eq!(
            Message::unpack_from(&unknown),
            Err(UnpackError::Malformed(Malformed::UnknownTag(raw))),
            "the unknown tag names itself"
        );
    }
}
