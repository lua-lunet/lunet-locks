//! The rejoin gossip's joiner half (uvrr-core
//! `docs/uvrr-rejoin-gossip-and-witnesses.md` §2): the host obligation the
//! strand exposed. Rejoining is a gossip protocol OUTSIDE the main uVRR
//! protocol — a node outside the cluster never assumes the cluster will
//! come to it: it gossips "I want to join" to every node it knows about,
//! on its own resend timer, and the cluster streams it until it is
//! promoted.
//!
//! Why the send is the host's: the core emits no `GossipRequest` — a
//! `Joining` node's tick drives only an already-open fetch
//! (`ext/uvrr-core/src/replica/mod.rs` `plan_tick`), and the fetch opens
//! only through paths a fenced fresh boot never reaches. The core HANDLES
//! the message on receive (`plan_gossip_request`: every node that hears
//! it records the sender as a gossip-witness; the leader answers with the
//! missed-range push above the sender's frontier plus a fresh commit).
//!
//! Why the datagram carries the node's CURRENT view: the leader's answer
//! echoes the request's view, and the echo is what qualifies the push at
//! the boot fence (`plan_new_state`: a boot-fenced node that opened no
//! fetch accepts the chunk only at its own current view) — the entry
//! ticket names the view the node already holds, never one it has merely
//! heard of. The frontiers ride the body; a fresh boot names none
//! (`Slot::NONE`), so the leader's push re-sends the whole history and
//! the install path dedups what the node already holds.

use vrr::ids::{Era, Slot, View, ViewId};
use vrr::message::{Body, Message};
use vrr::wire::{Header, Pack, Tag};

/// The join gossip's resend cadence: the joiner's own timer, the
/// reliability mechanism of the gossip itself (a request lost in flight
/// is covered by a resend, and a node that missed the original gossip
/// learns of the joiner from one).
pub const GOSSIP_RESEND_MS: u64 = 500;

/// The join-gossip datagram: the core's own `Body::GossipRequest` wire
/// frame, packed with the core's own `Pack` at the node's current
/// `(era, view)`. The header slot is the `Absent` sentinel
/// (`vrr::invariant::header_slot_role`); the body carries the frontiers
/// the host can name — a fresh boot names none.
pub fn gossip_datagram(era: u32, view: u32) -> Vec<u8> {
    let message = Message {
        header: Header {
            tag: Tag::GossipRequest,
            view: ViewId {
                era: Era(era),
                view: View(view),
            },
            slot: Slot::NONE,
        },
        body: Body::GossipRequest {
            prepared: Slot::NONE,
            committed: Slot::NONE,
        },
    };
    let mut frame = vec![0u8; message.packed_len()];
    let written = message
        .pack_into(&mut frame)
        .expect("the core packs its own body");
    debug_assert_eq!(written, frame.len());
    frame
}

#[cfg(test)]
mod tests {
    use super::*;
    use vrr::wire::Unpack;

    /// The datagram this host sends IS the frame the core's own `pack`
    /// emits and its own `unpack` reads back: the join ticket is derived
    /// from the wire, not hand-copied bytes. A body-layout change
    /// upstream fails here before it can silently disarm the rejoin.
    #[test]
    fn gossip_datagram_round_trips_through_the_cores_own_codec() {
        let frame = gossip_datagram(4, 13);
        assert_eq!(frame.len(), 20 + 1 + 8 + 8, "header + discriminant + two slots");
        assert_eq!(
            u32::from_be_bytes(frame[0..4].try_into().expect("4 bytes")),
            17,
            "the header tag is GossipRequest"
        );
        assert_eq!(
            u32::from_be_bytes(frame[4..8].try_into().expect("4 bytes")),
            4,
            "the header carries the node's era"
        );
        assert_eq!(
            u32::from_be_bytes(frame[8..12].try_into().expect("4 bytes")),
            13,
            "the header carries the node's view"
        );
        assert_eq!(frame[20], 17, "the body discriminant mirrors the tag");
        assert_eq!(
            u64::from_be_bytes(frame[21..29].try_into().expect("8 bytes")),
            0,
            "prepared rides at offsets 21..29 unnamed"
        );
        assert_eq!(
            u64::from_be_bytes(frame[29..37].try_into().expect("8 bytes")),
            0,
            "committed rides at offsets 29..37 unnamed"
        );
        let message = Message::unpack_from(&frame).expect("the core reads its own frame");
        assert_eq!(message.header.tag, Tag::GossipRequest);
        assert_eq!((message.header.view.era.0, message.header.view.view.0), (4, 13));
        assert_eq!(message.header.slot, Slot::NONE);
        assert_eq!(
            message.body,
            Body::GossipRequest {
                prepared: Slot::NONE,
                committed: Slot::NONE
            }
        );
    }

    /// The frame is exact-length (W3): nothing the core would reject as
    /// trailing noise is ever produced, and the length constant above is
    /// the packed length the core itself computes.
    #[test]
    fn gossip_datagram_length_is_the_cores_packed_length() {
        let probe = Message {
            header: Header {
                tag: Tag::GossipRequest,
                view: ViewId {
                    era: Era(1),
                    view: View(0),
                },
                slot: Slot::NONE,
            },
            body: Body::GossipRequest {
                prepared: Slot::NONE,
                committed: Slot::NONE,
            },
        };
        assert_eq!(gossip_datagram(1, 0).len(), probe.packed_len());
    }
}
