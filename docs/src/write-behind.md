# The write-behind lock table: local state durability

The ruling this documents: a voting node holds its lock table in memory,
and the cluster's protocol durability is the quorum's. Yet a node that
shuts down cleanly must come back with its locks, and a lease may run for
days. The mechanism is the node's own **lock-state artefact**: a framed
file written once in the node's life, at the clean shutdown, read back
once at the clean boot that follows. The boot fence decides what the
artefact is worth, and a dirty boot never reads it.

This is not a WAL of the protocol stream, and it is not a running tape of
lock events. The protocol state, the journal, the slots, stays in quorum
memory and recovers from the peers' streams, exactly as the architecture
document's durability paragraph states. The artefact carries the
application state alone: the shape of the locks, at the moment of the
clean halt.

The write-behind AOF writer is built and defended, and not wired into any
host path: the only in-tree constructor of an AOF sink is the example
crate's standby mode (`--aof-dir`), while the C ABI and the Lua host carry
the blocking journal where event evidence is needed (the console feed, the
bench chain) or the disabled sink elsewhere — both shapes proven by the
state seam's tests, which run the full stop and boot schedule with the
sink disabled. The writer stays compiled and tested by `make ext-test`.
Wiring it into a host surface is gated on the witness watermarks and the
three sync-point rules (the decisions register).

## Why a frame and not a running series

The contract that settles the shape is the boot fence's: **a dirty boot
never trusts the local series**. A node that crashed reincarnates and
recovers its locks from the live peers, whose memory holds the live locks
and the inflight commands whole. The local artefact is therefore read on
exactly one path, the clean boot, and everything else is cost. From that
the shape follows:

- A running append series written during operation buys nothing: its
  records would exist to shrink a shutdown dump that is already tiny,
  and a running series must be lossless, so its queue can backpressure
  the serving path, the one thing the serving path must never do.
- A frame written once at shutdown has no queue, no cadence, no loss
  window. The serving path never touches the disk at all: the node's
  I/O jitter in operation is zero, not deferred.
- The witnesses' append-only series remain the right tool in their
  place: a continuous tape of the wire stream is what the console
  follows and what the sync-point certificate rides. Incremental is
  what a tape is for. A table at a point in time is what a frame is for.

## The artefact

One framed file per life, written beside the marker files:

1. **The sync-start marker**: a checksummed record naming the life (the
   identity pair) and the committed frontier the state corresponds to.
2. **The live-lock records**: the table's live locks, each checksummed.
3. **The end marker**: a checksummed record that closes the frame. The
   end marker vouches for completeness: a frame without it is a torn
   write, and a torn frame is refused, never repaired.

The write is the marker store's own discipline: written to a sibling
temporary file, forced, renamed over the live name, the parent directory
synced. The reader sees the old frame or the new one, never a half.

## The sweeper

Leases run for hours or days, and a lock nothing renews is a dead record
carried forever. The **sweeper** reaps them, and it is driven by the
leader's heartbeat cadence rather than a timer of its own: the heartbeat
already paces the cluster's liveness, and piggybacking it keeps the
sweep deterministic and centralised. A lock whose lease has expired is
released in the table; the frame at shutdown then contains the live set
alone. No tombstones are written: there is no running series to keep
consistent, and the dead are simply absent from the frame.

The sweeper is what keeps the artefact honest to its sizing claim: the
live set is tiny because the dead set is removed, and the shutdown flush
is cheap because there is little live state to write.

## The clean-shutdown ceremony

The ceremony's order is fixed, and the flushed marker vouches for all of
it:

1. **Broadcast the sync point** to all nodes, fire and forget, so every
   witness syncs its tape. Their acknowledgements are never awaited: the
   witnesses' series are the backup certificate, and the node's
   certificate is its own frame.
2. **Stop the network loop.** No further protocol traffic is accepted;
   anything arriving after this point is dropped.
3. **Write the frame**: the sync-start marker, the live-lock records,
   the end marker, forced and renamed by the artefact's discipline. The
   frame write and the superblock marker rounds are independent work and
   proceed concurrently.
4. **When all of it is done, the node halts.** The flushed marker round
   is written last and vouches for the frame beneath it. A halt that did
   not finish the frame does not write flushed; the restart classifies
   dirty and reincarnates, and the frame is never read.

## Startup

The boot fence classifies the start, and the classification chooses the
path, exactly as the architecture document states it:

- **Clean** (the marker reads flushed): the node reads its frame,
  validates it whole (the start and end markers agree, every record's
  checksum holds, the life pair matches the marker's), rebuilds the lock
  table from it, and carries on: it rejoins the cluster for the protocol
  state, which comes from the live peers as ever. The frame restores the
  locks; the peers restore the protocol. Neither crosses into the
  other's lane.
- **Dirty**: the running sentinel means the last life did not finish.
  The node reincarnates under a bumped identity and never reads the
  frame. The lock table comes back through the cluster, from the live
  nodes' memory.
- **A full-cluster cold start** re-forms empty. The witnesses' series
  remain the operator's backup: the record exists for an operator-driven
  recovery, and no startup path replays it automatically.

## The nuances the design rests on

- **The artefact is per-life in effect**: it is written by one identity
  at one clean halt and read back only on the clean boot of the
  deployment it was written in. The boot fence's identity law is
  unchanged: a dirty marker disqualifies the frame along with the
  identity.
- **A clean boot with a bad frame is a contradiction the operator must
  see.** Checksum every record, demand the end marker, refuse on any
  disagreement, never repair in place. The refusal path is the
  operator's.
- **The sweep runs on the ordinary heartbeat cadence, before any flush
  it protects.** The ceremony writes what is live at that moment; a
  lease that expired one tick ago is released, not persisted.
- **No lazy per-acquisition flush.** Under the boot fence's contract the
  frame is read only on a clean boot, so incremental forcing in normal
  operation pays for a read that never happens on the path that pays.
  The single forced write sits where the operator chose to pay for it:
  inside the shutdown, where the flush and the marker rounds already
  live.
- **The witnesses' cadence is untouched.** Their sync rules force one
  early flush per ceremony and nothing else; the broadcast is fire and
  forget because the node's certificate is its own frame, not the
  witnesses' replies.
- **The cost is paid where it is cheap.** The serving path never touches
  the disk; the shutdown pays one framed write of a small live set; the
  startup reads it once. The gift of the design is that the costly work
  sits in the clean shutdown, and this mechanism is the reason the gift
  survives the power cycle.

See [the telemetry AOF](telemetry-aof.md) for the write-behind discipline
the witnesses' tape follows, and [the decisions register](decisions.md)
for the witness-as-state-holder ruling the ceremony cites.
