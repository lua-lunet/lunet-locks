# The write-behind lock table: local state durability

The ruling this documents: a voting node holds its lock table in memory,
and the cluster's protocol durability is the quorum's. Yet a node that
shuts down cleanly must come back with its locks, and a lease may run for
days. The mechanism is the **write-behind**: a local append-only series
that carries the lock state, written asynchronously and never forced on
the serving path, flushed exactly once in the node's life, at the clean
shutdown. The boot fence then decides what the series is worth.

This is not a WAL of the protocol stream. The protocol state, the
journal, the slots, stays in quorum memory and recovers from the peers'
streams, exactly as the architecture document's durability paragraph
states. The write-behind carries the application state alone: the shape
of the locks.

## The series

Every voting node runs a write-behind writer for its lock state: a
dedicated writer behind a bounded queue, the same discipline the
telemetry AOF follows. The records are lock-state transitions: the holds,
the renews, the releases, the breaks, as the lock table itself applies
them. The series grows one record per transition and is otherwise
invisible:

- **Nothing on the serving path forces the disk.** The writes are
  buffered appends; no fsync, no `O_SYNC`, no `O_DIRECT`. The serving
  path's I/O jitter is zero by construction.
- **The series is never fsynced in normal operation.** Not on a timer,
  not on a checkpoint. The one forced flush happens inside the shutdown
  sequence, where the operator has chosen to pay for it.
- **The content is the live lock set**, not the wire traffic. A record
  names a lock-state transition; replaying the series rebuilds the table.

## The sweeper

Leases run for hours or days, and a lock nothing renews is a dead record
carried forever. The **sweeper** reaps them: on its timer it walks the
lock table, and every lock whose lease has expired is released in the
table and recorded in the series as such. The sweeper is what keeps the
series honest to its sizing claim: the live set is tiny because the dead
set is removed, and the shutdown flush is cheap because there is little
live state to write.

## The clean-shutdown ceremony

The ceremony's order is fixed, and the flushed marker vouches for all of
it:

1. **Broadcast the sync point** to all nodes, fire and forget. The
   witnesses receiving it apply their sync rules (acknowledge if already
   flushed, force the flush if buffered, hold and flush on arrival if
   unseen) and their replies are never awaited. The witness series is the
   backup certificate, not the restore source.
2. **Stop the message loop.** No further protocol traffic is accepted;
   anything arriving after this point is dropped.
3. **Force the local sync**: the write-behind's pending records and the
   current live-lock set are flushed to disk, forced, in parallel with
   the superblock marker rounds. The flush of the live set and the marker
   rounds are independent work and proceed concurrently.
4. **When all of it is done, the node halts.** The flushed marker round
   is written last and vouches for what is under it: a marker that says
   flushed asserts the write-behind beneath it is complete. A halt that
   did not finish the flush does not write flushed; the restart
   classifies dirty and reincarnates, and the series is never trusted.

## Startup

The boot fence classifies the start, and the classification chooses the
path, exactly as the architecture document states it:

- **Clean** (the marker reads flushed): the node reads its write-behind
  series, rebuilds the lock table from it, and carries on: it rejoins
  the cluster for the protocol state, which comes from the live peers as
  ever. The local series restores the locks; the peers restore the
  protocol. Neither crosses into the other's lane.
- **Dirty**: the running sentinel means the last life did not finish.
  The node reincarnates under a bumped identity and never reads the
  series: a node does not trust the tail of a life that did not close.
  The lock table comes back through the cluster, from the live nodes'
  memory, which holds the live locks and the inflight commands whole.
- **A full-cluster cold start** re-forms empty. The witnesses' series
  remain the operator's backup: the record exists for an operator-driven
  recovery, and no startup path replays it automatically.

## The nuances the design rests on

- **The series is per-node and per-life in effect**: it is written by one
  identity and read back only on a clean boot of the deployment it was
  written in. The boot fence's identity law is unchanged: a dirty marker
  disqualifies the series along with the identity.
- **The write-behind is not a correctness input.** A torn or corrupt
  series at a clean boot refuses the start by the same paranoia the
  marker store has: checksum every record, refuse on disagreement, never
  repair in place. The refusal path is the operator's, because a clean
  boot that cannot read its own series is a contradiction the operator
  must see.
- **The sweep must run before the flush it protects.** A shutdown begins
  with the table already swept on the ordinary cadence; the ceremony
  writes what is live at that moment, and a lease that expired one tick
  ago is released, not persisted.
- **The witnesses' cadence is untouched.** Their sync rules force one
  early flush per ceremony and nothing else; the broadcast is fire and
  forget because the node's certificate is its own flush, not the
  witnesses' replies.
- **The cost is paid where it is cheap.** The serving path never forces
  I/O; the shutdown pays one forced flush of a small live set; the
  startup reads a small series once. The gift of the design is that the
  costly work sits in the clean shutdown, and this mechanism is the
  reason the gift survives the power cycle.

See [the telemetry AOF](telemetry-aof.md) for the write-behind discipline
the series shares, and [the decisions register](decisions.md) for the
witness-as-state-holder ruling the ceremony cites.
