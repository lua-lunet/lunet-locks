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

The **WAL snapshot store** — the lock table's periodic async snapshot over
the tbio-core engine — is the C ABI's mounted store: the node constructor
mounts it beside the state path, and nothing above the `StateStore` trait
knows which store the node holds. The node's ~1000 ms tick loop drives it
and the clean-shutdown stop takes the eager flush.
[wal-snapshot](wal-snapshot.md) is the store's own page; a refused
construction never blocks the boot — the store is a fallback mechanism,
never a safety mechanism, so the node boots on the file store and says so
loudly while the lock table rebuilds from the replica stream.

The write-behind AOF writer's mount surface — the C ABI's
`lunet_lock_node_new_aof` and the server's `--aof-dir` (with
`--aof-flush-ms`) — is [architecture](architecture.md)'s; the example
crate's standby mode (`--aof-dir`) constructs the same sink. The writer is
compiled and tested by `make ext-test` and the WAL node tests.

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
carried forever. The **sweeper** reaps them, and it rides the leader's
steady state tick rather than a timer of its own: the tick already paces
the node's durability loop, and sweeping on the leader only keeps the
reaping deterministic and centralised. Each expired lease is broken as a
committed operation — the break rides the replication log, so every
replica's table agrees; a local mutation of one node's table would
diverge the quorum state. The break is the keeper's verb: the generation
is bumped, the holder is nil, and the frame at shutdown then contains
the live set alone. No tombstones are written: there is no running
series to keep consistent, and the dead are simply absent from the
frame. The sweep is bounded per tick — a backlog of dead leases drains
over ticks, never stampedes the log.

The sweeper is what keeps the artefact honest to its sizing claim: the
live set is tiny because the dead set is removed, and the shutdown flush
is cheap because there is little live state to write.

## The clean-shutdown ceremony

The ceremony's order is fixed, and the flushed marker vouches for all of
it ([the ruling](decisions.md)): broadcast the sync point fire-and-forget,
stop the network loop, then write the frame — the sync-start marker, the
live-lock records, the end marker, forced and renamed by the artefact's
discipline, in parallel with the superblock marker rounds — and halt, the
flushed marker round written last. A halt that did not finish the frame
does not write flushed; the restart classifies dirty and reincarnates,
and the frame is never read.

## Startup

The boot fence classifies the start, and the classification chooses the
path ([the ruling](decisions.md)). **Clean** (the marker reads flushed):
the node reads its frame, validates it whole — the start and end markers
agree, every record's checksum holds, the life pair matches the marker's —
rebuilds the lock table from it, and rejoins the cluster for the protocol
state, which comes from the live peers as ever. A clean boot with a bad
frame is a contradiction the operator must see: refuse on any
disagreement, never repair in place; the refusal path is the operator's.
**Dirty**: the node reincarnates under a bumped identity and never reads
the frame; the lock table comes back from the live nodes' memory. The
frame restores the locks; the peers restore the protocol. Neither crosses
into the other's lane.

See [the telemetry AOF](telemetry-aof.md) for the write-behind discipline
the UI server's tape follows, and [the decisions register](decisions.md)
for the UI-server-is-the-state-holder ruling the ceremony cites.
