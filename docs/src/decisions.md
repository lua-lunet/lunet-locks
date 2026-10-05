# Decisions

Decisions of record for the service: what was decided, why, and what
the decision binds. Each entry states a ruling that code and
configuration are held to. History belongs in the commit log, not
here; an entry describes the standing rule, not the moment it was
made.

## Three voting nodes is the minimum

A cluster that cannot commit is not a lock service, and the sizes
below that cannot are not deployments.

- **One node** has no quorum to draw from. It accepts a client
  proposal and can never commit it: the accepted-uncommitted proposal
  sits forever, because there is no self-addressed prepare to advance
  it and no second member to answer one. It also self-fences
  periodically against its own compliance clock. A singleton is a
  process that appears to serve and never does.
- **Two nodes** are replication without fault tolerance. The quorum is
  both members, so any one failure stops all progress; there is no
  third member to carry a majority. The failure mode is the same as
  the singleton's, reached one node later.
- **Three nodes** are the smallest configuration whose majority
  survives the loss of any single member. Two of three remain a
  quorum, so the cluster continues to commit and elect through a
  single-node failure.

**The ruling: a cluster never runs with fewer than three voting
members.** "Voting" is load-bearing: a member folded into the
configuration as a learner carries weight zero, contributes nothing to
a quorum, and does not count toward the three.

The ruling is enforced at two points, and at both it refuses rather
than repairs — a configuration that cannot safely commit is a
configuration the operator must be told about, not one to be silently
corrected into a different shape.

- **At boot, on the descriptor path.** A cluster descriptor naming
  fewer than three voting members is refused before any node starts.
- **At execution, on reconfiguration.** Any `Leave` or `Decrement` that
  would leave fewer than three voting members is refused before it
  drives. A member at weight zero leaving, and a departure that leaves
  three voters or more, both proceed: the floor bounds the voting
  count, not the row count.

Contraction is permitted down to exactly three. A fourth member folds
in as a learner at weight zero, is promoted to a fourth voter, and is
demoted again — the cluster returns to three and the floor stands.
The floor is a floor, not a fixed cluster size.

The enforcement is best-effort and is checked at the two points above.
A configuration assembled from a snapshot rather than a descriptor is
held to the same floor by the descriptor that produced it, not
re-derived from the snapshot at read time.

A reconfiguration is complete only once the serving era has caught the
configuration era on every member. An operation driven inside a
still-establishing era is refused — the transition has not committed,
so the cluster's voting count is not the one the operation was
authorised against.

## Failure detection is the flavoured-timeout model alone

Failure detection in this service is the flavoured-timeout model, and
the model is complete without a second estimator.

The flavoured-timeout model supplies failure detection itself: each
node watches a leader over a randomised timeout and, on the deadline,
acts — a host-forced view change, the `suspect` toggle, the flush. A
node that observes a real failure is therefore guaranteed to act on
its own schedule, with no further mechanism required to notice.

A second estimator of the same fact is a liveness liability, not a
redundancy. It is the same actuation, driven by a more elaborate
measurement of the silence the leader timeout already measures. Where
the two disagree, the extra state is a second thing that can be wrong
about liveness, and liveness reasoning is exactly where a second
opinion is a liability — an estimator that crosses on a healthy but
quiet cluster manufactures an unnecessary view change, and one that
does not cross withholds a necessary one.

**The ruling: the randomised leader timeout is the sole
failure-detection mechanism.** The detection contract a second
estimator served is already held by the leader timeout, and every
timeout-related rule in the service is expressed against that model.
The timeout naming law is unchanged by this: timeout unqualified
remains short for leader timeout.

See [Failure detection and the timeouts](failure-detection.md) for the
mechanisms this ruling governs.

## Witnesses hold the durable stream; clean shutdown is a sync ceremony

A voting node never writes protocol state to its local disk. The cluster's
store of record is quorum memory, and a restarting node recovers its state
from the streams of its peers. Local disk on a voter carries the lifecycle
marker, which fences, and observability, which vouches for nothing. The
question that remains is where the state is durable when every voter is
down, and the answer is the witnesses.

**The ruling: the witness is the deployment's state holder.** Every
deployment registers at least one witness per data centre. A witness is
outside the roster, never votes, and receives the full phase-2 and commit
stream from every leader in turn. Its append-only series is the durable
copy of the stream: the witness records what it receives and tracks two
high-watermarks, the highest frontier it holds in its buffer and the
highest frontier it has flushed. The series is no longer observability
only; it is the copy a full-cluster cold start recovers from.

The witness's write cadence is unchanged by the role: deferred, batched,
force-flushed on a timer rather than on the replication path, so its I/O
jitter stays off the voters. The one exception is the sync point, below,
which forces an early flush of a batch already in hand: a small amount of
bookkeeping and an early flush, never a change to the streaming protocol.

### The shutdown ceremony

A node shutting down cleanly owes itself one act before it may call
itself flushed: its lock table durable under its own write-behind series.

1. The node broadcasts a sync point, its own committed frontier, to all
   nodes, fire and forget, so that every witness syncs. Every witness
   receiving the point applies the same rule: if its flushed watermark
   already covers the frontier it acknowledges immediately; if the
   frontier is in its buffer it forces the flush and acknowledges; if it
   has not yet seen the frontier it holds the point open, flushes the
   moment the stream covers it, and acknowledges then. No reply is
   awaited: the witnesses' series are the backup certificate, and the
   node's certificate is its own flush.
2. The node stops its message loop. No further protocol traffic is
   accepted, and anything arriving after this point is dropped.
3. The node forces its own local sync: the write-behind's pending records
   and the current live-lock set are flushed to disk, forced, in parallel
   with the superblock marker rounds. The two are independent work and
   proceed concurrently.
4. When all of it is done, the node halts. The flushed marker round is
   written last and vouches for the write-behind beneath it. A halt that
   did not finish the flush is not a clean halt: the marker does not
   claim flushed, and the restart classifies dirty, which reincarnates.
   The ceremony is not a precondition of stopping; a node always stops.
   It is the precondition of the marker that says the stop was clean.

### The startup side

The witness series is never on the startup path. A starting node reads
its boot fence, and the classification chooses the path:

- **Clean** (the marker reads flushed): the node reads its own
  write-behind series, rebuilds the lock table from it, and carries on.
  The local series restores the locks; the live peers restore the
  protocol state, as ever. Neither crosses into the other's lane.
- **Dirty**: the node reincarnates at once and never reads the series.
  It gossips its frontiers to every node it knows, every live node
  answers with its frontiers, not only the leader, and the state
  transfer carries the live state whole: all the live locks and every
  inflight command, the retransmission retention the base protocol
  family keeps for exactly this purpose. The live cluster's memory is
  the source of truth for a rejoining node; the tape is not.

A full-cluster cold start re-forms the cluster empty. The witness series
is the shutdown certificate and the console's record, and it is the
operator's backup: an ordered shutdown leaves the whole stream durable,
the record exists for an operator-driven recovery, and no startup path
replays it automatically.

Witness retention is bounded by the lease horizon, not by the deployment's
age. A witness retains its series indefinitely while it runs, rotates its
files at its own reboot, and may drop files old enough that every lease
they record has expired: the live set is small and the dead record is
bulk.

**What this binds.** The shutdown ceremony is enforced by the marker: the
flushed round vouches for the local write-behind flush, never for the
process's own intent. The witness's two watermarks and the three
sync-point rules are the AOF event loop's contract, tested red-green
around the loop: an ack for an already-flushed point, a forced flush for
a buffered point, a held point that flushes and acks when the stream
arrives. The end-to-end contract: a clean shutdown followed by a restart
preserves every lock the node's own series recorded; a dirty boot never
trusts the series it finds. See [the write-behind lock table](write-behind.md)
for the mechanism and its nuances, and [the telemetry AOF](telemetry-aof.md)
for the write-behind discipline both series share.
