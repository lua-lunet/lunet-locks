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
