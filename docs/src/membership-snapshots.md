# Membership snapshots

Era-qualified cluster membership snapshots ride the LAL peer protocol as
its third packet kind, alongside the opaque VRR traffic and the
application packets. They cover configuration discovery, not identity
rejoin: a node whose *configuration knowledge* is stale — including one
that cannot yet address the members it remembers — learns the live
membership through snapshots, while the `Reincarnation(old, new)` wire
body keeps solving identity rejoin. Neither replaces the other.

Snapshots are **advisory evidence and never a consensus mechanism**.
Safety stays with the replicated configuration commands: a discovered
configuration is adopted in memory, feeds the existing fenced-boot path,
and never bypasses a gate. The core's folded configuration remains the
commit truth.

## Wire shape

The pair rides the peer envelope with kind `\3`:

```text
\0LUNET_ADVISORY_LOCK_PEER\0 | \3 | membership fingerprint | payload
```

The membership fingerprint is the envelope's own: a snapshot for a
foreign deployment (a fingerprint mismatch) is dropped without ceremony
— the datagram is discarded and the replica is not quarantined, because
the packet never claims consensus authority.

The **request** payload is one byte, the tag `\1`. The requester needs no
state of its own; every responder answers from its current membership
model, so the booting node never has to work out who the leader is.

The **response** payload is one datagram, deliberately small — nodes and
voting weights, nothing else. Big-endian, fixed width, no varints:

```text
tag (\2)
era          u32
slot         u64
member count u16
per member:  id (u32) | weight (u32) | endpoint length (u16) | endpoint bytes
```

- `era` — the configuration generation the snapshot describes.
- `slot` — the log slot at which that configuration was chosen (the
  establishing operation's entry slot); the genesis configuration is
  slot 0. Era plus slot make the up-to-dateness comparison total, since
  every configuration knows the slot at which it was chosen.
- `members` — `{id, weight, endpoint}` per member: the voting weights as
  at that era. Members ride in canonical order, strictly ascending by
  id, so one membership has one wire representation. A configuration
  carries at most 16 members (the core's bound).
- `fingerprint` — carried by the envelope, not the payload.

A payload that does not decode exactly — truncated, mistagged, trailing
bytes, over the member cap, a non-IPv4 endpoint, or descending ids — is
dropped, never partially applied.

## The membership model

Every process carries a small in-memory membership model: an era, a
choosing slot, and the member list `{id, weight, endpoint}`. It boots
from the membership sidecar next to the incarnation marker when one
parses, otherwise from the deployment descriptor — the descriptor's
genesis lines in line order at weight 1, era 1 (the founding
configuration the core folds, chosen at slot 0). A post-genesis
descriptor line is an addressing row, not a membership fact: the node
can talk to that member, and snapshots and dissemination teach whether
it is still in the live configuration and at what weight.

The model moves only forward. A snapshot whose (era, slot) is newer than
the model's is adopted in memory, adds the addressing rows its
membership names, and writes behind; an equal or older snapshot is a
no-op everywhere. The model's generation counter advances exactly once
per reconfiguration commit the node itself drives — stamped with the
establishing operation's choosing slot, captured from the
reconfiguration drive's own sends — and by adoption whenever an observed
snapshot is newer.

## Boot-time discovery

A node coming up runs a bounded discovery loop before relying on the
ordinary fenced boot:

1. it sends the snapshot request to **every node it remembers** (its
   addressing tables); it does not need to know the leader;
2. it starts by needing a quorum within what it thought was the old
   cluster;
3. any response carrying a **higher era invalidates** earlier responses:
   the node moves to that era, adds the addressing rows that era's
   membership names, re-requests across them, and repeats;
4. every era bump drops the older-era responses;
5. it stops when, for some era that has not been invalidated, it holds a
   **quorum of agreeing membership snapshots**. That era and membership
   are adopted in memory; the ordinary fenced boot proceeds — and on the
   discovery deadline it proceeds regardless, with whatever (possibly
   nothing) has been adopted.

Two snapshots **agree** when they name the same configuration: identical
era, choosing slot, and member set. The quorum is the weighted majority
the deployment's configurations vote with (`WeightedMajority`): the
threshold is half the configuration's total weight plus one, summed over
the agreeing responders. Weight-0 learners' snapshots are collected but
contribute nothing, so learners never satisfy a quorum; the responder's
weight is taken from the snapshot's own membership, and a snapshot that
does not name its responder contributes nothing.

While discovery is running, a response only escalates the era or tallies
agreement within it — the model adopts at a quorum, never from one
node's word. After the discovery window, the dissemination semantics
apply.

## Post-commit dissemination

When the leader learns its proposed reconfiguration command was chosen
(committed — the same observation that settles the client
acknowledgment), it applies the committed verb to its membership model,
moves to the next generation at the establishing slot, and sends the
as-at-new-generation snapshot to the (new) membership. A recipient that
is already up to date does nothing; a lagging one adopts in memory
immediately and writes behind. Snapshots from a lower era are ignored
everywhere: era plus slot make the comparison total. Dissemination is
best-effort: it fires when the leader observes the establishing commit;
a verb whose commit lands after the leader stopped observing is carried
by the commit stream and by later snapshots instead.

## Lazy write-behind

Adopted membership facts persist asynchronously on a routine tick —
never on the message hot path. The sidecar sits next to the incarnation
marker at `<state-file>.membership`, one flattened JSON object per line:

```text
{"format":"membership-sidecar/v1","era":N,"slot":N}
{"id":N,"weight":N,"endpoint":"host:port"}
```

One header line, then one member line per member in canonical
ascending-id order, so the file diffs and greps like the deployment
descriptor. It is rewritten atomically (temporary file, rename) with no
fsync: the last write may be lost on power loss — the same documented
loss window the AOF writer carries, and nothing depends on the file for
safety. A sidecar that does not parse is ignored entirely at boot: the
model falls back to the descriptor and discovery re-learns. The
incarnation marker's bump semantics are untouched — the marker keeps its
fsync+rename+dir-sync discipline and its four-line boot classification.

## Boundaries

- Snapshots are advisory evidence; a node's core folds eras only through
  its own protocol paths, and a fenced node stays fenced until the
  stream proves currency. A discovered configuration teaches the host's
  addressing tables and model, never the core's folded configuration.
- A committed reconfiguration the node never observed (a deadline
  expiry, a core-driven forced step of the reincarnation walk) can leave
  the model below the live configuration until the next snapshot or
  dissemination arrives; the model never regresses and the core's folded
  configuration remains the commit truth.
- The addressing rows an adopted snapshot teaches grow additively and
  never regress: a departed member's rows leave through the leave verb's
  broadcast, and a reincarnated member's rows keep resolving through the
  reincarnation remap.

## The acceptance run

`examples/lease-sequencer/run-acceptance.sh` boots a 7th node on the
GENESIS descriptor while the live cluster is at era 6 (after three joins
and two increments), and asserts — from the node's log and its
membership sidecar — that it escalates to the live era, adopts a quorum
of agreeing era-6 snapshots in memory, has the adopted facts written
behind, joins through the ordinary fenced boot (the committed `Join`
and `Increment`), and keeps its sidecar current through the leader's
post-commit disseminations: era 7 after the join, era 8 after the
increment, with the member line exact.
