# The two lock kinds

The locking service serves **one lock table with two lifecycle classes** —
the industry's own split, confirmed across Chubby, ZooKeeper, etcd,
PostgreSQL advisory locks, Consul and Redis (the survey record is in the
session store; the six-system classification is its §2):

- **Ephemeral leader lease** — created by a granted `SET`, dies at expiry
  or release. Renewal is the CAS poll: a holder that does not poll to
  renew is not heartbeated by anyone — the lease runs to zero and the
  lock is deleted at that moment. No session machinery, no smart-client
  heartbeat protocol: the poll is the heartbeat. Identity optional; after
  expiry the lock is free and any holder may take it. This is the
  low-latency shape — the Corfu global sequencer, one singleton lease.
- **Durable named lock** — created **with** a name (required at first
  grant), survives expiry and restart with its identity, and is destroyed
  only by an explicit **delete**. A **release** is not a delete: the
  record stays with its counters and identity (state: kept), so the
  fencing token survives and a daily root key does not silently vanish —
  while a delete is the real tombstone that garbage-collects it, so the
  table does not accumulate yesterday's business-day keys over a weekend.
  The Corfu stripe-leader lock is this kind. Restart survival rides the
  durable stream's fat frames (`wal-snapshot.md`).

One state machine, one table, two classes — not two subsystems. The
sticky identity inheritance a `SET` already performs over the prior
record (live or not) is the durable class's seed; the classes make that
behavior explicit and gated.

## Durable identity: the name→id assignment

A durable lock's u64 is not chosen by its caller — it is **the slot of
the creating consensus write**. The create path:

1. The leader holds the requested name and asks its own name→id view:
   has this name been committed? If yes, the create is rejected at the
   door — no consensus round is spent.
2. If no, the leader drives the create through consensus. The upcall
   carries the write's slot; that slot's u64 is the lock's id.
3. At commit, the check runs again: has a crash-recovery value from
   another era or view beaten it — is the name already taken under the
   committed state? If still no, the name is created and its id is the
   slot's u64.

The mapping stored is **name→id only**; the reverse lookup is YAGNI (an
id→name resolution scans the durable records when something needs it).
The two kinds' id spaces never mix: an ephemeral's u64 is caller-chosen,
a durable's is consensus-assigned, and they live in separated WAL
streams — one shape per ring, one id space per stream
(`wal-snapshot.md`).

## What this service deliberately does not do

The line is drawn against Chubby's full apparatus — the apparatus
ZooKeeper copied — and its details matter for where lunet sits:

- **No trees, no children, no directories.** The lock table is flat over
  numeric lock ids. The `name` is display/path metadata — a ≤128-byte
  absolute path whose slashes exist so a UI can drill down — never a
  namespace with child entries.
- **No config blobs, no ACLs.** A lock carries identity and lease state;
  it is not a small database row with payload bytes (Chubby's files, ZK's
  znode data).
- **No reference-counted ephemeral lifetime.** Chubby deletes an
  ephemeral node when *no client has it open* — ref-counted by open
  handles, so it can outlive its creator's session; its ephemeral
  directories exist and may hold children until empty *and* unopened;
  its ephemeral nodes are not memory-only — both kinds live in the same
  replicated database, and an ephemeral can survive a master failover
  inside the client's grace window. None of that is adopted: the lease's
  lifetime is the expiry on the leader's clock and the poll that renews
  it, full stop.
- **Terminology, per system**: Chubby says **permanent** (not
  "persistent" — ZooKeeper's word); "permanent" does not mean
  undeletable — an authorized client deletes explicitly; it just is not
  tied to any session's lifetime.

## The survey's load-bearing findings

1. Every surveyed system splits durable-with-identity from
   ephemeral-lease exactly as above; there is no single-kind mainstream
   design to imitate.
2. **Nobody puts user labels on locks** — metadata rides the value blob
   or is server-maintained stat data. Labels here stay what they are:
   capped operator tags (≤8 × 1–32 bytes, sorted and deduplicated at
   decode, the admission caps closing the DoS vector), on the durable
   kind.
3. **Fencing tokens are the load-bearing feature everywhere correctness
   matters** — Chubby's sequencer, ZK's `zxid`, etcd's `mod_revision`;
   Redis lacks one and is criticised for precisely that. The bumped
   `lease_id` of the break's keeper record is this system's token, and
   it survives a restart through the identity sidecar.
4. Size caps live at the door — ZK's 1 MiB znode check, etcd's 1.5 MiB
   request cap — and this service's admission validation of
   name/labels before the replication log is the same pattern.

## Status

Spec for the future implementation; the WAL snapshot work
(`wal-snapshot.md`) proceeds independently and serves both classes as
specified there. The durable kind's restart survival is the identity
sidecar's job; nothing above the store changes for the ephemeral kind.
