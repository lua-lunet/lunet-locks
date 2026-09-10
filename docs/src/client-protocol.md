# External client protocol

Connect to a replica's TCP client endpoint and send one UTF-8 JSON object per
line. Responses are also one JSON object per line. A connection may carry
sequential requests; a client must wait for each response before sending its
next request.

Every request has a UUID `message_id`, a stable unsigned `client_id`, an
increasing unsigned `request_num`, and an unsigned `lock_id`. A retry uses the
same complete envelope. Replication deduplicates by `(client_id, request_num)`
and replays the exact prior reply; `message_id` is the client-routing key.

The protocol decodes strictly. A request that violates the field rules below
never enters the replication log. Fields a request does not carry are never
guessed at: unknown fields are ignored, and known optional fields are absent.
Every reply carries every documented field of its shape, with `null` where a
value is absent, so an observer can classify each event from the reply alone
without diffing successive states.

## Lock identity

A lock optionally carries a human display name and operator labels. Both are
lock attributes: they replicate in the lock table and ride every
lease-bearing reply.

- **`name`**: optional, at most 128 bytes, a zookeeper-style absolute path
  matching `^/(?:[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)$`. A lock acquires its
  display name at first grant and keeps it until a later grant supplies a new
  one: a SET that omits `name` leaves the stored name unchanged. There is no
  clearing verb; a new `name` replaces the old one.
- **`labels`**: optional, at most 8 entries, each 1–32 bytes matching
  `^[a-z0-9](?:[-a-z0-9]{0,30}[a-z0-9])?$`. Labels are deduplicated and
  canonically sorted on receipt, so any client ordering produces the same
  stored set; an empty list is stored as no labels. A SET that omits `labels`
  leaves the stored set unchanged; a SET that supplies `labels` replaces the
  stored set.

Both sizes are capped deliberately to stay WAL-conscious: at the caps a lock
entry carries at most about 392 bytes of identity.

## Lease-age counters

Each stored lease carries two state-machine-tracked counters:

- **`taken_at_ms`** (u64): the leader's execution tick at the holder-changing
  SET that installed the current holder. A same-ms holder change over a
  replaced record bumps the stored value by 1 ms so consecutive takes remain
  distinguishable. Release, expiry, and break clear it.
- **`renew_count`** (u32): incremented on each same-holder renewal. A holder
  change, a release, an expiry, and a break zero it.

The counters live on the leader's timeline. Every lease-bearing reply carries
`executed_at`, the leader's execution tick: interpret `taken_at_ms` and
`expiry` against `executed_at`, never against a client clock —
`executed_at - taken_at_ms` is the lease's age and `expiry - executed_at` its
remaining life, both as the leader measured them.

## GET

```json
{"op":"get","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":7,"lock_id":9001}
```

The response has `op: "get"`, echoes `message_id`, `request_num`, and
`lock_id`, and returns a live `lease` or `null`.

## SET

```json
{"op":"set","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":8,"lock_id":9001,"name":"/cluster/members/0000001","labels":["csv","prod"],"lease":{"lease_id":3,"holder":"02020202-0202-0202-0202-020202020202","expiry":1722600001000}}
```

`name` and `labels` are optional; the lease object is required and carries
only `lease_id`, `holder`, and `expiry` — the counters are never
client-writable.

SET returns `granted` and a `lease`. A lease is live only when
`expiry > execution_time`. An expired candidate is rejected. An absent or
expired incumbent is free; a live incumbent may be renewed or replaced only
by the same `holder`. A rejected SET does not change the lock table.

A granted SET replies with the stored lease — the candidate's `lease_id`,
`holder`, and `expiry` plus the lock's `name` (null when the lock has none),
canonical `labels` (null when it has none), and the counters as the state
machine set them: a first grant and every post-expiry take report
`renew_count: 0` and a fresh `taken_at_ms`; a same-holder renewal reports the
prior `taken_at_ms` and `renew_count` incremented by one. A refused SET
replies with the incumbent lease unchanged, counters included.

## RELEASE

```json
{"op":"release","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":8,"lock_id":9001,"holder":"02020202-0202-0202-0202-020202020202","lease_id":3}
```

RELEASE returns `released` and `lease`. It removes a live lease only if both
`holder` and `lease_id` match. A missing or expired lease is idempotently
successful (`released: true`, `lease: null`). A mismatched live lease returns
`released: false` with the incumbent lease unchanged, counters included.

## BREAK

```json
{"op":"break","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":9,"lock_id":9001}
```

BREAK is the privileged holder-break: an unconditional force-release that
names no holder. It releases the live lease through the same replication path
as every other transition — a proposed, committed operation that every
replica's state machine applies — and journals a distinct break event. On the
stored record it bumps `lease_id` by one, clears `holder`, `expiry`,
`taken_at_ms`, and `renew_count`, and keeps `name` and `labels`.

The reply has `op: "break"` and reports what happened:

- A live lease existed: `broken: true` and `lease` is the post-break record —
  the bumped `lease_id`, the retained `name` and `labels`, holder `nil`
  (`00000000-0000-0000-0000-000000000000`), and `expiry: 0`. That record is
  never live: a later GET reports `lease: null`, and the next grant replaces
  it while inheriting the retained `name` and `labels` unless the SET
  supplies its own.
- Nothing was stored under the `lock_id`: `broken: false` and `lease: null`.
  The lock was already free, with no record to break and no journal event.

Break is operator-trusted at exactly the same level as the membership verbs
and the lock verbs: the TCP client channel has no separate authentication,
and any client that can acquire locks can break one. The state machine
accepts `break` as a valid, documented command; authorization for break is
owned entirely by the admin edge that drives the channel, not by the state
machine. This is the deployment's stated trust model, not a gap.

## Field types

All lease fields are unsigned 64-bit JSON integers and are parsed in Rust, not
through LuaJIT numbers.

## Membership administration

Four admin verbs propose live membership changes over the same TCP channel:
`join`, `increment`, `decrement`, and `leave`. They are operator-trusted at
exactly the same level as the lock verbs — the TCP client channel has no
separate authentication, and any client that can acquire locks can propose
membership changes to the leader.

A `join` names the new member's id, name, and client-or-peer endpoint; the
other three name only the id:

```json
{"action":"join","message_id":"03030303-0303-0303-0303-030303030303","id":404,"name":"n4","endpoint":"127.0.0.1:27104"}
{"action":"increment","message_id":"03030303-0303-0303-0303-030303030304","id":404}
{"action":"decrement","message_id":"03030303-0303-0303-0303-030303030305","id":404}
{"action":"leave","message_id":"03030303-0303-0303-0303-030303030306","id":404}
```

A non-leader replica forwards a verb through the ordinary redirect machinery.
The leader drives the reconfiguration through the native adapter and answers
with one of three acknowledgment shapes:

- `{"action":"join","id":404,"accepted":true}` — the establishing operation
  committed and the leader's era advanced. The acknowledgment therefore
  arrives only after the commit and can take longer than a lock operation;
  a stop-the-world era entry in particular awaits the ordinary fence.
- `{"action":"leave","id":404,"accepted":false}` — the core refused the
  reconfiguration (fold gate, transition-outstanding gate); nothing entered
  the log.
- `{"action":"leave","id":404,"accepted":false,"reason":"deadline"}` — the
  establishing commit did not land within the client deadline. The verb may
  still commit later; re-drive with a fresh `message_id`. Duplicates of the
  timed-out verb replay this exact line from the leader's dedup cache, and
  every duplicate of any verb replays its first outcome.

The operator's sequences follow the weight rules: add a member with `join`
(the member enters at weight 0, a learner) then `increment` (the learner
becomes a voter); remove a member with `decrement` (the voter returns to
weight 0), wait for that era to commit, then `leave` — a leave of a member
above weight 0 is refused. Ids are the deployment descriptor's
admin-assigned, never-recycled member ids.
