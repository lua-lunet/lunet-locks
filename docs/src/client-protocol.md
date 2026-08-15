# External client protocol

Connect to a replica's TCP client endpoint and send one UTF-8 JSON object per
line. Responses are also one JSON object per line. A connection may carry
sequential requests; a client must wait for each response before sending its
next request.

Every request has a UUID `message_id`, a stable unsigned `client_id`, an
increasing unsigned `request_num`, and an unsigned `lock_id`. A retry uses the
same complete envelope. Replication deduplicates by `(client_id, request_num)`
and replays the exact prior reply; `message_id` is the client-routing key.

All lease fields are unsigned 64-bit JSON integers and are parsed in Rust, not
through LuaJIT numbers.

## The lease object

GET, SET, and BREAK replies carry an extended lease object:

| field         | type            | notes |
|---------------|-----------------|-------|
| `lease_id`    | u64             | fencing token; BREAK bumps it |
| `holder`      | UUID or null    | null only in the cleared state a BREAK reply echoes |
| `expiry`      | u64 or null     | epoch ms; null alongside `holder` |
| `name`        | string or null  | display path supplied on SET |
| `labels`      | string array    | deduplicated, canonically sorted |
| `taken_at_ms` | u64 or null     | last-taken watermark, reported only while a holder is recorded |
| `renew_count` | u32             | same-holder renewals since the last take |

`taken_at_ms` is a last-taken watermark. It is set when a holder take occurs
(acquire or CAS), left unchanged on renew, and only *reported* while a holder
is recorded — the stored value is never cleared, so a holder change landing in
the same millisecond as the previous one still yields a strictly increasing
`taken_at_ms` (a 1 ms collision bump). `renew_count` increments on a
same-holder renewal and resets to 0 on a holder change, release, expiry, or
break.

A lock-table row (with its `name`, `labels`, and fencing `lease_id`) survives
release, expiry, and break.

## The `event` field

SET, RELEASE, and BREAK replies classify what the state machine did:

- `acquire` — SET granted over a lock with no recorded holder (missing,
  released, or broken).
- `renew` — SET granted whose holder equals the recorded holder.
- `cas` — SET granted that changes the recorded holder (over an expired
  incumbent; a live incumbent rejects a foreign holder outright).
- `deny` — SET rejected (expired candidate, live foreign incumbent, or
  invalid name/labels).
- `release` — RELEASE that actually removed a live lease.
- `break` — BREAK op (always emitted, even when idempotent).

SET and BREAK replies always carry `event`. RELEASE carries `event` only when
it removed a live lease (`release`); idempotent and mismatched RELEASEs omit
the field.

## Name and labels validation

SET accepts optional `name` and `labels`. Validation happens before any state
change, so an invalid SET never touches the lock table and replies `deny`.

- `name` matches `^/(?:[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)$` — a
  zookeeper-style absolute path of one or more non-empty segments drawn from
  ASCII alphanumerics, `.`, `_`, `-` — and is at most 128 bytes.
- Each label matches `^[a-z0-9](?:[-a-z0-9]{0,30}[a-z0-9])?$` (1–32 bytes).
  Labels are deduplicated and canonically sorted on acceptance; at most 8
  unique labels per lock.

## GET

```json
{"op":"get","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":7,"lock_id":9001}
```

The response has `op: "get"`, echoes `message_id`, `request_num`, and
`lock_id`, and returns a live extended `lease` or `null`.

## SET

```json
{"op":"set","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":8,"lock_id":9001,"lease":{"lease_id":3,"holder":"02020202-0202-0202-0202-020202020202","expiry":1722600001000},"name":"/cluster/members/0000001","labels":["db","us-east"]}
```

`name` and `labels` are optional. SET returns `granted`, an `event`, and a
`lease`. A lease is live only when `expiry > execution_time`. An expired
candidate is rejected. An absent or expired incumbent is free; a live
incumbent may be renewed or replaced only by the same `holder`. A rejected
SET (`granted: false`, `event: "deny"`) does not change the lock table and
returns the live incumbent's lease, or `null` when there is none. A granted
SET returns the recorded extended lease.

## RELEASE

```json
{"op":"release","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":8,"lock_id":9001,"holder":"02020202-0202-0202-0202-020202020202","lease_id":3}
```

RELEASE returns `released` and `lease`. It removes a live lease only if both
`holder` and `lease_id` match, in which case the reply carries
`event: "release"` and `lease: null`. A missing or expired lease is
idempotently successful (`released: true`, `lease: null`, no `event`). A
mismatched live lease returns `released: false` with the incumbent lease
triple unchanged. RELEASE's `lease` is the plain `{lease_id, holder, expiry}`
triple, not the extended object.

## BREAK

```json
{"op":"break","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":9,"lock_id":9001}
```

BREAK is an unconditional force-release. Over a live lease it bumps
`lease_id` (wrapping), clears `holder`/`expiry`, zeroes `renew_count`, and
replies `broken: true` with the cleared extended lease (`holder`/`expiry`/
`taken_at_ms` null). Over a missing or expired lock it is idempotent:
`broken: false`, `lease: null`. Every BREAK reply carries `event: "break"`.

The core performs no authorization on BREAK: any client that can reach the
protocol can break any lock. Authorization is owned by the admin edge in
front of the cluster, which is the intended issuer of break operations.
