# External client protocol

Connect to a replica's TCP client endpoint and send one UTF-8 JSON object per
line. Responses are also one JSON object per line. A connection may carry
sequential requests; a client must wait for each response before sending its
next request.

Every request has a UUID `message_id`, a stable unsigned `client_id`, an
increasing unsigned `request_num`, and an unsigned `lock_id`. A retry uses the
same complete envelope. Replication deduplicates by `(client_id, request_num)`
and replays the exact prior reply; `message_id` is the client-routing key.

## GET

```json
{"op":"get","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":7,"lock_id":9001}
```

The response has `op: "get"`, echoes `message_id`, `request_num`, and
`lock_id`, and returns a live `lease` or `null`.

## SET

```json
{"op":"set","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":8,"lock_id":9001,"lease":{"lease_id":3,"holder":"02020202-0202-0202-0202-020202020202","expiry":1722600001000}}
```

SET returns `granted` and a `lease`. A lease is live only when
`expiry > execution_time`. An expired candidate is rejected. An absent or
expired incumbent is free; a live incumbent may be renewed or replaced only
by the same `holder`. A rejected SET does not change the lock table.

## RELEASE

```json
{"op":"release","message_id":"01010101-0101-0101-0101-010101010101","client_id":42,"request_num":8,"lock_id":9001,"holder":"02020202-0202-0202-0202-020202020202","lease_id":3}
```

RELEASE returns `released` and `lease`. It removes a live lease only if both
`holder` and `lease_id` match. A missing or expired lease is idempotently
successful (`released: true`, `lease: null`). A mismatched live lease returns
`released: false` with the incumbent lease unchanged.

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
