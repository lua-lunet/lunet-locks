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
