# Lunet advisory lock

`lunet-advisory-lock` is an advisory-lock service for expiring leases with
live, reconfigurable membership. Lunet and Teal provide the TCP/UDP process;
a small Rust adapter owns lock execution and delegates replication to
[uvrr-core](https://github.com/lua-lunet/uvrr-core), vendored as the
`ext/uvrr-core` submodule.

## Service topology

Every member starts from the same JSONL deployment descriptor. Each member
has one UDP peer endpoint and one TCP client endpoint:

```console
lunet-run build/server.lua \
  --node n1 --client 127.0.0.1:8001 --state /var/lib/lunet-lock/n1.state \
  --cluster /var/lib/lunet-lock/cluster.jsonl
```

The descriptor is one JSON object per line, `{"id":101,"name":"n1","host":
"127.0.0.1","port":7001,"genesis":true}`. Each entry's `id` is that
replica's live `NodeId`: admin-assigned, sparse, and never recycled. The
`genesis` lines, in line order, are the deployment's founding membership
and the genesis succession sequence; the first genesis line's member is the
genesis primary. At least three genesis members are required, member names
must be unique, endpoints must be literal IPv4 `host:port` values, and
`--node` must appear exactly once in the descriptor. A member that joined a
live cluster is appended after the genesis lines with `"genesis":false`;
running processes never reload the file — live membership changes travel
through the admin verbs and the peer broadcast, not through the file. The
supplied `lunet-run` is the project-local official `v0.8.0` runtime from
`.lunet/v0.8.0/`, not a binary from `PATH`.

- [Architecture](architecture.md) describes client forwarding, live
  reconfiguration, reincarnation on restart, and operational boundaries.
- [Event journal](event-journal.md) documents the append-only lock-event
  journal, the lock-feed server, and the console catch-up model.
- [Standby telemetry](telemetry-aof.md) documents the standby node's AOF
  write-behind series: the async writer, 2 MiB erasure-block rolling, the
  deferred fsync policy, and the console follow path.
- [External client protocol](client-protocol.md) specifies GET, SET, and
  RELEASE, and the membership-administration verbs.
- [Build and tests](build-and-tests.md) describes the pinned runtime and
  project commands.

Replication mechanics, wire behavior, and safety proofs belong to
[uvrr-core](https://github.com/lua-lunet/uvrr-core). This repository
deliberately does not duplicate them.
