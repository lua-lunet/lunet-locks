# Lunet advisory lock

`lunet-advisory-lock` is a fixed-membership service for expiring, advisory
leases. Lunet and Teal provide the TCP/UDP process; a small Rust adapter owns
lock execution and delegates replication to
[vrr-core](https://github.com/lua-lunet/vrr-core).

## Service topology

Start every member with the same lexically sorted membership list. Each member
has one UDP peer endpoint and one TCP client endpoint:

```console
lunet-run build/server.lua \
  --node n1 --client 127.0.0.1:8001 --state /var/lib/lunet-lock/n1.nonce \
  --member n1=127.0.0.1:7001 \
  --member n2=127.0.0.1:7002 \
  --member n3=127.0.0.1:7003
```

At least three uniquely named members are required. Member names must be
lexically sorted, endpoints must be literal IPv4 `host:port` values, and
`--node` must occur exactly once. The supplied `lunet-run` is the
project-local official `v0.8.0` runtime from `.lunet/v0.8.0/`, not a binary
from `PATH`.

- [Architecture](architecture.md) describes client forwarding, recovery, and
  operational boundaries.
- [External client protocol](client-protocol.md) specifies GET, SET, and
  RELEASE.
- [Build and tests](build-and-tests.md) describes the pinned runtime and
  project commands.

Replication mechanics, wire behavior, and safety proofs belong to
[vrr-core](https://github.com/lua-lunet/vrr-core). This repository deliberately
does not duplicate them.
