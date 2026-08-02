# lunet-locks

`lunet-locks` is a lightweight locking service. Its locks are advisory and represented by expiring
leases. Requests are ordered by Viewstamped Replication Revisited (VRR). Teal and lunet own runtime
orchestration. A Rust `cdylib` owns the replication state machine and lock state. LuaJIT connects the
two through a deliberately small FFI.

## Read this first

- [Architecture](architecture.md) explains the combined node facade, ownership, FFI boundary, and
  state flow.
- [Corrected formalism](corrected-formalism.md) gives a reviewer-oriented statement of the protocol
  model, quorum rules, and safety obligations.
- [Glossary](glossary.md) defines the project terminology and its limited mapping to the paper.
- [External client protocol](client-protocol.md) specifies the JSON GET/SET messages and lease
  rules.
- [Internal VRR protocol](internal-protocol.md) specifies the 16-byte header and replica messages.
- [Build and tests](build-and-tests.md) records the ordered Rust, Cyan, LuaJIT, and documentation
  pipeline.

## Current scope

The fixed-membership protocol covers normal operation, epoch change, replica recovery, and
deterministic predicted execution values corresponding to paper Sections 4.1 through 4.4. The
formalism pages specify required behavior; [Build and tests](build-and-tests.md) separately records
the behavior currently exercised by tests.

The current core does not implement state transfer, checkpoints, reconfiguration (including the
paper's configuration generation, called its reconfiguration epoch-number), witnesses, batching,
or network encryption. Peer messages are bounded to one UDP datagram so a later lunet UDP transport
can encrypt and send each message without fragmentation at the application layer.

## Commands

```console
make build   # Rust checks/tests/release build, then Cyan
make check   # build plus all Teal type checks
make test    # check plus tested under LuaJIT
make docs    # build this Zensical site through uv
```

Source of truth remains the code. Generated Lua under `build/` and generated HTML under
`docs/site/` are artifacts.
