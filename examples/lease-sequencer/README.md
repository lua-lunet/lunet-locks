# lease-sequencer — the embedded-sequencer lease demo

The Corfu-style downstream design draft: six storage nodes, two per three
datacenters, each process embedding `lunet_advisory_lock::Node` (the same
adapter the LuaJIT host drives through the C ABI) and binding a UDP peer
port plus a TCP client NDJSON port with the same wire protocol as
`src/server.tl` (the LAL peer envelope, the membership/v3 genesis
fingerprint, NDJSON framing, the forward/redirect application channel).
The advisory lock holds the lease to be the SEQUENCER.

## The lease policy

Every node runs the same driver:

- if the lease is free, try to hold it (SET) with a 500 ms lease;
- if it holds it, renew 250 ms before the deadline (a fresh grant renews at
  250 ms after the grant);
- if another node holds it, poll it as a GET and schedule the next poll at
  the reported expiry plus `rand()*100 ms`.

Every attempt is logged to the node's log file as

```
lease-attempt ts=<ms> node=<id> op=set|renew|get|steal expiry=<ms>
```

with `note ...` lines for boot, membership, leader changes, grants, the
reincarnation remap, and undeliverable datagrams.

## Host policy: the client stream pauses during an era transition

A committed reconfiguration's establishing era completes only through the
§8.7.8 fence into the established era, and the primary's client stream is
exactly the activity that keeps that fence from arming (the PrepareOk
baseline refresh is gated while the transition is outstanding, but the
stream must actually stop). Every node therefore holds its lease driver —
and its forwarded traffic — while its view era differs from the folded
configuration era or it is not `Normal`; the lease lapses for the
transition's bounded window and a fresh grant re-acquires it. The measured
cost is one lease lapse per join, and the joins commit and complete.

## The cluster

`config/cluster.jsonl`: one genesis voter per DC (`dc1-node1`, `dc2-node1`,
`dc3-node1`) and one joiner per DC (`dc1-node2`, `dc2-node2`, `dc3-node2`)
that enters at weight 0 through the join verb. `run.sh` promotes `dc1-node2`
and `dc2-node2` with the increment verb; `dc3-node2` stays at weight 0 —
the zero-voting-weight member the downstream design wants. The TCP client
port is the descriptor's UDP peer port + 1000.

## Running

```
./run.sh     # the stability check (also as ./check.sh)
```

`run.sh` builds the crate, starts the six nodes with fresh state, drives the
joins and the two increments at the leader, runs the stability window
(cadence and poll assertions from the logs), and then three kill/restart
cycles: SIGKILL the current holder, wait 2000 ms, assert a survivor steals
the lease, restart the killed leader on the same state file, assert the
reincarnation rejoin (identity bump, the peers' remap notice), and assert
the cluster re-stabilizes. Every spawned process is killed on exit.

## Downstream consumption

This crate is the reference embedder. Downstream pins git revisions, exactly
as this repo does:

- `lunet-advisory-lock` has no release tag: pin the `lua-lunet/lunet-locks`
  commit the integration was validated against (`f5f8373` and later).
- The core comes in through the advisory-lock crate's dependency:
  `uvrr-core` upstream commit `0fc6380` plus the `lunet-locks/learner-era-fold`
  patch branch (submodule head `aacecda`), which carries the learner
  acquisition rule and the stop-the-world-under-stream completion. The
  `[patch]` section in `ext/advisory_lock/Cargo.toml` builds it from the
  vendored submodule; downstream mirrors that section with its own pin.
- The embedded surface is `Node::open` / `request` / `receive` / `idle` /
  `leader_timeout` / `recover` / `reconfigure` / `own_id` / `status` /
  `next_output` (`NodeStatus` carries both the view era and the folded
  configuration era). The C ABI is untouched.

## Layout

- `src/main.rs` — the node binary: host loop (heartbeat, election,
  fenced-boot recovery drive, output drain, receive pump), the TCP client
  NDJSON server, and the lease driver.
- `src/transport.rs` — the peer envelope, the membership/v3 genesis
  fingerprint, the `Reincarnation(old, new)` addressing notice, and the
  forward/redirect application channel.
- `src/bin/lease-client.rs` — the control client the orchestrator drives
  (lock and admin verbs over a node's TCP port).
- `config/cluster.jsonl` — the six-node deployment descriptor.
- `run.sh`, `check.sh` — the stability check.
