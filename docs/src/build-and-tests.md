# Build and tests

Install the project tools into their local locations:

```console
make init       # mise tools, then Cyan, Cerulean, and tested in .rocks/
make hooks      # enable the formatting pre-commit hook once after clone
```

The native adapter requires Rust 1.85 or newer. It depends on the exact pinned
`vrr-core` Git revision, fetched over HTTPS from the public upstream repository,
so neither local builds nor CI need any credential setup.

## Commands

```console
make fmt             # format Teal with Cerulean
make lint            # reject unformatted Teal
make build           # Rust checks/tests/release cdylib, then Cyan output
make check           # build plus all Teal type checks
make test            # check plus tested
make lunet-runtime   # fetch and verify Lunet v0.8.0 locally
make smoke           # build and run the three-process service smoke test
make simulation      # 30s TCP-NDJSON three-datacenter lease failover demo
make simulation-test # focused std-Rust simulator unit tests
make docker-build    # plain Docker image, including Linux Lunet v0.8.0
make docker-simulation # the same 30s simulation against a stable Docker cluster
make docs            # render the Zensical site
```

`make lunet-runtime` downloads the host-specific official Lunet `v0.8.0`
archive, verifies its SHA-256, and extracts it into `.lunet/v0.8.0/`. The
service and smoke test always use `.lunet/v0.8.0/lunet-run`; they do not use a
runtime from `PATH`. The shipped LuaCATS/Teal runtime documentation is at
`.lunet/v0.8.0/types/`.

`make smoke` starts three local nodes, connects through a nonleader, and
covers acquire, GET, contention, RELEASE, reacquisition, expiry takeover, and
one-replica restart while a quorum remains live. Temporary logs and process
state live under `.tmp/`; the downloaded runtime does not.

`make simulation` starts the same fixed three-node topology using only
`.lunet/v0.8.0/lunet-run`, then drives it through the TCP NDJSON client API for
30 seconds. The std-Rust harness starts `DC1-0001`, `DC2-0001`, and
`DC3-0001` with durable logical IDs 10001, 20001, and 30001, respectively.
They GET before SET, renew their 1,000 ms lease every 900 ms, and contend for
sentinel lock `0x0DDBA11`. Every three seconds it stops the observed holder,
waits 1.1 seconds, starts the next same-DC singleton, and verifies takeover.
It logs acquisitions, renewals, stops, and failovers but not ordinary polling.
The run exits nonzero on a conflicting holder observation or if a replacement
does not take over within five seconds. Logs and node process state are kept in
`.tmp/lease-failover-*`; the harness always terminates the node processes. Set
`SIM_DURATION` to a value from 1 to 30 for a shorter run.

## Docker / Colima demonstration

`make docker-simulation` first assembles a disposable Cargo-vendored context,
then invokes a conventional multi-stage `docker build`. This matches the
upstream `vrr-core` Docker strategy and deliberately uses neither BuildKit
features nor source/bind mounts. The image downloads and SHA-256 verifies its
own Linux Lunet v0.8.0 runtime, compiles the native adapter for the Docker
daemon's architecture, and contains Cyan output. The target builds and runs
only for that native daemon platform, then verifies that the image matches it;
it does not request cross-platform emulation.

The command creates an isolated Docker bridge with fixed internal addresses
for n1/n2/n3, one named Docker volume per container for its recovery nonce,
and three host TCP ports 31101–31103. The containers are stable throughout;
the standard-library host simulator uses those ports and is the only dynamic
participant. It captures simulator and container logs under
`.tmp/docker-lease-failover-*`, has bounded Docker calls, and removes the
containers, bridge, and demonstration volumes on exit.

`make docs` runs the `uv`-managed Zensical script at `docs/docs` and writes
generated HTML under `docs/site/`.

## Relevant files

| File | Responsibility |
|---|---|
| `ext/advisory_lock/src/locks.rs` | JSON lock protocol and lock state machine |
| `ext/advisory_lock/src/ffi.rs` | vrr-core adapter, C ABI, tick clock, recovery nonce |
| `src/advisory_lock.tl` | Teal wrapper and owned output draining |
| `src/server.tl` | TCP NDJSON server, UDP peers, and leader forwarding |
| `tests/lunet_smoke.sh` | three-process runtime smoke test |
| `tools/lease_failover_sim.rs` | std-Rust live TCP lease-failover simulator |
