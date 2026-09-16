# Build and tests

Install the project tools into their local locations:

```console
make init       # mise tools, then Cyan, Cerulean, and tested in .rocks/
make hooks      # enable the formatting pre-commit hook once after clone
```

The native adapter requires Rust 1.85 or newer. Its ordering core is the
vendored `ext/uvrr-core` git submodule, branch `lunet-locks/learner-era-fold`:
upstream commit `0fc6380` plus the learner-acquisition and
fence-under-load patch commits. The adapter manifest pins the upstream
revision and its `[patch]` section builds the dependency from the
submodule, so `cargo` fetches no git dependencies: local builds, the
vendored Docker context, and CI all compile the submodule source directly.

## The Flight Recorder build

The Flight Recorder is a cargo feature, OFF by default; the prod path is
unchanged without it. The recording build is an optimized release binary
of the same tree — the panic discipline is unchanged — and it MUST come
from a clean commit (the build fails on a dirty tree;
`FLIGHT_RECORDER_ALLOW_DIRTY=1` overrides, stamping the recording dirty):

```console
cargo build --release --features flight-recorder                  # the cdylib shape
cargo build --release --features flight-recorder -p lease-sequencer   # the rig binary
```

A node opts in per node via `LUNET_FLIGHT_RECORDER_DIR`; the reader is
`skaffold_flight_tape`. See [the Flight Recorder](flight-recorder.md).

## The build-confirmation gate

`make build-proof` is MANDATORY before every cloud test run. We test
head-of-push — the cluster never runs through CI, so no pipeline ever
stands between a commit and the rig — and the gate is what stops a run
from shipping a "only builds on my laptop" commit. It is a build
confirmation, not a deployment and not testing: nothing is deployed and
nothing from the image is run. The build IS the proof.

The target, in order:

1. asserts the tree is clean (`git status --porcelain` empty) and fails
   loudly with the instruction to commit first — the run ships HEAD,
   never the working tree;
2. asserts a Docker daemon is reachable (on macOS: `colima start`,
   `docker context use colima`);
3. runs the local build (`make build`);
4. assembles the disposable vendored context and builds the Dockerfile
   for **x86 (linux/amd64)**, whatever the host architecture is;
5. verifies the built image is `linux/amd64` — a non-x86 image is not a
   build proof for the cloud rig — and prints the verdict with the HEAD
   commit hash.

The macOS mechanics: colima runs an aarch64 VM, so an x86 build runs the
`RUN` steps under the registered emulation. One-time (and again after
every `colima start`, the VM kernel does not keep it):

```console
docker run --privileged --rm tonistiigi/binfmt --install amd64
```

The builder is the default (BuildKit) builder. The Dockerfile itself
stays compatible with the legacy builder — no BuildKit-only syntax — but
on Docker Engine 29.x the legacy builder cannot target linux/amd64 at
all: it silently builds the daemon's own architecture, which is exactly
the failure the gate's image-architecture assertion exists to catch.

The vendored context carries the Rust dependency sources, the
`ext/uvrr-core` submodule, and the `ext/lunet-locks-aof` subcrate (Rust
wrapper and Zig source). The image installs the pinned Zig 0.14.1
x86_64-linux toolchain (SHA-256 verified) because the adapter's
lifecycle marker compiles the AOF superblock through it in-image, and
downloads and SHA-256 verifies its own Lunet v0.10.0 runtime.

## The release gate

The softball run is MANDATORY before any release: a green softball
recording from the cloud rig — clean node restart, leader restart with
takeover, crash-stop reincarnation, and a complete teardown record —
precedes every release. Its step 0 is the build-confirmation gate: the
tree committed at HEAD and a Docker x86 (linux/amd64) image build of that
commit succeeding, so no release is cut from a "only builds on my laptop"
commit. The method and its acceptance gates are in
[the softball run](softball-run.md).

## Commands

```console
make fmt             # format Teal with Cerulean
make lint            # reject unformatted Teal
make build           # Rust checks/tests/release cdylib, then Cyan output
make check           # build plus all Teal type checks
make test            # check plus tested
make lunet-runtime   # fetch and verify Lunet v0.10.0 locally
make smoke           # build and run the three-process service smoke test
make simulation      # 30s TCP-NDJSON three-datacenter lease failover demo
make simulation-test # focused std-Rust simulator unit tests
make docker-build    # plain Docker image, including Linux Lunet v0.10.0
make docker-simulation # the same 30s simulation against a stable Docker cluster
make build-proof     # the build-confirmation gate: clean commit + x86 image build
make docs            # render the Zensical site
```

`make lunet-runtime` downloads the host-specific official Lunet `v0.10.0`
archive, verifies its SHA-256, and extracts it into `.lunet/v0.10.0/`. The
service and smoke test always use `.lunet/v0.10.0/lunet-run`; they do not use a
runtime from `PATH`. The shipped LuaCATS/Teal runtime documentation is at
`.lunet/v0.10.0/types/`.

`make smoke` starts three local nodes from a four-line deployment
descriptor, connects through a nonleader, and covers acquire, GET,
contention, RELEASE, reacquisition, and expiry takeover. It then kills one
replica and restarts it against the live quorum — a dirty restart whose
durable state file classifies the boot, so the replica reincarnates: the
identity bumps, the restart announces the `(old, new)` pair, and the leader
drives the two-era resurrection. Finally, while a client keeps acquiring,
renewing, releasing, and reading a lock through the nonleader without
interruption, a fourth replica boots the joiner way and the admin verbs
drive a full membership lifecycle: the replica joins at weight 0, is
promoted by its committed `Increment`, and departs through `Decrement`
then `Leave`, each verb acknowledged only after its establishing era has
committed. The stage reports the per-transition stream latencies and
timeline as measured. Temporary logs and process state live under
`.tmp/`; the downloaded runtime does not.

`make simulation` starts the same fixed three-node topology using only
`.lunet/v0.10.0/lunet-run`, then drives it through the TCP NDJSON client API for
30 seconds. The std-Rust harness starts `DC1-0001`, `DC2-0001`, and
`DC3-0001` with durable client ids 10001, 20001, and 30001, respectively.
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
then invokes a conventional multi-stage `docker build`. The prepared context
carries the vendored dependency sources, the `ext/uvrr-core` submodule
source at the relative position the manifest's `[patch]` section names,
and the `ext/lunet-locks-aof` subcrate source, so the image build fetches
no dependency sources over the network and uses neither BuildKit
features nor source/bind mounts; the image installs the pinned Zig
0.14.1 toolchain and downloads its own SHA-256-verified Linux Lunet
v0.10.0 runtime, compiles the native adapter for the Docker daemon's
architecture, and contains Cyan output. The target builds and runs
only for that native daemon platform, then verifies that the image matches it;
it does not request cross-platform emulation.

The command creates an isolated Docker bridge with fixed internal addresses
for n1/n2/n3, one named Docker volume per container for its durable state
file, and three host TCP ports 31101–31103. The containers are stable
throughout; the standard-library host simulator uses those ports and is the
only dynamic participant. It captures simulator and container logs under
`.tmp/docker-lease-failover-*`, has bounded Docker calls, and removes the
containers, bridge, and demonstration volumes on exit.

`make docs` runs the `uv`-managed Zensical script at `docs/docs` and writes
generated HTML under `docs/site/`.

## Relevant files

| File | Responsibility |
|---|---|
| `ext/advisory_lock/src/locks.rs` | JSON lock protocol and lock state machine |
| `ext/advisory_lock/src/ffi.rs` | uvrr-core adapter, C ABI, tick clock, incarnation marker |
| `src/advisory_lock.tl` | Teal wrapper and owned output draining |
| `src/cluster_config.tl` | JSONL deployment descriptor: parse, encode, genesis succession |
| `src/admin.tl` | Admin verb decode, ADMIN peer payload, acknowledgments, dedup cache |
| `src/server.tl` | TCP NDJSON server, UDP peers, leader forwarding, reconfiguration drives |
| `tests/lunet_smoke.sh` | three-process runtime smoke test with restart and live-reconfiguration stages |
| `tools/lease_failover_sim.rs` | std-Rust live TCP lease-failover simulator |
| `docs/src/softball-run.md` | the mandatory pre-release softball run: profile, phases, gates, checklist |
