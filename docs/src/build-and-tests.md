# Build and tests

Install the project tools into their local locations:

```console
make init       # mise tools, then Cyan, Cerulean, tested, and tl in .rocks/
make hooks      # enable the formatting pre-commit hook once after clone
```

The native adapter requires Rust 1.85 or newer. Its ordering core is the
vendored `ext/uvrr-core` git submodule, checked out on upstream main at
tag `v0.7.4` (commit `e5b0a79`, released 2026-09-19). The adapter manifest
pins the upstream tag and its `[patch]` section builds the dependency from
the submodule, so `cargo` fetches no git dependencies: local builds, the
vendored Docker context, and CI all compile the submodule source directly.

## Teal tooling

Run orchestration and generic tooling are typed Teal (typed Lua on LuaJIT)
alongside the shipped source. There is no shell orchestration: shell has
no types, no data structures, and fails silently — the class of defect
where a gate greps `$RUN/logs/n1.log` while the appender writes dated
files (`n1.<date>.log`) and the runner runs blind.

- Entry points are executable `.lua` files with `#!/usr/bin/env -S
  luajit` shebangs (`tools/smoke.lua`, `tools/snapshot_run.lua`,
  `tools/dangling.lua`, `examples/lease-sequencer/run.lua`, ...). Every
  module they call is a typed `.tl` library under `tools/lib/` behind a
  `record` contract; `tl check` verifies the implementations against the
  contracts.
- Each entry `dofile`s `tools/bootstrap.lua`: the prelude resolves the
  project-local `.rocks/` tree and the LuaRocks 5.1 paths (skipped when
  the environment already provides them), activates `tl.loader()`, and
  makes the sibling libraries requirable from any cwd. Paths resolve
  from `arg[0]`, never the cwd, so every target works under
  `env -u LUA_PATH -u LUA_CPATH`.
- `make init` installs `tl` into `.rocks/` for the LuaJIT 5.1 ABI
  (`--lua-version=5.1` throughout; a 5.5-tree `tl` is invisible under
  LuaJIT), idempotently. `make check` runs `tl check` over `tools/lib`.
- Processes are typed records (`Process { pid, name, argv, state,
  exit_code, signal }`) spawned through fork/exec with file redirects;
  log gates read the logs DIRECTORY and select files by node-name
  prefix — no globs, no exact dated names. On LuaJIT 5.1 `os.execute`
  returns a number; the tooling normalizes both shapes through one
  typed helper.
- Python stays for data mining (percentiles, plots, extraction of
  analysis series) and is not used for orchestration.

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

`make sanity` is MANDATORY before every cloud test run. We test
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
3. runs the fastbuild sanity payload in colima:
   `cargo check` for BOTH linux triples (`aarch64-unknown-linux-gnu`
   natively, `x86_64-unknown-linux-gnu` cross-built natively by rustc —
   never an emulated build target, never a qemu `--platform`), the
   adapter cdylib and the rig crate (the prod and flight-recorder
   shapes) and paxe-core (the prod and kat shapes), against the classic
   manifests-first deps layer cache;
4. prints the verdict with the HEAD commit hash. There is no binfmt
   registration anywhere in this flow, and no BuildKit
   (`DOCKER_BUILDKIT=0` throughout). The mechanics — the
   manifests-first deps layer, the cross gcc linker kit, the AOF zig
   target override — are in [build-and-release.md](build-and-release.md).

## The release gate

The softball run is MANDATORY before any release: a green softball
recording from the cloud rig — clean node restart, leader restart with
takeover, crash-stop reincarnation, and a complete teardown record —
precedes every release. Its step 0 is the build-confirmation gate:
`make sanity` (above) — the tree committed at HEAD and the colima
cross-check of that commit passing, so no release is cut from a
"only builds on my laptop" commit. The method and its acceptance gates
are in [the softball run](softball-run.md).

The RELEASE dual-arch image gate is `make build-proof`: the two linux
architecture images built from the committed tree, each carrying BOTH
binaries (prod and flight-recorder), with the native cross mechanics —
the x86 binaries are built natively by rustc in the aarch64 container,
and the amd64 image is assembled COPY-only on the amd64 base image
(no amd64 code runs at build time; no qemu anywhere). The full flow,
including the ghcr.io push, is `make release-images TAG=vX.Y.Z`; the
mechanics are in [build-and-release.md](build-and-release.md).

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
make bench           # the on-the-bench harness (see bench-harness.md)
make docker-build    # plain Docker image, including Linux Lunet v0.10.0
make docker-simulation # the same 30s simulation against a stable Docker cluster
make sanity          # the build-confirmation gate: clean commit + colima cross-check both triples
make build-proof     # the RELEASE dual-arch image gate (prod + flight binaries, no qemu)
make release-images TAG=vX.Y.Z # the full release image flow, through the ghcr.io push
make docs            # render the Zensical site
```

`make lunet-runtime` downloads the host-specific official Lunet `v0.10.0`
archive, verifies its SHA-256, and extracts it into `.lunet/v0.10.0/`. The
service and smoke test always use `.lunet/v0.10.0/lunet-run`; they do not use a
runtime from `PATH`. The shipped LuaCATS/Teal runtime documentation is at
`.lunet/v0.10.0/types/`.

`make smoke` starts three local nodes from a four-line deployment
descriptor, connects through a nonleader, and covers acquire, GET,
contention, RELEASE, reacquisition, and expiry takeover. Finally, while a client keeps acquiring,
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

## Run-state snapshots and the shutdown consistency check

Every trace capture — crash or no crash — snapshots the run's on-disk
state. Distributed state is the thing that gets mishandled, so the
capture copies aside every artifact class the run produced, per node:

- the superblock files (`<state>.superblock`) and the single-file
  `<incarnation> <state>` markers (`n1.state`, `n1.nonce`);
- the membership sidecars (`<state>.membership`) next to the markers;
- the flight-recorder tapes (`flight-*.jsonl`, rolled files included);
- the AOF telemetry trees (`--aof-dir` series, `*.aof` / `ev-*.bin` +
  metafiles);
- the regular logs (`*.log`, `*.nohup`, `*.out`, `*.err`) — the
  happens-before/happens-after evidence for shutdown correctness;
- the anchors file (`anchors*`).

`tools/snapshot_run.lua RUN_DIR [--out ARCHIVE.tar.gz]` walks the run
directory and writes a gzip tar archive holding all of it, paths
preserved, with a `SNAPSHOT_MANIFEST.txt` naming each captured file and
its class. A crashed run may be missing any piece: missing classes and
unreadable files are recorded as warnings in the manifest and on stderr,
never fatal. The archive lands next to the run directory by default
(`<RUN_DIR>.snapshot-<UTC timestamp>.tar.gz`).

The default archive is the form the check reads. The check tool is
`skaffold_flight_tape --check-shutdown RUN_DIR_OR_ARCHIVE`, and it reads
a raw run directory or a snapshot archive transparently (an archive is
extracted to a temporary directory first, so the superblock classifier
sees real files). It cross-checks the logs against the markers:

- every `drained and flushed` stop record in a node's logs demands a
  final marker showing flushed (or stopped) at that node's identity:
  the superblock quorum classification and the single-file marker must
  agree in state and incarnation. A log claiming a clean flush whose
  marker does not show it is INCONSISTENCY, reported with the log file
  and line, the record's timestamp, and the copy states (the
  single-file marker line and the superblock `(state, incarnation)`);
- the reverse holds: a final marker showing flushed/stopped with no
  stop record in any of the node's logs is INCONSISTENCY;
- the stop path's records are ordered (stop-begin, drain, flushed) and
  inversions are flagged: a stop record out of sequence, and any log
  activity after the persist order completed — work the runtime did
  after the stop path wrote its final marker;
- the superblock's write time (from the filesystem, or the tar entry's
  mtime for archives) is ordered against the stop records: a marker
  written before the stop began is an inversion.

The report exits 0 when every node is consistent, 1 when any
INCONSISTENCY or inversion was found, 2 on usage or input errors.
Earlier stop cycles inside one run directory are not contradicted by a
final marker a later life rewrote: only the last stop cycle of a node
is cross-checked against the final marker; a later boot (clean
continue or crashed bump) after a flushed record supersedes it.

Old on-disk state is removed before a new run only after a successful
snapshot. Every wipe step (`tools/smoke.lua`, the
`examples/lease-sequencer` run entries) calls the snapshot tool on the
directory it is about to remove and wipes only on its success; a failed
snapshot refuses the wipe loudly and leaves the state in place.

## Relevant files

| File | Responsibility |
|---|---|
| `ext/advisory_lock/src/locks.rs` | JSON lock protocol and lock state machine |
| `ext/advisory_lock/src/ffi.rs` | uvrr-core adapter, C ABI, tick clock, incarnation marker |
| `src/advisory_lock.tl` | Teal wrapper and owned output draining |
| `src/cluster_config.tl` | JSONL deployment descriptor: parse, encode, genesis succession |
| `src/admin.tl` | Admin verb decode, ADMIN peer payload, acknowledgments, dedup cache |
| `src/server.tl` | TCP NDJSON server, UDP peers, leader forwarding, reconfiguration drives |
| `tools/smoke.lua` | the three-process runtime smoke test with restart and live-reconfiguration stages |
| `tools/snapshot_run.lua` | the run-state snapshot: all six artifact classes into a dated gzip tar |
| `tools/test_snapshot_run.lua` | the snapshot tool's acceptance: six classes, tolerance, archive-equals-raw |
| `examples/lease-sequencer/src/shutdown_check.rs` | the shutdown-restart consistency check behind `--check-shutdown` |
| `tools/lease_failover_sim.rs` | std-Rust live TCP lease-failover simulator |
| `docker/Dockerfile.fastbuild` | the colima build stages: deps-layer cache, the sanity check payload, the release artifacts, the amd64 rootfs staging |
| `docker/Dockerfile.release` | the release image (both cdylib shapes + both node binaries + the pinned runtime) |
| `tools/release_images.sh` | the release flow: gate, flight builds, dual-arch image assembly, ghcr.io push |
| `docs/src/softball-run.md` | the mandatory pre-release softball run: profile, phases, gates, checklist |
