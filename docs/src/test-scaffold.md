# The test scaffold

The scaffold is three layers, ordered by determinism. Every layer is
green on its own before the next may be built: the ratchet is green,
commit, next. The compliance suite (73 cases over the real adapter) is
the ever-green gate at every layer — it never regresses.

1. **The in-memory message-fed layer** — an isolated node fed messages
   in memory: the leader sees X, does Y; the followers see and do Z.
   No IO, force-fed expectations.
2. **The pure-Rust networked harness** — real nodes, real sockets, real
   disk, driven step by step through the reboot and membership ladders.
3. **The run lanes** — the Lua orchestration (the smoke, the
   softball/polite scenario lanes) over packaged binaries.

The layering is deterministic by construction: a scenario is authored
at layer 1, and the same scenario at layer 2 must land the same result.
Nothing existing is duplicated or abandoned.

## The in-memory message-fed layer

The node under test is the adapter's `Node` (or the `lunet_lock` FFI
surface) driven by the test: outbound frames are captured in memory,
inbound frames are fed by the test. Time is the executor's logical
clock. The storage is the real marker store and AOF over a scratch
directory — the only thing that is not real is the network and the
wall clock.

The layer's guarantees:

- No process spawn, no fork, no sockets, no ports. The whole suite
  runs under `cargo test` without flake-by-construction: there is no
  scheduler to lose a race against.
- Every delivery is byte-exact named (the compliance executor's
  discipline): emissions beyond the named multiset fail the test.
- Boot, shutdown, crash (drop without the stop contract) and restart
  (reopen over the same markers) are each a plain function call — the
  obligations are exercised directly instead of being inferred from a
  process's outside.

The layer is the home of the protocol-state expectations: view-change
flow, NOMINATE leader assignment, the fence, reincarnation seating,
the drop discipline (every refusal named), the marker emission gate,
the flavoured-timeout transitions — each as a force-fed exchange.

## The pure-Rust networked harness

The harness boots real nodes (the `lease-node` binary in-process or
the release binary under `docker`) over loopback, with the real AOF
and marker stores on disk. It is the layer where the disk-IO, the
signal contract, and the process lifecycle meet the protocol.

The harness's disciplines:

- **Step, don't flood**: one client message before each stage, one
  after; slots advance; the committed values are the same at every
  node that needs them for the scenario; the frontiers all advance.
- **No era or view change where the scenario does not demand one**: a
  follower restart under a majority must not move the ballot.
- **The reboot ladders**, in build order:
  1. the 3-node boot with 2 running (a majority): the follower clean
     shutdown, restart — the slot advances, the ballot does not;
  2. the leader power cycle: a signal turns the leader off for
     security patches — it issues `StartViewChange`, broadcasts, and
     clean shuts down; the backup accepts and becomes leader (2 of 3
     nodes running; the third exists logically, not run); the leader
     must be seen to move; the leader can be clean shut down and
     restarted; the same rules between each action;
  3. the expansion ladder 3→5 and the contraction ladder 5→3;
  4. the crash + reincarnation ladders: the crashed node returns
     under its bumped identity, announces, and is walked back to
     voting weight by the leader's forced sequence.
- **The stale-binary rule**: the harness rebuilds the binary it boots
  or refuses a binary older than the source tree. A lane never
  measures a stale artifact.
- **The drift-window law**: when not aggressively stressed, the
  (era, view, committed slot) frontiers across all nodes must
  converge within a single-digit window. A lane terminates early on
  frontier lag beyond the window; a failure to converge without
  stress is an error of omission. Safety bugs stop the world; every
  liveliness finding is filed as one issue against local code, citing
  the commit and tag that carries the failing scenario.

### Docker discipline

The docker ladders run on colima aarch64 from a clean image: no
buildkit, no volume mounts, no cross-arch emulation. The image build
and the ladder run are separate steps; the image is rebuilt from the
pinned runtime when the tree moves.

## The observability contract

Every shutdown and startup path is proven, not assumed: the
zero-cost `maybe!`/trace discipline logs every path the node takes
through boot, drain, flush, and stop — a path that cannot show itself
in the trace is a defect. The flight recorder's keep-2 rotation is
respected; the traces land on the tape.

The logs are JSON lines. First-class observables: **view** and **era**
on every line that concerns the protocol; the slot frontier is
written to the tape and logged periodically — every heartbeat commit
is logged. The NOMINATE computations, the cluster gossip, the
timeouts, and the request/response streams are all transparent in the
log. Telemetry output is downstream of the recorder, never load-bearing.
The console's telemetry panel reads this series over the admin API
(`/api/v1/telemetry/log`, served by the loopback mock and the
aof-console-bridge with the same shape) — the named events plus the
tape's slot-frontier records ([the architecture](architecture.md)).

## The timeout-policy audit

The flavoured-timeout model (`ext/uvrr-core/src/timeout.rs`) maps
every (state, timeout) pair to an opinion; the upstream
`timeout-policy` tool prints the map for a pair. The audit pins the
matcher's table in our tree as living documentation and asserts — as a
test — that the host's actual drives match the matcher's opinion for
every pair the host acts on. The `Sorry{runbook}` verdicts
(`StoppingNotFlushed`: free disk space and retry the flush, or a hard
kill) are surfaced to the operator wherever `stop` fails.

## The suite layout

| Path | Layer | Responsibility |
|---|---|---|
| `ext/advisory_lock/tests/compliance.rs` | gate | the uVRR compliance suite: 73 cases over the real adapter |
| `ext/advisory_lock/src/*.rs` inline tests | 1 | the disk-IO units: aof, journal, recovery flush, marker store |
| `examples/lease-sequencer/tests/*.rs` | 1 + 2 | the message-fed expectations and the networked ladders |
| `ext/advisory_lock/tests/*_roundtrip_test.rs` | boundary | the wire, marker, and AOF roundtrips at exact normative lengths |
| `tests/` | 3 | the Teal learning tests and the pure-module suites |
| `tools/smoke.lua` | 3 | the three-process runtime smoke with restart and live reconfiguration |
| `tools/lib/{softball2,polite2}.tl` | 3 | the scenario lanes: hostile stress and politeness, drift-window-gated |

## Relevant files

| File | Responsibility |
|---|---|
| `ext/advisory_lock/src/ffi.rs` | the adapter: the `Node`, the boot gate, the emission gate, the drain-point stops |
| `ext/advisory_lock/src/marker_store.rs` | the marker pair mechanics, `GateStore`, the projection |
| `ext/advisory_lock/src/aof.rs` | the AOF: append, drain, roll, the erasure-block trap, torn-tail truncation |
| `ext/uvrr-core/src/timeout.rs` | the flavoured-timeout model: the matcher, the `Sorry{runbook}` verdicts |
| `ext/uvrr-core/src/bin/timeout-policy/` | the timeout-policy tool: the opinion for a (state, timeout) pair |
| `examples/lease-sequencer/src/main.rs` | the host loop: the drives, the signals, the stop contract |
| `examples/lease-sequencer/tests/shutdown_check.rs` | the shutdown-restart consistency check |
| `tools/dangling.lua` | the orphan-process sweep before any outer commit |
