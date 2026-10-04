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
flow, NOMINATE leader assignment, the fence, reincarnation seating, the
drop discipline (every refusal named), the marker emission gate, the
flavoured-timeout transitions — each as a force-fed exchange.

### The seam

The layer has one harness: `ext/advisory_lock/tests/seam/mod.rs`, a
message-fed cluster of adapter `Node`s. It is included by path into
every test binary that drives it, the same shape the compliance
executor's own module has (`tests/compliance/mod.rs`).

A scenario is a list of calls on it:

| Call | What it is |
|---|---|
| `Seam::boot(members, primary_timeout)` | the boot: one `Node` per roster member over a fresh marker store |
| `Seam::advance(ticks)`, `Seam::clock()` | the logical clock, a parameter; nothing sleeps |
| `Seam::tick(id)`, `Seam::tick_all()`, `Seam::leader_timeout(id)` | the timer events, driven at the named tick |
| `Seam::feed(&Inbound)`, `Seam::feed_all(&[Inbound])`, `Seam::gossip(wire)` | the force-fed exchange: the inbound frames, by identity pair and by hex wire |
| `Seam::deliver()` | the queued wire, delivered to quiet |
| `Seam::propose(id, payload)`, `Seam::reconfigure(id, op)`, `Seam::request(id, json)` | the application-boundary drives |
| `Seam::force_view(id, era, view)`, `Seam::announce(id)`, `Seam::note_timeout(id, …)` | the host-forced transitions |
| `Seam::shutdown(id)` | the stop contract |
| `Seam::crash(id)` | a drop with no stop contract taken |
| `Seam::restart(id, Restart::Clean \| Restart::Crashed)` | a reopen over the same markers |
| `Seam::join(id)` | a fresh identity over its own marker store |
| `Seam::settle()` | the timer sweep, until the cluster is quiet and every seat `Normal` |
| `Seam::emissions()` / `mark()` / `emissions_since()` | the frames the wire carried |
| `Seam::refusals()` / `refusals_since()` | the refusals, each named by node, call and code |
| `Seam::state(id)` / `states()` / `identities()` / `is_live(id)` | the observable state |
| `Seam::assert_emissions(named)` / `assert_emissions_since(mark, named)` | the exact named multiset |
| `Seam::assert_refusals(named)` / `assert_refusals_since(mark, named)` | the exact named refusal set |
| `Seam::assert_state(&Expect)` / `expectation_since(mark)` | the post state, in the compliance corpus's own `post` grammar |

Its vocabulary is the compliance executor's, not a second one: the
`system:counter` identity pairs, the lower-case hex wire bytes, the
`<Marker>@<system>:<counter>` marker rounds, the delivery record, the
post-state record and the refusal-code names all come from
`tests/compliance/mod.rs` and `src/advisory_lock.tl`. A scenario written
against the seam and a corpus case written against the executor name the
same things the same way.

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
  measures a stale artifact. The rule itself is one shared check
  ([`tools/lib/stale_binary.tl`](../tools/lib/stale_binary.tl)), and
  every lane calls it before it boots anything; see
  [the stale-binary rule](#the-stale-binary-rule).
- **The drift-window law**: when not aggressively stressed, the
  (era, view, committed slot) frontiers across all nodes must
  converge within a single-digit window. A lane terminates early on
  frontier lag beyond the window; a failure to converge without
  stress is an error of omission. Safety bugs stop the world; every
  liveliness finding is filed as one issue against local code, citing
  the commit and tag that carries the failing scenario.

## The stale-binary rule

A lane never measures a stale artifact. The check is one library and
one comparison; a lane names the artifact it is about to boot and the
comparison decides. There is no per-lane copy of the rule and no lane
that boots first and asks afterwards.

Every lane that boots a native artifact runs the check first:

| Lane | Entry point | Artifact |
|---|---|---|
| the runtime smoke | `tools/smoke.lua` | the `advisory_lock` cdylib |
| the failover simulation | `make simulation` | the `advisory_lock` cdylib |
| the Docker image lane | `make docker-simulation` | the image |
| the softball-2 crash/partition lane | `tools/softball2_run.lua` | the `lease-sequencer` ladder binaries |
| the polite-2 maintenance lane | `examples/lease-sequencer/run-polite2.lua` | the `lease-sequencer` ladder binaries |
| the rig lanes (stability, takeover, acceptance) | `examples/lease-sequencer/run.lua`, `run-takeover.lua`, `run-acceptance.lua`, `check.lua` | the `lease-sequencer` node binaries |
| the rig standby lane | `examples/lease-sequencer/check-standby.lua` | the node binaries and the console feed |
| the snapshot acceptance | `tools/test_snapshot_run.lua` | the flight-tape reader and the marker admin tool |
| the E1/E2 experiments | `examples/lease-sequencer/experiment.sh` | the `lease-sequencer` ladder binaries |
| the Lock Admin console | `console/tooling/console.lua` | the console feed |

The lanes a Makefile target owns run `tools/check_stale.lua`, which is
that one call and adds no logic to it.

A lane that rebuilds the artifact it boots asks AFTER the rebuild, not
before: the smoke lane builds the cdylib itself, and asking first would
refuse artifacts that rebuild was about to replace. Asking after it can
only catch an artifact that survived a rebuild without matching its
source — which is the whole of what there is left to catch.

### What is compared

Two legs, both required.

**Identity.** The build stamps its own identity into the artifact
(`ext/advisory_lock/build.rs` compiles `LUNET_INFO_VERSION`,
`LUNET_INFO_SHA`, `LUNET_INFO_DIRTY` and `LUNET_INFO_FEATURES` from the
git facts at build time), and
`lunet_lock_version_properties` is the read-only console that hands
them back — so the question is asked of the artifact itself and never
of a side file that could disagree with it. The tree's `HEAD` names the
commit it is. Two stamps naming the same commit pass; two naming no
commit at all pass (a release tarball and a Docker build context carry
no `.git`, so neither side can name one and neither can be behind the
other); a stamp naming a different commit, or naming a commit where the
tree names none, refuses. A stamp whose console reported `sha=unknown`
names no commit either and is treated as though the console were absent.

**Age.** No source file that feeds the artifact is newer than the
artifact. The source set is every tracked and untracked-not-ignored
file under the crate's own roots — its own crate, its path
dependencies, the vendored cargo config and the toolchain pin — so
build outputs and tool caches, all gitignored, can never raise a false
alarm. Age is a conservative superset of cargo's own mtime fingerprint:
anything cargo would rebuild, age calls stale, and a source file that
has not moved since the build is the only way age passes.

The two legs are not redundant. A commit moves no file inside a crate,
so an artifact can be the newest file on disk and still have been built
from an older commit — the age leg passes and only identity catches it.
An edit to a source file always post-dates the build that preceded it,
so the age leg catches what the stamp cannot see.

### The dirty tree

A build stamped `dirty=true` names its commit but not its content, so
identity cannot certify it and there is no third fact to ask for: the
stamp is one bool and one hash. The gates stamp dirty by design and run
on dirty trees constantly, so refusing every dirty build would make the
rule refuse every local run and prove nothing.

A dirty stamp is therefore annotated, never refused, and the age leg
stands behind it. What a dirty build actually needs is the one question
the stamp cannot answer — has a source file moved since the build — and
age answers it: a dirty build whose source has moved is refused, and a
dirty build whose source has not is passed and logged as the dirty build
of that commit that it is. Every pass names the identity the artifact
carries, so each lane log records which artifact the measurement is
about.

### The refusal

A refusal stops the lane before anything is booted. It names the
artifact and the identity the artifact carries, the tree's `HEAD`, the
leg that failed, the two moments compared, and the command that
rebuilds:

```
STALE BINARY: refusing to boot the advisory_lock cdylib
  artifact  /…/ext/advisory_lock/target/release/liblunet_advisory_lock.dylib
  stamp     sha=395dd93ebd9f dirty=true feature_shape=production version=unknown
  tree      HEAD=528eeb419a02
  reason    the artifact was built from commit 395dd93ebd9f and the tree is at 528eeb419a02
  newest source ext/advisory_lock/tests/skaffold_probe_test.rs at 2026-10-04T03:20:04Z.258014113
            the advisory_lock cdylib at 2026-10-04T03:44:55Z.307170000
  rebuild   cargo build --release --manifest-path ext/advisory_lock/Cargo.toml
```

The check refuses; it does not rebuild. A lane must not mutate the tree
it is measuring, and the refusal names the keystroke that fixes it.

### The Docker lane's artifact

The Docker lane's artifact is the image, not a file on the host, and its
identity leg cannot reach inside it: the prepared context is the
committed tree with `.git` excluded, so the in-image build of the cdylib
stamps `sha=unknown`. The image's own creation time is the host-side
fact that dates it, and the age leg compares that against the source
tree. Every refusal from that lane says so in full.

### What the rule does not cover

A packaged release archive carries no `.git` and no source tree it
claims to be built from: `make package-verify` verifies the shipped
artifact, not this tree's build of it, and the rule has nothing to
compare. The gate targets (`make bench`, the compliance shapes) build
the artifact they boot in the same recipe, immediately before booting
it, so there is no window for a stale artifact to stand in.

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
| `ext/advisory_lock/tests/seam/mod.rs` | 1 | the message-fed seam: the lifecycle calls, the logical clock, the exact emission multiset |
| `ext/advisory_lock/tests/seam_test.rs` | 1 | the seam's own laws: determinism, the named multiset, the source-level seam-only gate |
| `examples/lease-sequencer/tests/*.rs` | 1 + 2 | the message-fed expectations and the networked ladders |
| `ext/advisory_lock/tests/*_roundtrip_test.rs` | boundary | the wire, marker, and AOF roundtrips at exact normative lengths |
| `tests/` | 3 | the Teal learning tests and the pure-module suites |
| `tools/lib/stale_binary.tl` | 3 | the stale-binary rule: the one check every lane calls before it boots |
| `tests/stale_binary_test.tl` | 3 | the rule's decision table, over measured facts rather than a real artifact |
| `tools/smoke.lua` | 3 | the three-process runtime smoke with restart and live reconfiguration |
| `tools/lib/{softball2,polite2}.tl` | 3 | the scenario lanes: hostile stress and politeness, drift-window-gated |

## Relevant files

| File | Responsibility |
|---|---|
| `ext/advisory_lock/src/ffi.rs` | the adapter: the `Node`, the boot gate, the emission gate, the drain-point stops |
| `ext/advisory_lock/tests/compliance/mod.rs` | the compliance executor: the corpus's grammar, the naming vocabulary the seam shares |
| `ext/advisory_lock/tests/seam/mod.rs` | the message-fed seam of the in-memory layer |
| `ext/advisory_lock/src/marker_store.rs` | the marker pair mechanics, `GateStore`, the projection |
| `ext/advisory_lock/src/aof.rs` | the AOF: append, drain, roll, the erasure-block trap, torn-tail truncation |
| `ext/uvrr-core/src/timeout.rs` | the flavoured-timeout model: the matcher, the `Sorry{runbook}` verdicts |
| `ext/uvrr-core/src/bin/timeout-policy/` | the timeout-policy tool: the opinion for a (state, timeout) pair |
| `examples/lease-sequencer/src/main.rs` | the host loop: the drives, the signals, the stop contract |
| `examples/lease-sequencer/tests/shutdown_check.rs` | the shutdown-restart consistency check |
| `ext/advisory_lock/src/info.rs` | the read-only information console: the facts the build stamped, handed back on request |
| `tools/lib/stale_binary.tl` | the stale-binary rule: identity and age, one refusal, one rebuild command |
| `tools/check_stale.lua` | the same check as a command, for the lanes a Makefile target boots |
| `tools/dangling.lua` | the orphan-process sweep before any outer commit |
