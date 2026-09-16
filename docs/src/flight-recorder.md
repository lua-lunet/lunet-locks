# The Flight Recorder: the per-node internal trace

The Flight Recorder is a debug-level trace tool: one INTERNAL tape per
node, recording everything the node does during a test run — including the
internal private events the lock telemetry capture file never sees. The
node's crash plus its flight recording is the black box: no guessing is
required to see what tripped what. It is NOT intended for prod release and
carries NO long-term readability promise. Its disk use is bounded: one
node's tape series never exceeds the operator's 200 MiB cap (below).

## The two uses, two files, two planes

The **lock telemetry capture file** is the regular path's AOF — the
Zig TigerBeetle-format `{epoch}.aof` series on the separate non-voting
telemetry nodes ([standby telemetry](telemetry-aof.md)) — recording the
public, wire-visible events the console observer reads. Those nodes stop
and start cleanly.

The **Flight Recorder** is a different tool on a different plane: a
per-node internal trace of the node's own process, compiled in and out by
a build feature flag (`flight-recorder`). It logs everything the
telemetry never sees:

- every inbound datagram and client request, byte-exact (`hex`);
- every queued outbound datagram, byte-exact, with its target;
- every drive's input summary and outcome code;
- every internal lock-state journal flush (the in-memory committed state
  flushed to the sink) — hold, renew, release, break;
- every stop-path marker write and sink drain;
- and which message tripped which assert, maybe, or tripwire as the node
  self-arrests: the sticky-fault reason, the boundary panic's payload,
  and the two-tier maybe convention's trip sites.

## The 200 MiB bound: rotation + sweep

The operator's cap: one node's flight-tape series never exceeds **200
MiB** on disk. The mechanism mirrors the telemetry capture's
`{epoch}.aof` series:

- The ACTIVE tape `flight-<node>.jsonl` rotates to an epoch-named
  history file `flight-<node>-<epoch>.jsonl` (epoch in unix
  milliseconds; same-millisecond rotations add a `-1`, `-2`, ...
  disambiguator) when it passes **100 MiB** — half the cap, so the
  active tape and the retained history each own half.
- Rotation happens between flushed lines: every line is flushed before
  the rename, the fresh tape opens on a fresh header record, and the
  event sequence continues unbroken across files — the flush-per-line
  and crash-evidence properties are intact.
- After every rotation (and at every open) the history is swept: the
  newest rotated file is NEVER deleted, older files roll away
  oldest-first while the retained history sum exceeds the 100 MiB
  history budget. History is preserved as complete files for the admin
  up to that budget; nothing is truncated mid-line.
- A rotation or sweep failure poisons THIS recorder only — the tape
  stops, the node runs on — exactly like a write error. The recorder
  never touches the replication path.

History older than the newest rotated file is the thing the cap trades
away; teardown still pulls and moves the whole surviving series off the
host.

## The two uses, side by side

Both capture files stream the same `from,to,{json}` replay tape, and the
extraction story is the same for both: stream, filter
`^${from},${to}`, extract the `frame_hex` bytes, force-feed a node in a
unit test (`tests/scenario/mod.rs`). The planes differ in everything
else:

| | The lock telemetry capture file | The Flight Recorder |
| --- | --- | --- |
| Build | always present in prod builds | feature-flagged (`flight-recorder`), NOT for production |
| Node | the non-voting telemetry nodes | every node that opts in via `LUNET_FLIGHT_RECORDER_DIR` |
| Critical path | not on it (deferred write-behind) | off the replication path, capped at 200 MiB per node |
| Content | public, wire-visible events | hidden state that never goes on the wire |
| Format | stable-ish, UI-facing | unstable, internal |
| Reader gate | none | deep read: reader commit == recording commit |

The telemetry capture file is the prod surface — always available,
stable-ish in format, and read by the console UI. The Flight Recorder is
debug-only, unstable, and same-commit-readable only.

## The commit gate

The FIRST record of every flight recording is the header: the git commit
hash the recording build was compiled from, the dirty flag, the node id,
and the format version. A recording is readable ONLY by code as-at that
commit:

- A flight-recording build MUST come from a clean commit. The build-time
  guard (`ext/advisory_lock/build.rs`) fails
  `cargo build --features flight-recorder` on a dirty tree; the
  `FLIGHT_RECORDER_ALLOW_DIRTY=1` override proceeds but stamps the
  recording `dirty: true`, and the reader annotates that loudly.
- The reader path (`skaffold_flight_tape`, and the extraction module it
  wraps) refuses a recording whose commit the reading code is not, naming
  both the recorded and the reading commit — for the DEEP read. The gate
  binds every read beyond the stable slice: `--deep`, or any kind beyond
  `wire`. Cross-commit, the stable `from,to,jsonl` slice (the wire kinds)
  still streams, best-effort: the CSV shape is the stable surface, the
  bytes under it are not (mangled lines are skipped and counted, never
  guessed).

Debug-level tool: no promise is made that a recording survives a later
commit. To read an old recording's internal event log, check out its
commit.

## Builds

The flag OFF (the default) compiles none of it: the prod path is
unchanged. The flag ON build is an optimized release binary of the same
tree — it does not change the panic discipline (a release build's maybes
still log and continue, never crash) — it only makes the trace:

```console
cargo build --release --features flight-recorder   # the cdylib shape (the Lunet-hosted path)
```

For the rig, the lease-sequencer binary carries the same feature as a
passthrough; a node opts in per node by pointing the environment variable
`LUNET_FLIGHT_RECORDER_DIR` at a directory — each node writes its own
tape `flight-<node_id>.jsonl` there:

```console
cargo build --release --features flight-recorder -p lease-sequencer
export LUNET_FLIGHT_RECORDER_DIR=/var/lib/locks/flight   # per host
```

Without the variable set a recording build runs unrecorded. An open
failure runs the node unrecorded too — the recorder never touches the
replication path.

## Reading a recording

```console
cargo run -p lease-sequencer --bin skaffold_flight_tape -- \
    --file .tmp/flight/flight-44.jsonl [--node 44] [--from N] [--to N] \
    [--from-any] [--to-any] [--kinds wire|internal|<kind>[,...]] [--deep] \
    [--out PATH]
```

The bin streams the recording as one `from,to,{json}` CSV line per kept
event, in recording order — the same shape the telemetry tape streams
(`skaffold_aof_tape`) and the same playback engine
(`tests/scenario/mod.rs`) consumes. The trivial shell filter
`... | grep "^66,44,"` works on the plain output, and the endpoint flags
filter the RENDERED endpoints: what `--from 66 --to 44` keeps is exactly
what the shell grep keeps (`?` endpoints drop under a filter unless
`--from-any`/`--to-any`, as the telemetry tape's).

The from/to derivation is honest, printed in the bin's `--help`. The
flight event carries better attribution than the wire (which names no
sender):

- `receive-in` / `request-in`: `to` = the recording node's own id;
  `from` = the recorded sender when the record carries it (the node's
  own host named the peer; exact, never a guess), else `?`.
- `emit`: `from` = the recording node's own id; `to` = the record's
  target, else the node's own id.
- internal kinds (`drive-in`, `drive-out`, `fault`, `maybe`, `journal`,
  `marker`, `stop-drain`, ...): `from` = the record's own `from` when it
  carries one, else `?`; `to` = the record's own `to` when it carries
  one, else the recording node's own id.

## The deep read

`--deep` (or `--kinds` naming anything beyond `wire`) reads the full
internal event log: every event, wire and internal, rendered with its
`seq`, plus the header facts and per-kind counts on stderr — drive
outcomes, faults, maybes, journal flushes, markers, stop drains. The deep
read requires reader commit == recording commit and is loudly refused
otherwise; cross-commit offers only the stable `from,to,jsonl` slice. The
deep output is NOT a stable format: it exposes the recorder's internal
event log as-at its commit, with no promise of survival across commits.

Default output (the stable slice): the playback surface only — one
`from,to,{json}` CSV line per message. The sender attribution is exact —
the node's own host named every inbound datagram's sender — which the
wire header itself never carries.

## Run discipline

A cluster run with the Flight Recorder ON runs a flight-recorder-ON
binary on every node and pulls BOTH kinds of tape at teardown: the UI
AOF tapes (the lock telemetry capture file series) and the node internal
tapes. The node internal tapes also attach to the local skaffold ad-hoc
tools investigating a bug. Shutdown gains one step: move the
flight-recorder files off the host before powering off, to free host
space — the 200 MiB cap keeps a node's series bounded on the host, and
the teardown pull takes whatever history the cap preserved.
