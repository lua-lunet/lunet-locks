# The Flight Recorder: the per-node internal trace

The Flight Recorder is a debug-level trace tool: one INTERNAL tape per
node, recording everything the node does during a test run — including the
internal private events the lock telemetry capture file never sees. The
node's crash plus its flight recording is the black box: no guessing is
required to see what tripped what. It is NOT intended for prod release,
carries NO long-term readability promise, and may consume a LOT of disk —
all accepted, by design.

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

There is no size-reduction mechanism. A flight recording may be many
times larger than the telemetry capture file series; the operator trades
disk for exactness deliberately. Teardown moves the flight-recorder files
off the host to free space.

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
  both the recorded and the reading commit.

Debug-level tool: no promise is made that a recording survives a later
commit. To read an old recording, check out its commit.

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
    [--kinds wire|internal|<kind>[,...]] [--out PATH]
```

Default output: the playback surface only — one `from,to,{json}` CSV line
per message, the same shape the telemetry tape streams and the same
playback engine (`tests/scenario/mod.rs`) consumes. The trivial shell
filter `... | grep "^66,44,"` works on the plain output. `--kinds
internal` adds the node's private story: drive outcomes, faults,
maybes, journal flushes, stop markers. The sender attribution is exact —
the node's own host named every inbound datagram's sender — which the
wire header itself never carries.

## Run discipline

A cluster run with the Flight Recorder ON runs a flight-recorder-ON
binary on every node and pulls BOTH kinds of tape at teardown: the UI
AOF tapes (the lock telemetry capture file series) and the node internal
tapes. The node internal tapes also attach to the local skaffold ad-hoc
tools investigating a bug. Shutdown gains one step: move the
flight-recorder files off the host before powering off, to free host
space — the no-size-reduction rule means they are the largest artifact
the run produces.
