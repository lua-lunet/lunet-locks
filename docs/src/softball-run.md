# The softball run: the mandatory pre-release cluster test

The softball run is the release gate for cluster behaviour. No release
fires without a green softball recording. It exercises, on the three-host
cloud rig under a deliberately gentle load, exactly the operational
surfaces a release depends on: a clean stop and restart of a voting node,
a clean stop and leader restart with takeover, a crash stop with
reincarnation, and a teardown whose telemetry record is complete — no
error, no omission — else the run is void.

## The client profile

The load is one polite client per host, one contender thread each, and no
getters. `lease-load` carries the shape in its `--model polite` mode: the
not-holder probe is floored at 1,000 ms (about one lock attempt per
second), the holder renews at half the leader-echoed window (about four
renewals per second at the 500 ms lease / 0.5 renew-fraction knobs), and
the stats file emits one JSON window line every 2 s whose last line is
always the cumulative summary. Clients boot silent — the gate starts
every worker OFF until the start signal — and are started once before the
first phase, then run across every phase unpauseed.

```console
lease-load --server "[v6]:19301" --server "[v6]:19301" --server "[v6]:19301" \
    --lock-id <lock> --lease-ms 500 --renew-fraction 0.5 \
    --clients 1 --getters 0 --model polite \
    --id-base <base> --stats-out state/load.json
```

The server list carries all three voters' client ports, so a restarting
node's dropped connection rotates to a survivor. The knobs in force are
recorded on every run.

## The stabilization gate

After every phase the runner takes two stats snapshots four seconds
apart across all clients and reports the raw numbers. The gate is:

- one holder sustaining — `bump_ok` climbing at the renewal rate on
  exactly one client, the others probing at the 1 s floor;
- zero errors in both post-stabilization windows — the phase's
  cumulative error delta is reported as its error cost, quantified, not
  hidden;
- no hang — every client's window timestamp advanced, no window gap
  greater than twice the window;
- the restarted node's pid is new and its co-located processes untouched;
- each restarted host's flight tape grew during the phase.

A failed gate stops the run — no improvised fixes on the rig.

## The phases

Stop semantics are the adapter's termination obligations: SIGTERM is a
clean stop (wire closed, `stopped` and `flushed` written, the next boot
continues under the same incarnation with no reincarnation notice);
`kill -9` is the crash shape (dirty boot, identity bump, the
`(old, new)` reincarnation announcement). Every stop is anchored: the
acting host stamps the epoch millisecond immediately before the action
into the run's anchors file, live.

1. **Clean restart of a non-leader voting node.** SIGTERM, one second,
   `kill -9` backstop only if the process still lives, restart with the
   same boot line and state file, ten seconds of re-stabilization. The
   node rejoins under the same incarnation — no reincarnation marker —
   and the stabilization gate is green.
2. **Clean restart of the leader.** Same stop discipline on the leader.
   Takeover must occur within the sleep: a new leader is named by the
   survivors' heartbeat trails, the client's bump gap across the kill is
   measured, and the gate is green. The leader kill may transiently bump
   errors — quantified.
3. **Crash stop of a non-leader.** `kill -9`, no sleep, immediate
   restart, ten seconds to rejoin. The restarted node carries the dirty
   boot and its new incarnation; the gate is green.
4. **Teardown of the processes, not the VMs.** The flight-recorder tapes
   are pulled first and moved off the host to free space, then clients
   are silenced and every process is stopped per-PID, then the observer
   telemetry, logs, state files, membership sidecars, and client stats
   are pulled, the recording is checked for fault/maybe/tripwire sites,
   and the completeness checklist goes green before the VMs power off.

## The completeness checklist

ALL of it verified green before power-off, else the run is void:

- both tape kinds pulled — the lock telemetry capture file series (the
  UI AOF `{epoch}.aof` tapes) and the node internal flight recordings
  (`flight-<node>.jsonl` for every node);
- the flight tapes moved off the hosts (pulled, then removed on host);
- AOF bytes written versus the 512 MiB retention: file count and total
  bytes recorded before and after, no rolled-away files;
- voter and standby logs pulled, non-empty, with the `RUST_LOG=info`
  leader/view/fault trails present, and the variable verified in every
  sequencer's environment at boot;
- state files and membership sidecars pulled per host;
- the client stats files pulled per client, each ending in the final
  cumulative line of a clean exit;
- the anchors file complete, written live;
- the md5 of every pulled file recorded host-side and matched locally;
- the `chronyc tracking` capture at run start: a stepped source and
  |System time| < 10 ms on every host.

## Step 0: the build confirmation

Step 0 is `make build-proof`, and it is MANDATORY before the run: the
tree clean and committed at HEAD (the target fails loudly on a dirty
tree), then a Docker x86 (linux/amd64) image build of exactly that
commit — a build confirmation, not a deployment, so the cluster never
runs a "only builds on my laptop" commit. We test head-of-push, so no
CI covers this; the gate does. The target prints the verdict and the
commit hash last; tee both into the run dir. Nothing is deployed and
nothing from the image is run — the build IS the proof. The mechanics
(colima, the emulation registration, the x86 assertion) are in
[Build and tests](build-and-tests.md).

The re-minted rig binaries (flight-recorder ON for
`lease-sequencer`, plain release for the clients) are distributed to the
hosts and md5-verified against the mint manifest before any boot.

See [Build and tests](build-and-tests.md) for the build gate and the
[Flight Recorder](flight-recorder.md) for the two tape kinds and the
off-host rule.
