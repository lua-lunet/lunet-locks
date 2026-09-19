# The softball run: the mandatory pre-release cluster test

The softball run is the release gate for cluster behaviour. No release
fires without a green softball recording. It exercises, on the three-host
cloud rig under a deliberately gentle load, exactly the operational
surfaces a release depends on: a clean stop and restart of a voting node,
a clean stop and leader restart with takeover, a crash stop with
reincarnation, and a teardown whose telemetry record is complete — no
error, no omission — else the run is void.

## The two blocks of a run cycle

A run cycle is TWO blocks in ONE VM power cycle, fresh FILES between the
blocks (state, AOF, and flight directories recreated) — never VM
restarts:

1. **The softball block** — fresh files, on the shape this document
   describes. It is the SANITY TEST: under almost-no-load (the traces
   stay small) it exercises cluster reconfig while carrying just enough
   client load to show whether the cluster locks up. It is deliberately
   NOT enough load to see clients bitten by leader death every time —
   the cluster stabilises, and that is its purpose. A green softball
   says the release's operational surfaces hold.
2. **The polite block** — fresh flight records and fresh telemetry
   ([the polite run](#the-polite-run-the-disruption-recording)):
   one client per DC CONTINUOUSLY trying to take the lock for the whole
   block. This is the run where clients SEE the disruption — the
   recording the paper reports.
3. Then ALL the data is pulled off the hosts, and the VMs power off.

A future third block — the aggressive run — joins the sequence
(softball/polite/aggressive). The two blocks and the re-run methodology
(below) are REQUIRED on any release cycle, always from a clean commit.

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

- one holder sustaining — `extend_ok` climbing at the renewal rate on
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
   survivors' heartbeat trails, the client's lease-extension gap across the kill is
   measured, and the gate is green. The leader kill may transiently error
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
  (`flight-<node>.jsonl` and its rotated `flight-<node>-<epoch>.jsonl`
  history, for every node);
- the flight tapes moved off the hosts (pulled, then removed on host);
  one node's flight-tape series is capped at 200 MiB on the host
  (rotation + sweep), and the pulled series is the history the cap
  preserved;
- AOF bytes written versus the 100 MiB telemetry retention
  (`--aof-retention-mib 100`): file count and total bytes recorded
  before and after, no rolled-away files;
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

## The polite run: the disruption recording

The polite run is the paper's experiment: the same three hosts on fresh
files, one polite client per DC holding or chasing ONE lock
CONTINUOUSLY for the whole block — not much pressure, but sustained,
never paused (the same `--model polite` shape as the client profile
above) — while the cluster's disruptions land under it:

1. the boot and join sequence, the join-time leader churn recorded;
2. the stabilization gate (one holder sustaining, the others probing,
   zero errors) — the gate that says the chase is healthy before any
   disruption lands;
3. the anchored kill loop: the holder client is silenced and requeued
   one anchored action at a time, the successor verified before the
   next anchor — the takeover path measured, not narrated;
4. replace-one-node: a voter is crash-stopped and reincarnated under a
   fresh identity joining through the leader — the descriptor is a
   hint, membership evolves;
5. the storm window: a first view change with learners joined may
   storm — it is let run and fully recorded, never wiped mid-run;
6. teardown: the completeness checklist (above) applied to ALL the
   recordings of BOTH blocks, then everything pulled, then the VMs
   power off.

The knobs in force are recorded on the run. The polite run's numbers —
takeover deltas, error costs, heartbeat spacing — are the paper's data.

## The maintenance run: the polite block's disruption ladder

The polite block drives its disruptions in a fixed ladder, in this order,
under one continuous polite client fleet (one contender per voter plus the
per-operation stream client, the client profile above). Every action is
anchored live (epoch millisecond before the action) and every scenario
captures the same stat set: per-action durations from the anchors, the
client error window around the action (cumulative error deltas, never the
window summaries), the states and views every node held at every step, and
the wire surface the disruption produced. Exact counts, units and sample
sizes on every figure; a headline number is never an estimate.

1. **Security-patch rotation** — clean TERM stop and restart of every
   non-leader voting node in turn. Serving must continue through each
   stop (the surviving voters keep committing), the node must rejoin
   `Normal` under the same incarnation with no reincarnation notice, and
   the client error window must close. Stats per node: stop duration
   (TERM sent → process gone), rejoin time (boot → `state=Normal`),
   gap-to-error-free (last error → first ok operation), states at each
   step, and the error count inside the rejoin walk.
2. **Leader abdication, cycled ×3** — the operator verb
   [`abdicate`](client-protocol.md#leader-abdication) at the leader
   rotates the leadership around the cluster: each cycle measures
   abdication-to-new-leader-serving, the failover lower bound that is
   independent of failure detection. The measurement anchor is the verb
   drive; the landing is the successor's first committed client
   operation under the new view. The view states are captured throughout
   (view id per node at every step), and the pass gate is: the old leader
   stops accepting operations immediately, a single new leader serves
   inside the ordinary view change (no timeout wait), and the old leader
   rejoins `Normal` as a member. The cycle repeats until every node has
   held leadership (a 3-voter cluster: 3 cycles).
3. **Leader stop/start, the timeout-driven baseline** — clean TERM of the
   leader, no abdication: the survivors detect at the randomized leader
   timeout (the detector knobs in force) and take over. This scenario is
   the detection-bound contrast to scenario 2: same stop discipline, same
   measurement anchors, the detector's cost quantified. The leader is
   restarted afterwards and its rejoin time measured.
4. **Crash kills** — SIGKILL nodes (non-leaders first, the leader LAST)
   and let each come back. A crash boot is the dirty shape: identity bump,
   reincarnation announcement, remap. Stats per kill: crash-to-takeover
   (detection under the detector, the leader-last case), restart-to-
   serving (process start → the node serving again, and the lease
   stream's state), and the error window.
5. **Full cluster stop/start and full reboot** — every voter TERM'd
   (staggered, per-PID), the client fleet kept running and counted
   honestly, then all restarted: the resurrection measurement (first
   leader line after its own process start, first client operation
   served after the restart anchor, all voters `Normal`). The full reboot
   is the same shape with every process down before the first boot.
6. **Controlled reconfig swap** — a member departs without a crash and a
   replacement joins while serving: `decrement` the departing voter, wait
   for the era to commit, `leave` it, stop its process, then `join` the
   replacement (the same host under a fresh admin-assigned id is
   acceptable) and `increment` it. Stats: the swap's total duration, the
   per-verb commit latencies, the serving continuity across the swap,
   and the error window. The pass gate: the replacement serves at voting
   weight, the departed id is gone from the serving configuration, and
   the client stream never recorded a crash-shaped disruption.

After the ladder: the completeness checklist (above) over the whole run,
the snapshot tool over the run directory, and the shutdown-consistency
check on the raw run directory AND on the archive — both must read
consistent or the inconsistency is the finding.

## The crash and partition lane: the softball-2 run

The maintenance ladder above forbids crashes and partitions. The
softball-2 lane is the same three-voter loopback rig and the same polite
client fleet with the two forbidden fault shapes enabled, and it answers
two questions with numbers: does a crashed or partitioned member come
back, and does the cluster survive the deep state the crashes leave
behind. Every action is anchored live, every scenario captures the same
stat set (per-action durations from the anchors, client error deltas,
per-node states and views, the named wire-surface drops, superblock and
state snapshots at every restart), and a rejoin is bounded at a 150 s
budget — a bounded-out rejoin is recorded as BLOCKED with its evidence
(states, boot note, remap-notice count, newest named diagnostic) and the
node is parked; the run continues on the serving remainder. A 2-of-3
serving cluster is an acceptable recording, not a failure of the run.

The lane runs in dependency order, each family with the same stat set:

1. **The crash family (SIGKILL, non-leaders first, leader last) at
   shallow and at deep views.** A shallow kill lands in the first views
   after genesis; a deep kill lands after the view has been driven high
   by abdication cycles and rejoin walks (the view depth is recorded at
   every anchor). Each kill: SIGKILL, cluster serving window probe,
   restart with the same boot line, dirty-boot evidence (identity bump,
   reincarnation announcement), remap-notice count at the peers,
   rejoin-to-`Normal` inside the budget, and superblock/state snapshots
   of the killed node taken before the restart and after the settle.
   The shallow/deep pair is the family table: crash-restart-to-serving
   per view depth, with the fenced outcomes (the boot-fence/era-fold
   strand) quantified, not narrated. A fenced rejoin is bounded at the
   budget, recorded, and its node is parked; because a fenced rejoin
   storms the serving voters' view counter (views 1 to 1,200+ within
   minutes on this defect family), every kill after the first lands at
   whatever depth the previous fence's storm drove — the family table
   records the view depth AT each kill, and the storm is part of the
   row, not noise.
2. **The fresh-files boundary, then the deep family.** The shallow
   family leaves parked members and the deep-conditioning hammer needs
   a serving leader, so the shallow family ends at the runbook's
   fresh-files rule: all processes down, the run's snapshot taken (the
   crash evidence preserved), the durable state wiped, and the cluster
   booted fresh. No mid-run wipes of live state, ever — the boundary is
   a full stop, snapshot, and reprovision. Then the abdication-cycle
   hammer drives the view counter past 120 and the deep family repeats
   the kills at the deep views — every node killed twice across the run
   including the leader.
3. **The whole-cluster stop/start passes** (the ladder's finale,
   scenario 5): pass 1 the durable resurrection from the deep state the
   crashes left behind — the divergence observation (recovered or
   0/3-`Normal`) — then, on a pass-1 block, the snapshot-gated
   reprovision and the pass 2 fresh reboot with the resurrection
   measurement.
4. **Network partitions.** On the fresh post-reboot cluster the
   injection is process suspension: the partitioned member is SIGSTOPped
   — it sends nothing and receives nothing, the exact partition shape
   from every other node's vantage, with the node's durable and
   in-memory state intact so the heal is a true partition heal (no
   reboot, no identity bump); the heal is SIGCONT and the backlog burst
   that follows is recorded as the delayed-delivery data. On the cloud
   rig the same scenario cuts the link at the host firewall. Two
   partitions: a follower cut (the serving 2-of-3 continues; detection,
   serving continuity, the cut node's client-visible errors, heal,
   rejoin) and the leader cut (the survivors detect at the randomized
   leader timeout and take over on the partitioned view; detection,
   takeover, drop counts during the partition, heal, the old leader's
   rejoin and its storm).
5. **The compound: leader SIGKILL with partition timing.** The leader
   crashes and a survivor is partitioned in the same window, so the
   cluster sits at one live voter with no quorum; the partition then
   heals into a two-voter view change and the crashed leader restarts
   into the resulting higher view. Measured: the no-quorum window, the
   storm size (view-change records and the view counter's climb), the
   recovery, and the duplicate-suppression surface (late-ack drains and
   the named stale-evidence drops).
6. **Abdication under load.** The abdication verb driven while the load
   fleet hammers the lock (the polite contenders plus the aggressive
   shape for the window): failover time under load and the
   client-visible error count inside the window, contrasted with the
   quiet-ladder abdication numbers.

After the lane: the completeness checklist, the snapshot tool, and the
shutdown-consistency check on the raw run directory AND on the archive.

## The re-run methodology

Check softball, then polite. If ANY bug is found — or a test parameter
needs adjusting — the cycle is:

1. fix it (with its defensive test) on a CLEAN COMMIT;
2. power the VMs up;
3. run the softball block on fresh files;
4. move the files off the hosts;
5. run the polite block on fresh files;
6. copy ALL the data off, then stop the VMs.

Repeat until the pair is bug-free. The methodology is REQUIRED on any
release cycle, always from a clean commit — Step 0's build gate below
fires on exactly that. No improvised fixes on the rig, ever. The
aggressive scenario joins this sequence as its third block.

## Step 0: the build confirmation

Step 0 is `make sanity`, and it is MANDATORY before the run: the tree
clean and committed at HEAD (the target fails loudly on a dirty tree),
then the colima fastbuild cross-check of exactly that commit —
`cargo check` for both linux triples (aarch64 native + x86 cross-built
natively by rustc, in the prod and flight-recorder shapes) — a build
confirmation, not a deployment, so the cluster never runs a "only builds
on my laptop" commit. We test head-of-push, so no CI covers this; the
gate does. The target prints the verdict and the commit hash last; tee
both into the run dir. Nothing is deployed and nothing from the image is
run — the build IS the proof. The mechanics (the fastbuild stages, the
cross libc, the RELEASE dual-arch image follow-up) are in
[Build and release](build-and-release.md).

The re-minted rig binaries (flight-recorder ON for
`lease-sequencer`, plain release for the clients) are distributed to the
hosts and md5-verified against the mint manifest before any boot.

See [Build and tests](build-and-tests.md) for the build gate and the
[Flight Recorder](flight-recorder.md) for the two tape kinds and the
off-host rule.
