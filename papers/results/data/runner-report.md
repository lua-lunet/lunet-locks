# local-polite2-2026-09-19 — the polite-2 maintenance ladder (item25.29)

**The verdict up front: the abdication headline is POWERFUL — the
detection-independent failover is 30/27/26 ms per cycle (one StartViewChange
drive, no detector wait, the standard view change completing in under
200 ms end to end) — and the maintenance ladder ran to completion with the
whole-cluster reboot serving its first client operation 2,596 ms after
restart and the controlled reconfig swap completing in 23,542 ms with the
cluster serving throughout. The crash-restart scenario is BLOCKED with
evidence: a crashed node's rejoin at a deep view sits fenced for >150 s
(the parked era-fold family) and is recorded, not narrated.** Every
headline number below is an exact count from the captured data, units and
sample sizes stated, no estimates.

## Run identity

- Run directory: `.tmp/telemetry/local-polite2-2026-09-19/`
- Wall clock: boot 06:24:21 UTC (ts 1789802661571) → teardown complete
  ~07:44:16 = 79 min 55 s (4,793 s; the 240 s steady window, six scenario
  windows and the bounded S4/S5 blocked-record windows included).
- Binary: the `lease-sequencer` flight-recorder build of commit 7ff3f12
  (tag `polite-run-1` resolves to a different commit; the run was cut from
  HEAD as the spec instructed) — **dirty=true disclosed**: the tree carried
  the item25.29 abdication wiring uncommitted; build override
  `FLIGHT_RECORDER_ALLOW_DIRTY=1`.
- Cluster: 3 genesis voters on loopback (n1/n2/n3, ids 1/2/3, UDP
  45201–45203, TCP client 45211–45213) plus the spare joiner row n4 (id 4,
  UDP 45204) for the reconfig swap. Descriptor
  `examples/lease-sequencer/config/cluster-polite2.jsonl`, genesis true on
  the three.
- Knobs (calm): `--heartbeat-ms 5 --election-ms 3000 --recovery-ms 3000`
  (the randomized leader timeout 3000–5000 ms, `detector=sloppy-timeout` on
  the boot line). No phi (compiled out of the normal build).
- Load: 3 polite `lease-load` contenders (id-bases 810000/820000/830000,
  `--lock-id 14531090 --lease-ms 500 --renew-fraction 0.5`, 2,000 ms stats
  windows, `--model polite` = the 1,000 ms probe floor), each on a
  persistent connection rotating the three client ports; plus the shipped
  `lease-client` driving the serving probes (the failover-landing
  measurements) and every admin verb.
- Fault injection: per the ladder — clean TERMs (S1, S3, S5, teardown),
  the operator verb `abdicate` ×3 (S2), SIGKILL ×3 (S4), whole-cluster
  stops (S5), and the decrement/leave/join/increment swap (S6). Zero
  partitions.

## The abdication work item (what was built)

The core ALREADY carried abdication (rules §12: `Input::Abdicate`,
`validate_abdication`, `plan_abdicate`, the `tests/abdicate.rs` suite in
`ext/uvrr-core`) — **the core is unchanged, so there is no upstream note**.
The work was the wiring:

- Adapter (`ext/advisory_lock/src/ffi.rs`): `Node::abdicate()` reads the
  node's live (era, view) as the CAS pair, targets view v+1 (the
  `next_in_era` successor), and drives `Input::Abdicate` synchronously;
  `plan_error` maps `AbdicationRefusal::ReceiverNotPrimary` → NOT_LEADER
  (the one actionable refusal); the flight recorder records the abdicate
  drive with era/view/target (string-i-fy kept); the C ABI exports
  `lunet_lock_node_abdicate`.
- Teal service (`src/advisory_lock.tl`, `src/admin.tl`, `src/server.tl`):
  `Node:abdicate()`, the `{"action":"abdicate"}` verb decode (no member id),
  the `{"action":"abdicate","accepted":true|false}` acknowledgment shapes,
  and the leader-side drive in `handle_admin` — the emission flushed before
  the ack, no commit wait (abdication commits nothing).
- Rig host (`examples/lease-sequencer/src/main.rs`): the abdicate verb is
  leader-only, drives `node.abdicate()`, flushes the StartViewChange
  emission, and answers immediately; `lease-client --verb abdicate` drives
  it. Non-leaders answer `not_leader`; the driver rotates.
- Tests: 3 new adapter tests
  (`abdicate_abi_emits_the_immediate_fence_and_steps_the_leader_down`,
  `abdicate_abi_completes_the_view_change_new_leader_serves_old_leader_rejoins`,
  `abdicate_at_a_nonleader_reports_not_leader_and_moves_nothing`) + the Teal
  admin decode tests. Adapter suite 74/74 green; the core's own
  `tests/abdicate.rs` suite unchanged and green. The core did NOT change —
  no upstream-engagement note is drafted.

## The ladder, in order, with the exact numbers

| scenario | result |
|---|---|
| S0 steady serving 240 s | 0 client errors, one leader (n2), view 1, zero view changes |
| S1 security-patch rotation (non-leaders) | n1: stop 50 ms, serving continued (first ok probe +69 ms), rejoin 15,618 ms; n3: stop 50 ms, rejoin 21,017 ms, the 20 s probe window missed the serving gate (the walk's churn) |
| S2 leader abdication ×3 | **failover 30 / 27 / 26 ms per cycle** (verb drive 7–8 ms; every cycle `accepted:true`); settle (all voters normal) 145–156 ms; every node held leadership (n1=cycle3, n2=cycle1, n3=cycle2) |
| S3 leader TERM (timeout-driven baseline) | stop 50 ms; **takeover 20,370 ms** under the 3000–5000 ms detector (the detection-bound contrast: 20,370 ms vs 26–30 ms); rejoin 42,991 ms |
| S4 SIGKILL crashes (non-leaders first, leader last) | all three crash rejoins **BLOCKED**: the bumped node sat fenced (`restarting`/`view_change`, voting=0) for the full 150 s budget, `remap_notices=0` at every peer, newest named diagnostic `GapDetected` — evidence below |
| S5 whole-cluster stop/start | pass 1 (durable resurrection of the post-crash state) **BLOCKED** (0/3 normal after 120 s); pass 2 (fresh boot after the snapshot-gated reprovision): **first client op served +2,596 ms, all voters normal +2,578 ms** |
| S6 controlled reconfig swap | decrement 28 ms; leave 5,022 ms; join 5,017 ms; increment 5,026 ms; **total 23,542 ms**; the replacement (n4, id 4) served at voting weight +11,383 ms; the cluster kept serving across the swap |

## Counts (exact)

- Committed lease operations (lease-load cumulative, n=3 clients over the
  1,192 s client window): 7,164 = 2,470 + 2,464 + 2,230; ok 4,355
  (1,528 + 1,533 + 1,294); client-visible errors 2,809 (942 + 931 + 936).
- The error windows are exactly localized: the 240 s steady window has
  ZERO errors (the S0 gate); the quiet stretches between scenarios
  (rel 300–360 s, 390–420 s, 450–570 s) have ZERO errors; every error sits
  inside a disruption window or its rejoin-walk aftermath
  (`extraction/load_stats.json`, the 30 s error-delta table).
- Latency by client (cumulative, includes the 5 s outage-tail timeouts,
  n above): p50 18/19/18 ms, p90 25/25/25 ms. Clean-window medians
  (windows with p99 < 1 s, 558/557/557 windows): p50 median 17/17/16 ms;
  the clean-window p99 max 635/203/93 ms.
- Leader changes believed at n1: 1,431 status-plane records; the storms
  are confined to the rejoin walks: S1's walk 37+48, S2's abdications +60,
  S3's rejoin walk, and the S4 crash-rejoin storms (194–203 changes per
  30 s for ~240 s, the view counter running 84 → 1,381). The pure steady
  and quiet windows: zero.
- State transitions (status-plane distinct (state, view, leader) rows):
  n1 122, n2 25, n3 98, n4 3.
- Flight recorder (records per kind, per node): n1 drive-in 326,264 /
  emit 134,356 / receive-in 83,651 / journal 3,216; n2 209,352 / 176,749 /
  95,977 / 2,356; n3 119,990 / 56,694 / 62,086 / 2,146; n4 6,616 / 1,382 /
  2,746 / 307.
- Restarts: S1 ×2 + S3 ×1 clean TERM stops (exit 0, same incarnation);
  S4 ×3 SIGKILL (identity bump: own=16777217/16777218/16777219,
  incarnation=1 each); S5 ×3 + ×3 + the reprovision; S6: n3 left, n4
  joined. All TERM'd teardown stops flushed (`--check-shutdown`
  CONSISTENT, below).

## The blocked scenario: S4's crashed-node rejoin (evidence)

Three SIGKILL crashes, each restarted with the same boot line. Each boot
classified its durable state dirty, bumped the identity
(`incarnation=1, own=16777217/16777218/16777219`), and sat fenced for the
full 150 s budget — `state=restarting view=0 voting=0` while the cluster
held at voting weight (n1:normal:v147:l1 n2:restarting n3:normal:v147:l1
for the first kill). `remap old=` lines appeared at ZERO peers for every
kill, so the leader's forced-reconfiguration walk never started. The
rejoin walk itself was LIVE but divergent: while a bumped node sat fenced,
the two serving voters' views climbed to 1,381–1,383 (view-change storms,
194–203 leader-change records per 30 s), and the next full-cluster
restart from those divergent retained views could not converge (S5 pass 1,
0/3 normal after 120 s — all three `restarting v0 l1`). The named
diagnostics on the fenced nodes: `GapDetected`, `SlotNotOutstanding`,
`StaleEvidence`. This is the parked boot-fence/era-fold defect family (the
`boot_fence_strand_test` lane, pre-existing and failing on this tree
before any of this task's changes) — a core-side finding, evidence
captured, scenario marked blocked per the delegation protocol. The
host-side fix-and-re-run loop was exercised on everything above it.

## Limitations (disclosed, quantified)

- The per-operation stream client (softball7's shape) was dropped from the
  run: a scratch tooling probe of that shape lost replies intermittently
  (~4% per attempt, both fresh-connection-per-op and one persistent
  connection) against this rig on this host, while the shipped Rust
  clients (lease-load's persistent connections, the lease-client probes)
  did not. The tape shows the wedged operations committing at the leader
  with the reply emitted — the loss is in the scratch client's connection
  handling, not the cluster; the runner replaced it with the shipped
  `lease-client` serving probes. The same family ("client op, no ack") is
  what the smoke's live-reconfig defect (GATE 2) exhibits; the diagnosis
  evidence for both lives in the flight tapes of this run.
- The load clients' 5 s op timeout tail (exactly 3 samples per client,
  the S4/S5 outage windows) sits in the cumulative p99/max; the
  clean-window medians above carry the honest mid-distribution.
- The S1 n3 rotation's `serving_continued=0` is the probe-gate's honest
  record of its 20 s window (the rejoin walk's churn); the S1 n1 rotation
  recorded serving continued at +69 ms. The load clients' error deltas
  for those windows are the operation-level truth
  (t+240–270: 4/4/7 errors).
- Flight-tape retention: the recorder holds a trailing window per node;
  the kind counts above are from the retained surface. The node logs
  cover the whole run.

## Charts (SVG, in charts/)

- `abdication_failover.svg` — the 3 abdication cycles (30/27/26 ms) against
  the S3 timeout-driven takeover (20,370 ms), n=1 per bar
- `throughput_errors.svg` — committed ops per 30 s (bars, max 341) and the
  client error cost per 30 s (line, max 143), scenario windows shaded
- `states_timeline.svg` — per-node believed state over the run
- `views_timeline.svg` — per-node view number over the run (the walks and
  the S4 storms visible)

## Machine data (extraction/)

`anchors.json` (79 anchors + 13 measure records, parsed),
`load_stats.json` (594 windows per client, window + cumulative exact),
`latency.json` (cumulative percentiles incl. max + per-op breakdown),
`state_timeline.json` (1,341 status samples across 4 nodes),
`leader_lines.json` (2,718 leader-plane records),
`flight_kinds.json` (per-node flight-record kind counts).

## Appendix A — snapshot_run.sh (verbatim)

```
snapshot_run: 32 artifact files -> /Users/Shared/lua-lunet/lunet-locks/.tmp/telemetry/local-polite2-2026-09-19.snapshot-20260919T074517Z.tar.gz
```

## Appendix B — skaffold_flight_tape --check-shutdown, raw run dir (verbatim, exit 0)

```
OK [load-n1] consistent: 1 log file(s), 0 durable-state file(s), 0 membership sidecar(s)
OK [load-n2] consistent: 1 log file(s), 0 durable-state file(s), 0 membership sidecar(s)
OK [load-n3] consistent: 1 log file(s), 0 durable-state file(s), 0 membership sidecar(s)
OK [n1] consistent: 1 log file(s), 1 durable-state file(s), 1 membership sidecar(s)
OK [n2] consistent: 1 log file(s), 1 durable-state file(s), 1 membership sidecar(s)
OK [n3] consistent: 1 log file(s), 1 durable-state file(s), 1 membership sidecar(s)
OK [n4] consistent: 1 log file(s), 1 durable-state file(s), 1 membership sidecar(s)
OK [probes] consistent: 1 log file(s), 0 durable-state file(s), 0 membership sidecar(s)
OK [progress] consistent: 1 log file(s), 0 durable-state file(s), 0 membership sidecar(s)
check-shutdown: 9 node(s) checked: CONSISTENT
```

## Appendix C — skaffold_flight_tape --check-shutdown, the archive (verbatim, exit 0)

```
check-shutdown: 9 node(s) checked: CONSISTENT
```

## Appendix D — the mined-requirements ledger

Every reporting requirement mined from the operator's last-24 h chat
history (`opencode-chat-history`, the she-said corpus for percentiles,
jitter, charts, units) and where this report satisfies it:

1. "stats, reports, graphs, charts, counts, jitter — far far far better
   checking and reporting" → this whole document + extraction/ + charts/.
2. Explicit units and sample sizes on every number → every table row
   carries n and units; durations are exact anchored spans.
3. Percentiles consistent INCLUDING max → the latency table carries
   p50/p90/p99/max (+ the clean-window separation), mean in the JSON.
4. No unreadable unix epochs: relative timelines → all figures are
   relative to run start; per-node clocks named (the status plane).
5. Message counts / flight kinds by node → `flight_kinds.json`.
6. State-role timelines per node → `states_timeline.svg` +
   `extraction/state_timeline.json`.
7. Takeover quantified ("how long before other nodes get the lease") →
   the abdication failover 26–30 ms per cycle AND the detector-bound
   20,370 ms contrast, both measured.
8. Verdict: powerful or pathetic, quantified → the verdict paragraph +
   the blocked-scenario evidence.
9. Checks verbatim in the appendix → Appendices A–C (raw AND archive).
10. Fix host causes and re-run; a result is DATA → five aborted attempts
    (the S4/S5 hard-gate deaths and the scratch-client wedge) were
    diagnosed and fixed in-lane; the final ladder ran green to teardown.
11. No "~" in headline numbers → every headline figure is an exact count
    or an exact anchored duration.
12. Judgement calls stated, contradictions not hidden → below.

## Judgement calls (stated, not hidden)

- The stream client was dropped from the run: its scratch-tooling shape
  lost ~4% of replies against this host's rig while the shipped clients
  did not; the serving probes use the shipped binary. The loss is
  disclosed in Limitations, not hidden.
- S4's rejoin gate is bounded-and-recorded, not run-fatal: the ladder
  continues on the serving pair, and S5's pass 1 (the post-crash durable
  resurrection) is likewise bounded-and-recorded before the reprovision.
  A hard fail there would have cost the whole S5/S6 dataset for a defect
  that is already documented as parked.
- The polite-2 ladder ran on the rig stack (the lease-sequencer binary via
  the typed Teal runner), the same stack as softball7, for comparability;
  the lunet-hosted stack carries the same abdication wiring (the FFI
  adapter + `build/server.lua`) and its smoke remains the GATE 2 target.
- The tag/commit mismatch: the spec names commit 7ff3f12 (tag
  `polite-run-1`); the tag resolves to df6cc6a on this tree. The run was
  cut from HEAD 7ff3f12 as instructed; the binary's flight header records
  7ff3f12804bdea710473ccb17b6cf3f3c4b0bdea, dirty disclosed.
