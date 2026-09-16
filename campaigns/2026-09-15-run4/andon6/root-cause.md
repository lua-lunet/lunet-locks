# ANDON 6 — run-4 kill#3: the lock verb path wedges while VRR keeps flowing

Run 4 (locks2, 2026-09-15, binaries lease-sequencer 796e5b08 /
lease-load b8184f29 with the ANDON-5 fixes). All rig access read-only;
the cluster stayed live throughout; gdb is absent on the rig (noted;
/proc thread evidence used instead).

## The five whys

1. **Why did every lock verb die while VRR kept flowing?** Because
   every verb path leads to a leader that commits — and after
   04:26:04.161 (slot 47722, +3.8 s past kill#3's client pause) no view
   ever committed again: the last three Prepares (view 42, slots
   47723-47725 — two driver GETs and w3c's client's renewal SET) went
   unacknowledged forever. Followers forward verbs to the leader and
   their conns wait on a 30 s pending deadline (main.rs:2263), so one
   dead commit path silences every client port on every host; the wire
   itself kept moving (view-change frames) for another ~85 s, which is
   why the AOFs stayed fresh.

2. **Why did no view ever commit again?** The voter triad collapsed
   into a view storm: leadership ping-ponged 44↔66 while views churned
   15→510+ at ~6 views/s for ~85 s, and node 55 — the incumbent leader
   of view 13 since bring-up — never appeared in the protocol again
   from the moment it lost view 13 (every view whose succession slot
   mapped to 55 was skipped). A two-live-voter cluster where each new
   leader dies or cannot reach quorum commits nothing.

3. **Why did the voters drop out one by one?** Each self-arrested
   through the core's never-repair contract and the adapter's silent
   poison: a core fault (the sticky `Fault`) makes the adapter poison
   on the next drive (`FAULTED → poisoned=true, outputs.clear(),
   SERVICE`), after which the node emits nothing forever. The evidence:
   55's leadership ended and it never spoke again (≈+2.0-2.4); 66 led
   view 14 and its output stream died mid-second at +3.8 — exactly the
   commit-stream death; w3c's voter is POISONED now (its client port
   answers `{"error":"rejected","code":-7}` = SERVICE, the loop-top
   poison guard), while w1b/w2b's voters answer instant `not_leader`
   (rc NOT_LEADER — NOT poisoned, but permanently non-Normal with an
   unresolvable leader and nothing left to fence with — the wire went
   fully silent at 04:27:29 mid-storm).

4. **Why did a node fault at all — what breach?** A core fault fires on
   a committed-history breach: the legality gate refusing a candidate,
   a journal refusal, or a `CommitFold::Breach` (the journal and the
   configuration history disagreeing about a committed slot), or a
   conflicting `StartView` suffix at a committed slot (§9.1). WHICH one
   fired first is NOT recoverable from the rig: the fault reason lives
   in memory only (`progress.fault()`), the adapter's FAULTED path
   logged nothing, RUST_LOG was unset (the empty `--log` files), and no
   panic printed (the voter nohups are 0 bytes — so the arrest was the
   FAULTED class, not a panic). The first breach is UNOBSERVABLE BY
   CONSTRUCTION — that is part of the root. What is pinned: the run's
   FIRST view change (v13→v14 at +2.45, leader 55→66) happened 7
   minutes into a clean run, the second fence (v14→v15) followed 1.36 s
   later, and the commit stream died inside that window; the polite
   clients' takeover committed cleanly before it (w3c's SET + 10
   renewals, +0.98 s to +3.61 s) — the breach is coincident with the
   first view change, not caused by the client traffic (the same
   traffic ran for 7 minutes).

5. **The root, in two halves.** (a) The upstream core (vendored
   `058acdc` = origin/main — no newer fix exists) produced a
   never-repair fault at the run's first view change in a cluster with
   weight-0 learners joined (eras 1-4), and the fault cascaded: each
   fence exposed another node to the poisoned/dead quorum until no
   voter could serve — this is a core-side breach, stop-and-report per
   the repo's submodule policy. (b) The adapter made the breach
   operationally invisible: the self-arrest carried no log line, no
   reply detail beyond `code -7`, and no exported reason — the rig's
   only observable was a six-hour silent wedge. The complete line
   fixable in this repo is (b): the arrest must be OBSERVABLE. (The
   nearest reproducible shape — 3 voters + a weight-0 learner, a
   settled 20-op stream, three rapid forced views — stays green
   in-process; the exact breach needs the upstream investigation, and
   the silent-fault fix is what makes the NEXT rig occurrence a one-line
   diagnosis instead of an ANDON.)

## Live evidence trail (what was pulled)

- Probes now: w1b/w2b voters `{"error":"not_leader"}` (instant, rc
  NOT_LEADER — alive, not poisoned, leader unresolvable); w3c voter
  `{"error":"rejected","code":-7}` (SERVICE — the poisoned loop-top
  guard). /proc threads: no blocked I/O anywhere (voter mains in
  hrtimer_nanosleep — the loops turn; the wedge is logic state, not a
  stuck syscall).
- Client counters (load.json, full window history): w1b's client
  hot-loops GET errors from 04:26:04 (get_err 31→1634 by 04:33);
  w3c's client hot-loops bump errors from 04:26:05 (bump_err
  7→1496 — it was the takeover holder, its renewals refused by the
  poisoned leader); w2b's client froze at 04:26:01 — the operator's
  own kill#3 USR1 pause, not a wedge symptom.
- dc1/dc3 standby AOFs (scp'd, timelines with the three anchors): the
  commit stream ran a steady ~115 ops/s (leader 55, view 13, dominated
  by the hosts' own heartbeat-driver GET flood on lock 14531089) and
  stopped cold mid-second at 04:26:04.161 (slot 47722); kill#3's
  takeover SET + ten renewals committed cleanly (+0.98 s → +3.61 s);
  three Prepares at view 42 never committed; the tail is pure
  StartViewChange/StartView ping-pong (zero Commits), views to 510+,
  the last StartView carrying the suffix to slot 47724; the wire died
  at ~04:27:29 mid-storm.
- phi samples (marker-5): view 13 (leader 55) → view 14 (leader 66) at
  +2.45 s → view 15 (leader 44) at +3.81 s → 44↔66 alternation with
  55's succession slots skipped forever.
- No panic output anywhere (all voter nohups 0 bytes); the FAULTED
  poison path prints nothing — pre-fix.

## The fix (this repo's line — commit pending)

`ext/advisory_lock/src/ffi.rs`:
- The self-arrest is observable, the arrest itself unchanged (poison
  stays sticky, every further entry reports SERVICE):
  - the FIRST fault observation records its reason (`record_fault`):
    the sticky `PlanRefusal::Faulted`, the publish `IllegalCandidate`
    (legality gate / planner-declared breach), `JournalRefused`, the
    Parked-under-Volatile impossibility, and boundary panics — said
    once on stderr, exactly where the runbook reads;
  - the poison transition prints the arrest line with the recorded
    reason;
  - `NodeStatus` reports `poisoned` + `fault_note`;
  - a new ABI `lunet_lock_node_fault` returns the NUL-terminated reason
    (TOO_LARGE-with-needed-size mirror of `lunet_lock_node_next`),
    observation-only, valid on a poisoned node.
- The drive's return codes are byte-identical to before (plan
  `Faulted`→FAULTED, publish errors→SERVICE, NotPrimary→NOT_LEADER) —
  no host-visible behavior changes, only the record and the saying.

## Defensive tests

- `ffi::tests::a_self_arrested_node_names_its_fault` — red on the old
  code (verified by stubbing the recording: `fault_note: None` —
  exactly the rig's silence), green after. A genuine core fault cannot
  be manufactured through the public API (faults require real
  breaches — the rig's own lesson), so the recording seam is driven
  directly and the first-observation-sticks semantics asserted.
- `ffi::tests::rapid_fences_with_learners_keep_the_voters_serving` —
  the run-4 shape replay: a voter triad + weight-0 learner, a settled
  20-op stream, then three rapid forced views (the phi actuation
  against a live leader, the ping-pong shape), asserting every fence
  installs Normal, no member self-arrests (rc SERVICE is the named
  failure), and the stream commits through each new view. Currently
  green — the nearest reproducible shape is healthy; it guards the
  shape for the upstream fix to come.

## Upstream escalation (stop-and-report)

The first-breach identity needs the fault reason ON THE RIG — which
the observability fix now provides. Reproduce on the rig (or in
upstream's harness) with: 3 voters + 3 weight-0 learners (eras 1-4),
~115 ops/s client traffic, hb 5 ms, phi 1.0/2.0, the first forced view
change after a long Normal period, and lossy UDP (the in-process
perfect-delivery replay does not breach — a dropped datagram in the
fence's evidence exchange is the suspected missing ingredient). The
vendored head (`058acdc`) equals origin/main — no fix exists upstream;
per this repo's rules the submodule is not changed here.

## Suite states (honest)

- ext/advisory_lock: 96 green (62 lib + integration) — all suites,
  including the two new tests.
- lease-sequencer: lib 36/36, bins 22/22, bridge 10/10,
  embedded_client 1/1, phi 13/13, telemetry 16/16, playback 6/6,
  stage1/2/4 green (stage4 passed this round under load ~4.9); stage3
  red in its pre-existing latency-verdict class (RTT bucket +
  heartbeat-gap 422 ms under sustained machine load 3.7-4.9 — the
  recorded disposition; untouched by this work).

## Can run 4's earlier data survive?

Partially — and the burden is proof, not salvage. The sustain softball
and kills #1-#2 produced a healthy, committed, single-holder history
(one holder renewing at ~4.5/s, two honest floor probes, zero errors,
two clean probe→race takeovers) with no view change anywhere in it:
that prefix is internally consistent and usable as telemetry. BUT the
run's purpose — the kill-loop rotation measurements through a healthy
cluster — cannot be certified while the collapse's root (the core
breach at the first view change) is unexplained: any analysis of
kill#3's takeover window is confounded by the wedge, and the
downstream ops (1142/1634 error loops) are artifacts. Verdict: keep
the pre-kill#3 prefix as evidence (it is what the AOF timelines in
this ANDON rest on), repeat the experiment after the upstream breach
is fixed and the observability lands on the rig; treat kill#3 and
everything after as void.

## RUST_LOG operational finding (runbook)

The sequencer's `--log` files are empty because RUST_LOG is unset: the
tracing env-filter default records NOTHING — the operator lost the
leader-change/view-change/`plan refused` trails for the whole run. The
runbook should export RUST_LOG=info (warn+debug at the operator's
discretion) on every rig host, AND note that the self-arrest line
(after this fix lands) prints on STDERR unconditionally — no log level
needed — so the nohup capture (`2>&1`) always carries it.
