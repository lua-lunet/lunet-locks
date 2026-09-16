# ANDON 5 — run-2 bring-up: three clients acked bumps the committed lease state cannot account for

 spawned: 2026-09-15, mid-run-2 (cluster LIVE, do not kill anything without asking)
 severity: safety-correctness break — candidate split-brain / phantom-ack line
 state: cluster and clients stay RUNNING (evidence preservation) unless the
 operator says otherwise. READ-ONLY rig access per the run-sheet below.

## What happened (observed facts only)

Run 2 (locks2-2026-09-15) bring-up, ~13:50Z-ish 2026-09-15:

1. chrony installed + synced on rig-w1b/w2b/w3c (offsets 0.05–0.34 ms,
   `^*` on time4.google.com all three; probe + tracking recorded in
   `.tmp/telemetry/locks2-2026-09-15/chrony-*`).
2. New binary distributed first: lease-sequencer md5
   `7a052cf5cecf043ba0a53ba979b439fd` (the descriptor-as-hint change:
   `boot_nodes()` in `examples/lease-sequencer/src/main.rs`; NOTE: the
   change is NOT in the executed path for these boots — all six node
   names ARE listed in cluster3.jsonl; the six boots took the
   unchanged-listed-name path). lease-load `96c6e632…` and lease-client
   `98c7635b…` UNCHANGED from run 1.
3. Wipe (`state/*`, `/root/aof` recreated), then 3 voters booted
   (w1b/w2b/w3c, cluster3.jsonl, hb 5 ms, election 1000 ms, phi
   1.0/2.0/10/200), then 3 standbys (dc1/2/3-tel with --aof-dir).
4. Joins had NOT_LEADER CHURN: 77 accepted on w1b first probe; 88/99
   needed retries with 10–20 s waits between; 99 accepted via w1b,
   refused `not_leader` on w2b, `accepted:false` (duplicate) on w3c.
   So leader flapped during the join phase.
5. 3 polite clients started (lease-load --model polite --lock-id
   14531090 --lease-ms 500 --id-base 800002/800004/800006,
   --stats-out state/load.json), gated silent by default, then
   `pkill -USR2 -x lease-load` started them.

## The anomaly (facts)

After ~170 s of running, per-client cumulative stats (all three hosts,
files `/root/rig/state/load.json`):

- w1b: set_ok=1 bump_ok=295 bump_err=7 get_ok=1 get_err=0
- w2b: set_ok=1 bump_ok=313 bump_err=7 get_ok=1 get_err=0
- w3c: set_ok=1 bump_ok=230 bump_err=8 get_ok=1 get_err=0

Simultaneously, `lease-client --verb get --lock 14531090` via EACH of
the three voters' 19301 ports returns ONE coherent holder:

- same holder UUID `00000000-0000-0000-fb84-3133094f979d`,
  same taken_at_ms 1789434679499, renew_count 43 → 45 → 47 across three
  probes ~500 ms apart (≈4/s = ONE holder's renew cadence at
  lease_ms 500 / renew 0.5), lease_id advancing 444 → 446 → 448.

So the committed lease state accounts for ~47 renewals, while the three
clients together count 838 acked bumps (err only 7/7/8). All three
clients also show get_ok=1 — each probed once, then never went back to
probing (a refused bump returns a contender to probing; that never
happened). Run 1 (same lease-load/lease-client binaries, same
cluster3.jsonl, leader was 66 then; w1b is leader now) did NOT show
this: one holder at a time, rotations visible via set_ok increments
through the kill loop.

## Your job

Five whys to the ROOT, then fix the COMPLETE line (no band-aid), then a
defensive regression test that replays the exact failing shape. No git
stash; `git add` only your files unless the tree is clean (then one
full commit). Docs-before-code if the semantics need restating.

Candidate lines to five-why (do NOT stop at the first plausible one):

1. The forwarding path (PR #24, `a76334e`): which replies count as
   "committed acks" at the lease-load Contender? Is a FORWARD_NOT_LEADER
   or an error reply being counted as bump_ok? Read
   `examples/lease-sequencer/src/bin/lease-load.rs` (the Contender is
   the shared decision machinery) and
   `examples/lease-sequencer/src/embedded_client.rs`.
2. The duration-lease grant (`a7b5fc0`,
   `ext/advisory_lock/src/locks.rs` + `ffi.rs`): the leader's
   free-or-SAME-HELD check — what identity does a BUMP carry and what
   does "same-held" compare? Could all three clients' bumps be
   considered same-held against one lease?
3. The leader flap during the standby joins (weight-0 joins at hb 5 ms
   with phi flapping): did each client's initial SET commit against a
   DIFFERENT leader/view, each granting a lease, with the bump fencing
   failing to refuse the two stale grantees? Check each voter's log for
   era/view lines around the join window.
4. The GET path: my three GETs returned one coherent state — because
   they were forwarded to one leader? Or answered locally from a shared
   state? If forwarded, why do two clients' bumps not ALSO forward and
   get refused?

## Rig run-sheet (READ-ONLY; one invocation per action; every call
gtimeout-wrapped; NEVER pkill/killall/reboot anything)

```
gtimeout 15 ssh -o BatchMode=yes rig-w1b 'cd /root/rig && ./lease-client-x86 --server "[2001:bc8:1e80:23c8:dc00:ff:feae:8d10]:19301" --verb get --lock 14531090'
gtimeout 15 ssh -o BatchMode=yes rig-w2b 'cd /root/rig && ./lease-client-x86 --server "[2001:bc8:1d90:2c26:dc00:ff:fe00:18fc]:19301" --verb get --lock 14531090'
gtimeout 15 ssh -o BatchMode=yes rig-w3c 'cd /root/rig && ./lease-client-x86 --server "[2001:bc8:1fb0:a30:dc00:ff:fee1:8d42]:19301" --verb get --lock 14531090'
gtimeout 30 pdsh -R ssh -w rig-w1b,rig-w2b,rig-w3c 'tail -n 80 /root/rig/w*.log /root/rig/dc*-tel.log 2>/dev/null'
gtimeout 30 pdsh -R ssh -w rig-w1b,rig-w2b,rig-w3c 'grep -n "era\|view\|leader" /root/rig/w1b.log 2>/dev/null | tail -40'
gtimeout 30 pdsh -R ssh -w rig-w1b,rig-w2b,rig-w3c 'python3 - <<EOF
import json
recs = [json.loads(l) for l in open("/root/rig/state/load.json") if l.strip()]
print(json.dumps(recs[-1])[:400])
EOF'
```

You MAY also scp the current standby AOF files down read-only into
`/Users/Shared/lua-lunet/lunet-locks/.tmp/telemetry/locks2-2026-09-15/dc{1,2,3}-live/`
(they roll at 2 MiB) and run the repo's `tools/aof-trace-tool.py`
(`--kind locks-timeline`) on them for the holder history. All laptop
scratch inside the repo `.tmp/` — NEVER outside
`/Users/Shared/lua-lunet/lunet-locks`.

## Facts you need

- v6 addrs: w1b [2001:bc8:1e80:23c8:dc00:ff:feae:8d10], w2b
  [2001:bc8:1d90:2c26:dc00:ff:fe00:18fc], w3c
  [2001:bc8:1fb0:a30:dc00:ff:fee1:8d42]; voter client ports 19301,
  standby 19302; peer UDP 9101/9102.
- The polite Contender: `examples/lease-sequencer/src/embedded_client.rs`
  (duration-lease build_set, reply_remaining_ms), the load driver
  `src/bin/lease-load.rs`.
- The lease core: `ext/advisory_lock/src/locks.rs` (+ `ffi.rs`), tests
  `ext/advisory_lock/tests/lease_duration_grant.rs`.
- Run-1 context: `.tmp/telemetry/locks1-2026-09-14/ANALYSIS-2026-09-14.md`
  (correct behavior recorded), and the UDS playback harness
  (`examples/lease-sequencer/src/uds_harness.rs`,
  `tests/uds_playback_test.rs`) which replays real rig corpora
  deterministically — the defensive test belongs there if the failing
  shape can be replayed as given-messages.
- Write your working notes to `.tmp/delegation/andon5/` (in-repo).

## Definition of done

1. The five whys written to `.tmp/delegation/andon5/root-cause.md` with
   the evidence trail (which reply path counted the acks; which check
   let the bumps through; what the era/view history was).
2. The complete-line fix, implemented + tested (cargo test green for
   what you touched; every existing suite still green).
3. A defensive regression test that fails on the old code and passes on
   the new (replay the exact shape: three clients, one lock, a leader
   flap mid-join).
4. Report back: root cause in five lines, the fix's file list, the test
   names, and whether run 2's data is salvageable or the run must be
   repeated after the fix.
