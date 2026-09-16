# ANDON 6 — run-4 kill#3: the lock verb path wedges while VRR keeps flowing

spawned: 2026-09-15, mid-run-4 (cluster LIVE, wedge IN PROGRESS — best
evidence is live). Your ANDON 5 context stands
(.tmp/delegation/andon5.context.md); this is a NEW, different break.

## Observed facts (run 4 = locks run after your uuid fix, live)

Binaries: lease-sequencer `796e5b08`, lease-load `b8184f29` (your fix
+ the wire canonical-uuid guards), lease-client unchanged. Bring-up
clean: chrony 18–46 µs offsets, 3 voters + 3 standbys joined, 3 polite
clients, and SOFTBALL SUSTAIN WAS PERFECT: one holder renewing at ~4.5/s
(w3c first: set_ok=1 bump 137 in 30 s), the other two probing at the
1 s floor, zero errors.

The exp1 kill loop, anchors recorded in `.tmp/telemetry/run4/anchors.txt`
(each anchor = `date +%s%3N` on the holder's host, then `pkill -USR1`):

- kill#1 anchor=1789446140458 (pause w3c, the holder): w1b took over
  (set 0→1, bump 36→49 climbing) — CLEAN.
- kill#2 anchor=1789446333602 (pause w1b, the new holder): w2b took over
  (set 0→1, bump 22→35→86 climbing) — CLEAN.
- kill#3 anchor=1789446360343 (pause w2b, the holder): w3c's client
  took over (set 1→2, bump 238→248) — takeover COMMITTED — and then
  ~04:27 EVERYTHING on the verb path froze:
  - all three clients' op counters froze (load.json still gets window
    records — fresh mtimes — but zero op progress across 10+ s);
  - `lease-client --verb get` against w3c's AND w1b's voters: silent
    timeout (TCP accepted, no reply within 15 s);
  - all six sequencer processes alive, state Sl, CPU 2–23%, no spin;
  - the standby AOFs KEEP RECORDING (fresh 04:27:29 writes, hb samples
    + wire frames still flowing) — the VRR layer is fully alive.
- So: VRR alive, lock verbs dead, everywhere, quietly.

Also: the `--log` files are EMPTY (RUST_LOG unset — the env-filter
default records nothing). Operational finding for the runbook, and it
means the evidence is the AOF wire/telemetry corpus + the live stacks.

## Your job

Five whys to the ROOT, fix the complete line, defensive test, honest
suite states. The rig stays live and untouched except your run-sheet.

## Rig run-sheet (read-only + stack dumps; one invocation per action;
gtimeout-wrapped; never kill/restart anything)

```
gtimeout 15 ssh -o BatchMode=yes rig-w1b 'cd /root/rig && ./lease-client-x86 --server "[2001:bc8:1e80:23c8:dc00:ff:feae:8d10]:19301" --verb get --lock 14531090'
gtimeout 15 ssh -o BatchMode=yes rig-w2b 'cd /root/rig && ./lease-client-x86 --server "[2001:bc8:1d90:2c26:dc00:ff:fe00:18fc]:19301" --verb get --lock 14531090'
gtimeout 15 ssh -o BatchMode=yes rig-w3c 'cd /root/rig && ./lease-client-x86 --server "[2001:bc8:1fb0:a30:dc00:ff:fee1:8d42]:19301" --verb get --lock 14531090'
gtimeout 30 pdsh -R ssh -w rig-w1b,rig-w2b,rig-w3c 'ss -tnp | grep 19301 | head; ss -tn state established "( sport = :19301 )" | head'
```

Stack dumps (read-only attach; gdb may be absent — if so, note it and
use /proc/<pid>/stack or /proc/<pid>/wchan):

```
gtimeout 60 ssh -o BatchMode=yes rig-w1b 'command -v gdb >/dev/null && for p in $(ps -C lease-sequencer -o pid=); do echo "== $p"; gdb -batch -p $p -ex "thread apply all bt" 2>/dev/null | head -60; done' 
gtimeout 60 ssh -o BatchMode=yes rig-w2b 'command -v gdb >/dev/null && for p in $(ps -C lease-sequencer -o pid=); do echo "== $p"; gdb -batch -p $p -ex "thread apply all bt" 2>/dev/null | head -60; done'
gtimeout 60 ssh -o BatchMode=yes rig-w3c 'command -v gdb >/dev/null && for p in $(ps -C lease-sequencer -o pid=); do echo "== $p"; gdb -batch -p $p -ex "thread apply all bt" 2>/dev/null | head -60; done'
```

You MAY scp the live AOF dirs read-only into
`.tmp/telemetry/run4/dc{1,2,3}-live/` and run `tools/aof-trace-tool.py`
(`--kind locks-timeline --anchor 1789446140458 --anchor 1789446333602
--anchor 1789446360343`) over them. All scratch inside the repo
`.tmp/delegation/andon6/`.

## Candidate lines (five-whys; do not stop at the first)

1. The leader's verb execution path: what serializes the service's
   verb handling (a mutex around the embedded runner?), and can a
   pause/kill takeover shape leave it held? The wedge froze ALL THREE
   voters' client ports — followers forward verbs, so ONE wedged
   leader wedges all paths. WHO is the leader post-kill#3 (AOF
   marker-5 samples carry `leader`)?
2. The forwarding path (your PR #24): the forwarder's TCP conn to the
   leader — if the leader's conn handler wedged mid-reply, do follower
   forwards queue forever without timeout? GETs on followers AND on
   the leader both timed out silently — is there a shared accept loop
   or a bounded thread pool starved by stuck conns?
3. The takeover shape itself: kill#3 = pause holder (USR1) → holder's
   lease expires → successor SETs → the expired holder's in-flight ops
   (bump/renew that the paused client had already sent?) — the USR1
   gate stops NEW ops but what about ops IN FLIGHT when the gate
   closed? Kills 1-2 were clean; what was different at kill#3 (w3c's
   contender had just requeued seconds before — a race between its
   requeue probe and the expiry takeover)?
4. The client TCP listener: each sequencer's 19301/19302 accept loop —
   is the lease-client's silent-timeout shape a conn that got accepted
   but never read (a wedged reader thread), and does the standby's
   AOF-recording thread share a lock with the conn path (AOF alive =
   that lock is NOT the wedged one)?

## Definition of done

1. `.tmp/delegation/andon6/root-cause.md` — five whys with the live
   evidence (stacks if gdb cooperated, AOF timeline around
   anchor 1789446360343, which leader, which op wedged first).
2. The complete-line fix + tests (red on the old code, green on the
   new; replay the exact shape via the UDS playback harness if the
   failing message corpus can be isolated).
3. Honest suite states.
4. Whether run 4's earlier kills' data (kill#1, kill#2 + the sustain
   softball) survives as valid telemetry or the whole run repeats.
5. The RUST_LOG operational finding folded into the runbook note.
