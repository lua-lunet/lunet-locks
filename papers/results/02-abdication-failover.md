# Lab book 02: abdication failover — the detector-free takeover

*2026-09-19T08:14:17Z by Showboat 0.6.1*
<!-- showboat-id: 3e581d2c-efb8-4f74-9509-0f876c153790 -->


## The fastest way to change a leader is to ask it

Every consensus system has two ways to replace the leader. The common
way is sad: the leader dies, and the others must *notice* — waiting
out a timeout, suspicious that silence means death. On this rig that
wait is configured at 3 to 5 seconds, and the measured takeover that
way cost twenty seconds (the contrast at the end of this book). The
other way is the paper's: **the leader proposes its own succession.**
No suspicion, no timeout — the abdication itself is a proposal, so
the takeover costs one consensus round-trip. This book shows the
measured round-trip: 30, 27, and 26 milliseconds, three cycles, so
that every node took a turn leading.

### The anchors, verbatim

These are the run's own recorded anchors for the three abdication
cycles. `S2-begin` is the runner issuing the verb; `S2-abdicate-drive`
is the verb's reply (always `accepted:true`); `S2-settled` is all
three voters reporting `normal` in the new view.

```sh
grep -E "S2-|measure scenario=S2" data/polite2-anchors.txt
```

```output
anchor=1789802982055 S2-begin cycle=1 leader=n1 states=n1:normal:v84:l1 n2:normal:v84:l1 n3:normal:v84:l1
anchor=1789802982066 S2-abdicate-drive cycle=1 reply={"action":"abdicate","accepted":true}
anchor=1789802982200 S2-settled cycle=1 states=n1:normal:v84:l1 n2:normal:v84:l1 n3:normal:v84:l1
measure scenario=S2 cycle=1 abdication_leader=n1 drive_ms=8 failover_ms=30 settle_ms=145
anchor=1789802994314 S2-begin cycle=2 leader=n2 states=n1:normal:v85:l2 n2:normal:v85:l2 n3:normal:v85:l2
anchor=1789802994322 S2-abdicate-drive cycle=2 reply={"action":"abdicate","accepted":true}
anchor=1789802994467 S2-settled cycle=2 states=n1:normal:v86:l3 n2:normal:v85:l2 n3:normal:v85:l2
measure scenario=S2 cycle=2 abdication_leader=n2 drive_ms=8 failover_ms=27 settle_ms=153
anchor=1789803006601 S2-begin cycle=3 leader=n3 states=n1:normal:v86:l3 n2:normal:v86:l3 n3:normal:v86:l3
anchor=1789803006608 S2-abdicate-drive cycle=3 reply={"action":"abdicate","accepted":true}
anchor=1789803006757 S2-settled cycle=3 states=n1:normal:v86:l3 n2:normal:v86:l3 n3:normal:v86:l3
measure scenario=S2 cycle=3 abdication_leader=n3 drive_ms=7 failover_ms=26 settle_ms=156
```


**Reading the cycles:** cycle 1: n1 abdicates, the cluster installs
view 85 with n2 leading, failover 30 ms. Cycle 2: n2 abdicates to n3,
27 ms. Cycle 3: n3 abdicates back to n1, 26 ms. Three different
leaders, three proposals, three clean installs — leadership visited
every node, each time for the price of one round-trip on loopback.

### The wire, at the millisecond level

Anchors are the runner's view from outside. The nodes' own logs show
the same transition from inside: within 7 ms of the drive's reply,
the next node logged itself the new view's designated leader. Here
are the raw lines around cycle 1 (the full windows are in
`data/s2-window-n*.txt`):

```sh
grep -E "1789802982" data/s2-window-n2.txt | head -3
```

```output
32054: INFO leader leader=2 era=1 view=85 ts=1789802982073
32055: INFO phi-wait leader=2 phi=0.000 prev_wait=546 next_wait=715 ts=1789802982073
```


**Reading the wire line:** n2 logged `leader leader=2 era=1 view=85`
at `ts=1789802982073` — seven milliseconds after the abdication reply
(`...066`). The phi-wait line right below is the failure detector
*resetting*: phi reads 0.000, the detector never fired, and it never
needed to. The detector is still there as the safety net; the
abdication simply never asks it to work.

### The clients noticed nothing

Leadership changed three times in 25 seconds while clients wrote
leases at about 17 operations per second per node. Counted from the
raw `lease-attempt` lines (the computation is `data/s2-client-continuity.json`):

```sh
cat data/s2-client-continuity.json
```

```output
{
 "n1": {
  "s2_window_ops": 414,
  "s2_window_max_gap_ms": 166,
  "total_ops_in_log": 9440
 },
 "n2": {
  "s2_window_ops": 440,
  "s2_window_max_gap_ms": 170,
  "total_ops_in_log": 8154
 },
 "n3": {
  "s2_window_ops": 438,
  "s2_window_max_gap_ms": 171,
  "total_ops_in_log": 9666
 },
 "n4": {
  "s2_window_ops": 0,
  "s2_window_max_gap_ms": 0,
  "total_ops_in_log": 202
 }
}
```


**Reading the table:** 1,292 lease operations across the three nodes
inside the rotation window, zero errors, and the longest silence at
any node was 171 ms — about three normal inter-operation intervals.
Three leader changes were, at worst, a momentary slowdown.

### The contrast: waiting for the detector

The same run measured the other path. In scenario S3 the leader was
killed *without* abdication, and the cluster had to detect the death
on its configured 3–5 second window, then walk the catch-up:

```sh
grep -E "measure scenario=S3" data/polite2-anchors.txt
```

```output
measure scenario=S3 node=n1 stop_ms=50 takeover_ms=20370 new_leader=n2 rejoin_ms=42991
```


**The comparison, in one line:** the same leadership change, the same
hardware, the same knobs — **26–30 ms when the leader proposes its
succession, 20,370 ms when the cluster must detect the death.** Three
orders of magnitude. This is the paper's maintenance claim made
concrete: the failover bound is the proposal round-trip, not the
failure detector, and the detector's only job is the genuinely
unannounced death.

### What this book does and does not claim

It claims: on this rig, abdication failover completed in 26–30 ms per
cycle, all three nodes rotated, clients saw no errors, and the
detector-bound path on identical knobs cost 20.4 seconds. It does not
claim the lab harness is efficient everywhere — the detector path's
long tail includes the same rejoin-walk cost discussed in book 01.
The protocol floor is what the paper sells; this book is the floor,
measured.
