# Lab book 04: full reboot and the reconfiguration swap

*2026-09-19T08:14:17Z by Showboat 0.6.1*
<!-- showboat-id: 76a511e4-c3d1-4fca-b0aa-fa8bdd3de72c -->


## Turning it off and on again — for real

Two maintenance shapes remain. The first is the bluntest: *the whole
cluster goes down, and the whole cluster comes back.* The second is
the most delicate: *swap a member out for a replacement while the
service runs.* Both ran end-to-end on this rig. This book shows both,
and also shows the one honest failure inside the first — because the
failure and the recovery are the same story about what the durable
state can and cannot carry.

### S5, pass 1: the resurrection that could not

The run's S5 first tried the hardest version: all three nodes had
just been through the crash scenario (book 03), each holding
different retained views from the view storms (one node at view
1,383, others fenced at 0). The runner stopped everything, waited,
and restarted everything from that durable state. The cluster did not
converge in 120 seconds — three nodes, three different views of
history, each waiting for a message that could not come. This is the
same era-fold family as book 03's block, and it is recorded the same
way: blocked, with the state lines.

```sh
grep -E "S5-converge-BLOCKED|measure scenario=S5 pass=1" data/s5-anchors.txt
```

```output
69:anchor=1789803768740 S5-converge-BLOCKED pass=1 states=n1:restarting:v0:l1 n2:restarting:v0:l1 n3:restarting:v0:l1
```


### S5, pass 2: the reboot that worked

The runner then did what an operator does after a failed resurrection
attempt: snapshot the state for the record, re-provision fresh, and
reboot. All down for 30 seconds; all up. The whole cluster converged
in 2.6 seconds and served its first client operation in 2.6 seconds:

```sh
grep -E "S5-converged|measure scenario=S5 pass=2" data/s5-anchors.txt
```

```output
80:anchor=1789803810375 S5-converged pass=2 leader=n1
```

```sh
grep -E "measure scenario=S5 pass=2" data/polite2-anchors.txt
```

```output
measure scenario=S5 pass=2 down_ms=30001 first_op_ms=2596 converge_ms=2578 states=n1:normal:v0:l1 n2:normal:v0:l1 n3:normal:v0:l1
```


**Reading the pass:** 30 seconds fully down, then restart-all at
`1789803807797`, all three voters `normal` 2,578 ms later, the first
client operation served at 2,596 ms. For a three-node cluster cold
start that is the shape of the claim: boot, agree, serve — in the
time of a single page refresh.

### S6: swapping a member while the service runs

The last scenario is the operator's hardest planned change: retire n3
and bring in n4, live. The protocol does it as four reconfiguration
rounds — decrement the old member's weight, remove it, add the new
member, raise its weight — each a committed consensus round. The
anchors with the round times:

```sh
grep -E "S6-decrement|S6-leave|S6-join|S6-increment|S6-swap|measure scenario=S6" data/s6-anchors.txt
```

```output
83:anchor=1789803820789 S6-decrement reply={"action":"decrement","id":3,"accepted":true}
85:anchor=1789803830868 S6-leave reply={"action":"leave","id":3,"accepted":true}
90:anchor=1789803838953 S6-join reply={"action":"join","id":4,"accepted":true}
92:anchor=1789803844301 S6-increment reply={"action":"increment","id":4,"accepted":true}
94:anchor=1789803844303 S6-swap-complete state=normal leader=1 view=6
```


### The swap's measure line, verbatim

```sh
grep "measure scenario=S6" data/polite2-anchors.txt
```

```output
measure scenario=S6 departing=n3 replacement=n4 decrement_ms=28 leave_ms=5022 join_ms=5017 increment_ms=5026 total_ms=23542 rejoin_ms=11383 states=n1:normal:v6:l1 n2:normal:v6:l1 n3:down
```
