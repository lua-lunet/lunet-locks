# Lab book 01: clean maintenance — the security-patch walk

*2026-09-19T08:14:17Z by Showboat 0.6.1*
<!-- showboat-id: aa6c6aed-ae58-46ef-b1cc-b3d80200b944 -->


## What this book shows, and why you should care

Imagine a small cluster of three computers that must agree, at every
moment, on *who holds which lease* — the lunet-locks demonstration of
the paper's uVRR protocol. Now imagine it is Tuesday morning and the
operating system on all three machines needs a security patch. The
question this book answers with real data: **can you patch every
machine in a running cluster without stopping the service and without
any risky machinery?**

The paper's answer is the *clean maintenance walk*, and it is
deliberately boring: stop each non-leader politely, patch, restart
it; then ask the leader to step aside politely (that is the next
book), restart it last. Boring is the point. The disk writes happen
only on this polite path — the node flushes its state, writes a small
*boot fence* record saying "I stopped cleanly", and exits. On restart
it reads that record, learns it may keep its old identity, and asks
the cluster for anything it missed. No special recovery protocol, no
ambiguity, no risk.

Below: the actual log lines from the real run of 19 September 2026,
each one explained.

### The evidence trail, in one table

| what happened | where the data lives |
|---|---|
| the run itself | lunet-locks run `local-polite2-2026-09-19`, snapshot archives |
| raw log extracts used here | `data/s1-clean-stop-lines.txt`, `data/s1-s3-anchors.txt` |
| the runner's own report | `data/runner-report.md` |
| the commit that ran | `7ff3f12` (disclosed dirty: the abdication wiring in flight) |

```sh
grep "sigterm: clean stop" data/s1-clean-stop-lines.txt | head -4
```

```output
logs/n4.2026-09-19.log:1228: INFO sigterm: clean stop ts=1789803856013
logs/n3.2026-09-19.log:7108: INFO sigterm: clean stop ts=1789802930575
logs/n3.2026-09-19.log:19783: INFO sigterm: clean stop ts=1789803445232
logs/n3.2026-09-19.log:39633: INFO sigterm: clean stop ts=1789803769115
```


**What you are looking at:** each line is one polite shutdown. The
node receives the `TERM` signal, stops taking new messages, and writes
its stop marker. The timestamp (`ts=`) is milliseconds since the unix
epoch; the run began at `1789802661571`, so line `7108` above fired
about four and a half minutes in.

The second half of the walk is the *proof of the drain*: the node's
own log says its state was fully written before it exited.

```sh
grep "stop: drained and the drain proven" data/s1-clean-stop-lines.txt | head -3
```

```output
logs/n4.2026-09-19.log:1230: INFO stop: drained and the drain proven; the next boot continues under the same identity node=4 state=/Users/Shared/lua-lunet/lunet-locks/.tmp/telemetry/local-polite2-2026-09-19/state/n4.state
logs/n3.2026-09-19.log:7110: INFO stop: drained and the drain proven; the next boot continues under the same identity node=3 state=/Users/Shared/lua-lunet/lunet-locks/.tmp/telemetry/local-polite2-2026-09-19/state/n3.state
logs/n3.2026-09-19.log:40718: INFO stop: drained and the drain proven; the next boot continues under the same identity node=3 state=/Users/Shared/lua-lunet/lunet-locks/.tmp/telemetry/local-polite2-2026-09-19/state/n3.state
```


**What you are looking at:** "the next boot continues under the same
identity" is the whole claim in one sentence. The node wrote *stopping*,
flushed every piece of replication state to disk, wrote the final
marker, and exited. Nothing was lost; nothing is ambiguous. The paper
calls this the boot-gate contract: two rounds of marker writes with
the flush strictly between them.

### The measured walk: anchors from the run

The runner's own anchors record each rotation end to end:

```sh
grep -E "S1-|S3-" data/s1-s3-anchors.txt
```

```output
12:anchor=1789802904552 S1-begin target=n1 leader=n2
13:anchor=1789802904552 S1-TERM-begin n1
14:anchor=1789802904602 S1-TERM-done n1 exit=0
15:anchor=1789802906941 S1-restart n1
16:anchor=1789802922559 S1-rejoined n1 state=normal leader=3 view=35
18:anchor=1789802930570 S1-begin target=n3 leader=n2
19:anchor=1789802930573 S1-TERM-begin n3
20:anchor=1789802930623 S1-TERM-done n3 exit=0
21:anchor=1789802952940 S1-restart n3
22:anchor=1789802973957 S1-rejoined n3 state=normal leader=1 view=84
39:anchor=1789803018776 S3-begin target=n1 (the leader)
40:anchor=1789803018776 S3-TERM-begin n1
41:anchor=1789803018826 S3-TERM-done n1 exit=0
42:anchor=1789803041214 S3-restart n1
43:anchor=1789803084205 S3-rejoined n1 state=normal leader=3 view=146
```


**Reading the anchors:** each rotation is five anchors — begin,
signal sent, process exited (always `exit=0`: the polite stop worked),
restart, rejoined. A few things to notice as a student of the
protocol:

1. **The stop is fast and constant.** `TERM-begin` to `TERM-done` is
   50 ms every time — that is the two marker-write rounds plus the
   flush. This is the only place in the whole system where the disk
   sits on the critical path, and it is deliberately placed where no
   client is waiting for an answer.
2. **The restart takes longer, and that is honest.** n3 rejoined
   `normal` 21 seconds after its restart; n1 (the S3 leader rotation)
   took 43 seconds. The restarted node must fold the views the live
   cluster produced while it was down — by the second rotation the
   cluster had reached view 146. The tail is the cost of the churn it
   missed, walked at the harness's cadence; it is a *liveness* cost,
   not a safety question, and it is the number the paper's E1
   experiment family is designed to measure at sample counts.
3. **The service never stopped.** The runner's steady window and the
   whole ladder ended with `total_client_errors=0` (see
   `data/polite2-anchors.txt`, teardown anchor).

### What this book does and does not claim

It claims: on this lab rig, on this day, the clean maintenance walk
worked exactly as the paper's model describes — polite stops with the
drain proven on disk, same-identity restarts, the service up
throughout. It does not claim the lab code is finished: the rejoin
tail above is real, and the next books show both the protocol's best
number (abdication failover) and an honest account of what is still
broken (the crash-restart block). The paper's claim is about what a
team can build *from* the protocol — the floors, not this harness's
current efficiency.
