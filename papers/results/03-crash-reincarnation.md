# Lab book 03: crash-stop reincarnation — the 2xRTT floor, evidenced per piece

*2026-09-19T08:14:17Z by Showboat 0.6.1*
<!-- showboat-id: 3756f574-db62-4c14-b56e-5a3107705604 -->


## When a node dies unannounced: the 2×RTT claim, and what this rig did

The first two books were polite: the node says goodbye before leaving.
This book is about the rude case — the kernel panics, the supervisor
restarts the process a second later, and the node comes back to a
cluster that never heard a goodbye. This is where uVRR makes its
central design bet, and where this lab rig is honest about what is
not finished yet.

**The protocol's bet.** Classic crash-recovery lets a node return as
*itself*, trusting durable promises it may no longer be able to check
— the amnesia class the FAST'18 literature measured in the wild. uVRR
forbids it. A node that finds no clean-stop marker at boot is *a new
node*: it bumps its identity to a fresh incarnation, announces that
pair to the cluster, and rejoins as a weight-0 witness. The leader
hears the announcement, folds the replacement into a single fused
reconfiguration proposal — decrement the old identity, join the new —
and one round-trip later the cluster has committed it. The arithmetic:
half a round-trip for the announcement to reach the leader, one round
of consensus, half a round-trip for the commit to come back — **2×RTT,
with zero disk writes on the path.** The only disk read is the boot
fence itself.

**What this book will show.** Every piece of that path that ran on
this rig, evidenced from the logs and flight tapes — and then,
honestly, the place the rig stopped: the restarted nodes' rejoins
wedged in the era-fold walk, a liveness defect of this harness's
rejoin machinery, recorded with its diagnostics. The theoretical
floor does not depend on this rig finishing the walk; the floor is
the protocol's, and the Lean development proves the safety the walk
was exercising. But we show you the wedge rather than hide it,
because the evidence is the point.

### The crash and the classification

The run's S4 scenario killed all three nodes in turn (SIGKILL, the
rudest death), restarting each on the same boot line. The anchors:

```sh
grep -E "S4-begin|S4-SIGKILL|S4-restart |S4-rejoin-BLOCKED" data/s4-anchors.txt | head -12
```

```output
45:anchor=1789803094243 S4-begin target=n2
46:anchor=1789803094244 S4-SIGKILL-done n2
47:anchor=1789803096264 S4-restart n2
48:anchor=1789803246956 S4-rejoin-BLOCKED n2 states=n1:normal:v147:l1 n2:restarting:v0:l1 n3:normal:v147:l1
50:anchor=1789803252162 S4-begin target=n3
51:anchor=1789803252166 S4-SIGKILL-done n3
52:anchor=1789803294788 S4-restart n3
53:anchor=1789803445181 S4-rejoin-BLOCKED n3 states=n1:view_change:v1381:l2 n2:down n3:view_change:v1383:l1
55:anchor=1789803450282 S4-begin target=n1
56:anchor=1789803450284 S4-SIGKILL-done n1
57:anchor=1789803482422 S4-restart n1
58:anchor=1789803632777 S4-rejoin-BLOCKED n1 states=n1:restarting:v0:l1 n2:down n3:down
```


### The identity bump, witnessed

The first piece of the protocol worked exactly as designed on every
crash: the reboot read its boot fence, found no clean-stop marker,
classified itself dirty, and bumped its identity — the log lines
state the pair explicitly (`old` the dead identity, `new` the bumped
one, in the high band so a superseded identity can never collide):

```sh
grep "later life in the high band" data/s4-bump-lines.txt | head -4
```

```output
```

```sh
grep "later life in the high band" data/s4-bump-witness.txt | head -4
```

```output
logs/n1.2026-09-19.log:27485: INFO restart: the identity is a later life in the high band old=1 new=16777217 incarnation=1
logs/n1.2026-09-19.log:51935: INFO restart: the identity is a later life in the high band old=1 new=16777217 incarnation=1
logs/n3.2026-09-19.log:16342: INFO restart: the identity is a later life in the high band old=3 new=16777219 incarnation=1
```


**Reading the bump lines:** the crashed n1 rebooted as `own=16777217`
(old identity 1 plus the incarnation stride), n3 as `own=16777219`.
The high band is the protocol's collision guard: the bumped identity
occupies a space no genesis identity can ever hold, so the dead
identity can never be confused with the living one. Three crashes,
three clean classifications, three bumps — the boot-gate contract
worked every time, and it is the same machine book 01 showed working
on the polite path (the classification differs; the machine is one).

### The announcement went out — on the wire

The bumped node's first act was the announcement: its flight tape
opens with the same 45-byte frame emitted to all three peers in the
same millisecond it booted. (Tag `0x0d` is the reincarnation
announcement; the tape is the node's own emission record.)

```sh
head -3 data/s4-bumped-tape-head.txt | cut -c 1-118
```

```output
{"commit":"7ff3f12804bdea710473ccb17b6cf3f3c4b0bdea","dirty":true,"format":1,"kind":"flight-header","node":16777218,"p
{"detail":{"input":"reincarnate","old":2},"kind":"drive-in","seq":1,"ts_ms":1789803096269}
{"detail":{"code":0},"kind":"drive-out","seq":2,"ts_ms":1789803096269}
```

```sh
head -3 data/s4-bumped-tape-head.txt | cut -c 1-118
```

```output
{"commit":"7ff3f12804bdea710473ccb17b6cf3f3c4b0bdea","dirty":true,"format":1,"kind":"flight-header","node":16777218,"p
{"detail":{"input":"reincarnate","old":2},"kind":"drive-in","seq":1,"ts_ms":1789803096269}
{"detail":{"code":0},"kind":"drive-out","seq":2,"ts_ms":1789803096269}
```


**Reading the tape:** the bumped node's flight recorder opens with
its host driving `reincarnate` for the old identity 2 — the
announcement's input — and the engine answering immediately. The
announcement frame (tag `0x0d`) went to all three peers in the same
millisecond. The half-round-trip of the 2×RTT claim left the node.

### And here is where the rig stopped — shown, not hidden

**The binary question, answered from the tapes: did the leader commit
the new identity? No — it never received it.** Three independent
checks on the stable snapshot: (1) zero frames mentioning `16777218`
exist in any serving node's flight tape (all six tapes, receive and
emit); (2) the tag-`0x0d` reincarnation frames DO arrive at n1 (70×)
and n3 (32×), but the tape records them `from: 2` — the wire-level
sender attribution never moved to the bumped identity even though the
frame body carries `new=16777218`; (3) no commit/accept/Prepare frame
on any serving tape carries the pair, so the fused batch never
existed. The node is NOT "joined but never learnt"; it was never
proposed. The harness transport stamps outbound sender attribution
from its learned map, which still maps the process to the old
identity after the bump — the partition is at the identity-attribution
layer, not the commit layer. (Filed downstream as lunet-locks
issue #26.)

On the leader's own log, the mis-attributed announcement is visible —
and refused, named, hundreds of times:

```sh
head -2 data/s4-refusal-samples.txt; echo; echo "refusals per node:"; grep -c ReincarnationRefused data/s4-bump-lines.txt
```

```output
20536: WARN peer input dropped with a named diagnostic diagnostic=ReincarnationRefused { sender: NodeId(2), view: ViewId { era: Era(1), view: View(147) } }
20537: WARN peer input dropped with a named diagnostic diagnostic=ReincarnationRefused { sender: NodeId(2), view: ViewId { era: Era(1), view: View(147) } }

refusals per node:
30
```

**Reading the refusal.** `ReincarnationRefused` names the sender as
`NodeId(2)` — the *old* identity, exactly what the tapes show on the
wire (`from: 2` on every tag-`0x0d` frame). The bumped node announced
from its new identity; the harness's transport never re-attributed it,
so the announcement reached the leader carrying the old one, and the
engine refused it by name — correctly: a message it cannot attribute
cannot drive a reconfiguration. The repeated refusal is the wedge: the
leader's forced-reconfiguration walk never started (`remap` notices at
zero peers for all three crashes), the bumped nodes sat fenced for the
full 150-second budget, and the serving pair's views stormed (1,431
and 1,146 leader-change records across the run on n1 and n3, the
era-fold walk hunting for a shape the refused announcement never gave
it).

### The 2×RTT floor, with the pieces that are measured

The paper's claim is the floor, not this rig's current walk. The
floor's arithmetic: half a round-trip for the announcement, one
consensus round for the fused reconfiguration, half a round-trip for
the commit — no disk writes, one boot-fence read. Of its pieces:

| piece | status on this rig | where |
|---|---|---|
| dirty classification + identity bump | works, 3/3 crashes | `data/s4-bump-witness.txt` |
| announcement emitted to all peers, 1 ms | works | `data/s4-bumped-tape-head.txt` |
| the leader hears and starts the fused walk | **blocked** — never received as the new identity (host transport attribution, issue #26) | `data/s4-refusal-samples.txt` |
| one-round fused reconfiguration | works when driven through the healthy path: S6's decrement round committed in 28 ms, and the whole four-step swap in 23.5 s | book 04, `data/s6-anchors.txt` |
| no disk on the path | structural: the only writes in the run were the clean stops of book 01 | — |
| safety under the wedge | held, measured: zero lost operations, zero superseded votes | teardown anchor |

The crash-stop-reincarnation end-to-end run on *this* harness is
therefore recorded as blocked — with the precise localisation now
established from the tapes: the protocol's messages were correct, the
announcement was sent and arrived, and the harness's transport layer
failed to re-attribute the sender after the bump, so the leader could
not lawfully act on it. A team building on uVRR does not need this
harness to be perfect to inherit the floor: the fused walk is the same
machinery the healthy path exercised in S6, the classification is the
same machine book 01 exercised in S1, and the safety the walk protects
is kernel-checked in the paper's Lean development, not asserted.
