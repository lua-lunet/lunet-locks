# Failure detection and the timeouts

The leader-failure detector, the leader-election timeout, and the
cluster viewchange timeout are three distinct concepts with three
distinct mechanisms. This page states what each is for, how they are
wired, and the rules every test and configuration must respect. The
naming law governs every doc, log line, config key, and test name:

- **timeout** unqualified is short for **leader timeout** — the
  leader-failure timeout: the randomised wait every node runs against
  its leader.
- **cluster viewchange timeout** is the separate, randomized poll timer
  a node runs while it is participating in a view change.

## The leader timeout

Failure detection in this service is the flavoured-timeout model
([the timeout policy](timeout-policy.md) for what the core's matcher
returns at each timeout) and the randomised leader timeout for the
durations the host runs them with. The host watches a leader over a
uniform random wait in `[--leader-timeout-min-ms,
--leader-timeout-max-ms]` (defaults 500 / 1000 ms) per watched
(config era, leader) key, re-armed on leader evidence — the watched
key's birth (a leader change or a re-keying era), the fresh-commit
resume, and each heartbeat Commit arriving from the current leader.
When the deadline passes, the host drives the actuation:

- the `leader-timeout-detect` note (the observed silence and the armed
  deadline);
- the §14.2 host-forced view change (`force_view(era, view + 1)`, the
  ordinary `leader_timeout` tick on refusal);
- the `timedout` toggle (`suspect`);
- the output flush.

Every build names its compiled-in detector on the boot line and in the
boot trace JSON: `detector=sloppy-timeout`.

The detection latch is one detection per (era, leader) while the node is
`Normal`. Inside the view-change limbo the detector stands down and the
cluster viewchange timeout takes the polling, whose drive is the
retransmit of the attempt already armed.

The timing law is Raft's (Ongaro, "In Search of an Understandable
Consensus Algorithm", 2014 §4.2.3): broadcastTime ≪ electionTimeout ≪
MTBF, with the election timeout RANDOMISED in a generous fixed interval
— the canonical 150–300 ms against a 0.5–20 ms broadcast, 10–20× the
broadcast time. The randomisation IS the liveness mechanism (a
synchronised fleet must never time out in lockstep and stampede the
election); the generosity IS the stability mechanism (a fixed timeout
measurably causes split votes and election stalls). The evidence on
the deployment hardware: UDP loopback RTT 0.042 ms and fsync p50 0.023 ms —
three orders of magnitude below the observed heartbeat-arrival jitter
(12–23 ms against a 5 ms heartbeat, event-loop tick quantisation) — so
a generous randomised wait covers the noise floor by construction.

The profile table:

| profile | heartbeat | leader timeout (uniform random) | rationale |
|---|---|---|---|
| prod/cloud deploy | 200 ms | **2000–4000 ms** (10–20× heartbeat) | RTT tens of ms + fsync ≤ ~10 ms ≪ 2 s; ≪ MTBF by orders of magnitude |
| local in-memory harness | 5 ms | **250–500 ms** | measured jitter ≤ ~25 ms; 250 ms ≥ 10× jitter |

The shipped defaults stay the 500/1000 knobs (the local rig's profile
at 5 ms test heartbeats); a cloud deploy at the 200 ms heartbeat sets
`--leader-timeout-min-ms 2000 --leader-timeout-max-ms 4000`. This
repo's UDS harness pins its own knobs at 3000/5000 — its switch fabric
is the test thread, and a scheduler stall of that thread manufactures
wire silence (a 422 ms frame gap churned the view 1→16), so its floor
must exceed the stall a loaded host produces.

## The timeout is a steady-state leader-failure detector

The leader timeout exists for LOW-LATENCY detection of a STABLE
leader's crash or partition. It is not a view-change timer and it never
measures election costs. The 99.99% case is one datacenter link down or
one rack down for minutes or hours; the timeout's job is to notice that
a stable leader stopped committing, quickly, while everything else is
quiet.

By definition: **when a node estimates the leader is dead it no longer
runs the detector until it sees a fresh commit message.** The moment a
node times out on the leader and issues a view-change suggestion or a
positive vote, it toggles its timeout behaviour to `timedout = true`:

- the detector timer checks `if not timedout` before doing anything —
  the detector is NEITHER re-armed NOR checked while the toggle holds;
- the detector is never re-armed for leader-election costs: a view
  change's own latency is not leader-failure evidence, and charging
  election-phase silence to the leader would make every rolling
  reconfiguration look like a dead leader.

When the leader finally emerges and a commit arrives, the toggle flips
to `timedout = false` and the next tick of the detector timer resumes.
A node that re-elected the same leader may be pleasantly surprised —
the partition healed and the SAME leader returned; that is all good,
and its detector restarts clean for it.

The failover gap is skipped the same way: the gap between the old
leader's last commit and the new leader's first commit is not a
heartbeat interval under one leader — it is a failover's cost, and it
must not be charged to the next leader's detector. The host does so by
re-arming only on the fresh-commit resume and on the new key's birth,
so the gap interval never enters the deadline's evidence.

## The toggle logging

Every timeout toggle is recorded with the local clock: the new state
(`timedout` true/false), the toggle's current ts, and the ts of the
LAST toggle (kept in memory). The record lands in BOTH surfaces:

- the regular log (`info!` from the adapter, and the host's run log —
  `timedout=<bool> ts=<ms> last_toggle=<ms|none> why=<site>`);
- the Flight Recorder as one `timeout-toggle` event per toggle (the
  same detail fields), alongside the other internal events.

## The cluster viewchange timeout

A node that has SENT a view change or view votes is BY DEFINITION not
talking to the leader it suspects dead. While `timedout` holds, it
POLLS on that with its own fixed, RANDOMISED timeout:

```text
delay = min_ms + rand() * (max_ms - min_ms)
```

so a node does not get stuck when the network drops its view-change
messages. It polls on a libuv/host timer that is a DIFFERENT timer from
the detector timer: the detector timer stands down (`if not timedout`),
the viewchange timer takes over, and a fresh commit disarms the poll.
The delay law is the same `timeouts::random_wait_ms` both the leader
timeout's deadline and the viewchange schedule arm with.

What a due poll drives inside the limbo is the RETRANSMIT of the attempt
already armed: the same ballot, the same `StartViewChange` fence votes
and the same `DoViewChange` evidence, re-sent byte for byte. The attempt
is volatile and nothing else re-asks it, so a poll that fired a fresh
view change instead would manufacture an attempt the protocol never
asked for and inflate the view for as long as the leader stays silent.
The view number does not move on a retransmit. Outside the limbo the
poll's drive is the ordinary suspicion tick. The poll's own log line
carries the matcher's opinion for the pair (`opinion=retransmit`), the
`relayed` count of datagrams re-sent, and the `view` it held — a view
that repeats across polls is the property, and a `relay-out` line per
re-sent datagram says what went out.

Defaults: `min = 100 ms`, `max = 200 ms` — a 150 ms average poll. The
bounds are config (`--viewchange-timeout-min-ms`,
`--viewchange-timeout-max-ms`) and validated `min <= max`.

Too-low values cause view-change storms — hence the randomisation: a
synchronised fleet would otherwise re-poll in lockstep and stampede
every recovery. The floor is an RTT law: **the viewchange minimum must
be greater than 4x RTT.** At the deployment's 20 ms under-load RTT that
is exactly the 100/200 default.

## The heartbeat of a commit

The heartbeat is the leader's LAST COMMIT re-announced on the heartbeat
cadence, and it is one of the matcher's options on a seated member's
steady quiet ([the timeout policy](timeout-policy.md)): the frontier
announcement and the proof of life in one datagram. A leader whose last
interval carried no Commit of its own sends the commit it last released,
byte for byte, to every peer it released it to; nothing else goes out.

Both hosts take it that way. The Rust host keeps the released datagrams
in the relay ledger (`examples/lease-sequencer/src/relay.rs`) and the
leader's idle beat re-announces the newest commit per peer from it; the
Teal host keeps the same frontier per peer as `flush` sends
(`src/server.tl`) and its `heartbeat_loop` re-announces it while no
Commit has gone out in the last interval. Neither opens a client
transaction, so an idle leader's beats advance no journal slot at all: a
run with an idle leader and N beats advances the replication journal
zero times, which `examples/lease-sequencer/tests/relay_test.rs` counts
in the journal's own entries.

What it buys is idle-alive evidence. A backup's suspicion gate reads only
same-view `Prepare` and `Commit` from the legitimate primary, so a quiet
leader running the heartbeat is not deposed while a fully silent one
eventually is. What it costs is datagram flow: one commit's worth per
peer per beat, and nothing else. The beat is an option and not a
mechanism the protocol requires — a deployment that prefers silence
keeps the suspicion instead, and both are correct.

The log lines are `heartbeat-commit` on the leader (the ballot, the
frontier slot and the peers the commit went to) and `commit-in` on every
follower that reads it, with a `relay-out` line per re-sent datagram
beside them, so a re-announce is distinguishable from a first send.

## The nemesis testing rules

Interruption testing (nemesis, rolling partitions, process kills) obeys
the same RTT arithmetic:

- the interruption interval must be at least **2x the viewchange max
  timeout**;
- the tightest allowed interval is **1x the viewchange max** (never
  below it);
- the viewchange minimum must stay above **4x RTT**.

With RTT 20 ms and the 100/200 viewchange defaults: nemesis intervals
target >= 400 ms and never go below 200 ms. The softball and polite
run-sheets' phases already sit at >= 10 s sleeps, far above the floor;
the run-sheets carry the note.