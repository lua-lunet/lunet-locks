# Phi failure detection and the timeouts

Phi-accrual failure detection, leader-election timeouts, and the cluster
viewchange timeout are three distinct concepts with three distinct
mechanisms. This page states what each is for, how they are wired, and
the rules every test and configuration must respect. The naming law
governs every doc, log line, config key, and test name:

- **timeout** unqualified is short for **leader timeout** — the
  phi-based leader-failure timeout.
- **cluster viewchange timeout** is the separate, randomized poll timer
  a node runs while it is participating in a view change.

## Phi is a steady-state leader-failure detector

The phi detector exists for LOW-LATENCY detection of a STABLE leader's
crash or partition. It is not a view-change timer and it never measures
election costs. The 99.99% case is one datacenter link down or one rack
down for minutes or hours; phi's job is to notice that a stable leader
stopped committing, quickly, while everything else is quiet.

By definition: **when a node estimates the leader is dead it no longer
runs the phi detector until it sees a fresh commit message.** The
moment a node times out on the leader and issues a view-change
suggestion or a positive vote, it toggles its timeout behaviour to
`timedout = true`:

- the phi timer checks `if not timedout` before doing anything — phi is
  NEITHER updated NOR checked while the toggle holds;
- phi is never updated for leader-election costs: a view change's own
  latency is not leader-failure evidence, and feeding election-phase
  silence into a sketch makes every rolling reconfiguration look like a
  dead leader.

When the leader finally emerges and a commit arrives, the toggle flips
to `timedout = false` and the next tick of the phi timer resumes. A
node that re-elected the same leader may be pleasantly surprised — the
partition healed and the SAME leader returned; that is all good, and
the sketches restart clean for it.

## Phi-sketch protection across failover

The gap between the old leader's last commit and the new leader's first
commit is not a heartbeat interval under one leader — it is a
failover's cost, and it must NOT enter the sketch. This service takes
approach (a): **skip the non-adjacent update.**

The choice, and why: the sketch API (`phi::Table::observe(key, at_ms)`)
takes arrival timestamps only and measures the interval from the
previous arrival on the same sketch key. The honest way to skip a
non-adjacent gap with that API is to not observe across it, which the
host does with three mechanisms that compose:

1. The sketch table is keyed per (era, leader, addr, monitor): a
   leader or era change re-keys the table, and the fresh sketch's first
   arrival only seeds the clock.
2. While `timedout` holds, no heartbeat observation feeds the sketch at
   all.
3. On the fresh-commit resume, the host resets the live sketch before
   the next observation, so the first post-resume arrival only seeds —
   the gap interval is never learned.

(The rejected alternative, (b), was to observe the gap and let the
sliding window decay it: detection would just be twitchier until
stability, but the exported phi trace would show a large variance spike
across every failover — an artefact the run analysis would chase
forever. With (a) the phi floor stays a clean steady-state
network-hop measure.)

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
the phi timer: the phi timer stands down (`if not timedout`), the
viewchange timer takes over, and a fresh commit disarms the poll.

Defaults: `min = 100 ms`, `max = 200 ms` — a 150 ms average poll. The
bounds are config (`--viewchange-timeout-min-ms`,
`--viewchange-timeout-max-ms`) and validated `min <= max`.

Too-low values cause view-change storms — hence the randomisation: a
synchronised fleet would otherwise re-poll in lockstep and stampede
every recovery. The floor is an RTT law: **the viewchange minimum must
be greater than 4x RTT.** At the deployment's 20 ms under-load RTT that
is exactly the 100/200 default.

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
