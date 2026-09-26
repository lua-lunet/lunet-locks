# The bench harness

The bench harness is the fast local integration rig: three forked node
processes on real UDP, driven hard, with no sloppy timeouts and no real
disk in the lock protocol's durable path. It exists to answer one
question at machine pace: with no network RTT and no fsync latency, does
the protocol hold — and how fast is it?

```console
make bench          # build the phi + flight-recorder rig and run the bench
```

A run spawns three `lease-sequencer` node processes from a generated
three-line descriptor, loads the cluster with embedded CAS clients on
50 ms leases, walks the lifecycle scenario ladder, and ends with the
oracle verdict. Run state lives under `.tmp/bench-*`; every spawned
process is reaped on every exit path. The build is the
`experimental-phi` + `flight-recorder` shape: the sloppy randomised
timeout is not compiled in — detection is the phi-accrual monitor — and
each node records its flight tape under its run directory.

## The force-fed store

The lock protocol's durable state is the lifecycle marker set (the
superblock quorum plus the compatibility projection) and the committed-
transition sink. On the bench none of it touches a real disk: the
adapter's `LifecycleStore` rides a per-node unix socket to the driver,
which holds each node identity's state in memory and answers every
boot-read, commit, and drain. The node's on-disk obligations — the
quorum write, the projection, the forced I/O ordering — are the real
application's path; the bench substitutes the store, not the protocol:
every call the engine makes still arrives, in the engine's order, and
the driver enforces that order.

The store RPC is synchronous newline-delimited JSON, one request one
reply:

| Request | Driver's answer |
|---|---|
| `read_copies` | the identity's recorded verdict: none (first life), flushed (clean stop), or the running sentinel (a crash) |
| `commit` | recorded, then acknowledged |
| `drain` | recorded, then acknowledged |

The driver is not a passive file: it knows what the scenario signalled
and holds a discipline automaton per node identity. A boot-read is
answered with the flushed state only when the driver saw that identity's
ordered `Stopping` → drain → `Stopped` sequence; after a kill it is
answered with the sentinel. A store call that arrives unsignalled or out
of order — a commit from a node nobody asked to stop, a boot-read from
an identity that should be mid-life, a missing `Stopped` commit on a
clean stop — is a hard harness failure: the run stops and reports the
node, the call, and the expected sequence. The bench never tolerates a
store surprise; a surprise is the bug being hunted.

## The lifecycle controls

The driver owns each node process and drives its lifecycle with signals,
plus the existing TCP admin verbs:

- **SIGUSR1 — the clean cycle, in-process.** The node runs its real stop
  path — the wire closes, `Stopping` commits, the sink drains, `Stopped`
  commits, all over the store RPC — then boots again in the same
  process: the boot-read is answered flushed and the node resumes under
  the same identity. The transport survives; the adapter does not.
- **SIGUSR2 — the dirty cycle, in-process.** The node drops the adapter
  without the stop path — no marker round — and boots again: the
  boot-read is answered with the running sentinel, the classification is
  crashed, and the node reincarnates under the marker's next life.
- **SIGTERM — the clean swap.** The same clean stop path, then the
  process exits; the driver spawns the replacement, whose boot-read is
  answered from the driver's memory — the state outlives the process
  because the driver holds it.
- **SIGKILL — the crash.** No stop path; the driver spawns the
  replacement, which reincarnates off the sentinel.

Leader death is driven two ways, as separate measured steps: the
`abdicate` admin verb (detection-free failover — the lower bound), and
an unannounced kill (phi-detector-driven takeover — the baseline). An
abdication is always confirmed serving before the old leader is stopped.

Partition injection is not in this harness: the loopback cluster's
failure alphabet is stop, crash, and cycles.

## The oracle

Every node journals every committed lock transition it applies (the
event journal: hold, renew, release, break — each record carrying the
lock, the lease id, the holder, and the committed expiry). The journal
is the CAS chain: three replicas applying one replicated log must
produce one chain.

The driver also runs a TCP client per node that continuously reads the
lock and records what clients actually observe.

At the end of a run the oracle checks, and exits nonzero on any
violation:

1. **Chain equality** — the three journals' transition sequences are
   identical (records compare on their semantic content; the apply
   timestamp is each node's own).
2. **Client consistency** — every transition a client observed appears
   in the chain, in order; no client ever saw a state the chain does
   not contain.
3. **Discipline audit** — every store call in the run matched the
   scenario's signalled expectation (the automata end in their expected
   states).

The journals and flight tapes are the run's trace artefacts: they are
the input the later model-checking classification consumes.

## The scenario ladder

A bench run is a fixed ladder with the client load always on:

1. **Settle** — three nodes boot, elect, and serve; the chains begin
   identical and stay identical.
2. **Abdicate + clean cycle** — the leader abdicates; failover is
   measured; the old leader takes a SIGUSR1 cycle.
3. **Clean swap** — a non-leader is SIGTERMed and respawned; it resumes
   under the same identity.
4. **Crash swap** — a non-leader is SIGKILLed and respawned; it
   reincarnates bumped.
5. **Abdicate + leader swap** — the leader abdicates, is confirmed
   serving, then is SIGTERMed and respawned.
6. **Leader crash** — the leader is SIGKILLed; the phi detector drives
   the takeover; the replacement reincarnates.
7. **Dirty cycle** — a node takes a SIGUSR2 cycle mid-load.

Every step is bounded: a step that does not land inside its deadline
fails the run. The run prints the per-step failover and takeover numbers
as measured — the protocol's own pace with nothing else in the way.
