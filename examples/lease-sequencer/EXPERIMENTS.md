# lease-sequencer experiments — E1 (CSR kill/rejoin latency) and E2 (disk modes at the recovery boundary)

The local experiment harness for the uVRR advisory-lock cluster, implementing
the upstream experiment design (`docs/uvrr-experiment-design.md` on
`lua-lunet/uvrr-core`): the diskless claim under Crash-Stop-Reincarnation
measured as node-kill rejoin latency (E1) against forced-flush baselines at
the recovery boundary (E2). The decision rule governs both:

> If the measured CSR rejoin latency (E1) is **slower** than the forced-flush
> baseline (E2 variant 1) at the same sample count, the diskless claim
> **collapses**. Success is CSR rejoin latency matching or beating the
> forced-flush baseline.

## What runs

`experiment.sh` starts the **genesis 3-voter cluster** (the three DC primary
nodes; every voting node is a genesis member), soaks it to steady state,
starts the continuous SET/BUMP/GET load client, then runs `k` kill→rejoin
cycles. Per iteration it SIGKILLs one **voting non-leader** node (rotated
across the non-leader voting nodes), restarts the process immediately (the
reincarnation: volatile state lost, the dirty boot bumps the identity), and
waits for the reincarnated node to be **voting and serving** again — its
first status note with `state=0` and `voting=1`, the forced
reconfiguration walk complete. Iterations are cold counts: the next
iteration begins only after the rejoin and a soak interval.

Promoted joiners are deliberately absent: their rejoin path is blocked by
the upstream fourth finding (multi-era learner acquisition), so the
experiment shape is genesis-3 only.

## The variants

| Experiment | Recovery-boundary behavior |
|---|---|
| `e1` | diskless (the shipped default) |
| `e2v0` | diskless — no durable writes at the recovery boundary |
| `e2v1` | one 4 KiB block write + `fsync` against the node's scratch dir — the latency baseline, **never a safety claim** (FAST'18) |
| `e2v2` | double-ring write: a 64-byte checksum header + one 64-byte data line to ring one, the header+checksum copy with no payload to ring two; the two zones are 4 KiB-aligned and spaced 1 MiB apart |

Both flush variants carry fake data only and never read back: they measure
write-and-flush latency only. Variant 1 writes exactly one 4 KiB block then
fsyncs; variant 2's geometry (two spaced zones, checksum header copy
without payload) is unit-tested in
`ext/advisory_lock/src/recovery_flush.rs`.

## How to run

```
cd examples/lease-sequencer
./experiment.sh --experiment e1   --k 10
./experiment.sh --experiment e2v0 --k 10
./experiment.sh --experiment e2v1 --k 10
./experiment.sh --experiment e2v2 --k 10
```

Every run writes to its own `run/exp-<experiment>[-<tag>]/` directory:

- `results.jsonl` — one line per iteration: victim, bumped id, kill→serving
  timestamps, `rejoin_ms`, the (era, view) pair at kill and at serving
  (`eras_consumed`, `views_consumed`), and for the flush variants the
  `flush_latency_us` the adapter logged at that restart's recovery
  boundary;
- `summary-<experiment>.json` — the percentile summary
  (p50/p90/p99/max/mean) over the rejoin samples and, for variants 1/2,
  over the flush latencies;
- `load-stats.jsonl` — the load client's stats lines (one JSON line per
  2 s window: window percentiles plus cumulative percentiles, so the last
  line is the summary), and `load-final.json` — its last line.

## The knobs

Runner (`experiment.sh`):

| Knob | Default | Meaning |
|---|---|---|
| `--experiment` | (required) | `e1`, `e2v0`, `e2v1`, `e2v2` |
| `--k` | 3 | kill/rejoin iterations |
| `--soak-ms` | 2000 | soak interval between iterations (cold counts) |
| `--lease-ms` | 500 | the lease window the load client acquires/renews on |
| `--renew-fraction` | 0.5 | the renewal point as a fraction of the window (0.5 = renew 250 ms into a 500 ms window) |
| `--rate` | low | the load client's GET probe rate (low ≈ 250 ms, high ≈ 50 ms per getter) |
| `--tag` | — | suffix for the run directory |

The lease cadence defaults are the committed demo's aggressive timing
(500 ms lease renewed at half the window). The design doc's own numbers
(100 ms lease, renewal at 80%) run exactly with
`--lease-ms 100 --renew-fraction 0.8`.

Load client (`src/bin/lease-load.rs`) — the runner passes these, but the
client runs standalone too:

`--server` (repeatable; the client reconnects across the list),
`--clients` (contender loops that hold/renew/poll the lock),
`--getters` (fixed-interval GET probes), `--rate low|high`, `--lease-ms`,
`--renew-fraction`, `--window-ms`, `--stats-out`, `--lock-id`,
`--id-base`, `--seconds` (0 = run until killed).

Node (`lease-sequencer`): `--recovery-flush diskless|single|double-ring`
with `--recovery-scratch-dir PATH` selects the E2 variant; `--heartbeat-ms`,
`--election-ms`, `--recovery-ms` are the host's timing knobs;
`--embedded-client N --lock LOCK` runs N embedded lock-client contender
loops in-process against the node's own service (the same chase machine,
cadence knobs `--client-ttl-ms 500` / `--renew-fraction 0.5`), booted
silent behind the SIGUSR1/SIGUSR2 client gate.

## Timing measurement

Per §7 of the design: the client measures every operation's round trip on
its monotonic clock, one reading immediately before the request line is
written and one immediately after the reply line is read. Expiry is judged
in the leader's clock at execution time; the leader's reply echoes that
execution tick as `executed_at`, so the client interprets the lease's
remaining window as `expiry − executed_at` against the leader's timeline
without assuming synchronized clocks. The load client schedules its
renewals and polls from that leader-echoed remaining window.

## Note for the upstream doc PR: the design doc's §1.1 is stale against this tree

The upstream design document is the source of record for the experiment
DESIGN, and this harness implements it as written. Its §1.1 component
table, however, describes an earlier shape of the implementation and is
stale against this tree on three points:

- The LAL Peer Protocol's membership fingerprint is the **v3 genesis
  fingerprint** (domain-separated SHA-256 over the length-delimited genesis
  member list, first 16 lowercase hex chars — `config.tl` /
  `transport.tl`), not an earlier fingerprint scheme.
- The restart story is **reincarnation**, not the nonce-recovery story:
  the durable state file is the incarnation marker, a dirty boot bumps the
  identity deterministically (`low + k·2^24`), and the bumped node
  re-announces its superseded identity on every fenced-boot drive. There
  is no recovery nonce to recover with.

This harness does not edit the upstream document; the doc PR against
`lua-lunet/uvrr-core` should fold these three points into §1.1.
