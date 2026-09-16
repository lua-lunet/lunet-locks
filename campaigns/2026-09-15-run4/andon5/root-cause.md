# ANDON 5 — root cause: the phantom acks

(Follow-up 2026-09-15, post-d378114 — see "Follow-up: the sustain
break" at the end.)

Run 2 (locks2-2026-09-15), lock 14531090, three polite lease-load clients
(800002/800004/800006 on w1b/w2b/w3c). All rig access read-only; cluster
stayed RUNNING throughout.

## The five whys

1. **Why do the clients count ~12.2 bumps/s of `bump_ok` while the
   committed lease renews at ~3.4/s?** Because `bump_ok` counts every
   reply without an `error` field, and the leader's REFUSAL reply —
   `{"op":"set","granted":false,"lease":{the incumbent's live lease},
   "executed_at":…}` — carries no `error` field. Only one client's bumps
   pass the leader's free-or-SAME-HELD check (its holder UUID matches the
   stored lease's); the other two clients' bumps (~8.8/s) are executed,
   refused, and counted as `bump_ok`. (`lease-load.rs:320`,
   `reply_ok` = `error.is_none()`; the refusal shape:
   `ext/advisory_lock/src/locks.rs` `Response::Set` granted:false branch.)

2. **Why do the two refused clients never stop bumping (get_ok stays 1
   forever, set_ok stays 1 forever)?** Because `Contender::absorb`'s
   bump arm — `("bump", true, Some(remaining), _)` — treats ANY errorless
   bump reply carrying a live lease echo as a successful same-holder
   renewal: it keys on `(ok, remaining)` and ignores both the `granted`
   flag and whether the echoed lease names THIS contender (the `holds`
   value is computed at the top of `absorb` and is unused in that arm).
   A refusal is indistinguishable from a grant to exactly the two
   signals the arm reads. The refused client reschedules at the FOREIGN
   holder's `remaining - renew_margin` and re-bumps forever — never
   probing, never racing, never adopting. (`embedded_client.rs:215-222`.)

3. **Why was that arm written without the denial path?** The design
   comment in the SET-race arm says "a lost race corrects on the next
   renewal's denial (the backoff re-probes)" — the correction was
   specified in prose but never encoded: the renewal's denial matches the
   renewal arm before any denial-specific arm exists, and the gate only
   schedules a probe when holdership is withdrawn
   (`client_gate.rs:72-81`: schedule+holder ⇒ Bump, schedule+no-holder ⇒
   Get), while `absorb` never withdraws holdership on a denial. The
   withdrawal half of the correction is missing on both ends.

4. **Why did the bring-up shape guarantee two clients hit the poisoned
   path?** All three clients probe simultaneously against the freshly
   wiped lock (USR2 at 01:09:13Z, each via its own voter's client port,
   forwarded to the one leader), all three see FREE, all three race; the
   Service serializes the races: the first SET is granted (Hold), the
   other two are refused (granted:false + the incumbent's lease echo).
   The race arm then *stakes* holdership on any SET reply — including a
   refused one — and schedules the renewal. From there the two losers
   enter the closed loop of (2). The observed rates match exactly: the
   holder bumps at the renewal cadence (w3c: 3.25/s, straight line for
   14 min), the two losers at the refusal cadence (w1b 4.59/s, w2b
   4.31/s — faster because `remaining - margin` can fall in the past and
   re-fire immediately against a 500 ms window renewed every ~295 ms).

5. **Why does the committed state look coherent while all this happens
   (one holder, clean renew cadence, three voters agree)?** Because the
   server side is correct: the free-or-SAME-HELD check compares the
   candidate's holder UUID against the stored lease's holder UUID, and
   the three clients draw three DISTINCT holders (proven from the
   standby AOF: 800002 → `…fb844ecbc36bed71`, 800004 →
   `…fb84490eeba178e5`, 800006 → `…fb843133094f979d`). Only same-holder
   SETs renew; refusals commit as no-op operations and never touch the
   lease record. The committed state accounts for every grant it ever
   made; the clients' `bump_ok` counter was counting refusals. The
   break is entirely client-side: a lease ECHO was conflated with a
   lease GRANT.

## Evidence trail (what was pulled, what it shows)

- **Live GETs via each voter's 19301 (01:17Z)**: one coherent holder
  `00000000-0000-0000-fb84-3133094f979d`, taken_at 1789434690175,
  renew_count 1206→1212 at ~3.1/s across ~1 s probes, lease_id
  1594→1600 in lockstep. The holder string is client 800006's
  `{:032x}` xorshift draw (AOF-proven below).
- **per-host load.json (every 2 s window, 01:07:47Z→01:23:48Z)**: each
  client does EXACTLY one get (get_ok=1), one set (set_ok=1), then
  bumps forever. w1b 4.59/s, w2b 4.31/s, w3c 3.25/s — straight lines,
  no regime change in 14 minutes. No second probe, ever.
- **standby dc1 AOF locks series (02:23-02:25Z window, 16,743 events)**:
  1,604 committed SETs on 14531090 in 131 s ≈ 12.2/s — the three
  clients' combined bump rate — with THREE distinct candidate holders,
  one per client_id; 15,139 committed GETs on the sentinel lock
  14531089 (the voters' own driver noise — see "adjacent findings").
  Zero reconfiguration events in the window.
- **identity arithmetic**: the live lease's `lease_id - renew_count` =
  388 invariant; w3c's client had built ≈388 sets when it (re-)took the
  lease at 01:11:30Z, and its counter at my probes (≈1584) matches the
  stored lease_id (1594-1600) within in-flight slop — the holder is
  w3c's client, and its pre-take ~127 s were spent in the same refused
  bump loop (it lapsed into holdership when an earlier holder's lease
  expired under it and its next refused-shaped bump landed on the
  expired record — granted as a fresh take).
- **era/view history (candidate line 3)**: the voters' nohup logs are
  empty (no tracing subscriber output reaches them; the standby nohups
  are AOF debug spam only), and the AOF history before 02:23Z is rolled
  away, so the join-phase flap has no surviving server-side log. What
  survives: the join-phase NOT_LEADER churn is real (operator-recorded),
  but every surviving observation of the lease state — three voters'
  GET replies agree at every probe, one contiguous committed log
  (monotone slots 125522→142254 in the AOF window), one holder at a
  time with a clean renew cadence — is consistent with ONE coherent
  leader the whole client phase. No split-brain is needed to explain
  the anomaly, and nothing in the surviving evidence shows one.
- **candidate line 2 (duration-lease grant identity)**: exonerated by
  the AOF — three distinct candidate holders, and the committed lease's
  holder never oscillates across probes (if the same-held check passed
  foreign bumps, the stored holder would flip ~12/s and probes 1 s
  apart would disagree; they never did).
- **candidate line 4 (GET path)**: the lease-client GETs are answered
  from the one leader (each voter's client port forwards lock verbs;
  the coherent replies prove it), and the losers' bumps ARE forwarded to
  that same leader and refused there — the refusal reply is the phantom
  ack, so forwarding is not implicated.

## The fix (complete line)

1. `embedded_client.rs` — the denied renewal arm: a bump reply whose
   lease echo names a foreign holder withdraws the gate's holdership and
   backs off (100-300 ms), so `next_op` schedules a GET probe — the
   correction the SET-race arm's design comment always promised. The
   granted-renewal arm now requires the reply to name THIS contender.
2. `lease-load.rs` — the stats layer counts granted outcomes, not
   completed round trips: `bump_ok`/`set_ok` require `granted:true` for
   set/bump replies (new `reply_granted` helper in `embedded_client.rs`;
   `reply_ok` keeps its "completed round trip" meaning for the
   Contender's transport-error logic).

## Adjacent findings (not this break, flagged for follow-up)

- The sentinel lock 14531089 sits with a lease the voters' own embedded
  drivers poll at ~38.5/s per host (15,139 GETs in 131 s, zero SETs in
  the window) — a driver-side hot loop or a stale long-expiry lease;
  separate line, not touched here.
- dc2-tel's AOF stream stopped writing at ~01:27Z (files frozen, then
  rolled away); dc1-tel and dc3-tel kept writing. The w2b voter itself
  serves fine (its 19301 GET answered coherently). Standby telemetry
  gap only.
- Someone signalled the clients between 01:23:48Z (last load.json window
  in my dump) and 02:23Z (AOF shows request_num/lease_id reset while
  cumulative-style bump volume continued) — a silence+restart in that
  gap; the bump-loop pathology resumed identically after it, per the
  AOF window.

## Is run 2's data salvageable?

No — as a lease-correctness measurement it must be repeated after the
fix. The cluster's committed history is internally coherent (the
leader never double-granted; the state machine's invariants held), but
the run's client-side ack accounting is ~72-95% phantom (bump_ok counts
refusals as acks), the two non-holders were absent from the chase
design's probe cadence for the whole run, and every "handover" observed
was the bug's own artifact (a stuck client's bump landing on a lapsed
lease) rather than the designed probe→race takeover. The polite
rotation data the run was meant to produce (takeover latency through
the probe→race path) does not exist in it.

## Verification (the fix + the defensive tests)

Fix files:
- `examples/lease-sequencer/src/embedded_client.rs` — the denied
  renewal arm (withdraw the stake, back off, re-probe); the granted
  renewal now requires the reply's lease to name THIS contender; new
  `reply_granted` helper.
- `examples/lease-sequencer/src/bin/lease-load.rs` — the stats layer
  counts granted outcomes for set/bump (`reply_granted`), so `bump_ok`
  can never again claim renewals the committed state cannot account
  for.

Defensive tests (all verified to FAIL on the old bump arm and PASS on
the new):
- `embedded_client::tests::denied_renewal_withdraws_the_stake_and_reprobes`
  — the run-2 loser's exact reply bytes (`granted:false`, the
  incumbent's lease echoed, no error field).
- `embedded_client::tests::holder_denied_a_renewal_also_reprobes` —
  the lost-grant (leader-flap) entry state.
- `embedded_client::tests::runner_denied_renewal_returns_to_probing` —
  the in-process runner path the sequencer host's embedded clients take.
- `embedded_client::tests::granted_outcome_semantics_separate_completions_from_grants`
  — the stats-layer semantics (the refusal is `reply_ok` yet never a
  granted bump).
- `stage4_three_clients_race_one_free_lock` (uds_harness_test) — the
  end-to-end replay: three contenders started together against one
  free lock; the losers' op mix must show a GET after the denied
  renewal (old code: `["get","set","bump","bump",…]` forever — the
  exact rig shape — and the paused-holder takeover arrives as a blind
  renewal with `sets=1`; new code: re-probe at the polite floor and a
  takeover through the probe→SET race with `sets>=2`). The scenario's
  verdicts key on the trace and op ordering, never wall-clock bounds,
  so a loaded host cannot flake its truth.

Suite states (examples/lease-sequencer):
- lib unit tests: 31/31 green (27 before + 4 new).
- bridge_test 10/10, embedded_client_test 1/1, phi_detector_test 13/13,
  telemetry_test 16/16, uds_playback_test 6/6 (rig-corpus playback),
  signal_gate_test 0+2 ignored (as before), bins 22/22.
- uds_harness_test: stage1 ✅, stage2 ✅, stage4 ✅; stage3 ❌ —
  PRE-EXISTING at baseline on the UNMODIFIED tree (two baseline runs,
  both red, different verdict mixes: heartbeat-gap 473 ms; then
  takeover=None + grants_after_resume=3 + RTT 8). With the fix under
  the same sustained machine load (load avg 3.4-4.4, shared host) it
  stays red in the same latency-verdict class (RTT bucket, heartbeat
  gap 333-413 ms, a lapsed-lease "steal" during commit stalls — all
  shapes already observed pre-fix). The test file's own disposition
  note (2026-09-14) records this flake class as
  resolved-unreproducible-under-normal-load. Not this break's line;
  not modified here.
- ext/advisory_lock: 90/90 green. ext/lunet-locks-aof: 29/29 green.
- `cargo build --release` (run.sh's shipped form): green.


---

# Follow-up: the sustain break after d378114 (2026-09-15)

The phantom-ack fix held (bump_err counts refusals; the losers re-probe
at the floor), but the lease stopped SUSTAINING: rotations every ~2 s
per client (set_ok 27/29/30 at ~60 s), bump_ok ≈ set_ok (one granted
renewal per tenure, then lost), the timeline showing 888 SET rows vs
65 same-holder pairs, and a coherent-but-rotating lease (live GETs
minutes apart: holder fbe1-fa25… renew_count 0, then holder
fbe1-fa8d… renew_count 1).

## The five whys

1. **Why does the lease rotate instead of sustaining?** Every GRANTED
   renewal is absorbed as a denial: the renewal arm added in d378114
   requires `reply_holds(reply, &self.holder)` — a string comparison of
   the reply's `lease.holder` against the Contender's identity — and
   the two strings are the same 16 bytes in different encodings, so the
   match never fires; the denial arm withdraws the stake, the holder
   stops renewing its own lease, the lease lapses, and the next prober
   takes it.

2. **Why do the encodings differ?** The Contender drew its identity as
   `format!("{:032x}", rng.next())` — the bare 32-hex form
   (`0000000000000000fb843133094f979d`) — while the leader's
   `Response::Set` echoes `lease.holder` as a `uuid::Uuid` serialized in
   the hyphenated form (`00000000-0000-0000-fb84-3133094f979d`). The
   REQUEST's holder string is parsed into a `Uuid` either way (serde
   accepts both), so the wire round-trip was silent: the mismatch only
   surfaced at the client's comparison.

3. **Why was the mismatch latent until d378114?** Nothing keyed on the
   match before: the pre-fix bump arm ignored `holds` entirely (that
   was the phantom-ack bug), and the own-lease re-adopt GET arm was
   unreachable in practice. d378114 made `holds` load-bearing for
   renewals — correct in principle — and the first granted renewal on
   the rig exposed the encoding split. The sequencer host's own driver
   (main.rs) holds its identity as a real `Uuid::new_v4()`, so it never
   had this bug — the mismatch was exclusive to the Contender's draw.

4. **Why did no test catch it?** Every existing unit test built replies
   with `contender.holder()` — the client's own string — so the
   comparison was tautologically true in whatever encoding the draw
   produced; the test seam replayed the client's encoding, never the
   server's. The e2e stages asserted op MIX (re-probe after denial,
   takeover path) but never SUSTAIN — a holder that discards its own
   renewals still re-probes politely, races honestly, and takes over
   through the probe→SET path, so stage4's verdicts stayed green while
   no tenure survived.

5. **Root: the identity is one UUID held as two string encodings, and
   the coverage never pinned a sustained tenure.** The fix draws the
   identity directly in the wire's canonical UUID form
   (`Uuid::from_u64_pair(0, draw).to_string()` — same 64 bits of
   entropy, the exact string the leader echoes), and the defensive
   coverage now includes a sustain test: the holder absorbs four
   consecutive renewals executed by the REAL `Service` (the leader's
   own reply bytes), never withdrawing, never building a second SET,
   until an external silence.

## The double-grant question (exonerated)

The "two acquires 625 µs apart, both committed" rows are a grant plus a
refusal: the timeline tool labels every committed SET by the
`client_id` that offered it, and a refused SET commits too (as a
no-op). The Service executes sequentially — the first grant installs a
live 500 ms lease and the second SET, 625 µs later, fails the
free-or-same-held check (the live GETs show `lease_ms: 500` honored,
expiry = taken + 500). The rig's set_ok counters (27-30/min per
client) vs the timeline's SET volume (~84/min per client) confirm ~2/3
of the SET stream was refused races.

## Fix (this round)

- `examples/lease-sequencer/src/embedded_client.rs` — the identity
  construction: `Uuid::from_u64_pair(0, rng.next()).to_string()`, the
  wire's canonical hyphenated form, so the reply-absorb comparison is
  like-for-like; plus the sustain unit test
  `the_holder_sustains_renewals_against_the_real_service`.
- `examples/lease-sequencer/src/uds_harness.rs` — the stage4 sustain
  verdict ("the holder sustains: renewals only, no rotation": the
  winner's granted sets ≥2 from renewals alone, the losers zero grants
  in the window) and the takeover wait widened to 20 s with its
  liveness-vs-discriminator roles stated (a pause coinciding with a
  phi-suspicion view storm makes the contenders probe through
  not_leader backoffs until a leader re-stabilizes; the verdict's truth
  remains the successor's op mix).

## Verification

- `the_holder_sustains_renewals_against_the_real_service`: RED on
  d378114 (the first granted renewal — `granted:true, renew_count:1`
  in the Service's own reply — fails "keeps the tenure" because the
  hyphenated holder echo `00000000-0000-0000-37c5-…` does not match the
  bare `000000000000000037c5…` draw), GREEN after. This is the
  deterministic deliverable.
- stage4 e2e on d378114: red (the re-entered winner instantly
  re-acquires — `grants_after_resume=2`, the rotation's signature).
  After the fix: all verdicts green, including "the holder sustains
  (renewals only, no rotation)" with winner_granted_sets=8,
  others=0.
- Suites: lib 32/32; bridge 10/10; embedded_client 1/1; phi 13/13;
  telemetry 16/16; uds_playback 6/6; bins 22/22; stage1/2/4 green
  under load (load avg 3.3-4.4); `cargo build --release` green;
  ext/advisory_lock 90/90 (untouched). stage3 remains red in its
  pre-existing latency-verdict class under this machine's sustained
  load (heartbeat-noise-floor / RTT-bucket verdicts; recorded
  disposition in the test file; unmodified by this work).

## Can this run be salvaged?

No. The rotation regime (every tenure ≤1-2 s, one granted renewal per
tenure) produced no sustain data at all — no renewal cadence, no
takeover-through-expiry measurements, no rotation-free windows. The
cluster's committed state stayed correct and single-holder (the
Service never double-granted); the clients' counters are now truthful;
but the experiment's object — a sustained polite tenure until the
external kill — never occurred. Rebuild the rig with this fix and
repeat the run.
