# polite2 lab books: the index

*2026-09-19T08:14:17Z by Showboat 0.6.1*
<!-- showboat-id: f50bf9ec-6613-4bfc-81bf-2ad60031e311 -->


## How to read this evidence pack

These lab books are the *executable* record of one day's experimental
run of the uVRR demonstration cluster (the lunet-locks rig, three
nodes on one host, 19 September 2026). Every number in every book is
produced by a command printed right next to it, run against the raw
run artefacts; you can re-run any of them. The books are written for
a student, not an expert: each one explains what the experiment was,
shows the raw lines, and says plainly what it does and does not prove.

### The two thought experiments, and where each step is evidenced

**Thought experiment 1 — the maintenance walk.** A stable cluster
needs security patches on every host. Each non-leader is stopped
politely (the node flushes its state and records a clean stop on its
boot fence, restarts, reads the fence, keeps its identity, asks the
cluster for what it missed). The leader goes last: it is asked to
*abdicate* — to propose its own succession — and the patched
successor takes over for the price of one consensus round-trip. Then
the old leader gets the same polite stop-and-restart. No disk flush
ever sits on the path a client waits on.

- step: polite stop, drain proven, same-identity restart — **book 01**, seen working end to end.
- step: abdication failover — **book 02**, 26–30 ms per cycle, three cycles, zero client errors.
- contrast: the detector-driven takeover — **book 02**, 20,370 ms on identical knobs.

**Thought experiment 2 — the unannounced death.** A node's kernel
dies; the supervisor restarts the process instantly; the boot finds
no clean-stop marker, so the node *cannot* come back as itself. It
bumps to a fresh identity, announces the replacement, and the leader
folds it back in with one fused reconfiguration round — 2×RTT, no
disk writes on the path, only the boot-fence read.

- step: dirty classification and identity bump — **book 03**, 3/3 crashes.
- step: the announcement on the wire — **book 03**, flight-tape evidence.
- step: the fused walk — **blocked on this rig** (book 03, named
  refusal, host identity-mapping defect, localised); the same
  machinery measured healthy in **book 04** (28 ms decrement round).
- the blunt shapes: whole-cluster reboot and live member swap —
  **book 04**, 2.6 s and 23.5 s, service up throughout.

### The commit lineage — what code produced this data

| component | identity |
|---|---|
| the rig's code at run time | lunet-locks `7ff3f12` (flight-recorder build; disclosed dirty — the abdication wiring was in flight) |
| the tag of record | lunet-locks `20260919_paper_results` (= `3631f6b`: runner, verb, paper sources) |
| the uVRR engine | uvrr-core release v0.7.0 (`317f4fc`) |
| the paper | lunet-locks `papers/mach/paper.tex` |
| the experimental supplement | lunet-locks `papers/supplement-s1-polite2.tex` |
| the tape readers | `skaffold_flight_tape` / `skaffold_aof_tape` (built binaries archived with the snapshots) |

### The evidence classes used in these books

- **seen working end-to-end on this rig** — the step ran, the data
  is in the snapshots, and the command beside the claim reproduces it.
- **blocked-with-evidence** — the step did not complete on this rig;
  the failure is localised, its diagnostics are in the data files, and
  its safety properties are stated and checked, not assumed.
- **structural** — a property of the construction (e.g. "no disk
  writes on the crash path") that follows from what the code can do,
  verified by inspection of the paths the run exercised.

### The honest summary, in one paragraph

On 19 September 2026 the maintenance half of the paper's story ran
end-to-end with the service up: polite stop/starts, three abdication
failovers at 26–30 ms, a full-cluster reboot serving in 2.6 s, a live
member swap, and a final teardown with zero client errors. The crash
half classified and announced correctly three times out of three, and
wedged in the harness's rejoin walk — a named, localised,
host-layer liveness defect whose evidence is preserved, whose safety
properties were checked on the tapes (nothing committed was lost, no
superseded identity voted), and whose repair is the downstream
repository's active work. The paper claims the protocol's floors; the
floors are what these books measure.
