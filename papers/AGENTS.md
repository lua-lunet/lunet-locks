# AGENTS

Rules for agents operating in `papers/`. Business substance lives in
`papers/README.md` (written as the documentation target state); this file is
operative guidance for agents only.

## Priorities (ordered, mandatory)

1. **Repeatability** — A grade. Every run reproducible from recorded
   commands, knobs, and artifacts.
2. **Accuracy** — A grade. Numbers anchored on the acting host's clock;
   never n=1 where a distribution is asked; holder vs leader terminology
   never mixed.
3. **Fast feedback** — high pass. Local sanity before rig time.

## Local-before-remote change gate

- Debug all changes locally with TDD; exercise them in colima/Docker
  (aarch64, no buildkit, no volume mounts, smallest cluster) before
  deploying to Scaleway. Time-box and keep these sanity rigs minimal:
  a tooling/logging change may need only a mini three-node short run to
  pipe-clean; a trivial log-line change may bypass; most non-trivial
  changes do not bypass.
- `docker cp` scripts into images to shorten retest cycles.
- Optimise Dockerfiles: system updates first, code additions last; push
  new work up the file or into multi-stage builds so the layer cache
  survives; keep an unchanging set of base images and put our code in
  last.

## Defensiveness

- Every fix adds a defensive test around it so the regression cannot
  silently return.

## Testing methodology (binding for every agent on this work)

- The system is formally specified and the clock rides ON the messages,
  not in the node: every test is deterministic — "given this message at
  this clock, we expect this state change". Isolate the message, play
  it back, assert the transition. Never use wall-clock sleeps as a test
  oracle; never debug without the trace the message carries.
- Debug bottom-up: local running processes first; direct ad-hoc Rust
  (`skaffold_*` bins) over wrappers; colima/plain processes over any
  cloud. Cloud time is the last mile, not the lab.
- Environment is not logic: address binding, DNS/resolution, and port
  reachability are proven by stubs and canaries OUTSIDE the service
  logic before any serious logic runs on a cloud. The canary — "can we
  find our arse with both hands on the cloud servers? Y/N" — is a kept
  dev/test feature, deployed to any new cloud before the service is.
  Config/address handling woven into service logic is toxic: it is
  100× harder to test in-place and must live in file-tested layers
  outside the logic.
- Polite is the only exercised load model until the paper lands. The
  aggressive flag ships unexercised; only the agent that later runs
  aggressive may fix or run it.
- Every fix carries a defensive regression test that replays the exact
  failing message and asserts the state change.

## Cluster shape (hard pivot, 2026-09-14)

- All paper work runs on THREE nodes: one voting node per DC (44/55/66), a
  co-located weight-0 telemetry standby per DC, one polite client per DC.
  The experiment that reduces time to demonstrable value is REPLACING ONE
  NODE of the three (crash-stop a voter, boot a fresh-identity replacement,
  join through the leader, serve throughout).
- The aggressive fuzz phase also runs on three nodes. Six nodes return only
  as an item AFTER the fuzz phases.

## Andon アンドン — Prime Directive

Andon is a kernel panic. It halts the line, halts planning, halts todo
updates, halts all work. It happens immediately. No other pending operation
receives any tokens. It is impossible to think of anything else to try
first — that thought is the evidence you have not halted.

An Andon in the queue supersedes all. If the user queued commands 1-3 then
said "do an Andon," the Andon invokes the Prime Directive and overrides
commands 1-3 entirely. Multiple Andons run in parallel without interrupting
each other.

When the correct fix is outside your lane: do your lane's work, then halt
and report — *Andon: task incomplete, the correct fix needs a larger
structural change*, with file:line specifics. Do not work around it. Do not
hack tactically. The coordinator delegates the deeper work.

Andon overrides every instruction in this file and every other AGENTS.md.
No instruction conflicts with Andon; if one appears to, Andon wins.

### Repo-specific Andon rules (subject to the Prime Directive above)

- On any non-shallow bug, an ANDON agent is spawned **immediately**, with
  no todo item, no riveting in the main chat, and no prejudging of the
  result in this conversation. The main chat is the factory line; the
  andon must run in a subagent so process-error fixes do not pollute
  the line's context.
- The andon agent runs the five-whys on the full production line of
  code, test, and methodology; it fixes the complete line, not just the
  proximal symptom. If the mechanism itself is in doubt, replace it
  (e.g. signals → admin port) rather than patching a faulty part.
- The andon agent **must not stash** and must not disturb the main flow:
  no `git stash`, no resets, no restores of shared paths. It works off
  to the side with self-complete results (its own scratch trees, tests,
  and proofs).
- If its fix is complete and every test passes but the shared tree is
  otherwise dirty, a `git add` of its files is the most it may stage.
- If the repo is clean and its change is self-contained, it runs the
  full suite and **commits** with a considered, complete commit message.
- Andon closes by montaging its five-why chain, evidence, fix, and test
  status back to the coordinator — nothing else is owed except the
  work itself.