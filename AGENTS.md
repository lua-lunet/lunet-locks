# AGENTS

Local guidance for this repo. Keep it short, factual, and unsurprising.

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

## Subagent delegation

- Agents SHOULD prefer the subagent delegation skill (listed in
  `opencode`'s available skills) wherever doing so does not overwrite any
  other instruction here or the user's stated preferences.
- Within that skill: work a numbered todo list
  (`item00`, `item01`, …), write each item's spec to the gitignored
  `.tmp/delegation/` scratch as `itemNN.md`, launch one agent per spec,
  gate commits on verified-green work, and use a `wip:` prefix until the
  feature set is complete. Where doing so does not overwrite any other
  instruction here or the user's stated preferences.
- Item numbers are session-local: they live in the todo and the `.tmp/`
  spec filename only. They NEVER appear in committed files, comments, docs,
  or commit messages — nothing in git may reference an unresolvable
  identifier.

## New user work lands as two todos at the end

- A new user message that introduces work is, by default, non-blocking: append
  two items to the END of the todo list — (1) "plan <work>" and (2) "<work>"
  — then continue the item in progress. `opencode-chat-history` may be used
  during the planning item to recover the user's stated details.
- Exceptions (obey the user's words over this default): they say do it next,
  immediately, without a todo list, or name a specific order — then do what
  they said.

## Architecture note: phi vs timeouts

- Phi failure detection, leader-election (leader) timeouts, and the
  cluster viewchange timeout are DISTINCT concepts with DISTINCT
  mechanisms. Phi is a steady-state leader-failure detector; while a
  node is timed out (`timedout` toggle) phi is neither updated nor
  checked, and the randomized cluster viewchange timeout polls instead.
  See `docs/src/phi-and-timeouts.md`. Every unit test, feature, and
  config must reflect this distinction — a test, feature, or config key
  that conflates them is wrong.

## Fast line: pure Rust, then wrappers

- Debugging is ad-hoc Rust first: small throwaway CLI bins named
  `skaffold_xxxx` that solve exactly one thing. Wrappers (Lua/Python/FFI)
  are pure overhead while debugging; defer them to reuse, and when they
  are written, base them on a battle-tested Rust console (AOF + logs +
  lock client/server interaction) — a small skin, not new machinery.
- Every fix carries a defensive test so the regression cannot return.

## Dangling runs: no slop, no leftovers

- Work ends in git, in the todo, moved out of the way, or terminated.
  Nothing may dangle.
- Before any outer commit, run `tools/dangling.lua`: processes started
  by cancelled or completed agent attempts (smoke servers, simulators,
  orphans holding ports) get listed, then terminated deliberately —
  clear orphans only; NEVER the operator's own deliberate long-running
  jobs (when in doubt, list and report instead of killing).
- Files produced by cancelled attempts go to `.tmp/attic/`. At a
  milestone tidy-up, on the operator's explicit order, attic content
  moves to `/tmp/` (that order is the one sanctioned exception to the
  write boundary below).

## Legacy-free (alpha, unreleased)

- THIS REPO IS ALPHA AND UNRELEASED. There is NO backwards
  compatibility, NO migration, NO legacy support. Old logic, old paths,
  old on-disk formats, and old tooling get deleted (deleting-dead-code
  discipline) — never kept alive "for compatibility". The very latest
  writes are the only ones documented in the MDD docs and doc comments.
- If an on-disk format changes, old files are deleted before the next
  cloud run. Legacy free. Period.
- All markdown is MDD to the future target release state: no caveats,
  no "not yet implemented", no contemporaneous commentary.
- No file in git may name any `itemNN` identifier — such references are
  purged on sight.

## Read boundary

- Agents are FORBIDDEN from reading outside this repo. A dependency's code is
  a git dependency: read it at its GitHub repository (e.g.
  `https://github.com/lua-lunet/uvrr-core` / raw.githubusercontent.com at the
  pinned ref), never a sibling checkout on disk and never cargo's home cache.

## Write boundary (hard rule)

- Agents are FORBIDDEN from writing outside this repo. No `/tmp/*`, no home
  dotfiles, no macOS temp dirs.
- Paths are ABSOLUTE and pinned inside the repo root
  (`/Users/Shared/lua-lunet/lunet-locks/.tmp/…`) — never bare relative paths
  (`./..` cwd drift is how agents end up outside). Any absolute path outside
  the workspace (`/tmp/…:15`, `$HOME/…`) is evidence of a violation. This is
  not a style choice: out-of-repo writes trigger security approval dialogs
  that stop the operator's world.
- Any command or prompt an agent issues must carry no absolute path outside
  the workspace. If a tool defaults to an external temp, redirect it into
  the repo root pinned `.tmp/` or run without it.

## Scope

- `src/` is the shipped Teal source tree.
- `build/` is Cyan output. Never hand-edit it.
- `tests/` holds the forward-pass tests and the learning tests.

## Toolchain

- Build with [Cyan](https://github.com/teal-language/cyan), not ad-hoc `tl gen` loops.
- Install Lua deps into the project-local `.rocks/` tree.
- Format Teal with [Cerulean](https://github.com/efredriksson/cerulean) using its default
  opinionated conventions (4-space indent, double quotes, sorted requires, 88-char
  line length). Run `make fmt` to format in place and `make lint` (included in
  `check`) to reject unformatted code. After clone, run `make hooks` once to enable
  the pre-commit guard.
- Runtime target is LuaJIT / Lua 5.1, so `gen_target = "5.1"` and `gen_compat = "off"`.
- Run orchestration and generic tooling are Teal too. Entry points are executable
  `.lua` files with `#!/usr/bin/env -S luajit` shebangs that `dofile`
  `tools/bootstrap.lua` (rocks-path resolution + `tl.loader()`); every module they
  call is a typed `.tl` library under `tools/lib/` behind a `record` contract.
  Paths resolve from `arg[0]`, never the cwd. There is no shell orchestration:
  shell has no types and fails silently (the dated-log gate class). Python stays
  for data mining only.
- `make init` also installs `tl` into `.rocks/` for the LuaJIT 5.1 ABI,
  idempotently; `make check` runs `tl check` over `tools/lib`.

## Runtime and upstream boundaries

- The only service/smoke runtime is the project-local official Lunet `v0.10.0`
  release. Run `make lunet-runtime` or `make smoke`; do not use a `lunet-run`
  from `PATH`. Its authoritative shipped LuaCATS/Teal docs are under
  `.lunet/v0.10.0/types/`.
- The authoritative uvrr-core source is the vendored submodule
  `ext/uvrr-core`, branch `lunet-locks/learner-era-fold`: upstream commit
  `0fc6380` plus the learner-acquisition and fence-under-load patch
  commits. The adapter manifest's `[patch]` section builds the dependency
  from the submodule, so this tree always builds against that branch. Do
  not revalidate or change the submodule unless a concrete adapter API need
  requires it. A serious correctness, safety, or replication bug is a
  stop-and-report issue; report upstream engagement to the coordinator.

## Teal no-surprises recap

Do not trust memory on Teal syntax. If unsure, check the learning tests first:

- [`tests/teal_learning_test.tl`](tests/teal_learning_test.tl)
- `tests/fixtures/no_return_annotation_fails.tl`
- `tests/fixtures/map_syntax_passes.tl`
- `tests/fixtures/nominal_a.tl`
- `tests/fixtures/nominal_b_fails.tl`

The currently pinned surprises are:

- Functions need explicit return annotations if they return values.
- Maps use `{K:V}` syntax.
- Re-declaring the same-looking record in two modules creates distinct nominal types.
- `tested` marks no-assert tests and unhandled-exception tests as `invalid`.

## Docs

- Markdown Driven Development: every doc on `main` is written as-at the
  release cut from `main`. Docs state what the system is and does as fact —
  no "status" banners, no "not yet implemented", no WIP/design-note framing,
  no contemporaneous commentary. Where code has not landed yet, the doc is
  the spec that drives the implementation, written in the same factual
  voice. History belongs in git log and release notes, never in the docs.

## Testing split

- Pure modules stay free of `require("lunet")` and are testable in `tests/`.

## Releases

- Pushing a `v*` tag runs the full CI matrix, packages per-platform archives
  (`tests/package_release.sh`; `build/` + the native cdylib + `src/` + docs +
  `LICENSE`), runs the Docker simulation, and then the `publish-release` job
  creates the GitHub release with
  `lunet-locks-{linux-amd64,linux-arm64,macos}.tar.gz`.
- No Windows asset: the Lua native loader has no `.dll` suffix handling.
- Verify an archive with `make package-verify` (extracts it and runs the full
  three-replica smoke against the packaged tree using the pinned runtime).

## Bug provenance

Correctness and liveness defects are host-side until proven otherwise.
A correctness bug may go upstream only when every resurrection
requirement was met (the loop shut down, the state flushed) and a
failure to comply with the safety obligations has been disproven. A
liveness bug is an absence of messages: a node at rest must time out
and send, so a wedge is direct evidence of a host-requirement failure
leaving a livelock, and uVRR is suspect only when evidence shows
reasonable timeouts producing unnecessary messages. Agents assume every
bug is their own and must find the defect in the uVRR spec or protocol
before raising anything upstream.
