# AGENTS

Local guidance for this repo. Keep it short, factual, and unsurprising.

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

## New user work lands as two todos at the end

- A new user message that introduces work is, by default, non-blocking: append
  two items to the END of the todo list — (1) "plan <work>" and (2) "<work>"
  — then continue the item in progress. `opencode-chat-history` may be used
  during the planning item to recover the user's stated details.
- Exceptions (obey the user's words over this default): they say do it next,
  immediately, without a todo list, or name a specific order — then do what
  they said.

## Fast line: pure Rust, then wrappers

- Debugging is ad-hoc Rust first: small throwaway CLI bins named
  `skaffold_xxxx` that solve exactly one thing. Wrappers (Lua/Python/FFI)
  are pure overhead while debugging; defer them to reuse, and when they
  are written, base them on a battle-tested Rust console (AOF + logs +
  lock client/server interaction) — a small skin, not new machinery.
- Every fix carries a defensive test so the regression cannot return.

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

## Runtime and upstream boundaries

- The only service/smoke runtime is the project-local official Lunet `v0.8.0`
  release. Run `make lunet-runtime` or `make smoke`; do not use a `lunet-run`
  from `PATH`. Its authoritative shipped LuaCATS/Teal docs are under
  `.lunet/v0.8.0/types/`.
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
