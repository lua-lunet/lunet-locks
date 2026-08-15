# AGENTS

Local guidance for this repo. Keep it short, factual, and unsurprising.

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
- The authoritative vrr-core source is the pinned tag v0.2.0
  (`6cbc1f36fb3a8bfa3aa7a66d12d697d3539b8ff0`). The sibling local checkout
  may be ahead of the tag; check it out at the tag (or read via
  `git show v0.2.0:...`) before using it as API authority. Do not revalidate
  or change it unless a concrete adapter API need requires it.
  A serious correctness, safety, or replication bug is a stop-and-report issue.
  A small focused additive ergonomic change may be staged locally (never
  committed or pushed) only with a corresponding upstream GitHub issue; report
  it to the coordinator.

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
