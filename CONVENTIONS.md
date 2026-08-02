# Conventions

Teal-first project on the [lunet](https://github.com/lua-lunet/lunet) runtime (embedded LuaJIT).
These are the rules; upstream tools keep their own docs and we link rather than restate them.

## Root discipline

Nothing lives in the repo root unless it is required to build or run the solution.

Permitted at root: `Makefile`, `tlconfig.lua`, `CONVENTIONS.md`, `.gitignore`, `.env`.

Anything optional goes in a subdirectory — e.g. Docker is **not** mandated, so no `Dockerfile`
at root; container assets live in `docker/`. Same for CI, editor and packaging files.

## Layout

| Path | Contents | Committed |
|---|---|---|
| `src/` | Teal sources that ship. `.d.tl` declarations for runtime C modules. | yes |
| `build/` | Cyan output. Machine-generated, never edited. | no |
| `scripts/` | Executable dev/test harnesses (see [Scripts](#scripts)) | yes |
| `scripts/lib/` | Teal modules used by harnesses | yes |
| `tests/` | Forward-pass tests and learning tests | yes |
| `docs/` | Zensical source/configuration; `docs/site/` is generated | source only |
| `vendor/` | Third-party plain Lua, vendored verbatim | yes |
| `ext/` | Native / Rust FFI extensions | yes |
| `docker/` | Optional container assets | yes |
| `bin/` | Fetched runtime binaries | no |
| `.rocks/` | Project-local LuaRocks tree | no |
| `.tmp/` | Scratch: experiments, logs, generated configs, pid files | `.tmp/keep` only |

Nothing writes outside `.tmp/`, `build/`, `bin/`, `.rocks/`, `docs/site/`, and native-extension
`target/` directories.

For sandboxing and workspace locality, use of the operating system `/tmp` is discouraged. All
project temporary work belongs under the repository's `.tmp/`; the tracked `.tmp/keep` preserves
that workspace while every other scratch artifact remains ignored.

## Build

[Cyan](https://github.com/teal-language/cyan) is the build tool; it type checks, resolves
dependencies, transpiles and does incremental builds. Layout and config keys are Cyan's —
see [`docs/tlconfig.md`](https://github.com/teal-language/cyan/blob/main/docs/tlconfig.md).
Ours is [`tlconfig.lua`](tlconfig.lua) (`src/` → `build/`).

- `make build` → `cyan build`. `make clean` removes `build/`. `build/` is in `.gitignore`.
- `gen_target = "5.1"`, `gen_compat = "off"` — lunet embeds LuaJIT and ships no `compat53`.
- Generated Lua is an artifact: not committed, not hand-edited, not reviewed as source.

## Collected ordering

Struct fields and method parameters SHOULD follow
[NOMA Collected Ordering](https://gist.github.com/simbo1905/c969d505ca531a301fea7f24f52ee0c9):
collect technical and business concerns separately, put the technical prefix first, follow logical
encounter order within each domain, and put metadata before the data it describes. This is a rule
of thumb; one genuine linchpin parameter may be made conspicuous when consistency would hide it.

## Type checking is the lint

The type checker is the primary static gate ([warnings](https://teal-language.org/book/latest/compiler_options.html)).
`make check` type checks everything outside `source_dir` (harnesses, specs) that `cyan build`
does not reach.

Unexpected Teal facts should be captured as **learning tests**, not copied into prose. See
[`tests/teal_learning_test.tl`](tests/teal_learning_test.tl) and its fixtures for the current
library of "no surprises" checks (explicit return annotations, map syntax, nominal record types,
and `tested` invalid-test behavior).

Running [luacheck](https://github.com/lunarmodules/luacheck) over `build/` is *future work*: most
of its findings are already impossible by construction, so it needs a ruleset that filters
"Teal-isms" in generated output before it is worth wiring up. Handwritten Lua (`vendor/`,
bootstraps) is the part that would benefit.

## Scripts

**No `sh`/`bash`/`zsh` scripts.** Ad-hoc automation is Teal.

An executable in `scripts/` is `chmod +x` with `#!/usr/bin/env luajit` and contains only a thin
bootstrap: resolve the repo root, extend `package.path`, call
[`tl.loader()`](https://github.com/teal-language/tl#loading-teal-code-from-lua), hand off to a
typed module in `scripts/lib/`. `tl.loader()` resolves `.tl` through the existing `?.lua`
patterns, so no extra path entry is needed.

Worked example: [`scripts/nginx-counter-demo`](scripts/nginx-counter-demo) →
[`scripts/lib/nginx_counter.tl`](scripts/lib/nginx_counter.tl) — boots openresty on an ephemeral
port, drives a shared-dict counter over `nc`, shuts down by pid. PID handling
([`scripts/lib/proc.tl`](scripts/lib/proc.tl)) always escalates `TERM` → `KILL`; a `SIGSTOP`ped
process never reaps a `TERM`.

The loader is for **build and test only**. Shipped code runs precompiled from `build/`, so the
runtime carries no compiler and startup pays no compile cost.

Harnesses that need lunet's event loop cannot use the luajit shebang — they are launched by
`bin/lunet-run` from a `make` target.

## Tests

Split by coupling:

- **Pure modules must not `require("lunet")`.** They are unit-testable outside the runtime, in
  `tests/`, and the forward pass uses [`tested`](https://fouriertransformer.github.io/tested/).
- **Runtime-coupled modules** (anything touching sockets/timers) are covered by integration
  harnesses run under `bin/lunet-run`, not by unit tests.

`tested` is Teal-native and supports `tests/*_test.tl`. Its sharp edge is that some failures are
reported as **invalid**, not failed: unhandled exceptions, tests with no assertions, and tests
whose declared `expected` result is not actually produced. That behavior is pinned by a learning
test in [`tests/teal_learning_test.tl`](tests/teal_learning_test.tl).

[busted](https://lunarmodules.github.io/busted) remains the more established Lua test framework
and is what `tl` and `cyan` themselves use. If we adopt it later, keep it alongside `tests/` only
with a concrete reason; do not switch frameworks casually.

## Dependencies

- `make init` is the developer bootstrap entrypoint; it runs sub-targets and wires the bare checkout.
- Lua deps install into the project-local tree: `make deps` → `luarocks --tree=.rocks`. No global
  installs, no `sudo`. `.rocks/` is gitignored.
- The toolchain runs on LuaJIT (`--lua-version=5.1`) so there is one VM for build, scripts and runtime.
- Runtime binaries are fetched, not vendored: `make fetch` → `bin/`.

## Vendoring

Third-party plain Lua goes in `vendor/`, copied verbatim (upstream licence header intact), never
reformatted, never reflowed to our style. Add `vendor` to `include_dir` in `tlconfig.lua` and
describe its surface with a `.d.tl` if callers need types.

## Native / FFI extensions

One directory per extension under `ext/<name>/`, mirroring
[lunet's own `ext/` layout](https://github.com/lua-lunet/lunet) (Rust crate + a thin Lua/Teal
wrapper). Loaded through the LuaJIT FFI, typed with a `.d.tl`, and built by `make build` so a
clean checkout needs no manual step.

These Rust crates are currently internal FFI components, not independent library releases. Their
`Cargo.lock` files are ignored. Revisit that policy when the project packages its own binary or
deliberately releases a native library.

`lnt_shared` (shared-counter extension) is shipped in the v0.7.0 macOS binary release:
`bin/lunet/liblnt_shared.dylib` + `bin/lunet/lnt_shared.lua`. `require("lunet.lnt_shared")`
works without configuration under `lunet-run`. Upstream PR
[lua-lunet/lunet#134](https://github.com/lua-lunet/lunet/pull/134) documents the archive
layout and standalone FFI path. The scaffold (`src/delete_me_skaffold.tl`) uses `lnt_shared`
for its counter, proving the full chain.

## Make is the entrypoint

Every workflow is a `make` target: `init`, `deps`, `fetch`, `build`, `check`, `test`, `harness`, `docs`, `load-test`, `clean`.
Cyan owns Teal compilation and type checking; `make` owns orchestration (LuaRocks deps, fetched
runtime binaries, optional harness scripts, future FFI/container hooks). Agents and CI call the
targets, not the tools directly.
