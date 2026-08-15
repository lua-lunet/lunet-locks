# Conventions

Teal-first project on the [lunet](https://github.com/lua-lunet/lunet) runtime (embedded LuaJIT).
These are the rules; upstream tools keep their own docs and we link rather than restate them.

## Layout

| Path | Contents | Committed |
|---|---|---|
| `src/` | Teal sources that ship. `.d.tl` declarations for runtime C modules. | yes |
| `build/` | Cyan output. Machine-generated, never edited. | no |
| `tests/` | Forward-pass tests and learning tests | yes |
| `docs/` | Zensical source/configuration; `docs/site/` is generated | source only |
| `ext/` | Native / Rust FFI extensions | yes |
| `.rocks/` | Project-local LuaRocks tree | no |
| `.tmp/` | Scratch: experiments, logs, generated configs, pid files | `.tmp/keep` only |

Nothing writes outside `.tmp/`, `build/`, `.rocks/`, `docs/site/`, and native-extension
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
`make check` type checks everything outside `source_dir` that `cyan build` does not reach.

Unexpected Teal facts should be captured as **learning tests**, not copied into prose. See
[`tests/teal_learning_test.tl`](tests/teal_learning_test.tl) and its fixtures for the current
library of "no surprises" checks (explicit return annotations, map syntax, nominal record types,
and `tested` invalid-test behavior).

## Tests

`tested` is Teal-native and supports `tests/*_test.tl`. Its sharp edge is that some failures are
reported as **invalid**, not failed: unhandled exceptions, tests with no assertions, and tests
whose declared `expected` result is not actually produced. That behavior is pinned by a learning
test in [`tests/teal_learning_test.tl`](tests/teal_learning_test.tl).

`make test` is the stable validation entry point: it runs `make check`, the Teal tests, and the
native extension's `cargo test` suite. Cargo discovers the extension's integration tests, so do
not add per-suite Make targets that merely repeat `cargo test`.

The shipped Teal, FFI ABI, core Rust crate, and default test path use stable Rust and long-lived
dependencies. A stable test-only Rust dependency belongs in `[dev-dependencies]` and must not add
a dependency edge into shipped artifacts.

Model checking and fuzzing harnesses are excluded from the default test path. Any such harness
belongs entirely beneath `not_stable_rust/`, with its own explicit toolchain and documented invocation;
that isolated package may depend on the core crate, but production code and default targets must
not depend on it. It is not run by `make test`. Document supported platforms, resource limits,
corpus ownership, and counterexample reproduction next to any such harness before adding it.

## Dependencies

- Lua deps install into the project-local tree: `make deps` → `luarocks --tree=.rocks`. No global
  installs, no `sudo`. `.rocks/` is gitignored.

## Native / FFI extensions

One directory per extension under `ext/<name>/`, mirroring
[lunet's own `ext/` layout](https://github.com/lua-lunet/lunet) (Rust crate + a thin Lua/Teal
wrapper). Loaded through the LuaJIT FFI, typed with a `.d.tl`, and built by `make build`.

These Rust crates are currently internal FFI components, not independent library releases. Their
`Cargo.lock` files are ignored.
