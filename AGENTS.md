# AGENTS

Local guidance for this repo. Keep it short, factual, and unsurprising.

## Scope

- `src/` is the shipped Teal source tree.
- `build/` is Cyan output. Never hand-edit it.
- `tests/` holds the forward-pass tests and the learning tests.
- `scripts/` holds executable harnesses; no shell scripts.

## Toolchain

- Build with [Cyan](https://github.com/teal-language/cyan), not ad-hoc `tl gen` loops.
- Install Lua deps into the project-local `.rocks/` tree.
- Runtime target is LuaJIT / Lua 5.1, so `gen_target = "5.1"` and `gen_compat = "off"`.

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

## Testing split

- Pure modules stay free of `require("lunet")` and are testable in `tests/`.
- Runtime-coupled code is exercised through harnesses, not unit tests.

## Declarations / vendoring / FFI

- Third-party runtime modules get `.d.tl` declaration files.
- Vendored plain Lua belongs in `vendor/`, verbatim.
- Native/Rust FFI belongs under `ext/<name>/` and is wired through `make`, not manual steps.
- `lnt_shared` (shared-counter extension) is shipped in the v0.7.0 binary release; `require("lunet.lnt_shared")` works under `lunet-run`. Upstream docs tracked at [lua-lunet/lunet#134](https://github.com/lua-lunet/lunet/pull/134).
