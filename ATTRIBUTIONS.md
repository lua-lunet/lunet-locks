# Attributions

This project is licensed under the MIT License — see [`LICENSE`](LICENSE).
The entries below record every third-party work the repository carries,
fetches, or links, with the version, licence, and the path by which it
enters the build. Licence facts were verified against the published licence
declarations of each source (upstream `LICENSE` files at pinned refs and
crates.io's published licence field for each pinned crate version). Where a
licence could not be verified this way, it is listed under "Not covered by
this audit" instead of asserted.

## Vendored code

| Path | Upstream | Version | Licence |
|---|---|---|---|
| `ext/uvrr-core/` (git submodule) | [lua-lunet/uvrr-core](https://github.com/lua-lunet/uvrr-core) | content of upstream tag `v0.3.0` (branch `lunet-locks/learner-era-fold`, submodule commit `058acdc`) | MIT (see `ext/uvrr-core/LICENSE` and `ext/uvrr-core/Cargo.toml`, `license = "MIT"`) |

The submodule carries the upstream commit plus two patch-branch commits,
marked here: learner acquisition, and fence-under-load (both shipped
upstream as part of tag `v0.3.0`). Upstream copyright (Copyright (c) 2026
Simon Massey) and licence text are preserved in the submodule.

The adapter manifest's `[patch]` section resolves the `vrr-core` git
dependency to this submodule, so every build lane — local, CI, and the
vendored Docker context — compiles the submodule source directly and
fetches no git dependencies.

| Path | Upstream | Version | Licence |
|---|---|---|---|
| `ext/lunet-locks-aof/zig/src/` | [tigerbeetle/tigerbeetle](https://github.com/tigerbeetle/tigerbeetle) | release tag `0.17.9` (the AOF strip; file map in `ext/lunet-locks-aof/VENDORED.md`) | Apache-2.0 (see `ext/lunet-locks-aof/LICENSE-TigerBeetle`) |

The vendored tree carries only the AOF code path and its minimal
dependency closure, compiled to a C-ABI cdylib behind the safe Rust
wrapper (`ext/lunet-locks-aof`, `links = "lunet_locks_aof"`). Upstream
licence facts verified against the pinned ref's `LICENSE` (Apache License
2.0). The system description, attribution block, and the licence
correction to the item spec's AGPL premise are in
`ext/lunet-locks-aof/AOF.md`.

| Path | Upstream | Version | Licence |
|---|---|---|---|
| `console/assets/styles.css` | [Nocturne](https://github.com/typora/theme.typora.io) (Typora Themes directory) | upstream source style | GNU Public License (per the upstream README: "The contents of this repository are licensed under the GNU Public License.") |

`console/assets/styles.css` is a derived work of the Nocturne theme, as
stated in that file's header. As a derived work it is distributed under the
GPL; the remainder of this repository remains MIT.

## Registry dependencies (Rust crates)

Versions are the pins in each committed `Cargo.lock`. Licence identifiers
were verified against the crates.io licence field for each pinned version.

### `ext/advisory_lock` — `lunet-advisory-lock` (the shipped native cdylib)

| Crate | Version | Licence | Enters the build as |
|---|---|---|---|
| `serde` | 1.0.229 | MIT OR Apache-2.0 | direct dependency, compiled into the cdylib |
| `serde_json` | 1.0.151 | MIT OR Apache-2.0 | direct dependency, compiled into the cdylib |
| `uuid` | 1.24.0 | Apache-2.0 OR MIT | direct dependency, compiled into the cdylib |
| `tracing` | 0.1.44 | MIT | direct dependency, compiled into the cdylib |
| `tracing-attributes` | 0.1.31 | MIT | proc-macro of `tracing` |
| `tracing-core` | 0.1.36 | MIT | dependency of `tracing` |
| `io-uring` | 0.7.15 | MIT OR Apache-2.0 | Linux-only direct dependency, compiled into the cdylib |
| `vrr-core` | upstream tag `v0.3.0` content | MIT | resolved by `[patch]` to the `ext/uvrr-core` submodule, linked into the cdylib |
| `proptest` | 1.11.0 | MIT OR Apache-2.0 | dev-dependency; tests only, not shipped |

### `ext/lock_feed` — `lock-feed` (smoke/tooling, not in the release archive)

| Crate | Version | Licence | Enters the build as |
|---|---|---|---|
| `tokio` | 1.53.1 | MIT | direct dependency |
| `tokio-tungstenite` | 0.26.2 | MIT | direct dependency |
| `futures-util` | 0.3.34 | MIT OR Apache-2.0 | direct dependency |
| `inotify` | 0.11.5 | ISC | Linux-only direct dependency |
| `serde_json` | 1.0.151 | MIT OR Apache-2.0 | direct dependency |
| `lunet-advisory-lock` | workspace path `../advisory_lock` | MIT | path dependency |

### `examples/lease-sequencer` — `lease-sequencer` (demo binaries shipped in the Docker image only)

| Crate | Version | Licence | Enters the build as |
|---|---|---|---|
| `serde_json` | 1.0.151 | MIT OR Apache-2.0 | direct dependency |
| `tracing` | 0.1.44 | MIT | direct dependency |
| `tracing-appender` | 0.2.5 | MIT | direct dependency |
| `tracing-subscriber` | 0.3.23 | MIT | direct dependency |
| `uuid` | 1.26.0 | Apache-2.0 OR MIT | direct dependency |
| `lunet-advisory-lock` | workspace path `../../ext/advisory_lock` | MIT | path dependency |

## Fetched at build/run time, not part of this repository

The official Lunet `v0.8.0` runtime is downloaded by `make lunet-runtime`
and by the Docker build, verified by SHA-256 (per-platform pins in the
`Makefile` and `docker/Dockerfile`), and extracted into `.lunet/v0.8.0/`.
It is a fetched tool: it is not stored in this repository, not rebuilt by
it, and not included in the release archive. Its upstream is
[lua-lunet/lunet](https://github.com/lua-lunet/lunet), whose `LICENSE` at
tag `v0.8.0` is the MIT License (Copyright (c) 2025 xialeistudio,
Copyright (c) 2025–2026 Simon Massey). The released archive tarball itself
carries no licence file; the licence record is the upstream repository's.

## Console assets served from CDNs

Not vendored into this repository; loaded by the browser at runtime:

- [Apache ECharts](https://echarts.apache.org/) 5.5.1 — Apache-2.0,
  served from cdn.jsdelivr.net, SRI-pinned in `console/assets/index.html`
- [Inter](https://rsms.me/inter/) and
  [JetBrains Mono](https://www.jetbrains.com/lp/mono/) — SIL Open Font
  License 1.1, served from fonts.googleapis.com / fonts.gstatic.com

The remainder of the `console/` tree (components, workers, mock, tooling)
is first-party code and carries no third-party licences.

## What the release archive carries

`tests/package_release.sh` builds the archive from: `build/` (compiled Lua
service tree), `lib/` (the native advisory-lock cdylib for the target
platform), `src/` (Teal sources), `docs/` (the rendered-site markdown
sources), `README.md`, and `LICENSE`. The archive carries no third-party
source files and no fetched Lunet runtime: all third-party crates are
compiled into the cdylib, and their MIT and MIT/Apache-2.0 licences carry
attribution obligations met by this file and the shipped `LICENSE`.

## Statement on bugs in vendored and derived code

Bugs in the vendored `ext/uvrr-core` submodule or the derived
`console/assets/styles.css` are routed to this project in the first
instance only. We reproduce the bug against the vendored tree and, when the
defect exists upstream, report it to the upstream project so both
communities receive the fix.

## Not covered by this audit

- Transitive crates.io dependencies beyond those named above (the
  `tracing` macro/core crates and the direct dependencies of `lock-feed`
  and `lease-sequencer`): their versions are pinned in the committed
  `Cargo.lock` files, but their individual licences were not recorded here.
- The vendored crate sources assembled by `tools/docker_prepare_context.sh`
  into the Docker build context: generated at build time from the pinned
  `Cargo.lock`, not stored in this repository.
- The degree of derivation of `console/assets/styles.css` from Nocturne:
  the file's header states it is a derived work, and that claim is carried
  as stated; the upstream licence (GPL) is verified, the derivation
  boundary itself is not.

Dependency bumps refresh this file: every new or re-pinned entry gains a
version and a verified licence at the bump.
