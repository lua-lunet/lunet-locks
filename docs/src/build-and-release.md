# Build and release architecture

## Two builds, two different jobs

1. **The sanity build** — "does it compile and typecheck". Run on colima
   as a *cross-compile*, never an emulation: `cargo check --target
   x86_64-unknown-linux-gnu` (and `--target aarch64-unknown-linux-gnu`)
   skips linking entirely and is dramatically faster. Cache persists
   identically. Growth gate: a clean commit **and** a passing sanity
   build on colima BEFORE any cloud deploy. No test run on a dirty
   commit. Period.
2. **The release build** — optimised binaries, built per release, both
   product shapes: the prod binary (no recorder) and the flight
   recorder binary (`--features flight-recorder`): swappable when
   investigation demands.

## Build-host rules (colima on an aarch64 Mac)

- **Never qemu / emulated build targets.** An emulated x86 container is
  horridly slow and burns the host CPU. Rust cross-compiles natively:
  rustc runs on aarch64 at full speed and emits x86 codegen. The only
  slow part is cross *linking*, and the cross gcc driver suite
  (`crossbuild-essential-amd64`) covers the whole of it natively — the
  cross gcc driver emits x86 code through its own x86 sysroot, the crt objects and
  target libgcc included (`[target.x86_64-unknown-linux-gnu] linker =
  "x86_64-linux-gnu-gcc"` in the image's cargo config).
- **The vendored AOF's zig compiles its explicit target.** The Rust
  build script of `ext/lunet-locks-aof` runs the pinned Zig 0.14.1
  toolchain (the aarch64 one, running natively) and, for a cross
  invocation, takes the target from `LUNET_LOCKS_AOF_TARGET` (the
  build's rust target triple, e.g. `x86_64-linux-gnu`); without it the
  library builds for the host. Zig's per-arch baselines omit the
  hardware-AES extensions the AOF checksum hard-requires, so every
  invocation carries an explicit CPU: x86_64 gets
  `-Dcpu=baseline+aes+avx`, aarch64 gets `-Dcpu=baseline+aes` — no
  emulation, and AOF codegen is reproducible per target instead of
  tracking whatever CPU the build host reported.
- **The commit stamps itself.** The docker context is the committed tree
  with `.git` excluded, so the fastbuild/release gate passes HEAD
  through `LUNET_LOCKS_HEAD`: the Flight Recorder's build scripts stamp
  `FLIGHT_COMMIT` from it (a gate-asserted clean commit is the
  authority; the potentially in-flight working tree is never read).
- **The internal crates resolve unpinned.** The `ext/*/Cargo.lock`
  files are deliberately untracked (.gitignore: internal FFI components
  are not independent releases) — they resolve lockless exactly as CI
  does; the example crate's lockfile IS tracked and pinned
  (`--locked`). The in-image resolutions land in the image's layer
  cache, not repo truth.
- **No host mounts.** colima runs `mountType: none`: nothing on the
  macOS disk is mounted into the VM (virtiofs/9p over Rust's tens of
  thousands of small files is an IO disaster), so all build caches live
  on the lima VM's own disk — persistent across `colima stop/start`,
  wiped only by `colima delete`. Keeping the work on the VM disk is the
  point: fast fix-and-rebuild between test runs.
- **No BuildKit.** The classic layer cache is the cache: the fastbuild
  image's deps stage copies the manifests and dependency sources only
  (`ext/uvrr-core` source, the AOF crate's zig/ tree, empty stubs for
  the own crates) and compiles the whole dependency closure for BOTH
  triples; that layer survives until a member manifest changes, and
  source changes recompile only our crates above it. The sanity and
  release payloads start `FROM` that layer. `.dockerignore` carries
  `target` (and the nested crate targets), `.git`, `.rocks`, `build`,
  `.lunet`, `.tmp` so local junk never busts it.
- **The sanity profile** (`CARGO_PROFILE_DEV_OPT_LEVEL=0`,
  `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_INCREMENTAL=0`): no optimisation,
  no debug info (the big link win), while `debug_assertions` stays ON —
  the `maybe!` tripwires still execute. The workload is IO-bound;
  compile speed beats code speed for these builds.
- Artifacts leave the build via `docker create` + `docker cp` — there
  are no volume mounts, no BuildKit, no platform emulation.

## Cloud test rigs (x86 linux)

- Never do a release build there. The rig builds ONLY the
  flight-recorder binary — dev profile (optimisations off, `maybe!`
  tripwires live) with `--features flight-recorder` — against a
  maximised local cargo cache on the rig's own disk.
- Cloud binaries MUST have `maybe!` on and the flight recorder
  enabled; see `testing-on-cloud.md`.
- The gateway to the cloud: clean commit + sanity build (above). See
  `softball-run.md` for the run cycle.

## The release flow

1. Apply the release tag to main — must be a clean commit.
2. Locally, before touching the tag push: `make sanity` (the gate:
   clean-commit assert + the cross-check of both triples in the colima
   fastbuild stages) and the flight-recorder build (the host toolchain
   runs `cargo build --features flight-recorder -p lease-sequencer`)
   must pass.
3. In parallel, all free:
   - Push the tag → the GitHub Actions release build runs
     (`tests/package_release.sh`; macOS, linux aarch64, linux x86, each
     in prod and flight-recorder shapes) and publishes the release page
     with the binaries and release notes.
   - A macOS release build locally.
   - A colima dual-arch release image build (`make build-proof`, or the
     full `make release-images TAG=vX.Y.Z`): aarch64 and x86 images
     tagged to the git tag, each carrying BOTH binaries (prod and
     flight-recorder — one can be swapped for the other; the workload
     is IO-bound, so one recorder node in a 5-node cluster is no
     penalty when it is not the leader), then push to ghcr.io
     (`tools/release_images.sh`; gh authenticates).
4. Definition of Done for a release: linux aarch64 + x86 images tagged
   to the git tag present locally in colima and pushed to ghcr.io;
   the tagged release on GitHub with the full binary matrix and notes.

## The colima dual-arch image build

`tools/release_images.sh TAG` runs `make sanity`, the host
flight-recorder build, `make build` (the Cyan tree the image carries),
and the two image assemblies. Each image is
`lunet-locks:<tag>-amd64` / `lunet-locks:<tag>-arm64` (docker reference
names carry no slash in the tag, so per-arch images get the arch
suffix; the aggregate multi-arch manifest carries the plain tag) and
both carry the Lunet v0.10.0 runtime, the Cyan service tree, both
adapter cdylib shapes, and the lease-sequencer node binary in both
shapes (the entrypoint defaults to the prod cdylib; one node swaps to
the flight-recorder cdylib via `LUNET_ADVISORY_LOCK_LIB` plus
`LUNET_FLIGHT_RECORDER_DIR`).

The mechanics (all inside `docker/Dockerfile.fastbuild`, no BuildKit,
no volume mounts, no platform emulation at any step):

1. `--target artifacts`: the release stage `FROM` the cached deps layer
   compiles the optimised payloads — the aarch64 payloads natively, and
   the x86 payloads the same way (rustc cross-compiles; the cross gcc
   links; zig emits the x86 AOF). `file(1)` on the produced machine
   code verifies both triples in-image.
2. The `artifacts` and `rootfs` stages lay `/out/` out for extraction,
   and the flow extracts them with `docker create` + `docker cp`.
3. The **aarch64 image** builds natively: the classic builder
   `FROM debian:bookworm-slim` for the daemon's own architecture, the
   runtime `apt-get` set inside, the extracted payloads COPYed on top
   (`docker/Dockerfile.release`).
4. The **amd64 image** is assembled COPY-only on the amd64 base and
   no amd64 code ever runs to build it:
   - `docker pull --platform linux/amd64 debian:bookworm-slim` — the
     base image's layers are a DATA pull, never executed;
   - the fastbuild's `rootfs` stage (running on the aarch64 host)
     `dpkg --add-architecture amd64` + `apt-get --download-only` the
     amd64 runtime packages (another data pull) and unpacks them with
     `dpkg -x` (arch-agnostic unpacking) into `/out/amd64-rootfs`;
   - `docker create --platform linux/amd64` a container from the base
     (`docker create` never runs code), `docker cp` the unpacked
     runtime tree and the extracted x86 release payloads into it, then
     `docker commit` stamps the image with the entrypoint and default
     environment. The commit inherits the amd64 base image config, so
     the image IS `linux/amd64`, asserted after the fact with
     `docker image inspect`.
   - Push to ghcr.io: `docker push` for each per-arch image, then
     `docker manifest create` + `docker manifest push` assembles the
     aggregate `ghcr.io/<owner>/lunet-locks:<tag>` (server-side
     registry assembly — nothing is compiled or emulated to do it).

## Where the instructions live

- The testing rules (clean commit, sanity build, recorder-on binaries)
  live in `testing-on-cloud.md`, linked from the main `README.md` as
  "Testing On Cloud" — the cloud is not named.
- The mandatory pre-release cluster cycle lives in `softball-run.md`.

## Tag names

- Sanity-check builds (the gate: does it compile) are tagged
  `lunet-locks:latest`.
- Release builds (binaries) are tagged `lunet-locks:<git-tag>`.
- The optimised builder image (layered OS base / toolchain-deps / code
  layers, shared cross-project build cache on the VM disk with the
  deps-drift `repo_branch.json` export, vendored Zig built in a GitHub
  Action and layered in as a cdylib) is maintained OUTSIDE this
  repository's scope: docker builds are not deployed to the cloud rigs,
  and the builder-image work is owned elsewhere.
