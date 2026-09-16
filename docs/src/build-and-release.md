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

- **Never qemu / emulated build targets.** qemulated x86 containers are
  horridly slow and burn the host CPU. Rust cross-compiles natively:
  rustc runs on aarch64 at full speed and emits x86 codegen. The only
  slow part is cross *linking*: `clang` + `lld` +
  `libc6-dev-amd64-cross` with
  `--target=x86_64-linux-gnu --sysroot=/usr/x86_64-linux-gnu
  --fuse-ld=lld` covers it.
- **No host mounts.** colima runs `mountType: none`: nothing on the
  macOS disk is mounted into the VM (virtiofs/9p over Rust's tens of
  thousands of small files is an IO disaster), so all build caches live
  on the lima VM's own disk — persistent across `colima stop/start`,
  wiped only by `colima delete`. Keeping the work on the VM disk is the
  point: fast fix-and-rebuild between test runs.
- **No BuildKit.** The classic layer cache is the cache: a deps stage
  copies manifests only and compiles all dependencies for both triples;
  that layer survives until `Cargo.lock` changes; source changes
  recompile only our crates on top. `.dockerignore` carries `target` and
  `.git` so local junk never busts it.
- **Sanity profile** (`CARGO_PROFILE_DEV_OPT_LEVEL=0`,
  `CARGO_PROFILE_DEV_DEBUG=0`, `CARGO_INCREMENTAL=0`): no optimisation,
  no debug info (the big link win), while `debug_assertions` stays ON —
  the `maybe!` tripwires still execute. The workload is IO-bound;
  compile speed beats code speed for these builds.
- Artifacts leave the build via `docker create` + `docker cp` — there
  are no volume mounts and no buildkit.

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
2. Locally, before touching the tag push: the sanity build and the
   flight-recorder build must pass.
3. In parallel, all free:
   - Push the tag → the GitHub Actions release build runs
     (`tests/package_release.sh`; macOS, linux aarch64, linux x86, each
     in prod and flight-recorder shapes) and publishes the release page
     with the binaries and release notes.
   - A macOS release build locally.
   - A colima dual-arch release image build: aarch64 and x86 images
     tagged to match the git tag, each carrying BOTH binaries (prod and
     flight-recorder — one can be swapped for the other; the workload
     is IO-bound, so one recorder node in a 5-node cluster is no
     penalty when it is not the leader), then push to ghcr.io.
4. Definition of Done for a release: linux aarch64 + x86 images tagged
   to the git tag present locally in colima and pushed to ghcr.io;
   the tagged release on GitHub with the full binary matrix and notes.

## Where the instructions live

- The testing rules (clean commit, sanity build, recorder-on binaries)
  live in `testing-on-cloud.md`, linked from the main `README.md` as
  "Testing On Cloud" — the cloud is not named.
- The mandatory pre-release cluster cycle lives in `softball-run.md`.
