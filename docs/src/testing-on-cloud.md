# Testing On Cloud

These are userland rules for testing this project on cloud
infrastructure. The cloud is not named.

## The gateway rules — before every cloud deploy

1. **The commit must be clean.** `git status --porcelain` is empty; the
   work is committed at HEAD. No test run on a dirty commit. Period.
2. **The sanity build must pass on the colima build host** —
   `cargo check --target aarch64-unknown-linux-gnu --target
   x86_64-unknown-linux-gnu` cross-compiled natively (rustc on aarch64
   emitting x86 codegen — never an emulated build target). If the
   sanity build passes, that is the gate to cloud deploy. See
   [build-and-release.md](build-and-release.md) for the mechanics.
3. **The cloud binaries must have `maybe!` on and the flight recorder
   enabled.** The rig builds only the flight-recorder binary
   (dev profile: optimisations off, recorder on); it does NOT do a
   release build. Cache is maximised on the rig's own disk; the target
   platform for cloud tests is x86 linux. The rig command shape (the
   dev-profile recorder builds; recorded nodes opt in per node via
   `LUNET_FLIGHT_RECORDER_DIR`):

   ```console
   cargo build --features flight-recorder       # dev profile, x86 linux target
   cargo build --features flight-recorder -p lease-sequencer
   ```

   x86 linux rigs run it natively; any other build host passes the
   triple (`--target x86_64-unknown-linux-gnu`) and links through the
   cross gcc kit per [build-and-release.md](build-and-release.md) —
   never an emulated run. The recorder build's clean-commit guard is the
   adapter's build script: a checkout supplies git directly, the Docker
   flows stamp `LUNET_LOCKS_HEAD` (the gate-asserted commit).

## The build-confirmation gate

The pre-flight step is `make sanity` on the build host (colima):
clean-commit assert → cross-check both triples. This is a build
confirmation, not deployment and not testing — the point is never to
test on an "only builds on my laptop" commit.

## The mandatory pre-release cluster cycle

The [softball run](softball-run.md) — then the polite run — is the
mandatory pair before any release: on a clean commit, power the VMs up,
test softball (fresh files), move the files off, test polite (fresh
files), copy everything off, then stop the VMs. Repeat until the pair
is bug-free. An aggressive run joins the sequence soon.

Counter-confirmation: releases additionally follow
[build-and-release.md](build-and-release.md) — release binaries are
built only at release time, locally and in CI, in both prod and
flight-recorder shapes.
