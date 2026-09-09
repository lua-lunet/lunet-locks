#!/bin/sh
# Assemble a disposable, self-contained legacy-Docker build context. This
# vendors the dependency sources and the uvrr-core submodule (the manifest's
# [patch] section resolves vrr-core to it) before Docker sees them, avoiding
# BuildKit SSH secrets, host mounts, runtime source mounts, and git fetches.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
context=${1:?usage: docker_prepare_context.sh CONTEXT_DIR}

test -d "$context" || {
    echo "docker context directory does not exist: $context" >&2
    exit 2
}
test -d "$root/build" || {
    echo "missing Cyan output; run make build first" >&2
    exit 2
}

mkdir -p "$context/.cargo" "$context/ext/advisory_lock"
cargo vendor --manifest-path "$root/ext/advisory_lock/Cargo.toml" --locked --versioned-dirs "$context/vendor" >"$context/.cargo/config.toml.generated"
sed "s|directory = \".*\"|directory = \"/app/vendor\"|" "$context/.cargo/config.toml.generated" >"$context/.cargo/config.toml"

cp "$root/ext/advisory_lock/Cargo.toml" "$root/ext/advisory_lock/Cargo.lock" "$context/ext/advisory_lock/"
cp -R "$root/ext/advisory_lock/src" "$context/ext/advisory_lock/src"
# The learner-era-fold patch branch: the [patch] section of the manifest
# resolves vrr-core to the uvrr-core submodule, so the vendored context
# carries its source at the same relative position the manifest names.
test -d "$root/ext/uvrr-core/src" || {
    echo "missing the uvrr-core submodule source; run git submodule update --init" >&2
    exit 2
}
mkdir -p "$context/ext/uvrr-core"
cp "$root/ext/uvrr-core/Cargo.toml" "$context/ext/uvrr-core/Cargo.toml"
cp -R "$root/ext/uvrr-core/src" "$context/ext/uvrr-core/src"
# The diagnosis-kit stage builds the demo crate (the lease-sequencer node,
# the lease-client control client, the lease-load traffic generator) and the
# std-only rtt_probe. Its dependency closure comes from the demo crate's own
# Cargo.lock, vendored beside the advisory-lock closure (the two locks
# resolve different versions, so they cannot share one vendor directory).
mkdir -p "$context/examples/lease-sequencer" "$context/tools"
cp "$root/examples/lease-sequencer/Cargo.toml" "$root/examples/lease-sequencer/Cargo.lock" \
    "$context/examples/lease-sequencer/"
cp -R "$root/examples/lease-sequencer/src" "$context/examples/lease-sequencer/src"
cp -R "$root/examples/lease-sequencer/config" "$context/examples/lease-sequencer/config"
cp "$root/tools/rtt_probe.rs" "$context/tools/rtt_probe.rs"
cargo vendor --manifest-path "$root/examples/lease-sequencer/Cargo.toml" --locked --versioned-dirs \
    "$context/vendor-demo" >"$context/.cargo/config-demo.toml.generated"
sed "s|directory = \".*\"|directory = \"/app/vendor-demo\"|" \
    "$context/.cargo/config-demo.toml.generated" >"$context/.cargo/config-demo.toml"
cp -R "$root/build" "$context/build"
mkdir -p "$context/docker"
cp "$root/docker/Dockerfile" "$root/docker/entrypoint.sh" "$root/docker/cluster.jsonl" "$context/docker/"
