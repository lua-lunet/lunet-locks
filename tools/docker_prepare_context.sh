#!/bin/sh
# Assemble a disposable, self-contained legacy-Docker build context. This
# vendors the exact private vrr-core revision before Docker sees it, avoiding
# BuildKit SSH secrets, host mounts, and runtime source mounts.
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
cp -R "$root/build" "$context/build"
mkdir -p "$context/docker"
cp "$root/docker/Dockerfile" "$root/docker/entrypoint.sh" "$root/docker/cluster.jsonl" "$context/docker/"
