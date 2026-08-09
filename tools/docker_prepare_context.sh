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
cp -R "$root/build" "$context/build"
mkdir -p "$context/docker"
cp "$root/docker/Dockerfile" "$root/docker/entrypoint.sh" "$context/docker/"
