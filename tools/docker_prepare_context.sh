#!/bin/sh
# Assemble a disposable, self-contained legacy-Docker build context. This
# vendors the dependency sources — cargo fetches the `uvrr-core` git tag the
# manifest names into the vendor directory — before Docker sees them, avoiding
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

# The build script travels with the crate: it stamps the build's identity
# facts (`FLIGHT_*`, `LUNET_INFO_*`) that `src/info.rs` reads back through
# `env!`. A context without it leaves those macros undefined and the cdylib
# stage fails to compile.
cp "$root/ext/advisory_lock/Cargo.toml" "$root/ext/advisory_lock/Cargo.lock" \
    "$root/ext/advisory_lock/build.rs" "$context/ext/advisory_lock/"
cp -R "$root/ext/advisory_lock/src" "$context/ext/advisory_lock/src"
# The vendored AOF subcrate: the adapter's lifecycle marker rides its
# quorum-of-copies superblock, which is Zig source. The Dockerfile's aof
# stage compiles that zig/ tree inside the image with the pinned 0.14.1
# toolchain (downloaded with its official SHA-256 verified), so the context
# carries the subcrate's Rust wrapper and its zig/ source tree; the zig
# caches stay behind — they are build state, not source.
mkdir -p "$context/ext/lunet-locks-aof/zig"
cp "$root/ext/lunet-locks-aof/Cargo.toml" "$root/ext/lunet-locks-aof/build.rs" \
    "$context/ext/lunet-locks-aof/"
cp "$root/ext/lunet-locks-aof/zig/build.zig" "$context/ext/lunet-locks-aof/zig/"
cp -R "$root/ext/lunet-locks-aof/src" "$context/ext/lunet-locks-aof/src"
cp -R "$root/ext/lunet-locks-aof/zig/src" "$context/ext/lunet-locks-aof/zig/src"
# The diagnosis-kit stage builds the demo crate (the lease-sequencer node,
# the lease-client control client, the lease-load traffic generator) and the
# std-only rtt_probe. Its dependency closure comes from the demo crate's own
# Cargo.lock, vendored beside the advisory-lock closure (the two locks
# resolve different versions, so they cannot share one vendor directory).
mkdir -p "$context/examples/lease-sequencer" "$context/tools"
cp "$root/examples/lease-sequencer/Cargo.toml" "$root/examples/lease-sequencer/Cargo.lock" \
    "$root/examples/lease-sequencer/build.rs" "$context/examples/lease-sequencer/"
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
