#!/bin/sh
# Package a release archive for one target platform. CI calls this on tagged
# builds after `make test`/`make smoke` have already passed, so the compiled
# build/ tree and the Rust cdylib already exist by the time this runs.
#
# Usage: tests/package_release.sh <target_key> <output.tar.gz>
#   target_key: linux-amd64 | linux-arm64 | macos
#
# The archive layout is what a downstream deployment consumes:
#   build/    compiled Lua service tree (cyan output)
#   lib/      the native advisory-lock cdylib for this platform
#   src/      Teal sources, for Teal-toolchain consumers
#   docs/     rendered-site markdown sources
#   README.md
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
cd "$root"

target_key=${1:-""}
output=${2:-""}

case "$target_key" in
    linux-amd64 | linux-arm64) libname=lunet_advisory_lock.so ;;
    macos) libname=lunet_advisory_lock.dylib ;;
    "")
        echo "package release: usage: tests/package_release.sh <target_key> <output.tar.gz>" >&2
        exit 64
        ;;
    *)
        # Windows is deliberately unsupported: the Lua loader in
        # src/advisory_lock.tl has no .dll suffix handling, matching the
        # reduced-scope Windows CI job.
        echo "package release: unsupported target key: $target_key" >&2
        exit 64
        ;;
esac
test -n "$output" || {
    echo "package release: missing output archive path" >&2
    exit 64
}

lib="ext/advisory_lock/target/release/lib$libname"
test -f "$lib" || {
    echo "package release: missing $lib (run make build first)" >&2
    exit 66
}
test -f build/server.lua || {
    echo "package release: missing build/server.lua (run make build first)" >&2
    exit 66
}

stage=$(mktemp -d "$root/.tmp/package-release.XXXXXX")
trap 'rm -rf "$stage"' EXIT INT TERM HUP

mkdir -p "$stage/lib"
cp -R build "$stage/build"
cp "$lib" "$stage/lib/"
cp -R src "$stage/src"
cp -R docs/src "$stage/docs"
cp README.md "$stage/"

tar -C "$stage" -czf "$output" .
echo "package release: wrote $output"
