#!/bin/sh
set -eu

# Containers intentionally expose TCP to the host via Docker port publishing;
# Lunet requires this explicit opt-in for a non-loopback listener.
exec /opt/lunet/lunet-run --dangerously-skip-loopback-restriction /app/build/server.lua "$@"
