#!/usr/bin/env sh
# Thin wrapper — all lifecycle logic lives in tooling/lib/consolectl.tl.
set -eu
here=$(CDPATH='' cd -- "$(dirname -- "$0")" && pwd)
exec "$here/../tooling/console.lua" down
