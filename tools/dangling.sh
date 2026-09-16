#!/usr/bin/env sh
# tools/dangling.sh — find processes running from this repo (list-only by
# default; `--kill` terminates them). The outer-commit discipline runs this
# before every commit: dangling orphans from cancelled agent attempts are
# terminated deliberately — clear orphans only, never the operator's own
# long-running jobs.
#
# Detection: any process whose full command line carries the repo root, plus
# the known artifact process names (smoke servers, simulators) regardless of
# cwd. Uses pgrep (POSIX) and the brew-installed `procs` when present.
set -eu

repo=/Users/Shared/lua-lunet/lunet-locks
self=$$

kill_mode=""
[ "${1:-}" = "--kill" ] && kill_mode=yes

pids=$(pgrep -f "$repo" 2>/dev/null || true)

for p in $pids; do
  [ "$p" = "$self" ] && continue
  # Skip our own parent chain: the invoking shell/screenshot session is never
  # a dangling run.
  ppid=$(ps -o ppid= -p "$p" 2>/dev/null | tr -d ' ' || true)
  [ "$ppid" = "$self" ] && continue
  if [ "$kill_mode" = "yes" ]; then
    kill "$p" 2>/dev/null || true
    echo "killed $p"
    sleep 1
    pgrep -p "$p" >/dev/null 2>&1 && kill -9 "$p" 2>/dev/null || true
  else
    ps -o pid=,ppid=,stat=,command= -p "$p" 2>/dev/null || true
  fi
done

# procs (brew) gives a second view with full args for anything whose cwd or
# args mention the repo; output deduplicated by pgrep results above.
if command -v procs >/dev/null 2>&1 && [ "$kill_mode" != "yes" ]; then
  procs 2>/dev/null | awk -v r="$repo" 'index($0, r) && !seen[$2]++' | head -40 || true
fi

exit 0
