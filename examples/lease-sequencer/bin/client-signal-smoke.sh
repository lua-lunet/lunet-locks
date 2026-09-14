#!/usr/bin/env sh
# client-signal-smoke.sh — shellcheck + POSIX behavioural smoke for
# client-signal.sh: usage errors shellcheck clean; a fake trapped sh
# daemon under a pidfile proves the script finds the daemon by pidfile and
# delivers USR1 (silence) and USR2 (start). Teardown kills what it spawned.

set -eu

here=$(cd "$(dirname "$0")" && pwd)
SIGNAL="$here/client-signal.sh"

tmp=${TMPDIR:-/tmp}/client-signal-smoke.$$
daemon=
trap '
	rm -rf "$tmp"
	[ -n "$daemon" ] && kill -9 "$daemon" 2>/dev/null || true
' EXIT
mkdir -p "$tmp"

fail() {
	echo "client-signal-smoke.sh: $1" >&2
	exit 1
}

# ---- shellcheck ------------------------------------------------------------

if command -v shellcheck >/dev/null 2>&1; then
	shellcheck "$SIGNAL"
	echo "shellcheck: clean"
else
	fail "shellcheck not on PATH (mise: aqua:koalaman/shellcheck)"
fi

# ---- usage discipline ------------------------------------------------------

if sh "$SIGNAL" 2>/dev/null; then
	fail "no-argument invocation must exit 2"
fi
if sh "$SIGNAL" restart 2>/dev/null; then
	fail "unknown action must exit 2"
fi

# ---- fake trapped daemon under a pidfile -----------------------------------

pidfile="$tmp/lease-load.pid"
stopped_marker="$tmp/stopped.marker"
started_marker="$tmp/started.marker"

# The fake daemon: a sh that traps USR1 (touches its marker and exits —
# silence is final) and USR2 (touches its marker and keeps running).
# The sleep loop means a trap fires at the next wake-up, so every
# assertion polls instead of racing.
_spawn() {
	sh -c '
		trap ": > \"$0\"; exit 0" USR1
		trap ": > \"$1\"" USR2
		while :; do
			sleep 0.05
		done
	' "$stopped_marker" "$started_marker" &
	daemon=$!
}

# Wait for a marker file; polls its retirement, never races it.
_await() {
	i=0
	while [ $i -lt 100 ]; do
		test -f "$1" && return 0
		sleep 0.05
		i=$((i + 1))
	done
	fail "$1 never appeared"
}

rm -f "$tmp"/*.marker
_spawn
echo "$daemon" >"$pidfile"
sleep 0.2
kill -0 "$daemon" 2>/dev/null || fail "fake daemon did not start"

sh "$SIGNAL" stop --pidfile "$pidfile" >"$tmp/stop.out"
_await "$stopped_marker"
sleep 0.5
kill -9 "$daemon" 2>/dev/null || true
wait "$daemon" 2>/dev/null || true

rm -f "$tmp"/*.marker
_spawn
echo "$daemon" >"$pidfile"
sleep 0.2
sh "$SIGNAL" start --pidfile "$pidfile" >"$tmp/start.out"
_await "$started_marker"

sh "$SIGNAL" status --pidfile "$pidfile" >"$tmp/status.out" || true
sleep 0.1
grep -q "running" "$tmp/status.out" || fail "status did not report the live daemon"

kill -9 "$daemon" 2>/dev/null || true
wait "$daemon" 2>/dev/null || true
sleep 0.5
if sh "$SIGNAL" status --pidfile "$pidfile" 2>/dev/null; then
	fail "status reported a dead pid as running"
fi

echo "client-signal-smoke.sh: ok"
