#!/usr/bin/env sh
# client-signal.sh — find a lease-load client and drive its signal gate.
#
# stop   = SIGUSR1 silence: the client stops every outbound op, forgets
#          holdership, and re-enters as a NON-holder on the next start.
# start  = SIGUSR2: the silent client begins the chase from a GET probe.
# status = report whether the exact process name is alive.
#
# pid resolution: --pidfile first; otherwise `pgrep -x lease-load` (the
# exact-name discipline — never `pkill -f`, which matches this script's
# own ssh command line). With no pidfile, 0 or >1 matches is an error.
#
# --host runs the identical resolution and signal over one ssh
# invocation. Anchors (the wall-clock ms in the operator sheet) are the
# operator's job, logged on the acting host immediately before the
# action — this script timestamps nothing.

set -eu

usage() {
	echo "usage: client-signal.sh stop|start|status [--pidfile FILE] [--host USER@HOST]" >&2
	exit 2
}

case ${1:-} in
stop | start | status) action=$1 ;;
*) usage ;;
esac
shift

pidfile=
host=
while [ "$#" -gt 0 ]; do
	case $1 in
	--pidfile)
		if [ "$#" -lt 2 ]; then
			usage
		fi
		pidfile=$2
		shift 2
		;;
	--host)
		if [ "$#" -lt 2 ]; then
			usage
		fi
		host=$2
		shift 2
		;;
	*) usage ;;
	esac
done

# Single-quote for the remote `sh -s --` argument stream. POSIX-safe.
quote() {
	printf "'"
	printf '%s' "$1" | sed "s/'/'\\\\''/g"
	printf "'"
}

if [ -n "$host" ]; then
	# Pipe this script to the remote shell: one ssh invocation, no
	# remote installation, identical pid resolution on the target host.
	remote_args="$action"
	if [ -n "$pidfile" ]; then
		remote_args="$remote_args --pidfile $(quote "$pidfile")"
	fi
	exec ssh "$host" "sh -s -- $remote_args" <"$0"
fi

resolve_pid() {
	if [ -n "$pidfile" ]; then
		if [ ! -f "$pidfile" ]; then
			echo "client-signal.sh: no pidfile $pidfile" >&2
			exit 1
		fi
		pid=$(cat "$pidfile")
		case $pid in
		'' | *[!0-9]*)
			echo "client-signal.sh: pidfile $pidfile has no pid" >&2
			exit 1
			;;
		esac
		return
	fi
	pids=$(pgrep -x lease-load || true)
	case $pids in
	'')
		echo "client-signal.sh: no lease-load process found (pgrep -x)" >&2
		exit 1
		;;
	*"$LF"*)
		echo "client-signal.sh: ambiguous: more than one lease-load" \
			"process; use --pidfile:$LF$pids" >&2
		exit 1
		;;
	esac
	pid=$pids
}

kill_to() {
	case $1 in
	--start) signal=USR2 ;;
	--stop) signal=USR1 ;;
	*) usage ;;
	esac
	resolve_pid
	kill -"$signal" "$pid"
	echo "lease-load pid=$pid: SIG$signal sent"
}

status_report() {
	resolve_pid
	if kill -0 "$pid" 2>/dev/null; then
		echo "lease-load pid=$pid: running"
	else
		echo "lease-load pid=$pid: not running"
		exit 1
	fi
}

LF='
'

case $action in
stop) kill_to --stop ;;
start) kill_to --start ;;
status) status_report ;;
esac
