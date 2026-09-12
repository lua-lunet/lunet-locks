#!/bin/sh
# The item19 phi-accrual kill-leader test: a 3-node genesis cluster on
# 127.0.0.1, 10 ms heartbeats, convergence verified, then the leader is
# SIGKILLed and the run measures:
#   - phi detection latency (the phi-detect log line's ts vs the kill ts),
#   - the actual new-leader (view change) latency,
#   - lease reacquire latency (the steal grant).
# Logs land in .tmp/delegation/item19-report/phi-run/.
set -u
cd "$(dirname "$0")" || exit 1

BIN=./target/release/lease-sequencer
CLIENT=./target/release/lease-client
REPORT=../../.tmp/delegation/item19-report
RUN="$REPORT/phi-run"
CONFIG="$REPORT/phi-cluster.jsonl"
NAMES="n1 n2 n3"

fail() {
    echo "FAIL: $*"
    exit 1
}

pkill -9 -f "release/lease-sequencer --name n" 2>/dev/null
sleep 0.2

rm -rf "$RUN"
mkdir -p "$RUN/logs" "$RUN/state" || exit 1
: > "$RUN/pids"

port_of() {
    awk -v n="$1" 'index($0, "\"name\":\"" n "\"") {
        match($0, /"port":[0-9]+/)
        print substr($0, RSTART + 8, RLENGTH - 8)
    }' "$CONFIG"
}

client_port_of() {
    echo $(( $(port_of "$1") + 1000 ))
}

start_node() {
    name=$1
    tcp=$(client_port_of "$name")
    RUST_LOG="${RUST_LOG:-info}" "$BIN" --name "$name" --config "$CONFIG" \
        --client "127.0.0.1:$tcp" \
        --state "$RUN/state/$name.state" \
        --log "$RUN/logs/$name.log" \
        --heartbeat-ms 10 --phi-threshold 1.0 --phi-safety 2.0 \
        > "$RUN/logs/$name.stderr" 2>&1 &
    echo $! > "$RUN/pid.$name"
    echo $! >> "$RUN/pids"
}

cleanup() {
    for pid in $(cat "$RUN/pids" 2>/dev/null) $(cat "$RUN"/pid.* 2>/dev/null); do
        kill -9 "$pid" 2>/dev/null
    done
}
trap cleanup EXIT INT TERM

now_ms() {
    "$CLIENT" now
}

id_of() {
    awk -v n="$1" 'index($0, "\"name\":\"" n "\"") {
        match($0, /"id":[0-9]+/)
        print substr($0, RSTART + 5, RLENGTH - 5)
    }' "$CONFIG"
}

start_node n1
start_node n2
start_node n3

# --- convergence: the cluster elects a leader and holds the lease -------
deadline=$(( $(now_ms) + 30000 ))
while [ "$(grep -hc 'grant node=' "$RUN"/logs/*.log* 2>/dev/null | paste -sd+ | bc 2>/dev/null || echo 0)" -lt 1 ]; do
    [ "$(now_ms)" -gt "$deadline" ] && fail "cluster did not grant a lease"
    sleep 0.3
done
echo "converged: lease granted"

# Identify the consensus leader: each node logs status lines naming leader=<id>.
sleep 2
leader_id=$(awk '
    /status state=/{
        ts = 0; node = 0; leader = 0; state = ""
        for (i = 1; i <= NF; i++) {
            if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
            else if (index($i, "node=") == 1) node = substr($i, 6) + 0
            else if (index($i, "state=") == 1) state = substr($i, 7)
            else if (index($i, "leader=") == 1) leader = substr($i, 8) + 0
        }
        if (state == "0" && ts > best) { best = ts; found = leader }
    }
    END { print found + 0 }' "$RUN"/logs/*.log*)
[ "$leader_id" -eq 0 ] 2>/dev/null && fail "no leader identified in the status logs"
leader_name=""
for n in $NAMES; do
    [ "$(id_of "$n")" = "$leader_id" ] && leader_name=$n
done
[ -z "$leader_name" ] && fail "leader id $leader_id maps to no node name"
leader_pid=$(cat "$RUN/pid.$leader_name")
echo "leader: $leader_name (id $leader_id, pid $leader_pid)"

# --- steady state: heartbeat interval distribution ----------------------
steady_t0=$(now_ms)
sleep 4
steady_t1=$(now_ms)
awk -v t0="$steady_t0" -v t1="$steady_t1" '
    /phi-interval /{
        ts = 0; dt = 0; node = ""
        for (i = 1; i <= NF; i++) {
            if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
            else if (index($i, "dt=") == 1) dt = substr($i, 4) + 0
            else if (index($i, "node=") == 1) node = substr($i, 6) + 0
        }
        if (ts >= t0 && ts <= t1 && dt > 0 && dt <= 200) {
        n[node]++; s[node] += dt
        if (!(node in mn) || dt < mn[node]) mn[node] = dt
        if (dt > mx[node]) mx[node] = dt
    }
    }
    END {
        for (node in n) printf "STEADY node=%s samples=%d min=%d mean=%.1f max=%d\n", node, n[node], mn[node], s[node]/n[node], mx[node]
    }' "$RUN"/logs/*.log* | tee "$RUN/steady.txt"

# --- kill the leader ----------------------------------------------------
kill_ts=$(now_ms)
kill -9 "$leader_pid" 2>/dev/null
echo "kill: SIGKILL $leader_name (pid $leader_pid) at $kill_ts"

# The phi detector fires in milliseconds; the core's own view-change gate
# (primary_timeout, 5000 ms) decides when the fence actually runs. Let the
# aftermath settle before parsing it.
sleep 8

# phi detection: the FIRST phi-detect line after the kill (head -1: one per
# survivor, take the earliest report).
detect_line=$(awk -v kill="$kill_ts" '
    /phi-detect /{
        ts = 0
        for (i = 1; i <= NF; i++) if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
        if (ts >= kill && (best == 0 || ts < best)) { best = ts; line = $0 }
    }
    END { if (best) print line }' "$RUN"/logs/*.log*)
[ -z "$detect_line" ] && echo "phi-detect: NOT LOGGED (phi monitoring inert)"
detect_ts=$(printf '%s\n' "$detect_line" | head -1 | awk '{ for (i = 1; i <= NF; i++) if (index($i, "ts=") == 1) print substr($i, 4) + 0 }')

# the new leader: the first post-kill leader-change status line naming a
# different leader id.
new_leader_line=$(awk -v kill="$kill_ts" -v dead="$leader_id" '
    /leader leader=/{
        ts = 0; node = 0; leader = 0; era = 0; view = 0
        for (i = 1; i <= NF; i++) {
            if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
            else if (index($i, "node=") == 1) node = substr($i, 6) + 0
            else if (index($i, "leader=") == 1) leader = substr($i, 8) + 0
            else if (index($i, "era=") == 1) era = substr($i, 5) + 0
            else if (index($i, "view=") == 1) view = substr($i, 6) + 0
        }
        if (ts >= kill && node != leader && leader != dead) { print; exit }
    }' "$RUN"/logs/*.log*)
new_leader_ts=$(printf '%s\n' "$new_leader_line" | head -1 | awk '{ for (i = 1; i <= NF; i++) if (index($i, "ts=") == 1) print substr($i, 4) + 0 }')
echo "new leader line: ${new_leader_line:-NONE}"

# lease reacquire: the first post-kill grant by a different node.
steal_line=$(awk -v kill="$kill_ts" -v dead="$leader_id" '
    /grant node=/{
        ts = 0; node = 0; op = ""
        for (i = 1; i <= NF; i++) {
            if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
            else if (index($i, "node=") == 1) node = substr($i, 6) + 0
            else if (index($i, "op=") == 1) op = substr($i, 4)
        }
        if (ts >= kill && node != dead) { print; exit }
    }' "$RUN"/logs/*.log*)
steal_ts=$(printf '%s\n' "$steal_line" | head -1 | awk '{ for (i = 1; i <= NF; i++) if (index($i, "ts=") == 1) print substr($i, 4) + 0 }')
echo "steal line: ${steal_line:-NONE}"

# exactly-one-new-leader: every survivor's last leader line agrees.
echo "--- last leader lines per survivor ---"
for n in $NAMES; do
    [ "$n" = "$leader_name" ] && continue
    last=$(grep -h "leader leader=" "$RUN/logs/$n."*".log" 2>/dev/null | tail -1)
    echo "$n: ${last:-none}"
done

echo ""
echo "=== item19 phi kill-leader measurement ==="
echo "kill ts:                    $kill_ts"
if [ -n "$detect_ts" ]; then
    echo "phi detection latency:      $((detect_ts - kill_ts)) ms"
else
    echo "phi detection latency:      NOT DETECTED"
fi
if [ -n "$new_leader_ts" ]; then
    echo "new leader latency:         $((new_leader_ts - kill_ts)) ms  (baseline ~5000 ms)"
else
    echo "new leader latency:         NOT CONVERGED within the window"
fi
if [ -n "$steal_ts" ]; then
    echo "lease reacquire latency:    $((steal_ts - kill_ts)) ms"
else
    echo "lease reacquire latency:    NOT CONVERGED within the window"
fi
{
    echo "kill_ts=$kill_ts"
    echo "detect_latency=$((detect_ts - kill_ts))"
    echo "new_leader_latency=$((new_leader_ts - kill_ts))"
    echo "steal_latency=$((steal_ts - kill_ts))"
} > "$RUN/measurements.txt"
exit 0
