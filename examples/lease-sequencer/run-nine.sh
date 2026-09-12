#!/bin/sh
# The nine-process localhost proof (item22 M4): 6 voting nodes (all
# genesis) + 3 zero-weight telemetry standbys over 127.0.0.1. The standbys
# join through the join verb and are never promoted; the leader's fan-out
# goes to 8 peers; the three standby AOF series record the commit stream
# with local nanosecond-clock headers.
#
# Phases:
#   A. 6 voting nodes only (5 peers), steady-state measurement window.
#   B. the 3 standbys start and join at weight 0 (8 peers), steady-state
#      measurement window.
#   C. SIGTERM the standbys: the clean-stop teardown record + the
#      unconditional flush.
#   D. the three AOF series parse through the typed reader and the merged
#      ns-aligned timeline excerpt prints.
#
# Every spawned process is killed on exit.
set -u
cd "$(dirname "$0")" || exit 1

BIN=./target/release/lease-sequencer
CLIENT=./target/release/lease-client
TIMELINE=./target/release/aof-timeline
CONFIG=config/cluster-nine.jsonl
RUN=run/nine
REPORT=${REPORT:-../../.tmp/delegation/item22-report}
VOTERS="dc1-node1 dc2-node1 dc3-node1 dc1-node2 dc2-node2 dc3-node2"
STANDBYS="dc1-tel dc2-tel dc3-tel"
MEASURE_A=20
MEASURE_B=25

fail() {
    echo "FAIL: $*"
    exit 1
}

pkill -9 -f "release/lease-sequencer --name" 2>/dev/null
sleep 0.3

cargo build --release --quiet || fail "build"

rm -rf "$RUN"
mkdir -p "$RUN/logs" "$RUN/state" "$RUN/aof/dc1-tel" "$RUN/aof/dc2-tel" "$RUN/aof/dc3-tel" || exit 1
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

id_of() {
    awk -v n="$1" 'index($0, "\"name\":\"" n "\"") {
        match($0, /"id":[0-9]+/)
        print substr($0, RSTART + 5, RLENGTH - 5)
    }' "$CONFIG"
}

start_node() {
    name=$1
    tcp=$(client_port_of "$name")
    args="--name $name --config $CONFIG --client 127.0.0.1:$tcp \
--state $RUN/state/$name.state --log $RUN/logs/$name.log \
--heartbeat-ms 100 --election-ms 1000 --recovery-ms 1000 \
--phi-timeout-min-ms 500 --phi-timeout-max-ms 1000"
    case "$name" in
        *-tel)
            RUST_LOG="${RUST_LOG:-info}" "$BIN" $args --aof-dir "$RUN/aof/$name" \
                > "$RUN/logs/$name.stderr" 2>&1 &
            ;;
        *)
            RUST_LOG="${RUST_LOG:-info}" "$BIN" $args \
                --telemetry-aof-dir "$RUN/aof/$name" \
                > "$RUN/logs/$name.stderr" 2>&1 &
            ;;
    esac
    echo $! > "$RUN/pid.$name"
    echo $! >> "$RUN/pids"
}

start_voters() {
    for name in $VOTERS; do
        start_node "$name"
    done
}

total_renews() {
    grep -h "lease-attempt " "$RUN"/logs/*.log* 2>/dev/null | grep -c "op=renew"
}

cadence_ms() {
    # The holder's renewal cadence: the median gap between consecutive
    # renew stamps of one node, from the logs, in ms (awk over ts=).
    grep -h "lease-attempt " "$RUN"/logs/*.log* 2>/dev/null \
        | grep "op=renew" | sed 's/.*ts=\([0-9]*\).*/\1/' | sort -n \
        | awk 'NR > 1 { print $1 - previous } { previous = $1 }' \
        | sort -n | awk '{ a[NR] = $1 } END { if (NR > 0) print a[int((NR + 1) / 2)]; else print -1 }'
}

cleanup() {
    for pid in $(cat "$RUN/pids" 2>/dev/null) $(cat "$RUN"/pid.* 2>/dev/null); do
        kill -9 "$pid" 2>/dev/null
    done
}
trap cleanup EXIT INT TERM

drive_verb() {
    action=$1
    id=$2
    name=${3:-}
    endpoint=${4:-}
    deadline=$(( $(date +%s%3N) + 90000 ))
    while [ "$(date +%s%3N)" -lt "$deadline" ]; do
        for n in $VOTERS; do
            reply=$("$CLIENT" --server "127.0.0.1:$(client_port_of "$n")" \
                --verb "$action" --id "$id" \
                ${name:+--name "$name"} ${endpoint:+--endpoint "$endpoint"} 2>/dev/null)
            case "$reply" in
                *'"accepted":true'*)
                    echo "$reply"
                    return 0
                    ;;
            esac
        done
        sleep 1
    done
    return 1
}

# -------------------------------------------------------------- phase A ----
echo "== phase A: 6 voting nodes (5 peers) =="
start_voters

deadline=$(( $(date +%s%3N) + 60000 ))
while [ "$(total_renews)" -lt 4 ]; do
    [ "$(date +%s%3N)" -gt "$deadline" ] && fail "phase A: cluster did not stabilize"
    sleep 0.3
done
echo "phase A: cluster stabilized"
cadence_a_before=$(cadence_ms)
sleep "$MEASURE_A"
cadence_a=$(cadence_ms)
echo "phase A: steady-state renewal cadence ~${cadence_a}ms (measured over ${MEASURE_A}s)"

# -------------------------------------------------------------- phase B ----
echo "== phase B: + 3 zero-weight standbys (8 peers) =="
for name in $STANDBYS; do
    start_node "$name"
done

for spec in "7 dc1-tel 127.0.0.1:41107" "8 dc2-tel 127.0.0.1:41108" "9 dc3-tel 127.0.0.1:41109"; do
    set -- $spec
    drive_verb join "$1" "$2" "$3" || fail "join $2 (id $1) not accepted within 90s"
    echo "phase B: join id $1 ($2) accepted at weight 0"
done

sleep 8
cadence_b_before=$(cadence_ms)
sleep "$MEASURE_B"
cadence_b=$(cadence_ms)
echo "phase B: steady-state renewal cadence ~${cadence_b}ms (measured over ${MEASURE_B}s)"

# -------------------------------------------------------------- phase C ----
echo "== phase C: clean stop (SIGTERM) of the standbys =="
for name in $STANDBYS; do
    kill -TERM "$(cat "$RUN/pid.$name")" && echo "phase C: SIGTERM $name"
done
sleep 3
for name in $STANDBYS; do
    grep -q "sigterm: clean stop" "$RUN/logs/$name"*.log* || fail "phase C: $name did not take the clean-stop path"
done
echo "phase C: all three standbys logged the clean stop"

# -------------------------------------------------------------- phase D ----
echo "== phase D: three AOF traces through the typed reader =="
for name in $STANDBYS; do
    [ -n "$(ls "$RUN/aof/$name"/*.aof 2>/dev/null)" ] || fail "phase D: $name has no .aof series"
done
wire_counts=$(for name in $STANDBYS; do
    count=$(cat "$RUN/aof/$name"/*.aof 2>/dev/null | LC_ALL=C grep -a -o $'\x01' | wc -l | tr -d ' ')
    echo -n "$name=$count "
done)
echo "phase D: raw marker-byte hits per standby: $wire_counts"

mkdir -p "$REPORT"
{
    "$TIMELINE" --stats --limit 60 \
        "$RUN/aof/dc1-tel" "$RUN/aof/dc2-tel" "$RUN/aof/dc3-tel"
} > "$REPORT/nine-timeline.txt" 2> "$REPORT/nine-timeline.stderr" || fail "phase D: the timeline tool failed (see $REPORT/nine-timeline.stderr)"

cp -r "$RUN/logs" "$REPORT/nine-logs" 2>/dev/null
head -20 "$REPORT/nine-timeline.txt"
echo "PASS: nine-process proof complete (report under $REPORT)"
