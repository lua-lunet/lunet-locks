#!/bin/sh
# The standby AOF + console demo check.
#
# Builds the node and feed binaries, starts the six-node / three-DC cluster,
# runs dc1-node2 as the STANDBY TELEMETRY NODE, and drives the join verb on
# all three DC joiners (the standby first) and the increment verb on the two
# non-standby joiners. The standby enters at weight 0 (never promoted),
# holds no vote, and feeds its committed lock transitions to the async AOF
# writer (--aof-dir), whose active file rolls at exactly 2 MiB with fsync
# only on the periodic timer, at roll, and at shutdown. Its lease driver
# still converses with the leader like every node's does: the round-trip
# traffic is what carries the era evidence the standby needs to keep
# tracking the cluster while it applies the stream.
#
# The script then starts the console stack (bun mock + lock-feed against the
# standby's AOF directory + nginx with the static SPA) and asserts,
# headless (curl/grep only):
#
#   1. committed events land in the standby's AOF files;
#   2. the feed serves them (REST listing and file bytes);
#   3. the console's data endpoint (/feed/files through the nginx edge)
#      returns them;
#   4. the cluster cadence is unaffected: the holder's renewal cadence is
#      ~250 ms (2x the poll cadence), measured from the voting nodes' logs
#      while the standby's AOF writer and the feed run.
#
# Every spawned process (nodes, mock, feed, nginx) is killed on exit.
set -u
cd "$(dirname "$0")" || exit 1

BIN=./target/release/lease-sequencer
CLIENT=./target/release/lease-client
CONFIG=config/cluster.jsonl
RUN=run/standby
NAMES="dc1-node1 dc2-node1 dc3-node1 dc1-node2 dc2-node2 dc3-node2"
STANDBY=dc1-node2
CONSOLE=../../console
FEED_BIN=../../ext/lock_feed/target/release/lock-feed
FEED_PORT=8482
CONSOLE_PORT=8480

fail() {
    echo "FAIL: $*"
    exit 1
}

# A stale process from an earlier run would hold our ports; they belong to
# this example, so kill them before binding.
pkill -9 -f "release/lease-sequencer --name" 2>/dev/null
pkill -9 -f "release/lock-feed --dir" 2>/dev/null
make -C "$CONSOLE" down >/dev/null 2>&1
sleep 0.2

cargo build --release --quiet || fail "build"
cargo build --release --quiet --manifest-path ../../ext/lock_feed/Cargo.toml || fail "feed build"

rm -rf "$RUN"
mkdir -p "$RUN/logs" "$RUN/state" "$RUN/aof" || exit 1
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
    # Per-node info-level files by default; the operator opts the cluster
    # into debug/trace detail by exporting RUST_LOG (see README).
    if [ "$name" = "$STANDBY" ]; then
        RUST_LOG="${RUST_LOG:-info}" "$BIN" --name "$name" --config "$CONFIG" \
            --client "127.0.0.1:$tcp" \
            --state "$RUN/state/$name.state" \
            --log "$RUN/logs/$name.log" \
            --aof-dir "$RUN/aof" --aof-flush-ms 500 \
            --heartbeat-ms 100 --election-ms 1000 --recovery-ms 1000 \
            > "$RUN/logs/$name.stderr" 2>&1 &
    else
        RUST_LOG="${RUST_LOG:-info}" "$BIN" --name "$name" --config "$CONFIG" \
            --client "127.0.0.1:$tcp" \
            --state "$RUN/state/$name.state" \
            --log "$RUN/logs/$name.log" \
            --heartbeat-ms 100 --election-ms 1000 --recovery-ms 1000 \
            > "$RUN/logs/$name.stderr" 2>&1 &
    fi
    echo $! > "$RUN/pid.$name"
    echo $! >> "$RUN/pids"
}

cleanup() {
    for pid in $(cat "$RUN/pids" 2>/dev/null) $(cat "$RUN"/pid.* 2>/dev/null); do
        kill -9 "$pid" 2>/dev/null
    done
    make -C "$CONSOLE" down >/dev/null 2>&1
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

total_renews() {
    grep -h "lease-attempt " "$RUN"/logs/*.log* 2>/dev/null | grep -c "op=renew"
}

aof_events() {
    cat "$RUN"/aof/*.bin 2>/dev/null | LC_ALL=C grep -a -o "LKE1" | wc -l | tr -d ' '
}

# Drive one admin verb at the cluster: try every node's client port until
# one accepts (the leader); non-leaders answer {"error":"not_leader"}.
drive_verb() {
    action=$1
    id=$2
    name=${3:-}
    endpoint=${4:-}
    deadline=$(( $(now_ms) + 90000 ))
    while [ "$(now_ms)" -lt "$deadline" ]; do
        for n in $NAMES; do
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

# ---------------------------------------------------------------- boot ----

for name in $NAMES; do
    start_node "$name"
done

deadline=$(( $(now_ms) + 60000 ))
while [ "$(total_renews)" -lt 4 ]; do
    [ "$(now_ms)" -gt "$deadline" ] && fail "cluster did not stabilize (renews=$(total_renews))"
    sleep 0.3
done
echo "boot: cluster stabilized, holder stream running"

# ----------------------------------------------------- joins/promotes ----

# The standby must be the FIRST post-genesis joiner: the learner folds the
# era that admits it and then tracks the cluster across the later eras.
for spec in "4 dc1-node2 127.0.0.1:41104" "5 dc2-node2 127.0.0.1:41105" "6 dc3-node2 127.0.0.1:41106"; do
    set -- $spec
    drive_verb join "$1" "$2" "$3" || fail "join $2 (id $1) not accepted within 90s"
    echo "join: id $1 ($2) accepted"
done
drive_verb increment 5 || fail "increment 5 not accepted within 90s"
echo "increment: id 5 accepted"
drive_verb increment 6 || fail "increment 6 not accepted within 90s"
echo "increment: id 6 accepted ($STANDBY stays at weight 0: the standby telemetry node)"

deadline=$(( $(now_ms) + 30000 ))
before=$(total_renews)
while [ $(( $(total_renews) - before )) -lt 8 ]; do
    [ "$(now_ms)" -gt "$deadline" ] && fail "cluster did not re-stabilize after joins"
    sleep 0.3
done

# ---------------------------------------------- console stack (feed) ----

# lock-feed serves the STANDBY's AOF series; nginx serves the static SPA and
# maps /feed/ to the feed. The bun mock backs the console's admin views.
PORT=$CONSOLE_PORT MOCK_PORT=8481 FEED_PORT=$FEED_PORT \
    FEED_DIR="$(pwd)/$RUN/aof" FEED_BIN="$(cd "$(dirname "$FEED_BIN")" && pwd)/$(basename "$FEED_BIN")" \
    make -C "$CONSOLE" up >/dev/null || fail "console stack did not come up (see $CONSOLE/tmp)"

creds=$(cat "$CONSOLE/tmp/credentials") || fail "console credentials missing"
echo "console: up (127.0.0.1:$CONSOLE_PORT, feed port $FEED_PORT, AOF dir $RUN/aof)"

# ------------------------------------------------- headless assertions ----

# 1. Events land in the standby's AOF files. The cadence here is the
#    cluster's own: the standby applies the voting nodes' committed stream.
deadline=$(( $(now_ms) + 30000 ))
while [ "$(aof_events)" -lt 20 ]; do
    [ "$(now_ms)" -gt "$deadline" ] && fail "standby AOF never received events (count=$(aof_events))"
    sleep 0.5
done
echo "aof: $(aof_events) events landed in the standby's AOF series"
aof_count=$(aof_events)

# 2. The feed serves them (REST listing + file bytes, CRC-validated parse).
files_json=$(curl -s --max-time 3 "http://127.0.0.1:$FEED_PORT/files") || fail "feed /files unreachable"
echo "$files_json" | grep -q '"open":true' || fail "feed /files shows no open AOF file: $files_json"
open_name=$(printf '%s' "$files_json" | sed -n 's/.*"name":"\(ev-open-[0-9]*\.bin\)".*/\1/p')
[ -n "$open_name" ] || fail "feed /files missing the open AOF file name: $files_json"
curl -s --max-time 3 "http://127.0.0.1:$FEED_PORT/files/$open_name" -o "$RUN/feed-bytes.bin" \
    || fail "feed /files/$open_name unreachable"
feed_count=$(LC_ALL=C grep -a -o "LKE1" "$RUN/feed-bytes.bin" | wc -l | tr -d ' ')
[ "$feed_count" -ge 20 ] || fail "feed served only $feed_count events from the AOF open file"
echo "feed: served $feed_count events from $open_name (of $(aof_events) in the series)"

# 3. The console's data endpoint (/feed/* through the nginx edge) returns
#    the same series to the SPA.
edge_json=$(curl -s -u "$creds" --max-time 3 "http://127.0.0.1:$CONSOLE_PORT/feed/files") \
    || fail "console edge /feed/files unreachable"
echo "$edge_json" | grep -q '"open":true' || fail "console edge /feed/files shows no open AOF file: $edge_json"
printf '%s' "$edge_json" | grep -q "$open_name" || fail "console edge /feed/files missing the AOF open file: $edge_json"
edge_bytes=$(curl -s -u "$creds" --max-time 3 "http://127.0.0.1:$CONSOLE_PORT/feed/files/$open_name" -o "$RUN/edge-bytes.bin" \
    && LC_ALL=C grep -a -o "LKE1" "$RUN/edge-bytes.bin" | wc -l | tr -d ' ')
[ "$edge_bytes" = "$feed_count" ] || fail "console edge served $edge_bytes events, feed served $feed_count"
echo "console: /feed/files through the edge returns the AOF series ($edge_bytes events)"

# --------------------------------------------- stability + cadence ----

T0=$(now_ms)
sleep 6
T1=$(now_ms)

echo "stability window: $T0 .. $T1"

cadence=$(awk -v t0="$T0" -v t1="$T1" '
    /lease-attempt /{
        ts = 0; node = ""; op = ""
        for (i = 2; i <= NF; i++) {
            if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
            else if (index($i, "node=") == 1) node = substr($i, 6)
            else if (index($i, "op=") == 1) op = substr($i, 4)
        }
        if (ts < t0 || ts > t1) next
        if (op == "renew") {
            renew_n[node]++
            if (prev_renew[node] > 0) { s_renew += ts - prev_renew[node]; n_renew++ }
            prev_renew[node] = ts
        }
        if (op == "get") {
            get_n[node]++
            if (prev_get[node] > 0) { s_get[node] += ts - prev_get[node]; c_get[node]++ }
            prev_get[node] = ts
        }
    }
    END {
        holder = ""; best = 0
        for (n in renew_n) if (renew_n[n] > best) { best = renew_n[n]; holder = n }
        if (holder == "") { print "FAIL no renew attempts in the stability window"; exit 1 }
        if (best < 10) { print "FAIL holder " holder " renewed only " best " times in the window (stream not steady)"; exit 1 }
        if (n_renew < 1) { print "FAIL no renew interval measurable"; exit 1 }
        mean_renew = s_renew / n_renew
        if (mean_renew < 150 || mean_renew > 400) { print "FAIL renewal cadence " mean_renew " ms outside ~250 ms"; exit 1 }
        poll_seen = 0
        for (n in get_n) {
            if (n == holder) continue
            if (get_n[n] < 3) { print "FAIL non-holder " n " polled only " get_n[n] " times (<3)"; exit 1 }
            if (c_get[n] > 0) {
                pm = s_get[n] / c_get[n]
                poll_seen = 1
                if (pm < 300 || pm > 900) { print "FAIL poll cadence " n " " pm " ms outside the expected window"; exit 1 }
                if (pm < 1.2 * mean_renew || pm > 3.5 * mean_renew) { print "FAIL poll/renew ratio " pm "/" mean_renew " for " n; exit 1 }
                printf "MEASURED poll %s %.0f\n", n, pm
            }
        }
        if (!poll_seen) { print "FAIL no non-holder poll cadence measurable"; exit 1 }
        printf "MEASURED renew %.0f\n", mean_renew
        printf "MEASURED holder %s\n", holder
    }
' "$RUN"/logs/*.log*) || fail "stability-window assertions: $cadence"

echo "$cadence"
MEAN_RENEW=$(printf '%s\n' "$cadence" | awk '/^MEASURED renew /{printf "%.0f", $3}')
echo "measured renewal cadence with the standby running: ${MEAN_RENEW} ms"

# The AOF kept receiving events through the window (the standby's series is
# live, not a boot-time snapshot).
final_count=$(aof_events)
[ "$final_count" -gt "$aof_count" ] || fail "the AOF series stopped growing during the stability window ($aof_count -> $final_count)"
echo "aof: series grew $aof_count -> $final_count events across the stability window"

# ------------------------------------------------------------ report ----

echo ""
echo "GREEN: standby AOF + console check passed"
echo "measured renewal cadence with the standby running: ${MEAN_RENEW} ms"
printf '%s\n' "$cadence" | grep '^MEASURED poll' | while read -r _ name value; do
    echo "measured poll cadence $name: ${value} ms"
done
exit 0
