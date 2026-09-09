#!/bin/sh
# The lease-sequencer stability check.
#
# Builds the node binary, starts the six-node / three-DC cluster, drives the
# join verb on the three DC joiners and the increment verb on two of them
# (one joiner stays at weight 0), lets the lease policy run, and then
# asserts, from the per-node logs:
#
#   1. the cluster stabilizes: the holder's renew stream is steady;
#   2. each non-holder polls at least 3 times, with per-poll log lines;
#   3. the holder's renewal cadence is ~250 ms (2x the poll cadence);
#   4. per kill cycle: SIGKILL the holder, wait 2000 ms, and a survivor
#      steals the lease within a bounded window (a new holder's grant after
#      the old expiry);
#   5. per kill cycle: the restarted leader reincarnates (identity bump,
#      the peers' remap notice) and the cluster re-stabilizes;
#   6. no two holders' lease windows ever overlap.
#
# Three kill/restart cycles run. Every spawned process is killed on exit.
set -u
cd "$(dirname "$0")" || exit 1

BIN=./target/release/lease-sequencer
CLIENT=./target/release/lease-client
CONFIG=config/cluster.jsonl
RUN=run/check
NAMES="dc1-node1 dc2-node1 dc3-node1 dc1-node2 dc2-node2 dc3-node2"

fail() {
    echo "FAIL: $*"
    exit 1
}

# A stale node from an earlier manual run would hold our ports; it belongs
# to this example binary, so kill it before binding.
pkill -9 -f "release/lease-sequencer --name" 2>/dev/null
sleep 0.2

cargo build --release --quiet || fail "build"

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
    # Per-node info-level files by default; the operator opts the cluster
    # into debug/trace detail by exporting RUST_LOG (see README).
    RUST_LOG="${RUST_LOG:-info}" "$BIN" --name "$name" --config "$CONFIG" \
        --client "127.0.0.1:$tcp" \
        --state "$RUN/state/$name.state" \
        --log "$RUN/logs/$name.log" \
        --heartbeat-ms 100 --election-ms 1000 --recovery-ms 1000 \
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

name_of() {
    # The live id may be a bumped (high-band) identity; the descriptor id is
    # the low band: id mod 2^24.
    desc=$(( $1 % 16777216 ))
    for n in $NAMES; do
        [ "$(id_of "$n")" = "$desc" ] && {
            echo "$n"
            return 0
        }
    done
    echo ""
}

total_renews() {
    grep -h "lease-attempt " "$RUN"/logs/*.log* 2>/dev/null | grep -c "op=renew"
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

# The node id holding the lease (the latest grant across all logs).
holder_id() {
    grep -h "grant node=" "$RUN"/logs/*.log* 2>/dev/null | awk '
        {
            ts = 0; node = 0
            for (i = 1; i <= NF; i++) {
                if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                if (index($i, "node=") == 1) node = substr($i, 6) + 0
            }
            if (ts > best) { best = ts; holder = node }
        }
        END { print holder + 0 }'
}

# The last granted expiry of one node id.
last_expiry_of() {
    grep -h "grant node=" "$RUN"/logs/*.log* 2>/dev/null | awk -v holder="$1" '
        {
            ts = 0; node = 0; expiry = 0
            for (i = 1; i <= NF; i++) {
                if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                if (index($i, "node=") == 1) node = substr($i, 6) + 0
                if (index($i, "expiry=") == 1) expiry = substr($i, 8) + 0
            }
            if (node == holder && ts > best) { best = ts; found = expiry }
        }
        END { print found + 0 }'
}

# A grant by a node other than `holder` after `after`; prints "ts node op".
wait_for_steal() {
    after=$1
    holder=$2
    deadline=$3
    while [ "$(now_ms)" -lt "$deadline" ]; do
        line=$(grep -h "grant node=" "$RUN"/logs/*.log* 2>/dev/null | awk -v after="$after" -v holder="$holder" '
            {
                ts = 0; node = 0; op = ""
                for (i = 1; i <= NF; i++) {
                    if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                    if (index($i, "node=") == 1) node = substr($i, 6) + 0
                    if (index($i, "op=") == 1) op = substr($i, 4)
                }
                if (ts > after && node != holder) { print ts, node, op; exit }
            }')
        [ -n "$line" ] && {
            echo "$line"
            return 0
        }
        sleep 0.3
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

for spec in "4 dc1-node2 127.0.0.1:41104" "5 dc2-node2 127.0.0.1:41105" "6 dc3-node2 127.0.0.1:41106"; do
    set -- $spec
    drive_verb join "$1" "$2" "$3" || fail "join $2 (id $1) not accepted within 90s"
    echo "join: id $1 ($2) accepted"
done
drive_verb increment 4 || fail "increment 4 not accepted within 90s"
echo "increment: id 4 accepted"
drive_verb increment 5 || fail "increment 5 not accepted within 90s"
echo "increment: id 5 accepted (id 6 stays at weight 0)"

deadline=$(( $(now_ms) + 30000 ))
before=$(total_renews)
while [ $(( $(total_renews) - before )) -lt 8 ]; do
    [ "$(now_ms)" -gt "$deadline" ] && fail "cluster did not re-stabilize after joins"
    sleep 0.3
done

# ---------------------------------------------------- stability window ----

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
echo "measured renewal cadence: ${MEAN_RENEW} ms"

overlap=$(awk '
    { for (i = 1; i <= NF; i++) if (index($i, "ts=") == 1) print substr($i, 4), $0 }
' "$RUN"/logs/*.log* | sort -n | cut -d' ' -f2- | awk '
    /grant node=/{
        ts = 0; node = 0; expiry = 0
        for (i = 1; i <= NF; i++) {
            if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
            else if (index($i, "node=") == 1) node = substr($i, 6) + 0
            else if (index($i, "expiry=") == 1) expiry = substr($i, 8) + 0
        }
        for (h in live) if (h != node && live[h] > ts) {
            print "holders " h " and " node " overlap at " ts
            bad = 1
        }
        if (expiry > live[node]) live[node] = expiry
    }
    END { exit bad ? 1 : 0 }
') || fail "overlapping lease windows: $overlap"
echo "overlap check: no two holders' windows overlap across $(cat "$RUN"/logs/*.log* | grep -c 'grant node=') grants"

# ------------------------------------------------------- kill cycles ----

cycle=1
while [ "$cycle" -le 3 ]; do
    hid=$(holder_id)
    [ "$hid" -eq 0 ] && fail "cycle $cycle: no holder to kill"
    hname=$(name_of "$hid")
    [ -z "$hname" ] && fail "cycle $cycle: holder id $hid maps to no descriptor name"
    hpid=$(cat "$RUN/pid.$hname")
    old_expiry=$(last_expiry_of "$hid")
    kill -9 "$hpid" 2>/dev/null
    kill_ts=$(now_ms)
    echo "cycle $cycle: SIGKILL holder $hname (id $hid, pid $hpid) at $kill_ts, old expiry $old_expiry"

    sleep 2

    after=$(( kill_ts > old_expiry ? kill_ts : old_expiry ))
    steal=$(wait_for_steal "$after" "$hid" $(( kill_ts + 30000 ))) \
        || fail "cycle $cycle: lease not stolen by a survivor within 30 s of the kill"
    echo "cycle $cycle: stolen at $steal"

    start_node "$hname"

    boot_deadline=$(( $(now_ms) + 30000 ))
    bumped=""
    while [ "$(now_ms)" -lt "$boot_deadline" ]; do
        bumped=$(awk -v after="$kill_ts" -v desc="$hid" '
            /boot name=/{
                ts = 0; own = 0; inc = 0
                for (i = 1; i <= NF; i++) {
                    if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                    else if (index($i, "own=") == 1) own = substr($i, 5) + 0
                    else if (index($i, "incarnation=") == 1) inc = substr($i, 13) + 0
                }
                if (ts > after && inc >= 1 && own != desc) { print own; exit }
            }' "$RUN/logs/$hname."*".log"*)
        [ -n "$bumped" ] && break
        sleep 0.3
    done
    [ -z "$bumped" ] && fail "cycle $cycle: restarted $hname did not reincarnate (no identity bump in its boot note)"
    echo "cycle $cycle: $hname reincarnated as id $bumped"

    remap_deadline=$(( $(now_ms) + 30000 ))
    remaps=0
    while [ "$(now_ms)" -lt "$remap_deadline" ]; do
        remaps=$(grep -l "remap old=$hid new=$bumped" "$RUN"/logs/*.log* 2>/dev/null | grep -v "$hname" | wc -l | tr -d ' ')
        [ "$remaps" -ge 2 ] && break
        sleep 0.3
    done
    [ "$remaps" -ge 2 ] || fail "cycle $cycle: remap notice old=$hid new=$bumped seen at only $remaps peers"
    echo "cycle $cycle: remap notice seen at $remaps peers"

    held_again=$(grep -h "grant node=" "$RUN"/logs/$hname.*.log | awk -v bumped="$bumped" '
        {
            node = 0
            for (i = 1; i <= NF; i++) if (index($i, "node=") == 1) node = substr($i, 6) + 0
            if (node == bumped) { found = 1 }
        }
        END { if (found) print "yes"; else print "no" }')

    deadline=$(( $(now_ms) + 30000 ))
    before=$(total_renews)
    while [ $(( $(total_renews) - before )) -lt 8 ]; do
        [ "$(now_ms)" -gt "$deadline" ] && fail "cycle $cycle: cluster did not re-stabilize after the restart"
        sleep 0.3
    done
    echo "cycle $cycle: re-stabilized; $hname held the lease again after rejoin: $held_again"
    echo "$cycle $hid $bumped $steal held_again=$held_again" >> "$RUN/cycles.txt"
    cycle=$(( cycle + 1 ))
done

# ------------------------------------------------------------- report ----

echo ""
echo "GREEN: stability check passed (3 kill/restart cycles)"
echo "measured renewal cadence: ${MEAN_RENEW} ms"
printf '%s\n' "$cadence" | grep '^MEASURED poll' | while read -r _ name value; do
    echo "measured poll cadence $name: ${value} ms"
done
[ -f "$RUN/cycles.txt" ] && cat "$RUN/cycles.txt"
exit 0
