#!/bin/sh
# The E1/E2 local experiment runner (the experiment design's §2-§4).
#
#   E1   kill/rejoin latency under CSR: k kill->rejoin cycles rotating the
#        victim across the non-leader voting nodes, cold counts, under the
#        continuous SET/BUMP/GET load.
#   E2   the same shape per disk-mode variant at the recovery boundary:
#        e2v0 diskless (the default), e2v1 the single 4 KiB block + fsync
#        baseline, e2v2 the double-ring write.
#
# The cluster is the genesis 3-voter shape. The promoted-joiner legs are
# blocked by the upstream fourth finding (multi-era learner acquisition);
# this runner does NOT use promoted joiners -- every voting node is a
# genesis member, so a reincarnated node always reopens over the deployment
# genesis and rejoins through the ordinary forced-reconfiguration walk.
#
# Per iteration the runner records, from the logs: the kill->rejoin-serving
# latency (kill timestamp -> the reincarnated node's first status note with
# state=0 and voting=1), the (era, view) pair at kill and at serving, and
# for the E2 flush variants the recovery-boundary flush latency the adapter
# logged at the restart's dirty boot. Output: results.jsonl (one line per
# iteration) plus summary-<experiment>.json (percentiles per variant) and
# the same table on stdout.
#
# Every spawned process is killed on exit.
#
#   usage: experiment.sh --experiment e1|e2v0|e2v1|e2v2 [--k N]
#                        [--soak-ms N] [--lease-ms N] [--renew-fraction F]
#                        [--rate low|high] [--tag NAME]
set -u
cd "$(dirname "$0")" || exit 1

BIN=./target/release/lease-sequencer
CLIENT=./target/release/lease-client
LOAD=./target/release/lease-load
CONFIG=config/experiment.jsonl
NAMES="dc1-node1 dc2-node1 dc3-node1"

EXPERIMENT=""
K=3
SOAK_MS=2000
LEASE_MS=500
RENEW_FRACTION=0.5
RATE=low
TAG=""

usage_fail() {
    echo "FAIL: $*"
    exit 1
}

while [ $# -gt 0 ]; do
    case "$1" in
        --experiment) EXPERIMENT=$2; shift 2 ;;
        --k) K=$2; shift 2 ;;
        --soak-ms) SOAK_MS=$2; shift 2 ;;
        --lease-ms) LEASE_MS=$2; shift 2 ;;
        --renew-fraction) RENEW_FRACTION=$2; shift 2 ;;
        --rate) RATE=$2; shift 2 ;;
        --tag) TAG=$2; shift 2 ;;
        *) usage_fail "unknown argument $1" ;;
    esac
done

case "$EXPERIMENT" in
    e1)   VARIANT=diskless ;;
    e2v0) VARIANT=diskless ;;
    e2v1) VARIANT=single ;;
    e2v2) VARIANT=double-ring ;;
    *) usage_fail "--experiment must be one of e1|e2v0|e2v1|e2v2" ;;
esac

fail() {
    echo "FAIL: $*"
    exit 1
}

cargo build --release --quiet || fail "build"

RUN="run/exp-${EXPERIMENT}-${TAG:-manual}"
if [ -d "$RUN" ]; then
    RUN="${RUN}-$(date +%s)"
fi
mkdir -p "$RUN/logs" "$RUN/state" || exit 1
: > "$RUN/pids"

# A stale node or load client from an earlier experiment run holds our
# ports; both belong to this runner's shapes, so kill them before binding.
pkill -9 -f "config/experiment.jsonl" 2>/dev/null
pkill -9 -f "lease-load --server 127.0.0.1:4130" 2>/dev/null
sleep 0.2

port_of() {
    awk -v n="$1" 'index($0, "\"name\":\"" n "\"") {
        match($0, /"port":[0-9]+/)
        print substr($0, RSTART + 8, RLENGTH - 8)
    }' "$CONFIG"
}

id_of() {
    awk -v n="$1" 'index($0, "\"name\":\"" n "\"") {
        match($0, /"id":[0-9]+/)
        print substr($0, RSTART + 5, RLENGTH - 5)
    }' "$CONFIG"
}

start_node() {
    name=$1
    tcp=$(( $(port_of "$name") + 1000 ))
    flush_flags=""
    if [ "$VARIANT" != diskless ]; then
        flush_flags="--recovery-flush $VARIANT --recovery-scratch-dir $RUN/scratch/$name"
    fi
    RUST_LOG="${RUST_LOG:-info}" "$BIN" --name "$name" --config "$CONFIG" \
        --client "127.0.0.1:$tcp" --state "$RUN/state/$name.state" \
        --log "$RUN/logs/$name.log" $flush_flags \
        --heartbeat-ms 100 --election-ms 1000 --recovery-ms 1000 \
        >> "$RUN/logs/$name.stderr" 2>&1 &
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

log_files_of() {
    ls "$RUN/logs/$1."*".log"* 2>/dev/null
}

# The current leader's descriptor id, from the newest state=0 status note
# across all node logs.
current_leader() {
    grep -h "status state=0" "$RUN"/logs/*.log* 2>/dev/null | awk '
        {
            ts = 0; leader = 0
            for (i = 1; i <= NF; i++) {
                if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                if (index($i, "leader=") == 1) leader = substr($i, 8) + 0
            }
            if (ts > best) { best = ts; found = leader }
        }
        END { print found + 0 }'
}

last_state_of() {
    # One status note's (era, view) for node $1, the newest note at or
    # before $2 ("era view"); empty when the node has none yet.
    grep -h "status state=0" $(log_files_of "$1") 2>/dev/null | awk -v before="$2" '
        {
            ts = 0; era = 0; view = 0
            for (i = 1; i <= NF; i++) {
                if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                if (index($i, "era=") == 1) era = substr($i, 5) + 0
                if (index($i, "view=") == 1) view = substr($i, 6) + 0
            }
            if (ts <= before && ts > best) { best = ts; e = era; v = view }
        }
        END { if (best > 0) print e, v }'
}

grants_since() {
    grep -h "grant node=" "$RUN"/logs/*.log* 2>/dev/null | awk -v after="$1" '
        { for (i = 1; i <= NF; i++) if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
          if (ts > after) n++ }
        END { print n + 0 }'
}

# ------------------------------------------------------------ boot ----

for name in $NAMES; do
    start_node "$name"
done

deadline=$(( $(now_ms) + 60000 ))
while :; do
    all_voting=1
    for name in $NAMES; do
        grep -h "status state=0" $(log_files_of "$name") 2>/dev/null | grep -q "voting=1" || all_voting=0
    done
    leader=$(current_leader)
    [ "$all_voting" = 1 ] && [ "${leader:-0}" -gt 0 ] && break
    [ "$(now_ms)" -gt "$deadline" ] && fail "cluster did not reach steady state (3 voting genesis members with a leader)"
    sleep 0.3
done
echo "boot: 3-node genesis cluster steady, leader id $leader"

# ------------------------------------------------------- load client ----

LOAD_SERVERS=""
for name in $NAMES; do
    LOAD_SERVERS="$LOAD_SERVERS --server 127.0.0.1:$(( $(port_of "$name") + 1000 ))"
done
"$LOAD" $LOAD_SERVERS --clients 1 --getters 2 --rate "$RATE" \
    --lease-ms "$LEASE_MS" --renew-fraction "$RENEW_FRACTION" \
    --window-ms 2000 --id-base 800000 \
    --stats-out "$RUN/load-stats.jsonl" &
LOAD_PID=$!
echo "$LOAD_PID" >> "$RUN/pids"
echo "load: SET/BUMP/GET generator running (pid $LOAD_PID, rate $RATE)"
sleep 2

# ------------------------------------------------------- k cycles ----

iteration=0
victim_slot=0
while [ "$iteration" -lt "$K" ]; do
    iteration=$(( iteration + 1 ))
    leader=$(current_leader)
    [ "$leader" -eq 0 ] && fail "iteration $iteration: no leader"

    # Rotate the victim across the non-leader voting nodes.
    victim=""
    rotation=0
    for name in $NAMES; do
        desc=$(id_of "$name")
        [ "$desc" = "$leader" ] && continue
        if [ "$rotation" = $(( victim_slot % (3 - 1) )) ]; then
            victim=$name
        fi
        rotation=$(( rotation + 1 ))
    done
    victim_slot=$(( victim_slot + 1 ))
    [ -n "$victim" ] || fail "iteration $iteration: no non-leader voting node to kill"
    vpid=$(cat "$RUN/pid.$victim")
    vdesc=$(id_of "$victim")

    kill -9 "$vpid" 2>/dev/null
    kill_ts=$(now_ms)
    echo "iteration $iteration: SIGKILL $victim (id $vdesc, pid $vpid) at $kill_ts"

    # Crash-Stop: the restart is immediate; the reincarnation is the
    # restart's dirty boot (identity bump), classified at the adapter's
    # recovery boundary.
    start_node "$victim"

    bumped=""
    deadline=$(( $(now_ms) + 30000 ))
    while [ "$(now_ms)" -lt "$deadline" ]; do
        bumped=$(grep -h "boot name=$victim " $(log_files_of "$victim") 2>/dev/null | awk -v after="$kill_ts" '
            { ts = 0; own = 0; inc = 0
              for (i = 1; i <= NF; i++) {
                  if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                  if (index($i, "own=") == 1) own = substr($i, 5) + 0
                  if (index($i, "incarnation=") == 1) inc = substr($i, 13) + 0
              }
              if (ts > after && inc >= 1) { print own; exit } }')
        [ -n "$bumped" ] && break
        sleep 0.3
    done
        [ -n "$bumped" ] || fail "iteration $iteration: $victim did not reincarnate within 30 s"
        echo "iteration $iteration: $victim reincarnated as id $bumped"

    # Rejoin-serving: the reincarnated node's first status note with
    # state=0 (past the fenced boot) and voting=1 (the leader's forced
    # reconfiguration walk complete).
    serving_ts=""
    deadline=$(( $(now_ms) + 180000 ))
    while [ "$(now_ms)" -lt "$deadline" ]; do
        serving_ts=$(grep -h "status state=0" $(log_files_of "$victim") 2>/dev/null | awk -v after="$kill_ts" '
            {
                ts = 0; voting = -1; folded = 0; cfg = 1; era = 0
                for (i = 1; i <= NF; i++) {
                    if (index($i, "ts=") == 1) ts = substr($i, 4) + 0
                    if (index($i, "voting=") == 1) voting = substr($i, 8) + 0
                    if (index($i, "config_era=") == 1) cfg = substr($i, 12) + 0
                    if (index($i, "era=") == 1) era = substr($i, 5) + 0
                }
                if (ts > after && voting == 1 && cfg == era) { print ts; exit }
            }')
        [ -n "$serving_ts" ] && break
        sleep 0.3
    done
    [ -n "$serving_ts" ] || fail "iteration $iteration: $victim not voting and serving within 180 s"
    rejoin_ms=$(( serving_ts - kill_ts ))

    era_pair=$(last_state_of "$victim" "$kill_ts")
    era_kill=$(echo "$era_pair" | awk '{print $1 + 0}')
    view_kill=$(echo "$era_pair" | awk '{print $2 + 0}')
    era_pair=$(last_state_of "$victim" "$serving_ts")
    era_serving=$(echo "$era_pair" | awk '{print $1 + 0}')
    view_serving=$(echo "$era_pair" | awk '{print $2 + 0}')

    flush_us=""
    flush_bytes=""
    if [ "$VARIANT" != diskless ]; then
        line=$(grep -h "recovery-boundary flush executed" $(log_files_of "$victim") 2>/dev/null | tail -1)
        flush_us=$(printf '%s\n' "$line" | awk '{for (i = 1; i <= NF; i++) if (index($i, "latency_us=") == 1) print substr($i, 12)}')
        flush_bytes=$(printf '%s\n' "$line" | awk '{for (i = 1; i <= NF; i++) if (index($i, "bytes=") == 1) print substr($i, 7)}')
        [ -n "$flush_us" ] || fail "iteration $iteration: no recovery-boundary flush logged for $victim"
    fi

    record="{\"experiment\":\"$EXPERIMENT\",\"variant\":\"$VARIANT\",\"iteration\":$iteration,\"victim\":\"$victim\",\"victim_id\":$vdesc,\"bumped_id\":$bumped,\"kill_ts\":$kill_ts,\"serving_ts\":$serving_ts,\"rejoin_ms\":$rejoin_ms,\"era_kill\":$era_kill,\"view_kill\":$view_kill,\"era_serving\":$era_serving,\"view_serving\":$view_serving,\"eras_consumed\":$(( era_serving - era_kill )),\"views_consumed\":$(( view_serving - view_kill )),\"flush_variant\":\"$VARIANT\",\"flush_latency_us\":${flush_us:-null},\"flush_bytes\":${flush_bytes:-null}}"
    echo "$record" >> "$RUN/results.jsonl"
    echo "iteration $iteration: serving again at $serving_ts, rejoin ${rejoin_ms} ms, era $era_kill->$era_serving view $view_kill->$view_serving${flush_us:+ flush ${flush_us} us}"

    # Soak: the cluster keeps serving through the soak interval (cold
    # counts: the next iteration starts only after this).
    sleep $(( SOAK_MS / 1000 )).$(( SOAK_MS % 1000 ))
    [ "$(grants_since "$kill_ts")" -gt 0 ] || fail "iteration $iteration: no lock grants served after the kill"
done

# ------------------------------------------------------------ report ----

# Percentiles over the rejoin samples (rank = ceil(p * n), the load
# client's convention).
samples=$(awk -F'"rejoin_ms":' 'NF > 1 { split($2, a, ","); print a[1] }' "$RUN/results.jsonl")
count=$(printf '%s\n' "$samples" | grep -c .)
read -r P50 P90 P99 MAX MEAN <<EOF
$(printf '%s\n' "$samples" | sort -n | awk '
    { a[NR] = $1; sum += $1 }
    END {
        if (NR == 0) exit 1
        r50 = int(NR * 0.50 + 0.999); if (r50 > NR) r50 = NR
        r90 = int(NR * 0.90 + 0.999); if (r90 > NR) r90 = NR
        r99 = int(NR * 0.99 + 0.999); if (r99 > NR) r99 = NR
        print a[r50], a[r90], a[r99], a[NR], sum / NR
    }')
EOF
[ -n "$P50" ] || fail "no rejoin samples recorded"
echo "rejoin latency ($EXPERIMENT, variant $VARIANT, $count samples): p50=${P50} p90=${P90} p99=${P99} max=${MAX} mean=${MEAN} (ms)"

F50=""; F90=""; FMAX=""; FMEAN=""
if [ "$VARIANT" != diskless ]; then
    read -r F50 F90 FMAX FMEAN <<EOF
$(awk -F'"flush_latency_us":' 'NF > 1 { split($2, a, ","); print a[1] }' "$RUN/results.jsonl" | sort -n | awk '
    { a[NR] = $1; sum += $1 }
    END {
        if (NR == 0) exit 1
        r50 = int(NR * 0.50 + 0.999); if (r50 > NR) r50 = NR
        r90 = int(NR * 0.90 + 0.999); if (r90 > NR) r90 = NR
        print a[r50], a[r90], a[NR], sum / NR
    }')
EOF
    [ -n "$F50" ] || fail "no flush latencies recorded"
    echo "recovery-boundary flush (variant $VARIANT): p50=${F50} p90=${F90} max=${FMAX} mean=${FMEAN} (us)"
fi

{
    printf '{"experiment":"%s","variant":"%s","k":%d,"rejoin_ms":{"p50":%d,"p90":%d,"p99":%d,"max":%d,"mean":%.0f},' \
        "$EXPERIMENT" "$VARIANT" "$count" "$P50" "$P90" "$P99" "$MAX" "$MEAN"
    if [ -n "$F50" ]; then
        printf '"flush_us":{"p50":%d,"p90":%d,"max":%d,"mean":%.0f}}\n' "$F50" "$F90" "$FMAX" "$FMEAN"
    else
        printf '"flush_us":null}\n'
    fi
} > "$RUN/summary-$EXPERIMENT.json"
cp "$RUN/summary-$EXPERIMENT.json" "$RUN/summary.json"

# The load client's last window line carries the cumulative round-trip
# percentiles (proof-of-life discipline).
sleep 2.5
kill -9 "$LOAD_PID" 2>/dev/null
if [ -s "$RUN/load-stats.jsonl" ]; then
    tail -1 "$RUN/load-stats.jsonl" > "$RUN/load-final.json"
    echo "load client cumulative (last window line):"
    cat "$RUN/load-final.json"
fi

echo "results: $RUN/results.jsonl summary: $RUN/summary-$EXPERIMENT.json"
exit 0
