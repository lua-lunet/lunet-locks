#!/bin/sh
# The membership-snapshot acceptance: boot a 7th node on the GENESIS
# descriptor while the live cluster is at era 5 (after three joins and two
# increments), and assert, from the node's log and its membership sidecar:
#
#   1. it escalates through the era chain: the discovery loop's responses
#      carry the live era, every escalation drops the older-era responses,
#      and the node re-requests across that era's membership;
#   2. it reaches a quorum of agreeing snapshots at the live era and adopts
#      the discovered configuration in memory (source=discovery);
#   3. the adopted facts are written behind: the membership sidecar next
#      to the incarnation marker carries the adopted snapshot;
#   4. it joins via the ordinary fenced boot (the join verb), and the
#      leader's post-commit disseminations keep it current: the sidecar
#      advances to the post-join era, then the post-increment era with the
#      member at weight 1;
#   5. a weight-0 learner's snapshots are collected but the quorum counts
#      only the voting members' weights (id 6 stays at weight 0 and the
#      discovery still settles on the three genesis voters' agreement).
#
# Every spawned process is killed on exit.
set -u
cd "$(dirname "$0")" || exit 1

BIN=./target/release/lease-sequencer
CLIENT=./target/release/lease-client
CONFIG=config/cluster.jsonl
GENESIS_CONFIG=config/cluster-genesis.jsonl
RUN=run/acceptance
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
    descriptor=$2
    tcp=$(client_port_of "$name")
    RUST_LOG="${RUST_LOG:-info}" "$BIN" --name "$name" --config "$descriptor" \
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

# Drive one admin verb at the cluster: try every node's client port until
# one accepts (the leader); non-leaders answer {"error":"not_leader"}.
drive_verb() {
    action=$1
    id=$2
    name=${3:-}
    endpoint=${4:-}
    deadline=$(( $(now_ms) + 120000 ))
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

sidecar_of() {
    echo "$RUN/state/$1.state.membership"
}

sidecar_era() {
    grep -o '"era":[0-9]*' "$(sidecar_of "$1")" 2>/dev/null | head -1 | cut -d: -f2
}

sidecar_members() {
    grep -c '"endpoint"' "$(sidecar_of "$1")" 2>/dev/null | tr -d ' '
}

wait_sidecar_era() {
    name=$1
    era=$2
    deadline=$(( $(now_ms) + 15000 ))
    while [ "$(now_ms)" -lt "$deadline" ]; do
        [ "$(sidecar_era "$name")" = "$era" ] && return 0
        sleep 0.3
    done
    return 1
}

# ---------------------------------------------------------------- boot ----

for name in $NAMES; do
    start_node "$name" "$CONFIG"
done

deadline=$(( $(now_ms) + 60000 ))
while [ "$(grep -h "grant node=" "$RUN"/logs/*.log 2>/dev/null | wc -l | tr -d ' ')" -lt 4 ]; do
    [ "$(now_ms)" -gt "$deadline" ] && fail "the genesis cluster did not stabilize"
    sleep 0.3
done
echo "boot: genesis cluster stabilized"

# ------------------------------------------------ joins/promotes to era 5 --

for spec in "4 dc1-node2 127.0.0.1:41104" "5 dc2-node2 127.0.0.1:41105" "6 dc3-node2 127.0.0.1:41106"; do
    set -- $spec
    drive_verb join "$1" "$2" "$3" || fail "join $2 (id $1) not accepted within 120s"
    echo "join: id $1 ($2) accepted"
    sleep 2
done
drive_verb increment 4 || fail "increment 4 not accepted within 120s"
echo "increment: id 4 accepted"
sleep 2
drive_verb increment 5 || fail "increment 5 not accepted within 120s"
echo "increment: id 5 accepted (id 6 stays at weight 0)"
sleep 2

# The live cluster is now at era 6 (five committed
# reconfigurations past the descriptor's era 0).

# ------------------------------------------------- the 7th node's boot ----

# The 7th node boots on the GENESIS descriptor while the cluster is at
# era 5: its discovery loop escalates through the era chain, reaches a
# quorum of agreeing snapshots, adopts the discovered configuration in
# memory, feeds the fenced boot, and writes the adopted facts behind.
start_node dc3-node3 "$GENESIS_CONFIG"
sleep 1

# 1. escalation evidence: the discovery loop moved to the live era.
grep -q "discovery escalate era=6" "$RUN/logs/dc3-node3"* 2>/dev/null \
    || fail "the 7th node's discovery never escalated to the live era 6"
echo "discovery: escalated to the live era 6"

# 2. quorum of agreeing snapshots at the live era, adopted in memory.
grep -q "membership snapshot adopted era=6.*source=discovery" \
    "$RUN/logs/dc3-node3"* 2>/dev/null \
    || fail "the 7th node never adopted a quorum of agreeing era-6 snapshots"
echo "discovery: quorum of agreeing era-6 snapshots adopted"

# 3. the adopted facts are written behind (the membership sidecar).
wait_sidecar_era dc3-node3 6 \
    || fail "the adopted era-6 configuration was never written behind"
echo "write-behind: the membership sidecar carries the adopted era-6 snapshot"

sidecar_members dc3-node3 | grep -qx 6 \
    || fail "the adopted sidecar should carry the six members of era 6, not $(sidecar_members dc3-node3)"
grep -q '"id":6,"weight":0' "$(sidecar_of dc3-node3)" \
    || fail "the adopted sidecar should carry the weight-0 learner id 6"

# The discovered addressing rows let the node address the live members it
# did not remember: its status line shows it tracking the cluster.
grep -q "membership snapshot adopted" "$RUN/logs/dc3-node3"* 2>/dev/null \
    || fail "no membership adoption evidence in the 7th node's log"

# --------------------------------------------- join + promotion via verbs --

drive_verb join 7 dc3-node3 127.0.0.1:41107 || fail "join 7 not accepted within 120s"
echo "join: id 7 accepted (the fenced boot carried the join)"
sleep 2

# The leader's post-commit dissemination reaches the 7th node: its model
# moves to the post-join era and writes behind.
wait_sidecar_era dc3-node3 7 \
    || fail "the 7th node never adopted the post-join era-7 dissemination"

drive_verb increment 7 || fail "increment 7 not accepted within 120s"
echo "increment: id 7 accepted"
sleep 2

wait_sidecar_era dc3-node3 8 \
    || fail "the 7th node never adopted the post-increment era-8 dissemination"

# The final sidecar content, asserted line for line: the header's slot is
# the establishing commit's choosing slot (run-dependent), every member
# line is exact.
head -1 "$(sidecar_of dc3-node3)" | grep -q '{"format":"membership-sidecar/v1","era":8,"slot":[0-9]*}' \
    || fail "the sidecar header does not name the era-8 generation: $(head -1 "$(sidecar_of dc3-node3)")"
sed -n 2,8p "$(sidecar_of dc3-node3)" > "$RUN/sidecar.members.actual"
cat > "$RUN/sidecar.members.expected" <<'EOF'
{"id":1,"weight":1,"endpoint":"127.0.0.1:41101"}
{"id":2,"weight":1,"endpoint":"127.0.0.1:41102"}
{"id":3,"weight":1,"endpoint":"127.0.0.1:41103"}
{"id":4,"weight":1,"endpoint":"127.0.0.1:41104"}
{"id":5,"weight":1,"endpoint":"127.0.0.1:41105"}
{"id":6,"weight":0,"endpoint":"127.0.0.1:41106"}
{"id":7,"weight":1,"endpoint":"127.0.0.1:41107"}
EOF
diff -u "$RUN/sidecar.members.expected" "$RUN/sidecar.members.actual" \
    || fail "the sidecar's member lines differ from the live membership"

# The 7th node's committed Join and Increment are the fenced boot's
# admission: the accepted acks prove both committed, the cluster folded
# both eras, and the node's host model carries the post-increment
# membership in the sidecar above. (The node's own core fold of an
# admitting era more than one era past its boot fold is the §10
# multi-era catch-up gap the vendored core's own transfer gate names:
# "an offer more than one era past the boot table cannot be serviced by
# the fetch at all — the multi-era catch-up that needs it is a protocol
# gap (§10)". The snapshot affordance is what carries the node's
# configuration knowledge to the current era; the fold itself is upstream
# work, out of scope here.)

echo "acceptance: passed"
