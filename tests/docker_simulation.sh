#!/bin/sh
# Run the std-only dynamic-client simulation against three long-lived service
# containers. The cluster itself is never killed; only TCP clients are dynamic.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
sim=${SIM_BIN:-"$root/.tmp/lease-failover-sim"}
image=${DOCKER_IMAGE:-lunet-advisory-lock}
platform=${DOCKER_PLATFORM:-native}
duration=${SIM_DURATION:-30}
work=$(mktemp -d "$root/.tmp/docker-lease-failover.XXXXXX")
suffix=$(basename "$work" | tr -cd '[:alnum:]')
network="lunet-lock-$suffix"
containers=""
volumes=""
completed=false

run_docker() {
    perl -e 'alarm shift @ARGV; exec @ARGV' 60 docker "$@"
}

capture_logs() {
    for name in $containers; do
        run_docker logs "$name" >"$work/$name.log" 2>&1 || true
    done
}

cleanup() {
    capture_logs
    for name in $containers; do
        run_docker rm --force "$name" >/dev/null 2>&1 || true
    done
    run_docker network rm "$network" >/dev/null 2>&1 || true
    for volume in $volumes; do
        run_docker volume rm "$volume" >/dev/null 2>&1 || true
    done
    if "$completed"; then
        echo "docker simulation: passed; logs retained in $work"
    else
        echo "docker simulation: failed; logs retained in $work" >&2
    fi
}
trap cleanup EXIT INT TERM HUP

test -x "$sim" || {
    echo "docker simulation: missing simulator at $sim; run make simulation-test first" >&2
    exit 127
}
run_docker info >/dev/null
daemon_platform=$(docker version --format '{{.Server.Os}}/{{.Server.Arch}}')
if test "$platform" = native; then
    platform=$daemon_platform
fi
test "$daemon_platform" = "$platform" || {
    echo "docker simulation: daemon is $daemon_platform; cross-platform runs are not supported" >&2
    exit 1
}
image_platform=$(docker image inspect --format '{{.Os}}/{{.Architecture}}' "$image")
test "$image_platform" = "$platform" || {
    echo "docker simulation: image is $image_platform; expected native $platform" >&2
    exit 1
}
run_docker network create --subnet 172.30.77.0/24 "$network" >/dev/null

start_node() {
    name=$1
    address=$2
    host_port=$3
    volume="${network}-${name}-state"
    run_docker volume create "$volume" >/dev/null
    volumes="$volumes $volume"
    run_docker run --detach --name "${network}-${name}" \
        --platform "$platform" \
        --network "$network" --ip "$address" \
        --mount "type=volume,source=$volume,target=/var/lib/lunet-lock" \
        --publish "127.0.0.1:${host_port}:2910${name#n}" \
        "$image" \
        --node "$name" --client "0.0.0.0:2910${name#n}" \
        --state "/var/lib/lunet-lock/${name}.nonce" \
        --member n1=172.30.77.11:29111 \
        --member n2=172.30.77.12:29112 \
        --member n3=172.30.77.13:29113 >/dev/null
    containers="$containers ${network}-${name}"
}

start_node n1 172.30.77.11 31101
start_node n2 172.30.77.12 31102
start_node n3 172.30.77.13 31103

# Let the documented stagger elect n1 before the external clients begin.
sleep 4
for name in $containers; do
    running=$(run_docker inspect --format '{{.State.Running}}' "$name")
    test "$running" = true || {
        echo "docker simulation: $name exited during startup" >&2
        exit 1
    }
done
SIM_ROOT="$root" "$sim" --duration "$duration" --external-ports 31101,31102,31103 \
    | tee "$work/simulation.log"
completed=true
