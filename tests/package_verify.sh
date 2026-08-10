#!/bin/sh
# Verify a packaged release archive end to end: extract it into a scratch
# directory and run the full three-replica advisory-lock smoke flow against
# the packaged build/ tree, with the native adapter resolved from the
# archive's lib/ directory. This is the guard against shipping a broken
# archive: it exercises exactly the layout downstream deployments consume,
# with no dependency on src/, ext/, or the Teal toolchain.
#
# Usage: tests/package_verify.sh <archive.tar.gz>
#
# Requires the project-local Lunet runtime (LUNET_RUN, defaulting to the
# pinned v0.8.0 tree) and perl for the NDJSON client. POSIX only.
set -eu

root=$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)
run=${LUNET_RUN:-"$root/.lunet/v0.8.0/lunet-run"}

archive=${1:-""}
test -n "$archive" || {
    echo "package verify: usage: tests/package_verify.sh <archive.tar.gz>" >&2
    exit 64
}
test -f "$archive" || {
    echo "package verify: archive not found: $archive" >&2
    exit 66
}
test -x "$run" || {
    echo "package verify: missing project-local Lunet runtime at $run; run make lunet-runtime" >&2
    exit 127
}

work=$(mktemp -d "$root/.tmp/package-verify.XXXXXX")
pids=""
completed=false

stop_process() {
    pid=$1
    if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        sleep 1
        if kill -0 "$pid" 2>/dev/null; then
            kill -KILL "$pid" 2>/dev/null || true
        fi
    fi
    wait "$pid" 2>/dev/null || true
}

cleanup() {
    for pid in $pids; do
        stop_process "$pid"
    done
    if "$completed"; then
        rm -rf "$work"
    else
        echo "package verify: retained failure logs in $work" >&2
        # The runner (local or CI) is torn down with the job; the CI log is
        # the only place these survive, so print them rather than pointing at
        # a path nobody can read afterward.
        for log in "$work"/*.out "$work"/*.err; do
            test -f "$log" || continue
            echo "--- $log ---" >&2
            cat "$log" >&2
        done
    fi
}
trap cleanup EXIT INT TERM HUP

tar -C "$work" -xzf "$archive"
test -f "$work/build/server.lua" || {
    echo "package verify: archive has no build/server.lua" >&2
    exit 65
}

# The packaged tree has no ext/ directory; point the loader at the archive's
# lib/ so `require("advisory_lock")` inside the extracted build/ resolves the
# shipped cdylib instead of the repo-relative target/release fallback.
case $(uname -s) in
    Darwin) libname=lunet_advisory_lock.dylib ;;
    *) libname=lunet_advisory_lock.so ;;
esac
export LUNET_ADVISORY_LOCK_LIB="$work/lib/lib$libname"
test -f "$LUNET_ADVISORY_LOCK_LIB" || {
    echo "package verify: archive has no lib/lib$libname" >&2
    exit 65
}

cd "$work"

start() {
    name=$1
    client_port=$2
    peer_port=$3
    "$run" build/server.lua \
        --node "$name" --client "127.0.0.1:$client_port" \
        --state "$work/$name.nonce" \
        --member n1=127.0.0.1:27101 \
        --member n2=127.0.0.1:27102 \
        --member n3=127.0.0.1:27103 \
        >"$work/$name.out" 2>"$work/$name.err" &
    pid=$!
    pids="$pids $pid"
    printf '%s\n' "$pid" >"$work/$name.pid"
    # Keep the arguments explicit: client_port and peer_port document the
    # topology at every call site and prevent accidental port reuse.
    test "$peer_port" -ge 1
}

# Send every line on one connection, preserving the server's sequential client
# path. The expected marker list has one fixed JSON fragment per response.
request_lines() {
    port=$1
    input=$2
    expected=$3
    output=$work/client.out
    printf '%s' "$input" | perl -MIO::Select -MIO::Socket::INET -e '
        my $port = shift;
        sub connect_socket {
            my $socket = IO::Socket::INET->new(
                PeerAddr => "127.0.0.1", PeerPort => $port, Proto => "tcp",
            ) or die "connect: $!\n";
            $socket->autoflush(1);
            return $socket;
        }
        my $socket = connect_socket();
        while (my $line = <STDIN>) {
            # NDJSON frames are newline-delimited. Single-line shell arguments
            # have no terminator, so add one before sending the frame.
            $line .= "\n" if $line !~ /\n\z/;
            print {$socket} $line or die "write: $!\n";
            IO::Select->new($socket)->can_read(5)
                or die "missing reply within 5 seconds\n";
            my $reply = <$socket>;
            defined $reply or die "missing reply\n";
            syswrite STDOUT, $reply or die "stdout: $!\n";
        }
    ' "$port" >"$output"
    oldifs=$IFS
    IFS='|'
    set -- $expected
    IFS=$oldifs
    for marker in "$@"; do
        grep -F -- "$marker" "$output" >/dev/null || {
            echo "package verify: expected response marker not found: $marker" >&2
            cat "$output" >&2
            exit 1
        }
    done
}

# Identical scenario to tests/lunet_smoke.sh: three replicas, client traffic
# through n2 (forwarding), lease expiry takeover, then n3 restart recovery.
start n1 28101 27101
start n2 28102 27102
start n3 28103 27103

# n1's zero election stagger makes it leader under the documented defaults;
# sending through n2 verifies application forwarding rather than direct leader I/O.
sleep 3.5
future=$(perl -MTime::HiRes=time -e 'printf "%.0f", time() * 1000 + 5000')
holder1=11111111-1111-1111-1111-111111111111
holder2=22222222-2222-2222-2222-222222222222
holder3=33333333-3333-3333-3333-333333333333

request_lines 28102 "{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-000000000001\",\"client_id\":1,\"request_num\":1,\"lock_id\":9001,\"lease\":{\"lease_id\":1,\"holder\":\"$holder1\",\"expiry\":$future}}
{\"op\":\"get\",\"message_id\":\"00000000-0000-0000-0000-000000000002\",\"client_id\":1,\"request_num\":2,\"lock_id\":9001}
{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-000000000003\",\"client_id\":2,\"request_num\":1,\"lock_id\":9001,\"lease\":{\"lease_id\":2,\"holder\":\"$holder2\",\"expiry\":$future}}
{\"op\":\"release\",\"message_id\":\"00000000-0000-0000-0000-000000000004\",\"client_id\":1,\"request_num\":3,\"lock_id\":9001,\"holder\":\"$holder1\",\"lease_id\":1}
{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-000000000005\",\"client_id\":2,\"request_num\":2,\"lock_id\":9001,\"lease\":{\"lease_id\":2,\"holder\":\"$holder2\",\"expiry\":$future}}
{\"op\":\"release\",\"message_id\":\"00000000-0000-0000-0000-000000000006\",\"client_id\":2,\"request_num\":3,\"lock_id\":9001,\"holder\":\"$holder2\",\"lease_id\":2}
" '"granted":true|"op":"get"|"granted":false|"released":true|"granted":true|"released":true'

soon=$(perl -MTime::HiRes=time -e 'printf "%.0f", time() * 1000 + 3000')
request_lines 28102 "{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-000000000007\",\"client_id\":3,\"request_num\":1,\"lock_id\":9001,\"lease\":{\"lease_id\":3,\"holder\":\"$holder1\",\"expiry\":$soon}}" '"granted":true'
sleep 3.1
takeover_expiry=$(perl -MTime::HiRes=time -e 'printf "%.0f", time() * 1000 + 5000')
request_lines 28102 "{\"op\":\"set\",\"message_id\":\"00000000-0000-0000-0000-000000000008\",\"client_id\":4,\"request_num\":1,\"lock_id\":9001,\"lease\":{\"lease_id\":4,\"holder\":\"$holder3\",\"expiry\":$takeover_expiry}}" '"granted":true'

# A restarted replica recovers against the still-live n1/n2 quorum. Kill only
# n3's process, retain its nonce file, then prove the client path remains live.
n3pid=$(cat "$work/n3.pid")
stop_process "$n3pid"
pids=$(printf '%s\n' "$pids" | sed "s/ $n3pid//")
start n3 28103 27103
sleep 3
request_lines 28102 "{\"op\":\"get\",\"message_id\":\"00000000-0000-0000-0000-000000000009\",\"client_id\":4,\"request_num\":2,\"lock_id\":9001}" '"op":"get"|33333333-3333-3333-3333-333333333333'

completed=true
echo "package verify: passed ($archive)"
