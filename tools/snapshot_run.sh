#!/bin/sh
# The snapshot rule: capture a run's on-disk state before anything touches
# it. Six artifact classes, per node, paths preserved, into a gzip tar the
# check tool (`skaffold_flight_tape --check-shutdown`) reads transparently:
#
#   1. superblock files (`*.superblock`)
#   2. state files — the single-file `<incarnation> <state>` markers
#      (any file with a `.superblock` or `.membership` sibling, plus
#      `*.state` files)
#   3. flight-recorder tapes (`flight-*.jsonl`, rolled files included)
#   4. AOF telemetry trees (directories holding `*.aof` / `ev-*.bin`
#      capture series)
#   5. regular logs (`*.log`, `*.nohup`, `*.out`, `*.err`)
#   6. the anchors file and membership sidecars (`anchors*`,
#      `*.membership`)
#
# usage: tools/snapshot_run.sh RUN_DIR [--out ARCHIVE.tar.gz]
#
# A crashed run may be missing any piece: missing classes and unreadable
# files are warnings, never fatal. Exit 0 = the archive exists; 2 = usage
# or input error; 1 = the archive could not be produced. The wipe gate
# refuses loudly on any nonzero exit.
set -u

fail() {
    echo "snapshot_run: $*" >&2
    exit 2
}

[ "$#" -ge 1 ] || fail "usage: snapshot_run.sh RUN_DIR [--out ARCHIVE.tar.gz]"
run=$1
shift
[ -d "$run" ] || fail "not a directory: $run (nothing to snapshot)"

out=""
while [ "$#" -gt 0 ]; do
    case $1 in
        --out)
            [ "$#" -ge 2 ] || fail "--out needs a value"
            out=$2
            shift 2
            ;;
        *) fail "unknown argument: $1" ;;
    esac
done

run=$(CDPATH='' cd -- "$run" && pwd)
parent=$(dirname "$run")
base=$(basename "$run")
if [ -z "$out" ]; then
    out="$parent/$base.snapshot-$(date -u +%Y%m%dT%H%M%SZ).tar.gz"
fi
case $out in
    /*) ;;
    *) out="$(pwd)/$out" ;;
esac

stage=$(mktemp -d "$parent/.snapshot-stage.XXXXXX") || {
    echo "snapshot_run: cannot create a staging dir next to $run" >&2
    exit 1
}
trap 'rm -rf "$stage"' EXIT INT TERM HUP

warn_log=$stage/.snapshot-warnings.txt
: >"$warn_log"
manifest=$stage/SNAPSHOT_MANIFEST.txt
: >"$manifest"
idx=$stage/.captured.idx
: >"$idx"

# One class capture: the file list relative to the run dir, each file
# copied into the staging tree at its original relative path. A copy
# failure is a warning, never fatal; the manifest records what made it.
copy_class() {
    class=$1
    shift
    # shellcheck disable=SC2086
    find "$run" \( "$@" \) -type f 2>>"$warn_log" | sort | while IFS= read -r path; do
        rel=${path#"$run"/}
        mkdir -p "$stage/$(dirname "$rel")" 2>>"$warn_log" ||
            echo "snapshot_run: cannot stage $rel" >&2
        cp -p "$path" "$stage/$rel" 2>>"$warn_log" || {
            echo "snapshot_run: could not copy $rel" >&2
            continue
        }
        printf '%s\t%s\n' "$class" "$rel" >>"$manifest"
        printf '%s\n' "$rel" >>"$idx"
    done
}

copy_class superblock -name '*.superblock'
copy_class state -name '*.state'
copy_class flight -name 'flight-*.jsonl'
copy_class logs -name '*.log' -o -name '*.nohup' -o -name '*.out' -o -name '*.err'
copy_class anchors -name 'anchors*'
copy_class sidecar -name '*.membership'

# The single-file markers spelled neither `*.state` nor caught by the
# patterns above (e.g. `n1.nonce`): every base file that owns a
# `.superblock` or `.membership` sibling is a state file.
find "$run" \( -name '*.superblock' -o -name '*.membership' \) -type f 2>>"$warn_log" | sort | {
    while IFS= read -r path; do
        echo "${path%.superblock}"
        echo "${path%.membership}"
    done
} | sort -u | while IFS= read -r base_path; do
    [ -f "$base_path" ] || continue
    rel=${base_path#"$run"/}
    [ "$rel" != "$base_path" ] || continue
    grep -Fqx "$rel" "$idx" && continue
    mkdir -p "$stage/$(dirname "$rel")" 2>>"$warn_log" ||
        echo "snapshot_run: cannot stage $rel" >&2
    cp -p "$base_path" "$stage/$rel" 2>>"$warn_log" || {
        echo "snapshot_run: could not copy $rel" >&2
        continue
    }
    printf 'state\t%s\n' "$rel" >>"$manifest"
    printf '%s\n' "$rel" >>"$idx"
done

# The AOF telemetry trees: every directory holding a capture series file
# rides along whole, so the metafiles and the rolled blocks stay together.
find "$run" \( -name '*.aof' -o -name 'ev-*.bin' -o -name 'ev-*.meta' \) -type f 2>>"$warn_log" | sort | {
    while IFS= read -r path; do
        dirname "$path"
    done
} | sort -u | while IFS= read -r dir; do
    rel=${dir#"$run"/}
    [ "$rel" != "$dir" ] || continue
    [ -n "$rel" ] || continue
    mkdir -p "$stage/$(dirname "$rel")" 2>>"$warn_log" ||
        echo "snapshot_run: cannot stage $rel" >&2
    cp -pR "$dir" "$stage/$(dirname "$rel")/" 2>>"$warn_log" || {
        echo "snapshot_run: could not copy the AOF tree $rel" >&2
        continue
    }
    find "$dir" -type f 2>>"$warn_log" | sort | while IFS= read -r aof_file; do
        rel=${aof_file#"$run"/}
        grep -Fqx "$rel" "$idx" && continue
        printf 'aof\t%s\n' "$rel" >>"$manifest"
        printf '%s\n' "$rel" >>"$idx"
    done
done

{
    echo "# snapshot_run $run"
    echo "# $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    cat "$manifest"
    if [ -s "$warn_log" ]; then
        echo "# warnings"
        sed 's/^/# warning: /' "$warn_log"
    fi
} >"$manifest.tmp" && mv "$manifest.tmp" "$manifest"

rm -f "$warn_log" "$idx"
tar -czf "$out" -C "$stage" . || {
    echo "snapshot_run: tar failed; no archive at $out" >&2
    exit 1
}

captured=$(grep -c -v '^#' "$manifest" 2>/dev/null || true)
echo "snapshot_run: $captured artifact files -> $out"
if grep -q '^# warning' "$manifest"; then
    echo "snapshot_run: completed with warnings (see SNAPSHOT_MANIFEST.txt)" >&2
fi
exit 0
