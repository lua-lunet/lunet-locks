#!/bin/sh
# The snapshot tool's acceptance (the snapshot rule):
#   (a) a snapshot captures all six artifact classes from a synthetic
#       run dir (superblock, state, flight, aof, logs, anchors/sidecar);
#   (b) the tool tolerates missing pieces: an empty run dir still
#       snapshots, and a missing run dir is a loud refusal;
#   (c) reading the check from the snapshot archive equals reading from
#       the raw run directory.
# Everything is built under the repo's .tmp/ and removed on exit.
set -eu

root=$(CDPATH='' cd -- "$(dirname -- "$0")/.." && pwd)
work=$(mktemp -d "$root/.tmp/snapshot-test.XXXXXX")
failures=0

# shellcheck disable=SC2329  # invoked through the trap above every exit
cleanup() {
    rm -rf "$work"
}
trap cleanup EXIT INT TERM HUP

pass() {
    echo "snapshot-test: pass: $*"
}

fail() {
    echo "snapshot-test: FAIL: $*" >&2
    failures=$((failures + 1))
}

assert_contains() {
    # assert_contains LISTFILE ENTRY
    if grep -Fqx -- "$2" "$1"; then
        pass "$2 in the archive"
    else
        fail "$2 missing from the archive listing"
    fi
}

snapshot=$root/tools/snapshot_run.sh
check=${SKAFFOLD_FLIGHT_TAPE:-"$root/examples/lease-sequencer/target/release/skaffold_flight_tape"}
nuke=${NUKE:-"$root/ext/advisory_lock/target/release/lunet_locks_nuke"}
test -x "$check" || {
    cargo build --release --quiet --manifest-path "$root/examples/lease-sequencer/Cargo.toml" \
        --bin skaffold_flight_tape
}
test -x "$nuke" || {
    cargo build --release --quiet --manifest-path "$root/ext/advisory_lock/Cargo.toml" --bin lunet_locks_nuke
}

# ---------------------------------------------------------------------------
# (a) The six artifact classes ride the archive.
# ---------------------------------------------------------------------------
run_dir=$work/run
mkdir -p "$run_dir/state" "$run_dir/flight" "$run_dir/aof/dc1"
printf '3 flushed\n' >"$run_dir/state/n1.state"
# The superblock copies are REAL marker files — written by the vendored
# Zig store through the lunet_locks_nuke tool's fresh-format reset — so
# the check's
# classification path runs against production bytes.
"$nuke" "$run_dir/state/n1.state" --set-state flushed --set-incarnation 3 \
    --dangerously-skip-review >/dev/null 2>&1
printf '{"era":1,"slot":0}\n' >"$run_dir/state/n1.state.membership"
printf 'recording\n' >"$run_dir/flight/flight-1.jsonl"
printf 'rolled\n' >"$run_dir/flight/flight-1-1789725519188.jsonl"
printf 'aof\n' >"$run_dir/aof/dc1/1789725519.aof"
printf 'meta\n' >"$run_dir/aof/dc1/1789725519.meta"
printf ' INFO stop: drained and flushed; the next boot continues under the same incarnation node=1\n' \
    >"$run_dir/n1.2026-09-18.log"
printf 'nohup\n' >"$run_dir/n1.nohup"
printf '## teardown: TERM n1 ts=2026-09-18T10:00:06Z\n' >"$run_dir/anchors.md"

archive=$work/snapshot.tar.gz
sh "$snapshot" "$run_dir" --out "$archive" || fail "the snapshot tool exited nonzero"
test -s "$archive" || fail "the archive was not produced"

listing=$work/listing.txt
tar -tzf "$archive" >"$listing"

assert_contains "$listing" "./state/n1.state.superblock"
assert_contains "$listing" "./state/n1.state"
assert_contains "$listing" "./flight/flight-1.jsonl"
assert_contains "$listing" "./flight/flight-1-1789725519188.jsonl"
assert_contains "$listing" "./aof/dc1/1789725519.aof"
assert_contains "$listing" "./aof/dc1/1789725519.meta"
assert_contains "$listing" "./n1.2026-09-18.log"
assert_contains "$listing" "./n1.nohup"
assert_contains "$listing" "./anchors.md"
assert_contains "$listing" "./state/n1.state.membership"
assert_contains "$listing" "./SNAPSHOT_MANIFEST.txt"

manifest=$work/manifest.txt
tar -xOzf "$archive" ./SNAPSHOT_MANIFEST.txt >"$manifest"
for class in superblock state flight logs anchors sidecar aof; do
    if grep -q "^$class	" "$manifest"; then
        pass "the manifest names class $class"
    else
        fail "the manifest is missing class $class"
    fi
done
if grep -q '^# snapshot_run' "$manifest"; then
    pass "the manifest records the snapshot header"
else
    fail "the manifest lost its snapshot header"
fi

# ---------------------------------------------------------------------------
# The (a) fixture, with its real flushed marker copies, is a clean run:
# the check reads it consistent, raw.
# ---------------------------------------------------------------------------
clean_report=$work/clean-report.txt
check_status_clean=0
"$check" --check-shutdown "$run_dir" >"$clean_report" 2>/dev/null || check_status_clean=$?
if [ "$check_status_clean" -eq 0 ] \
    && grep -q "node(s) checked: CONSISTENT" "$clean_report" \
    && grep -q "OK \[n1\] consistent" "$clean_report"; then
    pass "the clean run checks consistent"
else
    fail "the clean run's check is not green (exit $check_status_clean):"
    cat "$clean_report" >&2
fi

# ---------------------------------------------------------------------------
# (b) Tolerance: a crashed run's missing pieces never stop the capture.
# ---------------------------------------------------------------------------
empty_dir=$work/empty
mkdir -p "$empty_dir"
empty_archive=$work/empty.tar.gz
if sh "$snapshot" "$empty_dir" --out "$empty_archive" && test -s "$empty_archive"; then
    pass "an empty run dir snapshots to a valid archive"
else
    fail "the empty run dir's snapshot failed"
fi

missing_archive=$work/missing.tar.gz
if sh "$snapshot" "$work/does-not-exist" --out "$missing_archive" 2>/dev/null; then
    fail "a missing run dir was silently accepted"
else
    pass "a missing run dir is refused loudly"
fi

# ---------------------------------------------------------------------------
# (c) The check reads the archive exactly like the raw run dir: here a
# planted inconsistency (a log claiming a clean flush against a
# superblock whose final state is NOT flushed — the marker copies sit at
# unflushed at the same incarnation) is found identically both ways.
# ---------------------------------------------------------------------------
"$nuke" "$run_dir/state/n1.state" --set-state running --set-incarnation 3 \
    --dangerously-skip-review >/dev/null 2>&1
archive=$work/planted-snapshot.tar.gz
sh "$snapshot" "$run_dir" --out "$archive" >/dev/null 2>&1 || fail "the planted run's snapshot failed"
test -s "$archive" || fail "the planted run's archive was not produced"
raw_report=$work/raw-report.txt
archive_report=$work/archive-report.txt
check_status_raw=0
check_status_archive=0
"$check" --check-shutdown "$run_dir" >"$raw_report" 2>/dev/null || check_status_raw=$?
"$check" --check-shutdown "$archive" >"$archive_report" 2>/dev/null || check_status_archive=$?

if [ "$check_status_raw" -ne 0 ] && [ "$check_status_raw" = "$check_status_archive" ]; then
    pass "the check's exit code agrees raw vs archive ($check_status_raw)"
else
    fail "the check's exit codes disagree: raw=$check_status_raw archive=$check_status_archive"
fi
if cmp -s "$raw_report" "$archive_report"; then
    pass "the check's report is identical raw vs archive"
else
    fail "the check's reports differ:"
    diff "$raw_report" "$archive_report" >&2 || true
fi
if grep -q "log-claims-flush-marker-not-flushed" "$raw_report" \
    && grep -q "log-claims-flush-marker-not-flushed" "$archive_report"; then
    pass "the planted inconsistency is detected raw and in the archive"
else
    fail "the planted inconsistency was not detected in both reads"
fi

# ---------------------------------------------------------------------------
# The verdict line and a clean run stay clean.
# ---------------------------------------------------------------------------
if grep -q "node(s) checked: INCONSISTENT" "$raw_report"; then
    pass "the report verdicts INCONSISTENT on the planted run"
else
    fail "the report misses the INCONSISTENT verdict"
fi

if [ "$failures" -eq 0 ]; then
    echo "snapshot-test: passed"
    exit 0
fi
echo "snapshot-test: $failures failure(s)" >&2
exit 1
