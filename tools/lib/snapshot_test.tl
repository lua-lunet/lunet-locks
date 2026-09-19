-- The snapshot tool's acceptance (the snapshot rule):
--   (a) a snapshot captures all six artifact classes from a synthetic
--       run dir (superblock, state, flight, aof, logs, anchors/sidecar);
--   (b) the tool tolerates missing pieces: an empty run dir still
--       snapshots, and a missing run dir is a loud refusal;
--   (c) reading the check from the snapshot archive equals reading from
--       the raw run directory.
-- Everything is built under the repo's .tmp/ and removed on exit.

local fs = require("tools.lib.fs")
local shell = require("tools.lib.shell")
local snapshot = require("tools.lib.snapshot")

local record M
    main: function(string, {string}): integer
end

local root: string
local work: string
local failures = 0

local function cleanup()
    fs.remove_tree(work)
end

local function pass(message: string)
    print("snapshot-test: pass: " .. message)
end

local function fail(message: string)
    io.stderr:write("snapshot-test: FAIL: " .. message .. "\n")
    failures = failures + 1
end

local function assert_contains(listfile: string, entry: string)
    local data = fs.read_all(listfile)
    for line in (data .. "\n"):gmatch("(.-)\n") do
        if line == entry then
            pass(entry .. " in the archive")
            return
        end
    end
    fail(entry .. " missing from the archive listing")
end

function M.main(argv_root: string, argv: {string}): integer
    root = argv_root
    local _argv = argv
    work = fs.unique_sibling(fs.join(root, ".tmp"), "snapshot-test")
    fs.mkdir_p(work)

    local check = os.getenv("SKAFFOLD_FLIGHT_TAPE")
        or fs.join(root, "examples/lease-sequencer/target/release/skaffold_flight_tape")
    local nuke = os.getenv("NUKE")
        or fs.join(root, "ext/advisory_lock/target/release/lunet_locks_nuke")
    if not fs.exists(check) then
        if not shell.run(
            "cargo build --release --quiet --manifest-path "
                .. shell.quote(fs.join(root, "examples/lease-sequencer/Cargo.toml"))
                .. " --bin skaffold_flight_tape"
        ) then
            fail("could not build skaffold_flight_tape")
        end
    end
    if not fs.exists(nuke) then
        if not shell.run(
            "cargo build --release --quiet --manifest-path "
                .. shell.quote(fs.join(root, "ext/advisory_lock/Cargo.toml"))
                .. " --bin lunet_locks_nuke"
        ) then
            fail("could not build lunet_locks_nuke")
        end
    end

    -- (a) The six artifact classes ride the archive.
    local run_dir = fs.join(work, "run")
    fs.mkdir_p(fs.join(run_dir, "state"))
    fs.mkdir_p(fs.join(run_dir, "flight"))
    fs.mkdir_p(fs.join(run_dir, "aof/dc1"))
    fs.write_all(fs.join(run_dir, "state/n1.state"), "3 flushed\n")
    -- The superblock copies are REAL marker files — written by the
    -- vendored Zig store through the lunet_locks_nuke tool's
    -- fresh-format reset — so the check's classification path runs
    -- against production bytes.
    shell.run(
        shell.quote(nuke)
            .. " "
            .. shell.quote(fs.join(run_dir, "state/n1.state"))
            .. " --set-state flushed --set-incarnation 3 --dangerously-skip-review >/dev/null 2>&1"
    )
    fs.write_all(fs.join(run_dir, "state/n1.state.membership"), '{"era":1,"slot":0}\n')
    fs.write_all(fs.join(run_dir, "flight/flight-1.jsonl"), "recording\n")
    fs.write_all(fs.join(run_dir, "flight/flight-1-1789725519188.jsonl"), "rolled\n")
    fs.write_all(fs.join(run_dir, "aof/dc1/1789725519.aof"), "aof\n")
    fs.write_all(fs.join(run_dir, "aof/dc1/1789725519.meta"), "meta\n")
    fs.write_all(
        fs.join(run_dir, "n1.2026-09-18.log"),
        " INFO stop: drained and flushed; the next boot continues under the same incarnation node=1\n"
    )
    fs.write_all(fs.join(run_dir, "n1.nohup"), "nohup\n")
    fs.write_all(
        fs.join(run_dir, "anchors.md"), "## teardown: TERM n1 ts=2026-09-18T10:00:06Z\n"
    )

    local archive = fs.join(work, "snapshot.tar.gz")
    local ok, out_path, message = snapshot.snapshot(run_dir, archive)
    if not ok then
        fail("the snapshot tool failed: " .. message)
    end
    local _out_path = out_path
    if not fs.exists(archive) then
        fail("the archive was not produced")
    end

    local listing = fs.join(work, "listing.txt")
    fs.write_all(listing, shell.capture("tar -tzf " .. shell.quote(archive)))

    assert_contains(listing, "./state/n1.state.superblock")
    assert_contains(listing, "./state/n1.state")
    assert_contains(listing, "./flight/flight-1.jsonl")
    assert_contains(listing, "./flight/flight-1-1789725519188.jsonl")
    assert_contains(listing, "./aof/dc1/1789725519.aof")
    assert_contains(listing, "./aof/dc1/1789725519.meta")
    assert_contains(listing, "./n1.2026-09-18.log")
    assert_contains(listing, "./n1.nohup")
    assert_contains(listing, "./anchors.md")
    assert_contains(listing, "./state/n1.state.membership")
    assert_contains(listing, "./SNAPSHOT_MANIFEST.txt")

    local manifest_path = fs.join(work, "manifest.txt")
    fs.write_all(
        manifest_path,
        shell.capture(
            "tar -xOzf " .. shell.quote(archive) .. " ./SNAPSHOT_MANIFEST.txt"
        )
    )
    local manifest = fs.read_all(manifest_path)
    local in_class: {string: boolean} = {}
    for line in (manifest .. "\n"):gmatch("(.-)\n") do
        local class = line:match("^([a-z]+)\t")
        if class then
            in_class[class] = true
        end
    end
    for _, class in ipairs(
        {"superblock", "state", "flight", "logs", "anchors", "sidecar", "aof"}
    ) do
        if in_class[class] then
            pass("the manifest names class " .. class)
        else
            fail("the manifest is missing class " .. class)
        end
    end
    if manifest:find("^# snapshot_run") then
        pass("the manifest records the snapshot header")
    else
        fail("the manifest lost its snapshot header")
    end

    -- The (a) fixture, with its real flushed marker copies, is a clean
    -- run: the check reads it consistent, raw.
    local clean_report = fs.join(work, "clean-report.txt")
    fs.write_all(
        clean_report,
        shell.capture(
            shell.quote(check) .. " --check-shutdown " .. shell.quote(run_dir)
        )
    )
    local check_status_clean = shell.status(
        shell.quote(check)
            .. " --check-shutdown "
            .. shell.quote(run_dir)
            .. " > "
            .. shell.quote(clean_report)
            .. " 2>/dev/null"
    )
    local clean = fs.read_all(clean_report)
    if
        check_status_clean == 0
        and clean:find("node(s) checked: CONSISTENT", 1, true)
        and clean:find("OK [n1] consistent", 1, true)
    then
        pass("the clean run checks consistent")
    else
        fail("the clean run's check is not green (exit " .. check_status_clean .. "):")
        io.stderr:write(clean .. "\n")
    end

    -- (b) Tolerance: a crashed run's missing pieces never stop the
    -- capture.
    local empty_dir = fs.join(work, "empty")
    fs.mkdir_p(empty_dir)
    local empty_archive = fs.join(work, "empty.tar.gz")
    local ok_empty, _p, _m = snapshot.snapshot(empty_dir, empty_archive)
    if ok_empty and fs.exists(empty_archive) then
        pass("an empty run dir snapshots to a valid archive")
    else
        fail("the empty run dir's snapshot failed")
    end

    local ok_missing, _p2, message2 = snapshot.snapshot(
        fs.join(work, "does-not-exist"), fs.join(work, "missing.tar.gz")
    )
    if ok_empty and not ok_missing and message2 ~= "" then
        pass("a missing run dir is refused loudly")
    else
        fail("a missing run dir was silently accepted")
    end

    -- (c) The check reads the archive exactly like the raw run dir: here
    -- a planted inconsistency (a log claiming a clean flush against a
    -- superblock whose final state is NOT flushed — the marker copies
    -- sit at unflushed at the same incarnation) is found identically
    -- both ways.
    shell.run(
        shell.quote(nuke)
            .. " "
            .. shell.quote(fs.join(run_dir, "state/n1.state"))
            .. " --set-state running --set-incarnation 3 --dangerously-skip-review >/dev/null 2>&1"
    )
    local planted_archive = fs.join(work, "planted-snapshot.tar.gz")
    local ok_planted, _p3, _m3 = snapshot.snapshot(run_dir, planted_archive)
    if not ok_planted then
        fail("the planted run's snapshot failed")
    end
    if not fs.exists(planted_archive) then
        fail("the planted run's archive was not produced")
    end
    local raw_report = fs.join(work, "raw-report.txt")
    local archive_report = fs.join(work, "archive-report.txt")
    local check_status_raw = shell.status(
        shell.quote(check)
            .. " --check-shutdown "
            .. shell.quote(run_dir)
            .. " > "
            .. shell.quote(raw_report)
            .. " 2>/dev/null"
    )
    local check_status_archive = shell.status(
        shell.quote(check)
            .. " --check-shutdown "
            .. shell.quote(planted_archive)
            .. " > "
            .. shell.quote(archive_report)
            .. " 2>/dev/null"
    )

    if check_status_raw ~= 0 and check_status_raw == check_status_archive then
        pass("the check's exit code agrees raw vs archive (" .. check_status_raw .. ")")
    else
        fail(
            "the check's exit codes disagree: raw="
                .. check_status_raw
                .. " archive="
                .. check_status_archive
        )
    end
    if fs.read_all(raw_report) == fs.read_all(archive_report) then
        pass("the check's report is identical raw vs archive")
    else
        fail("the check's reports differ:")
        io.stderr:write(
            shell.capture(
                "diff "
                    .. shell.quote(raw_report)
                    .. " "
                    .. shell.quote(archive_report)
                    .. " || true"
            )
        )
    end
    if
        fs.read_all(raw_report):find("log-claims-flush-marker-not-flushed", 1, true)
        and fs.read_all(archive_report):find(
            "log-claims-flush-marker-not-flushed", 1, true
        )
    then
        pass("the planted inconsistency is detected raw and in the archive")
    else
        fail("the planted inconsistency was not detected in both reads")
    end

    -- The verdict line and a clean run stay clean.
    if fs.read_all(raw_report):find("node(s) checked: INCONSISTENT", 1, true) then
        pass("the report verdicts INCONSISTENT on the planted run")
    else
        fail("the report misses the INCONSISTENT verdict")
    end

    local failed = failures > 0
    cleanup()
    if failed then
        io.stderr:write("snapshot-test: " .. failures .. " failure(s)\n")
        return 1
    end
    print("snapshot-test: passed")
    return 0
end

return M
