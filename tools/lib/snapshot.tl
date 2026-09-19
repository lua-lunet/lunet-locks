-- The snapshot rule: capture a run's on-disk state before anything
-- touches it. Six artifact classes, per node, paths preserved, into a
-- gzip tar the check tool (`skaffold_flight_tape --check-shutdown`)
-- reads transparently:
--
--   1. superblock files (`*.superblock`)
--   2. state files — the single-file `<incarnation> <state>` markers
--      (any file with a `.superblock` or `.membership` sibling, plus
--      `*.state` files)
--   3. flight-recorder tapes (`flight-*.jsonl`, rolled files included)
--   4. AOF telemetry trees (directories holding `*.aof` / `ev-*.bin`
--      capture series)
--   5. regular logs (`*.log`, `*.nohup`, `*.out`, `*.err`)
--   6. the anchors file and membership sidecars (`anchors*`,
--      `*.membership`)
--
-- A crashed run may be missing any piece: missing classes and unreadable
-- files are warnings, never fatal. The wipe gate
-- (`snapshot_then_wipe`) removes the run dir only after a successful
-- snapshot; a failed snapshot refuses the wipe loudly.

local fs = require("tools.lib.fs")
local shell = require("tools.lib.shell")

local record M
    snapshot: function(string, string): boolean, string, string
    snapshot_then_wipe: function(string): boolean, string
    class_of: function(string): string
end

-- The class of one file by its run-relative path, or "" when the file
-- belongs to none of the six classes.
function M.class_of(rel: string): string
    local base = fs.basename(rel)
    if base:match("%.superblock$") then
        return "superblock"
    end
    if base:match("%.state$") then
        return "state"
    end
    if base:match("^flight%-.*%.jsonl$") then
        return "flight"
    end
    if
        base:match("%.log$")
        or base:match("%.nohup$")
        or base:match("%.out$")
        or base:match("%.err$")
    then
        return "logs"
    end
    if base:match("^anchors") then
        return "anchors"
    end
    if base:match("%.membership$") then
        return "sidecar"
    end
    return ""
end

-- Captures the run dir into a gzip tar. Returns (ok, out_path,
-- message): ok=false with a usage-class message means "nothing to
-- snapshot", ok=false otherwise means the archive could not be
-- produced. Missing pieces are warnings, never fatal.
function M.snapshot(run_dir: string, out: string): boolean, string, string
    if not fs.is_dir(run_dir) then
        return false, "", "not a directory: " .. run_dir .. " (nothing to snapshot)"
    end
    local run = fs.resolve(run_dir)
    if out == "" then
        local stamp = os.date("!%Y%m%dT%H%M%SZ")
        out = fs.join(
            fs.dirname(run), fs.basename(run) .. ".snapshot-" .. stamp .. ".tar.gz"
        )
    else
        out = fs.resolve(out)
    end

    local stage = fs.unique_sibling(fs.dirname(run), ".snapshot-stage")
    if not fs.mkdir_p(stage) then
        return false, "", "cannot create a staging dir next to " .. run
    end

    local warnings: {string} = {}
    local manifest: {string} = {}
    local captured: {string: boolean} = {}

    local function warn(msg: string)
        warnings[#warnings + 1] = msg
    end

    -- One class capture: the file copied into the staging tree at its
    -- original relative path. A copy failure is a warning, never fatal.
    local function capture(rel: string, class: string): boolean
        local src = fs.join(run, rel)
        local dst = fs.join(stage, rel)
        local dst_dir = fs.dirname(dst)
        if not fs.is_dir(dst_dir) and not fs.mkdir_p(dst_dir) then
            warn("cannot stage " .. rel)
            return false
        end
        if not fs.copy_file(src, dst) then
            warn("could not copy " .. rel)
            return false
        end
        manifest[#manifest + 1] = class .. "\t" .. rel
        captured[rel] = true
        return true
    end

    -- The AOF telemetry trees: every directory holding a capture series
    -- file rides along whole, so the metafiles and the rolled blocks stay
    -- together.
    local aof_dirs: {string: boolean} = {}
    for _, rel in ipairs(fs.walk(run)) do
        local base = fs.basename(rel)
        local parent_rel = fs.dirname(rel)
        if
            parent_rel ~= "."
            and (
                base:match("%.aof$")
                or base:match("^ev%-.*%.bin$")
                or base:match("^ev%-.*%.meta$")
            )
        then
            aof_dirs[parent_rel] = true
        end
    end

    for _, rel in ipairs(fs.walk(run)) do
        local class = M.class_of(rel)
        if class ~= "" then
            capture(rel, class)
        end
    end

    -- The single-file markers spelled neither `*.state` nor caught by the
    -- classes above (e.g. `n1.nonce`): every base file that owns a
    -- `.superblock` or `.membership` sibling is a state file.
    for _, rel in ipairs(fs.walk(run)) do
        local suffix = rel:match("%.superblock$") or rel:match("%.membership$")
        if suffix then
            local base_rel = rel:sub(1, #rel - #suffix)
            if fs.exists(fs.join(run, base_rel)) and not captured[base_rel] then
                capture(base_rel, "state")
            end
        end
    end

    for aof_dir_rel in pairs(aof_dirs) do
        local src_dir = fs.join(run, aof_dir_rel)
        local dst_parent = fs.join(stage, fs.dirname(aof_dir_rel))
        if not fs.is_dir(dst_parent) then
            fs.mkdir_p(dst_parent)
        end
        if not fs.copy_tree(src_dir, fs.join(stage, aof_dir_rel)) then
            warn("could not copy the AOF tree " .. aof_dir_rel)
        end
        for _, file_rel in ipairs(fs.walk(src_dir)) do
            local rel = aof_dir_rel .. "/" .. file_rel
            if not captured[rel] then
                manifest[#manifest + 1] = "aof\t" .. rel
                captured[rel] = true
            end
        end
    end

    local lines: {string} = {
        "# snapshot_run " .. run,
        "# " .. os.date("!%Y-%m-%dT%H:%M:%SZ"),
    }
    for _, line in ipairs(manifest) do
        lines[#lines + 1] = line
    end
    for _, w in ipairs(warnings) do
        lines[#lines + 1] = "# warning: " .. w
    end
    fs.write_all(
        fs.join(stage, "SNAPSHOT_MANIFEST.txt"), table.concat(lines, "\n") .. "\n"
    )

    local tar_cmd = "tar -czf "
        .. shell.quote(out)
        .. " -C "
        .. shell.quote(stage)
        .. " ."
    if not shell.run(tar_cmd) then
        fs.remove_tree(stage)
        return false, out, "tar failed; no archive at " .. out
    end
    fs.remove_tree(stage)

    local n = 0
    for _, line in ipairs(manifest) do
        if not line:match("^#") then
            n = n + 1
        end
    end
    print("snapshot_run: " .. n .. " artifact files -> " .. out)
    if #warnings > 0 then
        io.stderr:write(
            "snapshot_run: completed with warnings (see SNAPSHOT_MANIFEST.txt)\n"
        )
    end
    return true, out, ""
end

-- The wipe gate: the run dir is removed only after a successful
-- snapshot of it; a failed snapshot refuses the wipe loudly and the
-- state stays in place.
function M.snapshot_then_wipe(run_dir: string): boolean, string
    if not fs.is_dir(run_dir) then
        return true, ""
    end
    local ok, _out, message = M.snapshot(run_dir, "")
    if not ok then
        io.stderr:write(
            "REFUSED to wipe "
                .. run_dir
                .. ": the snapshot failed ("
                .. message
                .. "); the run dir stays\n"
        )
        return false, message
    end
    if not fs.remove_tree(run_dir) then
        io.stderr:write("REFUSED to wipe " .. run_dir .. ": the wipe itself failed\n")
        return false, "wipe failed"
    end
    print("snapshot_run: snapshotted and wiped " .. run_dir)
    return true, ""
end

return M
