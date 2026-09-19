#!/usr/bin/env -S luajit
-- tools/softball2_run.lua — the softball-2 crash/partition run driver.
-- usage: tools/softball2_run.lua RUN_DIR
-- Exit 0 = the ladder ran to teardown; anything else = the gate that
-- stopped it (the message is the finding). On any failure the driver's
-- cleanup reaps every process the runner spawned — nothing dangles; it
-- never touches a process it did not fork itself.

-- The repo root: walk up from this script's directory (bootstrap's own
-- walk runs from the main script's arg[0], which for a dofile'd prelude
-- is still this driver's).
local entry_dir = arg[0]:match("^(.*)/[^/]*$") or "."
local pipe = io.popen("cd " .. entry_dir .. " 2>/dev/null && pwd")
local abs_entry = pipe and pipe:read("*l") or nil
if pipe then
    pipe:close()
end
if not abs_entry or abs_entry == "" then
    abs_entry = entry_dir
end
local function exists(p)
    local f = io.open(p, "rb")
    if f then
        f:close()
        return true
    end
    return false
end
local root = abs_entry
while root ~= "/" and not exists(root .. "/Makefile") do
    root = root:match("^(.*)/[^/]*$") or "/"
end
dofile(root .. "/tools/bootstrap.lua")

local proc = require("tools.lib.proc")
local softball2 = require("tools.lib.softball2")

local run_dir = arg[1]
if not run_dir or run_dir == "" then
    io.stderr:write("softball2_run: usage: softball2_run.lua RUN_DIR\n")
    os.exit(2)
end

local ok, err = pcall(softball2.run, root, run_dir)
if not ok then
    io.stderr:write(tostring(err) .. "\n")
    proc.stop_all()
    os.exit(1)
end
os.exit(0)
