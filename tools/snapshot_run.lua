#!/usr/bin/env -S luajit
-- tools/snapshot_run.lua — the run-state snapshot: the six artifact
-- classes, per node, paths preserved, into a dated gzip tar with a
-- SNAPSHOT_MANIFEST.txt. A crashed run may be missing any piece;
-- missing classes and unreadable files are warnings, never fatal.
--
-- usage: tools/snapshot_run.lua RUN_DIR [--out ARCHIVE.tar.gz]
-- Exit 0 = the archive exists; 2 = usage or input error; 1 = the
-- archive could not be produced. The wipe gate refuses loudly on any
-- nonzero exit.

dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local snapshot = require("tools.lib.snapshot")

local usage = function()
   io.stderr:write("snapshot_run: usage: snapshot_run.lua RUN_DIR [--out ARCHIVE.tar.gz]\n")
   os.exit(2)
end

local run = arg[1]
if not run then
   usage()
end

local out = ""
local i = 2
while arg[i] do
   if arg[i] == "--out" then
      if not arg[i + 1] then
         usage()
      end
      out = arg[i + 1]
      i = i + 2
   else
      io.stderr:write("snapshot_run: unknown argument: " .. arg[i] .. "\n")
      usage()
   end
end

local ok, _out, message = snapshot.snapshot(run, out)
if not ok then
   if message:find("not a directory") then
      io.stderr:write("snapshot_run: " .. message .. "\n")
      os.exit(2)
   end
   io.stderr:write("snapshot_run: " .. message .. "\n")
   os.exit(1)
end
os.exit(0)
