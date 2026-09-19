#!/usr/bin/env -S luajit
-- The stability-check entry point: the full run in ./run.lua.
dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/../../tools/bootstrap.lua")

local rig = require("tools.lib.rig")

os.exit(rig.stability((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/../.."))
