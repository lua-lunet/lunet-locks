#!/usr/bin/env -S luajit
dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/../../tools/bootstrap.lua")

local rig = require("tools.lib.rig")

os.exit(rig.standby((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/../.."))
