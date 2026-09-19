#!/usr/bin/env -S luajit
-- tools/smoke.lua — `make smoke`: the three-process runtime smoke with
-- restart and live-reconfiguration stages, against the pinned
-- project-local Lunet v0.10.0 runtime.

dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local smoke = require("tools.lib.smoke")

os.exit(smoke.main((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/..", arg))
