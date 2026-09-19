#!/usr/bin/env -S luajit
-- tools/dangling.lua — find processes running from this repo (list-only
-- by default; --kill terminates them). The outer-commit discipline runs
-- this before every commit: dangling orphans from cancelled agent
-- attempts are terminated deliberately — clear orphans only, never the
-- operator's own long-running jobs (when in doubt, list and report
-- instead of killing).

dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local dangling = require("tools.lib.dangling")

os.exit(dangling.main(arg))
