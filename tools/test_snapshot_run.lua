#!/usr/bin/env -S luajit
-- tools/test_snapshot_run.lua — the snapshot tool's acceptance run:
-- six classes, tolerance, archive-equals-raw.

dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local snapshot_test = require("tools.lib.snapshot_test")

os.exit(snapshot_test.main((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/..", arg))
