#!/usr/bin/env -S luajit
dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/../../tools/bootstrap.lua")

local polite2 = require("tools.lib.polite2")

os.exit(polite2.maintenance(
    (arg[0]:match("^(.*)/[^/]*$") or ".") .. "/../..",
    "/Users/Shared/lua-lunet/lunet-locks/.tmp/telemetry/local-polite2-2026-09-19"
))
