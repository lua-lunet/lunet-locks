#!/usr/bin/env -S luajit
-- Entry point: `bin/up.sh` (or down/status) runs this.

local here = dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local ctl = require("lib.consolectl")

os.exit(ctl.main(arg[1] or "status", here))
