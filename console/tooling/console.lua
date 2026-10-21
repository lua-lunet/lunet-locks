#!/usr/bin/env -S luajit

local here = dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local ctl = require("lib.consolectl")

os.exit(ctl.main(arg[1] or "status", here))
