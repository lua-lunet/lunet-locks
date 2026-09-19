#!/usr/bin/env -S luajit
dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/../../tools/bootstrap.lua")

local stream = require("tools.lib.polite2_stream")

os.exit(stream.main())
