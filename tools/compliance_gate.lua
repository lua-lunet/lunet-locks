#!/usr/bin/env -S luajit
-- tools/compliance_gate.lua — the upstream uVRR compliance corpus, run
-- over the transport against the Lua compliance host. See
-- tools/lib/compliance_gate.tl for what it does and
-- docs/src/compliance-suite.md for what the run proves.

dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local compliance_gate = require("tools.lib.compliance_gate")

os.exit(compliance_gate.main((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/..", arg))
