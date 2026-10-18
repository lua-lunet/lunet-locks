#!/usr/bin/env -S luajit
-- tools/check_stale.lua — the stale-binary rule as a command, for the lanes
-- a Makefile target boots and no Lua lane owns: the smoke, the failover
-- simulation, and the Docker image lane. The lanes with a Lua driver call
-- tools.lib.stale_binary themselves; this entry is that one call, so the
-- rule has exactly one implementation and this file adds no logic to it.
--
-- usage: tools/check_stale.lua cdylib | lease-node | lease-ladder |
--                        standby-stack | snapshot-tools | image IMAGE_NAME
-- Exit 0 = the artifact may be booted; nonzero = the refusal, on stderr,
-- naming the artifact's identity, the tree's identity and the rebuild.

local root = dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local stale = require("tools.lib.stale_binary")

local profile = nil
local which = arg[1] or ""
if which == "cdylib" then
    profile = stale.cdylib(root)
elseif which == "lease-node" then
    profile = stale.lease_node(root)
elseif which == "lease-ladder" then
    profile = stale.lease_ladder(root)
elseif which == "standby-stack" then
    profile = stale.standby_stack(root)
elseif which == "snapshot-tools" then
    profile = stale.snapshot_tools(root)
elseif which == "image" then
    local name = arg[2]
    if not name or name == "" then
        io.stderr:write("check_stale: usage: tools/check_stale.lua image IMAGE_NAME\n")
        os.exit(2)
    end
    profile = stale.image(name)
else
    io.stderr:write(
        "check_stale: usage: tools/check_stale.lua cdylib | lease-node |"
            .. " lease-ladder | standby-stack | snapshot-tools | image IMAGE_NAME\n"
    )
    os.exit(2)
end

local ok, err = pcall(stale.check, root, profile)
if not ok then
    io.stderr:write(tostring(err) .. "\n")
    os.exit(1)
end
os.exit(0)
