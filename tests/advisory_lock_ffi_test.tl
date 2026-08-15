-- The uVRR v0.2.0 rebuild removes the previous alpha C ABI that this
-- integration test exercised (`advisory_lock.new`, `node:status`,
-- `node:leader_for_view`, `node:request`, `node:close`). Those assertions
-- return with the rebuilt native adapter. No live FFI assertions remain here.

local tested = require("tested")

return tested
