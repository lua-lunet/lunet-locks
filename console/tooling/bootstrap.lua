-- Resolve the repo-local rocks tree (../.rocks, LuaJIT 5.1 ABI) without a
-- pre-sourced LUA_PATH, activate the Teal loader, and make sibling modules
-- requirable from any cwd. Returns the directory containing this file.

local here = arg[0]:match("^(.*)/[^/]*$") or "."
local rocks = here .. "/../../.rocks"

if not pcall(require, "tl") then
   package.path = rocks .. "/share/lua/5.1/?.lua;"
      .. rocks .. "/share/lua/5.1/?/init.lua;"
      .. package.path
   package.cpath = rocks .. "/lib/lua/5.1/?.so;" .. package.cpath
end

local ok, tl = pcall(require, "tl")
if not ok then
   io.stderr:write("ERROR: Teal (tl) not found in ../.rocks. Run: make init\n")
   os.exit(1)
end
tl.loader()

package.path = here .. "/?.lua;" .. here .. "/?.tl;" .. package.path

return here
