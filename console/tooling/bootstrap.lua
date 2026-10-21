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

package.path = here .. "/?.lua;" .. here .. "/?.tl;"
   .. here .. "/../../?.tl;" .. package.path

return here
