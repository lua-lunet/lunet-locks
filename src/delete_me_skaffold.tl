local lunet = require("lunet")
local socket = require("lunet.socket")
local lnt = require("lunet.lnt_shared")

assert(#arg >= 1, "usage: <port>")
local port = (tonumber(arg[1]) or 0) as integer
assert(port > 0, "usage: <port>")

-- 64 KiB shared dictionary; incr with init=0 creates the key if absent.
-- The key survives process restarts only within the same mmap region;
-- for this scaffold we start fresh each launch.
local dict = lnt.open("scaffold_counter", 65536)

lunet.spawn(function()
   local listener, err = socket.listen("tcp", "127.0.0.1", port)
   assert(listener, "listen: " .. (err or "unknown"))

   print("READY:" .. tostring(port))

   while true do
      local client, _ = socket.accept(listener)
      if client then
         local data = socket.read(client)
         local line: string
         if data then
            line = data:gsub("[\r\n]+$", "")
         else
            line = ""
         end

         if line == "INCR" then
            local val = dict:incr("hits", 1, 0, 0)
            socket.write(client, tostring(val or 0) .. "\n")
         elseif line == "GET" then
            local val = dict:get("hits")
            socket.write(client, tostring(val or 0) .. "\n")
         else
            socket.write(client, "UNKNOWN\n")
         end
         socket.close(client)
      end
   end
end)
