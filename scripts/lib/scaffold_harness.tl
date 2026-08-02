-- Typed harness: boot scaffold counter, drive with nc, verify, shut down.
local proc = require("proc")
local harness = require("harness")

local function send_line(port: integer, line: string): string
   local cmd = "printf " .. proc.quote(line .. "\n") .. " | nc 127.0.0.1 " .. tostring(port) .. " 2>/dev/null"
   local p = io.popen(cmd)
   if p == nil then
      return ""
   end
   local out = p:read("*l")
   p:close()
   return out or ""
end

return function(root: string): integer
   local t = harness.new_tally()
   local log_path = root .. "/.tmp/scaffold-load-test.log"

   local port = harness.free_port()
   if port == 0 then
      print("FAIL - could not find a free port")
      return 1
   end

   local server = proc.spawn(
      root .. "/bin/lunet-run " .. proc.quote(root .. "/build/delete_me_skaffold.lua") .. " " .. tostring(port),
      log_path)

   local started = harness.wait_port(port, 5000)
   harness.check(t, started, "scaffold service listening on 127.0.0.1:" .. tostring(port))
   if not started then
      print(harness.read_file(log_path))
      proc.stop(server.pid, 2000)
      return harness.finish(t)
   end

   for i = 1, 20 do
      local resp = send_line(port, "INCR")
      harness.check(t, resp == tostring(i),
         "INCR #" .. tostring(i) .. " returned " .. tostring(i) .. " (got " .. resp .. ")")
   end

   local count = send_line(port, "GET")
   harness.check(t, count == "20", "GET returned 20 (got " .. count .. ")")

   local bogus = send_line(port, "BOGUS")
   harness.check(t, bogus == "UNKNOWN", "BOGUS returned UNKNOWN (got " .. bogus .. ")")

   harness.check(t, proc.stop(server.pid, 3000), "service stopped by pid")
   harness.check(t, not harness.port_busy(port), "port released after shutdown")

   return harness.finish(t)
end
