-- Typed test-harness helpers: ports, files, HTTP-over-nc, pass/fail tally.
local proc = require("proc")

local record Tally
   checks: integer
   failures: integer
end

local M = {
   Tally = Tally,
}

function M.mkdir_p(path: string): boolean
   return proc.shell_ok("mkdir -p " .. proc.quote(path))
end

function M.write_file(path: string, data: string): boolean
   local f = io.open(path, "w")
   if f == nil then
      return false
   end
   f:write(data)
   f:close()
   return true
end

function M.read_file(path: string): string
   local f = io.open(path, "r")
   if f == nil then
      return ""
   end
   local data = f:read("*a")
   f:close()
   return data or ""
end

-- true when something is already listening on the port.
function M.port_busy(port: integer): boolean
   return proc.shell_ok("nc -z 127.0.0.1 " .. tostring(port) .. " 2>/dev/null")
end

-- Pick a free-looking ephemeral port. Racy by nature; callers should treat a
-- bind failure as retryable rather than assuming exclusivity.
function M.free_port(): integer
   math.randomseed(math.floor(os.time()) + math.floor(os.clock() * 100000))
   for _ = 1, 50 do
      local port = 20000 + math.floor(math.random() * 40000)
      if not M.port_busy(port) then
         return port
      end
   end
   return 0
end

function M.wait_port(port: integer, timeout_ms: integer): boolean
   return proc.wait_until(function(): boolean
      return M.port_busy(port)
   end, timeout_ms, 50)
end

-- Minimal HTTP/1.0 GET via nc. Returns the raw response.
function M.http_get(port: integer, path: string): string
   local cmd = "printf 'GET " .. path .. " HTTP/1.0\\r\\nHost: 127.0.0.1\\r\\n\\r\\n'"
      .. " | nc 127.0.0.1 " .. tostring(port) .. " 2>/dev/null"
   local p = io.popen(cmd)
   if p == nil then
      return ""
   end
   local out = p:read("*a")
   p:close()
   return out or ""
end

-- Last non-empty line of an HTTP response, i.e. a single-line body.
function M.body_line(response: string): string
   local last = ""
   for line in response:gmatch("[^\r\n]+") do
      last = line
   end
   return last
end

function M.new_tally(): Tally
   return { checks = 0, failures = 0 }
end

function M.check(t: Tally, cond: boolean, msg: string)
   t.checks = t.checks + 1
   if cond then
      print("ok   - " .. msg)
   else
      t.failures = t.failures + 1
      print("FAIL - " .. msg)
   end
end

function M.finish(t: Tally): integer
   if t.failures == 0 then
      print("PASS: " .. tostring(t.checks) .. " checks")
      return 0
   end
   print("FAIL: " .. tostring(t.failures) .. "/" .. tostring(t.checks) .. " checks failed")
   return 1
end

return M
