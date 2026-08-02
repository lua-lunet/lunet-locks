-- Typed process/PID management for ad-hoc harness scripts.
--
-- Loaded via tl.loader() from luajit-shebang executables in scripts/.
-- See CONVENTIONS.md ("Scripts") for why these are .tl and not shell.

local record Proc
   pid: integer
   log: string
end

local M = {
   Proc = Proc,
}

function M.quote(s: string): string
   return "'" .. (s:gsub("'", "'\\''")) .. "'"
end

-- os.execute returns a boolean on Lua 5.3+, but an exit code on LuaJIT/5.1.
-- Teal type-checks against the 5.3 stdlib, so normalise here rather than
-- inspecting the raw return at every call site.
function M.shell_ok(cmd: string): boolean
   local res: any = os.execute(cmd)
   if type(res) == "boolean" then
      return res as boolean
   end
   return (res as number) == 0
end

function M.sleep_ms(ms: integer)
   M.shell_ok("sleep " .. tostring(ms / 1000))
end

function M.wait_until(pred: function(): boolean, timeout_ms: integer, step_ms: integer): boolean
   local waited = 0
   while waited < timeout_ms do
      if pred() then
         return true
      end
      M.sleep_ms(step_ms)
      waited = waited + step_ms
   end
   return pred()
end

-- Background a command and capture its pid. stdout/stderr go to log_path.
function M.spawn(cmd: string, log_path: string): Proc
   local p = io.popen(cmd .. " > " .. M.quote(log_path) .. " 2>&1 & echo $!")
   local line = p and p:read("*l") or nil
   if p then
      p:close()
   end
   return { pid = math.floor(tonumber(line) or 0), log = log_path }
end

function M.alive(pid: integer): boolean
   if pid <= 0 then
      return false
   end
   return M.shell_ok("kill -0 " .. tostring(pid) .. " 2>/dev/null")
end

function M.read_pid_file(path: string): integer
   local f = io.open(path, "r")
   if f == nil then
      return 0
   end
   local line = f:read("*l")
   f:close()
   return math.floor(tonumber(line or "") or 0)
end

-- TERM, then escalate to KILL. A SIGSTOPped process never reaps a TERM,
-- so the escalation is load-bearing, not defensive padding.
function M.stop(pid: integer, timeout_ms: integer): boolean
   if pid <= 0 then
      return true
   end
   M.shell_ok("kill -TERM " .. tostring(pid) .. " 2>/dev/null")
   local gone = M.wait_until(function(): boolean
      return not M.alive(pid)
   end, timeout_ms, 50)
   if gone then
      return true
   end
   M.shell_ok("kill -KILL " .. tostring(pid) .. " 2>/dev/null")
   return M.wait_until(function(): boolean
      return not M.alive(pid)
   end, 1000, 50)
end

return M
