-- Lock Admin console lifecycle: render the nginx conf, mint loopback
-- credentials, spawn the bun mock and nginx, and babysit their pidfiles.
-- All paths live under console/tmp; the only logic-free shell is in bin/.

local record Paths
   root: string
   assets: string
   tmp: string
   conf: string
   template: string
   htpasswd: string
   credentials: string
   mockPid: string
   nginxPid: string
   mockLog: string
end

local record Ctl
   main: function(cmd: string, toolingDir: string): integer
end

local function sh(cmd: string): string
   local p = io.popen(cmd, "r")
   if not p then return "" end
   local out = p:read("*l") or ""
   p:close()
   return out
end

local function readFile(path: string): string
   local f = io.open(path, "rb")
   if not f then return "" end
   local data = f:read("*a") or ""
   f:close()
   return data
end

local function writeFile(path: string, data: string): boolean
   local f = io.open(path, "w")
   if not f then return false end
   f:write(data)
   f:close()
   return true
end

local function paths(root: string): Paths
   local tmp = root .. "/tmp"
   return {
      root = root,
      assets = root .. "/assets",
      tmp = tmp,
      conf = tmp .. "/nginx.conf",
      template = root .. "/nginx.conf.template",
      htpasswd = tmp .. "/htpasswd",
      credentials = tmp .. "/credentials",
      mockPid = tmp .. "/mock.pid",
      nginxPid = tmp .. "/nginx.pid",
      mockLog = tmp .. "/mock.log",
   }
end

local function pidOf(pidfile: string): string
   local f = io.open(pidfile, "r")
   if not f then return "" end
   local pid = f:read("*l") or ""
   f:close()
   return pid
end

local function alive(pidfile: string): boolean
   local pid = pidOf(pidfile)
   if pid == "" then return false end
   return os.execute("kill -0 " .. pid .. " 2>/dev/null") == true
end

local function randomToken(n: integer): string
   local chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
   local out: {string} = {}
   local f = io.open("/dev/urandom", "rb")
   if not f then return "" end
   local bytes = f:read(n) or ""
   f:close()
   for i = 1, #bytes do
      local b = string.byte(bytes, i) % #chars
      out[#out + 1] = chars:sub(b + 1, b + 1)
   end
   return table.concat(out)
end

local function render(template: string, vars: {string:string}): string
   local out = template
   for k, v in pairs(vars) do
      out = (out:gsub("@" .. k .. "@", (v:gsub("%%", "%%%%"))))
   end
   return out
end

-- Returns user, password, created. Credentials persist in tmp/credentials so
-- restarts keep the same loopback login.
local function ensureCredentials(p: Paths): string, string, boolean
   local existing = readFile(p.credentials)
   local user, pw = existing:match("^([^:]+):([^\n]+)")
   if user and pw then return user, pw, false end
   pw = randomToken(12)
   if pw == "" then
      io.stderr:write("ERROR: could not read /dev/urandom for credentials\n")
      os.exit(1)
   end
   -- Fed via stdin so the password never appears in the process table.
   local hash = sh('printf "%s" "' .. pw .. '" | openssl passwd -apr1 -stdin')
   if hash == "" then
      io.stderr:write("ERROR: openssl passwd failed\n")
      os.exit(1)
   end
   writeFile(p.htpasswd, "admin:" .. hash .. "\n")
   writeFile(p.credentials, "admin:" .. pw .. "\n")
   os.execute('chmod 600 "' .. p.credentials .. '" "' .. p.htpasswd .. '"')
   return "admin", pw, true
end

local function startMock(p: Paths, mockPort: string)
   if alive(p.mockPid) then return end
   local cmd = string.format(
      '(cd "%s/mock" && MOCK_PORT=%s exec bun admin.mjs) >> "%s" 2>&1 & echo $!',
      p.root, mockPort, p.mockLog)
   local pid = sh(cmd)
   if pid == "" then
      io.stderr:write("ERROR: failed to spawn bun mock (is bun on PATH?)\n")
      os.exit(1)
   end
   writeFile(p.mockPid, pid .. "\n")
end

local function startNginx(p: Paths)
   if alive(p.nginxPid) then return end
   if os.execute(string.format('nginx -t -q -c "%s"', p.conf)) ~= true then
      io.stderr:write("ERROR: nginx config test failed; see " .. p.tmp .. "/nginx-error.log\n")
      os.exit(1)
   end
   if os.execute(string.format('nginx -c "%s"', p.conf)) ~= true then
      io.stderr:write("ERROR: nginx failed to start; see " .. p.tmp .. "/nginx-error.log\n")
      os.exit(1)
   end
end

local function waitHealthy(port: string, user: string, pw: string): boolean
   for _ = 1, 40 do
      local code = sh(string.format(
         'curl -s -o /dev/null -w "%%{http_code}" --max-time 1 -u "%s:%s" "http://127.0.0.1:%s/api/v1/health"',
         user, pw, port))
      if code == "200" then return true end
      os.execute("sleep 0.25")
   end
   return false
end

local function up(root: string): integer
   local p = paths(root)
   local port = os.getenv("PORT") or "8480"
   local mockPort = os.getenv("MOCK_PORT") or "8481"
   os.execute('mkdir -p "' .. p.tmp .. '"')

   local conf = render(readFile(p.template), {
      TMP = p.tmp,
      ASSETS = p.assets,
      PORT = port,
      MOCK_PORT = mockPort,
   })
   if not writeFile(p.conf, conf) then
      io.stderr:write("ERROR: cannot write " .. p.conf .. "\n")
      return 1
   end

   local user, pw = ensureCredentials(p)
   startMock(p, mockPort)
   startNginx(p)

   if not waitHealthy(port, user, pw) then
      io.stderr:write("ERROR: console did not become healthy; run `make logs`\n")
      return 1
   end

   io.write("Lock Admin console is up\n")
   io.write("  url:      http://127.0.0.1:" .. port .. "/\n")
   io.write("  user:     " .. user .. "\n")
   io.write("  password: " .. pw .. "\n")
   io.write("  warning:  loopback demo only — plain HTTP + basic auth; TLS+mTLS required off-loopback\n")
   return 0
end

local function killPidfile(pidfile: string): boolean
   local pid = pidOf(pidfile)
   if pid == "" then return false end
   os.execute("kill " .. pid .. " 2>/dev/null")
   for _ = 1, 20 do
      if os.execute("kill -0 " .. pid .. " 2>/dev/null") ~= true then break end
      os.execute("sleep 0.1")
   end
   os.remove(pidfile)
   return true
end

local function down(root: string): integer
   local p = paths(root)
   local stopped = false
   if killPidfile(p.nginxPid) then
      io.write("nginx stopped\n")
      stopped = true
   end
   if killPidfile(p.mockPid) then
      io.write("mock stopped\n")
      stopped = true
   end
   if not stopped then io.write("nothing running\n") end
   return 0
end

local function status(root: string): integer
   local p = paths(root)
   local port = os.getenv("PORT") or "8480"
   local mockUp = alive(p.mockPid)
   local nginxUp = alive(p.nginxPid)
   io.write("mock:   " .. (mockUp and "running (pid " .. pidOf(p.mockPid) .. ")" or "stopped") .. "\n")
   io.write("nginx:  " .. (nginxUp and "running (pid " .. pidOf(p.nginxPid) .. ")" or "stopped") .. "\n")
   if mockUp and nginxUp then
      local creds = readFile(p.credentials):gsub("%s+$", "")
      io.write("url:    http://127.0.0.1:" .. port .. "/  (" .. creds .. ")\n")
      return 0
   end
   return 1
end

function Ctl.main(cmd: string, toolingDir: string): integer
   local root = toolingDir .. "/.."
   if cmd == "up" then return up(root) end
   if cmd == "down" then return down(root) end
   if cmd == "status" then return status(root) end
   io.stderr:write("usage: console.lua {up|down|status}\n")
   return 2
end

return Ctl
