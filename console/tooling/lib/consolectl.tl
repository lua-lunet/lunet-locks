-- Lock Admin console lifecycle: render the OpenResty conf, mint loopback
-- credentials, spawn the 3-node lock cluster (pinned Lunet runtime,
-- telemetry on) and OpenResty, and babysit their pidfiles.
-- All paths live under console/tmp; the only logic-free shell is in bin/.

local record Node
   name: string
   peer: string
   client: string
end

local record Paths
   root: string
   repo: string
   assets: string
   tmp: string
   conf: string
   template: string
   htpasswd: string
   credentials: string
   nginxPid: string
   clusterJson: string
   telemetryBase: string
   serverLua: string
   lunetRun: string
end

local record Ctl
   main: function(cmd: string, toolingDir: string): integer
end

-- Smoke topology (tests/lunet_smoke.sh): peers 27101-27103, clients
-- 28101-28103, members n1/n2/n3.
local NODES: {Node} = {
   { name = "n1", peer = "127.0.0.1:27101", client = "127.0.0.1:28101" },
   { name = "n2", peer = "127.0.0.1:27102", client = "127.0.0.1:28102" },
   { name = "n3", peer = "127.0.0.1:27103", client = "127.0.0.1:28103" },
}

-- Records are buffered until rotation/close (src/telemetry_log.tl flushes on
-- rotate). 64 KiB keeps telemetry seconds-fresh at the workload's op rate
-- while producing ~100x fewer segment files than the original 512 (item47:
-- an ordinary session grew 4000+ segments and every console scan re-read
-- them all). This one constant feeds both the node command line
-- (--telemetry-min-bytes, ~line 191) and the telemetry.min_bytes echo in
-- cluster.json (~line 215), keeping the two consistent by construction.
local TELEMETRY_MIN_BYTES = 65536

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
   local repo = root .. "/.."
   return {
      root = root,
      repo = repo,
      assets = root .. "/assets",
      tmp = tmp,
      conf = tmp .. "/nginx.conf",
      template = root .. "/nginx.conf.template",
      htpasswd = tmp .. "/htpasswd",
      credentials = tmp .. "/credentials",
      nginxPid = tmp .. "/nginx.pid",
      clusterJson = tmp .. "/cluster.json",
      telemetryBase = tmp .. "/telemetry",
      serverLua = repo .. "/build/server.lua",
      lunetRun = repo .. "/.lunet/v0.8.0/lunet-run",
   }
end

local function nodePidfile(p: Paths, name: string): string
   return p.tmp .. "/" .. name .. ".pid"
end

local function nodeLogfile(p: Paths, name: string): string
   return p.tmp .. "/" .. name .. ".log"
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

local function checkPreconditions(p: Paths): boolean
   local ok = true
   local f = io.open(p.serverLua, "r")
   if f then
      f:close()
   else
      io.stderr:write("ERROR: " .. p.serverLua .. " missing; run `make -C .. build`\n")
      ok = false
   end
   if os.execute('test -x "' .. p.lunetRun .. '"') ~= true then
      io.stderr:write("ERROR: pinned runtime " .. p.lunetRun
         .. " missing; run `make -C .. lunet-runtime`\n")
      ok = false
   end
   if sh("command -v openresty") == "" then
      io.stderr:write("ERROR: openresty not on PATH\n")
      ok = false
   end
   return ok
end

local function startNode(p: Paths, node: Node)
   local pidfile = nodePidfile(p, node.name)
   if alive(pidfile) then return end
   -- Stale nonce state from a previous cluster leaves the client path
   -- brain-dead (accepts connections, never replies); always start fresh.
   os.remove(p.tmp .. "/" .. node.name .. ".nonce")
   local telemetryDir = p.telemetryBase .. "/" .. node.name
   os.execute('mkdir -p "' .. telemetryDir .. '"')
   local members: {string} = {}
   for _, n in ipairs(NODES) do
      members[#members + 1] = "--member " .. n.name .. "=" .. n.peer
   end
   local cmd = string.format(
      '"%s" "%s" --node %s --client %s --state "%s" %s '
         .. '--telemetry-log "%s" --telemetry-min-bytes %d '
         .. '>> "%s" 2>&1 & echo $!',
      p.lunetRun, p.serverLua, node.name, node.client,
      p.tmp .. "/" .. node.name .. ".nonce",
      table.concat(members, " "),
      telemetryDir, TELEMETRY_MIN_BYTES,
      nodeLogfile(p, node.name))
   local pid = sh(cmd)
   if pid == "" then
      io.stderr:write("ERROR: failed to spawn node " .. node.name
         .. "; see " .. nodeLogfile(p, node.name) .. "\n")
      os.exit(1)
   end
   writeFile(pidfile, pid .. "\n")
end

local function writeClusterJson(p: Paths, workload: string): boolean
   local members: {string} = {}
   for _, n in ipairs(NODES) do
      members[#members + 1] = string.format(
         '{"id":"%s","name":"%s","peer":"%s","client":"%s"}',
         n.name, n.name, n.peer, n.client)
   end
   local json = string.format(
      '{"members":[%s],"telemetry":{"base":"%s","min_bytes":%d},"workload":"%s"}\n',
      table.concat(members, ","), p.telemetryBase, TELEMETRY_MIN_BYTES, workload)
   return writeFile(p.clusterJson, json)
end

local function startOpenresty(p: Paths)
   if alive(p.nginxPid) then return end
   if os.execute(string.format('openresty -t -q -c "%s"', p.conf)) ~= true then
      io.stderr:write("ERROR: openresty config test failed; see " .. p.tmp .. "/nginx-error.log\n")
      os.exit(1)
   end
   if os.execute(string.format('openresty -c "%s"', p.conf)) ~= true then
      io.stderr:write("ERROR: openresty failed to start; see " .. p.tmp .. "/nginx-error.log\n")
      os.exit(1)
   end
end

-- Single `get` probe against a node client port (consolectl runs under the
-- system luajit without a socket library; perl is already a project tool).
local function probeNode(port: string): boolean
   local cmd = "perl -MIO::Select -MIO::Socket::INET -e '"
      .. 'my $s = IO::Socket::INET->new(PeerAddr=>"127.0.0.1",PeerPort=>shift,'
      .. 'Proto=>"tcp",Timeout=>1) or exit 1; $s->autoflush(1); print $s '
      .. 'qq({"op":"get","message_id":"00000000-0000-0000-0000-0000000000aa",'
      .. '"client_id":9,"request_num":1,"lock_id":1}\\n); '
      .. "IO::Select->new($s)->can_read(2) or exit 1; "
      .. "my $line = <$s>; exit($line ? 0 : 1);' " .. port .. " 2>/dev/null"
   return os.execute(cmd) == true
end

local function anyNodeUp(): boolean
   for _, n in ipairs(NODES) do
      local port = n.client:match(":(%d+)$")
      if port and probeNode(port) then return true end
   end
   return false
end

local function waitHealthy(port: string, user: string, pw: string): boolean
   for _ = 1, 60 do
      local code = sh(string.format(
         'curl -s -o /dev/null -w "%%{http_code}" --max-time 1 -u "%s:%s" "http://127.0.0.1:%s/api/v1/health"',
         user, pw, port))
      if code == "200" and anyNodeUp() then return true end
      os.execute("sleep 0.25")
   end
   return false
end

local function up(root: string): integer
   local p = paths(root)
   local port = os.getenv("PORT") or "8480"
   local workload = os.getenv("WORKLOAD") or "on"
   if workload ~= "on" and workload ~= "off" then
      io.stderr:write("ERROR: WORKLOAD must be on|off (got '" .. workload .. "')\n")
      return 2
   end
   os.execute('mkdir -p "' .. p.tmp .. '"')

   if not checkPreconditions(p) then return 1 end

   if not writeClusterJson(p, workload) then
      io.stderr:write("ERROR: cannot write " .. p.clusterJson .. "\n")
      return 1
   end

   local conf = render(readFile(p.template), {
      ROOT = p.root,
      TMP = p.tmp,
      ASSETS = p.assets,
      PORT = port,
      WORKLOAD = workload,
   })
   if not writeFile(p.conf, conf) then
      io.stderr:write("ERROR: cannot write " .. p.conf .. "\n")
      return 1
   end

   local user, pw = ensureCredentials(p)
   for _, n in ipairs(NODES) do
      startNode(p, n)
   end
   startOpenresty(p)

   if not waitHealthy(port, user, pw) then
      io.stderr:write("ERROR: console did not become healthy; run `make logs`\n")
      return 1
   end

   io.write("Lock Admin console is up\n")
   io.write("  url:      http://127.0.0.1:" .. port .. "/\n")
   io.write("  cluster:  n1 " .. NODES[1].client .. ", n2 " .. NODES[2].client
      .. ", n3 " .. NODES[3].client .. "\n")
   io.write("  workload: " .. workload .. "\n")
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
      io.write("openresty stopped\n")
      stopped = true
   end
   for _, n in ipairs(NODES) do
      if killPidfile(nodePidfile(p, n.name)) then
         io.write(n.name .. " stopped\n")
         stopped = true
      end
   end
   if not stopped then io.write("nothing running\n") end
   return 0
end

local function status(root: string): integer
   local p = paths(root)
   local port = os.getenv("PORT") or "8480"
   local nginxUp = alive(p.nginxPid)
   local allUp = nginxUp
   for _, n in ipairs(NODES) do
      local up = alive(nodePidfile(p, n.name))
      io.write(n.name .. ":      " .. (up and "running (pid " .. pidOf(nodePidfile(p, n.name)) .. ")" or "stopped") .. "\n")
      allUp = allUp and up
   end
   io.write("openresty: " .. (nginxUp and "running (pid " .. pidOf(p.nginxPid) .. ")" or "stopped") .. "\n")
   if allUp then
      local creds = readFile(p.credentials):gsub("%s+$", "")
      io.write("url:       http://127.0.0.1:" .. port .. "/  (" .. creds .. ")\n")
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
