-- Worked example of an ad-hoc test harness written in Teal:
-- boot openresty on an ephemeral port, drive a shared-dict counter over nc,
-- then shut it down by pid. Referenced from CONVENTIONS.md.
local proc = require("proc")
local harness = require("harness")

local function conf(port: integer): string
   return table.concat({
      "daemon off;",
      "worker_processes 1;",
      "pid logs/nginx.pid;",
      "error_log logs/error.log warn;",
      "events { worker_connections 64; }",
      "http {",
      "    access_log off;",
      "    lua_shared_dict counters 1m;",
      "    server {",
      "        listen 127.0.0.1:" .. tostring(port) .. ";",
      "        location = /hit {",
      "            default_type text/plain;",
      "            content_by_lua_block {",
      -- incr() returns (value, err, forcible); bind it or ngx.say prints all three.
      '                local hits = ngx.shared.counters:incr("hits", 1, 0)',
      "                ngx.say(hits)",
      "            }",
      "        }",
      "        location = /count {",
      "            default_type text/plain;",
      "            content_by_lua_block {",
      '                ngx.say(ngx.shared.counters:get("hits") or 0)',
      "            }",
      "        }",
      "    }",
      "}",
   }, "\n") .. "\n"
end

return function(root: string): integer
   local t = harness.new_tally()
   local prefix = root .. "/.tmp/nginx-counter-demo"
   local conf_path = prefix .. "/conf/nginx.conf"
   local pid_path = prefix .. "/logs/nginx.pid"

   proc.shell_ok("rm -rf " .. proc.quote(prefix))
   harness.mkdir_p(prefix .. "/conf")
   harness.mkdir_p(prefix .. "/logs")

   local port = harness.free_port()
   if port == 0 then
      print("FAIL - could not find a free port")
      return 1
   end
   harness.write_file(conf_path, conf(port))

   -- -c and -e are resolved relative to the -p prefix; -e also stops nginx
   -- probing its compiled-in default error log before reading the config.
   local server = proc.spawn(
      "openresty -p " .. proc.quote(prefix) .. " -c conf/nginx.conf -e logs/error.log",
      prefix .. "/logs/stdout.log")

   local started = harness.wait_port(port, 5000)
   harness.check(t, started, "openresty listening on 127.0.0.1:" .. tostring(port))
   if not started then
      print(harness.read_file(prefix .. "/logs/error.log"))
      print(harness.read_file(prefix .. "/logs/stdout.log"))
      proc.stop(server.pid, 2000)
      return harness.finish(t)
   end

   -- `daemon off` means the process we spawned is the master, so the pid file
   -- must agree with the pid we are holding.
   harness.check(t, proc.read_pid_file(pid_path) == server.pid,
      "pid file matches spawned master pid " .. tostring(server.pid))

   for i = 1, 3 do
      local body = harness.body_line(harness.http_get(port, "/hit"))
      harness.check(t, body == tostring(i), "GET /hit returned " .. tostring(i) .. " (got " .. body .. ")")
   end

   local total = harness.body_line(harness.http_get(port, "/count"))
   harness.check(t, total == "3", "shared-dict counter persisted across requests (got " .. total .. ")")

   harness.check(t, proc.stop(server.pid, 3000), "master stopped by pid")
   harness.check(t, not harness.port_busy(port), "port released after shutdown")

   return harness.finish(t)
end
