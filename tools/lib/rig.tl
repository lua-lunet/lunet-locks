-- The lease-sequencer rig: the typed runner library behind the example
-- entries (run.lua, run-phi.lua, run-acceptance.lua, check-standby.lua).
-- Six storage nodes, two per three datacenters, each a release rig
-- binary binding a UDP peer port and a TCP client NDJSON port. The
-- gates read the logs DIRECTORY and match node-name prefixes — no
-- globs, no exact dated names (the dated-log bug class died with the
-- shell). Every spawned process is a typed Process record, terminated
-- on exit. Configuration knobs are typed tables.

local client = require("tools.lib.client")
local clock = require("tools.lib.clock")
local fs = require("tools.lib.fs")
local gate = require("tools.lib.gate")
local logs = require("tools.lib.logs")
local proc = require("tools.lib.proc")
local shell = require("tools.lib.shell")
local snapshot = require("tools.lib.snapshot")

local record NodeSpec
    id: integer
    name: string
    port: integer
    client_port: integer
end

-- The per-run node knobs: cadences, phi thresholds, the standby's AOF
-- feed, and the descriptor the node boots.
local record NodeOpts
    heartbeat_ms: integer
    election_ms: integer
    recovery_ms: integer
    phi_threshold: string
    phi_safety: string
    aof_dir: string
    aof_flush_ms: integer
    descriptor: string
end

local function opts(
    heartbeat_ms: integer,
    election_ms: integer,
    recovery_ms: integer,
    descriptor: string
): NodeOpts
    return {
        heartbeat_ms = heartbeat_ms,
        election_ms = election_ms,
        recovery_ms = recovery_ms,
        phi_threshold = "",
        phi_safety = "",
        aof_dir = "",
        aof_flush_ms = 0,
        descriptor = descriptor,
    }
end

local record Rig
    root: string
    example_dir: string
    bin: string
    client_bin: string
    run_dir: string
    config: string
    names: {string}
    specs: {NodeSpec}
    by_name: {string: NodeSpec}
    procs: {string: proc.Process}
end

local record M
    stability: function(string): integer
    phi: function(string): integer
    acceptance: function(string): integer
    standby: function(string): integer
    Rig: Rig
end

-- ---------------------------------------------------------------- setup --

local function parse_specs(config_path: string): {NodeSpec}, {string: NodeSpec}
    local data = fs.read_all(config_path)
    local all: {NodeSpec} = {}
    local by_name: {string: NodeSpec} = {}
    for line in (data .. "\n"):gmatch("(.-)\n") do
        local name = line:match('"name":"([^"]*)"')
        local port = line:match('"port":(%d+)')
        local id = line:match('"id":(%d+)')
        if name and port and id then
            local spec: NodeSpec = {
                id = tonumber(id) as integer,
                name = name,
                port = tonumber(port) as integer,
                client_port = (tonumber(port) as integer) + 1000,
            }
            all[#all + 1] = spec
            by_name[name] = spec
        end
    end
    return all, by_name
end

local function new_rig(root: string, run_dir: string, config_path: string): Rig
    local example_dir = fs.join(root, "examples/lease-sequencer")
    local specs, by_name = parse_specs(config_path)
    local names: {string} = {}
    for _, spec in ipairs(specs) do
        names[#names + 1] = spec.name
    end
    local rig: Rig = {
        root = root,
        example_dir = example_dir,
        bin = fs.join(example_dir, "target/release/lease-sequencer"),
        client_bin = fs.join(example_dir, "target/release/lease-client"),
        run_dir = run_dir,
        config = config_path,
        names = names,
        specs = specs,
        by_name = by_name,
        procs = {},
    }
    return rig
end

-- Stale nodes from an earlier manual run hold our ports; they belong to
-- this example binary, so kill them before binding.
local function clear_stale()
    shell.status('pkill -9 -f "release/lease-sequencer --name" 2>/dev/null')
    clock.sleep(0.2)
end

-- The snapshot rule gates the wipe: the old run state is removed only
-- after a successful snapshot of it; a failed snapshot refuses the wipe
-- loudly and the run stops rather than writing over unvouched state.
local function prepare_run(rig: Rig, with_aof: boolean)
    local ok, message = snapshot.snapshot_then_wipe(rig.run_dir)
    if not ok then
        gate.fail("the run stops: " .. message)
    end
    fs.mkdir_p(fs.join(rig.run_dir, "logs"))
    fs.mkdir_p(fs.join(rig.run_dir, "state"))
    if with_aof then
        fs.mkdir_p(fs.join(rig.run_dir, "aof"))
    end
    fs.write_all(fs.join(rig.run_dir, "pids"), "")
end

local function start_node(rig: Rig, spec: NodeSpec, node_opts: NodeOpts): proc.Process
    local argv: {string} = {
        rig.bin,
        "--name",
        spec.name,
        "--config",
        node_opts.descriptor,
        "--client",
        "127.0.0.1:" .. spec.client_port,
        "--state",
        fs.join(rig.run_dir, "state", spec.name .. ".state"),
        "--log",
        fs.join(rig.run_dir, "logs", spec.name .. ".log"),
    }
    if node_opts.heartbeat_ms > 0 then
        argv[#argv + 1] = "--heartbeat-ms"
        argv[#argv + 1] = tostring(node_opts.heartbeat_ms)
    end
    if node_opts.election_ms > 0 then
        argv[#argv + 1] = "--election-ms"
        argv[#argv + 1] = tostring(node_opts.election_ms)
    end
    if node_opts.recovery_ms > 0 then
        argv[#argv + 1] = "--recovery-ms"
        argv[#argv + 1] = tostring(node_opts.recovery_ms)
    end
    if node_opts.phi_threshold ~= "" then
        argv[#argv + 1] = "--phi-threshold"
        argv[#argv + 1] = node_opts.phi_threshold
    end
    if node_opts.phi_safety ~= "" then
        argv[#argv + 1] = "--phi-safety"
        argv[#argv + 1] = node_opts.phi_safety
    end
    if node_opts.aof_dir ~= "" then
        argv[#argv + 1] = "--aof-dir"
        argv[#argv + 1] = node_opts.aof_dir
    end
    if node_opts.aof_flush_ms > 0 then
        argv[#argv + 1] = "--aof-flush-ms"
        argv[#argv + 1] = tostring(node_opts.aof_flush_ms)
    end
    -- Per-node info-level files by default; the operator opts the
    -- cluster into debug/trace detail by exporting RUST_LOG (see README).
    local env: {string: string} = {RUST_LOG = os.getenv("RUST_LOG") or "info"}
    local p = proc.spawn(
        argv,
        spec.name,
        {
            out_path = fs.join(rig.run_dir, "logs", spec.name .. ".stderr"),
            err_path = fs.join(rig.run_dir, "logs", spec.name .. ".stderr"),
            combine = true,
            env = env,
        }
    )
    rig.procs[spec.name] = p
    fs.write_all(fs.join(rig.run_dir, "pid." .. spec.name), tostring(p.pid) .. "\n")
    fs.append_all(fs.join(rig.run_dir, "pids"), tostring(p.pid) .. "\n")
    return p
end

local function start_all(rig: Rig, node_opts: NodeOpts)
    for _, spec in ipairs(rig.specs) do
        start_node(rig, spec, node_opts)
    end
end

local function start_one(rig: Rig, name: string, node_opts: NodeOpts): proc.Process
    return start_node(rig, rig.by_name[name], node_opts)
end

-- ------------------------------------------------------- log evidence --

-- The node's log-file evidence set: every file under the logs dir that
-- carries the node-name prefix (`n1.log`, `n1.stderr`, a rolled
-- `n1.<date>.log` — the whole set, never a glob).
local function node_logs(rig: Rig, name: string): {string}
    return logs.node_logs(fs.join(rig.run_dir, "logs"), name)
end

local function all_logs(rig: Rig): {string}
    return logs.files_for_all(fs.join(rig.run_dir, "logs"), rig.names)
end

local function total_renews(rig: Rig): integer
    return logs.count(all_logs(rig), "op=renew")
end

local function grant_lines(rig: Rig): {string}
    return logs.grep(all_logs(rig), "grant node=")
end

-- The node id holding the lease: the latest grant across all logs.
local function holder_id(rig: Rig): integer
    local best, holder = 0, 0
    for _, line in ipairs(grant_lines(rig)) do
        local kv = logs.kv(line)
        local ts = (tonumber(kv.ts) or 0) as integer
        local node = (tonumber(kv.node) or 0) as integer
        if ts > best then
            best = ts
            holder = node
        end
    end
    return holder
end

-- The last granted expiry of one node id.
local function last_expiry_of(rig: Rig, holder: integer): integer
    local best, expiry = 0, 0
    for _, line in ipairs(grant_lines(rig)) do
        local kv = logs.kv(line)
        local ts = (tonumber(kv.ts) or 0) as integer
        local node = (tonumber(kv.node) or 0) as integer
        if node == holder and ts > best then
            best = ts
            expiry = (tonumber(kv.expiry) or 0) as integer
        end
    end
    return expiry
end

-- The live id may be a bumped (high-band) identity; the descriptor id
-- is the low band: id mod 2^24.
local function name_of(rig: Rig, id: integer): string
    local desc = id % 16777216
    for _, spec in ipairs(rig.specs) do
        if spec.id == desc then
            return spec.name
        end
    end
    return ""
end

-- A grant by a node other than `holder` after `after`: "ts node op".
local function wait_for_steal(
    rig: Rig, after: integer, holder: integer, deadline_ms: integer
): string
    local ok, line = gate.wait_for(
        deadline_ms,
        300,
        function(): boolean, string
            for _, l in ipairs(grant_lines(rig)) do
                local kv = logs.kv(l)
                local ts = (tonumber(kv.ts) or 0) as integer
                local node = (tonumber(kv.node) or 0) as integer
                if ts > after and node ~= holder then
                    return true, l
                end
            end
            return false, "no grant by another node yet"
        end
    )
    if ok then
        return line
    end
    return ""
end

-- Drive one admin verb at the cluster: try every node's client port
-- until one accepts (the leader); non-leaders answer not_leader.
local function drive_verb(
    rig: Rig,
    action: string,
    id: integer,
    name: string,
    endpoint: string,
    deadline_ms: integer
): string
    local deadline = clock.now_ms() + deadline_ms
    while clock.now_ms() < deadline do
        for _, spec in ipairs(rig.specs) do
            local parts: {string} = {
                shell.quote(rig.client_bin),
                "--server",
                shell.quote("127.0.0.1:" .. spec.client_port),
                "--verb",
                shell.quote(action),
                "--id",
                tostring(id),
            }
            if name ~= "" then
                parts[#parts + 1] = "--name"
                parts[#parts + 1] = shell.quote(name)
            end
            if endpoint ~= "" then
                parts[#parts + 1] = "--endpoint"
                parts[#parts + 1] = shell.quote(endpoint)
            end
            local reply = shell.capture(table.concat(parts, " ") .. " 2>/dev/null")
            if reply:find('"accepted":true', 1, true) then
                return reply
            end
        end
        clock.sleep(1)
    end
    return ""
end

-- Poll `check` until it holds, or fail loud with the waited-for
-- condition and the live status.
local function wait_gate(
    deadline_ms: integer, message: string, check: function(): boolean, string
): string
    local ok, report = gate.wait_for(deadline_ms, 300, check)
    if not ok then
        gate.fail(message .. " (" .. report .. ")")
    end
    return report
end

-- --------------------------------------------- stability measurements --

-- The stability-window report: the holder's renewal cadence, the
-- non-holders' poll cadence, and the poll/renew ratio, measured from
-- the lease-attempt lines inside [t0, t1].
local function cadence_report(rig: Rig, t0: integer, t1: integer): boolean, string
    local renew_n: {string: integer} = {}
    local renew_prev: {string: integer} = {}
    local get_n: {string: integer} = {}
    local get_prev: {string: integer} = {}
    local s_get: {string: integer} = {}
    local c_get: {string: integer} = {}
    local s_renew, n_renew = 0, 0
    for _, line in ipairs(logs.grep(all_logs(rig), "lease%-attempt ")) do
        local kv = logs.kv(line)
        local ts = (tonumber(kv.ts) or 0) as integer
        local node = kv.node or ""
        local op = kv.op or ""
        if ts >= t0 and ts <= t1 then
            if op == "renew" then
                renew_n[node] = (renew_n[node] or 0) + 1
                local prev = renew_prev[node] or 0
                if prev > 0 then
                    s_renew = s_renew + (ts - prev)
                    n_renew = n_renew + 1
                end
                renew_prev[node] = ts
            elseif op == "get" then
                get_n[node] = (get_n[node] or 0) + 1
                local prev = get_prev[node] or 0
                if prev > 0 then
                    s_get[node] = (s_get[node] or 0) + (ts - prev)
                    c_get[node] = (c_get[node] or 0) + 1
                end
                get_prev[node] = ts
            end
        end
    end
    local holder_key, best = "", 0
    for node, n in pairs(renew_n) do
        if n > best then
            best = n
            holder_key = node
        end
    end
    if holder_key == "" then
        return false, "FAIL no renew attempts in the stability window"
    end
    if best < 10 then
        return false,
            "FAIL holder "
                .. holder_key
                .. " renewed only "
                .. best
                .. " times in the window (stream not steady)"
    end
    if n_renew < 1 then
        return false, "FAIL no renew interval measurable"
    end
    local mean_renew = s_renew / n_renew
    if mean_renew < 150 or mean_renew > 400 then
        return false,
            "FAIL renewal cadence "
                .. string.format("%.0f", mean_renew)
                .. " ms outside ~250 ms"
    end
    local report: {string} = {}
    local poll_seen = false
    for node, n in pairs(get_n) do
        if node ~= holder_key then
            if n < 3 then
                return false,
                    "FAIL non-holder " .. node .. " polled only " .. n .. " times (<3)"
            end
            local c = c_get[node] or 0
            if c > 0 then
                local pm = (s_get[node] or 0) / c
                poll_seen = true
                if pm < 300 or pm > 900 then
                    return false,
                        "FAIL poll cadence "
                            .. node
                            .. " "
                            .. string.format("%.0f", pm)
                            .. " ms outside the expected window"
                end
                if pm < 1.2 * mean_renew or pm > 3.5 * mean_renew then
                    return false,
                        "FAIL poll/renew ratio "
                            .. string.format("%.0f", pm)
                            .. "/"
                            .. string.format("%.0f", mean_renew)
                            .. " for "
                            .. node
                end
                report[#report + 1] = "MEASURED poll "
                    .. node
                    .. " "
                    .. string.format("%.0f", pm)
            end
        end
    end
    if not poll_seen then
        return false, "FAIL no non-holder poll cadence measurable"
    end
    report[#report + 1] = "MEASURED renew " .. string.format("%.0f", mean_renew)
    report[#report + 1] = "MEASURED holder " .. holder_key
    return true, table.concat(report, "\n")
end

-- No two holders' lease windows ever overlap: grants in ts order, the
-- live expiry per node id checked against every later grant.
local function overlap_check(rig: Rig): boolean, string
    local rows: {string} = {}
    for _, line in ipairs(grant_lines(rig)) do
        local ts = (tonumber(logs.kv(line).ts) or 0) as integer
        rows[#rows + 1] = string.format("%016d|%s", ts, line)
    end
    table.sort(rows)
    local live: {string: integer} = {}
    for _, row in ipairs(rows) do
        local line = row:match("|(.*)")
        local kv = logs.kv(line)
        local ts = (tonumber(kv.ts) or 0) as integer
        local node = tostring((tonumber(kv.node) or 0) as integer)
        local expiry = (tonumber(kv.expiry) or 0) as integer
        for h, until_ms in pairs(live) do
            if h ~= node and until_ms > ts then
                return false, "holders " .. h .. " and " .. node .. " overlap at " .. ts
            end
        end
        if expiry > (live[node] or 0) then
            live[node] = expiry
        end
    end
    return true, ""
end

local function measured_field(report: string, field: string): string
    for line in report:gmatch("[^\n]+") do
        local v = line:match("^MEASURED " .. field .. " (%S+)")
        if v then
            return v
        end
    end
    return "0"
end

-- Every runner body is guarded: a gate failure terminates every node
-- process before the run reports nonzero — the trap-cleanup discipline,
-- typed.
local function guarded(body: function(): integer): integer
    local ok, e = pcall(body)
    proc.stop_all()
    if not ok then
        io.stderr:write(tostring(e) .. "\n")
        return 1
    end
    return 0
end

-- ---------------------------------------------------- stability check --

-- The stability check: six-node cluster, three joins and two
-- increments, the cadence gates, and three SIGKILL/restart cycles
-- asserting the lease steal and the reincarnation rejoin.
local function stability_body(root: string)
    clear_stale()
    if not shell.run(
        "cargo build --release --quiet --manifest-path "
            .. shell.quote(fs.join(root, "examples/lease-sequencer/Cargo.toml"))
    ) then
        gate.fail("build")
    end

    local rig = new_rig(root, "run/check", "config/cluster.jsonl")
    prepare_run(rig, false)

    start_all(rig, opts(100, 1000, 1000, rig.config))

    wait_gate(
        clock.now_ms() + 60000,
        "cluster did not stabilize",
        function(): boolean, string
            local n = total_renews(rig)
            return n >= 4, "renews=" .. n
        end
    )
    print("boot: cluster stabilized, holder stream running")

    for _, spec_line in ipairs(
        {
            "4 dc1-node2 127.0.0.1:41104",
            "5 dc2-node2 127.0.0.1:41105",
            "6 dc3-node2 127.0.0.1:41106",
        }
    ) do
        local id, name, endpoint = spec_line:match("^(%d+) (%S+) (%S+)")
        local reply = drive_verb(
            rig, "join", tonumber(id) as integer, name, endpoint, 90000
        )
        if reply == "" then
            gate.fail("join " .. name .. " (id " .. id .. ") not accepted within 90s")
        end
        print("join: id " .. id .. " (" .. name .. ") accepted")
    end
    if drive_verb(rig, "increment", 4, "", "", 90000) == "" then
        gate.fail("increment 4 not accepted within 90s")
    end
    print("increment: id 4 accepted")
    if drive_verb(rig, "increment", 5, "", "", 90000) == "" then
        gate.fail("increment 5 not accepted within 90s")
    end
    print("increment: id 5 accepted (id 6 stays at weight 0)")

    local before = total_renews(rig)
    wait_gate(
        clock.now_ms() + 30000,
        "cluster did not re-stabilize after joins",
        function(): boolean, string
            local n = total_renews(rig)
            return (n - before) >= 8, "renews=" .. n
        end
    )

    -- The stability window.
    local T0 = clock.now_ms()
    clock.sleep(6)
    local T1 = clock.now_ms()
    print("stability window: " .. T0 .. " .. " .. T1)
    local cad_ok, report = cadence_report(rig, T0, T1)
    if not cad_ok then
        gate.fail("stability-window assertions: " .. report)
    end
    print(report)
    local mean_renew = measured_field(report, "renew")
    print("measured renewal cadence: " .. mean_renew .. " ms")

    local overlap_ok, overlap_msg = overlap_check(rig)
    if not overlap_ok then
        gate.fail("overlapping lease windows: " .. overlap_msg)
    end
    print(
        "overlap check: no two holders' windows overlap across "
            .. #grant_lines(rig)
            .. " grants"
    )

    -- Three kill/restart cycles.
    for cycle = 1, 3 do
        local hid = holder_id(rig)
        if hid == 0 then
            gate.fail("cycle " .. cycle .. ": no holder to kill")
        end
        local hname = name_of(rig, hid)
        if hname == "" then
            gate.fail(
                "cycle "
                    .. cycle
                    .. ": holder id "
                    .. hid
                    .. " maps to no descriptor name"
            )
        end
        local hproc = rig.procs[hname]
        local old_expiry = last_expiry_of(rig, hid)
        proc.kill(hproc)
        proc.wait(hproc)
        rig.procs[hname] = nil
        local kill_ts = clock.now_ms()
        print(
            "cycle "
                .. cycle
                .. ": SIGKILL holder "
                .. hname
                .. " (id "
                .. hid
                .. ", pid "
                .. hproc.pid
                .. ") at "
                .. kill_ts
                .. ", old expiry "
                .. old_expiry
        )

        clock.sleep(2)

        local after = kill_ts > old_expiry and kill_ts or old_expiry
        local steal = wait_for_steal(rig, after, hid, kill_ts + 30000)
        if steal == "" then
            gate.fail(
                "cycle "
                    .. cycle
                    .. ": lease not stolen by a survivor within 30 s of the kill"
            )
        end
        print("cycle " .. cycle .. ": stolen at " .. steal)

        start_one(rig, hname, opts(100, 1000, 1000, rig.config))

        local bumped = wait_gate(
            clock.now_ms() + 30000,
            "cycle "
                .. cycle
                .. ": restarted "
                .. hname
                .. " did not reincarnate (no identity bump in its boot note)",
            function(): boolean, string
                for _, line in ipairs(logs.grep(node_logs(rig, hname), "boot name=")) do
                    local kv = logs.kv(line)
                    local ts = (tonumber(kv.ts) or 0) as integer
                    local own = (tonumber(kv.own) or 0) as integer
                    local inc = (tonumber(kv.incarnation) or 0) as integer
                    if ts > kill_ts and inc >= 1 and own ~= hid % 16777216 then
                        return true, tostring(own)
                    end
                end
                return false, "no bumped boot note yet"
            end
        )
        print("cycle " .. cycle .. ": " .. hname .. " reincarnated as id " .. bumped)

        wait_gate(
            clock.now_ms() + 30000,
            "cycle "
                .. cycle
                .. ": remap notice old="
                .. hid
                .. " new="
                .. bumped
                .. " seen at too few peers",
            function(): boolean, string
                local n = 0
                for _, other in ipairs(rig.names) do
                    if other ~= hname then
                        if
                            logs.count(
                                node_logs(rig, other),
                                "remap old=" .. hid .. " new=" .. bumped
                            ) > 0
                        then
                            n = n + 1
                        end
                    end
                end
                return n >= 2, "seen at " .. n .. " peers"
            end
        )
        print("cycle " .. cycle .. ": remap notice seen at 2 peers")

        local before_cycle = total_renews(rig)
        wait_gate(
            clock.now_ms() + 30000,
            "cycle " .. cycle .. ": cluster did not re-stabilize after the restart",
            function(): boolean, string
                local n = total_renews(rig)
                return (n - before_cycle) >= 8, "renews=" .. n
            end
        )
        local held_again = false
        for _, line in ipairs(logs.grep(node_logs(rig, hname), "grant node=")) do
            if tonumber(logs.kv(line).node) == tonumber(bumped) then
                held_again = true
            end
        end
        print(
            "cycle "
                .. cycle
                .. ": re-stabilized; "
                .. hname
                .. " held the lease again after rejoin: "
                .. (held_again and "yes" or "no")
        )
        fs.append_all(
            fs.join(rig.run_dir, "cycles.txt"),
            cycle
                .. " "
                .. hid
                .. " "
                .. bumped
                .. " "
                .. steal
                .. " held_again="
                .. (held_again and "yes" or "no")
                .. "\n"
        )
    end

    print("")
    print("GREEN: stability check passed (3 kill/restart cycles)")
    print("measured renewal cadence: " .. mean_renew .. " ms")
    for line in report:gmatch("[^\n]+") do
        local poll_name, poll_value = line:match("^MEASURED poll (%S+) (%d+)")
        if poll_name then
            print("measured poll cadence " .. poll_name .. ": " .. poll_value .. " ms")
        end
    end
    if fs.exists(fs.join(rig.run_dir, "cycles.txt")) then
        print(fs.read_all(fs.join(rig.run_dir, "cycles.txt")):gsub("\n$", ""))
    end
end

function M.stability(root: string): integer
    return guarded(function(): integer stability_body(root); return 0 end)
end

-- ------------------------------------------------- phi kill-leader run --

-- The phi-accrual kill-leader measurement: a 3-node genesis cluster on
-- 127.0.0.1 with 10 ms heartbeats; the leader is SIGKILLed and the run
-- measures the phi detection latency, the actual new-leader (view
-- change) latency, and the lease reacquire latency. The report lands
-- in .tmp/phi-run/. Observational: the only hard gate is convergence.
function M.phi(root: string): integer
    return guarded(
        function(): integer
            clear_stale()
            local report_dir = fs.join(root, ".tmp/phi-run")
            local config_path = fs.join(report_dir, "phi-cluster.jsonl")
            if not fs.exists(config_path) then
                fs.mkdir_p(report_dir)
                -- The descriptor this run drives: three genesis nodes, client
                -- ports = peer port + 1000.
                local lines: {string} = {}
                for i = 1, 3 do
                    lines[#lines + 1] = string.format(
                        '{"id":%d,"name":"n%d","host":"127.0.0.1","port":%d,"genesis":true}',
                        i,
                        i,
                        43000 + i
                    )
                end
                fs.write_all(config_path, table.concat(lines, "\n") .. "\n")
            end
            if not shell.run(
                "cargo build --release --quiet --manifest-path "
                    .. shell.quote(fs.join(root, "examples/lease-sequencer/Cargo.toml"))
            ) then
                gate.fail("build")
            end

            local rig = new_rig(root, report_dir .. "/phi-run", config_path)
            prepare_run(rig, false)
            local phi_opts = opts(10, 0, 0, config_path)
            phi_opts.phi_threshold = "1.0"
            phi_opts.phi_safety = "2.0"
            start_all(rig, phi_opts)

            wait_gate(
                clock.now_ms() + 30000,
                "cluster did not grant a lease",
                function(): boolean, string
                    local n = #grant_lines(rig)
                    return n >= 1, "grants=" .. n
                end
            )
            print("converged: lease granted")

            -- The consensus leader: each node logs status lines naming leader=<id>.
            clock.sleep(2)
            local leader_id = 0
            local leader_ts = 0
            for _, line in ipairs(logs.grep(all_logs(rig), "status state=")) do
                local kv = logs.kv(line)
                if kv.state == "0" then
                    local ts = (tonumber(kv.ts) or 0) as integer
                    if ts > leader_ts then
                        leader_ts = ts
                        leader_id = (tonumber(kv.leader) or 0) as integer
                    end
                end
            end
            if leader_id == 0 then
                gate.fail("no leader identified in the status logs")
            end
            local leader_name = name_of(rig, leader_id)
            if leader_name == "" then
                gate.fail("leader id " .. leader_id .. " maps to no node name")
            end
            local leader_pid = rig.procs[leader_name].pid
            print(
                "leader: "
                    .. leader_name
                    .. " (id "
                    .. leader_id
                    .. ", pid "
                    .. leader_pid
                    .. ")"
            )

            -- Steady state: the heartbeat interval distribution.
            local steady_t0 = clock.now_ms()
            clock.sleep(4)
            local steady_t1 = clock.now_ms()
            local steady: {string} = {}
            local steady_n: {string: integer} = {}
            local steady_s: {string: integer} = {}
            local steady_mn: {string: integer} = {}
            local steady_mx: {string: integer} = {}
            for _, line in ipairs(logs.grep(all_logs(rig), "phi%-interval ")) do
                local kv = logs.kv(line)
                local ts = (tonumber(kv.ts) or 0) as integer
                local dt = (tonumber(kv.dt) or 0) as integer
                local node = kv.node or ""
                if ts >= steady_t0 and ts <= steady_t1 and dt > 0 and dt <= 200 then
                    steady_n[node] = (steady_n[node] or 0) + 1
                    steady_s[node] = (steady_s[node] or 0) + dt
                    local mn = steady_mn[node] or dt
                    if dt < mn then
                        mn = dt
                    end
                    steady_mn[node] = mn
                    local mx = steady_mx[node] or dt
                    if dt > mx then
                        mx = dt
                    end
                    steady_mx[node] = mx
                end
            end
            for node, n in pairs(steady_n) do
                steady[#steady + 1] = string.format(
                    "STEADY node=%s samples=%d min=%d mean=%.1f max=%d",
                    node,
                    n,
                    steady_mn[node] or 0,
                    (steady_s[node] or 0) / n,
                    steady_mx[node] or 0
                )
            end
            table.sort(steady)
            fs.write_all(
                fs.join(rig.run_dir, "steady.txt"), table.concat(steady, "\n") .. "\n"
            )
            for _, line in ipairs(steady) do
                print(line)
            end

            -- Kill the leader.
            local kill_ts = clock.now_ms()
            local leader = rig.procs[leader_name]
            proc.kill(leader)
            proc.wait(leader)
            rig.procs[leader_name] = nil
            print(
                "kill: SIGKILL "
                    .. leader_name
                    .. " (pid "
                    .. leader_pid
                    .. ") at "
                    .. kill_ts
            )

            -- The phi detector fires in milliseconds; the core's own
            -- view-change gate (primary_timeout, 5000 ms) decides when the
            -- fence actually runs. Let the aftermath settle before parsing.
            clock.sleep(8)

            -- phi detection: the FIRST phi-detect line after the kill.
            local detect_ts = 0
            for _, line in ipairs(logs.grep(all_logs(rig), "phi%-detect ")) do
                local ts = (tonumber(logs.kv(line).ts) or 0) as integer
                if ts >= kill_ts and (detect_ts == 0 or ts < detect_ts) then
                    detect_ts = ts
                end
            end

            -- The new leader: the first post-kill leader-change status line
            -- naming a different leader id.
            local new_leader_ts = 0
            local new_leader_line = ""
            for _, line in ipairs(logs.grep(all_logs(rig), "leader leader=")) do
                local kv = logs.kv(line)
                local ts = (tonumber(kv.ts) or 0) as integer
                local node_id = (tonumber(kv.node) or 0) as integer
                local leader_id_v = (tonumber(kv.leader) or 0) as integer
                if
                    ts >= kill_ts
                    and node_id ~= leader_id_v
                    and leader_id_v ~= leader_id
                then
                    new_leader_ts = ts
                    new_leader_line = line
                    break
                end
            end
            print(
                "new leader line: "
                    .. (new_leader_line ~= "" and new_leader_line or "NONE")
            )

            -- Lease reacquire: the first post-kill grant by a different node.
            local steal_ts = 0
            local steal_line = ""
            for _, line in ipairs(grant_lines(rig)) do
                local kv = logs.kv(line)
                local ts = (tonumber(kv.ts) or 0) as integer
                local node = (tonumber(kv.node) or 0) as integer
                if ts >= kill_ts and node ~= leader_id then
                    steal_ts = ts
                    steal_line = line
                    break
                end
            end
            print("steal line: " .. (steal_line ~= "" and steal_line or "NONE"))

            -- Exactly-one-new-leader: every survivor's last leader line.
            print("--- last leader lines per survivor ---")
            for _, name in ipairs(rig.names) do
                if name ~= leader_name then
                    local lines = logs.grep(node_logs(rig, name), "leader leader=")
                    local last = lines[#lines] or "none"
                    print(name .. ": " .. last)
                end
            end

            print("")
            print("=== phi kill-leader measurement ===")
            print("kill ts:                    " .. kill_ts)
            if detect_ts > 0 then
                print("phi detection latency:      " .. (detect_ts - kill_ts) .. " ms")
            else
                print("phi detection latency:      NOT DETECTED (phi monitoring inert)")
            end
            if new_leader_ts > 0 then
                print(
                    "new leader latency:         "
                        .. (new_leader_ts - kill_ts)
                        .. " ms  (baseline ~5000 ms)"
                )
            else
                print("new leader latency:         NOT CONVERGED within the window")
            end
            if steal_ts > 0 then
                print("lease reacquire latency:    " .. (steal_ts - kill_ts) .. " ms")
            else
                print("lease reacquire latency:    NOT CONVERGED within the window")
            end
            local measurements: {string} = {
                "kill_ts=" .. kill_ts,
                "detect_latency=" .. (detect_ts > 0 and (detect_ts - kill_ts) or -1),
                "new_leader_latency="
                    .. (new_leader_ts > 0 and (new_leader_ts - kill_ts) or -1),
                "steal_latency=" .. (steal_ts > 0 and (steal_ts - kill_ts) or -1),
            }
            fs.write_all(
                fs.join(rig.run_dir, "measurements.txt"),
                table.concat(measurements, "\n") .. "\n"
            )
            return 0
        end
    )
end

-- --------------------------------------------- membership-snapshot run --

-- The membership-snapshot acceptance: boot a 7th node on the GENESIS
-- descriptor while the live cluster is at era 5 (after three joins and
-- two increments), and assert, from the node's log and its membership
-- sidecar: the era-chain escalation, the quorum of agreeing snapshots
-- adopted in memory (source=discovery), the write-behind sidecar, the
-- fenced-boot join and the leader's post-commit dissemination carrying
-- it to the post-increment era, and the weight-0 learner's counted-but-
-- not-voting snapshots.
function M.acceptance(root: string): integer
    return guarded(
        function(): integer
            clear_stale()
            local example_dir = fs.join(root, "examples/lease-sequencer")
            if not shell.run(
                "cargo build --release --quiet --manifest-path "
                    .. shell.quote(fs.join(root, "examples/lease-sequencer/Cargo.toml"))
            ) then
                gate.fail("build")
            end

            local rig = new_rig(
                root,
                fs.join(example_dir, "run/acceptance"),
                fs.join(example_dir, "config/cluster.jsonl")
            )
            prepare_run(rig, false)

            local genesis = fs.join(example_dir, "config/cluster-genesis.jsonl")
            if not fs.exists(genesis) then
                gate.fail("the genesis descriptor is missing: " .. genesis)
            end

            -- The six driving nodes: the 7th node is driven by its own boot,
            -- never by a verb.
            local drive_specs: {NodeSpec} = {}
            for _, spec in ipairs(rig.specs) do
                if spec.id <= 6 then
                    drive_specs[#drive_specs + 1] = spec
                end
            end

            start_all(rig, opts(100, 1000, 1000, rig.config))

            wait_gate(
                clock.now_ms() + 60000,
                "the genesis cluster did not stabilize",
                function(): boolean, string
                    local n = #logs.grep(all_logs(rig), "grant node=")
                    return n >= 4, "grants=" .. n
                end
            )
            print("boot: genesis cluster stabilized")

            local join_specs = {
                "4 dc1-node2 127.0.0.1:41104",
                "5 dc2-node2 127.0.0.1:41105",
                "6 dc3-node2 127.0.0.1:41106",
            }
            for _, spec_line in ipairs(join_specs) do
                local id, name, endpoint = spec_line:match("^(%d+) (%S+) (%S+)")
                local reply = drive_verb(
                    rig, "join", (tonumber(id) or 0) as integer, name, endpoint, 120000
                )
                if reply == "" then
                    gate.fail(
                        "join " .. name .. " (id " .. id .. ") not accepted within 120s"
                    )
                end
                print("join: id " .. id .. " (" .. name .. ") accepted")
                clock.sleep(2)
            end
            if drive_verb(rig, "increment", 4, "", "", 120000) == "" then
                gate.fail("increment 4 not accepted within 120s")
            end
            print("increment: id 4 accepted")
            clock.sleep(2)
            if drive_verb(rig, "increment", 5, "", "", 120000) == "" then
                gate.fail("increment 5 not accepted within 120s")
            end
            print("increment: id 5 accepted (id 6 stays at weight 0)")
            clock.sleep(2)

            -- The live cluster is now at era 6 (five committed
            -- reconfigurations past the descriptor's era 0).

            -- The 7th node boots on the GENESIS descriptor while the cluster
            -- is at era 5: its discovery loop escalates through the era
            -- chain, reaches a quorum of agreeing snapshots, adopts the
            -- discovered configuration in memory, feeds the fenced boot, and
            -- writes the adopted facts behind.
            local joiner = "dc3-node3"
            start_one(rig, joiner, opts(100, 1000, 1000, genesis))
            clock.sleep(1)

            local node_files = logs.node_files(fs.join(rig.run_dir, "logs"), joiner)
            if logs.count(node_files, "discovery escalate era=6") == 0 then
                gate.fail("the 7th node's discovery never escalated to the live era 6")
            end
            print("discovery: escalated to the live era 6")
            if
                logs.count(node_files, "membership snapshot adopted era=6") == 0
                or logs.count(node_files, "source=discovery") == 0
            then
                gate.fail(
                    "the 7th node never adopted a quorum of agreeing era-6 snapshots"
                )
            end
            print("discovery: quorum of agreeing era-6 snapshots adopted")

            -- The adopted facts are written behind (the membership sidecar).
            local sidecar_of = function(name: string): string
                return fs.join(rig.run_dir, "state", name .. ".state.membership")
            end
            local sidecar_era = function(name: string): integer
                local data = fs.read_all(sidecar_of(name))
                local era = data:match('"era":(%d+)')
                return (tonumber(era) or 0) as integer
            end
            local sidecar_members = function(name: string): integer
                local data = fs.read_all(sidecar_of(name))
                local n = 0
                for _ in data:gmatch('[^\n]*"endpoint"[^\n]*') do
                    n = n + 1
                end
                return n
            end

            wait_gate(
                clock.now_ms() + 15000,
                "the adopted era-6 configuration was never written behind",
                function(): boolean, string
                    return sidecar_era(joiner) == 6,
                        "sidecar era=" .. sidecar_era(joiner)
                end
            )
            print(
                "write-behind: the membership sidecar carries the adopted era-6 snapshot"
            )

            if sidecar_members(joiner) ~= 6 then
                gate.fail(
                    "the adopted sidecar should carry the six members of era 6, not "
                        .. sidecar_members(joiner)
                )
            end
            if not fs.read_all(sidecar_of(joiner)):find(
                '"id":6,"weight":0', 1, true
            ) then
                gate.fail("the adopted sidecar should carry the weight-0 learner id 6")
            end

            -- The discovered addressing rows let the node address the live
            -- members it did not remember: its status line shows it tracking
            -- the cluster.
            if logs.count(node_files, "membership snapshot adopted") == 0 then
                gate.fail("no membership adoption evidence in the 7th node's log")
            end

            local reply_join7 = drive_verb(
                rig, "join", 7, joiner, "127.0.0.1:41107", 120000
            )
            if reply_join7 == "" then
                gate.fail("join 7 not accepted within 120s")
            end
            print("join: id 7 accepted (the fenced boot carried the join)")
            clock.sleep(2)
            wait_gate(
                clock.now_ms() + 15000,
                "the 7th node never adopted the post-join era-7 dissemination",
                function(): boolean, string
                    return sidecar_era(joiner) == 7,
                        "sidecar era=" .. sidecar_era(joiner)
                end
            )

            if drive_verb(rig, "increment", 7, "", "", 120000) == "" then
                gate.fail("increment 7 not accepted within 120s")
            end
            print("increment: id 7 accepted")
            clock.sleep(2)
            wait_gate(
                clock.now_ms() + 15000,
                "the 7th node never adopted the post-increment era-8 dissemination",
                function(): boolean, string
                    return sidecar_era(joiner) == 8,
                        "sidecar era=" .. sidecar_era(joiner)
                end
            )

            -- The final sidecar content, asserted line for line: the header's
            -- slot is the establishing commit's choosing slot
            -- (run-dependent), every member line is exact.
            local sidecar_data = fs.read_all(sidecar_of(joiner))
            local sidecar: {string} = {}
            for line in (sidecar_data .. "\n"):gmatch("(.-)\n") do
                sidecar[#sidecar + 1] = line
            end
            local header = sidecar[1] or ""
            if not header:match(
                '^{"format":"membership%-sidecar/v1","era":8,"slot":%d+}'
            ) then
                gate.fail(
                    "the sidecar header does not name the era-8 generation: " .. header
                )
            end
            local expected = {
                '{"id":1,"weight":1,"endpoint":"127.0.0.1:41101"}',
                '{"id":2,"weight":1,"endpoint":"127.0.0.1:41102"}',
                '{"id":3,"weight":1,"endpoint":"127.0.0.1:41103"}',
                '{"id":4,"weight":1,"endpoint":"127.0.0.1:41104"}',
                '{"id":5,"weight":1,"endpoint":"127.0.0.1:41105"}',
                '{"id":6,"weight":0,"endpoint":"127.0.0.1:41106"}',
                '{"id":7,"weight":1,"endpoint":"127.0.0.1:41107"}',
            }
            for i, want in ipairs(expected) do
                local got = sidecar[i + 1] or ""
                if got ~= want then
                    gate.fail(
                        "the sidecar's member line "
                            .. i
                            .. " differs from the live membership:\n  want: "
                            .. want
                            .. "\n  got:  "
                            .. got
                    )
                end
            end
            fs.write_all(
                fs.join(rig.run_dir, "sidecar.members.actual"),
                table.concat(sidecar, "\n")
            )

            print("acceptance: passed")
            return 0
        end
    )
end

-- ------------------------------------------------------ standby check --

-- The standby AOF + console demo check: six nodes with dc1-node2 as the
-- STANDBY TELEMETRY NODE (weight 0, never promoted, feeding its
-- committed lock transitions to the async AOF writer), then the console
-- stack (bun mock + lock-feed + nginx) driven headless. Asserts the
-- events land in the standby's AOF series, the feed serves them, the
-- console's edge returns them, and the cluster cadence is unaffected.
function M.standby(root: string): integer
    return guarded(
        function(): integer
            clear_stale()
            local example_dir = fs.join(root, "examples/lease-sequencer")
            if not shell.run(
                "cargo build --release --quiet --manifest-path "
                    .. shell.quote(fs.join(root, "examples/lease-sequencer/Cargo.toml"))
            ) then
                gate.fail("build")
            end
            if not shell.run(
                "cargo build --release --quiet --manifest-path "
                    .. shell.quote(fs.join(root, "ext/lock_feed/Cargo.toml"))
            ) then
                gate.fail("feed build")
            end

            local rig = new_rig(
                root,
                fs.join(example_dir, "run/standby"),
                fs.join(example_dir, "config/cluster.jsonl")
            )
            prepare_run(rig, true)

            local standby_name = "dc1-node2"

            local function node_opts_for(spec: NodeSpec): NodeOpts
                local base = opts(100, 1000, 1000, rig.config)
                if spec.name == standby_name then
                    base.aof_dir = fs.join(rig.run_dir, "aof")
                    base.aof_flush_ms = 500
                end
                return base
            end
            for _, spec in ipairs(rig.specs) do
                start_node(rig, spec, node_opts_for(spec))
            end

            wait_gate(
                clock.now_ms() + 60000,
                "cluster did not stabilize",
                function(): boolean, string
                    local n = total_renews(rig)
                    return n >= 4, "renews=" .. n
                end
            )
            print("boot: cluster stabilized, holder stream running")

            -- The standby must be the FIRST post-genesis joiner: the learner
            -- folds the era that admits it and then tracks the cluster across
            -- the later eras.
            for _, spec_line in ipairs(
                {
                    "4 dc1-node2 127.0.0.1:41104",
                    "5 dc2-node2 127.0.0.1:41105",
                    "6 dc3-node2 127.0.0.1:41106",
                }
            ) do
                local id, name, endpoint = spec_line:match("^(%d+) (%S+) (%S+)")
                local reply = drive_verb(
                    rig, "join", (tonumber(id) or 0) as integer, name, endpoint, 90000
                )
                if reply == "" then
                    gate.fail(
                        "join " .. name .. " (id " .. id .. ") not accepted within 90s"
                    )
                end
                print("join: id " .. id .. " (" .. name .. ") accepted")
            end
            if drive_verb(rig, "increment", 5, "", "", 90000) == "" then
                gate.fail("increment 5 not accepted within 90s")
            end
            print("increment: id 5 accepted")
            if drive_verb(rig, "increment", 6, "", "", 90000) == "" then
                gate.fail("increment 6 not accepted within 90s")
            end
            print(
                "increment: id 6 accepted ("
                    .. standby_name
                    .. " stays at weight 0: the standby telemetry node)"
            )

            local before_joins = total_renews(rig)
            wait_gate(
                clock.now_ms() + 30000,
                "cluster did not re-stabilize after joins",
                function(): boolean, string
                    local n = total_renews(rig)
                    return (n - before_joins) >= 8, "renews=" .. n
                end
            )

            -- The AOF event count: LKE1 magic occurrences across the
            -- standby's capture series.
            local aof_events = function(): integer
                local n = 0
                for _, rel in ipairs(fs.walk(fs.join(rig.run_dir, "aof"))) do
                    if rel:match("%.bin$") then
                        local data = fs.read_all(fs.join(rig.run_dir, "aof", rel))
                        for _ in data:gmatch("LKE1") do
                            n = n + 1
                        end
                    end
                end
                return n
            end

            -- Console stack: lock-feed serves the standby's AOF series; nginx
            -- serves the static SPA and maps /feed/ to the feed. The bun mock
            -- backs the console's admin views.
            local console_dir = fs.join(root, "console")
            local feed_bin = fs.join(root, "ext/lock_feed/target/release/lock-feed")
            local up_cmd = "env PORT=8480 MOCK_PORT=8481 FEED_PORT=8482"
                .. " FEED_DIR="
                .. shell.quote(fs.join(rig.run_dir, "aof"))
                .. " FEED_BIN="
                .. shell.quote(fs.resolve(feed_bin))
                .. " make -C "
                .. shell.quote(console_dir)
                .. " up"
            if shell.status(up_cmd .. " >/dev/null") ~= 0 then
                gate.fail(
                    "console stack did not come up (see " .. console_dir .. "/tmp)"
                )
            end
            local creds = fs.read_all(fs.join(console_dir, "tmp/credentials")):gsub(
                "\n", ""
            )
            print(
                "console: up (127.0.0.1:8480, feed port 8482, AOF dir "
                    .. fs.join(rig.run_dir, "aof")
                    .. ")"
            )

            -- 1. Events land in the standby's AOF files.
            wait_gate(
                clock.now_ms() + 30000,
                "standby AOF never received events",
                function(): boolean, string
                    local n = aof_events()
                    return n >= 20, "count=" .. n
                end
            )
            local aof_count = aof_events()
            print("aof: " .. aof_count .. " events landed in the standby's AOF series")

            -- 2. The feed serves them (REST listing + file bytes).
            local files_json, files_err = client.http_get(8482, "/files", "")
            if files_err ~= "" then
                gate.fail("feed /files unreachable: " .. files_err)
            end
            if not files_json:find('"open":true', 1, true) then
                gate.fail("feed /files shows no open AOF file: " .. files_json)
            end
            local open_name = files_json:match('"name":"(ev%-open%-%d+%.bin)"')
            if not open_name then
                gate.fail("feed /files missing the open AOF file name: " .. files_json)
            end
            local feed_bytes, feed_err = client.http_get(
                8482, "/files/" .. open_name, ""
            )
            if feed_err ~= "" and feed_bytes == "" then
                gate.fail("feed /files/" .. open_name .. " unreachable: " .. feed_err)
            end
            local feed_count = 0
            for _ in feed_bytes:gmatch("LKE1") do
                feed_count = feed_count + 1
            end
            if feed_count < 20 then
                gate.fail(
                    "feed served only "
                        .. feed_count
                        .. " events from the AOF open file"
                )
            end
            print(
                "feed: served "
                    .. feed_count
                    .. " events from "
                    .. open_name
                    .. " (of "
                    .. aof_count
                    .. " in the series)"
            )
            fs.write_all(fs.join(rig.run_dir, "feed-bytes.bin"), feed_bytes)

            -- 3. The console's data endpoint (/feed/* through the nginx edge)
            -- returns the same series to the SPA.
            local edge_json, edge_err = client.http_get(8480, "/feed/files", creds)
            if edge_err ~= "" then
                gate.fail("console edge /feed/files unreachable: " .. edge_err)
            end
            if not edge_json:find('"open":true', 1, true) then
                gate.fail(
                    "console edge /feed/files shows no open AOF file: " .. edge_json
                )
            end
            if not edge_json:find(open_name, 1, true) then
                gate.fail(
                    "console edge /feed/files missing the AOF open file: " .. edge_json
                )
            end
            local edge_bytes_raw, edge_bytes_err = client.http_get(
                8480, "/feed/files/" .. open_name, creds
            )
            if edge_bytes_err ~= "" and edge_bytes_raw == "" then
                gate.fail(
                    "console edge /feed/files/"
                        .. open_name
                        .. " unreachable: "
                        .. edge_bytes_err
                )
            end
            local edge_count = 0
            for _ in edge_bytes_raw:gmatch("LKE1") do
                edge_count = edge_count + 1
            end
            if edge_count ~= feed_count then
                gate.fail(
                    "console edge served "
                        .. edge_count
                        .. " events, feed served "
                        .. feed_count
                )
            end
            print(
                "console: /feed/files through the edge returns the AOF series ("
                    .. edge_count
                    .. " events)"
            )
            fs.write_all(fs.join(rig.run_dir, "edge-bytes.bin"), edge_bytes_raw)

            -- 4. The cluster cadence is unaffected.
            local T0 = clock.now_ms()
            clock.sleep(6)
            local T1 = clock.now_ms()
            print("stability window: " .. T0 .. " .. " .. T1)
            local cad_ok, report = cadence_report(rig, T0, T1)
            if not cad_ok then
                gate.fail("stability-window assertions: " .. report)
            end
            print(report)
            print(
                "measured renewal cadence with the standby running: "
                    .. measured_field(report, "renew")
                    .. " ms"
            )

            -- The AOF kept receiving events through the window.
            local final_count = aof_events()
            if final_count <= aof_count then
                gate.fail(
                    "the AOF series stopped growing during the stability window ("
                        .. aof_count
                        .. " -> "
                        .. final_count
                        .. ")"
                )
            end
            print(
                "aof: series grew "
                    .. aof_count
                    .. " -> "
                    .. final_count
                    .. " events across the stability window"
            )

            shell.status(
                "make -C " .. shell.quote(console_dir) .. " down >/dev/null 2>&1"
            )

            print("")
            print("GREEN: standby AOF + console check passed")
            return 0
        end
    )
end

return M
