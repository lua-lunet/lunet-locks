-- The softball-2 crash/partition run (typed runner library): the
-- crash-and-partition lane of docs/src/softball-run.md under one
-- continuous polite client fleet — partitions (process-suspension
-- injection on the loopback rig), SIGKILL crash families at shallow and
-- deep views, the leader-crash+partition compound, abdication under
-- load, and the whole-cluster stop/start passes after the deep state.
-- Every action is anchored live (epoch millisecond before the action,
-- into anchors.md) and every scenario's durations, states, drop deltas
-- and client-error deltas are captured as key=value measure lines in
-- the same anchors file. Cluster: three genesis voters on loopback.
-- Knobs: the calm profile (heartbeat 5 ms, election 3000 ms,
-- recovery 3000 ms — the randomized leader timeout 3000–5000 ms) on the
-- flight-recorder build. Every rejoin is bounded by its own gate
-- deadline (150 s); a bounded-out rejoin is recorded as BLOCKED with
-- its evidence and the node is parked — the run continues on the
-- serving remainder.

local clock = require("tools.lib.clock")
local fs = require("tools.lib.fs")
local gate = require("tools.lib.gate")
local logs = require("tools.lib.logs")
local proc = require("tools.lib.proc")
local shell = require("tools.lib.shell")
local snapshot = require("tools.lib.snapshot")

local record Spec
    id: integer
    name: string
    port: integer
    client_port: integer
end

local record M
    run: function(string, string): integer
end

local GENESIS = {"n1", "n2", "n3"}
local LEASE_LOCK = 14531090
local STREAM_LOCK = 9201
local LEADER_DEADLINE_MS = 60000
local REJOIN_DEADLINE_MS = 150000
local STEADY_S = 180
local DEEP_VIEW_TARGET = 120

local root = ""
local run_dir = ""
local specs: {Spec} = {}
local by_name: {string: Spec} = {}
local procs: {string: proc.Process} = {}
local load_procs: {string: proc.Process} = {}
local hammer_procs: {proc.Process} = {}
local probe_epoch = 0

-- ------------------------------------------------------------ records --

local function anchor(label: string): integer
    local ts = clock.now_ms()
    fs.append_all(
        fs.join(run_dir, "anchors.md"), "anchor=" .. ts .. " " .. label .. "\n"
    )
    return ts
end

local function progress(line: string)
    fs.append_all(
        fs.join(run_dir, "progress.log"), os.date("!%H:%M:%S") .. " " .. line .. "\n"
    )
end

local function measure(kv: string)
    fs.append_all(fs.join(run_dir, "anchors.md"), "measure " .. kv .. "\n")
    progress("measure " .. kv)
end

local function phase(line: string)
    progress("== " .. line .. " ==")
end

-- -------------------------------------------------------------- reads --

local function parse_specs(config_path: string)
    local data = fs.read_all(config_path)
    for line in (data .. "\n"):gmatch("(.-)\n") do
        local name = line:match('"name":"([^"]*)"')
        local port = line:match('"port":(%d+)')
        local id = line:match('"id":(%d+)')
        if name and port and id then
            local spec: Spec = {
                id = tonumber(id) as integer,
                name = name,
                port = tonumber(port) as integer,
                client_port = (tonumber(port) as integer) + 10,
            }
            specs[#specs + 1] = spec
            by_name[name] = spec
        end
    end
end

local function node_log_files(name: string): {string}
    return logs.node_logs(fs.join(run_dir, "logs"), name)
end

-- The node's newest status note at or after `since_ms` (the status plane
-- is a 2 s cadence; every note carries ts=<epoch ms>).
local function status_at(name: string, since_ms: integer): {string: string}
    local latest: {string: string} = {}
    for _, line in ipairs(logs.grep(node_log_files(name), "status state=")) do
        local kv = logs.kv(line)
        local ts = tonumber(kv.ts) or 0
        if ts >= since_ms then
            latest = kv
        end
    end
    return latest
end

-- The current leader: the live voter whose newest status names itself.
local function leader_name(): string
    for _, name in ipairs(GENESIS) do
        local p = procs[name]
        if p ~= nil and proc.alive(p) then
            local kv = status_at(name, 0)
            if
                kv.state == "normal" and (tonumber(kv.leader) or 0) == by_name[name].id
            then
                return name
            end
        end
    end
    return ""
end

-- The states every voter holds right now, one capture string. A
-- suspended member reports up:frozen (the process is alive but the
-- status plane is silent).
local function states_snapshot(): string
    local parts: {string} = {}
    for _, name in ipairs(GENESIS) do
        local p = procs[name]
        if p ~= nil and proc.alive(p) then
            local kv = status_at(name, clock.now_ms() - 8000)
            parts[#parts + 1] = name
                .. ":"
                .. (kv.state ~= "" and kv.state or "up")
                .. ":v"
                .. (kv.view or "?")
                .. ":l"
                .. (kv.leader or "?")
        else
            parts[#parts + 1] = name .. ":down"
        end
    end
    return table.concat(parts, " ")
end

-- The highest view any live voter's newest status reports.
local function view_depth(): integer
    local depth = 0
    for _, name in ipairs(GENESIS) do
        local p = procs[name]
        if p ~= nil and proc.alive(p) then
            local kv = status_at(name, clock.now_ms() - 6000)
            local v = (tonumber(kv.view) or 0) as integer
            if v > depth then
                depth = v
            end
        end
    end
    return depth
end

-- The durable-state files of the cluster copied into the SIBLING
-- state-snaps dir (outside the run directory: the check tool reads the
-- run dir's markers as the nodes' live durable state, and an archival
-- copy of an older marker inverts against the teardown stop record).
local function snapshot_state(label: string)
    local dst = fs.join(fs.dirname(run_dir), fs.basename(run_dir) .. "-state-snaps", label)
    fs.mkdir_p(dst)
    shell.status(
        "cp -p " .. shell.quote(fs.join(run_dir, "state")) .. "/* " .. shell.quote(dst)
            .. "/ 2>/dev/null"
    )
end

-- ----------------------------------------------------------- mutators --

local function start_node(name: string): integer
    local spec = by_name[name]
    local argv: {string} = {
        fs.join(root, "examples/lease-sequencer/target/release/lease-sequencer"),
        "--name",
        spec.name,
        "--config",
        fs.join(root, "examples/lease-sequencer/config/cluster-softball2.jsonl"),
        "--client",
        "127.0.0.1:" .. spec.client_port,
        "--state",
        fs.join(run_dir, "state", spec.name .. ".state"),
        "--log",
        fs.join(run_dir, "logs", spec.name .. ".log"),
        "--heartbeat-ms",
        "5",
        "--election-ms",
        "3000",
        "--recovery-ms",
        "3000",
    }
    local p = proc.spawn(
        argv,
        spec.name,
        {
            out_path = fs.join(run_dir, "logs", spec.name .. ".stderr"),
            err_path = fs.join(run_dir, "logs", spec.name .. ".stderr"),
            combine = true,
            env = {
                RUST_LOG = os.getenv("RUST_LOG") or "info",
                LUNET_FLIGHT_RECORDER_DIR = fs.join(run_dir, "flight"),
            },
        }
    )
    procs[spec.name] = p
    fs.write_all(fs.join(run_dir, "pid." .. spec.name), tostring(p.pid) .. "\n")
    fs.append_all(fs.join(run_dir, "pids"), tostring(p.pid) .. "\n")
    return p.pid
end

-- The clean stop: TERM (proc.stop's KILL backstop never fires on a
-- healthy node — the adapter's stop contract exits on its own). A
-- suspended member is resumed first: a frozen process cannot TERM.
local function term_node(name: string, grace_ms: integer): integer
    local p = procs[name]
    if p == nil then
        return 0
    end
    if p.state == "running" then
        proc.resume(p)
    end
    local code = proc.stop(p, grace_ms)
    procs[name] = nil
    return code
end

local function start_clients()
    local servers: {string} = {}
    for _, name in ipairs(GENESIS) do
        table.insert(servers, "--server")
        table.insert(servers, "127.0.0.1:" .. by_name[name].client_port)
    end
    local bases: {string: string} = {n1 = "810000", n2 = "820000", n3 = "830000"}
    for _, name in ipairs(GENESIS) do
        local argv: {string} = {
            fs.join(root, "examples/lease-sequencer/target/release/lease-load"),
        }
        for _, a in ipairs(servers) do
            table.insert(argv, a)
        end
        local flags: {string} = {
            "--seconds",
            "3600",
            "--clients",
            "1",
            "--getters",
            "0",
            "--model",
            "polite",
            "--lease-ms",
            "500",
            "--renew-fraction",
            "0.5",
            "--lock-id",
            tostring(LEASE_LOCK),
            "--id-base",
            bases[name],
            "--window-ms",
            "2000",
            "--stats-out",
            fs.join(run_dir, "stats", "load-" .. name .. ".jsonl"),
        }
        for _, f in ipairs(flags) do
            table.insert(argv, f)
        end
        local p = proc.spawn(
            argv,
            "load-" .. name,
            {
                out_path = fs.join(run_dir, "logs", "load-" .. name .. ".out"),
                err_path = fs.join(run_dir, "logs", "load-" .. name .. ".out"),
                combine = true,
            }
        )
        load_procs[name] = p
        fs.append_all(fs.join(run_dir, "pids"), tostring(p.pid) .. "\n")
    end
end

-- The client-start gate: USR2 opens the workers' gate; 3 s later every
-- client must be alive with a stats file holding at least one window
-- (the vacuous-zero-error pass is the defect this gate exists to
-- prevent). A dead client is respinned ONCE with its decoded exit state
-- in the progress log; a second death is a loud gate failure.
local function verify_clients_started()
    clock.sleep(3)
    for _, name in ipairs(GENESIS) do
        local p = load_procs[name]
        local dead = p == nil
            or (not proc.alive(p))
            or not fs.exists(fs.join(run_dir, "stats", "load-" .. name .. ".jsonl"))
        if dead then
            progress(
                "client " .. name .. " died at start (pid "
                    .. (p ~= nil and tostring(p.pid) or "?") .. "); respawning"
            )
            local poll_dead = p ~= nil and proc.poll(p)
            if poll_dead then
                progress(
                    "client " .. name .. " decode: state=" .. (p.state or "?")
                        .. " exit=" .. p.exit_code .. " signal=" .. p.signal
                )
            end
            if p ~= nil then
                proc.poll(p)
            end
            local servers: {string} = {}
            for _, other in ipairs(GENESIS) do
                table.insert(servers, "--server")
                table.insert(servers, "127.0.0.1:" .. by_name[other].client_port)
            end
            local bases: {string: string} = {
                n1 = "810000", n2 = "820000", n3 = "830000",
            }
            local argv: {string} = {
                fs.join(root, "examples/lease-sequencer/target/release/lease-load"),
            }
            for _, a in ipairs(servers) do
                table.insert(argv, a)
            end
            local flags: {string} = {
                "--seconds",
                "3600",
                "--clients",
                "1",
                "--getters",
                "0",
                "--model",
                "polite",
                "--lease-ms",
                "500",
                "--renew-fraction",
                "0.5",
                "--lock-id",
                tostring(LEASE_LOCK),
                "--id-base",
                bases[name],
                "--window-ms",
                "2000",
                "--stats-out",
                fs.join(run_dir, "stats", "load-" .. name .. ".jsonl"),
            }
            for _, f in ipairs(flags) do
                table.insert(argv, f)
            end
            local q = proc.spawn(
                argv,
                "load-" .. name,
                {
                    out_path = fs.join(run_dir, "logs", "load-" .. name .. ".out"),
                    err_path = fs.join(run_dir, "logs", "load-" .. name .. ".out"),
                    combine = true,
                }
            )
            load_procs[name] = q
            fs.append_all(fs.join(run_dir, "pids"), tostring(q.pid) .. "\n")
            clock.sleep(1)
            shell.status("kill -USR2 " .. q.pid .. " 2>/dev/null")
            clock.sleep(3)
            if
                not proc.alive(q)
                or not fs.exists(fs.join(run_dir, "stats", "load-" .. name .. ".jsonl"))
            then
                local decoded = proc.poll(q)
                gate.fail(
                    "client load-"
                        .. name
                        .. " died at spawn twice (state="
                        .. (q.state or "?")
                        .. " exit="
                        .. q.exit_code
                        .. " signal="
                        .. q.signal
                        .. " polled="
                        .. tostring(decoded ~= nil)
                        .. ")"
                )
            end
        end
    end
end

-- The abdication-under-load hammer: the aggressive shape on the SAME
-- lock (two contenders, two getters, no probe floor — the hammering
-- window's client-visible cost comes from here).
local function start_hammer()
    local servers: {string} = {}
    for _, name in ipairs(GENESIS) do
        table.insert(servers, "--server")
        table.insert(servers, "127.0.0.1:" .. by_name[name].client_port)
    end
    local argv: {string} = {
        fs.join(root, "examples/lease-sequencer/target/release/lease-load"),
    }
    for _, a in ipairs(servers) do
        table.insert(argv, a)
    end
    local flags: {string} = {
        "--seconds",
        "600",
        "--clients",
        "2",
        "--getters",
        "2",
        "--model",
        "aggressive",
        "--lease-ms",
        "500",
        "--renew-fraction",
        "0.5",
        "--lock-id",
        tostring(LEASE_LOCK),
        "--id-base",
        "840000",
        "--window-ms",
        "2000",
        "--stats-out",
        fs.join(run_dir, "stats", "hammer.jsonl"),
    }
    for _, f in ipairs(flags) do
        table.insert(argv, f)
    end
    local p = proc.spawn(
        argv,
        "hammer",
        {
            out_path = fs.join(run_dir, "logs", "hammer.out"),
            err_path = fs.join(run_dir, "logs", "hammer.out"),
            combine = true,
        }
    )
    hammer_procs[#hammer_procs + 1] = p
    fs.append_all(fs.join(run_dir, "pids"), tostring(p.pid) .. "\n")
    -- The hammer boots silent: the start signal must land after the
    -- client's SIGUSR2 handler is installed (the abort-3 finding).
    clock.sleep(1)
    shell.status("kill -USR2 " .. p.pid .. " 2>/dev/null")
end

local function signal_procs(list: {proc.Process}, signal_name: string)
    for _, p in ipairs(list) do
        if proc.alive(p) then
            shell.status("kill -" .. signal_name .. " " .. p.pid .. " 2>/dev/null")
        end
    end
end

local function signal_clients(signal_name: string)
    local list: {proc.Process} = {}
    for _, p in pairs(load_procs) do
        list[#list + 1] = p
    end
    signal_procs(list, signal_name)
end

-- -------------------------------------------------------------- gates --

-- The first live genesis voter other than `exclude` ("" = none excluded);
-- the probe target when the named node is down.
local function live_probe_target(exclude: string): string
    for _, name in ipairs(GENESIS) do
        if name ~= exclude then
            local p = procs[name]
            if p ~= nil and proc.alive(p) then
                return name
            end
        end
    end
    return exclude
end

-- The serving probe: `n` successful lock reads through the SHIPPED Rust
-- client, one process each (the probe shape that never loses a reply),
-- at the named node's client port. Returns the wall ts of the n-th ok
-- probe — the failover-landing measurement — and -1 when the deadline
-- expired first. Each probe's duration is logged to probes.log.
local function probe_serving(
    node: string, n: integer, deadline_ms: integer
): boolean, integer
    local landed = -1
    local client_bin = fs.join(
        root, "examples/lease-sequencer/target/release/lease-client"
    )
    local port = by_name[node].client_port
    local t_deadline = clock.now_ms() + deadline_ms
    local count = 0
    while clock.now_ms() < t_deadline do
        local t0 = clock.mono_ms()
        local reply = shell.capture(
            client_bin
                .. " --server 127.0.0.1:"
                .. port
                .. " --verb get --lock "
                .. STREAM_LOCK
                .. " 2>/dev/null"
        )
        local dur = clock.mono_ms() - t0
        if reply:find('"op":"get"', 1, true) ~= nil then
            count = count + 1
            if landed < 0 then
                landed = clock.now_ms() as integer
            end
            fs.append_all(
                fs.join(run_dir, "probes.log"),
                "rel="
                    .. (landed - (probe_epoch > 0 and probe_epoch or landed))
                    .. " node="
                    .. node
                    .. " dur_ms="
                    .. dur
                    .. "\n"
            )
            if count >= n then
                return true, landed
            end
        end
        clock.sleep(0.3)
    end
    return false, landed
end

-- Every LIVE genesis voter sits at state=normal with one named leader
-- (all-live is not required: the crashed or parked members are skipped).
local function wait_quorum(min_live: integer, deadline_ms: integer): boolean, string
    return gate.wait_for(
        clock.now_ms() + deadline_ms,
        500,
        function(): boolean, string
            local found, normal_n, live_n = "", 0, 0
            for _, name in ipairs(GENESIS) do
                local p = procs[name]
                if p ~= nil and proc.alive(p) then
                    live_n = live_n + 1
                    local kv = status_at(name, clock.now_ms() - 6000)
                    if kv.state == "normal" and (tonumber(kv.leader) or 0) > 0 then
                        normal_n = normal_n + 1
                        if (tonumber(kv.leader) or 0) == by_name[name].id then
                            found = name
                        end
                    end
                end
            end
            if live_n < min_live then
                return false, "live voters: " .. live_n .. "/" .. #GENESIS
            end
            if normal_n < live_n then
                return false, "normal voters: " .. normal_n .. "/" .. live_n
            end
            if found == "" then
                return false, "no live voter names itself leader"
            end
            return true, "leader=" .. found
        end
    )
end

local function wait_converged(expected: string, deadline_ms: integer): boolean, string
    return gate.wait_for(
        clock.now_ms() + deadline_ms,
        500,
        function(): boolean, string
            local found, normal_n, live_n = "", 0, 0
            for _, name in ipairs(GENESIS) do
                local p = procs[name]
                if p ~= nil and proc.alive(p) then
                    live_n = live_n + 1
                    local kv = status_at(name, clock.now_ms() - 6000)
                    if kv.state == "normal" and (tonumber(kv.leader) or 0) > 0 then
                        normal_n = normal_n + 1
                        if (tonumber(kv.leader) or 0) == by_name[name].id then
                            found = name
                        end
                    end
                end
            end
            if live_n < #GENESIS then
                return false, "live voters: " .. live_n .. "/" .. #GENESIS
            end
            if normal_n < #GENESIS then
                return false, "normal voters: " .. normal_n .. "/" .. #GENESIS
            end
            if found == "" then
                return false, "no voter names itself leader"
            end
            if expected ~= "" and found ~= expected then
                return false, "leader is " .. found .. ", waiting for " .. expected
            end
            return true, "leader=" .. found
        end
    )
end

-- A node's newest status at/after `since_ms` shows state=normal at voting
-- weight (its boot or rejoin evidence).
local function wait_node_normal(
    name: string, since_ms: integer, deadline_ms: integer
): boolean, string
    return gate.wait_for(
        clock.now_ms() + deadline_ms,
        300,
        function(): boolean, string
            local kv = status_at(name, since_ms)
            if
                kv.state == "normal"
                and kv.voting == "1"
                and (tonumber(kv.leader) or 0) > 0
            then
                return true,
                    "state=normal leader="
                        .. (kv.leader or "?")
                        .. " view="
                        .. (kv.view or "?")
            end
            return false,
                "latest status="
                    .. (kv.state ~= "" and kv.state or "none")
                    .. " voting="
                    .. (kv.voting or "?")
        end
    )
end

-- One admin verb drive at one node's client port (the rig client binary,
-- fresh message id). Returns the reply line and the drive's wall
-- duration in ms.
local function drive_verb(
    name: string, action: string, id: integer, extra: {string}
): string, integer
    local parts: {string} = {
        fs.join(root, "examples/lease-sequencer/target/release/lease-client"),
        "--server",
        "127.0.0.1:" .. by_name[name].client_port,
        "--verb",
        action,
    }
    if id > 0 then
        table.insert(parts, "--id")
        table.insert(parts, tostring(id))
    end
    for _, a in ipairs(extra or {}) do
        table.insert(parts, a)
    end
    local t0 = clock.mono_ms()
    local reply = shell.capture(table.concat(parts, " ") .. " 2>/dev/null")
    return reply, (clock.mono_ms() - t0)
end

-- One admin verb drive with the driver's rotation: fresh message ids per
-- attempt (each lease-client invocation mints its own), every live
-- genesis voter's client port in turn, until the verb is accepted or the
-- deadline expires.
local function drive_verb_retry(
    action: string, id: integer, extra: {string}, deadline_ms: integer
): string, integer
    local t_deadline = clock.now_ms() + deadline_ms
    local last_reply, last_ms = "", 0
    while clock.now_ms() < t_deadline do
        local candidates: {string} = {}
        for _, name in ipairs(GENESIS) do
            local p = procs[name]
            if p ~= nil and proc.alive(p) then
                candidates[#candidates + 1] = name
            end
        end
        for _, name in ipairs(candidates) do
            local reply, ms = drive_verb(name, action, id, extra)
            last_reply, last_ms = reply, ms
            if reply:find('"accepted":true', 1, true) ~= nil then
                return reply, ms
            end
            clock.sleep(1)
        end
        clock.sleep(1)
    end
    return last_reply, last_ms
end

-- The lease-load cumulative error count of one client's stats file (the
-- window summaries always report errors: 0; the cumulative line is the
-- truth).
-- The lease-load stats JSON emits keys alphabetically (serde_json's
-- BTreeMap): per line the cumulative summary comes FIRST, the window
-- summary LAST, and the window's own errors field is always 0. The
-- cumulative counters are therefore the FIRST match per line — the
-- last-match read is the abort-4 finding (it read the window's 0).
local function cumulative_errors_file(stats_path: string): integer
    local f = io.open(stats_path, "r")
    if f == nil then
        return 0
    end
    local data = f:read("*a")
    f:close()
    local last = 0
    for line in data:gmatch("[^\r\n]+") do
        local first = line:match('"errors":(%d+)')
        if first ~= nil then
            last = tonumber(first) as integer
        end
    end
    return last
end

local function stats_path(name: string): string
    return fs.join(run_dir, "stats", "load-" .. name .. ".jsonl")
end

local function cumulative_errors(name: string): integer
    return cumulative_errors_file(stats_path(name))
end

local function total_errors(): integer
    local n = 0
    for _, name in ipairs(GENESIS) do
        n = n + cumulative_errors(name)
    end
    return n
end

local function hammer_errors(): integer
    return cumulative_errors_file(fs.join(run_dir, "stats", "hammer.jsonl"))
end

-- A log-count delta helper: the count of `pattern` matches in the
-- node's logs captured before and after a window; the difference is the
-- window's count (no ts on every line, so the line totals bracket it).
local function line_count(name: string, pattern: string): integer
    return logs.count(node_log_files(name), pattern)
end

-- The named-diagnostic drop counter: per-reason counts over a node's
-- whole log surface (windowed deltas are taken by the caller at two
-- points in time).
local function drop_reasons(name: string): {string: integer}
    local reasons: {string} = {
        "ViewMismatch",
        "GapDetected",
        "SlotNotOutstanding",
        "StaleEvidence",
        "StaleTransfer",
        "StaleViewChange",
        "StartViewFromStaleView",
        "StatusGate",
        "PrepareOkNotPrimary",
        "ReincarnationRefused",
        "TransferNotServed",
    }
    local counts: {string: integer} = {}
    for _, reason in ipairs(reasons) do
        counts[reason] = line_count(name, "diagnostic=" .. reason)
    end
    return counts
end

-- The dirty-boot evidence of one node's newest boot note: identity bump
-- fields, or "" when no bumped boot note exists.
local function bumped_boot(name: string, since_ms: integer): string
    local found = ""
    for _, line in ipairs(logs.grep(node_log_files(name), "boot name=")) do
        local kv = logs.kv(line)
        if
            (tonumber(kv.ts) or 0) >= since_ms
            and (tonumber(kv.incarnation) or 0) >= 1
        then
            found = "incarnation="
                .. (kv.incarnation or "?")
                .. " own="
                .. (kv.own or "?")
        end
    end
    return found
end

-- The newest named diagnostic in one node's logs (the fenced node's
-- newest complaint).
local function newest_diagnostic(name: string): string
    local lines = logs.grep(node_log_files(name), "diagnostic=")
    local n = #lines
    if n == 0 then
        return ""
    end
    return lines[n]:match("diagnostic=([A-Za-z]+)") or ""
end

-- ------------------------------------------------------ the scenarios --

-- The steady serving window: the polite profile under zero disruptions.
-- The gate is the phase's cumulative ERROR DELTA from steady-begin (the
-- join walk's own cost is the boot's, not S0's), and it demands the
-- evidence: every client's stats file exists with its cumulative
-- counter climbing — a missing file is a failed gate, never a vacuous
-- zero.
local steady_err_begin = 0

local function scenario_steady()
    phase("steady serving " .. STEADY_S .. " s")
    local begin_err = total_errors()
    clock.sleep(STEADY_S)
    local err = total_errors() - begin_err
    for _, name in ipairs(GENESIS) do
        local f = io.open(stats_path(name), "r")
        if f == nil then
            gate.fail(
                "steady serving: the stats file for load-"
                    .. name
                    .. " does not exist — the client fleet is not running"
            )
        end
        f:close()
        local count = 0
        local data = fs.read_all(stats_path(name))
        for line in data:gmatch("[^\r\n]+") do
            local first = line:match('"count":(%d+)')
            if first ~= nil then
                count = (tonumber(first) or 0) as integer
            end
        end
        if count == 0 then
            gate.fail(
                "steady serving: load-" .. name .. " recorded count=0 over "
                    .. STEADY_S .. " s — the client is not chasing the lock"
            )
        end
    end
    if err > 0 then
        gate.fail(
            "steady serving recorded " .. err .. " client errors in the "
                .. "phase window; the calm profile demands zero"
        )
    end
    steady_err_begin = begin_err
    measure(
        "scenario=S0 steady_s=" .. STEADY_S .. " client_errors=0 view_depth="
            .. view_depth()
            .. " states=" .. states_snapshot()
    )
end

-- The partition pair: P1 the follower cut (the serving 2-of-3
-- continues), P2 the leader cut (the timeout-driven takeover on the
-- partitioned view). Injection is process suspension (SIGSTOP); the
-- heal is SIGCONT and its backlog burst is the delayed-delivery data.
local function scenario_partitions()
    phase("P: network partitions (follower cut, leader cut)")
    local leader = leader_name()
    if leader == "" then
        gate.fail("P: no leader before the partitions")
    end
    local follower = ""
    for _, name in ipairs(GENESIS) do
        if name ~= leader and follower == "" then
            follower = name
        end
    end
    local other = ""
    for _, name in ipairs(GENESIS) do
        if name ~= leader and name ~= follower then
            other = name
        end
    end

    -- P1: cut the follower.
    anchor(
        "P1-begin target=" .. follower .. " leader=" .. leader
            .. " states=" .. states_snapshot()
    )
    local t0_err = total_errors()
    local t_cut = anchor("P1-STOP " .. follower)
    proc.suspend(procs[follower] as proc.Process)
    clock.sleep(20)
    local cont_ok, cont_landed = probe_serving(other, 2, 20000)
    local t_err = total_errors()
    anchor(
        "P1-cut-window-done " .. follower
            .. " serving_continued=" .. tostring(cont_ok)
            .. " states=" .. states_snapshot()
    )
    local p1_heal = anchor("P1-CONT " .. follower)
    proc.resume(procs[follower] as proc.Process)
    local f_drops_before = drop_reasons(follower)
    local p1_lc_before = line_count(leader, "leader change")
    local p1_lf_before = line_count(follower, "leader change")
    local ok_rejoin, rejoin_report = wait_node_normal(
        follower, p1_heal, REJOIN_DEADLINE_MS
    )
    if not ok_rejoin then
        gate.fail("P1: " .. follower .. " did not rejoin after the heal: " .. rejoin_report)
    end
    local t_rejoined = anchor("P1-rejoined " .. follower .. " " .. rejoin_report)
    local f_drops_after = drop_reasons(follower)
    local f_storm: {string} = {}
    for reason, before in pairs(f_drops_before) do
        local after = f_drops_after[reason] or 0
        if after - before > 0 then
            f_storm[#f_storm + 1] = reason .. "=" .. (after - before)
        end
    end
    measure(
        "scenario=P1 node=" .. follower .. " leader=" .. leader
            .. " serving_continued=" .. (cont_ok and "1" or "0")
            .. " first_ok_gap_ms="
            .. (cont_landed >= 0 and tostring(cont_landed - t_cut) or "-1")
            .. " client_errors_in_window=" .. (t_err - t0_err)
            .. " heal_rejoin_ms=" .. (t_rejoined - p1_heal)
            .. " leader_changes_in_window="
            .. (line_count(leader, "leader change") - p1_lc_before)
            .. " follower_changes_in_window="
            .. (line_count(follower, "leader change") - p1_lf_before)
            .. " healed_drops " .. table.concat(f_storm, " ")
            .. " states=" .. states_snapshot()
    )
    clock.sleep(8)

    -- P2: cut the leader.
    leader = leader_name()
    if leader == "" then
        gate.fail("P2: no leader before the leader cut")
    end
    local survivor = live_probe_target(leader)
    anchor(
        "P2-begin target=" .. leader .. " states=" .. states_snapshot()
    )
    local t0_err2 = total_errors()
    local p2_cut = anchor("P2-STOP " .. leader)
    proc.suspend(procs[leader] as proc.Process)
    local ok_takeover, landed = probe_serving(survivor, 1, 30000)
    if not ok_takeover then
        gate.fail(
            "P2: no client operation served within 30 s of the leader cut"
        )
    end
    local takeover_ms = landed - p2_cut
    local t_err2 = total_errors()
    local new_leader = ""
    for _, name in ipairs(GENESIS) do
        if name ~= leader and procs[name] ~= nil then
            local kv = status_at(name, p2_cut)
            if
                kv.state == "normal" and (tonumber(kv.leader) or 0) == by_name[name].id
            then
                new_leader = name
            end
        end
    end
    local lc_before = line_count(new_leader ~= "" and new_leader or survivor, "leader change")
    anchor(
        "P2-takeover new_leader=" .. new_leader .. " states=" .. states_snapshot()
    )
    local p2_heal = anchor("P2-CONT " .. leader)
    proc.resume(procs[leader] as proc.Process)
    local old_drops_before = drop_reasons(leader)
    local ok2, report2 = wait_node_normal(leader, p2_heal, REJOIN_DEADLINE_MS)
    if not ok2 then
        gate.fail("P2: " .. leader .. " did not rejoin after the heal: " .. report2)
    end
    local t_rejoined2 = anchor("P2-rejoined " .. leader .. " " .. report2)
    local old_drops_after = drop_reasons(leader)
    local storm_parts: {string} = {}
    for reason, before in pairs(old_drops_before) do
        local after = old_drops_after[reason] or 0
        if after - before > 0 then
            storm_parts[#storm_parts + 1] = reason
                .. "="
                .. (after - before)
        end
    end
    measure(
        "scenario=P2 node=" .. leader .. " takeover_ms=" .. takeover_ms
            .. " new_leader=" .. new_leader
            .. " client_errors_in_window=" .. (t_err2 - t0_err2)
            .. " heal_rejoin_ms=" .. (t_rejoined2 - p2_heal)
            .. " leader_changes_on_new_leader_in_window="
            .. (line_count(new_leader ~= "" and new_leader or survivor, "leader change") - lc_before)
            .. " healed_drops " .. table.concat(storm_parts, " ")
            .. " states=" .. states_snapshot()
    )
    progress("P2: the partitioned leader rejoined; drop deltas recorded")
    clock.sleep(8)
end

-- The SIGKILL crash family, non-leaders first, the leader LAST: the
-- dirty boot (identity bump, reincarnation announcement, remap), the
-- 150 s rejoin budget, superblock snapshots before the restart and at
-- the settle, and the BLOCKED path (bounded rejoin: evidence captured,
-- node parked, run continues).
local function scenario_crashes(tag: string)
    local leader = leader_name()
    if leader == "" then
        gate.fail("C(" .. tag .. "): no leader before the crash family")
    end
    local order: {string} = {}
    for _, name in ipairs(GENESIS) do
        if name ~= leader then
            order[#order + 1] = name
        end
    end
    order[#order + 1] = leader
    for i, name in ipairs(order) do
        local p = procs[name]
        if p == nil then
            gate.fail("C(" .. tag .. "): " .. name .. " has no process record")
        end
        local label = tag .. "-k" .. i .. "-" .. name
        anchor(
            "C-begin tag=" .. tag .. " target=" .. name
                .. " view_depth=" .. view_depth()
                .. " states=" .. states_snapshot()
        )
        snapshot_state("pre-restart-" .. label)
        proc.kill(p as proc.Process)
        proc.wait(p as proc.Process)
        procs[name] = nil
        anchor("C-SIGKILL-done tag=" .. tag .. " " .. name)
        -- The cluster keeps serving across a non-leader crash; for the
        -- leader the survivors detect under the detector.
        probe_serving(live_probe_target(name), 1, 30000)
        clock.sleep(2)
        local remaps_before = 0
        for _, other in ipairs(GENESIS) do
            if other ~= name then
                remaps_before = remaps_before + line_count(other, "remap ")
            end
        end
        local t_boot = anchor("C-restart " .. name)
        start_node(name)
        local ok, report = wait_node_normal(name, t_boot, REJOIN_DEADLINE_MS)
        local bumped = bumped_boot(name, t_boot)
        local remaps = 0
        for _, other in ipairs(GENESIS) do
            if other ~= name then
                remaps = remaps + line_count(other, "remap ")
            end
        end
        remaps = remaps - remaps_before
        if not ok then
            -- The bounded rejoin: the fence, the boot note, the remap
            -- count and the newest named diagnostic ARE the evidence;
            -- the node is parked so the serving remainder carries the
            -- ladder on.
            snapshot_state("blocked-" .. label)
            anchor(
                "C-rejoin-BLOCKED tag=" .. tag .. " " .. name
                    .. " states=" .. states_snapshot()
            )
            measure(
                "scenario=C tag=" .. tag .. " node=" .. name
                    .. " crash_restart_to_serving_ms=BLOCKED"
                    .. " rejoin_ms=BLOCKED view_depth_at_stuck=" .. view_depth()
                    .. " "
                    .. (bumped ~= "" and bumped or "no_bump_seen")
                    .. " remap_notices=" .. remaps
                    .. " stuck_status=" .. report
                    .. " newest_diagnostic=" .. newest_diagnostic(name)
                    .. " states=" .. states_snapshot()
            )
            progress(
                "C(" .. tag .. ") " .. name .. ": the crashed rejoin stayed "
                    .. "fenced (blocked, recorded, node parked)"
            )
            term_node(name, 2000)
            snapshot_state("parked-" .. label)
            clock.sleep(5)
        else
            local t_serving = anchor("C-serving " .. name .. " " .. report)
            snapshot_state("post-settle-" .. label)
            measure(
                "scenario=C tag=" .. tag .. " node=" .. name
                    .. " crash_restart_to_serving_ms=" .. (t_serving - t_boot)
                    .. " "
                    .. (bumped ~= "" and bumped or "no_bump_seen")
                    .. " remap_notices=" .. remaps
                    .. " states=" .. states_snapshot()
            )
            clock.sleep(8)
        end
    end
end

-- The compound: the leader SIGKILLed AND one survivor partitioned in
-- the same window (no quorum), the partition heals into a two-voter
-- view change, and the crashed leader restarts into the resulting
-- higher view. Measured: the no-quorum window, the storm (leader-change
-- records and the view climb), the recovery, the duplicate suppression
-- (late-ack drains), and the restarted leader's rejoin.
local function scenario_compound()
    phase("X: compound leader SIGKILL + survivor partition")
    local leader = leader_name()
    if leader == "" then
        gate.fail("X: no leader before the compound")
    end
    local partitioned = ""
    for _, name in ipairs(GENESIS) do
        if name ~= leader and partitioned == "" then
            partitioned = name
        end
    end
    anchor(
        "X-begin leader=" .. leader .. " partitioned=" .. partitioned
            .. " view_depth=" .. view_depth()
            .. " states=" .. states_snapshot()
    )
    local t0_err = total_errors()
    anchor("X-SIGKILL " .. leader)
    local lp = procs[leader] as proc.Process
    proc.kill(lp)
    proc.wait(lp)
    procs[leader] = nil
    local pp = procs[partitioned] as proc.Process
    local t_cut = anchor("X-STOP " .. partitioned)
    proc.suspend(pp)
    -- The no-quorum window: nothing serves (one live voter, no quorum).
    local ok_noquorum, _landed = probe_serving(partitioned, 1, 15000)
    local t_err = total_errors()
    anchor(
        "X-no-quorum-window measured=" .. (ok_noquorum and "0" or "1")
            .. " states=" .. states_snapshot()
    )
    local x_heal = anchor("X-CONT " .. partitioned)
    proc.resume(pp)
    local lc_before = line_count(partitioned, "leader change")
    local healed_drops_before = drop_reasons(partitioned)
    local ok_elect, elect_report = wait_quorum(2, LEADER_DEADLINE_MS)
    if not ok_elect then
        gate.fail("X: the healed pair did not elect: " .. elect_report)
    end
    local t_elect = anchor("X-elected " .. elect_report .. " states=" .. states_snapshot())
    local ok_probe, probe_landed = probe_serving(partitioned, 1, 30000)
    if not ok_probe then
        gate.fail("X: no client operation served after the heal")
    end
    local t_err2 = total_errors()
    local healed_drops_after = drop_reasons(partitioned)
    measure(
        "scenario=X leader=" .. leader .. " partitioned=" .. partitioned
            .. " no_quorum_serving=0"
            .. " no_quorum_window_ms=" .. (x_heal - t_cut)
            .. " client_errors_in_noquorum_window=" .. (t_err - t0_err)
            .. " elect_ms=" .. (t_elect - x_heal)
            .. " first_serve_ms=" .. (probe_landed - x_heal)
            .. " client_errors_in_heal_window=" .. (t_err2 - t_err)
            .. " leader_changes_on_healed_in_window="
            .. (line_count(partitioned, "leader change") - lc_before)
            .. " states=" .. states_snapshot()
    )
    local function storm_line(k: string, before: integer, after: integer): string
        return k .. "=" .. (after - before)
    end
    local storm_parts: {string} = {}
    for reason, before in pairs(healed_drops_before) do
        local after = healed_drops_after[reason] or 0
        if after - before > 0 then
            storm_parts[#storm_parts + 1] = storm_line(reason, before, after)
        end
    end
    measure("scenario=X healed_drops " .. table.concat(storm_parts, " "))
    -- The crashed leader restarts into the higher view.
    clock.sleep(5)
    local lateacks_before = 0
    for _, name in ipairs(GENESIS) do
        lateacks_before = lateacks_before + line_count(name, "late ack")
    end
    local t_boot = anchor("X-restart " .. leader)
    start_node(leader)
    local ok_rejoin, rejoin_report = wait_node_normal(leader, t_boot, REJOIN_DEADLINE_MS)
    local lateacks = 0
    for _, name in ipairs(GENESIS) do
        lateacks = lateacks + line_count(name, "late ack")
    end
    lateacks = lateacks - lateacks_before
    local leader_drops_before = drop_reasons(leader)
    if not ok_rejoin then
        snapshot_state("blocked-X-" .. leader)
        anchor(
            "X-rejoin-BLOCKED " .. leader .. " states=" .. states_snapshot()
        )
        measure(
            "scenario=X node=" .. leader .. " rejoin_ms=BLOCKED"
                .. " " .. (bumped_boot(leader, t_boot) ~= "" and bumped_boot(leader, t_boot) or "no_bump_seen")
                .. " stuck_status=" .. rejoin_report
                .. " newest_diagnostic=" .. newest_diagnostic(leader)
                .. " late_ack_drains_in_window=" .. lateacks
                .. " states=" .. states_snapshot()
        )
        progress("X: the crashed leader's rejoin stayed fenced (blocked, recorded)")
        term_node(leader, 2000)
        clock.sleep(5)
    else
        local t_rejoined = anchor("X-rejoined " .. leader .. " " .. rejoin_report)
        local leader_drops_after = drop_reasons(leader)
        local rejoined_storm: {string} = {}
        for reason, before in pairs(leader_drops_before) do
            local after = leader_drops_after[reason] or 0
            if after - before > 0 then
                rejoined_storm[#rejoined_storm + 1] = storm_line(reason, before, after)
            end
        end
        measure(
            "scenario=X node=" .. leader .. " rejoin_ms=" .. (t_rejoined - t_boot)
                .. " " .. (bumped_boot(leader, t_boot) ~= "" and bumped_boot(leader, t_boot) or "no_bump_seen")
                .. " late_ack_drains_in_window=" .. lateacks
                .. " rejoined_drops " .. table.concat(rejoined_storm, " ")
                .. " states=" .. states_snapshot()
        )
        clock.sleep(8)
    end
end

-- Abdication under load: three abdication cycles while the aggressive
-- hammer hammers the SAME lock — failover per cycle and the
-- client-visible error count inside the window. The settle gate counts
-- the LIVE voters (a parked member must not fail the gate).
local function scenario_abdication_under_load()
    phase("A: abdication under load (aggressive hammer on the same lock)")
    start_hammer()
    clock.sleep(10)
    local live: {string} = {}
    for _, name in ipairs(GENESIS) do
        local p = procs[name]
        if p ~= nil and proc.alive(p) then
            live[#live + 1] = name
        end
    end
    for cycle = 1, 3 do
        local leader = leader_name()
        if leader == "" then
            gate.fail("A cycle " .. cycle .. ": no leader to abdicate")
        end
        local t0_err = total_errors()
        local h0_err = hammer_errors()
        local t0 = anchor(
            "A-begin cycle=" .. cycle .. " leader=" .. leader
                .. " view_depth=" .. view_depth()
                .. " live=" .. #live
                .. " states=" .. states_snapshot()
        )
        local reply, drive_ms = drive_verb(leader, "abdicate", 0, {})
        anchor("A-abdicate-drive cycle=" .. cycle .. " reply=" .. reply)
        if not reply:find('"accepted":true', 1, true) then
            gate.fail("A cycle " .. cycle .. ": abdication refused: " .. reply)
        end
        local target = live_probe_target("")
        local ok, landed = probe_serving(target, 1, 30000)
        if not ok then
            gate.fail(
                "A cycle " .. cycle .. ": no client operation served within 30 s"
            )
        end
        local failover_ms = landed - t0
        local ok_conv, conv_report = wait_quorum(#live, LEADER_DEADLINE_MS)
        if not ok_conv then
            gate.fail("A cycle " .. cycle .. ": the view did not settle: " .. conv_report)
        end
        local t_err = total_errors()
        local h_err = hammer_errors()
        measure(
            "scenario=A cycle=" .. cycle .. " abdication_leader=" .. leader
                .. " drive_ms=" .. drive_ms
                .. " failover_ms=" .. failover_ms
                .. " polite_errors_in_window=" .. (t_err - t0_err)
                .. " hammer_errors_in_window=" .. (h_err - h0_err)
                .. " states=" .. states_snapshot()
        )
        clock.sleep(5)
    end
    signal_procs(hammer_procs, "USR1")
    clock.sleep(2)
    for _, p in ipairs(hammer_procs) do
        if proc.alive(p) then
            proc.stop(p, 1000)
        end
    end
    progress("A: hammer silenced and stopped; the polite fleet continues")
    clock.sleep(5)
end

-- The deep-view conditioning: abdication cycles drive the view high
-- (the polite-2 S4 condition), measured; then the deep crash family.
local function scenario_deep_family()
    phase("D: deep-view conditioning (abdication cycles) + deep crash family")
    local v0 = view_depth()
    local t0 = anchor("D-conditioning-begin view_depth=" .. v0)
    local lc_before = line_count(GENESIS[1], "leader change")
    local cycles = 0
    while view_depth() < DEEP_VIEW_TARGET and cycles < 200 do
        local reply, _ms = drive_verb_retry("abdicate", 0, {}, 30000)
        if not reply:find('"accepted":true', 1, true) then
            gate.fail(
                "D: abdication refused at cycle " .. cycles .. ": " .. reply
            )
        end
        cycles = cycles + 1
        clock.sleep(0.5)
    end
    local v1 = view_depth()
    if v1 < DEEP_VIEW_TARGET then
        gate.fail(
            "D: the conditioning only reached view " .. v1 .. " (target "
                .. DEEP_VIEW_TARGET .. ")"
        )
    end
    anchor(
        "D-conditioning-done cycles=" .. cycles .. " view_depth=" .. v1
            .. " states=" .. states_snapshot()
    )
    measure(
        "scenario=D conditioning_cycles=" .. cycles
            .. " view_before=" .. v0
            .. " view_after=" .. v1
            .. " leader_changes_on_n1_in_window="
            .. (line_count(GENESIS[1], "leader change") - lc_before)
            .. " conditioning_ms=" .. (clock.now_ms() - t0)
    )
    clock.sleep(5)
    scenario_crashes("deep")
end

-- The fresh-files boundary between the crash family and the rest of
-- the lane: all processes down, the run's snapshot taken (the crash
-- evidence preserved), the durable state wiped, the cluster booted
-- fresh. The polite fleet is restarted on the fresh cluster.
local function fresh_files_boundary(label: string)
    phase("FF: fresh-files boundary (" .. label .. ")")
    anchor("FF-begin label=" .. label .. " states=" .. states_snapshot())
    for _, name in ipairs(GENESIS) do
        if procs[name] ~= nil then
            anchor("FF-TERM-begin " .. name)
            term_node(name, 2000)
            anchor("FF-TERM-done " .. name)
        end
    end
    local snap_ok, _out, snap_msg = snapshot.snapshot(run_dir, "")
    if not snap_ok then
        gate.fail("the fresh-files boundary's snapshot failed: " .. snap_msg)
    end
    fs.remove_tree(fs.join(run_dir, "state"))
    fs.mkdir_p(fs.join(run_dir, "state"))
    for _, name in ipairs(GENESIS) do
        anchor("FF-boot " .. name)
        start_node(name)
    end
    local ok_conv, conv_report = wait_converged("", LEADER_DEADLINE_MS)
    if not ok_conv then
        gate.fail("the fresh boot did not converge: " .. conv_report)
    end
    anchor(
        "FF-converged " .. conv_report .. " states=" .. states_snapshot()
    )
    clock.sleep(5)
end

-- The whole-cluster stop/start passes: pass 1 the durable resurrection
-- from the post-crash deep state (the divergence observation), pass 2
-- the fresh reboot after a snapshot-gated reprovision.
local function scenario_full_cycle()
    phase("S5: whole-cluster stop/start and full reboot")
    anchor("S5-begin pass=1 gap_s=10 states=" .. states_snapshot())
    for _, name in ipairs(GENESIS) do
        anchor("S5-TERM-begin pass=1 " .. name)
        term_node(name, 2000)
        anchor("S5-TERM-done pass=1 " .. name)
    end
    snapshot_state("pre-restart-s5-pass1")
    local t_all_down = anchor("S5-all-down pass=1")
    clock.sleep(10)
    local t_restart = anchor("S5-restart-all pass=1")
    for _, name in ipairs(GENESIS) do
        start_node(name)
    end
    local ok, report = wait_converged("", 120000)
    if ok then
        local t_conv = anchor("S5-converged pass=1 " .. report)
        local ok2, first_op = probe_serving("n1", 1, 30000)
        if not ok2 then
            gate.fail(
                "S5 pass 1: no client operation served within 30 s of the restart"
            )
        end
        snapshot_state("post-settle-s5-pass1")
        measure(
            "scenario=S5 pass=1"
                .. " down_ms=" .. (t_restart - t_all_down)
                .. " first_op_ms=" .. (first_op - t_restart)
                .. " converge_ms=" .. (t_conv - t_restart)
                .. " states=" .. states_snapshot()
        )
    else
        snapshot_state("blocked-s5-pass1")
        anchor(
            "S5-converge-BLOCKED pass=1 states=" .. states_snapshot()
        )
        measure(
            "scenario=S5 pass=1 converge=BLOCKED"
                .. " down_ms=" .. (t_restart - t_all_down)
                .. " stuck_report=" .. report
                .. " states=" .. states_snapshot()
        )
        progress(
            "S5 pass 1: the durable resurrection did not converge (blocked, recorded)"
        )
        for _, name in ipairs(GENESIS) do
            if procs[name] ~= nil then
                term_node(name, 2000)
            end
        end
        local snap_ok, _out, snap_msg = snapshot.snapshot(run_dir, "")
        if not snap_ok then
            gate.fail("the state reprovision's snapshot failed: " .. snap_msg)
        end
        fs.remove_tree(fs.join(run_dir, "state"))
        fs.mkdir_p(fs.join(run_dir, "state"))
    end
    clock.sleep(5)
    anchor("S5-begin pass=2 gap_s=30")
    for _, name in ipairs(GENESIS) do
        anchor("S5-TERM-begin pass=2 " .. name)
        term_node(name, 2000)
        anchor("S5-TERM-done pass=2 " .. name)
    end
    local t_all_down_b = anchor("S5-all-down pass=2")
    clock.sleep(30)
    local t_restart_b = anchor("S5-restart-all pass=2")
    for _, name in ipairs(GENESIS) do
        start_node(name)
    end
    local ok_b, report_b = wait_converged("", 120000)
    if not ok_b then
        gate.fail("S5 pass 2: the cluster did not converge: " .. report_b)
    end
    local t_conv_b = anchor("S5-converged pass=2 " .. report_b)
    local ok2b, first_op_b = probe_serving("n1", 1, 30000)
    if not ok2b then
        gate.fail("S5 pass 2: no client operation served within 30 s of the restart")
    end
    snapshot_state("post-settle-s5-pass2")
    measure(
        "scenario=S5 pass=2"
            .. " down_ms=" .. (t_restart_b - t_all_down_b)
            .. " first_op_ms=" .. (first_op_b - t_restart_b)
            .. " converge_ms=" .. (t_conv_b - t_restart_b)
            .. " states=" .. states_snapshot()
    )
    clock.sleep(10)
end

-- ------------------------------------------------------------ teardown --

local function teardown()
    phase("T: teardown")
    anchor("teardown-begin")
    signal_clients("USR1")
    signal_procs(hammer_procs, "USR1")
    clock.sleep(1)
    local all_procs: {proc.Process} = {}
    for _, p in pairs(load_procs) do
        all_procs[#all_procs + 1] = p
    end
    for _, p in ipairs(hammer_procs) do
        all_procs[#all_procs + 1] = p
    end
    for _, p in ipairs(all_procs) do
        if proc.alive(p) then
            proc.stop(p, 1000)
        end
    end
    for _, name in ipairs(GENESIS) do
        if procs[name] ~= nil then
            anchor("T-TERM-begin " .. name)
            local code = term_node(name, 2000)
            anchor("T-TERM-done " .. name .. " exit=" .. code)
        end
    end
    anchor("teardown-complete total_client_errors=" .. total_errors())
    progress("teardown complete")
end

-- ---------------------------------------------------------------- main --

function M.run(root_path: string, run_dir_path: string): integer
    root = root_path
    run_dir = run_dir_path
    parse_specs(
        fs.join(root, "examples/lease-sequencer/config/cluster-softball2.jsonl")
    )
    local ok = snapshot.snapshot_then_wipe(run_dir)
    if not ok then
        gate.fail("the run stops: the old run state could not be snapshotted")
    end
    fs.mkdir_p(fs.join(run_dir, "logs"))
    fs.mkdir_p(fs.join(run_dir, "state"))
    fs.mkdir_p(fs.join(run_dir, "flight"))
    fs.mkdir_p(fs.join(run_dir, "stats"))
    fs.write_all(fs.join(run_dir, "pids"), "")
    fs.write_all(fs.join(run_dir, "anchors.md"), "")
    fs.write_all(fs.join(run_dir, "progress.log"), "")
    fs.write_all(
        fs.join(run_dir, "anchors.md"),
        "# local-softball8-2026-09-19 — the crash/partition run (item25.30)\n"
            .. "# knobs: heartbeat 5ms, election 3000ms (sloppy detector 3000-5000ms), recovery 3000ms\n"
            .. "# binary: flight-recorder build; load: lease-load polite x3 + aggressive hammer windows\n"
            .. "# partition injection: SIGSTOP/SIGCONT process suspension (loopback; heal = backlog burst)\n"
    )
    anchor("run-begin softball8 crash/partition lane")
    progress("run begin: " .. states_snapshot())

    phase("B: boot")
    for _, name in ipairs(GENESIS) do
        anchor("boot " .. name)
        start_node(name)
    end
    local ok_conv, conv_report = wait_converged("", LEADER_DEADLINE_MS)
    if not ok_conv then
        gate.fail("the cluster did not converge: " .. conv_report)
    end
    anchor("converged " .. conv_report .. " states=" .. states_snapshot())
    -- The serving confirmation comes BEFORE the clients start: the
    -- prober's first ops must land on a settled leader (the abort-6
    -- finding: two join-walk errors at the prober's first ops).
    probe_serving("n1", 2, 60000)
    phase("S: clients start")
    probe_epoch = anchor("USR2 start all clients")
    start_clients()
    -- The 1 s settle: the client's SIGUSR2 handler (signal-hook) must be
    -- installed before the start signal — an early USR2 is the default
    -- action (terminate), the abort-3 finding.
    clock.sleep(1)
    signal_clients("USR2")
    verify_clients_started()
    anchor("steady-serving-begin")

    scenario_steady()
    scenario_crashes("shallow")
    fresh_files_boundary("after the shallow crash family")
    scenario_deep_family()
    scenario_full_cycle()
    scenario_partitions()
    scenario_compound()
    fresh_files_boundary("after the compound (the parked leader restored)")
    scenario_abdication_under_load()

    anchor("ladder-complete states=" .. states_snapshot())
    teardown()
    return 0
end

return M
