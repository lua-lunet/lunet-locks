-- The polite-2 maintenance run (typed runner library): the six-scenario
-- disruption ladder from docs/src/softball-run.md under one continuous
-- polite client fleet, every action anchored live (epoch millisecond
-- before the action, into anchors.md), every scenario's durations and
-- states captured as key=value measure lines in the same anchors file.
-- Cluster: three genesis voters on loopback plus one spare joiner row for
-- the controlled reconfig swap. Knobs: the calm profile (heartbeat 5 ms,
-- election 3000 ms, recovery 3000 ms — the randomized leader timeout
-- 3000–5000 ms) on the flight-recorder build. Every stage is bounded by
-- its own gate deadline; a failed gate stops the run loudly.

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
    maintenance: function(string, string): integer
end

local GENESIS = {"n1", "n2", "n3"}
local SPARE = "n4"
local LEASE_LOCK = 14531090
local STREAM_LOCK = 9201
local LEADER_DEADLINE_MS = 60000
local REJOIN_DEADLINE_MS = 150000

local root = ""
local run_dir = ""
local specs: {Spec} = {}
local by_name: {string: Spec} = {}
local procs: {string: proc.Process} = {}
local load_procs: {string: proc.Process} = {}
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

-- The states every voter holds right now, one capture string.
local function states_snapshot(): string
    local parts: {string} = {}
    for _, name in ipairs(GENESIS) do
        local p = procs[name]
        if p ~= nil and proc.alive(p) then
            local kv = status_at(name, 0)
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

-- ----------------------------------------------------------- mutators --

local function start_node(name: string): integer
    local spec = by_name[name]
    local argv: {string} = {
        fs.join(root, "examples/lease-sequencer/target/release/lease-sequencer"),
        "--name",
        spec.name,
        "--config",
        fs.join(root, "examples/lease-sequencer/config/cluster-polite2.jsonl"),
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
-- healthy node — the adapter's stop contract exits on its own).
local function term_node(name: string, grace_ms: integer): integer
    local p = procs[name]
    if p == nil then
        return 0
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
            "2700",
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

local function signal_clients(signal_name: string)
    for _, p in pairs(load_procs) do
        if proc.alive(p) then
            shell.status("kill -" .. signal_name .. " " .. p.pid .. " 2>/dev/null")
        end
    end
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

-- Every live genesis voter sits at state=normal with one named leader
-- (expected ~= "" pins which).
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
-- fresh message id; the run retries at the next replica on not_leader
-- exactly like every admin drive before it). Returns the reply line and
-- the drive's wall duration in ms.
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
-- deadline expires. The runner's version of the smoke's drive_admin.
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
local function cumulative_errors(name: string): integer
    local data = fs.read_all(fs.join(run_dir, "stats", "load-" .. name .. ".jsonl"))
    local last = 0
    for value in data:gmatch('"errors":(%d+)') do
        last = tonumber(value) as integer
    end
    return last
end

local function total_errors(): integer
    local n = 0
    for _, name in ipairs(GENESIS) do
        n = n + cumulative_errors(name)
    end
    return n
end

-- ------------------------------------------------------ the scenarios --

-- The steady serving window: the polite profile under zero disruptions.
local function scenario_steady()
    phase("steady serving 240 s")
    clock.sleep(240)
    local err = total_errors()
    if err > 0 then
        gate.fail(
            "steady serving recorded "
                .. err
                .. " client errors; the calm profile demands zero"
        )
    end
    measure("scenario=S0 steady_s=240 client_errors=0 states=" .. states_snapshot())
end

local function scenario_rotation()
    phase("S1: security-patch rotation through the non-leaders")
    local leader = leader_name()
    if leader == "" then
        gate.fail("S1: no leader before the rotation")
    end
    for _, name in ipairs(GENESIS) do
        if name ~= leader then
            anchor("S1-begin target=" .. name .. " leader=" .. leader)
            local t_term = anchor("S1-TERM-begin " .. name)
            local code = term_node(name, 2000)
            local t_gone = anchor("S1-TERM-done " .. name .. " exit=" .. code)
            local cont_ok, first_op = probe_serving(leader, 2, 20000)
            progress(
                "S1 "
                    .. name
                    .. ": serving continued="
                    .. tostring(cont_ok)
                    .. " first_ok_rel="
                    .. (first_op >= 0 and tostring(first_op - t_term) or "none")
            )
            clock.sleep(2)
            local t_boot = anchor("S1-restart " .. name)
            start_node(name)
            local ok, report = wait_node_normal(name, t_boot, REJOIN_DEADLINE_MS)
            if not ok then
                gate.fail("S1: " .. name .. " did not rejoin normal: " .. report)
            end
            local t_rejoined = anchor("S1-rejoined " .. name .. " " .. report)
            measure(
                "scenario=S1 node="
                    .. name
                    .. " stop_ms="
                    .. (t_gone - t_term)
                    .. " rejoin_ms="
                    .. (t_rejoined - t_boot)
                    .. " serving_continued="
                    .. (cont_ok and "1" or "0")
                    .. " first_ok_gap_ms="
                    .. (first_op >= 0 and tostring(first_op - t_term) or "-1")
            )
            clock.sleep(8)
        end
    end
end

-- Scenario 2: leader abdication cycled ×3. Each cycle measures
-- abdication-to-new-leader-serving (the failover lower bound, independent
-- of failure detection) and the full settle (every voter normal).
local function scenario_abdication()
    phase("S2: leader abdication x3 (leadership rotation)")
    local led: {string: string} = {}
    for cycle = 1, 3 do
        local leader = leader_name()
        if leader == "" then
            gate.fail("S2 cycle " .. cycle .. ": no leader to abdicate")
        end
        led[leader] = "cycle" .. cycle
        local states_before = states_snapshot()
        local t0 = anchor(
            "S2-begin cycle="
                .. cycle
                .. " leader="
                .. leader
                .. " states="
                .. states_before
        )
        local reply, drive_ms = drive_verb(leader, "abdicate", 0, {})
        anchor("S2-abdicate-drive cycle=" .. cycle .. " reply=" .. reply)
        if not reply:find('"accepted":true', 1, true) then
            gate.fail("S2 cycle " .. cycle .. ": abdication refused: " .. reply)
        end
        local ok, landed = probe_serving(leader, 1, 30000)
        if not ok then
            gate.fail(
                "S2 cycle "
                    .. cycle
                    .. ": no client operation served within 30 s of the abdication"
            )
        end
        local failover_ms = landed - t0
        local ok_conv, conv_report = wait_converged("", LEADER_DEADLINE_MS)
        if not ok_conv then
            gate.fail(
                "S2 cycle " .. cycle .. ": the view did not settle: " .. conv_report
            )
        end
        local t_settled = anchor(
            "S2-settled cycle=" .. cycle .. " states=" .. states_snapshot()
        )
        measure(
            "scenario=S2 cycle="
                .. cycle
                .. " abdication_leader="
                .. leader
                .. " drive_ms="
                .. drive_ms
                .. " failover_ms="
                .. failover_ms
                .. " settle_ms="
                .. (t_settled - t0)
        )
        clock.sleep(12)
    end
    local held: {string} = {}
    for _, name in ipairs(GENESIS) do
        held[#held + 1] = name .. "=" .. (led[name] or "never")
    end
    for _, name in ipairs(GENESIS) do
        if led[name] == nil then
            gate.fail(
                "S2: "
                    .. name
                    .. " never held leadership across the 3 cycles: "
                    .. table.concat(held, " ")
            )
        end
    end
    progress("S2 leadership rotation complete: " .. table.concat(held, " "))
end

-- Scenario 3: clean TERM stop/start of the leader, the timeout-driven
-- takeover baseline (no abdication): detection + takeover under the
-- detector, then the old leader's rejoin.
local function scenario_leader_term()
    phase("S3: leader stop/start (timeout-driven takeover baseline)")
    local leader = leader_name()
    if leader == "" then
        gate.fail("S3: no leader to stop")
    end
    anchor("S3-begin target=" .. leader .. " (the leader)")
    local t_term = anchor("S3-TERM-begin " .. leader)
    local code = term_node(leader, 2000)
    local t_gone = anchor("S3-TERM-done " .. leader .. " exit=" .. code)
    local target_probe = live_probe_target(leader)
    local ok, landed = probe_serving(target_probe, 1, 30000)
    if not ok then
        gate.fail("S3: no client operation served within 30 s of the leader stop")
    end
    local takeover_ms = landed - t_term
    local new_leader = ""
    for _, name in ipairs(GENESIS) do
        if name ~= leader and procs[name] ~= nil and proc.alive(procs[name]) then
            local kv = status_at(name, t_term)
            if
                kv.state == "normal" and (tonumber(kv.leader) or 0) == by_name[name].id
            then
                new_leader = name
            end
        end
    end
    progress(
        "S3: takeover under the detector in "
            .. takeover_ms
            .. " ms; new leader "
            .. new_leader
    )
    clock.sleep(2)
    local t_boot = anchor("S3-restart " .. leader)
    start_node(leader)
    local ok2, report2 = wait_node_normal(leader, t_boot, REJOIN_DEADLINE_MS)
    if not ok2 then
        gate.fail("S3: " .. leader .. " did not rejoin: " .. report2)
    end
    local t_rejoined = anchor("S3-rejoined " .. leader .. " " .. report2)
    measure(
        "scenario=S3 node="
            .. leader
            .. " stop_ms="
            .. (t_gone - t_term)
            .. " takeover_ms="
            .. takeover_ms
            .. " new_leader="
            .. new_leader
            .. " rejoin_ms="
            .. (t_rejoined - t_boot)
    )
    clock.sleep(10)
end

local function scenario_crashes()
    phase("S4: crash kills (SIGKILL), non-leaders first, leader last")
    local leader = leader_name()
    local order: {string} = {}
    for _, name in ipairs(GENESIS) do
        if name ~= leader then
            order[#order + 1] = name
        end
    end
    order[#order + 1] = leader
    for _, name in ipairs(order) do
        local p = procs[name]
        if p == nil then
            gate.fail("S4: " .. name .. " has no process record")
        end
        anchor("S4-begin target=" .. name)
        proc.kill(p)
        proc.wait(p)
        procs[name] = nil
        anchor("S4-SIGKILL-done " .. name)
        -- The cluster keeps serving across a non-leader crash; for the
        -- leader the survivors detect under the detector.
        probe_serving(live_probe_target(name), 1, 30000)
        clock.sleep(2)
        local t_boot = anchor("S4-restart " .. name)
        start_node(name)
        local ok, report = wait_node_normal(name, t_boot, REJOIN_DEADLINE_MS)
        -- The dirty-boot evidence: the bumped boot note and the remap
        -- notices at the peers.
        local bumped = ""
        for _, line in ipairs(logs.grep(node_log_files(name), "boot name=")) do
            local kv = logs.kv(line)
            if
                (tonumber(kv.ts) or 0) >= t_boot
                and (tonumber(kv.incarnation) or 0) >= 1
            then
                bumped = "incarnation="
                    .. (kv.incarnation or "?")
                    .. " own="
                    .. (kv.own or "?")
            end
        end
        local remaps = 0
        for _, other in ipairs(GENESIS) do
            if other ~= name then
                remaps = remaps + logs.count(node_log_files(other), "remap ")
            end
        end
        if not ok then
            -- The crashed-node rejoin is bounded, not fatal to the ladder:
            -- the rejoin walk at a deep view can sit fenced (the parked
            -- era-fold family). The states, the boot note, the remap count
            -- and the newest named diagnostic ARE the evidence; the node's
            -- process is stopped so the serving pair carries the ladder.
            local diagnostic_name = ""
            for _, line in ipairs(logs.grep(node_log_files(name), "diagnostic=")) do
                diagnostic_name = line:match("diagnostic=(%S+)") or ""
                break
            end
            anchor("S4-rejoin-BLOCKED " .. name .. " states=" .. states_snapshot())
            measure(
                "scenario=S4 node="
                    .. name
                    .. " crash_restart_to_serving_ms=BLOCKED"
                    .. " rejoin_ms=BLOCKED"
                    .. " "
                    .. (bumped ~= "" and bumped or "no_bump_seen")
                    .. " remap_notices="
                    .. remaps
                    .. " stuck_status="
                    .. report
                    .. " diagnostic="
                    .. diagnostic_name
                    .. " states="
                    .. states_snapshot()
            )
            progress(
                "S4 "
                    .. name
                    .. ": the crashed rejoin stayed fenced (blocked, recorded)"
            )
            term_node(name, 2000)
            clock.sleep(5)
        else
            local t_serving = anchor("S4-serving " .. name .. " " .. report)
            measure(
                "scenario=S4 node="
                    .. name
                    .. " crash_restart_to_serving_ms="
                    .. (t_serving - t_boot)
                    .. " "
                    .. (bumped ~= "" and bumped or "no_bump_seen")
                    .. " remap_notices="
                    .. remaps
                    .. " states="
                    .. states_snapshot()
            )
            clock.sleep(8)
        end
    end
end

-- Scenario 5: full cluster stop/start, then a full reboot cycle (all
-- down, all up). The first pass restarts from the durable state wherever
-- the ladder left it; a non-convergence there is recorded as blocked (the
-- post-crash divergent retained views are the parked era-fold family's
-- downstream), the state is snapshotted and reprovisioned, and the
-- reboot pass runs the resurrection measurement on the fresh boot.
local function scenario_full_cycle()
    phase("S5: whole-cluster stop/start and full reboot")
    -- Pass 1: the durable resurrection on the ladder's end state.
    anchor("S5-begin pass=1 gap_s=10")
    for _, name in ipairs(GENESIS) do
        anchor("S5-TERM-begin pass=1 " .. name)
        term_node(name, 2000)
        anchor("S5-TERM-done pass=1 " .. name)
    end
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
        measure(
            "scenario=S5 pass=1"
                .. " down_ms="
                .. (t_restart - t_all_down)
                .. " first_op_ms="
                .. (first_op - t_restart)
                .. " converge_ms="
                .. (t_conv - t_restart)
                .. " states="
                .. states_snapshot()
        )
    else
        anchor("S5-converge-BLOCKED pass=1 states=" .. states_snapshot())
        measure(
            "scenario=S5 pass=1 converge=BLOCKED"
                .. " down_ms="
                .. (t_restart - t_all_down)
                .. " stuck_report="
                .. report
                .. " states="
                .. states_snapshot()
        )
        progress(
            "S5 pass 1: the durable resurrection did not converge (blocked, recorded)"
        )
        for _, name in ipairs(GENESIS) do
            if procs[name] ~= nil then
                term_node(name, 2000)
            end
        end
        -- Reprovision: the snapshot rule gates the wipe; the fresh boot is
        -- the reboot pass's base.
        local snap_ok, _out, snap_msg = snapshot.snapshot(run_dir, "")
        if not snap_ok then
            gate.fail("the state reprovision's snapshot failed: " .. snap_msg)
        end
        fs.remove_tree(fs.join(run_dir, "state"))
        fs.mkdir_p(fs.join(run_dir, "state"))
    end
    clock.sleep(5)
    -- Pass 2: the full reboot (all down 30 s, all up, converge, serve).
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
    measure(
        "scenario=S5 pass=2"
            .. " down_ms="
            .. (t_restart_b - t_all_down_b)
            .. " first_op_ms="
            .. (first_op_b - t_restart_b)
            .. " converge_ms="
            .. (t_conv_b - t_restart_b)
            .. " states="
            .. states_snapshot()
    )
    clock.sleep(10)
end

-- Scenario 6: the controlled reconfig swap — decrement, era commit,
-- leave, stop, replacement joins, increment. Same host class, fresh
-- admin-assigned id; no crash anywhere.
local function scenario_reconfig_swap()
    phase("S6: controlled reconfig swap (decrement/leave + join/increment)")
    local leader = leader_name()
    if leader == "" then
        gate.fail("S6: no leader for the swap")
    end
    local target = ""
    for _, name in ipairs(GENESIS) do
        if name ~= leader then
            target = name
        end
    end
    local spec = by_name[target]
    local spare = by_name[SPARE]
    local t0 = anchor(
        "S6-begin departing="
            .. target
            .. " replacement="
            .. SPARE
            .. " leader="
            .. leader
    )
    local d_reply, d_ms = drive_verb_retry("decrement", spec.id, {}, 120000)
    anchor("S6-decrement reply=" .. d_reply .. " drive_ms=" .. d_ms)
    if not d_reply:find('"accepted":true', 1, true) then
        gate.fail("S6: decrement refused: " .. d_reply)
    end
    probe_serving(leader, 1, 30000)
    local l_reply, l_ms = drive_verb_retry("leave", spec.id, {}, 120000)
    anchor("S6-leave reply=" .. l_reply .. " drive_ms=" .. l_ms)
    if not l_reply:find('"accepted":true', 1, true) then
        gate.fail("S6: leave refused: " .. l_reply)
    end
    anchor("S6-departed " .. target)
    term_node(target, 2000)
    anchor("S6-" .. target .. "-stopped")
    clock.sleep(2)
    local t_boot = anchor("S6-restart " .. SPARE)
    start_node(SPARE)
    local j_reply, j_ms = drive_verb_retry(
        "join",
        spare.id,
        {
            "--name",
            spare.name,
            "--endpoint",
            "127.0.0.1:" .. spare.port,
        },
        120000
    )
    anchor("S6-join reply=" .. j_reply .. " drive_ms=" .. j_ms)
    if not j_reply:find('"accepted":true', 1, true) then
        gate.fail("S6: join refused: " .. j_reply)
    end
    probe_serving(leader, 1, 30000)
    local i_reply, i_ms = drive_verb_retry("increment", spare.id, {}, 120000)
    anchor("S6-increment reply=" .. i_reply .. " drive_ms=" .. i_ms)
    if not i_reply:find('"accepted":true', 1, true) then
        gate.fail("S6: increment refused: " .. i_reply)
    end
    local ok, report = wait_node_normal(SPARE, t_boot, 180000)
    if not ok then
        gate.fail("S6: " .. SPARE .. " did not reach normal at weight: " .. report)
    end
    local t_done = anchor("S6-swap-complete " .. report)
    measure(
        "scenario=S6 departing="
            .. target
            .. " replacement="
            .. SPARE
            .. " decrement_ms="
            .. d_ms
            .. " leave_ms="
            .. l_ms
            .. " join_ms="
            .. j_ms
            .. " increment_ms="
            .. i_ms
            .. " total_ms="
            .. (t_done - t0)
            .. " rejoin_ms="
            .. (t_done - t_boot)
            .. " states="
            .. states_snapshot()
    )
    clock.sleep(10)
end

-- ------------------------------------------------------------ teardown --

local function teardown()
    phase("T: teardown")
    anchor("teardown-begin")
    signal_clients("USR1")
    clock.sleep(1)
    for _, p in pairs(load_procs) do
        proc.stop(p, 1000)
    end
    for _, name in ipairs(GENESIS) do
        if procs[name] ~= nil then
            anchor("T-TERM-begin " .. name)
            local code = term_node(name, 2000)
            anchor("T-TERM-done " .. name .. " exit=" .. code)
        end
    end
    if procs[SPARE] ~= nil then
        term_node(SPARE, 2000)
    end
    anchor("teardown-complete total_client_errors=" .. total_errors())
    progress("teardown complete")
end

-- ---------------------------------------------------------------- main --

function M.maintenance(root_path: string, run_dir_path: string): integer
    root = root_path
    run_dir = run_dir_path
    parse_specs(fs.join(root, "examples/lease-sequencer/config/cluster-polite2.jsonl"))
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
        "# local-polite2-2026-09-19 — the calm maintenance ladder (item25.29)\n"
            .. "# knobs: heartbeat 5ms, election 3000ms (sloppy detector 3000-5000ms), recovery 3000ms\n"
            .. "# binary: flight-recorder build; load: lease-load polite + stream client\n"
    )
    anchor("run-begin polite2 calm maintenance ladder")
    progress("run begin: " .. states_snapshot())

    -- Boot, converge, clients, steady serving.
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
    phase("S: clients start")
    probe_epoch = anchor("USR2 start all clients")
    start_clients()
    signal_clients("USR2")
    probe_serving("n1", 2, 60000)
    anchor("steady-serving-begin")

    scenario_steady()

    scenario_rotation()
    scenario_abdication()
    scenario_leader_term()
    scenario_crashes()
    scenario_full_cycle()
    scenario_reconfig_swap()

    anchor("ladder-complete states=" .. states_snapshot())
    teardown()
    return 0
end

return M
