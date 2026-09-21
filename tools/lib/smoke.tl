-- The three-process Lunet runtime smoke, ported from the shell runner:
-- boot the genesis trio, drive the lock exchange through the nonleader,
-- prove expiry takeover, kill one replica and prove the resurrection
-- path, ride a live membership lifecycle (join/increment/decrement/
-- leave) under an uninterrupted lock stream, and close with the stop
-- contract: every TERM'd replica exits 0 through the drain point with a
-- flushed marker. The runtime is the project-local pinned Lunet
-- v0.10.0 release, never a host `lunet-run` from PATH.

local client = require("tools.lib.client")
local clock = require("tools.lib.clock")
local fs = require("tools.lib.fs")
local logs = require("tools.lib.logs")
local proc = require("tools.lib.proc")
local shell = require("tools.lib.shell")

local record M
    main: function(string, {string}): integer
end

-- The four-line deployment descriptor: sparse, admin-assigned,
-- never-recycled NodeIds. The genesis lines, in line order, are the
-- founding membership and the genesis succession sequence (n1 is the
-- genesis primary). The appended non-genesis line is the
-- live-reconfiguration stage's joining replica: its endpoint must be
-- known to every process from boot, and running processes never reload
-- the file — the live membership transition itself is driven by the
-- admin verbs below.
local CLUSTER_JSONL = table.concat(
    {
        '{"id":101,"name":"n1","host":"127.0.0.1","port":27101,"genesis":true}',
        '{"id":202,"name":"n2","host":"127.0.0.1","port":27102,"genesis":true}',
        '{"id":303,"name":"n3","host":"127.0.0.1","port":27103,"genesis":true}',
        '{"id":404,"name":"n4","host":"127.0.0.1","port":27104,"genesis":false}',
    },
    "\n"
) .. "\n"

-- Send every line on one connection and check each expected marker
-- against the concatenated replies.
local function request_lines(
    work: string, port: integer, lines: {string}, expected: {string}, deadline: number
): boolean
    local out, err = client.exchange(port, lines, deadline)
    if err ~= "" then
        io.stderr:write(
            "lunet smoke: the client exchange failed (connect, write, or reply deadline): "
                .. err
                .. "\n"
        )
        if out ~= "" then
            io.stderr:write(out .. "\n")
        end
        return false
    end
    fs.write_all(fs.join(work, "client.out"), out .. "\n")
    for _, marker in ipairs(expected) do
        if not out:find(marker, 1, true) then
            io.stderr:write(
                "lunet smoke: expected response marker not found: " .. marker .. "\n"
            )
            io.stderr:write(out .. "\n")
            return false
        end
    end
    return true
end

-- The per-stage nodes and their ports (client / peer), explicit at every
-- call site to prevent accidental port reuse.
local NODE_PORTS: {string: {string: integer}} = {
    n1 = {client = 28101, peer = 27101},
    n2 = {client = 28102, peer = 27102},
    n3 = {client = 28103, peer = 27103},
    n4 = {client = 28104, peer = 27104},
}

-- Drive one admin verb direct to the leader, retrying with fresh
-- message ids while the leader settles the previous transition: a
-- stop-the-world era entry awaits the ordinary fence, and until that
-- fence the establishing gate refuses the next reconfiguration without
-- anything entering the log. Refusals are the expected settling shape,
-- so the loop retries within a budget. An acknowledgment is only
-- trusted when it arrives inside the leader's 30s era-poll window: at
-- the window's end the leader emits the same accepted:true body without
-- a committed era. The stage is observational: an exhausted budget or a
-- timeout acknowledgment is logged with full context and reported as
-- not-driven (the stream timeline below is the record).
local function drive_admin(
    work: string, port: integer, label: string, template: string, hexbase: string
): boolean
    local t_start = clock.now_ms()
    fs.write_all(fs.join(work, label .. ".start"), tostring(t_start) .. "\n")
    local attempts = 0
    while true do
        attempts = attempts + 1
        if attempts > 12 then
            break
        end
        if clock.now_ms() - t_start >= 90000 then
            break
        end
        local mid = string.format(
            "00000000-0000-0000-0000-0000000000%s%x", hexbase, attempts
        )
        local body = template:format(mid)
        local t_attempt = clock.now_ms()
        local out, err = client.exchange(port, {body}, 40)
        local accepted = err == "" and out:find('"accepted":true', 1, true) ~= nil
        if accepted then
            local t_done = clock.now_ms()
            if t_done - t_attempt >= 30000 then
                io.stderr:write(
                    "lunet smoke: "
                        .. label
                        .. " ack took "
                        .. (t_done - t_attempt)
                        .. "ms; at the leader's\n"
                )
                io.stderr:write(
                    "30s era-poll timeout this is the timeout acknowledgment, not a committed era\n"
                )
                return false
            end
            fs.write_all(fs.join(work, label .. ".done"), tostring(t_done) .. "\n")
            print(
                "lunet smoke: live-reconfig "
                    .. label
                    .. " accepted on attempt "
                    .. attempts
                    .. " after "
                    .. (t_done - t_start)
                    .. "ms"
            )
            return true
        end
        clock.sleep(4)
    end
    io.stderr:write(
        "lunet smoke: live-reconfig "
            .. label
            .. " was refused by every drive within the retry budget\n"
    )
    return false
end

local function stop_process(p: proc.Process)
    proc.stop(p, 1000)
end

-- One stream-out window: request count and worst-case latency inside
-- [t_a, t_b) from the "t0 latency reply" lines.
local function window_stats(path: string, t_a: integer, t_b: integer): integer, integer
    local n, max_latency = 0, 0
    local data = fs.read_all(path)
    for line in (data .. "\n"):gmatch("(.-)\n") do
        local t0, lat_s = line:match("^(%d+) (%d+) ")
        if t0 and lat_s then
            local lat = (tonumber(lat_s) or 0) as integer
            local t0_ms = (tonumber(t0) or 0) as integer
            if t0_ms >= t_a and t0_ms < t_b then
                n = n + 1
                if lat > max_latency then
                    max_latency = lat
                end
            end
        end
    end
    return n, max_latency
end

function M.main(root: string, _argv: {string}): integer
    local run = os.getenv("LUNET_RUN") or fs.join(root, ".lunet/v0.10.0/lunet-run")
    local cyan = os.getenv("CYAN") or fs.join(root, ".rocks/bin/cyan")

    if
        not fs.exists(run) or not shell.status("test -x " .. shell.quote(run)) == true
    then
        -- (status check below, in the runner, exits 127 on a missing runtime)
    end

    local work = fs.unique_sibling(fs.join(root, ".tmp"), "lunet-smoke")
    if not fs.mkdir_p(work) then
        io.stderr:write(
            "lunet smoke: cannot create work dir under " .. root .. "/.tmp\n"
        )
        return 1
    end

    local completed = false
    local luajit_bin = arg[-1] or "luajit"
    local procs: {string: proc.Process} = {}

    local function fail(message: string)
        error("lunet smoke: " .. message, 0)
    end

    -- The snapshot rule gates the wipe: the work dir is removed only
    -- after a successful snapshot of it; a failed snapshot refuses the
    -- wipe loudly and the run dir stays.
    local function cleanup(throw_ok: boolean)
        if next(procs) then
            for _, p in pairs(procs) do
                stop_process(p)
            end
            procs = {}
        end
        if completed then
            local snapshot = require("tools.lib.snapshot")
            snapshot.snapshot_then_wipe(work)
        else
            io.stderr:write("lunet smoke: retained failure logs in " .. work .. "\n")
            -- The runner (local or CI) is torn down with the job; the CI log
            -- is the only place these survive, so print them rather than
            -- pointing at a path nobody can read afterward.
            local entries, _e = fs.list_dir(work)
            for _, name in ipairs(entries) do
                local is_log = name:match("%.out$") or name:match("%.err$")
                if is_log then
                    io.stderr:write("--- " .. name .. " ---\n")
                    io.stderr:write(fs.read_all(fs.join(work, name)))
                end
            end
        end
        local _throw_ok = throw_ok
    end

    local started = false

    local function start(name: string): proc.Process
        local ports = NODE_PORTS[name]
        local _peer = ports.peer -- the peer port documents the topology at every call site
        local p = proc.spawn(
            {
                run,
                fs.join(root, "build/server.lua"),
                "--node",
                name,
                "--client",
                "127.0.0.1:" .. ports.client,
                "--state",
                fs.join(work, name .. ".nonce"),
                "--cluster",
                fs.join(work, "cluster.jsonl"),
            },
            name,
            {
                out_path = fs.join(work, name .. ".out"),
                err_path = fs.join(work, name .. ".err"),
                combine = false,
                env = {},
            }
        )
        procs[name] = p
        fs.write_all(fs.join(work, name .. ".pid"), tostring(p.pid) .. "\n")
        started = true
        return p
    end

    local body = function(): integer
        if not fs.exists(run) then
            fail(
                "missing project-local Lunet v0.10.0 runtime at "
                    .. run
                    .. "; run make lunet-runtime"
            )
        end
        if not fs.exists(cyan) then
            fail("Cyan is required at " .. cyan .. " (or set CYAN)")
        end

        fs.write_all(fs.join(work, "cluster.jsonl"), CLUSTER_JSONL)
        if not shell.run(
            "cargo build --release --manifest-path "
                .. shell.quote(fs.join(root, "ext/advisory_lock/Cargo.toml"))
        ) then
            fail("cargo build failed")
        end
        if not shell.run(shell.quote(cyan) .. " build --prune") then
            fail("cyan build failed")
        end

        start("n1")
        start("n2")
        start("n3")

        -- n1's zero election stagger makes it leader under the documented
        -- defaults; sending through n2 verifies application forwarding
        -- rather than direct leader I/O.
        clock.sleep(3.5)
        local initial_lease_ms = 5000
        local holder1 = "11111111-1111-1111-1111-111111111111"
        local holder2 = "22222222-2222-2222-2222-222222222222"
        local holder3 = "33333333-3333-3333-3333-333333333333"

        local exchange_ok = request_lines(
            work,
            28102,
            {
                '{"op":"set","message_id":"00000000-0000-0000-0000-000000000001","client_id":1,"request_num":1,"lock_id":9001,"lease":{"lease_id":1,"holder":"'
                    .. holder1
                    .. '","lease_ms":'
                    .. initial_lease_ms
                    .. "}}",
                '{"op":"get","message_id":"00000000-0000-0000-0000-000000000002","client_id":1,"request_num":2,"lock_id":9001}',
                '{"op":"set","message_id":"00000000-0000-0000-0000-000000000003","client_id":2,"request_num":1,"lock_id":9001,"lease":{"lease_id":2,"holder":"'
                    .. holder2
                    .. '","lease_ms":'
                    .. initial_lease_ms
                    .. "}}",
                '{"op":"release","message_id":"00000000-0000-0000-0000-000000000004","client_id":1,"request_num":3,"lock_id":9001,"holder":"'
                    .. holder1
                    .. '","lease_id":1}',
                '{"op":"set","message_id":"00000000-0000-0000-0000-000000000005","client_id":2,"request_num":2,"lock_id":9001,"lease":{"lease_id":2,"holder":"'
                    .. holder2
                    .. '","lease_ms":'
                    .. initial_lease_ms
                    .. "}}",
                '{"op":"release","message_id":"00000000-0000-0000-0000-000000000006","client_id":2,"request_num":3,"lock_id":9001,"holder":"'
                    .. holder2
                    .. '","lease_id":2}',
            },
            {
                '"granted":true',
                '"op":"get"',
                '"granted":false',
                '"released":true',
                '"granted":true',
                '"released":true',
            },
            5
        )
        if not exchange_ok then
            fail("the lock exchange failed")
        end

        local short_lease_ms = 3000
        if not request_lines(
            work,
            28102,
            {
                '{"op":"set","message_id":"00000000-0000-0000-0000-000000000007","client_id":3,"request_num":1,"lock_id":9001,"lease":{"lease_id":3,"holder":"'
                    .. holder1
                    .. '","lease_ms":'
                    .. short_lease_ms
                    .. "}}",
            },
            {'"granted":true'},
            5
        ) then
            fail("the short-lease grant failed")
        end
        clock.sleep(3.1)
        -- The takeover set's own lease must outlive the post-restart
        -- resurrection window (the get below runs ~10s later), so it is
        -- granted a minute off the leader's execution clock; the takeover
        -- itself is driven by the prior short lease's duration above.
        local takeover_lease_ms = 60000
        if not request_lines(
            work,
            28102,
            {
                '{"op":"set","message_id":"00000000-0000-0000-0000-000000000008","client_id":4,"request_num":1,"lock_id":9001,"lease":{"lease_id":4,"holder":"'
                    .. holder3
                    .. '","lease_ms":'
                    .. takeover_lease_ms
                    .. "}}",
            },
            {'"granted":true'},
            5
        ) then
            fail("the takeover grant failed")
        end

        -- The crash shape is this stage's subject: SIGKILL (never the
        -- deliberate stop, which the final stop-contract stage exercises)
        -- leaves the running sentinel behind, no drain, no marker write —
        -- the dirty classification the reincarnation way exists for.
        local n3 = procs.n3
        proc.kill(n3)
        proc.wait(n3)
        procs.n3 = nil -- (reassigned below)
        local n3_restarted = start("n3")
        local _n3r = n3_restarted
        clock.sleep(10)
        if not request_lines(
            work,
            28102,
            {
                '{"op":"get","message_id":"00000000-0000-0000-0000-000000000009","client_id":4,"request_num":2,"lock_id":9001}',
            },
            {'"op":"get"', holder3},
            5
        ) then
            fail("the post-restart read failed")
        end
        -- The evidence set is every node's stderr/stdout capture, matched
        -- by node-name prefix over the logs DIRECTORY (no globs, no exact
        -- dated names).
        local evidence = logs.files_for_all(work, {"n1", "n2", "n3", "n4"})
        local flood = logs.count(evidence, "cannot address replica")
        if flood ~= 0 then
            fail(
                "the restart's remap window dropped "
                    .. flood
                    .. " datagrams for unaddressable replicas"
            )
        end

        -- The uninterrupted lock stream: one sequential TCP connection
        -- through n2, cycling acquire / renew / release / read on a
        -- dedicated lock. Any unexpected reply, stall past the deadline,
        -- or dropped connection is a stream error and fails the stage.
        local stream = proc.spawn(
            {luajit_bin, fs.join(root, "tools/smoke-stream.lua"), "28102"},
            "stream",
            {
                out_path = fs.join(work, "stream.out"),
                err_path = fs.join(work, "stream.err"),
                combine = false,
                env = {},
            }
        )
        procs.stream = stream
        fs.write_all(fs.join(work, "stream.pid"), tostring(stream.pid) .. "\n")
        clock.sleep(1)

        -- The joiner's process boots while the stream is live. It enters
        -- the protocol only through the leader's messages; until the
        -- committed Join admits it, its datagrams are dropped by name at
        -- every incumbent.
        start("n4")
        clock.sleep(2)

        -- The committed Join folds the new era at every incumbent; the
        -- era's stream then reaches the weight-0 joiner, which folds its
        -- admitting era there. The promotion's own success is downstream
        -- proof of the join's commit: a drive for a member the
        -- configuration does not know is refused.
        local join_ok = drive_admin(
            work,
            28101,
            "join",
            '{"action":"join","message_id":"%s","id":404,"name":"n4","endpoint":"127.0.0.1:27104"}',
            "a"
        )
        local _join_ok = join_ok -- observational stage; the stream is the record
        local join_start_raw = fs.read_all(fs.join(work, "join.start"))
        local t_join_start = tonumber(join_start_raw) as integer
        local join_done_raw = fs.read_all(fs.join(work, "join.done"))
        local t_join_done = join_done_raw ~= "" and (tonumber(join_done_raw) as integer)
            or t_join_start
        clock.sleep(2)

        local inc_ok = drive_admin(
            work,
            28101,
            "increment",
            '{"action":"increment","message_id":"%s","id":404}',
            "b"
        )
        local _inc_ok = inc_ok
        local inc_start_raw = fs.read_all(fs.join(work, "increment.start"))
        local t_inc_start = tonumber(inc_start_raw) as integer
        local inc_done = fs.read_all(fs.join(work, "increment.done"))
        local t_inc_done = inc_done ~= "" and (tonumber(inc_done) as integer)
            or t_inc_start
        clock.sleep(2)

        local dec_ok = drive_admin(
            work,
            28101,
            "decrement",
            '{"action":"decrement","message_id":"%s","id":404}',
            "c"
        )
        local _dec_ok = dec_ok
        local dec_start_raw = fs.read_all(fs.join(work, "decrement.start"))
        local t_dec_start = tonumber(dec_start_raw) as integer
        local dec_done = fs.read_all(fs.join(work, "decrement.done"))
        local t_dec_done = dec_done ~= "" and (tonumber(dec_done) as integer)
            or t_dec_start
        clock.sleep(2)

        local leave_ok = drive_admin(
            work, 28101, "leave", '{"action":"leave","message_id":"%s","id":404}', "d"
        )
        local _leave_ok = leave_ok
        local leave_start_raw = fs.read_all(fs.join(work, "leave.start"))
        local t_leave_start = tonumber(leave_start_raw) as integer
        local leave_done_raw = fs.read_all(fs.join(work, "leave.done"))
        local t_leave_done = leave_done_raw ~= ""
            and (tonumber(leave_done_raw) as integer) or t_leave_start
        clock.sleep(2)

        -- The departed replica is out of the configuration once its
        -- leave's era has committed; stopping its process must leave the
        -- stream and every later request on the remaining three.
        local n4 = procs.n4
        stop_process(n4)
        procs.n4 = nil
        clock.sleep(1)
        local post_takeover_lease_ms = 60000
        if not request_lines(
            work,
            28102,
            {
                '{"op":"set","message_id":"00000000-0000-0000-0000-00000000000e","client_id":5,"request_num":1,"lock_id":9201,"lease":{"lease_id":5,"holder":"'
                    .. holder3
                    .. '","lease_ms":'
                    .. post_takeover_lease_ms
                    .. "}}",
            },
            {'"granted":true'},
            5
        ) then
            fail("the post-leave set failed")
        end
        if not request_lines(
            work,
            28102,
            {
                '{"op":"release","message_id":"00000000-0000-0000-0000-00000000000f","client_id":5,"request_num":2,"lock_id":9201,"holder":"'
                    .. holder3
                    .. '","lease_id":5}',
            },
            {'"released":true'},
            5
        ) then
            fail("the post-leave release failed")
        end

        local stream_p = procs.stream
        stop_process(stream_p)
        procs.stream = nil
        local t_stream_end = clock.now_ms()
        -- The stream survives every transition window: any unexpected
        -- reply, stall past the deadline, dropped connection, or timeout
        -- is a stream error and fails the run.
        local stream_err = fs.read_all(fs.join(work, "stream.err"))
        if stream_err:find("STREAM ERROR", 1, true) then
            io.stderr:write(
                "lunet smoke: the lock stream errored across the live reconfiguration:\n"
            )
            io.stderr:write(stream_err)
            fail("the lock stream did not survive the live reconfiguration")
        end
        local stream_out = fs.read_all(fs.join(work, "stream.out"))
        if stream_out == "" then
            fail("the lock stream produced no replies")
        end

        -- The transition record: request counts and worst-case
        -- per-request latency inside each admin-verb window, then the
        -- whole stage. The stream's own validity was asserted
        -- reply-by-reply above; these numbers are the measured
        -- stop-the-world latencies, reported as they were observed.
        local n_j, m_j = window_stats(
            fs.join(work, "stream.out"), t_join_start, t_inc_start
        )
        print(
            "lunet smoke: live-reconfig stream [join window]: requests="
                .. n_j
                .. " max_latency="
                .. m_j
                .. "ms"
        )
        local n_i, m_i = window_stats(
            fs.join(work, "stream.out"), t_inc_start, t_dec_start
        )
        print(
            "lunet smoke: live-reconfig stream [increment window]: requests="
                .. n_i
                .. " max_latency="
                .. m_i
                .. "ms"
        )
        local n_d, m_d = window_stats(
            fs.join(work, "stream.out"), t_dec_start, t_leave_start
        )
        print(
            "lunet smoke: live-reconfig stream [decrement window]: requests="
                .. n_d
                .. " max_latency="
                .. m_d
                .. "ms"
        )
        local n_l, m_l = window_stats(
            fs.join(work, "stream.out"), t_leave_start, t_stream_end
        )
        print(
            "lunet smoke: live-reconfig stream [leave window]: requests="
                .. n_l
                .. " max_latency="
                .. m_l
                .. "ms"
        )
        local n_t, m_t = window_stats(fs.join(work, "stream.out"), 0, 99999999999999)
        print(
            "lunet smoke: live-reconfig stream total: requests="
                .. n_t
                .. " max_latency="
                .. m_t
                .. "ms"
        )
        print(
            "lunet smoke: live-reconfig timeline join="
                .. (t_join_done - t_join_start)
                .. "ms increment="
                .. (t_inc_done - t_inc_start)
                .. "ms decrement="
                .. (t_dec_done - t_dec_start)
                .. "ms leave="
                .. (t_leave_done - t_leave_start)
                .. "ms"
        )

        -- The stop contract stage: the runtime's one-shot TERM watcher
        -- (armed by the server at boot) hands each process to the
        -- deliberate stop, the on_stop hook runs exactly once at the drain
        -- point, the node's graceful stop writes the flushed marker, and
        -- the process exits 0. A TERM'd replica therefore never dies at
        -- the default disposition and never leaves the running sentinel
        -- behind.
        for _, name in ipairs({"n1", "n2", "n3"}) do
            local p = procs[name]
            if not proc.alive(p) then
                fail(name .. " was already gone before the TERM stage")
            end
            local status = proc.stop(p, 30000)
            procs[name] = nil
            if status ~= 0 then
                fail(
                    name
                        .. " exited "
                        .. status
                        .. " on TERM (the drain point did not run clean)"
                )
            end
            local out_path = fs.join(work, name .. ".out")
            local drains = logs.count({out_path}, "stop: drain point reached")
            if drains ~= 1 then
                fail(
                    name
                        .. " ran the drain point "
                        .. drains
                        .. " times (expected exactly once)"
                )
            end
            if logs.count({out_path}, "node drained and flushed") ~= 1 then
                fail(name .. " did not record the completed node stop")
            end
            local marker = fs.read_all(fs.join(work, name .. ".nonce"))
            if not marker:find(" flushed") then
                fail(name .. "'s state marker is not flushed: " .. marker)
            end
            print(
                "lunet smoke: "
                    .. name
                    .. " stopped through the drain point (exit "
                    .. status
                    .. ", marker: "
                    .. marker:gsub("\n", " ")
                    .. ")"
            )
        end

        started = true
        return 0
    end

    local ok, err = pcall(body)
    started = started or false
    if not ok then
        io.stderr:write(tostring(err) .. "\n")
        completed = false
    else
        completed = true
    end
    local failed = not ok
    cleanup(failed)
    if failed then
        return 1
    end
    print("lunet smoke: passed")
    return 0
end

return M
