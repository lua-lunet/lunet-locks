-- The polite run's per-operation stream client (typed body): one lock, a
-- cycle of acquire → 3× extend → release → get, one log line per completed
-- operation in key=value form (rel_ms, op, ok, dur_ms, detail). It is the
-- per-op latency sample source and the honest error recorder: a connect
-- refusal, a write failure, a reply deadline, or an error body is one
-- error record, never silent.
--
-- Connection discipline: ONE persistent connection to the leader-side
-- server list, the server's sequential client path (one op in flight at a
-- time, one line in, one frame out). A dropped or timed-out connection is
-- re-established before the next op — a fresh connection never carries a
-- stale message id. The rotation advances once per reconnect.

local socket = require("socket") -- typed via tools/lib/socket.d.tl
local clock = require("tools.lib.clock")
local fs = require("tools.lib.fs")

local record M
    main: function(): integer
end

-- The socket shape the tooling's luasocket declaration names (the local
-- mirror so the held connection keeps its typed methods).
local record Tcp
    settimeout: function(Tcp, number)
    connect: function(Tcp, string, integer): boolean, string
    send: function(Tcp, string): number, string
    receive: function(Tcp, string): string, string
    close: function(Tcp)
end

-- A UUID-shaped id (uniqueness inside the run is all the demo protocol
-- asks of a client's ids); the LCG is seeded from the wall clock and
-- advances on every call, so the sequence never cycles back onto an
-- earlier id — a repeated message_id is a duplicate operation by the
-- adapter's exactly-once law, and a duplicate of a long-committed op
-- never gets a fresh reply.
local counter = 0

local function next_hex(): string
    counter = counter + 1
    local seed = (os.time() * 7919 + counter * 104729) % 2147483647
    local hex = ""
    for _ = 1, 32 do
        seed = (seed * 1103515245 + 12345) % 2147483647
        hex = hex .. string.format("%x", seed % 16)
    end
    return hex
end

function M.main(): integer
    local run_dir = os.getenv("POLITE2_RUN_DIR") or "."
    local ports: {integer} = {}
    for port_text in (os.getenv("POLITE2_PORTS") or "45211 45212 45213"):gmatch(
        "%d+"
    ) do
        ports[#ports + 1] = tonumber(port_text) as integer
    end
    local lock_id = (tonumber(os.getenv("POLITE2_LOCK_ID")) or 9201) as integer
    local lease_ms = (tonumber(os.getenv("POLITE2_LEASE_MS")) or 500) as integer

    local log_path = fs.join(run_dir, "stream-client.log")
    local log_file = io.open(log_path, "a")
    if not log_file then
        io.stderr:write("stream-client: cannot open " .. log_path .. "\n")
        return 1
    end

    local boot_ms = clock.now_ms()
    local request_num = 0
    local cursor = 0
    -- The live connection in a one-element list (a typed nil-able slot).
    local held: {Tcp} = {}

    local function new_uuid(): string
        local hex = next_hex()
        return hex:sub(1, 8)
            .. "-"
            .. hex:sub(9, 12)
            .. "-"
            .. hex:sub(13, 16)
            .. "-"
            .. hex:sub(17, 20)
            .. "-"
            .. hex:sub(21, 32)
    end

    local holder = new_uuid()

    local function log_line(op: string, ok: boolean, dur_ms: integer, detail: string)
        log_file:write(
            "rel_ms="
                .. (clock.now_ms() - boot_ms)
                .. " op="
                .. op
                .. " ok="
                .. (ok and "1" or "0")
                .. " dur_ms="
                .. dur_ms
                .. " detail="
                .. detail
                .. "\n"
        )
        log_file:flush()
    end

    -- One persistent connection; a refused or dropped one is re-established
    -- at the next port in the rotation (the load clients' rotation).
    local function conn(): (Tcp, string)
        if held[1] ~= nil then
            return held[1], ""
        end
        for _ = 1, #ports do
            counter = counter % #ports + 1
            local attempt = socket.tcp() as Tcp
            attempt:settimeout(6.0)
            local ok = attempt:connect("127.0.0.1", ports[counter])
            if ok then
                held[1] = attempt
                return attempt, ""
            end
            attempt:close()
        end
        return nil, "every server refused the connection"
    end

    local function drop_conn()
        -- The dead socket object is dropped, not closed: the garbage
        -- collector finalizes it (luasocket closes on collect), and the
        -- reconnect replaces the slot below.
        held[1] = nil
    end

    -- One op: send the line, read the reply frame. A connect failure, a
    -- write failure, a reply deadline, or an error body is the error.
    local function one_op(op: string, body: string): boolean, integer, string
        request_num = request_num + 1
        local line = '{"op":"'
            .. op
            .. '","message_id":"'
            .. new_uuid()
            .. '","client_id":910000,"request_num":'
            .. request_num
            .. ',"lock_id":'
            .. lock_id
            .. body
            .. "}\n"
        local t0 = clock.mono_ms()
        local s, cerr = conn()
        if s == nil then
            drop_conn()
            return false, clock.mono_ms() - t0, cerr
        end
        local sent, werr = s:send(line)
        if sent == nil or (sent as integer) == 0 then
            drop_conn()
            return false, clock.mono_ms() - t0, "write: " .. tostring(werr)
        end
        local reply, rerr = s:receive("*l")
        local dur = clock.mono_ms() - t0
        if not reply then
            drop_conn()
            return false,
                dur,
                "reply deadline or dropped connection: " .. tostring(rerr)
        end
        return reply:find('"granted":true', 1, true) ~= nil
            or reply:find('"released":true', 1, true) ~= nil
            or reply:find('"op":"get"', 1, true) ~= nil, dur, (reply:gsub("%s+", " "))
    end

    local lease_payload = ',"lease":{"lease_id":1,"holder":"'
        .. holder
        .. '","lease_ms":'
        .. lease_ms
        .. "}"

    while true do
        local ok, dur, detail = one_op("set", lease_payload)
        log_line("acquire", ok, dur, detail)
        if ok then
            for _ = 1, 3 do
                clock.sleep(0.3)
                local eok, edur, edetail = one_op("set", lease_payload)
                log_line("extend", eok, edur, edetail)
                if not eok then
                    break
                end
            end
            clock.sleep(0.2)
            local rok, rdur, rdetail = one_op(
                "release", ',"holder":"' .. holder .. '","lease_id":1'
            )
            log_line("release", rok, rdur, rdetail)
        else
            clock.sleep(0.5)
        end
        local gok, gdur, gdetail = one_op("get", "")
        log_line("get", gok, gdur, gdetail)
        clock.sleep(0.5)
    end
    return 0
end

return M
