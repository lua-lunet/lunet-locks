#!/usr/bin/env -S luajit
-- The lock stream subprocess of the smoke: one sequential TCP
-- connection through n2, cycling acquire / renew / release / read on a
-- dedicated lock. Any unexpected reply, stall past the deadline, or
-- dropped connection is a STREAM ERROR on stderr and exit 1; every
-- reply is logged with its start time and latency so the transition
-- windows can be measured afterwards.
--
-- usage: tools/smoke-stream.lua PORT

dofile((arg[0]:match("^(.*)/[^/]*$") or ".") .. "/bootstrap.lua")

local socket = require("socket")
local clock = require("tools.lib.clock")

local port = tonumber(arg[1])
if not port then
    io.stderr:write("usage: smoke-stream.lua PORT\n")
    os.exit(2)
end

local holder = "44444444-4444-4444-4444-444444444444"
local client_id = 9
local seq = 0
local lease_seq = 0

local function now_ms()
    return clock.now_ms()
end

local function uuid()
    seq = seq + 1
    return string.format("aaaa0000-0000-4000-8000-%012d", seq)
end

local sk = socket.tcp()
if not sk then
    io.stderr:write("STREAM ERROR: connect: no socket\n")
    os.exit(1)
end
sk:settimeout(60)
local ok, err = sk:connect("127.0.0.1", port)
if not ok then
    io.stderr:write("STREAM ERROR: connect: " .. tostring(err) .. "\n")
    os.exit(1)
end

-- The 30 s lease survives any legitimate transition stall; the stream
-- holds the lock continuously across the reconfiguration, so a grant
-- wrested by anyone else would surface as a refused renew here. The
-- leader stamps the absolute expiry off its own execution clock.
while true do
    lease_seq = lease_seq + 1
    local lease_id = lease_seq
    local t0 = now_ms()

    local function request(json, marker, what)
        local sent, werr = sk:send(json .. "\n")
        if not sent then
            io.stderr:write(
                "STREAM ERROR: write (" .. what .. "): " .. tostring(werr) .. "\n"
            )
            os.exit(1)
        end
        local reply, rerr = sk:receive("*l")
        if not reply then
            io.stderr:write(
                "STREAM ERROR: connection closed or stalled awaiting "
                    .. what
                    .. " reply: "
                    .. tostring(rerr)
                    .. "\n"
            )
            os.exit(1)
        end
        local lat = now_ms() - t0
        print(t0, lat, reply)
        io.stdout:flush()
        if not reply:find(marker, 1, true) then
            io.stderr:write(
                "STREAM ERROR: unexpected " .. what .. " reply: " .. reply .. "\n"
            )
            os.exit(1)
        end
    end

    request(
        '{"op":"set","message_id":"'
            .. uuid()
            .. '","client_id":'
            .. client_id
            .. ',"request_num":'
            .. seq
            .. ',"lock_id":9101,"lease":{"lease_id":'
            .. lease_id
            .. ',"holder":"'
            .. holder
            .. '","lease_ms":30000}}',
        '"granted":true',
        "acquire"
    )
    clock.sleep(0.1)
    request(
        '{"op":"set","message_id":"'
            .. uuid()
            .. '","client_id":'
            .. client_id
            .. ',"request_num":'
            .. seq
            .. ',"lock_id":9101,"lease":{"lease_id":'
            .. lease_id
            .. ',"holder":"'
            .. holder
            .. '","lease_ms":30000}}',
        '"granted":true',
        "renew"
    )
    clock.sleep(0.1)
    request(
        '{"op":"release","message_id":"'
            .. uuid()
            .. '","client_id":'
            .. client_id
            .. ',"request_num":'
            .. seq
            .. ',"lock_id":9101,"holder":"'
            .. holder
            .. '","lease_id":'
            .. lease_id
            .. "}",
        '"released":true',
        "release"
    )
    clock.sleep(0.1)
    request(
        '{"op":"get","message_id":"'
            .. uuid()
            .. '","client_id":'
            .. client_id
            .. ',"request_num":'
            .. seq
            .. ',"lock_id":9101}',
        '"lease":null',
        "get"
    )
    clock.sleep(0.1)
end
