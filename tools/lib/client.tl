-- Typed TCP NDJSON client over luasocket: the smoke's request/response
-- exchanges and the headless HTTP probes. One line in, one frame out —
-- the server's sequential client path — with a per-reply deadline.

local socket = require("socket")
local base64 = require("tools.lib.base64")

local record M
    exchange: function(integer, {string}, number): string, string
    http_get: function(integer, string, string): string, string
end

-- Sends each line on one connection, preserving the server's sequential
-- client path, and returns every reply's text. NDJSON frames are
-- newline-delimited; single-line arguments have no terminator, so one is
-- added before sending. A connect failure, a write failure, or a reply
-- that misses its deadline is the error return.
function M.exchange(port: integer, lines: {string}, deadline_s: number): string, string
    local sk = socket.tcp()
    if not sk then
        return "", "connect: no socket"
    end
    sk:settimeout(deadline_s)
    local ok, err = sk:connect("127.0.0.1", port)
    if not ok then
        sk:close()
        return "", "connect to 127.0.0.1:" .. port .. ": " .. tostring(err)
    end
    local out: {string} = {}
    for _, line in ipairs(lines) do
        if line:sub(-1) ~= "\n" then
            line = line .. "\n"
        end
        local sent, werr = sk:send(line)
        if not sent then
            sk:close()
            return "", "write: " .. tostring(werr)
        end
        local reply, rerr = sk:receive("*l")
        if not reply then
            sk:close()
            return table.concat(out, "\n"),
                "reply deadline or dropped connection: " .. tostring(rerr)
        end
        out[#out + 1] = reply
    end
    sk:close()
    return table.concat(out, "\n"), ""
end

-- A plain HTTP GET to 127.0.0.1 (the headless console/feed assertions).
-- `auth` is the `user:password` string for the basic-auth edge; nil
-- sends no Authorization header.
function M.http_get(port: integer, path: string, auth: string): string, string
    local sk = socket.tcp()
    if not sk then
        return "", "connect: no socket"
    end
    sk:settimeout(3)
    local ok, err = sk:connect("127.0.0.1", port)
    if not ok then
        sk:close()
        return "", "connect: " .. tostring(err)
    end
    local head = "GET " .. path .. " HTTP/1.0\r\nHost: 127.0.0.1\r\n"
    if auth and auth ~= "" then
        head = head .. "Authorization: Basic " .. base64.encode(auth) .. "\r\n"
    end
    head = head .. "\r\n"
    local sent, werr = sk:send(head)
    if not sent then
        sk:close()
        return "", "write: " .. tostring(werr)
    end
    local body: {string} = {}
    while true do
        local chunk, rerr = sk:receive("*a")
        if chunk then
            body[#body + 1] = chunk
        end
        if rerr == "closed" then
            break
        end
        if rerr == "timeout" then
            break
        end
        if not chunk and rerr then
            sk:close()
            return table.concat(body), "read: " .. tostring(rerr)
        end
    end
    sk:close()
    return table.concat(body), ""
end

return M
