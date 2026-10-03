-- The compliance corpus's transport host, on a socket
-- (`docs/uvrr-host-compliance.md` §8).
--
-- This is a development-only service. It is the one place in this tree
-- that loads the Compliance ABI, and it exists because a `compatibility_
-- suite` build carries nine exports that WRITE protocol state
-- (`docs/src/compliance-abi.md`): the corpus's raw payloads commit
-- without a Service decode, the executor's logical tick stands in for the
-- wall clock, and the journal, the folded configuration and the
-- boot-gate marker schedule are readable directly. A production boot
-- must never serve this; the library it loads is the gated build shape
-- and the symbol table proves the production shape has no such entry.
--
-- The socket is its own process on its own port, separate by
-- construction from every lock's client port: this host boots its own
-- in-process cluster of adapter nodes and speaks HTTP/1.1 + JSON to a
-- client, and no lock client protocol passes through it.
--
-- One request per connection, exactly as the harness contract states:
-- `Content-Type: application/json`, a `Content-Length`, and
-- `Connection: close` on both sides.

local record DebugInfo
    source: string
end

-- lunet-run does not add the script directory to package.path. The host
-- is invoked as `lunet-run build/compliance_server.lua`, so make sibling
-- Cyan output available before loading it.
local source = (debug.getinfo(1, "S") as DebugInfo).source
local directory = source:match("^@(.+)/[^/]+$") or "."
package.path = directory .. "/?.lua;" .. package.path

local lunet = require("lunet")
local socket = require("lunet.socket")

local advisory_lock = require("advisory_lock")
local host_module = require("compliance_host")

local MAX_REQUEST_BYTES = 16777216
local MAX_HEAD_BYTES = 65536

local REASONS: {integer: string} = {
    [200] = "OK",
    [400] = "Bad Request",
    [404] = "Not Found",
    [405] = "Method Not Allowed",
    [413] = "Payload Too Large",
    [431] = "Request Header Fields Too Large",
    [500] = "Internal Server Error",
}

local function response(status: integer, body: string): string
    return "HTTP/1.1 "
        .. status
        .. " "
        .. (REASONS[status] or "Unknown")
        .. "\r\nContent-Type: application/json\r\nContent-Length: "
        .. #body
        .. "\r\nConnection: close\r\n\r\n"
        .. body
end

-- One request's head: the request line and the headers, then the body
-- the `Content-Length` names. Returns nil and a status for a malformed
-- request or an oversized one.
local function read_request(connection: any): string, string, string, integer | nil
    local head = ""
    local rest = ""
    while true do
        local stop = head:find("\r\n\r\n", 1, true)
        if stop ~= nil then
            -- The body may have arrived in the same read as the head, so
            -- whatever follows the header break is kept, not discarded.
            rest = string.sub(head, stop + 4)
            head = string.sub(head, 1, stop + 3)
            break
        end
        if #head > MAX_HEAD_BYTES then
            return nil, nil, nil, 431
        end
        local chunk, failure = socket.read(connection)
        if chunk == nil then
            if failure == nil or #head == 0 then
                return nil, nil, nil, 400
            end
            return nil, nil, nil, 400
        end
        head = head .. chunk
        if #head > MAX_REQUEST_BYTES then
            return nil, nil, nil, 431
        end
    end
    local lines = {}
    for line in head:gmatch("[^\r\n]+") do
        table.insert(lines, line)
    end
    local request_line = lines[1]
    if request_line == nil then
        return nil, nil, nil, 400
    end
    local method, target = request_line:match("^(%u+) (%S+) HTTP/1%.1$")
    if method == nil or target == nil then
        return nil, nil, nil, 400
    end
    local length = 0
    for index = 2, #lines do
        local name, value = lines[index]:match("^([%a%-]+):%s*(.*)$")
        if name ~= nil then
            local key = string.lower(name)
            if key == "content-length" then
                length = (tonumber(value) or -1) as integer
            end
        end
    end
    if length < 0 or length > MAX_REQUEST_BYTES then
        return nil, nil, nil, 413
    end
    local body = rest
    while #body < length do
        local chunk = socket.read(connection)
        if chunk == nil then
            return nil, nil, nil, 400
        end
        body = body .. chunk
    end
    return method, target, string.sub(body, 1, length), nil
end

local record Options
    host: string
    port: integer
    root: string
end

local function usage(): string
    return "usage: compliance_server --host 127.0.0.1 --port 8099 --root <marker dir>"
end

local function parse_options(argv: {string}): Options
    local options: Options = {
        host = "127.0.0.1",
        port = 8099,
        root = "",
    }
    local index = 1
    while index <= #argv do
        local flag = argv[index]
        local value = argv[index + 1]
        assert(value ~= nil, "compliance_server: missing value for " .. flag)
        if flag == "--host" then
            options.host = value
        elseif flag == "--port" then
            local port = tonumber(value)
            assert(
                port ~= nil and port > 0 and port < 65536,
                "compliance_server: --port is a TCP port"
            )
            options.port = port as integer
        elseif flag == "--root" then
            options.root = value
        else
            error("compliance_server: no such flag " .. flag, 0)
        end
        index = index + 2
    end
    assert(
        options.root ~= "", "compliance_server: --root is the marker-store directory"
    )
    return options
end

local function serve(connection: any, host: host_module.Host)
    local method, target, body, refusal = read_request(connection)
    if refusal ~= nil then
        socket.write(
            connection,
            response(refusal, '{"error":"the request is malformed or oversized"}')
        )
        socket.close(connection)
        return
    end
    -- The target's query is not part of the corpus contract; the route
    -- sees the path alone.
    local path = target as string
    local query = path:find("?", 1, true)
    if query ~= nil then
        path = string.sub(path, 1, query - 1)
    end
    local ok, status, payload = pcall(
        host.route, host, method as string, path, body as string
    )
    if not ok then
        socket.write(
            connection,
            response(
                500,
                '{"error":"the host refused the request: ' .. tostring(status) .. '"}'
            )
        )
        socket.close(connection)
        return
    end
    socket.write(connection, response(status as integer, payload as string))
    socket.close(connection)
    for _, line in ipairs(host:take_verdicts()) do
        io.stdout:write(line .. "\n")
    end
    io.stdout:flush()
end

local function main(options: Options)
    local properties = advisory_lock.version_properties()
    local host = host_module.host(options.root, properties["version"] or "unknown")
    local listener, listen_error = socket.listen("tcp", options.host, options.port)
    if listener == nil then
        io.stderr:write(
            "compliance_server: the corpus endpoint cannot listen on "
                .. options.host
                .. ":"
                .. tostring(options.port)
                .. ": "
                .. tostring(listen_error)
                .. "\n"
        )
        os.exit(1)
    end
    io.stdout:write(
        string.format(
            "compliance_server: uvrr-core corpus on http://%s:%d (version %s, shape %s)\n",
            options.host,
            options.port,
            properties["version"] or "unknown",
            properties["feature_shape"] or "unknown"
        )
    )
    io.stdout:flush()
    while true do
        local connection = socket.accept(listener)
        if connection ~= nil then
            lunet.spawn(function() serve(connection, host) end)
        else
            lunet.sleep(50)
        end
    end
end

local argv: {string} = {}
for index = 1, #arg do
    table.insert(argv, arg[index])
end

local ok, options = pcall(function(): Options return parse_options(argv) end)
if not ok then
    io.stderr:write(tostring(options) .. "\n" .. usage() .. "\n")
    os.exit(2)
end

lunet.spawn(function() main(options as Options) end)
