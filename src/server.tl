-- Runnable lunet entrypoint for the advisory-lock service.
--
-- This file intentionally owns only transport and client correlation.  Lock
-- ordering, duplicate suppression, clock sampling, and recovery nonces stay
-- in the native adapter.
local record DebugInfo
    source: string
end

-- lunet-run does not add the script directory to package.path.  The service
-- is invoked as `lunet-run build/server.lua`, so make sibling Cyan output
-- available before loading it.
local source = (debug.getinfo(1, "S") as DebugInfo).source
local directory = source:match("^@(.+)/[^/]+$") or "."
package.path = directory .. "/?.lua;" .. package.path

local advisory_lock = require("advisory_lock")
local config = require("config")
local lunet = require("lunet")
local membership_guard = require("membership_guard")
local socket = require("lunet.socket")
local udp = require("lunet.udp")
local transport = require("transport")

local FORWARD_REQUEST = transport.FORWARD_REQUEST
local FORWARD_RESPONSE = transport.FORWARD_RESPONSE
local FORWARD_NOT_LEADER = transport.FORWARD_NOT_LEADER
local MESSAGE_ID_BYTES = transport.MESSAGE_ID_BYTES
local CLIENT_DEADLINE_MS = 30000
local POLL_MS = 10
local MAX_CLIENT_REQUEST = transport.MAX_DATAGRAM - transport.PEER_HEADER_BYTES - 1 - MESSAGE_ID_BYTES

local record Options
    node: string
    client: string
    state: string
    members: {string}
    heartbeat_ms: integer
    election_ms: integer
    recovery_ms: integer
end

local record Pending
    client: any
    json: string
    result: string
    done: boolean
    forwarded_to: integer
    redirect_epoch: integer
end

local record Output
    kind: string
    replica: integer
    message_id: string
    bytes: string
end

local function usage(): string
    return "usage: lunet-run build/server.lua --node NAME --client IPv4:PORT "
        .. "--state PATH --member NAME=IPv4:PORT (at least three) "
        .. "[--heartbeat-ms N] [--election-ms N] [--recovery-ms N]"
end

local function positive(value: string, name: string): integer
    local parsed = tonumber(value)
    assert(
        parsed ~= nil and parsed >= 1 and parsed == math.floor(parsed),
        name .. " must be positive"
    )
    return parsed as integer
end

local function parse_options(argv: {string}): Options
    local node: string = ""
    local client: string = ""
    local state: string = ""
    local members: {string} = {}
    local heartbeat_ms: integer = 200
    local election_ms: integer = 1200
    local recovery_ms: integer = 2500
    local index = 1
    while index <= #argv do
        local flag = argv[index]
        local value = argv[index + 1]
        assert(value ~= nil, "server: missing value for " .. flag)
        if flag == "--node" then
            assert(node == "", "server: --node may appear once")
            node = value
        elseif flag == "--client" then
            assert(client == "", "server: --client may appear once")
            client = value
        elseif flag == "--state" then
            assert(state == "", "server: --state may appear once")
            state = value
        elseif flag == "--member" then
            table.insert(members, value)
        elseif flag == "--heartbeat-ms" then
            heartbeat_ms = positive(value, "server: --heartbeat-ms")
        elseif flag == "--election-ms" then
            election_ms = positive(value, "server: --election-ms")
        elseif flag == "--recovery-ms" then
            recovery_ms = positive(value, "server: --recovery-ms")
        else
            error("server: unknown option " .. flag)
        end
        index = index + 2
    end
    assert(
        node ~= "" and client ~= "" and state ~= "", "server: required options missing"
    )
    config.endpoint(client)
    config.members(node, members)
    assert(
        election_ms >= heartbeat_ms,
        "server: election timeout must be at least heartbeat"
    )
    return {
        node = node,
        client = client,
        state = state,
        members = members,
        heartbeat_ms = heartbeat_ms,
        election_ms = election_ms,
        recovery_ms = recovery_ms,
    }
end

local function uuid_bytes(json: string): string
    local uuid = json:match('"message_id"%s*:%s*"([0-9a-fA-F%-]+)"')
    assert(uuid ~= nil and #uuid == 36, "server: request has no UUID message_id")
    local hex = uuid:gsub("-", "")
    assert(#hex == 32 and hex:find("[^0-9a-fA-F]") == nil, "server: invalid message_id")
    local bytes = ""
    for index = 1, #hex, 2 do
        local value = tonumber(hex:sub(index, index + 1), 16)
        assert(value ~= nil, "server: invalid message_id")
        bytes = bytes .. string.char(value)
    end
    return bytes
end

local function split_endpoint(value: string): (string, integer)
    local parsed = config.endpoint(value)
    return parsed.host, parsed.port
end

local function log_membership(message: string)
    io.stdout:write(message .. "\n")
    io.stdout:flush()
    io.stderr:write(message .. "\n")
    io.stderr:flush()
end

local function main(options: Options)
    local client_host, client_port = split_endpoint(options.client)
    local members = config.members(options.node, options.members)
    local member_names: {string} = {}
    local index_by_endpoint: {string: integer} = {}
    local endpoint_by_index: {integer: string} = {}
    local own_index = 0
    local udp_host = ""
    local udp_port = 0
    for index, member in ipairs(members) do
        table.insert(member_names, member.name)
        local key = member.endpoint.host .. ":" .. tostring(member.endpoint.port)
        index_by_endpoint[key] = index - 1
        endpoint_by_index[index - 1] = key
        if member.name == options.node then
            own_index = index - 1
            udp_host = member.endpoint.host
            udp_port = member.endpoint.port
        end
    end
    assert(own_index >= 0, "server: own member missing")

    local node = advisory_lock.new(member_names, options.node, options.state)
    local fingerprint = config.membership_fingerprint(members)
    local membership_log = "advisory-lock membership fingerprint="
        .. fingerprint
        .. " encoding=lunet-advisory-lock/membership/v1"
    log_membership(membership_log)
    local peer, peer_error = udp.bind(udp_host, udp_port)
    assert(peer ~= nil, "server: UDP bind failed: " .. (peer_error or "unknown"))
    local listener, listen_error = socket.listen("tcp", client_host, client_port)
    assert(
        listener ~= nil, "server: TCP listen failed: " .. (listen_error or "unknown")
    )

    local pending: {string: Pending} = {}
    local forwarded_from: {string: string} = {}
    local active_clients: {string: any} = {}
    local leader_elapsed_ms = 0
    local guard = membership_guard.new(fingerprint)
    local listener_open = true

    local function close_client(client: any)
        pcall(function() socket.close(client) end)
    end

    local function quarantine(received_fingerprint: string)
        if guard.dirty then
            return
        end
        guard.dirty = true
        local message = "advisory-lock dirty membership configured="
            .. fingerprint
            .. " received="
            .. received_fingerprint
            .. "; closing client listener"
        log_membership(message)
        if listener_open then
            listener_open = false
            close_client(listener)
        end
        for _, client in pairs(active_clients) do
            close_client(client)
        end
        for message_id, waiter in pairs(pending) do
            close_client(waiter.client)
            pending[message_id] = nil
        end
    end

    local function send_endpoint(endpoint: string, payload: string)
        local host, port = split_endpoint(endpoint)
        local ok, send_error = udp.send(peer, host, port, payload)
        assert(ok ~= nil, "server: UDP send failed: " .. (send_error or "unknown"))
    end

    local function send_vrr(index: integer, payload: string)
        local endpoint = endpoint_by_index[index]
        assert(endpoint ~= nil, "server: native output has unknown replica")
        send_endpoint(
            endpoint, transport.encode_peer(transport.PEER_VRR, fingerprint, payload)
        )
    end

    local function send_application(endpoint: string, payload: string)
        send_endpoint(
            endpoint,
            transport.encode_peer(transport.PEER_APPLICATION, fingerprint, payload)
        )
    end

    local function send_application_peer(index: integer, payload: string)
        local endpoint = endpoint_by_index[index]
        assert(endpoint ~= nil, "server: application output has unknown replica")
        send_application(endpoint, payload)
    end

    local function flush(outputs: {Output})
        for _, output in ipairs(outputs) do
            if output.kind == "broadcast" then
                for index = 0, #members - 1 do
                    if index ~= own_index then
                        send_vrr(index, output.bytes)
                    end
                end
            elseif output.kind == "send" then
                send_vrr(output.replica, output.bytes)
            elseif output.kind == "reply" then
                local origin = forwarded_from[output.message_id]
                if origin ~= nil then
                    forwarded_from[output.message_id] = nil
                    send_application(
                        origin, FORWARD_RESPONSE .. output.message_id .. output.bytes
                    )
                else
                    local waiter = pending[output.message_id]
                    if waiter ~= nil then
                        waiter.result = output.bytes
                        waiter.done = true
                    end
                end
            end
        end
    end

    local function forward_to(waiter: Pending, leader: integer, message_id: string)
        assert(leader >= 0 and leader < #members, "server: no current leader")
        waiter.forwarded_to = leader
        send_application_peer(leader, FORWARD_REQUEST .. message_id .. waiter.json)
    end

    local function submit(waiter: Pending, message_id: string)
        assert(
            guard:allows_client_operation(), "server: membership configuration is dirty"
        )
        local status = node:status()
        if status.leader == own_index then
            waiter.forwarded_to = -1
            flush(node:request(waiter.json) as {Output})
            return
        end
        forward_to(waiter, status.leader, message_id)
    end

    local function receive_loop()
        while true do
            local packet, host, port = udp.recv(peer)
            if packet ~= nil and host ~= nil and port ~= nil then
                local source_endpoint = host .. ":" .. tostring(port)
                if transport.source_is_member(host, port, options.members) then
                    local valid, kind, received_fingerprint, payload = transport.decode_peer(
                        packet
                    )
                    if valid and not guard:accept_fingerprint(received_fingerprint) then
                        quarantine(received_fingerprint)
                    elseif valid and kind == transport.PEER_APPLICATION then
                        local direction = payload:sub(1, 1)
                        if
                            direction == FORWARD_REQUEST
                            and #payload > 1 + MESSAGE_ID_BYTES
                        then
                            local message_id = payload:sub(2, 1 + MESSAGE_ID_BYTES)
                            local json = payload:sub(2 + MESSAGE_ID_BYTES)
                            local status = node:status()
                            if
                                guard:allows_client_operation()
                                and status.leader == own_index
                            then
                                forwarded_from[message_id] = source_endpoint
                                local ok = pcall(
                                    function() flush(node:request(json) as {Output}) end
                                )
                                if not ok then
                                    forwarded_from[message_id] = nil
                                end
                            elseif guard:allows_client_operation() then
                                send_application(
                                    source_endpoint,
                                    transport.encode_not_leader(
                                        message_id, status.epoch
                                    )
                                )
                            end
                        elseif
                            direction == FORWARD_RESPONSE
                            and #payload > 1 + MESSAGE_ID_BYTES
                        then
                            local message_id = payload:sub(2, 1 + MESSAGE_ID_BYTES)
                            local waiter = pending[message_id]
                            local source_index = index_by_endpoint[source_endpoint]
                            if
                                waiter ~= nil and source_index == waiter.forwarded_to
                            then
                                waiter.result = payload:sub(2 + MESSAGE_ID_BYTES)
                                waiter.done = true
                            end
                        elseif direction == FORWARD_NOT_LEADER then
                            local redirect_valid, message_id, epoch = transport.decode_not_leader(
                                payload
                            )
                            local waiter = pending[message_id]
                            local source_index = index_by_endpoint[source_endpoint]
                            if
                                redirect_valid
                                and waiter ~= nil
                                and source_index ~= nil
                                and transport.valid_not_leader(
                                    waiter.forwarded_to, source_index
                                )
                            then
                                if guard:allows_client_operation() then
                                    waiter.redirect_epoch = epoch
                                    local leader = node:leader_for_epoch(epoch)
                                    local ok = pcall(
                                        function()
                                            forward_to(waiter, leader, message_id)
                                        end
                                    )
                                    if not ok then
                                        -- A concurrent leader change is retried by the deadline loop.
                                    end
                                end
                            end
                        end
                    elseif valid and kind == transport.PEER_VRR then
                        local replica = index_by_endpoint[source_endpoint]
                        if replica ~= nil then
                            if node:status().leader == replica then
                                leader_elapsed_ms = 0
                            end
                            local ok = pcall(
                                function()
                                    flush(node:receive(replica, payload) as {Output})
                                end
                            )
                            if not ok then
                                -- Untrusted/malformed peer datagrams are discarded.
                            end
                        end
                    end
                end
            end
            lunet.sleep(POLL_MS)
        end
    end

    local function heartbeat_loop()
        while true do
            flush(node:idle() as {Output})
            lunet.sleep(options.heartbeat_ms)
        end
    end

    local function election_loop()
        local stagger = own_index * 200
        while true do
            lunet.sleep(POLL_MS)
            local status = node:status()
            if status.state == "normal" and status.leader == own_index then
                leader_elapsed_ms = 0
            else
                leader_elapsed_ms = leader_elapsed_ms + POLL_MS
                if leader_elapsed_ms >= options.election_ms + stagger then
                    leader_elapsed_ms = 0
                    flush(node:leader_timeout() as {Output})
                end
            end
        end
    end

    local function recovery_loop()
        while true do
            lunet.sleep(options.recovery_ms)
            if node:status().state ~= "normal" then
                flush(node:recover() as {Output})
            end
        end
    end

    local function wait_for_reply(client: any, json: string): boolean
        if not guard:allows_client_operation() then
            return false
        end
        local message_id = uuid_bytes(json)
        local waiter = pending[message_id]
        local submit_request = waiter == nil
        if waiter == nil then
            waiter = {
                client = client,
                json = json,
                result = "",
                done = false,
                forwarded_to = -1,
                redirect_epoch = 0,
            }
            pending[message_id] = waiter
        end
        local attempts = 0
        while
            guard:allows_client_operation()
            and not waiter.done
            and attempts < CLIENT_DEADLINE_MS / options.heartbeat_ms
        do
            if submit_request then
                local ok = pcall(function() submit(waiter, message_id) end)
                if not ok then
                    -- A leader may be unavailable while an election/recovery progresses.
                end
            end
            local slept = 0
            while
                guard:allows_client_operation()
                and not waiter.done
                and slept < options.heartbeat_ms
            do
                lunet.sleep(POLL_MS)
                slept = slept + POLL_MS
            end
            attempts = attempts + 1
        end
        if pending[message_id] == waiter then
            pending[message_id] = nil
        end
        if guard:allows_client_operation() and waiter.done then
            local write_error = socket.write(client, waiter.result .. "\n")
            return write_error == nil
        end
        return false
    end

    local function serve_client(client: any)
        local client_key = tostring(client)
        active_clients[client_key] = client
        local buffer = ""
        while true do
            local chunk = socket.read(client)
            if chunk == nil then
                active_clients[client_key] = nil
                close_client(client)
                return
            end
            local ok, messages, tail = pcall(
                function(): ({string}, string)
                    return transport.feed_ndjson(buffer, chunk, MAX_CLIENT_REQUEST)
                end
            )
            if not ok then
                active_clients[client_key] = nil
                close_client(client)
                return
            end
            buffer = tail as string
            for _, json in ipairs(messages as {string}) do
                local sent = false
                local waited = pcall(function() sent = wait_for_reply(client, json) end)
                if not waited or not sent then
                    active_clients[client_key] = nil
                    close_client(client)
                    return
                end
            end
        end
    end

    lunet.spawn(receive_loop)
    lunet.spawn(heartbeat_loop)
    lunet.spawn(election_loop)
    lunet.spawn(recovery_loop)
    while true do
        if not guard:allows_client_operation() or not listener_open then
            lunet.sleep(1000)
        else
            local client = socket.accept(listener)
            if client ~= nil then
                lunet.spawn(function() serve_client(client) end)
            end
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
