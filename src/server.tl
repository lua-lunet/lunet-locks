-- Runnable lunet entrypoint for the advisory-lock service.
--
-- This file intentionally owns only transport and client correlation.  Lock
-- ordering, duplicate suppression, clock sampling, and the durable
-- incarnation marker stay in the native adapter.
--
-- The reconfiguration admin verbs (join/increment/leave) ride the same TCP
-- client channel, the same forward/redirect machinery, and the same peer
-- envelope as the lock operations. They are operator-trusted at exactly the
-- same level as the lock verbs: no separate authentication exists on either
-- channel, and this is the deployment's stated trust model, not a gap. The
-- leader drives the reconfiguration through the native adapter; the peers
-- learn an endpoint change through the application-level ADMIN broadcast
-- (a join adds the addressing row before the commit, a leave removes it
-- after).
--
-- A restarted replica reincarnates (the core's Crash-Stop-Self-Evict
-- protocol): the native adapter classifies the durable state file dirty,
-- bumps the identity, and announces the `Reincarnation(old, new)` pair on
-- the VRR channel. The wire body IS the addressing notice — the receive
-- loop remaps the old id's endpoint row to the new id when the pair arrives
-- from the socket the deployment attributes to the old id, with no
-- application-level broadcast. The genesis membership fingerprint is
-- untouched: a restart changes no descriptor line.
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
local admin = require("admin")
local cluster_config = require("cluster_config")
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
-- The native adapter reports u32::MAX when no leader is known for an era/view
-- (booting or recovering cluster, or an era outside the retention window).
local LEADER_UNKNOWN = 4294967295

local record Options
    node: string
    client: string
    state: string
    cluster: string
    nodes: {cluster_config.ClusterNode}
    heartbeat_ms: integer
    election_ms: integer
    recovery_ms: integer
    event_journal_dir: string
    event_journal_roll_bytes: integer
end

local record Pending
    client: any
    json: string
    result: string
    done: boolean
    forwarded_to: integer
end

local record Output
    kind: string
    replica: integer
    era: integer
    view: integer
    slot: integer
    message_id: string
    bytes: string
end

local function usage(): string
    return "usage: lunet-run build/server.lua --node NAME --client IPv4:PORT "
        .. "--state PATH --cluster PATH (JSONL deployment descriptor, "
        .. "at least three entries) "
        .. "[--heartbeat-ms N] [--election-ms N] [--recovery-ms N] "
        .. "[--event-journal-dir PATH] [--event-journal-roll-bytes N]"
end

local function positive(value: string, name: string): integer
    local parsed = tonumber(value)
    assert(
        parsed ~= nil and parsed >= 1 and parsed == math.floor(parsed),
        name .. " must be positive"
    )
    return parsed as integer
end

-- The deployment descriptor is the live membership: every entry's id is the
-- replica's NodeId and its line order is the genesis succession sequence.
local function read_descriptor(path: string): {cluster_config.ClusterNode}
    local file = io.open(path, "r")
    assert(file ~= nil, "server: cannot read cluster descriptor " .. path)
    local text = file:read("a")
    file:close()
    return cluster_config.parse_jsonl(text)
end

local function parse_options(argv: {string}): Options
    local node: string = ""
    local client: string = ""
    local state: string = ""
    local cluster: string = ""
    local heartbeat_ms: integer = 200
    local election_ms: integer = 1200
    local recovery_ms: integer = 2500
    local event_journal_dir: string = ""
    local event_journal_roll_bytes: integer = 8388608
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
        elseif flag == "--cluster" then
            assert(cluster == "", "server: --cluster may appear once")
            cluster = value
        elseif flag == "--heartbeat-ms" then
            heartbeat_ms = positive(value, "server: --heartbeat-ms")
        elseif flag == "--election-ms" then
            election_ms = positive(value, "server: --election-ms")
        elseif flag == "--recovery-ms" then
            recovery_ms = positive(value, "server: --recovery-ms")
        elseif flag == "--event-journal-dir" then
            assert(
                event_journal_dir == "", "server: --event-journal-dir may appear once"
            )
            event_journal_dir = value
        elseif flag == "--event-journal-roll-bytes" then
            event_journal_roll_bytes = positive(
                value, "server: --event-journal-roll-bytes"
            )
        else
            error("server: unknown option " .. flag)
        end
        index = index + 2
    end
    assert(
        node ~= "" and client ~= "" and state ~= "" and cluster ~= "",
        "server: required options missing"
    )
    config.endpoint(client)
    local nodes = read_descriptor(cluster)
    config.members(node, nodes)
    assert(
        election_ms >= heartbeat_ms,
        "server: election timeout must be at least heartbeat"
    )
    return {
        node = node,
        client = client,
        state = state,
        cluster = cluster,
        nodes = nodes,
        heartbeat_ms = heartbeat_ms,
        election_ms = election_ms,
        recovery_ms = recovery_ms,
        event_journal_dir = event_journal_dir,
        event_journal_roll_bytes = event_journal_roll_bytes,
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
    local members = config.members(options.node, options.nodes)
    -- The deployment fingerprint is over the GENESIS membership in
    -- descriptor line order (membership/v3): stable across live superset
    -- growth, so a joining replica's descriptor never quarantines the
    -- peers it joins; a differently-founded deployment still does.
    local fingerprint = config.genesis_fingerprint(options.nodes)
    local membership_log = "advisory-lock membership fingerprint="
        .. fingerprint
        .. " encoding=lunet-advisory-lock/membership/v3 scope=genesis"
    -- Live addressing is the descriptor's admin-assigned ids: the native
    -- adapter speaks member ids on the ABI (`receive` from, send `to`,
    -- leader outs), and this table set is the id <-> endpoint view of the
    -- descriptor, grown at runtime by the admin broadcast (a joining
    -- member's row is added, a departed member's rows are removed) and by
    -- the reincarnation remap (a restarted member's row moves to its
    -- bumped id). The live identity comes from the adapter — after a dirty
    -- restart it is the bumped high-band id, not the descriptor row.
    local endpoint_by_id: {integer: string} = {}
    local id_by_endpoint: {string: integer} = {}
    local own_id = 0
    local own_rank = 0
    local udp_host = ""
    local udp_port = 0
    for rank, member in ipairs(members) do
        local key = member.endpoint.host .. ":" .. tostring(member.endpoint.port)
        endpoint_by_id[member.id] = key
        id_by_endpoint[key] = member.id
        if member.name == options.node then
            own_rank = rank - 1
            udp_host = member.endpoint.host
            udp_port = member.endpoint.port
        end
    end
    assert(udp_host ~= "", "server: own member missing")

    local node = advisory_lock.new(
        options.nodes,
        options.node,
        options.state,
        options.event_journal_dir,
        options.event_journal_roll_bytes
    )
    own_id = node:own_id()
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

    local function send_vrr(replica: integer, payload: string)
        local endpoint = endpoint_by_id[replica]
        -- A replica this process cannot address yet (a joining member whose
        -- row the admin broadcast has not reached us) is unsendable, not
        -- fatal: the datagram is dropped with a note and the core's
        -- retransmission/timers carry the protocol. The pre-broadcast race
        -- window is the only producer of such a send.
        if endpoint == nil then
            log_membership(
                "advisory-lock cannot address replica "
                    .. tostring(replica)
                    .. "; datagram dropped"
            )
            return
        end
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

    local function send_application_peer(replica: integer, payload: string)
        local endpoint = endpoint_by_id[replica]
        assert(endpoint ~= nil, "server: application output has unknown replica")
        send_application(endpoint, payload)
    end

    -- The membership change, carried to every replica this process can
    -- address. Operator-trusted: the admin verbs travel the same
    -- unauthenticated TCP client channel and the same peer envelope as the
    -- lock operations — no separate auth exists; that is the deployment's
    -- trust model, stated here as fact.
    local function broadcast_member(payload: string)
        for id, endpoint in pairs(endpoint_by_id) do
            if id ~= own_id then
                send_application(endpoint, payload)
            end
        end
    end

    -- Drives one admin verb on this node. Returns (true, ack) when the
    -- reconfiguration was proposed and its establishing operation committed
    -- (the era advances at the commit — the core's folded configuration is
    -- the commit truth), (false, "not_leader") when this node must redirect,
    -- and raises through the ABI's other refusals. A join adds the joining
    -- member's addressing row before the drive (the core's first send to
    -- the new member must resolve) and broadcasts it at proposal time —
    -- additive and idempotent at the peers, safe because the core sends
    -- nothing to a pre-commit non-member. A leave broadcasts after the
    -- commit, so a proposal that dies never strands a streaming member.
    local function handle_admin(
        action: string, id: integer, name: string, endpoint: string
    ): (boolean, string)
        if action == "join" and endpoint_by_id[id] == nil then
            endpoint_by_id[id] = endpoint
            id_by_endpoint[endpoint] = id
        end
        local driven, proposed, leader_error = pcall(
            function(): (boolean, string) return node:reconfigure(action, id) end
        )
        if not driven then
            -- A core refusal (fold gate, transition-outstanding, poisoned
            -- replica) is reported to the operator as a rejected verb, the
            -- same honesty the lock errors get: the operation never entered
            -- the log.
            return true,
                '{"action":"'
                    .. action
                    .. '","id":'
                    .. tostring(id)
                    .. ',"accepted":false}'
        end
        if proposed == nil then
            -- NotPrimary: the one actionable refusal; the caller redirects.
            return false, leader_error or "not_leader"
        end
        if action == "join" then
            broadcast_member(admin.encode_join(id, name, endpoint))
        end
        local era = node:status().era
        local waited = 0
        while node:status().era <= era and waited < CLIENT_DEADLINE_MS do
            lunet.sleep(POLL_MS)
            waited = waited + POLL_MS
        end
        if action == "leave" then
            broadcast_member(admin.encode_leave(id))
            local departed = endpoint_by_id[id]
            endpoint_by_id[id] = nil
            if departed ~= nil then
                id_by_endpoint[departed] = nil
            end
        end
        return true, admin.encode_ack(action, id)
    end

    local function flush(outputs: {Output})
        for _, output in ipairs(outputs) do
            if output.kind == "send" then
                -- The core emits one unicast send per destination; route as
                -- addressed (a self-addressed send is delivered like any other).
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

    -- A bumped boot queued its `Reincarnation(old, new)` announcement at
    -- construction (the wire phase always follows the bump); it goes out
    -- here, before any loop runs.
    flush(node:outputs() as {Output})

    local function forward_to(waiter: Pending, leader: integer, message_id: string)
        -- A booting/recovering cluster may have no known leader; leave the
        -- waiter for the deadline loop to retry rather than asserting.
        if leader == LEADER_UNKNOWN or endpoint_by_id[leader] == nil then
            return
        end
        waiter.forwarded_to = leader
        send_application_peer(leader, FORWARD_REQUEST .. message_id .. waiter.json)
    end

    local function submit(waiter: Pending, message_id: string)
        assert(
            guard:allows_client_operation(), "server: membership configuration is dirty"
        )
        -- The admin verbs (join/increment/leave) ride the same client
        -- channel and the same redirect machinery as the lock operations.
        local is_verb, verb = admin.decode_verb(waiter.json)
        if is_verb then
            local status = node:status()
            if status.leader == own_id then
                local driven, ack = handle_admin(
                    verb.action, verb.id, verb.name, verb.endpoint
                )
                if driven then
                    waiter.result = ack
                    waiter.done = true
                    waiter.forwarded_to = -1
                    return
                end
            elseif
                status.leader ~= own_id
                and status.leader ~= LEADER_UNKNOWN
                and endpoint_by_id[status.leader] ~= nil
            then
                forward_to(waiter, status.leader, message_id)
                return
            end
            -- No known leader (or this node just lost leadership): the
            -- deadline loop retries with the then-current state.
            return
        end
        local accepted, request_error = node:request(waiter.json)
        flush(node:outputs() as {Output})
        if accepted then
            waiter.forwarded_to = -1
            return
        end
        -- NOT_LEADER is non-raising: re-read status and re-forward when a
        -- valid leader is known; otherwise the deadline loop retries.
        if request_error ~= "not_leader" then
            error("server: request failed: " .. (request_error or "unknown"), 2)
        end
        local status = node:status()
        if
            status.leader ~= own_id
            and status.leader ~= LEADER_UNKNOWN
            and endpoint_by_id[status.leader] ~= nil
        then
            forward_to(waiter, status.leader, message_id)
        end
    end

    local function receive_loop()
        while true do
            local packet, host, port = udp.recv(peer)
            if packet ~= nil and host ~= nil and port ~= nil then
                local source_endpoint = host .. ":" .. tostring(port)
                if transport.source_is_member(host, port, options.nodes) then
                    local valid, kind, received_fingerprint, payload = transport.decode_peer(
                        packet
                    )
                    if valid and not guard:accept_fingerprint(received_fingerprint) then
                        quarantine(received_fingerprint)
                    elseif valid and kind == transport.PEER_APPLICATION then
                        local direction = payload:sub(1, 1)
                        if direction == admin.ADMIN_DIRECTION then
                            -- A membership change from a peer (the leader's
                            -- broadcast of a live reconfiguration). Same
                            -- validation class as the lock ops: decode
                            -- strictly, apply idempotently; a payload that does
                            -- not decode is dropped, never partially applied.
                            local change_valid, change = admin.decode_change(payload)
                            if change_valid then
                                if
                                    change.kind == "join"
                                    and endpoint_by_id[change.id] == nil
                                then
                                    endpoint_by_id[change.id] = change.endpoint
                                    id_by_endpoint[change.endpoint] = change.id
                                elseif change.kind == "leave" then
                                    local departed = endpoint_by_id[change.id]
                                    endpoint_by_id[change.id] = nil
                                    if departed ~= nil then
                                        id_by_endpoint[departed] = nil
                                    end
                                end
                            end
                        elseif
                            direction == FORWARD_REQUEST
                            and #payload > 1 + MESSAGE_ID_BYTES
                        then
                            local message_id = payload:sub(2, 1 + MESSAGE_ID_BYTES)
                            local json = payload:sub(2 + MESSAGE_ID_BYTES)
                            local status = node:status()
                            local is_verb, verb = admin.decode_verb(json)
                            if
                                is_verb
                                and guard:allows_client_operation()
                                and status.leader ~= own_id
                            then
                                -- A forwarded admin verb reaches the
                                -- leader only: redirect through the
                                -- existing machinery so the forwarder
                                -- re-resolves and re-forwards.
                                send_application(
                                    source_endpoint,
                                    transport.encode_not_leader(
                                        message_id, status.era, status.view
                                    )
                                )
                            elseif is_verb and guard:allows_client_operation() then
                                -- The poll for the establishing commit must
                                -- not stall the receive loop: the commit's
                                -- own round trip runs there.
                                lunet.spawn(
                                    function()
                                        local driven, ack = handle_admin(
                                            verb.action,
                                            verb.id,
                                            verb.name,
                                            verb.endpoint
                                        )
                                        if driven then
                                            send_application(
                                                source_endpoint,
                                                transport.FORWARD_RESPONSE
                                                    .. message_id
                                                    .. ack
                                            )
                                        else
                                            send_application(
                                                source_endpoint,
                                                transport.encode_not_leader(
                                                    message_id, status.era, status.view
                                                )
                                            )
                                        end
                                    end
                                )
                            elseif
                                guard:allows_client_operation()
                                and status.leader == own_id
                            then
                                forwarded_from[message_id] = source_endpoint
                                local ok = pcall(
                                    function()
                                        node:request(json)
                                        flush(node:outputs() as {Output})
                                    end
                                )
                                if not ok then
                                    forwarded_from[message_id] = nil
                                end
                            elseif guard:allows_client_operation() then
                                send_application(
                                    source_endpoint,
                                    transport.encode_not_leader(
                                        message_id, status.era, status.view
                                    )
                                )
                            end
                        elseif
                            direction == FORWARD_RESPONSE
                            and #payload > 1 + MESSAGE_ID_BYTES
                        then
                            local message_id = payload:sub(2, 1 + MESSAGE_ID_BYTES)
                            local waiter = pending[message_id]
                            local source_id = id_by_endpoint[source_endpoint]
                            if waiter ~= nil and source_id == waiter.forwarded_to then
                                waiter.result = payload:sub(2 + MESSAGE_ID_BYTES)
                                waiter.done = true
                            end
                        elseif direction == FORWARD_NOT_LEADER then
                            local redirect_valid, message_id, era, view = transport.decode_not_leader(
                                payload
                            )
                            local waiter = pending[message_id]
                            local source_id = id_by_endpoint[source_endpoint]
                            if
                                redirect_valid
                                and waiter ~= nil
                                and source_id ~= nil
                                and transport.valid_not_leader(
                                    waiter.forwarded_to, source_id
                                )
                            then
                                if guard:allows_client_operation() then
                                    local leader = node:leader_for_view(era, view)
                                    -- LEADER_UNKNOWN: the era/view maps to no
                                    -- known leader; the deadline loop retries.
                                    if leader ~= LEADER_UNKNOWN then
                                        forward_to(waiter, leader, message_id)
                                    end
                                end
                            end
                        end
                    elseif valid and kind == transport.PEER_VRR then
                        local replica = id_by_endpoint[source_endpoint]
                        -- The reincarnation remap: a `Reincarnation(old,
                        -- new)` announcement arriving from the socket the
                        -- deployment attributes to `old` IS the restarted
                        -- process's entry ticket — the wire body is the
                        -- notice, no application broadcast. The row moves
                        -- to the bumped id (every bumped identity lives in
                        -- the high band the native adapter's bump
                        -- arithmetic derives), and the announcement is
                        -- delivered attributed to the new identity, where
                        -- the core's own gates apply (only the leader acts;
                        -- a forged or degenerate pair is dropped by name).
                        local pair_valid, pair_old, pair_new = transport.reincarnation_pair(
                            payload
                        )
                        if
                            pair_valid
                            and replica == pair_old
                            and pair_old ~= pair_new
                            and pair_new >= 16777216
                            and endpoint_by_id[pair_new] == nil
                        then
                            endpoint_by_id[pair_new] = source_endpoint
                            endpoint_by_id[pair_old] = nil
                            id_by_endpoint[source_endpoint] = pair_new
                            replica = pair_new
                        end
                        if replica ~= nil then
                            if node:status().leader == replica then
                                leader_elapsed_ms = 0
                            end
                            local ok = pcall(
                                function()
                                    node:receive(replica, payload)
                                    flush(node:outputs() as {Output})
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
            -- The binding drives the core's single liveness input (Input::Tick);
            -- this loop supplies the heartbeat cadence.
            node:idle()
            flush(node:outputs() as {Output})
            lunet.sleep(options.heartbeat_ms)
        end
    end

    local function election_loop()
        -- Election stagger spreads the genesis succession ranks so two
        -- simultaneous timers cannot collide; the descriptor's line order
        -- (the genesis succession sequence) gives every member a stable
        -- rank.
        local stagger = own_rank * 200
        while true do
            lunet.sleep(POLL_MS)
            local status = node:status()
            if status.state == "normal" and status.leader == own_id then
                leader_elapsed_ms = 0
            else
                leader_elapsed_ms = leader_elapsed_ms + POLL_MS
                if leader_elapsed_ms >= options.election_ms + stagger then
                    leader_elapsed_ms = 0
                    -- Leader-silence detection; also drives Input::Tick.
                    node:leader_timeout()
                    flush(node:outputs() as {Output})
                end
            end
        end
    end

    local function recovery_loop()
        while true do
            lunet.sleep(options.recovery_ms)
            -- Drive a fenced-boot attempt only while the replica is
            -- recovering: the drive ticks the core (the ms clock is the
            -- only tick source) and, on a reincarnated node, re-announces
            -- its `Reincarnation(old, new)` pair — the §8 re-announce,
            -- which is what keeps a rotated leadership armed. Calling it
            -- in "normal" or "view_change" would have no protocol effect.
            if node:status().state == "recovering" then
                node:recover()
                flush(node:outputs() as {Output})
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
