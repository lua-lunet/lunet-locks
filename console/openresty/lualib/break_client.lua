-- Cosocket client for the privileged BREAK op (item28).
--
-- Talks the documented client protocol (docs/src/client-protocol.md): TCP
-- NDJSON, one request waits one reply. Pure request-build/reply-parse
-- functions are ngx-free and unit-tested in console/tests/break_client_test.lua;
-- the transport (`break_lock`) is a thin ngx cosocket adapter.
--
-- SECURITY: the core performs NO authorization on BREAK — any client that can
-- reach the protocol can break any lock. Authorization is owned by this admin
-- edge: loopback bind + `auth_basic` are the current bar. Production
-- deployments must terminate mTLS/RBAC in front of this handler, and this
-- route must never be exposed without `auth_basic` (or stronger) in front.
--
-- client_id: we use the reserved admin id 4294967295 (2^32-1, max u32).
-- Replication deduplicates by (client_id, request_num); a high reserved id
-- keeps edge-issued breaks out of the id space of real lease clients, which
-- allocate from small sequential ids. request_num comes from
-- ngx.shared.lockadmin via dict:incr so it survives across requests within a
-- worker.
--
-- Redirects: the pinned vrr-core maps epoch → leader by
-- `epoch % membership_size` over the ordered membership (verified in
-- vrr-core src/vrr.rs `n()`: `epoch % self.members.len()`, surfaced through
-- src/advisory_lock.tl `Node:leader_for_epoch` and used by src/server.tl's
-- FORWARD_NOT_LEADER path; the internal redirect carries (message_id, epoch)
-- per src/transport.tl decode_not_leader). We follow at most 4 not_leader
-- redirects using that mapping before giving up.
--
-- Reply mapping (mirrors the mock's break route in console/mock/admin.mjs):
--   broken:true  → ok, {broken=true, lease=...}
--   broken:false → not-held (core treats break on a missing/expired lock as
--                  idempotent broken:false with lease null — NOT a 404)
--   not_leader   → redirect (up to 4)
--   garbage      → malformed reply error
-- Unreachable / leaderless after redirects → cluster unavailable (502).

local break_client = {}

break_client.ADMIN_CLIENT_ID = 4294967295 -- 2^32 - 1, reserved admin id
break_client.MAX_REDIRECTS = 4
break_client.TIMEOUT_MS = 2000

-- ---- pure helpers -----------------------------------------------------------

-- leader_for_epoch(epoch, member_count) → 0-based leader index into the
-- ordered membership. nil when member_count == 0 (leaderless/unknown).
function break_client.leader_for_epoch(epoch, member_count)
    if member_count == nil or member_count <= 0 then
        return nil
    end
    return epoch % member_count
end

-- RFC 4122 version-4 UUID. `rand16` returns an integer 0..65535; injectable
-- for tests, defaults to math.random.
function break_client.new_uuid4(rand16)
    rand16 = rand16 or function()
        return math.random(0, 65535)
    end
    local function hex4()
        return string.format("%04x", rand16() % 65536)
    end
    local a = hex4() .. hex4()
    local b = hex4()
    local c = string.format("%04x", (rand16() % 4096) + 0x4000) -- version 4
    local d = string.format("%04x", (rand16() % 16384) + 0x8000) -- variant 10xx
    local e = hex4() .. hex4() .. hex4()
    return a .. "-" .. b .. "-" .. c .. "-" .. d .. "-" .. e
end

-- Increasing request_num, persisted in the shared dict (same injectable dict
-- interface as handlers.lua: dict:incr(key, delta, init)).
function break_client.next_request_num(dict)
    return dict:incr("break:request_num", 1, 0)
end

-- build_request(lock_id, message_id, client_id, request_num) → JSON line
-- (no trailing newline). Numbers only, so string.format suffices; field
-- order is fixed but irrelevant per protocol.
function break_client.build_request(lock_id, message_id, client_id, request_num)
    return string.format(
        '{"op":"break","message_id":"%s","client_id":%d,"request_num":%d,"lock_id":%d}',
        message_id,
        client_id,
        request_num,
        lock_id
    )
end

-- parse_reply(line) → table | nil, err
--   {kind="reply", broken=boolean, lease_id=number|nil, event=string|nil}
--   {kind="not_leader", epoch=number}
-- Malformed input yields nil, "malformed reply".
function break_client.parse_reply(line)
    if type(line) ~= "string" then
        return nil, "malformed reply"
    end
    local epoch = line:match('"epoch"%s*:%s*(%d+)')
    if line:find("not_leader", 1, true) then
        if epoch == nil then
            return nil, "malformed reply"
        end
        return { kind = "not_leader", epoch = tonumber(epoch) }
    end
    local broken = line:match('"broken"%s*:%s*(%a+)')
    if broken == "true" or broken == "false" then
        local lease_id = line:match('"lease_id"%s*:%s*(%d+)')
        local event = line:match('"event"%s*:%s*"([^"]*)"')
        return {
            kind = "reply",
            broken = (broken == "true"),
            lease_id = lease_id ~= nil and tonumber(lease_id) or nil,
            event = event,
        }
    end
    return nil, "malformed reply"
end

-- parse_cluster(text) → ordered members [{name=, host=, port=}...].
-- Tolerant string parse of cluster.json (item30 owns the real format):
-- members = [{name=..., client="host:port", ...}, ...]. Returns nil on
-- anything unparseable so the caller can map it to 502.
function break_client.parse_cluster(text)
    if type(text) ~= "string" then
        return nil
    end
    local members = {}
    for name, client in
        text:gmatch('"name"%s*:%s*"([^"]+)"[^{}]*"client"%s*:%s*"([^"]+)"')
    do
        local host, port = client:match("^(.-):(%d+)$")
        if host ~= nil then
            members[#members + 1] = {
                name = name,
                host = host,
                port = tonumber(port),
            }
        end
    end
    -- Also tolerate client-before-name within a member object.
    if #members == 0 then
        for client, name in
            text:gmatch('"client"%s*:%s*"([^"]+)"[^{}]*"name"%s*:%s*"([^"]+)"')
        do
            local host, port = client:match("^(.-):(%d+)$")
            if host ~= nil then
                members[#members + 1] = {
                    name = name,
                    host = host,
                    port = tonumber(port),
                }
            end
        end
    end
    if #members == 0 then
        return nil
    end
    return members
end

-- ---- ngx cosocket transport --------------------------------------------------

-- One NDJSON round-trip against members[index+1]. Returns the raw reply line
-- or nil, err.
local function exchange(sock, member, request_line, timeout_ms)
    sock:settimeout(timeout_ms)
    local ok, err = sock:connect(member.host, member.port)
    if ok == nil then
        return nil, err or "connect failed"
    end
    local sent, serr = sock:send(request_line .. "\n")
    if sent == nil then
        sock:close()
        return nil, serr or "send failed"
    end
    local line, rerr = sock:receive("*l")
    sock:close()
    if line == nil then
        return nil, rerr or "receive failed"
    end
    return line
end

-- break_lock(lock_id, cluster, opts) → result | nil, err
--   cluster = {members = [{name=, client="host:port", ...}...]} (parsed form
--             as returned by parse_cluster, or the raw cluster.json members).
--   opts = { dict= (request_num counter), uuid4= (uuid factory, tests),
--            socket_tcp= (ngx.socket.tcp factory, tests) }
-- result = { broken=boolean, lease_id=number|nil, event=string|nil }
-- err    = { status=502|0, error=string }
function break_client.break_lock(lock_id, cluster, opts)
    opts = opts or {}
    local dict = opts.dict
    local uuid4 = opts.uuid4 or break_client.new_uuid4
    local socket_tcp = opts.socket_tcp
        or function()
            return ngx.socket.tcp()
        end

    local members = cluster and cluster.members or cluster
    if type(members) ~= "table" or #members == 0 then
        return nil, { status = 502, error = "cluster unavailable" }
    end
    -- Normalize raw cluster.json members ("client":"host:port") if needed.
    if members[1].host == nil then
        local norm = {}
        for _, m in ipairs(members) do
            local host, port = tostring(m.client or ""):match("^(.-):(%d+)$")
            if host == nil then
                return nil, { status = 502, error = "cluster unavailable" }
            end
            norm[#norm + 1] = {
                name = m.name,
                host = host,
                port = tonumber(port),
            }
        end
        members = norm
    end

    local request_num = dict ~= nil and break_client.next_request_num(dict) or 1
    local request_line = break_client.build_request(
        lock_id,
        uuid4(),
        break_client.ADMIN_CLIENT_ID,
        request_num
    )

    local index = 0 -- start at the first ordered member
    for _ = 0, break_client.MAX_REDIRECTS do
        local member = members[index + 1]
        if member == nil then
            return nil, { status = 502, error = "cluster unavailable" }
        end
        local sock = socket_tcp()
        local line, xerr = exchange(sock, member, request_line, break_client.TIMEOUT_MS)
        if line == nil then
            -- Unreachable node: try the next member rather than failing fast —
            -- a dead follower must not mask a live leader.
            index = (index + 1) % #members
            if xerr == "timeout" then
                -- keep cycling; deadline is bounded by MAX_REDIRECTS+1 tries
            end
        else
            local reply, perr = break_client.parse_reply(line)
            if reply == nil then
                return nil, { status = 502, error = "malformed cluster reply: " .. perr }
            end
            if reply.kind == "reply" then
                return reply
            end
            local leader = break_client.leader_for_epoch(reply.epoch, #members)
            if leader == nil then
                return nil, { status = 502, error = "cluster unavailable" }
            end
            index = leader
        end
    end
    return nil, { status = 502, error = "cluster unavailable" }
end

return break_client
