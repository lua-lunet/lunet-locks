-- Demo workload generator (item29).
--
-- Drives live client-protocol traffic against the cluster from inside
-- OpenResty so the console charts/table are non-empty. One driver timer per
-- worker (created in init_worker context, ngx.worker.id() == 0 guard); all
-- network I/O uses ngx cosockets, which are allowed in timer context.
--
-- Pure pieces (pool, pick_action, build_request, parse_reply,
-- update_holdings) are ngx-free and unit-tested in
-- console/tests/workload_test.lua; start() is the thin ngx adapter.
--
-- Reuses break_client (item28) for: new_uuid4 (holder/message ids),
-- parse_cluster (tolerant cluster.json parse), leader_for_epoch (redirect
-- mapping, epoch % membership_size) and the dict:incr request_num pattern.
-- break_client.break_lock itself is not reused: it hardcodes op=break with
-- the reserved admin client_id; the redirect-following exchange here mirrors
-- it but carries SET/GET/RELEASE envelopes with this worker's own
-- client_id = 1000000 + ngx.worker.id().
--
-- Protocol notes (docs/src/client-protocol.md): renewal = SET by the same
-- holder with a fresh expiry; replication dedups by (client_id, request_num)
-- so request_num must be strictly increasing per client_id (dict-backed).
-- cluster.json is owned by item30; the path is injected via config and a
-- missing/unparseable file is tolerated (log once, retry next tick).
--
-- The module does NOT self-start on require; the template (item30) calls
-- workload.start{enabled=...} from init_worker_by_lua*.

local break_client = require("break_client")

local workload = {}

workload.INTERVAL_MS = 700
workload.BACKOFF_MS = 5000
workload.MAX_TRIES = 5 -- initial try + not_leader redirects / dead members
workload.TIMEOUT_MS = 2000
workload.DEFAULT_CLUSTER_PATH = "console/tmp/cluster.json"

-- ---- pure: deterministic lock pool ------------------------------------------

local LABEL_POOL = { "leader", "ingest", "shard", "eu", "dev", "zk", "demo", "batch" }

-- pool() → 12 entries {lock_id=9001..9012, name=zk-style path, labels={..}}.
-- Deterministic: same table contents on every call. Every name satisfies
-- ^/(?:[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)$ (<=128 bytes) and every label
-- satisfies ^[a-z0-9](?:[-a-z0-9]{0,30}[a-z0-9])?$ (1-32 bytes, <=8/lock).
function workload.pool()
    local pool = {}
    for i = 1, 6 do
        pool[#pool + 1] = {
            lock_id = 9000 + i,
            name = string.format("/cluster/members/%06d", i),
            labels = { LABEL_POOL[1], LABEL_POOL[6] }, -- leader, zk
        }
    end
    for i = 1, 6 do
        pool[#pool + 1] = {
            lock_id = 9006 + i,
            name = string.format("/tenants/acme-corp/ingest/shard-%02d", i),
            labels = { LABEL_POOL[2], LABEL_POOL[3], LABEL_POOL[((i - 1) % 4) + 4] },
        }
    end
    return pool
end

-- ---- pure: action picker ------------------------------------------------------

-- pick_action(rng, holdings) → action
--   rng      : math.random-compatible (rng(n) → 1..n); injectable for tests.
--   holdings : lock_id → {holder=uuid, lease_id=number} for locks "we" hold.
-- action = { kind="acquire"|"renew"|"release"|"get", lock_id=, name=,
--            labels=, holding=(holdings entry for renew/release) }
-- Renew/release only ever target locks present in holdings; acquire only
-- targets locks absent from holdings.
function workload.pick_action(rng, holdings)
    rng = rng or math.random
    holdings = holdings or {}
    local pool = workload.pool()

    local held_ids = {}
    for lock_id in pairs(holdings) do
        held_ids[#held_ids + 1] = lock_id
    end
    local by_id = {}
    local free = {}
    for _, entry in ipairs(pool) do
        by_id[entry.lock_id] = entry
        if holdings[entry.lock_id] == nil then
            free[#free + 1] = entry
        end
    end

    -- Weighted choice; renew/release exist only when something is held,
    -- acquire only when something is free, get always.
    local choices = {}
    local total = 0
    local function add(kind, weight)
        choices[#choices + 1] = { kind = kind, weight = weight }
        total = total + weight
    end
    if #free > 0 then
        add("acquire", 40)
    end
    if #held_ids > 0 then
        add("renew", 25)
        add("release", 15)
    end
    add("get", 20)

    local roll = rng(total)
    local acc = 0
    local kind = choices[#choices].kind
    for _, c in ipairs(choices) do
        acc = acc + c.weight
        if roll <= acc then
            kind = c.kind
            break
        end
    end

    if kind == "acquire" then
        local entry = free[rng(#free)]
        return {
            kind = "acquire",
            lock_id = entry.lock_id,
            name = entry.name,
            labels = entry.labels,
        }
    elseif kind == "renew" or kind == "release" then
        local lock_id = held_ids[rng(#held_ids)]
        local entry = by_id[lock_id]
        return {
            kind = kind,
            lock_id = lock_id,
            name = entry.name,
            labels = entry.labels,
            holding = holdings[lock_id],
        }
    else
        local entry = pool[rng(#pool)]
        return {
            kind = "get",
            lock_id = entry.lock_id,
            name = entry.name,
            labels = entry.labels,
        }
    end
end

-- ---- pure: request building ---------------------------------------------------

-- build_request(action, ids) → JSON line (no trailing newline).
-- ids = { message_id=, client_id=, request_num=, holder= (uuid, set/release),
--         lease_id= (set/release), expiry_ms= (set) }
function workload.build_request(action, ids)
    local base = string.format(
        '"message_id":"%s","client_id":%d,"request_num":%d,"lock_id":%d',
        ids.message_id,
        ids.client_id,
        ids.request_num,
        action.lock_id
    )
    if action.kind == "get" then
        return string.format('{"op":"get",%s}', base)
    elseif action.kind == "release" then
        return string.format(
            '{"op":"release",%s,"holder":"%s","lease_id":%d}',
            base,
            ids.holder,
            ids.lease_id
        )
    else -- acquire / renew → SET
        local labels = {}
        for i, l in ipairs(action.labels) do
            labels[i] = '"' .. l .. '"'
        end
        return string.format(
            '{"op":"set",%s,"lease":{"lease_id":%d,"holder":"%s","expiry":%d},'
                .. '"name":"%s","labels":[%s]}',
            base,
            ids.lease_id,
            ids.holder,
            ids.expiry_ms,
            action.name,
            table.concat(labels, ",")
        )
    end
end

-- ---- pure: reply parsing --------------------------------------------------------

-- parse_reply(line) → table | nil, err
--   {kind="set", granted=boolean, event=string|nil, lease_id=number|nil,
--    holder=string|nil}
--   {kind="release", released=boolean}
--   {kind="get", holder=string|nil, lease_id=number|nil}  -- holder nil = free
--   {kind="not_leader", epoch=number}
function workload.parse_reply(line)
    if type(line) ~= "string" then
        return nil, "malformed reply"
    end
    if line:find("not_leader", 1, true) then
        local epoch = line:match('"epoch"%s*:%s*(%d+)')
        if epoch == nil then
            return nil, "malformed reply"
        end
        return { kind = "not_leader", epoch = tonumber(epoch) }
    end
    local granted = line:match('"granted"%s*:%s*(%a+)')
    if granted == "true" or granted == "false" then
        local lease_id = line:match('"lease_id"%s*:%s*(%d+)')
        local holder = line:match('"holder"%s*:%s*"([^"]+)"')
        return {
            kind = "set",
            granted = (granted == "true"),
            event = line:match('"event"%s*:%s*"([^"]*)"'),
            lease_id = lease_id ~= nil and tonumber(lease_id) or nil,
            holder = holder,
        }
    end
    local released = line:match('"released"%s*:%s*(%a+)')
    if released == "true" or released == "false" then
        return { kind = "release", released = (released == "true") }
    end
    if line:match('"op"%s*:%s*"get"') ~= nil then
        local free = line:find('"lease":null', 1, true)
            or line:find('"lease"%s*:%s*null')
        local lease_id = line:match('"lease_id"%s*:%s*(%d+)')
        local holder = line:match('"holder"%s*:%s*"([^"]+)"')
        return {
            kind = "get",
            holder = free and nil or holder,
            lease_id = free and nil or (lease_id ~= nil and tonumber(lease_id) or nil),
        }
    end
    return nil, "malformed reply"
end

-- ---- pure: holdings bookkeeping --------------------------------------------------

-- update_holdings(holdings, action, reply, holder) — mutates holdings in place.
-- holder is this worker's own holder uuid. After any reply that shows we do
-- not (or no longer) hold the lock, the entry is dropped so future
-- renew/release picks stay protocol-valid.
function workload.update_holdings(holdings, action, reply, holder)
    if reply == nil or action == nil then
        return
    end
    if reply.kind == "set" then
        if reply.granted then
            holdings[action.lock_id] = {
                holder = holder,
                lease_id = reply.lease_id or (action.holding and action.holding.lease_id) or 0,
            }
        else
            holdings[action.lock_id] = nil -- denied: someone else holds it
        end
    elseif reply.kind == "release" then
        holdings[action.lock_id] = nil -- released, or mismatched → we don't hold it
    elseif reply.kind == "get" then
        if reply.holder == nil or reply.holder ~= holder then
            holdings[action.lock_id] = nil
        else
            local h = holdings[action.lock_id]
            if h ~= nil and reply.lease_id ~= nil then
                h.lease_id = reply.lease_id
            end
        end
    end
end

-- ---- ngx side ----------------------------------------------------------------------

local function log_warn(state, msg)
    ngx.log(ngx.WARN, "workload: ", msg)
    state.warned = true
end

-- Load cluster.json members (tolerant); nil when missing/unparseable.
local function load_members(state)
    local f = io.open(state.cluster_path, "r")
    if f == nil then
        return nil
    end
    local text = f:read("*a")
    f:close()
    return break_client.parse_cluster(text)
end

local function next_request_num(state)
    if state.dict ~= nil then
        return state.dict:incr("workload:request_num", 1, 0)
    end
    state.local_rn = (state.local_rn or 0) + 1
    return state.local_rn
end

-- One NDJSON round-trip against members[index+1] (mirrors break_client's
-- exchange; break_client's own is file-local and break-specific).
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

-- Send request_line to a random member, following not_leader redirects via
-- break_client.leader_for_epoch. Returns parsed reply or nil, err.
local function send_request(state, request_line)
    local members = state.members
    local index = math.random(#members) - 1 -- random entry point, 0-based
    for _ = 1, workload.MAX_TRIES do
        local member = members[index + 1]
        if member == nil then
            return nil, "cluster unavailable"
        end
        local sock = ngx.socket.tcp()
        local line, xerr = exchange(sock, member, request_line, workload.TIMEOUT_MS)
        if line == nil then
            index = (index + 1) % #members -- dead member: try the next one
            if xerr == nil then
                return nil, "exchange failed"
            end
        else
            local reply, perr = workload.parse_reply(line)
            if reply == nil then
                return nil, "malformed cluster reply: " .. (perr or "?")
            end
            if reply.kind ~= "not_leader" then
                return reply
            end
            local leader = break_client.leader_for_epoch(reply.epoch, #members)
            if leader == nil then
                return nil, "cluster unavailable"
            end
            index = leader
        end
    end
    return nil, "cluster unavailable"
end

local function tick(state, premature)
    if premature then
        return
    end
    local now = ngx.now()
    if now >= (state.backoff_until or 0) then
        if state.members == nil then
            local members = load_members(state)
            if members == nil then
                if not state.warned then
                    log_warn(
                        state,
                        "cluster.json unavailable at " .. state.cluster_path .. "; retrying"
                    )
                end
            else
                state.members = members
                state.warned = false
            end
        end
        if state.members ~= nil then
            local action = workload.pick_action(math.random, state.holdings)
            local request_num = next_request_num(state)
            local lease_id = action.holding ~= nil and action.holding.lease_id or 0
            local line = workload.build_request(action, {
                message_id = break_client.new_uuid4(),
                client_id = state.client_id,
                request_num = request_num,
                holder = state.holder,
                lease_id = lease_id,
                expiry_ms = math.floor(now * 1000) + math.random(5, 60) * 1000,
            })
            local reply, err = send_request(state, line)
            if reply == nil then
                -- Backoff episode: one warn line, then silence until recovery.
                state.backoff_until = now + workload.BACKOFF_MS / 1000
                if not state.warned then
                    log_warn(state, "cluster unreachable (" .. tostring(err) .. "); backing off 5s")
                end
            else
                if state.warned then
                    state.warned = false -- recovered; next episode may log again
                end
                workload.update_holdings(state.holdings, action, reply, state.holder)
            end
        end
    end
    local ok, terr = ngx.timer.at(workload.INTERVAL_MS / 1000, tick, state)
    if ok == nil and terr ~= "process exiting" then
        ngx.log(ngx.ERR, "workload: failed to reschedule timer: ", terr)
    end
end

-- start(config) → true | false
-- config = { enabled=boolean (default true; false → no-op, item30 renders it
--            from WORKLOAD=on|off), cluster_path=string, dict=shared dict }
-- Call from init_worker_by_lua*; only worker 0 runs the driver.
function workload.start(config)
    config = config or {}
    if config.enabled == false then
        return false
    end
    if ngx == nil or ngx.worker == nil or ngx.timer == nil then
        return false
    end
    if ngx.worker.id() ~= 0 then
        return false
    end
    math.randomseed(math.floor(ngx.now() * 1000) % 2147483647)
    local state = {
        holdings = {},
        holder = break_client.new_uuid4(),
        client_id = 1000000 + ngx.worker.id(),
        dict = config.dict or ngx.shared.lockadmin,
        cluster_path = config.cluster_path or workload.DEFAULT_CLUSTER_PATH,
        members = nil,
        warned = false,
        backoff_until = 0,
    }
    local ok, err = ngx.timer.at(0, tick, state)
    if ok == nil then
        ngx.log(ngx.ERR, "workload: failed to start timer: ", err)
        return false
    end
    return true
end

return workload
