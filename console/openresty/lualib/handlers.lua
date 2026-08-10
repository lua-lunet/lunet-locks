-- /api/v1 read-only handlers: thin ngx adapters over a pure core.
--
-- The pure part (`handlers.handle(state, req)`) takes a plain request table
-- {method=, path=, query=} and returns status, body-table; it never touches
-- `ngx`, so it is unit-testable with the system luajit (see
-- console/tests/lock_state_test.lua, which injects a fake dict and a fake
-- now_ms). The ngx adapter (`handlers.ngx_handler(state)`) converts
-- ngx.req/ngx.var into that table and JSON-encodes the reply with cjson
-- (required lazily, only inside the adapter).
--
-- Caching: scan results are cached module-level per state with a ~1s TTL
-- (state.cache = {ms, records, seg_count, seg_bytes}); simpler than
-- serializing into ngx.shared and plenty for a console. The shared dict
-- itself is only used for counters/gauges, behind a tiny interface
-- (dict:incr(key, delta, init), dict:set(key, value)) so tests inject a
-- plain-table fake. Counters: req:<route> per endpoint; gauges:
-- scan:last_ms, scan:segments, scan:segment_bytes.
--
-- setup(config): config = {
--   cluster_json_path      = path to cluster.json (item30 writes it; a
--                            missing/unreadable file yields nodes:{}),
--   telemetry_dirs_by_node = { node_id → segment dir },
--   dict                   = optional ngx.shared.lockadmin-like table,
--   now_ms                 = optional clock override (tests),
--   listdir                = optional dir lister override (tests),
-- }

local lock_state = require("lock_state")

local handlers = {}

local CACHE_TTL_MS = 1000
local RATE_WINDOW_MS = 10000

-- ---- tiny shared-dict shim -------------------------------------------------

local function new_null_dict()
    local store = {}
    return {
        incr = function(_, key, delta, init)
            if store[key] == nil then
                store[key] = init or 0
            end
            store[key] = store[key] + delta
            return store[key]
        end,
        set = function(_, key, value)
            store[key] = value
            return true
        end,
        get = function(_, key)
            return store[key]
        end,
    }
end

local function default_now_ms()
    -- Second-resolution floor; adequate for telemetry reads. ngx callers
    -- may inject ngx.now()*1000 via config.now_ms for sub-second precision.
    return os.time() * 1000
end

function handlers.setup(config)
    return {
        cluster_json_path = config.cluster_json_path,
        telemetry_dirs_by_node = config.telemetry_dirs_by_node or {},
        dict = config.dict or new_null_dict(),
        now_ms = config.now_ms or default_now_ms,
        listdir = config.listdir,
        cache = nil,
    }
end

-- ---- cluster.json (tolerant; item30 owns the real format) ------------------

-- Extract node ids from cluster.json without a JSON dependency: accepts
-- either ["n1","n2"] or [{"id":"n1",...}, ...]. Anything unreadable or
-- unparseable yields an empty list (the endpoint must not error).
local function read_cluster_nodes(path)
    local nodes = {}
    if path == nil then
        return nodes
    end
    local fh = io.open(path, "rb")
    if fh == nil then
        return nodes
    end
    local data = fh:read("*a")
    fh:close()
    for id in data:gmatch('"id"%s*:%s*"([^"]+)"') do
        nodes[#nodes + 1] = id
    end
    if #nodes == 0 then
        for id in data:gmatch('"([^"]+)"') do
            nodes[#nodes + 1] = id
        end
    end
    return nodes
end

-- ---- pure core ---------------------------------------------------------------

local function refresh_scan(state, now_ms)
    local cache = state.cache
    if cache ~= nil and now_ms - cache.ms < CACHE_TTL_MS then
        return cache.records
    end
    local records, seg_count, seg_bytes =
        lock_state.scan_all(state.telemetry_dirs_by_node, state.listdir)
    state.cache = { ms = now_ms, records = records, seg_count = seg_count, seg_bytes = seg_bytes }
    state.dict:set("scan:last_ms", now_ms)
    state.dict:set("scan:segments", seg_count)
    state.dict:set("scan:segment_bytes", seg_bytes)
    return records
end

local function num(v, default)
    local n = tonumber(v)
    if n == nil then
        return default
    end
    return n
end

-- handlers.handle(state, req) → status, body. req = {method, path, query}.
function handlers.handle(state, req)
    local dict = state.dict
    local now_ms = state.now_ms()
    local path = req.path
    local query = req.query or {}

    if path == "/api/v1/health" then
        dict:incr("req:health", 1, 0)
        return 200, { status = "ok", nowMs = now_ms }
    end

    if path == "/api/v1/cluster" then
        dict:incr("req:cluster", 1, 0)
        local nodes = {}
        for _, id in ipairs(read_cluster_nodes(state.cluster_json_path)) do
            local dir = state.telemetry_dirs_by_node[id]
            local stats
            if dir ~= nil then
                stats = lock_state.node_stats(dir, now_ms, RATE_WINDOW_MS, state.listdir)
            else
                stats = {
                    locksHeld = 0,
                    acquirePerSec = 0,
                    renewPerSec = 0,
                    releasePerSec = 0,
                    casPerSec = 0,
                    breakPerSec = 0,
                    denyPerSec = 0,
                    expirePerSec = 0,
                    segmentCount = 0,
                    segmentBytes = 0,
                    lastRecordMs = nil,
                }
            end
            stats.id = id
            nodes[#nodes + 1] = stats
        end
        return 200, { nowMs = now_ms, nodes = nodes }
    end

    if path == "/api/v1/locks" then
        dict:incr("req:locks", 1, 0)
        local records = refresh_scan(state, now_ms)
        local reduced = lock_state.reduce(records, now_ms)
        local q = query.q or ""
        local state_filter = query.state
        local expiring_at = num(query.expiringAtMs, 0)
        local tolerance = num(query.toleranceMs, 5000)
        local out = {}
        for _, lock in pairs(reduced.locks) do
            if lock_state.match_q(lock, q)
                and (state_filter == nil or lock.state == state_filter)
            then
                out[#out + 1] = lock
            end
        end
        if expiring_at > 0 then
            -- Mirror the mock: held locks within toleranceMs of the target
            -- instant, sorted by closeness to it.
            local near = {}
            for _, lock in ipairs(out) do
                if lock.state == "held"
                    and lock.expiresAtMs ~= nil
                    and math.abs(lock.expiresAtMs - expiring_at) <= tolerance
                then
                    near[#near + 1] = lock
                end
            end
            table.sort(near, function(a, b)
                return math.abs(a.expiresAtMs - expiring_at)
                    < math.abs(b.expiresAtMs - expiring_at)
            end)
            out = near
        else
            table.sort(out, function(a, b)
                return a.name < b.name
            end)
        end
        return 200, { nowMs = now_ms, locks = out }
    end

    -- (Lua patterns cannot quantify a capture, so match the two shapes.)
    local lock_id = path:match("^/api/v1/locks/(%d+)$")
    local break_suffix = false
    if lock_id == nil then
        lock_id = path:match("^/api/v1/locks/(%d+)/break$")
        break_suffix = lock_id ~= nil
    end
    if lock_id ~= nil then
        lock_id = tonumber(lock_id)
        if break_suffix then
            -- The privileged break op is item28; only the 405 semantics of
            -- the mock are pinned down here.
            if req.method ~= "POST" then
                return 405, { error = "POST required" }
            end
            return 501, { error = "break not implemented (item28)" }
        end
        dict:incr("req:lock", 1, 0)
        local records = refresh_scan(state, now_ms)
        local reduced = lock_state.reduce(records, now_ms)
        local lock = reduced.locks[lock_id]
        if lock == nil then
            return 404, { error = "no such lock" }
        end
        local recent = {}
        for i = #reduced.events, 1, -1 do
            local e = reduced.events[i]
            if e.lockId == lock_id then
                recent[#recent + 1] = e
                if #recent == 8 then
                    break
                end
            end
        end
        return 200, { lock = lock, recentEvents = recent }
    end

    if path == "/api/v1/events" then
        dict:incr("req:events", 1, 0)
        local records = refresh_scan(state, now_ms)
        local reduced = lock_state.reduce(records, now_ms)
        local from_ms = num(query.fromMs, 0)
        local to_ms = num(query.toMs, math.huge)
        local lock_filter = tonumber(query.lockId)
        local kind = query.kind
        local q = (query.q or ""):lower()
        local limit = math.min(num(query.limit, 300), 1000)
        local events = reduced.events
        local out = {}
        for i = #events, 1, -1 do
            if #out >= limit then
                break
            end
            local e = events[i]
            if e.tsMs >= from_ms
                and e.tsMs <= to_ms
                and (lock_filter == nil or e.lockId == lock_filter)
                and (kind == nil or kind == "" or e.kind == kind)
                and (q == "" or e.name:lower():find(q, 1, true) ~= nil)
            then
                out[#out + 1] = e
            end
        end
        return 200, { events = out }
    end

    if path == "/api/v1/metrics/series" then
        dict:incr("req:series", 1, 0)
        local records = refresh_scan(state, now_ms)
        local from_ms = num(query.fromMs, now_ms - 3600000)
        local to_ms = num(query.toMs, now_ms)
        local bucket_ms = math.max(num(query.bucketMs, 5000), 1000)
        local buckets = lock_state.series(records, from_ms, to_ms, bucket_ms)
        return 200, { bucketMs = bucket_ms, buckets = buckets }
    end

    return 404, { error = "not found" }
end

-- ---- ngx adapter -------------------------------------------------------------

-- Minimal JSON encoder fallback would be a liability; inside OpenResty we
-- always have cjson, so the adapter requires it lazily (the pure core and
-- the tests never load it).
local function encode_json(body)
    local cjson = require("cjson.safe")
    return cjson.encode(body)
end

-- Returns an ngx content-phase function bound to `state`.
function handlers.ngx_handler(state)
    return function()
        local ngx = ngx
        local req = {
            method = ngx.req.get_method(),
            path = ngx.var.uri,
            query = ngx.req.get_uri_args(),
        }
        local status, body = handlers.handle(state, req)
        ngx.status = status
        ngx.header["content-type"] = "application/json"
        ngx.say(encode_json(body))
        return ngx.exit(status)
    end
end

return handlers
