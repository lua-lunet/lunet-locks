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
-- serializing into ngx.shared and plenty for a console. The scan itself is
-- incremental (item47): lock_state.scan_all retains {path → {size, offset}}
-- plus the merged node-tagged records in state.scan, so a refresh decodes
-- only new segments and grown tails — a shrink/removal forces a clean full
-- rescan. The shared dict itself is only used for counters/gauges, behind a
-- tiny interface (dict:incr(key, delta, init), dict:set(key, value)) so
-- tests inject a plain-table fake. Counters: req:<route> per endpoint;
-- scan:segments_read / scan:segments_skipped / scan:full_rescans per
-- refresh. Gauges: scan:last_ms, scan:segments, scan:segment_bytes.
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
        break_exec = config.break_exec, -- (state, lock_id, now_ms) → status, body
        cache = nil,
        scan = nil, -- incremental scan state (item47), owned by lock_state.scan_all
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
    local records, seg_count, seg_bytes, scan, stats =
        lock_state.scan_all(state.telemetry_dirs_by_node, state.listdir, now_ms, state.scan)
    state.scan = scan
    state.cache = { ms = now_ms, records = records, seg_count = seg_count, seg_bytes = seg_bytes }
    state.dict:set("scan:last_ms", now_ms)
    state.dict:set("scan:segments", seg_count)
    state.dict:set("scan:segment_bytes", seg_bytes)
    -- Incremental-scan observability: steady state reads ~1 grown tail and
    -- skips the sealed rest, so read should stay ≪ skipped.
    state.dict:incr("scan:segments_read", stats.read, 0)
    state.dict:incr("scan:segments_skipped", stats.skipped, 0)
    state.dict:incr("scan:full_rescans", stats.rescan and 1 or 0, 0)
    return records
end

-- Force a scan past the TTL (break). With the incremental scan state
-- retained in state.scan this costs a tail re-read of the grown
-- segment(s), never a full re-decode.
local function force_scan(state, now_ms)
    state.cache = nil
    return refresh_scan(state, now_ms)
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
        -- One shared scan for the whole cluster (item47): records are
        -- tagged with their owning node by scan_all, so per-node stats are
        -- derived here instead of re-scanning each node's dir.
        local records = refresh_scan(state, now_ms)
        local segs = state.scan.segs
        local nodes = {}
        for _, id in ipairs(read_cluster_nodes(state.cluster_json_path)) do
            local dir = state.telemetry_dirs_by_node[id]
            local stats
            if dir ~= nil then
                local mine = {}
                for _, r in ipairs(records) do
                    if r.node == id then
                        mine[#mine + 1] = r
                    end
                end
                local inv = segs[id] or { count = 0, bytes = 0 }
                stats = lock_state.node_stats(mine, inv.count, inv.bytes, now_ms, RATE_WINDOW_MS)
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
            -- SECURITY: the core performs NO authorization on BREAK (see
            -- docs/src/client-protocol.md); this edge owns it. Loopback bind
            -- + `auth_basic` are the current bar; production deployments must
            -- put mTLS/RBAC in front, and this route must never be served
            -- without `auth_basic` (or stronger).
            if req.method ~= "POST" then
                return 405, { error = "POST required" }
            end
            if state.break_exec == nil then
                return 501, { error = "break not implemented (item28)" }
            end
            dict:incr("req:break", 1, 0)
            return state.break_exec(state, lock_id, now_ms)
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

-- Break executor for the ngx edge: reads cluster.json (item30; missing or
-- unparseable → 502 cluster unavailable), issues BREAK via the cosocket
-- client, and maps the reply to the mock's contract:
--   broken:true  → 200 {lock, event}
--   broken:false → 409 {error="lock is not held"} (the core treats break on
--                  a missing/expired lock as idempotent broken:false — that
--                  is NOT a 404)
--   transport/leaderless/malformed → 502
-- For the 200 `lock` we rescan telemetry (cache invalidated) so the console
-- shows the reduced post-break row. Eventual-consistency caveat: the break
-- record lands on the leader's log first and the co-located node's segments
-- may lag behind it, so the rescan may not show the break yet; in that case
-- we fall back to the break reply's own lease data (fencing token bumped,
-- holder/expiry cleared) so the response is still truthful.
function handlers.ngx_break_exec()
    local break_client = require("break_client")
    return function(state, lock_id, now_ms)
        local cluster
        local fh = state.cluster_json_path ~= nil
            and io.open(state.cluster_json_path, "rb")
            or nil
        if fh ~= nil then
            local text = fh:read("*a")
            fh:close()
            local members = break_client.parse_cluster(text)
            if members ~= nil then
                cluster = { members = members }
            end
        end
        if cluster == nil then
            return 502, { error = "cluster unavailable" }
        end
        local reply, err = break_client.break_lock(lock_id, cluster, { dict = state.dict })
        if reply == nil then
            return err.status, { error = err.error }
        end
        if not reply.broken then
            -- Mirror the mock: unknown lock → 404, known-but-free → 409.
            local records = force_scan(state, now_ms)
            if lock_state.reduce(records, now_ms).locks[lock_id] == nil then
                return 404, { error = "no such lock" }
            end
            return 409, { error = "lock is not held" }
        end
        -- Force the break to be reflected if visible; incremental scan
        -- state makes this a tail re-read, not a full rescan.
        local records = force_scan(state, now_ms)
        local lock = lock_state.reduce(records, now_ms).locks[lock_id]
        if lock == nil or lock.state == "held" then
            -- Rescan lags the leader; synthesize from the break reply.
            local prior = lock or {
                id = lock_id,
                name = "",
                labels = {},
                leaseMs = 0,
                takenAtMs = nil,
            }
            lock = {
                id = lock_id,
                name = prior.name,
                labels = prior.labels,
                state = "free",
                holder = nil,
                fencingToken = reply.lease_id or prior.fencingToken or 0,
                leaseMs = prior.leaseMs or 0,
                expiresAtMs = nil,
                takenAtMs = nil,
                renewCount = 0,
            }
        end
        local event = {
            seq = 0,
            tsMs = now_ms,
            kind = "break",
            lockId = lock_id,
            name = lock.name,
            actor = "admin@console",
            detail = "admin force-release, fence " .. tostring(lock.fencingToken),
        }
        return 200, { lock = lock, event = event }
    end
end

-- Minimal JSON encoder fallback would be a liability; inside OpenResty we
-- always have cjson, so the adapter requires it lazily (the pure core and
-- the tests never load it).
local function encode_json(body)
    local cjson = require("cjson.safe")
    local function normalize_event_actor(event)
        if event ~= nil and event.actor == nil then
            event.actor = cjson.null
        end
    end
    normalize_event_actor(body.event)
    if body.events ~= nil then
        for _, event in ipairs(body.events) do
            normalize_event_actor(event)
        end
    end
    if body.recentEvents ~= nil then
        for _, event in ipairs(body.recentEvents) do
            normalize_event_actor(event)
        end
    end
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
