-- Pure lock-state reducer over telemetry records (console/openresty/lualib).
--
-- Plain LuaJIT, no `ngx`: unit-testable with the system luajit. The ngx
-- adapters live in handlers.lua; this module only turns records into the
-- API's Lock/Event shapes.
--
-- Record format v2 (src/telemetry_log.tl) carries expiry_ms, a 16-byte
-- holder UUID, and a labels CSV, so the reducer serves real state:
--   * a lock is held iff its latest fencing event is acquire/renew/cas AND
--     its expiry_ms is still in the future; release/break free it
--     immediately, a passed expiry frees it lazily
--   * `expire` events are synthesized at expiry_ms: a lock recorded held
--     whose expiry passes without a release/break before the next event or
--     stream end gets one (kind 5 stays reserved for a future explicit
--     expire record written by the service)
--   * lock.holder / lock.expiresAtMs / lock.labels come straight from the
--     record; event.actor is the holder UUID when known (null for break —
--     the core does not record the breaker)
--   * lock.leaseMs is served as 0 (schema-required, not derivable: a lease
--     duration is a client choice, not a lock-table fact)

local lock_log = require("lock_log")

local lock_state = {}

lock_state.KIND_NAMES = {
    [1] = "acquire",
    [2] = "renew",
    [3] = "release",
    [4] = "cas",
    [5] = "expire",
    [6] = "break",
    [7] = "deny",
}
local NAME_KINDS = {}
for k, v in pairs(lock_state.KIND_NAMES) do
    NAME_KINDS[v] = k
end
lock_state.NAME_KINDS = NAME_KINDS

-- Brief per-kind detail text, mirroring the mock's detailFor but limited to
-- what the record actually carries (fencing token only).
local function detail_for(kind_name, rec)
    if kind_name == "acquire" then
        return "fence " .. rec.fencing_token
    elseif kind_name == "renew" then
        return "fence " .. rec.fencing_token
    elseif kind_name == "release" then
        return "clean release"
    elseif kind_name == "cas" then
        return "fence " .. rec.fencing_token
    elseif kind_name == "expire" then
        return "lease lapsed"
    elseif kind_name == "break" then
        return "admin force-release, fence " .. rec.fencing_token
    elseif kind_name == "deny" then
        return "contended"
    end
    return ""
end
lock_state.detail_for = detail_for

-- Held iff the latest fencing event says so. deny does not change state.
local HELD_AFTER = { [1] = true, [2] = true, [4] = true } -- acquire/renew/cas
local FREE_AFTER = { [3] = true, [5] = true, [6] = true } -- release/expire/break

local function free_lock(lock)
    lock.state = "free"
    lock.holder = nil
    lock.expiresAtMs = nil
    lock.renewCount = 0
    lock.takenAtMs = nil
end

-- reduce(records, now_ms) → { locks = {lock_id → lock}, events = [...] }.
-- records must be in ts order (lock_log.scan already is). Events keep the
-- mock shape {seq, tsMs, kind, lockId, name, actor, detail}.
function lock_state.reduce(records, now_ms)
    local locks = {}
    local events = {}
    local function emit(ts_ms, kind_name, id, name, actor, fencing_token)
        events[#events + 1] = {
            seq = #events + 1,
            tsMs = ts_ms,
            kind = kind_name,
            lockId = id,
            name = name,
            actor = actor,
            detail = detail_for(kind_name, { fencing_token = fencing_token or 0 }),
        }
    end
    for _, rec in ipairs(records) do
        local id = rec.lock_id
        local lock = locks[id]
        if lock == nil then
            lock = {
                id = id,
                name = "",
                labels = {},
                state = "free",
                holder = nil,
                fencingToken = 0,
                leaseMs = 0,
                expiresAtMs = nil,
                takenAtMs = nil,
                renewCount = 0,
            }
            locks[id] = lock
        end
        -- Lazy expiry: the previous lease lapsed before this event — the
        -- lock went free at expiry_ms, so synthesize the expire event first.
        if
            lock.state == "held"
            and lock.expiresAtMs ~= nil
            and lock.expiresAtMs <= rec.ts_ms
        then
            emit(lock.expiresAtMs, "expire", id, lock.name, lock.holder, lock.fencingToken)
            free_lock(lock)
        end
        if rec.name ~= nil and rec.name ~= "" then
            lock.name = rec.name
        end
        if rec.labels ~= nil and #rec.labels > 0 then
            lock.labels = rec.labels
        end
        local prior_holder = lock.holder
        if HELD_AFTER[rec.kind] then
            local became_held = lock.state ~= "held"
            lock.state = "held"
            lock.holder = rec.holder
            lock.expiresAtMs = rec.expiry_ms ~= nil and rec.expiry_ms > 0
                    and rec.expiry_ms
                or nil
            if rec.kind == 1 or rec.kind == 4 then -- acquire or cas: new holder
                lock.takenAtMs = rec.ts_ms
            elseif became_held then
                lock.takenAtMs = rec.ts_ms
            end
            lock.fencingToken = rec.fencing_token
            lock.renewCount = rec.renew_count
        elseif FREE_AFTER[rec.kind] then
            lock.fencingToken = rec.fencing_token
            free_lock(lock)
        end
        -- kind 7 (deny): event only, no state change. break records no
        -- breaker in the core, so its actor is always null; release echoes
        -- no holder in the record, so fall back to the incumbent.
        local actor = rec.holder
        if rec.kind == 6 then
            actor = nil
        elseif actor == nil and rec.kind == 3 then
            actor = prior_holder
        end
        emit(
            rec.ts_ms,
            lock_state.KIND_NAMES[rec.kind] or "unknown",
            id,
            lock.name,
            actor,
            rec.fencing_token
        )
    end
    -- Stream-end expiry synthesis, in expiry order for determinism.
    local lapsed = {}
    for id, lock in pairs(locks) do
        if
            lock.state == "held"
            and lock.expiresAtMs ~= nil
            and lock.expiresAtMs <= now_ms
        then
            lapsed[#lapsed + 1] = lock
        end
    end
    table.sort(lapsed, function(a, b)
        if a.expiresAtMs ~= b.expiresAtMs then
            return a.expiresAtMs < b.expiresAtMs
        end
        return a.id < b.id
    end)
    for _, lock in ipairs(lapsed) do
        emit(lock.expiresAtMs, "expire", lock.id, lock.name, lock.holder, lock.fencingToken)
        free_lock(lock)
    end
    return { locks = locks, events = events }
end

-- matchQ: mirrors the mock exactly — space-separated terms; `tag:x` matches
-- a label prefix, `holder:x` a holder substring, else a name substring.
function lock_state.match_q(lock, q)
    if q == nil or q == "" then
        return true
    end
    for term in q:gmatch("%S+") do
        local t = term:lower()
        if t:sub(1, 4) == "tag:" then
            local prefix = t:sub(5)
            local hit = false
            for _, label in ipairs(lock.labels or {}) do
                if label:sub(1, #prefix) == prefix then
                    hit = true
                    break
                end
            end
            if not hit then
                return false
            end
        elseif t:sub(1, 7) == "holder:" then
            if not (lock.holder or ""):find(t:sub(8), 1, true) then
                return false
            end
        elseif not lock.name:lower():find(t, 1, true) then
            return false
        end
    end
    return true
end

-- Bucketed series over records. buckets: {tsMs, held, acquire..deny}; held
-- from held_gauge samples (last in bucket wins). Empty range → empty
-- buckets (the mock's fixed guard — do not regress it).
function lock_state.series(records, from_ms, to_ms, bucket_ms)
    local buckets = {}
    local start = from_ms - (from_ms % bucket_ms)
    local ts = start
    while ts < to_ms do
        buckets[#buckets + 1] = {
            tsMs = ts,
            held = 0,
            acquire = 0,
            renew = 0,
            release = 0,
            cas = 0,
            expire = 0,
            ["break"] = 0,
            deny = 0,
        }
        ts = ts + bucket_ms
    end
    if #buckets == 0 then
        return buckets
    end
    local function index_of(ts_ms)
        return math.floor((ts_ms - buckets[1].tsMs) / bucket_ms) + 1
    end
    for _, rec in ipairs(records) do
        if rec.ts_ms >= from_ms and rec.ts_ms < to_ms then
            local b = buckets[index_of(rec.ts_ms)]
            if b ~= nil then
                local kind_name = lock_state.KIND_NAMES[rec.kind]
                if kind_name ~= nil then
                    b[kind_name] = b[kind_name] + 1
                end
                b.held = rec.held_gauge -- last sample in the bucket wins
            end
        end
    end
    return buckets
end

-- Per-node stats for /cluster, derived from one node's segment dir.
-- Returns segmentCount, segmentBytes, locksHeld (latest held_gauge),
-- per-kind rates over the trailing `window_ms` ending at now_ms, and
-- lastRecordMs (nil when the node has no records).
function lock_state.node_stats(dir, now_ms, window_ms, listdir)
    local segs = lock_log.list_segments(dir, listdir)
    local bytes = 0
    for _, seg in ipairs(segs) do
        bytes = bytes + seg.size
    end
    local records = lock_log.scan(dir, listdir)
    local cutoff = now_ms - window_ms
    local counts = { acquire = 0, renew = 0, release = 0, cas = 0, expire = 0, ["break"] = 0, deny = 0 }
    local held = 0
    local last_ms = nil
    for _, rec in ipairs(records) do
        held = rec.held_gauge
        last_ms = rec.ts_ms
        if rec.ts_ms >= cutoff then
            local kind_name = lock_state.KIND_NAMES[rec.kind]
            if kind_name ~= nil then
                counts[kind_name] = counts[kind_name] + 1
            end
        end
    end
    local secs = window_ms / 1000
    local function rate(n)
        return math.floor((n / secs) * 100 + 0.5) / 100
    end
    return {
        locksHeld = held,
        acquirePerSec = rate(counts.acquire),
        renewPerSec = rate(counts.renew),
        releasePerSec = rate(counts.release),
        casPerSec = rate(counts.cas),
        breakPerSec = rate(counts["break"]),
        denyPerSec = rate(counts.deny),
        expirePerSec = rate(counts.expire),
        segmentCount = #segs,
        segmentBytes = bytes,
        lastRecordMs = last_ms,
    }
end

-- Scan all configured dirs and return records merged in ts order (stable),
-- plus the per-dir segment inventory {count, bytes} for the shared-dict
-- gauges.
function lock_state.scan_all(dirs_by_node, listdir)
    local all = {}
    local seg_count = 0
    local seg_bytes = 0
    for _, dir in pairs(dirs_by_node or {}) do
        for _, seg in ipairs(lock_log.list_segments(dir, listdir)) do
            seg_count = seg_count + 1
            seg_bytes = seg_bytes + seg.size
        end
        local records = lock_log.scan(dir, listdir)
        for _, r in ipairs(records) do
            all[#all + 1] = r
        end
    end
    table.sort(all, function(a, b)
        return a.ts_ms < b.ts_ms
    end)
    return all, seg_count, seg_bytes
end

return lock_state
