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
        -- breaker in the core, so its actor is always nil; release echoes
        -- no holder in the record, so fall back to the incumbent.
        -- nil actor values are converted to cjson.null by the handlers
        -- encoder so the field is always present in JSON output.
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

-- Per-node stats for /cluster, derived from one node's slice of the single
-- shared scan (item47: /cluster must not re-scan per node). `records` holds
-- only this node's records (tagged r.node by scan_all); seg_count/seg_bytes
-- come from scan_all's per-node inventory. Returns segmentCount,
-- segmentBytes, locksHeld (latest held_gauge), per-kind rates over the
-- trailing `window_ms` ending at now_ms, and lastRecordMs (nil when the
-- node has no records).
function lock_state.node_stats(records, seg_count, seg_bytes, now_ms, window_ms)
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
        segmentCount = seg_count,
        segmentBytes = seg_bytes,
        lastRecordMs = last_ms,
    }
end

-- How long scan_all retains decoded records. The widest window any endpoint
-- queries is /metrics/series' trailing hour (fromMs defaults to
-- now-3600000, matched by the frontend's historyWindowMs), so records older
-- than that cannot appear in any response; dropping them on refresh bounds
-- memory. /events is limit-capped to the newest 1000 and /locks reflects
-- live leases (constantly renewed), so neither needs older history.
lock_state.RETENTION_MS = 3600000

-- scan_all(dirs_by_node, listdir, now_ms, scan) → records, seg_count,
-- seg_bytes, scan, stats.
--
-- Incremental (item47): `scan` — pass nil on the first call, the returned
-- value after — remembers
--   files   = path → { size, offset, node }   (offset = bytes consumed)
--   records = retained, ts-sorted, node-tagged record set
--   segs    = node → { count, bytes }         (recomputed every call)
-- Sealed segments are immutable, so a segment whose size is unchanged is
-- skipped without being opened; only new files and grown tails are decoded,
-- and a grown tail is read from the remembered offset, not from byte 0.
-- Invalidation: a file that shrank or vanished (history reclamation is an
-- admin task), or a grown tail whose resume offset does not begin a valid
-- record (in-place rewrite), invalidates the whole snapshot — stats.rescan
-- is true and every segment is cleanly re-read, never silently corrupted.
-- stats = { read=, skipped=, rescan= } feeds the ngx.shared counters.
function lock_state.scan_all(dirs_by_node, listdir, now_ms, scan)
    scan = scan or {}
    scan.files = scan.files or {}
    scan.records = scan.records or {}
    -- Monotonic decode sequence. ts_ms has whole-second resolution, so
    -- records from the same second tie, and table.sort is not stable — a
    -- re-sort on a later refresh could permute a tied acquire/release pair
    -- and flip the derived lock state. The tie-break keeps the retained
    -- set a total order.
    scan.seq = scan.seq or 0

    -- Inventory every dir first, so a removal anywhere is detected before
    -- any incremental decision is made.
    local present = {} -- path → seg entry, annotated with .owner (dirs key)
    local segs = {}
    local seg_count = 0
    local seg_bytes = 0
    for node, dir in pairs(dirs_by_node or {}) do
        local inv = { count = 0, bytes = 0 }
        for _, seg in ipairs(lock_log.list_segments(dir, listdir)) do
            seg.owner = node
            inv.count = inv.count + 1
            inv.bytes = inv.bytes + seg.size
            present[seg.path] = seg
        end
        segs[node] = inv
        seg_count = seg_count + inv.count
        seg_bytes = seg_bytes + inv.bytes
    end
    scan.segs = segs

    -- A removed or shrunk file invalidates the retained snapshot.
    local rescan = false
    for path, f in pairs(scan.files) do
        local seg = present[path]
        if seg == nil or seg.size < f.size then
            rescan = true
            break
        end
    end
    if rescan then
        scan.files = {}
        scan.records = {}
    end

    local stats = { read = 0, skipped = 0, rescan = rescan }
    local function read_full(path, seg, out)
        stats.read = stats.read + 1
        local _, records, _, consumed = lock_log.read_segment(path, 0)
        scan.files[path] = { size = seg.size, offset = consumed, node = seg.owner }
        for _, r in ipairs(records) do
            r.node = seg.owner
            scan.seq = scan.seq + 1
            r.seq = scan.seq
            out[#out + 1] = r
        end
    end

    local fresh = {}
    if rescan then
        for path, seg in pairs(present) do
            read_full(path, seg, fresh)
        end
    else
        local corrupt = false
        for path, seg in pairs(present) do
            local f = scan.files[path]
            if f ~= nil and f.size == seg.size then
                stats.skipped = stats.skipped + 1
            else
                local offset = f ~= nil and f.offset or 0
                local _, records, _, consumed, status = lock_log.read_segment(path, offset)
                if status == "resume_corrupt" then
                    corrupt = true
                    break
                end
                stats.read = stats.read + 1
                scan.files[path] = { size = seg.size, offset = consumed, node = seg.owner }
                for _, r in ipairs(records) do
                    r.node = seg.owner
                    scan.seq = scan.seq + 1
                    r.seq = scan.seq
                    fresh[#fresh + 1] = r
                end
            end
        end
        if corrupt then
            -- In-place rewrite: wipe and re-read everything cleanly.
            stats.rescan = true
            stats.read = 0
            stats.skipped = 0
            scan.files = {}
            scan.records = {}
            fresh = {}
            for path, seg in pairs(present) do
                read_full(path, seg, fresh)
            end
        end
    end

    if #fresh > 0 then
        local all = scan.records
        for _, r in ipairs(fresh) do
            all[#all + 1] = r
        end
        table.sort(all, function(a, b)
            if a.ts_ms ~= b.ts_ms then
                return a.ts_ms < b.ts_ms
            end
            -- Same-second tie: keep decode order so the sort is a total
            -- order and re-sorts are deterministic. Records retained from
            -- before this field existed fall back to 0, which only affects
            -- ties among themselves.
            return (a.seq or 0) < (b.seq or 0)
        end)
    end
    if now_ms ~= nil then
        -- Retention bound: drop records older than the widest queried
        -- window so the retained set does not grow without limit.
        local cutoff = now_ms - lock_state.RETENTION_MS
        local all = scan.records
        local kept = 0
        for i = 1, #all do
            if all[i].ts_ms >= cutoff then
                kept = kept + 1
                all[kept] = all[i]
            end
        end
        for i = kept + 1, #all do
            all[i] = nil
        end
    end
    return scan.records, seg_count, seg_bytes, scan, stats
end

return lock_state
