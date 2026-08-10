-- Unit test for console/openresty/lualib/lock_state.lua + handlers.lua
-- (plain luajit, no framework: assert + exit code). Run from anywhere;
-- paths resolve relative to this script.
--
--   luajit console/tests/lock_state_test.lua

local script_dir = arg[0]:match("^(.*)/[^/]*$") or "."
package.path = script_dir .. "/../openresty/lualib/?.lua;" .. package.path

local lock_log = require("lock_log")
local lock_state = require("lock_state")
local handlers = require("handlers")

local FIXTURES = os.getenv("FIXTURES") or (script_dir .. "/fixtures/telemetry")

local failures = 0
local function check(cond, msg)
    if cond then
        print("ok - " .. msg)
    else
        failures = failures + 1
        print("FAIL - " .. msg)
    end
end

local function fake_dict()
    local store = {}
    return {
        store = store,
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

-- Record shorthand: kind mnemonics → numbers.
local K = { acquire = 1, renew = 2, release = 3, cas = 4, expire = 5, brk = 6, deny = 7 }
local function rec(kind, ts, id, token, renew, held, name)
    return {
        kind = K[kind] or kind,
        ts_ms = ts,
        lock_id = id,
        fencing_token = token or 0,
        renew_count = renew or 0,
        held_gauge = held or 0,
        name = name or "",
    }
end

-- ---- reduce over the committed fixture --------------------------------------
-- Fixture pattern (see lock_log_test.lua): record n (1..21) has kind
-- ((n-1)%7)+1 on lock 100+n, one record per lock; record 22 is a release.

local fixture_records = lock_log.scan(FIXTURES)
check(#fixture_records == 22, "fixture scan: 22 records")

local reduced = lock_state.reduce(fixture_records, 1700000100000)

check(
    reduced.locks[101] ~= nil and reduced.locks[101].state == "held",
    "fixture: lock 101 (acquire) is held"
)
check(
    reduced.locks[101] ~= nil and reduced.locks[101].name == "fixture-lock-1",
    "fixture: lock 101 name"
)
check(
    reduced.locks[101] ~= nil and reduced.locks[101].fencingToken == 5001,
    "fixture: lock 101 fencingToken"
)
check(
    reduced.locks[101] ~= nil and reduced.locks[101].holder == nil,
    "fixture: holder is null (record format carries none)"
)
check(
    reduced.locks[101] ~= nil and reduced.locks[101].expiresAtMs == nil,
    "fixture: expiresAtMs is null (record format carries none)"
)
check(
    reduced.locks[103] ~= nil and reduced.locks[103].state == "free",
    "fixture: lock 103 (release) is free"
)
check(
    reduced.locks[106] ~= nil and reduced.locks[106].state == "free",
    "fixture: lock 106 (break) is free"
)
check(
    reduced.locks[107] ~= nil and reduced.locks[107].state == "free",
    "fixture: lock 107 (deny, never held) stays free"
)
check(#reduced.events == 22, "fixture: 22 events")
check(
    reduced.events[1] ~= nil
        and reduced.events[1].kind == "acquire"
        and reduced.events[1].lockId == 101
        and reduced.events[1].tsMs == 1700000001000
        and reduced.events[1].seq == 1,
    "fixture: first event shape"
)
check(
    reduced.events[22] ~= nil and reduced.events[22].kind == "release",
    "fixture: last event is the release"
)
local expire_count = 0
for _, e in ipairs(reduced.events) do
    if e.kind == "expire" then
        expire_count = expire_count + 1
    end
end
check(
    expire_count == 3 and #reduced.events == #fixture_records,
    "fixture: only the 3 real kind-5 records; no expire synthesis (no expiry data in format)"
)

-- ---- reduce over synthetic records -------------------------------------------

local syn = lock_state.reduce({
    rec("acquire", 1000, 1, 10, 0, 1, "/a"),
    rec("renew", 2000, 1, 10, 1, 1),
    rec("release", 3000, 1, 10, 0, 0),
    rec("acquire", 1000, 2, 20, 0, 2, "/b"),
    rec("brk", 2000, 2, 21, 0, 1),
    rec("acquire", 1000, 3, 30, 0, 3, "/c"),
    rec("deny", 2000, 3, 30, 0, 3),
}, 4000)

check(syn.locks[1].state == "free" and syn.locks[1].takenAtMs == nil, "synthetic: release → free, takenAtMs cleared")
check(syn.locks[1].renewCount == 0, "synthetic: release resets renewCount")
check(syn.locks[2].state == "free" and syn.locks[2].fencingToken == 21, "synthetic: break → free, token kept")
check(syn.locks[3].state == "held", "synthetic: deny does not change held state")
check(syn.locks[3].takenAtMs == 1000, "synthetic: takenAtMs from acquire, renew/deny keep it")
local deny_ev = syn.events[7]
check(
    deny_ev.kind == "deny" and deny_ev.actor == nil and deny_ev.name == "/c",
    "synthetic: deny event shape (actor null, name carried forward)"
)

-- ---- series bucket math -------------------------------------------------------

local series_records = {
    rec("acquire", 1000, 1, 1, 0, 1, "/a"),
    rec("renew", 1500, 1, 1, 1, 1),
    rec("acquire", 2100, 2, 2, 0, 2, "/b"),
    rec("release", 2900, 1, 1, 0, 1),
    rec("deny", 3200, 2, 2, 0, 1),
}
local buckets = lock_state.series(series_records, 1000, 4000, 1000)
check(#buckets == 3, "series: 3 buckets over [1000,4000) @1000ms (got " .. #buckets .. ")")
check(
    buckets[1].tsMs == 1000 and buckets[1].acquire == 1 and buckets[1].renew == 1,
    "series: bucket 1 counts"
)
check(buckets[1].held == 1, "series: bucket 1 held = last gauge sample in bucket")
check(buckets[2].acquire == 1 and buckets[2].release == 1 and buckets[2].held == 1, "series: bucket 2 counts + last gauge wins")
check(buckets[3].deny == 1 and buckets[3].held == 1, "series: bucket 3 deny")
-- Empty-range guard (the mock's fixed bug): fromMs >= toMs → empty buckets.
check(#lock_state.series(series_records, 5000, 5000, 1000) == 0, "series: empty range → empty buckets (guard kept)")
check(#lock_state.series(series_records, 6000, 5000, 1000) == 0, "series: inverted range → empty buckets")
-- Records outside the window don't leak into buckets.
local outside = lock_state.series({ rec("acquire", 999, 1, 1, 0, 5, "/x") }, 1000, 2000, 1000)
check(#outside == 1 and outside[1].acquire == 0 and outside[1].held == 0, "series: record before fromMs excluded")

-- ---- handler-level: /locks filters -------------------------------------------

local function new_state(dirs)
    return handlers.setup({
        cluster_json_path = script_dir .. "/fixtures/no-such-cluster.json",
        telemetry_dirs_by_node = dirs,
        dict = fake_dict(),
        now_ms = function()
            return 1700000100000
        end,
    })
end

local state = new_state({ t0 = FIXTURES })

local status, body = handlers.handle(state, { method = "GET", path = "/api/v1/health" })
check(status == 200 and body.status == "ok" and body.nowMs == 1700000100000, "handler: /health")

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks" })
check(status == 200 and #body.locks == 22, "handler: /locks returns all 22 (got " .. #body.locks .. ")")
check(body.locks[1].name <= body.locks[#body.locks].name, "handler: /locks sorted by name")

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks", query = { q = "lock-2" } })
local q_ok = #body.locks > 0
for _, l in ipairs(body.locks) do
    if not l.name:find("lock%-2") then
        q_ok = false
    end
end
check(status == 200 and q_ok, "handler: /locks q substring filter (" .. #body.locks .. " matches)")

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks", query = { state = "held" } })
local held_ok = #body.locks > 0
for _, l in ipairs(body.locks) do
    if l.state ~= "held" then
        held_ok = false
    end
end
check(status == 200 and held_ok, "handler: /locks state=held filter (" .. #body.locks .. " held)")

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks", query = { state = "free" } })
check(status == 200 and #body.locks + 0 >= 0, "handler: /locks state=free ok")
local free_count = #body.locks
status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks", query = { state = "held" } })
check(free_count + #body.locks == 22, "handler: held + free == 22")

-- tag: prefix — labels are empty (not in the record format), so tag:
-- matches nothing; holder: similarly (holder null).
status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks", query = { q = "tag:anything" } })
check(status == 200 and #body.locks == 0, "handler: tag: matches nothing (labels not in log)")
status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks", query = { q = "holder:t0" } })
check(status == 200 and #body.locks == 0, "handler: holder: matches nothing (holder not in log)")

-- matchQ unit check with a synthetic lock that HAS labels/holder.
local probe = { name = "/Tenants/ACME", labels = { "tenant", "critical" }, holder = "node-2 s-1f" }
check(lock_state.match_q(probe, "tag:ten holder:node-2 acme"), "matchQ: tag+holder+name terms all match")
check(not lock_state.match_q(probe, "tag:zzz"), "matchQ: unknown tag prefix rejects")

-- expiringAtMs: expiresAtMs is null everywhere → empty result, no crash.
status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/locks",
    query = { expiringAtMs = "1700000100000", toleranceMs = "5000" },
})
check(status == 200 and #body.locks == 0, "handler: expiringAtMs yields empty (expiresAtMs null) without error")

-- ---- handler-level: /locks/{id} -----------------------------------------------

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks/101" })
check(status == 200 and body.lock.id == 101 and body.lock.state == "held", "handler: /locks/101 held")
check(#body.recentEvents == 1 and body.recentEvents[1].kind == "acquire", "handler: /locks/101 recentEvents")

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks/424242" })
check(status == 404 and body.error == "no such lock", "handler: unknown lock → 404 like the mock")

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/locks/101/break" })
check(status == 405 and body.error == "POST required", "handler: GET on break → 405 like the mock")

-- recentEvents: last 8, newest first.
local many = {}
for i = 1, 10 do
    many[i] = rec("renew", 1000 + i, 7, 1, i, 1, "/m")
end
local mstate = handlers.setup({
    cluster_json_path = nil,
    telemetry_dirs_by_node = {},
    dict = fake_dict(),
    now_ms = function()
        return 9999
    end,
})
mstate.cache = { ms = 9999, records = many, seg_count = 0, seg_bytes = 0 }
status, body = handlers.handle(mstate, { method = "GET", path = "/api/v1/locks/7" })
check(
    status == 200 and #body.recentEvents == 8 and body.recentEvents[1].tsMs == 1010,
    "handler: recentEvents capped at 8, newest first"
)

-- ---- handler-level: /events -----------------------------------------------------

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/events" })
check(status == 200 and #body.events == 22, "handler: /events returns all 22")
check(body.events[1].tsMs > body.events[#body.events].tsMs, "handler: /events newest first")

status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/events",
    query = { kind = "renew" },
})
local kind_ok = #body.events == 3 -- fixture kinds 2 at n=2,9,16
for _, e in ipairs(body.events) do
    if e.kind ~= "renew" then
        kind_ok = false
    end
end
check(status == 200 and kind_ok, "handler: /events kind=renew filter (3)")

status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/events",
    query = { fromMs = "1700000010000", toMs = "1700000012000" },
})
check(status == 200 and #body.events == 3, "handler: /events fromMs/toMs range (3)")
status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/events",
    query = { lockId = "101" },
})
check(status == 200 and #body.events == 1 and body.events[1].lockId == 101, "handler: /events lockId filter")
status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/events",
    query = { q = "LOCK-5" },
})
check(status == 200 and #body.events == 1, "handler: /events q name substring, case-insensitive")
status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/events",
    query = { limit = "5" },
})
check(status == 200 and #body.events == 5, "handler: /events limit honored")

-- ---- handler-level: /metrics/series ----------------------------------------------

status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/metrics/series",
    query = { fromMs = "1700000000000", toMs = "1700000010000", bucketMs = "5000" },
})
check(status == 200 and body.bucketMs == 5000 and #body.buckets == 2, "handler: series bucket count")
local total = 0
for _, b in ipairs(body.buckets) do
    total = total + b.acquire + b.renew + b.release + b.cas + b.expire + b["break"] + b.deny
end
check(total == 9, "handler: series first 9 records counted across buckets (got " .. total .. ")")
check(body.buckets[1].held == 5, "handler: series held gauge last-in-bucket wins (n=5 → gauge 1? got " .. body.buckets[1].held .. ")")

status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/metrics/series",
    query = { fromMs = "5000", toMs = "5000" },
})
check(status == 200 and #body.buckets == 0, "handler: series empty-range guard")

-- bucketMs floor: clamped to >= 1000.
status, body = handlers.handle(state, {
    method = "GET",
    path = "/api/v1/metrics/series",
    query = { fromMs = "1700000000000", toMs = "1700000002000", bucketMs = "10" },
})
check(status == 200 and body.bucketMs == 1000, "handler: bucketMs clamped to min 1000")

-- ---- handler-level: /cluster -------------------------------------------------------

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/cluster" })
check(status == 200 and #body.nodes == 0 and body.nowMs == 1700000100000, "handler: missing cluster.json → nodes:[] without error")

local cpath = script_dir .. "/../.tmp-lock-state-test-cluster.json"
local cfh = assert(io.open(cpath, "wb"))
cfh:write('[{"id":"t0","role":"follower"},{"id":"t1","role":"leader"}]')
cfh:close()
local cstate = handlers.setup({
    cluster_json_path = cpath,
    telemetry_dirs_by_node = { t0 = FIXTURES },
    dict = fake_dict(),
    now_ms = function()
        return 1700000022000
    end,
})
status, body = handlers.handle(cstate, { method = "GET", path = "/api/v1/cluster" })
check(status == 200 and #body.nodes == 2, "handler: /cluster 2 nodes")
local n0, n1 = body.nodes[1], body.nodes[2]
check(
    n0.id == "t0" and n0.segmentCount == 3 and n0.segmentBytes > 0 and n0.lastRecordMs == 1700000099999,
    "handler: /cluster t0 segment stats"
)
check(n0.locksHeld == 0, "handler: /cluster t0 locksHeld from last gauge (release record, 0)")
check(
    n1.id == "t1" and n1.segmentCount == 0 and n1.lastRecordMs == nil,
    "handler: /cluster node without dir → zero stats"
)
-- Trailing-10s rates: window ends at now=1700000100000 → records at
-- ts >= 1700000090000 count: n=19 (acquire), n=20 (renew), n=21 (release),
-- plus the final release at ...99999.
check(n0.acquirePerSec == 0.1 and n0.renewPerSec == 0.1 and n0.releasePerSec == 0.2, "handler: /cluster trailing-10s rates")
os.remove(cpath)

-- ---- 404 fallthrough + dict counters -------------------------------------------------

status, body = handlers.handle(state, { method = "GET", path = "/api/v1/nope" })
check(status == 404 and body.error == "not found", "handler: unknown path → 404 like the mock")
check(state.dict.store["req:health"] == 1 and state.dict.store["req:locks"] ~= nil, "handler: dict counters bumped")
check(state.dict.store["scan:segments"] == 3 and state.dict.store["scan:segment_bytes"] > 0, "handler: scan gauges refreshed")

if failures > 0 then
    print(failures .. " failure(s)")
    os.exit(1)
end
print("all tests passed")
os.exit(0)
