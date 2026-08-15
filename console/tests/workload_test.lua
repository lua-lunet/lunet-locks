-- Unit test for console/openresty/lualib/workload.lua pure functions
-- (plain luajit, no framework: assert + exit code). Run from anywhere;
-- paths resolve relative to this script.
--
--   luajit console/tests/workload_test.lua

local script_dir = arg[0]:match("^(.*)/[^/]*$") or "."
package.path = script_dir .. "/../openresty/lualib/?.lua;" .. package.path

local workload = require("workload")

local failures = 0
local function check(cond, msg)
    if cond then
        print("ok - " .. msg)
    else
        failures = failures + 1
        print("FAIL - " .. msg)
    end
end

-- ---- protocol regexes re-implemented with Lua patterns ----------------------
-- Lua patterns lack `?`, alternation and {m,n}; the checks below are
-- equivalent formulations of the item22 / client-protocol rules.

-- name: ^/(?:[A-Za-z0-9._-]+(?:/[A-Za-z0-9._-]+)*)$ and <= 128 bytes.
-- Equivalent: starts with "/", <=128 bytes, only segment chars, no empty
-- segment (no "//", no trailing "/"). Note %w already includes "_".
local function valid_name(name)
    if type(name) ~= "string" or #name > 128 then
        return false
    end
    if name:sub(1, 1) ~= "/" then
        return false
    end
    if name:sub(2):match("^[A-Za-z0-9%._%/-]+$") == nil then
        return false
    end
    if name:find("//", 1, true) ~= nil then
        return false
    end
    if name:sub(-1) == "/" then
        return false
    end
    return true
end

-- label: ^[a-z0-9](?:[-a-z0-9]{0,30}[a-z0-9])?$ (1-32 bytes).
-- Equivalent: length 1 → single [a-z0-9]; else 2-32 bytes, first and last
-- [a-z0-9], middle all [-a-z0-9].
local function valid_label(label)
    if type(label) ~= "string" or #label == 0 or #label > 32 then
        return false
    end
    if #label == 1 then
        return label:match("^[a-z0-9]$") ~= nil
    end
    return label:match("^[a-z0-9][-a-z0-9]*[a-z0-9]$") ~= nil
end

-- sanity: the re-implementations accept/reject the right shapes
check(valid_name("/cluster/members/000001"), "validator accepts zk-style name")
check(valid_name("/tenants/acme-corp/ingest/shard-01"), "validator accepts nested path")
check(not valid_name("cluster/members"), "validator rejects relative path")
check(not valid_name("/a//b"), "validator rejects empty segment")
check(not valid_name("/a/"), "validator rejects trailing slash")
check(not valid_name("/has space/x"), "validator rejects space in segment")
check(not valid_name("/" .. string.rep("a", 128)), "validator rejects >128 byte name")
check(valid_name("/" .. string.rep("a", 127)), "validator accepts 128 byte name")
check(valid_label("leader"), "validator accepts plain label")
check(valid_label("us-east-1"), "validator accepts hyphenated label")
check(valid_label("a"), "validator accepts single-char label")
check(not valid_label("-lead"), "validator rejects leading hyphen")
check(not valid_label("lead-"), "validator rejects trailing hyphen")
check(not valid_label("Leader"), "validator rejects uppercase label")
check(not valid_label(string.rep("a", 33)), "validator rejects 33-byte label")
check(valid_label(string.rep("a", 32)), "validator accepts 32-byte label")

-- ---- pool -------------------------------------------------------------------

local pool = workload.pool()
check(#pool == 12, "pool has 12 entries")

local seen_ids = {}
local all_names_ok = true
local all_labels_ok = true
local ids_sequential = true
for i, entry in ipairs(pool) do
    if seen_ids[entry.lock_id] then
        ids_sequential = false
    end
    seen_ids[entry.lock_id] = true
    if entry.lock_id ~= 9000 + i then
        ids_sequential = false
    end
    if not valid_name(entry.name) then
        all_names_ok = false
        print("  bad name: " .. tostring(entry.name))
    end
    if #entry.labels > 8 then
        all_labels_ok = false
    end
    for _, l in ipairs(entry.labels) do
        if not valid_label(l) then
            all_labels_ok = false
            print("  bad label: " .. tostring(l))
        end
    end
end
check(ids_sequential, "lock_ids are unique and cover 9001..9012")
check(all_names_ok, "every pool name matches the protocol name regex")
check(all_labels_ok, "every pool label matches the protocol label regex (<=8 per lock)")

local has_members_style = false
local has_shard_style = false
for _, entry in ipairs(pool) do
    if entry.name:match("^/cluster/members/") then
        has_members_style = true
    end
    if entry.name:match("^/tenants/acme%-corp/ingest/shard%-") then
        has_shard_style = true
    end
end
check(has_members_style and has_shard_style, "pool mixes zk-style members and tenant shard names")

-- ---- seeded RNG ----------------------------------------------------------------

-- Deterministic LCG: rng(n) → 1..n, rng(m, n) → m..n (math.random shape).
local function seeded_rng(seed)
    local state = seed
    local function next_u31()
        state = (state * 1103515245 + 12345) % 2147483648
        return state
    end
    return function(m, n)
        if n == nil then
            if m == nil then
                return next_u31() / 2147483648
            end
            return (next_u31() % m) + 1
        end
        return m + (next_u31() % (n - m + 1))
    end
end

-- ---- pick_action ---------------------------------------------------------------

local rng = seeded_rng(42)
local holdings = {}
local by_id = {}
for _, entry in ipairs(workload.pool()) do
    by_id[entry.lock_id] = entry
end

local kinds = {}
local renew_release_valid = true
local acquire_valid = true
for _ = 1, 4000 do
    local action = workload.pick_action(rng, holdings)
    kinds[action.kind] = true
    if action.kind == "renew" or action.kind == "release" then
        if holdings[action.lock_id] == nil or action.holding ~= holdings[action.lock_id] then
            renew_release_valid = false
        end
    elseif action.kind == "acquire" then
        if holdings[action.lock_id] ~= nil then
            acquire_valid = false
        end
    end
    -- simulate a successful outcome to evolve holdings
    if action.kind == "acquire" or action.kind == "renew" then
        holdings[action.lock_id] = { holder = "u", lease_id = 1 }
    elseif action.kind == "release" then
        holdings[action.lock_id] = nil
    end
end
check(renew_release_valid, "pick_action only renews/releases locks present in holdings")
check(acquire_valid, "pick_action only acquires locks not in holdings")
check(
    kinds.acquire and kinds.renew and kinds.release and kinds.get,
    "seeded run produces all four action kinds"
)

-- empty holdings → never renew/release
local empty_ok = true
local rng2 = seeded_rng(7)
for _ = 1, 500 do
    local a = workload.pick_action(rng2, {})
    if a.kind == "renew" or a.kind == "release" then
        empty_ok = false
    end
end
check(empty_ok, "empty holdings never yields renew/release")

-- full holdings → never acquire
local full = {}
for _, entry in ipairs(workload.pool()) do
    full[entry.lock_id] = { holder = "u", lease_id = 1 }
end
local full_ok = true
local rng3 = seeded_rng(99)
for _ = 1, 500 do
    local a = workload.pick_action(rng3, full)
    if a.kind == "acquire" then
        full_ok = false
    end
end
check(full_ok, "fully-held pool never yields acquire")

-- ---- build_request --------------------------------------------------------------

local acquire = { kind = "acquire", lock_id = 9003, name = "/cluster/members/000003", labels = { "leader", "zk" } }
local line = workload.build_request(acquire, {
    message_id = "m",
    client_id = 1000000,
    request_num = 5,
    holder = "h-uuid",
    lease_id = 0,
    expiry_ms = 1722600001000,
})
check(line:match('"op"%s*:%s*"set"') ~= nil, "acquire builds op=set")
check(line:find('"holder":"h-uuid"', 1, true) ~= nil, "set carries holder")
check(line:match('"expiry"%s*:%s*1722600001000') ~= nil, "set carries expiry ms")
check(line:find('"name":"/cluster/members/000003"', 1, true) ~= nil, "set carries name")
check(line:find('"labels":["leader","zk"]', 1, true) ~= nil, "set carries labels array")
check(line:find("\n") == nil, "request line has no embedded newline")

local rel = workload.build_request(
    { kind = "release", lock_id = 9003 },
    { message_id = "m", client_id = 1000000, request_num = 6, holder = "h-uuid", lease_id = 9 }
)
check(rel:match('"op"%s*:%s*"release"') ~= nil, "release builds op=release")
check(rel:match('"lease_id"%s*:%s*9') ~= nil, "release carries lease_id")

local get = workload.build_request(
    { kind = "get", lock_id = 9003 },
    { message_id = "m", client_id = 1000000, request_num = 7 }
)
check(get:match('"op"%s*:%s*"get"') ~= nil, "get builds op=get")
check(get:find("holder", 1, true) == nil, "get carries no holder")

-- ---- parse_reply ------------------------------------------------------------------

local r = workload.parse_reply(
    '{"op":"set","granted":true,"event":"acquire","lease":{"lease_id":11,'
        .. '"holder":"h-uuid","expiry":1722600001000}}'
)
check(r ~= nil and r.kind == "set" and r.granted == true, "parses granted set reply")
check(r.lease_id == 11 and r.event == "acquire", "set reply carries lease_id and event")

local d = workload.parse_reply('{"op":"set","granted":false,"event":"deny","lease":null}')
check(d ~= nil and d.kind == "set" and d.granted == false, "parses denied set reply")

local relr = workload.parse_reply('{"op":"release","released":true,"event":"release","lease":null}')
check(relr ~= nil and relr.kind == "release" and relr.released == true, "parses release reply")
local relf = workload.parse_reply('{"op":"release","released":false,"lease":{"lease_id":3}}')
check(relf ~= nil and relf.released == false, "parses mismatched release reply")

local g = workload.parse_reply(
    '{"op":"get","lease":{"lease_id":4,"holder":"h-uuid","expiry":1722600001000}}'
)
check(g ~= nil and g.kind == "get" and g.holder == "h-uuid" and g.lease_id == 4, "parses get reply with live lease")
local gfree = workload.parse_reply('{"op":"get","lease":null}')
check(gfree ~= nil and gfree.kind == "get" and gfree.holder == nil, "parses get reply with null lease")

local redir = workload.parse_reply('{"error":"not_leader","epoch":5}')
check(redir ~= nil and redir.kind == "not_leader" and redir.epoch == 5, "parses not_leader redirect")
check(workload.parse_reply("garbage!!!") == nil, "malformed garbage yields nil")
check(workload.parse_reply(nil) == nil, "non-string yields nil")

-- ---- update_holdings --------------------------------------------------------------

local H = {}
local holder = "h-uuid"
local acq_action = { kind = "acquire", lock_id = 9001 }

workload.update_holdings(H, acq_action, { kind = "set", granted = true, lease_id = 21 }, holder)
check(
    H[9001] ~= nil and H[9001].holder == holder and H[9001].lease_id == 21,
    "granted acquire records holder and lease_id"
)

workload.update_holdings(H, { kind = "renew", lock_id = 9001 }, { kind = "set", granted = true, lease_id = 21 }, holder)
check(H[9001] ~= nil and H[9001].lease_id == 21, "granted renew keeps holding")

workload.update_holdings(H, { kind = "renew", lock_id = 9001 }, { kind = "set", granted = false }, holder)
check(H[9001] == nil, "denied renew drops holding (lost the lock)")

H[9002] = { holder = holder, lease_id = 5 }
workload.update_holdings(H, { kind = "release", lock_id = 9002 }, { kind = "release", released = true }, holder)
check(H[9002] == nil, "successful release drops holding")

H[9003] = { holder = holder, lease_id = 6 }
workload.update_holdings(H, { kind = "release", lock_id = 9003 }, { kind = "release", released = false }, holder)
check(H[9003] == nil, "mismatched release drops holding (we do not hold it)")

H[9004] = { holder = holder, lease_id = 7 }
workload.update_holdings(H, { kind = "get", lock_id = 9004 }, { kind = "get", holder = "other-uuid", lease_id = 8 }, holder)
check(H[9004] == nil, "get showing foreign holder drops holding")

H[9005] = { holder = holder, lease_id = 7 }
workload.update_holdings(H, { kind = "get", lock_id = 9005 }, { kind = "get", holder = holder, lease_id = 9 }, holder)
check(H[9005] ~= nil and H[9005].lease_id == 9, "get showing our holder refreshes lease_id")

H[9006] = { holder = holder, lease_id = 3 }
workload.update_holdings(H, { kind = "get", lock_id = 9006 }, { kind = "get", holder = nil }, holder)
check(H[9006] == nil, "get showing free lock drops holding")

if failures > 0 then
    print(failures .. " FAILURES")
    os.exit(1)
end
print("all workload tests passed")
