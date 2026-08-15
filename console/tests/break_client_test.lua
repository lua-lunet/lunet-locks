-- Unit test for console/openresty/lualib/break_client.lua pure functions
-- (plain luajit, no framework: assert + exit code). Run from anywhere;
-- paths resolve relative to this script.
--
--   luajit console/tests/break_client_test.lua

local script_dir = arg[0]:match("^(.*)/[^/]*$") or "."
package.path = script_dir .. "/../openresty/lualib/?.lua;" .. package.path

local break_client = require("break_client")

local failures = 0
local function check(cond, msg)
    if cond then
        print("ok - " .. msg)
    else
        failures = failures + 1
        print("FAIL - " .. msg)
    end
end

-- ---- UUID v4 ----------------------------------------------------------------

local uuid = break_client.new_uuid4()
check(
    uuid:match("^%x%x%x%x%x%x%x%x%-%x%x%x%x%-4%x%x%x%-[89ab]%x%x%x%-%x%x%x%x%x%x%x%x%x%x%x%x$")
        ~= nil,
    "new_uuid4 returns RFC 4122 version-4 variant-1 UUID"
)
check(break_client.new_uuid4() ~= break_client.new_uuid4(), "uuids differ across calls")

-- ---- request build ------------------------------------------------------------

local dict_store = {}
local fake_dict = {
    incr = function(_, key, delta, init)
        if dict_store[key] == nil then
            dict_store[key] = init or 0
        end
        dict_store[key] = dict_store[key] + delta
        return dict_store[key]
    end,
}

local rn1 = break_client.next_request_num(fake_dict)
local rn2 = break_client.next_request_num(fake_dict)
check(rn2 == rn1 + 1, "next_request_num increases via dict:incr")

local line = break_client.build_request(9001, uuid, break_client.ADMIN_CLIENT_ID, rn1)
check(line:match('"op"%s*:%s*"break"') ~= nil, "request carries op=break")
check(line:find('"message_id":"' .. uuid .. '"', 1, true) ~= nil, "request echoes message_id")
check(
    line:match('"client_id"%s*:%s*4294967295') ~= nil,
    "request uses reserved admin client_id 4294967295"
)
check(
    line:match('"request_num"%s*:%s*' .. tostring(rn1)) ~= nil,
    "request carries request_num"
)
check(line:match('"lock_id"%s*:%s*9001') ~= nil, "request carries lock_id")
check(line:find("\n") == nil, "request line has no embedded newline")

-- ---- reply parsing ------------------------------------------------------------

local reply = break_client.parse_reply(
    '{"op":"break","message_id":"x","request_num":1,"lock_id":9001,'
        .. '"broken":true,"event":"break","lease":{"lease_id":42,"holder":null,'
        .. '"expiry":null,"name":"/a","labels":[],"taken_at_ms":null,"renew_count":0}}'
)
check(reply ~= nil and reply.kind == "reply", "parses break reply")
check(reply.broken == true, "broken:true parsed")
check(reply.lease_id == 42, "lease_id extracted from cleared lease")
check(reply.event == "break", "event extracted")

local free = break_client.parse_reply(
    '{"op":"break","message_id":"x","request_num":2,"lock_id":9001,'
        .. '"broken":false,"event":"break","lease":null}'
)
check(free ~= nil and free.broken == false, "broken:false (idempotent) parsed")
check(free.lease_id == nil, "lease null yields no lease_id")

local redirect = break_client.parse_reply('{"error":"not_leader","epoch":7}')
check(
    redirect ~= nil and redirect.kind == "not_leader" and redirect.epoch == 7,
    "not_leader redirect extracts epoch"
)

local bad, baderr = break_client.parse_reply("this is not json at all")
check(bad == nil and baderr ~= nil, "malformed garbage yields error")
check(break_client.parse_reply('{"error":"not_leader"}') == nil, "redirect without epoch is malformed")
check(break_client.parse_reply("") == nil, "empty line is malformed")
check(break_client.parse_reply(nil) == nil, "non-string is malformed")

-- ---- epoch → leader mapping ----------------------------------------------------
-- vrr-core src/vrr.rs n(): leader = epoch % membership_size (0-based index
-- into the ordered membership); surfaced via Node:leader_for_epoch and used
-- by src/server.tl's FORWARD_NOT_LEADER path.

for epoch = 0, 23 do
    check(
        break_client.leader_for_epoch(epoch, 3) == epoch % 3,
        "epoch " .. epoch .. " maps to " .. (epoch % 3) .. " of 3 members"
    )
end
for epoch = 0, 24 do
    check(
        break_client.leader_for_epoch(epoch, 5) == epoch % 5,
        "epoch " .. epoch .. " maps to " .. (epoch % 5) .. " of 5 members"
    )
end
check(break_client.leader_for_epoch(3, 0) == nil, "no membership → no leader")

-- ---- cluster.json parse ---------------------------------------------------------

local members = break_client.parse_cluster(
    '{"members":[{"name":"n1","client":"127.0.0.1:7001","peer":"127.0.0.1:9001"},'
        .. '{"name":"n2","client":"127.0.0.1:7002","peer":"127.0.0.1:9002"},'
        .. '{"name":"n3","client":"127.0.0.1:7003","peer":"127.0.0.1:9003"}]}'
)
check(members ~= nil and #members == 3, "parse_cluster reads 3 members")
check(
    members[1].name == "n1" and members[1].host == "127.0.0.1" and members[1].port == 7001,
    "member host/port split from client endpoint"
)
check(members[3].name == "n3" and members[3].port == 7003, "ordered membership preserved")
check(break_client.parse_cluster("") == nil, "empty cluster text yields nil")
check(break_client.parse_cluster("{}") == nil, "memberless cluster text yields nil")

-- ---- transport (fake socket) ----------------------------------------------------

-- Fake socket factory scripting per-connect outcomes.
local function fake_tcp_factory(script)
    local call = 0
    local connected_to = {}
    return function()
        call = call + 1
        local step = script[math.min(call, #script)]
        local sock = {
            settimeout = function() end,
            connect = function(_, host, port)
                connected_to[#connected_to + 1] = host .. ":" .. port
                if step.connect_err ~= nil then
                    return nil, step.connect_err
                end
                return 1
            end,
            send = function()
                return 1
            end,
            receive = function()
                if step.recv_err ~= nil then
                    return nil, step.recv_err
                end
                return step.reply
            end,
            close = function() end,
        }
        return sock
    end,
        connected_to
end

local cluster = {
    members = {
        { name = "n1", client = "127.0.0.1:7001" },
        { name = "n2", client = "127.0.0.1:7002" },
        { name = "n3", client = "127.0.0.1:7003" },
    },
}

-- Direct success on the first member.
local factory, hits = fake_tcp_factory({
    { reply = '{"broken":true,"event":"break","lease":{"lease_id":9}}' },
})
local res = break_client.break_lock(
    9001,
    cluster,
    { dict = fake_dict, socket_tcp = factory, uuid4 = function() return uuid end }
)
check(res ~= nil and res.broken == true and res.lease_id == 9, "transport: direct break succeeds")
check(#hits == 1 and hits[1] == "127.0.0.1:7001", "transport: starts at first ordered member")

-- not_leader epoch 2 with 3 members → leader index 2 (third member).
factory, hits = fake_tcp_factory({
    { reply = '{"error":"not_leader","epoch":2}' },
    { reply = '{"broken":true,"event":"break","lease":{"lease_id":10}}' },
})
res = break_client.break_lock(
    9001,
    cluster,
    { dict = fake_dict, socket_tcp = factory, uuid4 = function() return uuid end }
)
check(res ~= nil and res.broken == true, "transport: follows not_leader redirect")
check(
    #hits == 2 and hits[2] == "127.0.0.1:7003",
    "transport: epoch 2 of 3 redirects to member index 2"
)

-- Unreachable first member falls through to the next.
factory, hits = fake_tcp_factory({
    { connect_err = "connection refused" },
    { reply = '{"broken":false,"event":"break","lease":null}' },
})
res = break_client.break_lock(
    9001,
    cluster,
    { dict = fake_dict, socket_tcp = factory, uuid4 = function() return uuid end }
)
check(res ~= nil and res.broken == false, "transport: skips unreachable member")
check(#hits == 2 and hits[2] == "127.0.0.1:7002", "transport: unreachable → next member")

-- Always redirecting beyond the cap → 502.
factory = fake_tcp_factory({
    { reply = '{"error":"not_leader","epoch":0}' },
})
local rerr
res, rerr = break_client.break_lock(
    9001,
    cluster,
    { dict = fake_dict, socket_tcp = factory, uuid4 = function() return uuid end }
)
check(res == nil and rerr.status == 502, "transport: redirect cap yields 502")

-- Malformed reply → 502.
factory = fake_tcp_factory({ { reply = "garbage!!!" } })
res, rerr = break_client.break_lock(
    9001,
    cluster,
    { dict = fake_dict, socket_tcp = factory, uuid4 = function() return uuid end }
)
check(res == nil and rerr.status == 502, "transport: malformed reply yields 502")

-- Empty cluster → 502.
res, rerr = break_client.break_lock(9001, { members = {} }, { dict = fake_dict })
check(res == nil and rerr.status == 502, "transport: empty cluster yields 502")

if failures > 0 then
    print(failures .. " FAILURES")
    os.exit(1)
end
print("all break_client tests passed")
