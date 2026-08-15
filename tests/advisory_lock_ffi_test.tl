local advisory_lock = require("advisory_lock")
local tested = require("tested")

local members = {"n1", "n2", "n3"}

-- The request envelope the native Service decodes; message_id is the
-- canonical UUID text form of the 16 raw bytes the reply output carries.
local GET_JSON = '{"op":"get","message_id":"07070707-0707-0707-0707-070707070707","client_id":11,"request_num":13,"lock_id":17}'
local SET_JSON = '{"op":"set","message_id":"08080808-0808-0808-0808-080808080808","client_id":11,"request_num":14,"lock_id":9001,"lease":{"lease_id":1,"holder":"09090909-0909-0909-0909-090909090909","expiry":99999999999999}}'
local MESSAGE_ID_BYTES = 16

local function state_path(name: string): string
    return ".tmp/advisory-lock-ffi-" .. name .. "-" .. tostring(os.time()) .. ".nonce"
end

local function raw_message_id(uuid: string): string
    local hex = uuid:gsub("-", "")
    return (
        hex:gsub(
            "..",
            function(pair: string): string
                return string.char((tonumber(pair, 16) or 0) as integer)
            end
        )
    )
end

-- Deliver every queued "send" on every node to its destination, draining
-- whatever the destination emits in answer, until no node holds a send.
-- Reply outputs stay queued on their node. Bounded like the Rust harness.
local function route_until_quiet(nodes: {advisory_lock.Node})
    for _ = 1, 64 do
        local moved = false
        for source = 1, #nodes do
            for _, output in ipairs(nodes[source]:outputs()) do
                if output.kind == "send" then
                    moved = true
                    nodes[output.replica + 1]:receive(source - 1, output.bytes)
                end
            end
        end
        if not moved then
            return
        end
    end
    error("advisory_lock_ffi_test: route_until_quiet exceeded its bound")
end

-- Drain and discard every queued output (used to drop boot-time Recovery
-- traffic, mirroring the Rust harness's outputs.clear()).
local function drain(nodes: {advisory_lock.Node})
    for _, node in ipairs(nodes) do
        node:outputs()
    end
end

-- A fresh three-node cluster brought to Normal: every node boots fenced and
-- starts a recovery attempt inside `new`; the genesis primary (member index
-- 0) self-promotes on a tick and the Recovering backups adopt the view.
local function boot_cluster(name: string): {advisory_lock.Node}
    local nodes: {advisory_lock.Node} = {}
    for index, own in ipairs(members) do
        local path = state_path(name .. tostring(index))
        os.remove(path)
        nodes[index] = advisory_lock.new(members, own, path)
    end
    drain(nodes)
    nodes[1]:idle()
    route_until_quiet(nodes)
    return nodes
end

local function close_cluster(nodes: {advisory_lock.Node})
    for _, node in ipairs(nodes) do
        node:close()
    end
end

tested.test(
    "three in-process nodes boot fenced, recover, and agree on the genesis view",
    function()
        local nodes = boot_cluster("boot")
        local summary: {string} = {}
        for index, node in ipairs(nodes) do
            local status = node:status()
            table.insert(
                summary,
                status.state
                    .. ":"
                    .. tostring(status.era)
                    .. ":"
                    .. tostring(status.view)
                    .. ":"
                    .. tostring(status.leader)
                    .. ":"
                    .. tostring(node:leader_for_view(status.era, status.view))
                    .. ":"
                    .. tostring(index - 1)
            )
        end
        close_cluster(nodes)
        tested.assert(
            {
                given = "three nodes booted over temp nonce files and pumped quiet",
                should = "all reach normal in era 1 view 0 behind member 0",
                expected = "normal:1:0:0:0:0|normal:1:0:0:0:1|normal:1:0:0:0:2",
                actual = table.concat(summary, "|"),
            }
        )
    end
)

tested.test(
    "a client request on the leader commits and returns a correlated lock reply",
    function()
        local nodes = boot_cluster("request")
        local expected_id = raw_message_id("07070707-0707-0707-0707-070707070707")
        local ok = nodes[1]:request(GET_JSON)
        local replies: {advisory_lock.Output} = {}
        for _ = 1, 64 do
            local moved = false
            for source = 1, #nodes do
                for _, output in ipairs(nodes[source]:outputs()) do
                    if output.kind == "send" then
                        moved = true
                        nodes[output.replica + 1]:receive(source - 1, output.bytes)
                    else
                        table.insert(replies, output)
                    end
                end
            end
            if not moved then
                break
            end
        end
        local reply = replies[1]
        local reply_json = reply and reply.bytes or ""
        local granted = reply_json:match('"granted"%s*:%s*(%a+)')
        local reply_message_id = reply_json:match('"message_id"%s*:%s*"([0-9a-f%-]+)"')
        local lock_id = reply_json:match('"lock_id"%s*:%s*(%d+)')
        close_cluster(nodes)
        tested.assert(
            {
                given = "a get envelope proposed on the genesis primary",
                should = "commit and surface exactly one reply carrying the request message_id",
                expected = "true:1:true:07070707-0707-0707-0707-070707070707:17:true",
                actual = tostring(ok)
                    .. ":"
                    .. tostring(#replies)
                    .. ":"
                    .. tostring(
                        reply ~= nil
                            and reply.message_id == expected_id
                            and #reply.message_id == MESSAGE_ID_BYTES
                    ) .. ":"
                    .. tostring(reply_message_id)
                    .. ":"
                    .. tostring(lock_id)
                    .. ":"
                    .. tostring(granted == nil),
            }
        )
    end
)

tested.test(
    "a duplicate request replays the cached reply without re-executing",
    function()
        local nodes = boot_cluster("duplicate")
        local ok_first = nodes[1]:request(SET_JSON)
        local first_replies: {advisory_lock.Output} = {}
        for _ = 1, 64 do
            local moved = false
            for source = 1, #nodes do
                for _, output in ipairs(nodes[source]:outputs()) do
                    if output.kind == "send" then
                        moved = true
                        nodes[output.replica + 1]:receive(source - 1, output.bytes)
                    else
                        table.insert(first_replies, output)
                    end
                end
            end
            if not moved then
                break
            end
        end
        local first = first_replies[1]
        local first_granted = first and first.bytes:match('"granted"%s*:%s*(%a+)')
        -- Same message_id, same JSON: the adapter replays the cached reply
        -- bytes and proposes nothing to the cluster (B2).
        local ok_second = nodes[1]:request(SET_JSON)
        local second_outputs = nodes[1]:outputs()
        local replay = second_outputs[1]
        local sends = 0
        for _, output in ipairs(second_outputs) do
            if output.kind == "send" then
                sends = sends + 1
            end
        end
        -- A contended set from a different holder is rejected, proving the
        -- first acquisition is still the only one on the lock.
        local contested = '{"op":"set","message_id":"0a0a0a0a-0a0a-0a0a-0a0a-0a0a0a0a0a0a","client_id":12,"request_num":1,"lock_id":9001,"lease":{"lease_id":2,"holder":"0b0b0b0b-0b0b-0b0b-0b0b-0b0b0b0b0b0b","expiry":99999999999999}}'
        local ok_third = nodes[1]:request(contested)
        local third_replies: {advisory_lock.Output} = {}
        for _ = 1, 64 do
            local moved = false
            for source = 1, #nodes do
                for _, output in ipairs(nodes[source]:outputs()) do
                    if output.kind == "send" then
                        moved = true
                        nodes[output.replica + 1]:receive(source - 1, output.bytes)
                    else
                        table.insert(third_replies, output)
                    end
                end
            end
            if not moved then
                break
            end
        end
        local contested_reply = third_replies[1]
        local contested_granted = contested_reply
            and contested_reply.bytes:match('"granted"%s*:%s*(%a+)')
        close_cluster(nodes)
        tested.assert(
            {
                given = "a set request, its exact duplicate, then a contending set",
                should = "grant once, replay identical bytes, and refuse the contender",
                expected = "true:true:0:true:true:true:false",
                actual = tostring(ok_first)
                    .. ":"
                    .. tostring(ok_second)
                    .. ":"
                    .. tostring(sends)
                    .. ":"
                    .. tostring(
                        replay ~= nil and first ~= nil and replay.bytes == first.bytes
                    ) .. ":"
                    .. tostring(first_granted)
                    .. ":"
                    .. tostring(ok_third)
                    .. ":"
                    .. tostring(contested_granted),
            }
        )
    end
)

tested.test(
    "request on a non-leader returns not_leader instead of raising",
    function()
        local nodes = boot_cluster("notleader")
        local ok, err = nodes[2]:request(GET_JSON)
        local quiet = #nodes[2]:outputs() == 0
        close_cluster(nodes)
        tested.assert(
            {
                given = "a client envelope proposed on a backup",
                should = "return nil plus not_leader and queue nothing",
                expected = "nil:not_leader:true",
                actual = tostring(ok)
                    .. ":"
                    .. tostring(err)
                    .. ":"
                    .. tostring(quiet),
            }
        )
    end
)

tested.test(
    "receive with garbage bytes raises the replication-message error",
    function()
        local nodes = boot_cluster("garbage")
        local raised = pcall(function() nodes[1]:receive(1, string.rep("\255", 64)) end)
        local truncated = pcall(function() nodes[1]:receive(1, "\0\0\0\0") end)
        close_cluster(nodes)
        tested.assert(
            {
                given = "undecodable datagrams delivered to a node",
                should = "raise the mapped VRR message error on both",
                expected = "false:false",
                actual = tostring(raised) .. ":" .. tostring(truncated),
            }
        )
    end
)

tested.test(
    "outputs on a quiet node is an empty array",
    function()
        local nodes = boot_cluster("quiet")
        local drained = 0
        for _, node in ipairs(nodes) do
            drained = drained + #node:outputs()
        end
        close_cluster(nodes)
        tested.assert(
            {
                given = "a booted cluster with no in-flight work",
                should = "report no pending outputs on any node",
                expected = 0,
                actual = drained,
            }
        )
    end
)

return tested
