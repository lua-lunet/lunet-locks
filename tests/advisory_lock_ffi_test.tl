local advisory_lock = require("advisory_lock")
local cluster_config = require("cluster_config")
local tested = require("tested")

-- The sparse admin-assigned member ids, in descriptor (genesis succession)
-- order: 101 is the genesis primary.
local MEMBER_IDS = {101, 202, 303}

local function members(): {cluster_config.ClusterNode}
    return cluster_config.parse_jsonl(
        '{"id":101,"name":"n1","host":"127.0.0.1","port":27101,"genesis":true}\n'
            .. '{"id":202,"name":"n2","host":"127.0.0.1","port":27102,"genesis":true}\n'
            .. '{"id":303,"name":"n3","host":"127.0.0.1","port":27103,"genesis":true}\n'
    )
end

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
-- Reply outputs stay queued on their node. Peer addresses are member ids.
-- Bounded like the Rust harness.
local function index_of_id(id: integer): integer
    for index, member_id in ipairs(MEMBER_IDS) do
        if member_id == id then
            return index
        end
    end
    error("advisory_lock_ffi_test: unknown replica id " .. tostring(id))
end

local function route_until_quiet(nodes: {advisory_lock.Node})
    for _ = 1, 64 do
        local moved = false
        for source = 1, #nodes do
            for _, output in ipairs(nodes[source]:outputs()) do
                if output.kind == "send" then
                    moved = true
                    nodes[index_of_id(output.replica)]:receive(
                        MEMBER_IDS[source], output.bytes
                    )
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

-- A fresh three-node cluster brought to Normal: every node boots fenced;
-- the genesis primary (member id 101) self-promotes on a tick and the
-- Recovering backups adopt the view.
-- Pass journal_dir="" and journal_roll_bytes=0 to disable journaling.
local function boot_cluster(
    name: string, journal_dir: string, journal_roll_bytes: integer
): {advisory_lock.Node}
    local nodes: {advisory_lock.Node} = {}
    for index, own in ipairs(MEMBER_IDS) do
        local path = state_path(name .. tostring(index))
        os.remove(path)
        nodes[index] = advisory_lock.new(
            members(), "n" .. tostring(index), path, journal_dir, journal_roll_bytes
        )
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
        local nodes = boot_cluster("boot", "", 0)
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
                    .. tostring(MEMBER_IDS[index])
            )
        end
        close_cluster(nodes)
        tested.assert(
            {
                given = "three nodes booted over temp nonce files and pumped quiet",
                should = "all reach normal in era 1 view 0 behind member id 101",
                expected = "normal:1:0:101:101:101|normal:1:0:101:101:202|normal:1:0:101:101:303",
                actual = table.concat(summary, "|"),
            }
        )
    end
)

tested.test(
    "a client request on the leader commits and returns a correlated lock reply",
    function()
        local nodes = boot_cluster("request", "", 0)
        local expected_id = raw_message_id("07070707-0707-0707-0707-070707070707")
        local ok = nodes[1]:request(GET_JSON)
        local replies: {advisory_lock.Output} = {}
        for _ = 1, 64 do
            local moved = false
            for source = 1, #nodes do
                for _, output in ipairs(nodes[source]:outputs()) do
                    if output.kind == "send" then
                        moved = true
                        nodes[index_of_id(output.replica)]:receive(
                            MEMBER_IDS[source], output.bytes
                        )
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
        local nodes = boot_cluster("duplicate", "", 0)
        local ok_first = nodes[1]:request(SET_JSON)
        local first_replies: {advisory_lock.Output} = {}
        for _ = 1, 64 do
            local moved = false
            for source = 1, #nodes do
                for _, output in ipairs(nodes[source]:outputs()) do
                    if output.kind == "send" then
                        moved = true
                        nodes[index_of_id(output.replica)]:receive(
                            MEMBER_IDS[source], output.bytes
                        )
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
                        nodes[index_of_id(output.replica)]:receive(
                            MEMBER_IDS[source], output.bytes
                        )
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
        local nodes = boot_cluster("notleader", "", 0)
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
        local nodes = boot_cluster("garbage", "", 0)
        local raised = pcall(
            function() nodes[1]:receive(202, string.rep("\255", 64)) end
        )
        local truncated = pcall(function() nodes[1]:receive(202, "\0\0\0\0") end)
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
        local nodes = boot_cluster("quiet", "", 0)
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

tested.test(
    "journal-enabled boot writes fixed-size event records to the journal dir",
    function()
        local journal_dir = ".tmp/advisory-lock-ffi-journal-" .. tostring(os.time())
        os.execute('mkdir -p "' .. journal_dir .. '"')
        local nodes = boot_cluster("journal", journal_dir, 250)
        -- Drive a grant flow so the journal receives at least one hold event.
        nodes[1]:request(SET_JSON)
        for _ = 1, 64 do
            local moved = false
            for source = 1, #nodes do
                for _, output in ipairs(nodes[source]:outputs()) do
                    if output.kind == "send" then
                        moved = true
                        nodes[index_of_id(output.replica)]:receive(
                            MEMBER_IDS[source], output.bytes
                        )
                    end
                end
            end
            if not moved then
                break
            end
        end
        close_cluster(nodes)
        -- Scan the journal dir for any ev-*.bin file (open or rolled).
        local handle = io.popen('ls -1 "' .. journal_dir .. '" 2>/dev/null')
        local listing = handle and handle:read("*a") or ""
        if handle then
            handle:close()
        end
        local found_file: string = ""
        for line in listing:gmatch("[^\n]+") do
            if line:match("^ev%-.*%.bin$") then
                found_file = line
                break
            end
        end
        local size_ok = false
        if found_file ~= "" then
            local full_path = journal_dir .. "/" .. found_file
            local fh = io.open(full_path, "rb")
            if fh ~= nil then
                local data = fh:read("*a")
                fh:close()
                size_ok = (#data > 0) and (#data % 61 == 0)
            end
        end
        -- Clean up temp dir.
        os.execute('rm -rf "' .. journal_dir .. '"')
        tested.assert(
            {
                given = "a cluster booted with journal dir and tiny roll threshold",
                should = "produce at least one ev-*.bin whose size is a multiple of 61",
                expected = "true:true",
                actual = tostring(found_file ~= "") .. ":" .. tostring(size_ok),
            }
        )
    end
)

return tested
