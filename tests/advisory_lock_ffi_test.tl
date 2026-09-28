local advisory_lock = require("advisory_lock")
local cluster_config = require("cluster_config")
local tested = require("tested")

-- The sparse admin-assigned member ids, in descriptor (genesis succession)
-- order: 6619137 is the genesis primary (system 101, the genesis life).
local MEMBER_IDS = {6619137, 13238273, 19857409}

local function members(): {cluster_config.ClusterNode}
    return cluster_config.parse_jsonl(
        '{"id":6619137,"name":"n1","host":"127.0.0.1","port":27101,"genesis":true}\n'
            .. '{"id":13238273,"name":"n2","host":"127.0.0.1","port":27102,"genesis":true}\n'
            .. '{"id":19857409,"name":"n3","host":"127.0.0.1","port":27103,"genesis":true}\n'
    )
end

-- The request envelope the native Service decodes; message_id is the
-- canonical UUID text form of the 16 raw bytes the reply output carries.
local GET_JSON = '{"op":"get","message_id":"07070707-0707-0707-0707-070707070707","client_id":11,"request_num":13,"lock_id":17}'
local SET_JSON = '{"op":"set","message_id":"08080808-0808-0808-0808-080808080808","client_id":11,"request_num":14,"lock_id":9001,"lease":{"lease_id":1,"holder":"09090909-0909-0909-0909-090909090909","lease_ms":99999999999999}}'
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
-- the genesis primary (member id 6619137) self-promotes on a tick and the
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
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a client request on the leader commits and returns a correlated lock reply",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a duplicate request replays the cached reply without re-executing",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "request on a non-leader returns not_leader instead of raising",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "receive with garbage bytes raises the replication-message error",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "outputs on a quiet node is an empty array",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "journal-enabled boot writes fixed-size event records to the journal dir",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a stopped node refuses further entries and its marker records the clean drain",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

return tested
