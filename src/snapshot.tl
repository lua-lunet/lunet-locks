-- The cluster membership snapshot: a new LAL peer kind alongside the VRR
-- and application packets, the era-qualified request/response pair, the
-- era+slot total comparison, the weighted quorum over agreeing snapshots,
-- and the membership sidecar codec. Pure: no sockets, no FFI, no clock --
-- the whole contract is testable without a runtime.
--
-- Contract:
-- - Snapshots are ADVISORY evidence and never a consensus mechanism:
--   safety stays with the replicated configuration commands. A discovered
--   configuration is adopted in memory and feeds the existing fenced-boot
--   path; nothing here bypasses a gate.
-- - The wire payloads are strict, fixed-shape, big-endian, and small. A
--   payload that does not decode exactly is dropped, never partially
--   applied. The response carries era, slot, members {id, weight,
--   endpoint} and nothing else; the deployment fingerprint rides the peer
--   envelope (a foreign fingerprint drops without ceremony at the
--   receive boundary, not here).
-- - `era` is the host membership model's configuration generation: a
--   monotone counter that starts at 0 over the deployment descriptor and
--   advances exactly once per reconfiguration commit the node itself
--   drives, and by adoption whenever an observed snapshot is newer. The
--   core's folded configuration remains the commit truth; a lagging host
--   model catches up from the snapshot + commit pair and never regresses.
-- - `slot` is the log slot at which that configuration was chosen: the
--   establishing operation's entry slot, captured from the reconfiguration
--   drive's own sends. The genesis configuration is slot 0. Era plus slot
--   make the up-to-dateness comparison total.
-- - Quorum arithmetic follows the deployment's weight rules: the
--   threshold is half the configuration's total weight plus one, and the
--   sum is taken over the AGREEING responders' weights. Weight-0 learners'
--   snapshots are collected but contribute nothing, so they never satisfy
--   a quorum.
-- - The sidecar is the lazy write-behind target next to the incarnation
--   marker: one JSONL header line, then one member line per member. It is
--   written on a routine tick, never on the message hot path; its last
--   bytes may be lost on power loss (documented loss window) and nothing
--   depends on it for safety. The marker's bump semantics are untouched.

local record SnapshotMember
    id: integer
    weight: integer
    endpoint: string
end

local record Snapshot
    era: integer
    slot: integer
    members: {SnapshotMember}
end

local SNAPSHOT_REQUEST = "\1"
local SNAPSHOT_RESPONSE = "\2"
-- The core bounds a configuration's membership at MAX_MEMBERS = 16
-- (`uvrr-core/src/configuration.rs`); a snapshot carries at most that.
local MAX_MEMBERS = 16
local MAX_U32 = 4294967295
local MAX_U16 = 65535
local U32_BASE = 4294967296

local cluster_config = require("cluster_config")
local config = require("config")

local function u32_bytes(value: integer): string
    return string.char(
        math.floor(value / 16777216) % 256,
        math.floor(value / 65536) % 256,
        math.floor(value / 256) % 256,
        value % 256
    )
end

local function parse_u32(bytes: string): integer
    return (
        (bytes:byte(1) or 0) * 16777216 + (bytes:byte(2) or 0) * 65536 + (
            bytes:byte(3) or 0
        ) * 256 + (bytes:byte(4) or 0)
    ) as integer
end

local function u16_bytes(value: integer): string
    return string.char(math.floor(value / 256) % 256, value % 256)
end

local function parse_u16(bytes: string): integer
    return ((bytes:byte(1) or 0) * 256 + (bytes:byte(2) or 0)) as integer
end

local function valid_member(id: integer, weight: integer, endpoint: string): boolean
    if id < 0 or id > MAX_U32 or weight < 0 or weight > MAX_U32 then
        return false
    end
    local ok = pcall(function() config.endpoint(endpoint) end)
    return ok
end

-- The request is one tag byte: the requester needs no state of its own;
-- every responder answers from its current membership model.
local function encode_request(): string
    return SNAPSHOT_REQUEST
end

local function decode_request(payload: string): boolean
    return payload == SNAPSHOT_REQUEST
end

-- Members ride in canonical order: strictly ascending ids, unique. One
-- shape per membership, no second representation to reconcile.
local function canonical(members: {SnapshotMember}): boolean
    local previous = -1
    for _, member in ipairs(members) do
        if member.id <= previous then
            return false
        end
        previous = member.id
    end
    return true
end

local function encode_response(snap: Snapshot): string
    assert(snap.era >= 0 and snap.era <= MAX_U32, "snapshot: era must be a u32")
    assert(snap.slot >= 0, "snapshot: slot must be non-negative")
    assert(
        #snap.members >= 1 and #snap.members <= MAX_MEMBERS,
        "snapshot: membership must carry 1..16 members"
    )
    assert(canonical(snap.members), "snapshot: members must ascend by id")
    local encoded = SNAPSHOT_RESPONSE
        .. u32_bytes(snap.era)
        .. u32_bytes(math.floor(snap.slot / U32_BASE))
        .. u32_bytes(snap.slot % U32_BASE)
        .. u16_bytes(#snap.members)
    for _, member in ipairs(snap.members) do
        assert(
            valid_member(member.id, member.weight, member.endpoint),
            "snapshot: invalid member "
                .. tostring(member.id)
                .. " (weight "
                .. tostring(member.weight)
                .. ", endpoint "
                .. member.endpoint
                .. ")"
        )
        assert(#member.endpoint <= MAX_U16, "snapshot: endpoint too long")
        encoded = encoded
            .. u32_bytes(member.id)
            .. u32_bytes(member.weight)
            .. u16_bytes(#member.endpoint)
            .. member.endpoint
    end
    return encoded
end

local function decode_response(payload: string): (boolean, Snapshot)
    local nothing: Snapshot = {era = 0, slot = 0, members = {}}
    if #payload < 1 + 4 + 8 + 2 or payload:sub(1, 1) ~= SNAPSHOT_RESPONSE then
        return false, nothing
    end
    local era = parse_u32(payload:sub(2, 5))
    local slot = parse_u32(payload:sub(6, 9)) * U32_BASE + parse_u32(
        payload:sub(10, 13)
    )
    local count = parse_u16(payload:sub(14, 15))
    if count < 1 or count > MAX_MEMBERS then
        return false, nothing
    end
    local members: {SnapshotMember} = {}
    local cursor = 16
    local previous = -1
    for _ = 1, count do
        if #payload < cursor + 9 then
            return false, nothing
        end
        local id = parse_u32(payload:sub(cursor, cursor + 3))
        local weight = parse_u32(payload:sub(cursor + 4, cursor + 7))
        local endpoint_len = parse_u16(payload:sub(cursor + 8, cursor + 9))
        if #payload < cursor + 9 + endpoint_len then
            return false, nothing
        end
        local endpoint = payload:sub(cursor + 10, cursor + 9 + endpoint_len)
        cursor = cursor + 10 + endpoint_len
        if id <= previous or not valid_member(id, weight, endpoint) then
            return false, nothing
        end
        previous = id
        table.insert(members, {id = id, weight = weight, endpoint = endpoint})
    end
    if cursor ~= #payload + 1 then
        return false, nothing
    end
    return true, {era = era, slot = slot, members = members}
end

-- The up-to-dateness comparison: era plus slot, lexicographic -- every
-- configuration knows the slot at which it was chosen, so the pair is
-- total. `newer(a, b)` is true when the first names a configuration the
-- second does not cover yet.
local function newer(
    era: integer, slot: integer, own_era: integer, own_slot: integer
): boolean
    return era > own_era or (era == own_era and slot > own_slot)
end

-- Two snapshots agree when they name the same configuration: the same
-- era, the same choosing slot, and the same member set in the same
-- canonical order.
local function agreeing(a: Snapshot, b: Snapshot): boolean
    if a.era ~= b.era or a.slot ~= b.slot or #a.members ~= #b.members then
        return false
    end
    for index, member in ipairs(a.members) do
        local other = b.members[index]
        if
            other == nil
            or member.id ~= other.id
            or member.weight ~= other.weight
            or member.endpoint ~= other.endpoint
        then
            return false
        end
    end
    return true
end

local function member_weight(members: {SnapshotMember}, id: integer): integer
    for _, member in ipairs(members) do
        if member.id == id then
            return member.weight
        end
    end
    return -1
end

-- The weighted majority the deployment's configurations vote with
-- (`WeightedMajority`): the threshold is half the total weight plus one,
-- and the sum runs over the responders whose snapshots agree. A weight-0
-- learner's agreement is collected and contributes nothing, so learners
-- never satisfy a quorum.
local function quorum_reached(
    members: {SnapshotMember}, responded: {integer: boolean}
): boolean
    local total = 0
    for _, member in ipairs(members) do
        total = total + member.weight
    end
    local threshold = math.floor(total / 2) + 1
    local sum = 0
    for _, member in ipairs(members) do
        if responded[member.id] then
            sum = sum + member.weight
        end
    end
    return sum >= threshold
end

-- The boot state a node's descriptor gives it: era 1 (the founding
-- configuration the core folds, chosen at slot 0), the descriptor's
-- GENESIS lines in line order at weight 1. A post-genesis descriptor line
-- is an addressing row, not a membership fact: the node can talk to the
-- member, and snapshots and dissemination teach whether it is still in
-- the live configuration and at what weight.
local function descriptor_model(nodes: {cluster_config.ClusterNode}): Snapshot
    local members: {SnapshotMember} = {}
    for _, node in ipairs(nodes) do
        if node.genesis then
            table.insert(
                members,
                {
                    id = node.id,
                    weight = 1,
                    endpoint = node.host .. ":" .. tostring(node.port),
                }
            )
        end
    end
    table.sort(
        members,
        function(a: SnapshotMember, b: SnapshotMember): boolean return a.id < b.id end
    )
    return {era = 1, slot = 0, members = members}
end

local function endpoint_of(members: {SnapshotMember}, id: integer): string
    for _, member in ipairs(members) do
        if member.id == id then
            return member.endpoint
        end
    end
    return ""
end

local function sorted(members: {SnapshotMember}): {SnapshotMember}
    local copy: {SnapshotMember} = {}
    for _, member in ipairs(members) do
        table.insert(copy, member)
    end
    table.sort(
        copy,
        function(a: SnapshotMember, b: SnapshotMember): boolean return a.id < b.id end
    )
    return copy
end

-- The leader-side application of one committed admin verb to the
-- membership model: a join appends the member at weight 0 (additive and
-- idempotent), an increment promotes it to a voter, a decrement returns
-- it to weight 0, and a leave removes it. Refused changes report false
-- and touch nothing.
local function apply_change(
    members: {SnapshotMember}, action: string, id: integer, endpoint: string
): (boolean, {SnapshotMember})
    local updated: {SnapshotMember} = {}
    local exists = false
    for _, member in ipairs(members) do
        if member.id == id then
            exists = true
        else
            table.insert(updated, member)
        end
    end
    if action == "join" then
        if exists or not valid_member(id, 0, endpoint) then
            return false, members
        end
        table.insert(updated, {id = id, weight = 0, endpoint = endpoint})
        return true, sorted(updated)
    elseif action == "increment" then
        if not exists then
            return false, members
        end
        table.insert(
            updated, {id = id, weight = 1, endpoint = endpoint_of(members, id)}
        )
        return true, sorted(updated)
    elseif action == "decrement" then
        if not exists then
            return false, members
        end
        table.insert(
            updated, {id = id, weight = 0, endpoint = endpoint_of(members, id)}
        )
        return true, sorted(updated)
    elseif action == "leave" then
        if not exists then
            return false, members
        end
        return true, sorted(updated)
    else
        return false, members
    end
end

-- The membership sidecar next to the incarnation marker: line 1 is the
-- header `{"format":"membership-sidecar/v1","era":N,"slot":N}`, then one
-- line per member `{"id":N,"weight":N,"endpoint":"host:port"}` in the
-- canonical ascending-id order. Written atomically (temporary file,
-- rename); read back strictly at boot, and a sidecar that does not parse
-- is ignored entirely -- the node falls back to its descriptor and
-- re-learns through discovery.
local function encode_sidecar(snap: Snapshot): string
    assert(
        snap.era >= 0 and snap.era <= MAX_U32 and snap.slot >= 0,
        "snapshot: invalid sidecar generation"
    )
    assert(canonical(snap.members), "snapshot: members must ascend by id")
    local lines: {string} = {
        string.format(
            '{"format":"membership-sidecar/v1","era":%d,"slot":%d}', snap.era, snap.slot
        ),
    }
    for _, member in ipairs(snap.members) do
        table.insert(
            lines,
            string.format(
                '{"id":%d,"weight":%d,"endpoint":%s}',
                member.id,
                member.weight,
                string.format("%q", member.endpoint)
            )
        )
    end
    return table.concat(lines, "\n") .. "\n"
end

local function parse_sidecar_member(line: string, line_no: integer): SnapshotMember
    local id_text = line:match('"id"%s*:%s*(%d+)')
    local weight_text = line:match('"weight"%s*:%s*(%d+)')
    local endpoint_text = line:match('"endpoint"%s*:%s*"([^"]*)"')
    assert(
        id_text ~= nil and weight_text ~= nil and endpoint_text ~= nil,
        "snapshot: sidecar line "
            .. tostring(line_no)
            .. ' must be {"id":N,"weight":N,"endpoint":"host:port"}'
    )
    local id = tonumber(id_text) as integer
    local weight = tonumber(weight_text) as integer
    assert(
        valid_member(id, weight, endpoint_text),
        "snapshot: sidecar line " .. tostring(line_no) .. " carries an invalid member"
    )
    return {id = id, weight = weight, endpoint = endpoint_text}
end

local function decode_sidecar(text: string): (boolean, Snapshot)
    local nothing: Snapshot = {era = 0, slot = 0, members = {}}
    local lines: {string} = {}
    for line in (text .. "\n"):gmatch("([^\n]*)\n") do
        local trimmed = line:match("^%s*(.-)%s*$")
        if trimmed ~= "" then
            table.insert(lines, trimmed)
        end
    end
    if #lines < 2 then
        return false, nothing
    end
    local format = lines[1]:match('"format"%s*:%s*"([^"]*)"')
    local era_text = lines[1]:match('"era"%s*:%s*(%d+)')
    local slot_text = lines[1]:match('"slot"%s*:%s*(%d+)')
    if format ~= "membership-sidecar/v1" or era_text == nil or slot_text == nil then
        return false, nothing
    end
    local era = tonumber(era_text) as integer
    local slot = tonumber(slot_text) as integer
    if era > MAX_U32 then
        return false, nothing
    end
    local members: {SnapshotMember} = {}
    for index = 2, #lines do
        local member = parse_sidecar_member(lines[index], index)
        if #members > 0 and member.id <= members[#members].id then
            return false, nothing
        end
        table.insert(members, member)
    end
    if #members > MAX_MEMBERS then
        return false, nothing
    end
    return true, {era = era, slot = slot, members = members}
end

return {
    Snapshot = Snapshot,
    SnapshotMember = SnapshotMember,
    SNAPSHOT_REQUEST = SNAPSHOT_REQUEST,
    SNAPSHOT_RESPONSE = SNAPSHOT_RESPONSE,
    MAX_MEMBERS = MAX_MEMBERS,
    encode_request = encode_request,
    decode_request = decode_request,
    encode_response = encode_response,
    decode_response = decode_response,
    newer = newer,
    agreeing = agreeing,
    member_weight = member_weight,
    quorum_reached = quorum_reached,
    descriptor_model = descriptor_model,
    apply_change = apply_change,
    encode_sidecar = encode_sidecar,
    decode_sidecar = decode_sidecar,
}
