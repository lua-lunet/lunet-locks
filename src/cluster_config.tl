-- The cluster deployment descriptor and THE live membership-addressing
-- scheme: a sparse, admin-assigned integer id -> {name, host, port, genesis}
-- map, one flattened JSON object per line (JSONL) so the file diffs and greps
-- like a log, not a blob.
--
-- Contract:
-- - Every entry's `id` is the replica's live `NodeId` on the ABI and in peer
--   routing; `config.tl`'s `members()` derives membership from this
--   descriptor, and `server.tl` maps ids to endpoints through it. Ids are
--   admin-assigned, sparse, and never recycled across deployments: a
--   recycled id would alias a superseded incarnation's identity. Sparse and
--   never-recycled ids remain an operator invariant this module cannot
--   enforce across runs (no persistent id allocator); it only rejects
--   in-file duplicate ids and duplicate names.
-- - The `genesis` field marks the deployment's founding membership. The
--   genesis lines (in line order) are the explicit genesis succession
--   sequence the core folds as the founding configuration
--   (`primary(v) = order[v mod N]` in the core); every replica must carry
--   the identical genesis lines. A member that joined a live cluster
--   through a committed reconfiguration is appended after the genesis lines
--   with `"genesis":false` — the descriptor relation between processes is a
--   superset with the new member appended and identical line order for
--   shared members. Running processes never reload the file; live members
--   learn a joining member's endpoint through the admin broadcast.
-- - The membership fingerprint (config.tl) is over the genesis lines only:
--   the deployment's identity is its founding membership, stable across
--   live superset growth.
-- - An id must fit the core's `NodeId(u32)` and cannot be 4294967295: the
--   native adapter reserves that value as its LEADER_UNKNOWN report.
--
-- This module only reads/writes the deployment descriptor file; it never
-- opens sockets and stays pure for the test suite.

local record ClusterNode
    id: integer
    name: string
    host: string
    port: integer
    genesis: boolean
end

local function ipv4(host: string): boolean
    local count = 0
    for part in host:gmatch("[^.]+") do
        if part:find("[^0-9]") ~= nil or #part == 0 or #part > 3 then
            return false
        end
        local value = tonumber(part)
        if
            value == nil
            or value < 0
            or value > 255
            or (#part > 1 and part:sub(1, 1) == "0")
        then
            return false
        end
        count = count + 1
    end
    return count == 4 and host:find("%.") ~= nil
end

-- Minimal single-line JSON encode/decode restricted to this record's five
-- known keys. No general JSON support is needed (or wanted): a fixed,
-- typed schema keeps this module dependency-free and trivially auditable.
local function encode_line(node: ClusterNode): string
    return string.format(
        '{"id":%d,"name":%s,"host":%s,"port":%d,"genesis":%s}',
        node.id,
        string.format("%q", node.name),
        string.format("%q", node.host),
        node.port,
        node.genesis and "true" or "false"
    )
end

local function parse_line(line: string, line_no: integer): ClusterNode
    local id_text = line:match('"id"%s*:%s*(-?%d+)')
    local name_text = line:match('"name"%s*:%s*"([^"]*)"')
    local host_text = line:match('"host"%s*:%s*"([^"]*)"')
    local port_text = line:match('"port"%s*:%s*(%d+)')
    local genesis_text = line:match('"genesis"%s*:%s*(%a+)')
    assert(
        id_text ~= nil
            and name_text ~= nil
            and host_text ~= nil
            and port_text ~= nil
            and genesis_text ~= nil,
        "cluster_config: line "
            .. tostring(line_no)
            .. " must be {\"id\":N,\"name\":\"...\",\"host\":\"...\",\"port\":N,\"genesis\":bool}"
    )
    assert(
        genesis_text == "true" or genesis_text == "false",
        "cluster_config: line " .. tostring(line_no) .. " genesis must be true or false"
    )
    local id = tonumber(id_text) as integer
    local port = tonumber(port_text) as integer
    assert(id >= 0, "cluster_config: line " .. tostring(line_no) .. " id must be >= 0")
    assert(
        id <= 4294967294,
        "cluster_config: line "
            .. tostring(line_no)
            .. " id must fit u32 and cannot be the reserved LEADER_UNKNOWN value 4294967295"
    )
    assert(
        name_text:match("^[%w_-]+$") ~= nil,
        "cluster_config: line " .. tostring(line_no) .. " name must be [%w_-]+"
    )
    assert(
        ipv4(host_text),
        "cluster_config: line " .. tostring(line_no) .. " host must be literal IPv4"
    )
    assert(
        port >= 1 and port <= 65535,
        "cluster_config: line " .. tostring(line_no) .. " port must be 1..65535"
    )
    return {
        id = id,
        name = name_text,
        host = host_text,
        port = port,
        genesis = genesis_text == "true",
    }
end

-- Parses JSONL text: one node per non-blank, non-comment (#...) line.
-- Rejects duplicate ids and duplicate names. Sparseness and never-recycled
-- ids are an operator invariant this module cannot enforce across runs
-- (that would need persistent id-allocator state); it only rejects
-- in-file duplicates.
local function parse_jsonl(text: string): {ClusterNode}
    local nodes: {ClusterNode} = {}
    local seen_ids: {integer: boolean} = {}
    local seen_names: {string: boolean} = {}
    local line_no = 0
    for line in (text .. "\n"):gmatch("([^\n]*)\n") do
        line_no = line_no + 1
        local trimmed = line:match("^%s*(.-)%s*$")
        if trimmed ~= "" and trimmed:sub(1, 1) ~= "#" then
            local node = parse_line(trimmed, line_no)
            assert(
                not seen_ids[node.id],
                "cluster_config: duplicate id "
                    .. tostring(node.id)
                    .. " at line "
                    .. tostring(line_no)
            )
            assert(
                not seen_names[node.name],
                "cluster_config: duplicate name "
                    .. node.name
                    .. " at line "
                    .. tostring(line_no)
            )
            seen_ids[node.id] = true
            seen_names[node.name] = true
            table.insert(nodes, node)
        end
    end
    assert(#nodes > 0, "cluster_config: at least one node is required")
    return nodes
end

-- The genesis succession: the descriptor's genesis lines, in line order.
-- This is the founding membership every replica must carry identically and
-- the order the core folds as the genesis configuration.
local function genesis_members(nodes: {ClusterNode}): {ClusterNode}
    local genesis: {ClusterNode} = {}
    for _, node in ipairs(nodes) do
        if node.genesis then
            table.insert(genesis, node)
        end
    end
    assert(#genesis >= 3, "cluster_config: at least three genesis members are required")
    return genesis
end

-- The superset relation between two deployments' descriptors: every genesis
-- line of the second appears, in order, as a line of the first (ids, names
-- and endpoints identical). A live superset growth appends joining members;
-- shared lines never move or change.
local function superset_of(larger: {ClusterNode}, smaller: {ClusterNode}): boolean
    local cursor = 1
    for _, node in ipairs(smaller) do
        local found = false
        while cursor <= #larger and not found do
            local candidate = larger[cursor]
            cursor = cursor + 1
            if
                candidate.id == node.id
                and candidate.name == node.name
                and candidate.host == node.host
                and candidate.port == node.port
            then
                found = true
            end
        end
        if not found then
            return false
        end
    end
    return true
end

local function encode_jsonl(nodes: {ClusterNode}): string
    local lines: {string} = {}
    for _, node in ipairs(nodes) do
        table.insert(lines, encode_line(node))
    end
    return table.concat(lines, "\n") .. "\n"
end

return {
    ClusterNode = ClusterNode,
    parse_jsonl = parse_jsonl,
    encode_jsonl = encode_jsonl,
    encode_line = encode_line,
    genesis_members = genesis_members,
    superset_of = superset_of,
}
