-- The cluster deployment descriptor and THE live membership-addressing
-- scheme: a sparse, admin-assigned integer id -> {name, host, port} map, one
-- flattened JSON object per line (JSONL) so the file diffs and greps like a
-- log, not a blob.
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
-- - The file's line order is the explicit genesis succession sequence: the
--   descriptor order is passed to the core as the genesis member order, and
--   the core's `primary(v) = order[v mod N]` resolves over it. Line order —
--   not lexical name order — is authoritative, and every replica must carry
--   an identical descriptor file.
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

-- Minimal single-line JSON encode/decode restricted to this record's four
-- known keys. No general JSON support is needed (or wanted): a fixed,
-- typed schema keeps this module dependency-free and trivially auditable.
local function encode_line(node: ClusterNode): string
    return string.format(
        '{"id":%d,"name":%s,"host":%s,"port":%d}',
        node.id,
        string.format("%q", node.name),
        string.format("%q", node.host),
        node.port
    )
end

local function parse_line(line: string, line_no: integer): ClusterNode
    local id_text = line:match('"id"%s*:%s*(-?%d+)')
    local name_text = line:match('"name"%s*:%s*"([^"]*)"')
    local host_text = line:match('"host"%s*:%s*"([^"]*)"')
    local port_text = line:match('"port"%s*:%s*(%d+)')
    assert(
        id_text ~= nil and name_text ~= nil and host_text ~= nil and port_text ~= nil,
        "cluster_config: line "
            .. tostring(line_no)
            .. " must be {\"id\":N,\"name\":\"...\",\"host\":\"...\",\"port\":N}"
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
    return {id = id, name = name_text, host = host_text, port = port}
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
}
