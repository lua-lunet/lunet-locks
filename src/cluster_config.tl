-- Admin-facing cluster deployment descriptor: a sparse, admin-assigned
-- integer id -> {name, host, port} map, one flattened JSON object per line
-- (JSONL) so the file diffs and greps like a log, not a blob.
--
-- Scope note (see AGENTS.md "vrr-core" boundary): this module is additive
-- and local-only. `config.tl`'s `members()` derives each replica's
-- protocol-level index from its *position in a lexically-sorted name list*,
-- and that index is threaded straight through the vrr-core FFI boundary
-- (see `server.tl`'s `index_by_endpoint`/`endpoint_by_index` and the
-- `leader`/`to`/`replica` fields returned by the native library). Making
-- the ids defined here (sparse, admin-chosen, never recycled) the *actual*
-- replica addressing scheme would require vrr-core to accept non-positional
-- node identifiers -- a protocol change to the pinned, authoritative
-- vrr-core dependency; per AGENTS, any such change is staged locally only,
-- never committed or pushed, backed by a corresponding upstream GitHub
-- issue, and reported to the coordinator.
-- This module only reads/writes the deployment descriptor file; it does not
-- feed `config.members()` or `server.tl`.

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
