local cluster_config = require("cluster_config")
local tested = require("tested")

local function fails(f: function(): boolean): boolean
    local ok = pcall(f)
    return not ok
end

tested.test(
    "cluster_config parses sparse admin-assigned ids and round-trips to JSONL",
    function()
        local text = [[
# comment lines and blank lines are ignored

{"id":0,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}
{"id":7,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}
]]
        local nodes = cluster_config.parse_jsonl(text)
        local roundtrip = cluster_config.parse_jsonl(cluster_config.encode_jsonl(nodes))
        tested.assert(
            {
                given = "a sparse, comment-and-blank-line-tolerant JSONL descriptor",
                should = "parse both nodes with their admin-chosen ids and round-trip losslessly",
                expected = "0:n1:7001:true|7:n2:7002:true",
                actual = tostring(roundtrip[1].id)
                    .. ":"
                    .. roundtrip[1].name
                    .. ":"
                    .. tostring(roundtrip[1].port)
                    .. ":"
                    .. tostring(roundtrip[1].genesis)
                    .. "|"
                    .. tostring(roundtrip[2].id)
                    .. ":"
                    .. roundtrip[2].name
                    .. ":"
                    .. tostring(roundtrip[2].port)
                    .. ":"
                    .. tostring(roundtrip[2].genesis),
            }
        )
    end
)

tested.test(
    "cluster_config preserves line order: the genesis lines are the explicit succession",
    function()
        local nodes = cluster_config.parse_jsonl(
            '{"id":300,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                .. '{"id":100,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}\n'
                .. '{"id":200,"name":"n3","host":"127.0.0.1","port":7003,"genesis":true}\n'
        )
        tested.assert(
            {
                given = "a descriptor whose line order differs from id and name order",
                should = "keep line order verbatim: the core folds it as the genesis member order (primary(v) = order[v mod N])",
                expected = "300|100|200",
                actual = tostring(nodes[1].id)
                    .. "|"
                    .. tostring(nodes[2].id)
                    .. "|"
                    .. tostring(nodes[3].id),
            }
        )
    end
)

tested.test(
    "cluster_config rejects duplicate ids, duplicate names, and malformed entries",
    function()
        local duplicate_id = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":0,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                        .. '{"id":0,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}\n'
                )
                return true
            end
        )
        local duplicate_name = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":0,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                        .. '{"id":1,"name":"n1","host":"127.0.0.1","port":7002,"genesis":true}\n'
                )
                return true
            end
        )
        local bad_host = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":0,"name":"n1","host":"localhost","port":7001,"genesis":true}\n'
                )
                return true
            end
        )
        local empty = fails(
            function(): boolean cluster_config.parse_jsonl(""); return true end
        )
        tested.assert(
            {
                given = "descriptors with duplicate ids, duplicate names, non-literal hosts, or no nodes",
                should = "reject each",
                expected = true,
                actual = duplicate_id and duplicate_name and bad_host and empty,
            }
        )
    end
)

tested.test(
    "cluster_config ids are incarnation-0 ids in the low band the bump arithmetic reserves around",
    function()
        local reserved_derivation = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":16777215,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                )
                return true
            end
        )
        local high_band = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":16777216,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                )
                return true
            end
        )
        local u32_overflow = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":4294967295,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                )
                return true
            end
        )
        local negative = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":-1,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                )
                return true
            end
        )
        local max_allowed = cluster_config.parse_jsonl(
            '{"id":16777214,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
        )
        local zero_allowed = cluster_config.parse_jsonl(
            '{"id":0,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
        )
        tested.assert(
            {
                given = "ids outside the incarnation-0 low band: the reserved-derivation id,"
                    .. " the first high-band id, the reserved LEADER_UNKNOWN value, and a negative",
                should = "reject each and admit both band edges",
                expected = "true:true:true:true:16777214:0",
                actual = tostring(reserved_derivation)
                    .. ":"
                    .. tostring(high_band)
                    .. ":"
                    .. tostring(u32_overflow)
                    .. ":"
                    .. tostring(negative)
                    .. ":"
                    .. tostring(max_allowed[1].id)
                    .. ":"
                    .. tostring(zero_allowed[1].id),
            }
        )
    end
)

tested.test(
    "cluster_config requires an explicit genesis flag on every line",
    function()
        local missing = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":0,"name":"n1","host":"127.0.0.1","port":7001}\n'
                )
                return true
            end
        )
        local bad_value = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":0,"name":"n1","host":"127.0.0.1","port":7001,"genesis":"yes"}\n'
                )
                return true
            end
        )
        tested.assert(
            {
                given = "a line without the genesis field, and a line with a non-boolean genesis",
                should = "reject each: the founding membership is part of the deployment identity",
                expected = "true:true",
                actual = tostring(missing) .. ":" .. tostring(bad_value),
            }
        )
    end
)

tested.test(
    "genesis_members returns the founding membership in line order and requires a quorum of them",
    function()
        local nodes = cluster_config.parse_jsonl(
            '{"id":300,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                .. '{"id":100,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}\n'
                .. '{"id":200,"name":"n3","host":"127.0.0.1","port":7003,"genesis":true}\n'
                .. '{"id":400,"name":"n4","host":"127.0.0.1","port":7004,"genesis":false}\n'
        )
        local genesis = cluster_config.genesis_members(nodes)
        tested.assert(
            {
                given = "a descriptor with three genesis members and one appended joiner",
                should = "select the genesis lines in order and refuse a deployment without three of them",
                expected = "300|100|200:true",
                actual = tostring(genesis[1].id)
                    .. "|"
                    .. tostring(genesis[2].id)
                    .. "|"
                    .. tostring(genesis[3].id)
                    .. ":"
                    .. tostring(
                        fails(
                            function(): boolean
                                cluster_config.genesis_members(
                                    cluster_config.parse_jsonl(
                                        '{"id":300,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                                            .. '{"id":100,"name":"n2","host":"127.0.0.1","port":7002,"genesis":false}\n'
                                    )
                                )
                                return true
                            end
                        )
                    ),
            }
        )
    end
)

tested.test(
    "the descriptor superset relation holds for appended joining members and fails otherwise",
    function()
        local incumbents = cluster_config.parse_jsonl(
            '{"id":300,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                .. '{"id":100,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}\n'
                .. '{"id":200,"name":"n3","host":"127.0.0.1","port":7003,"genesis":true}\n'
        )
        local grown = cluster_config.parse_jsonl(
            '{"id":300,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                .. '{"id":100,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}\n'
                .. '{"id":200,"name":"n3","host":"127.0.0.1","port":7003,"genesis":true}\n'
                .. '{"id":400,"name":"n4","host":"127.0.0.1","port":7004,"genesis":false}\n'
        )
        local reordered = cluster_config.parse_jsonl(
            '{"id":100,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}\n'
                .. '{"id":300,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
                .. '{"id":200,"name":"n3","host":"127.0.0.1","port":7003,"genesis":true}\n'
                .. '{"id":400,"name":"n4","host":"127.0.0.1","port":7004,"genesis":false}\n'
        )
        tested.assert(
            {
                given = "a descriptor grown by an appended joiner, and one whose shared lines moved",
                should = "accept the appended superset and reject the reordered one: shared lines never move or change",
                expected = "true:false",
                actual = tostring(cluster_config.superset_of(grown, incumbents))
                    .. ":"
                    .. tostring(cluster_config.superset_of(reordered, incumbents)),
            }
        )
    end
)

return tested
