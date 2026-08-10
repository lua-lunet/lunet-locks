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

{"id":0,"name":"n1","host":"127.0.0.1","port":7001}
{"id":7,"name":"n2","host":"127.0.0.1","port":7002}
]]
        local nodes = cluster_config.parse_jsonl(text)
        local roundtrip = cluster_config.parse_jsonl(cluster_config.encode_jsonl(nodes))
        tested.assert(
            {
                given = "a sparse, comment-and-blank-line-tolerant JSONL descriptor",
                should = "parse both nodes with their admin-chosen ids and round-trip losslessly",
                expected = "0:n1:7001|7:n2:7002",
                actual = tostring(roundtrip[1].id)
                    .. ":"
                    .. roundtrip[1].name
                    .. ":"
                    .. tostring(roundtrip[1].port)
                    .. "|"
                    .. tostring(roundtrip[2].id)
                    .. ":"
                    .. roundtrip[2].name
                    .. ":"
                    .. tostring(roundtrip[2].port),
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
                    '{"id":0,"name":"n1","host":"127.0.0.1","port":7001}\n'
                        .. '{"id":0,"name":"n2","host":"127.0.0.1","port":7002}\n'
                )
                return true
            end
        )
        local duplicate_name = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":0,"name":"n1","host":"127.0.0.1","port":7001}\n'
                        .. '{"id":1,"name":"n1","host":"127.0.0.1","port":7002}\n'
                )
                return true
            end
        )
        local bad_host = fails(
            function(): boolean
                cluster_config.parse_jsonl(
                    '{"id":0,"name":"n1","host":"localhost","port":7001}\n'
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

return tested
