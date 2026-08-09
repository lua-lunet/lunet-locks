local config = require("config")
local tested = require("tested")
local transport = require("transport")

local function fails(f: function(): boolean): boolean
    local ok = pcall(f)
    return not ok
end

tested.test(
    "config accepts a sorted three-node literal IPv4 membership",
    function()
        local members = config.members(
            "n2",
            {
                "n1=127.0.0.1:7001",
                "n2=127.0.0.1:7002",
                "n3=127.0.0.1:7003",
            }
        )
        tested.assert(
            {
                given = "a sorted three-node membership containing --node once",
                should = "return parsed endpoints",
                expected = "127.0.0.1:7002",
                actual = members[2].endpoint.host
                    .. ":"
                    .. tostring(members[2].endpoint.port),
            }
        )
    end
)

tested.test(
    "config rejects unsorted, duplicate, non-literal, and missing local members",
    function()
        local unsorted = fails(
            function(): boolean
                config.members(
                    "n1",
                    {"n2=127.0.0.1:7002", "n1=127.0.0.1:7001", "n3=127.0.0.1:7003"}
                )
                return true
            end
        )
        local duplicate = fails(
            function(): boolean
                config.members(
                    "n1",
                    {"n1=127.0.0.1:7001", "n1=127.0.0.1:7002", "n3=127.0.0.1:7003"}
                )
                return true
            end
        )
        local hostname = fails(
            function(): boolean
                config.members(
                    "n1",
                    {"n1=localhost:7001", "n2=127.0.0.1:7002", "n3=127.0.0.1:7003"}
                )
                return true
            end
        )
        local absent = fails(
            function(): boolean
                config.members(
                    "n4",
                    {"n1=127.0.0.1:7001", "n2=127.0.0.1:7002", "n3=127.0.0.1:7003"}
                )
                return true
            end
        )
        tested.assert(
            {
                given = "invalid membership shapes",
                should = "reject each before opening sockets",
                expected = true,
                actual = unsorted and duplicate and hostname and absent,
            }
        )
    end
)

tested.test(
    "transport frames partial and multiple NDJSON requests without data loss",
    function()
        local first, remainder = transport.feed_ndjson("", "{\"op\":\"get\"}", 64)
        local second, tail = transport.feed_ndjson(
            remainder, "\n{\"op\":\"release\"}\n", 64
        )
        tested.assert(
            {
                given = "one partial request followed by two newline boundaries",
                should = "retain the partial bytes then return both complete requests",
                expected = "|{\"op\":\"get\"}|{\"op\":\"release\"}|",
                actual = table.concat(first, "|")
                    .. "|"
                    .. table.concat(second, "|")
                    .. "|"
                    .. tail,
            }
        )
    end
)

tested.test(
    "transport reserves forwarding magic and authenticates configured UDP sources",
    function()
        local forwarded = transport.encode_forward("opaque request")
        local is_forwarded, payload = transport.decode_forward(forwarded)
        local ordinary, ordinary_payload = transport.decode_forward("vrr datagram")
        local members = {
            "n1=127.0.0.1:7001",
            "n2=127.0.0.1:7002",
            "n3=127.0.0.1:7003",
        }
        tested.assert(
            {
                given = "forwarded and ordinary UDP payloads",
                should = "distinguish the reserved prefix and accept only member endpoints",
                expected = "true:opaque request:false:vrr datagram:true:false",
                actual = tostring(is_forwarded)
                    .. ":"
                    .. payload
                    .. ":"
                    .. tostring(ordinary)
                    .. ":"
                    .. ordinary_payload
                    .. ":"
                    .. tostring(transport.source_is_member("127.0.0.1", 7002, members))
                    .. ":"
                    .. tostring(
                        transport.source_is_member("127.0.0.1", 7999, members)
                    ),
            }
        )
    end
)

tested.test(
    "transport rejects oversized complete and incomplete NDJSON requests",
    function()
        local oversized_complete = fails(
            function(): boolean transport.feed_ndjson("", "12345\n", 4); return true end
        )
        local oversized_partial = fails(
            function(): boolean transport.feed_ndjson("", "12345", 4); return true end
        )
        tested.assert(
            {
                given = "messages above the configured UDP-safe limit",
                should = "reject them before forwarding",
                expected = true,
                actual = oversized_complete and oversized_partial,
            }
        )
    end
)

return tested
