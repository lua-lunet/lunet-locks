local cluster_config = require("cluster_config")
local config = require("config")
local membership_guard = require("membership_guard")
local tested = require("tested")
local transport = require("transport")

local function fails(f: function(): boolean): boolean
    local ok = pcall(f)
    return not ok
end

local function nodes(): {cluster_config.ClusterNode}
    return cluster_config.parse_jsonl(
        '{"id":11,"name":"n1","host":"127.0.0.1","port":7001}\n'
            .. '{"id":22,"name":"n2","host":"127.0.0.1","port":7002}\n'
            .. '{"id":33,"name":"n3","host":"127.0.0.1","port":7003}\n'
    )
end

tested.test(
    "config derives id-bearing members from the deployment descriptor",
    function()
        local members = config.members("n2", nodes())
        tested.assert(
            {
                given = "a parsed three-entry descriptor containing --node once",
                should = "return members in descriptor order with their admin ids and endpoints",
                expected = "11:n1:127.0.0.1:7001|22:n2:127.0.0.1:7002|33:n3:127.0.0.1:7003",
                actual = table.concat(
                    {
                        tostring(members[1].id)
                            .. ":"
                            .. members[1].name
                            .. ":"
                            .. members[1].endpoint.host
                            .. ":"
                            .. tostring(members[1].endpoint.port),
                        tostring(members[2].id)
                            .. ":"
                            .. members[2].name
                            .. ":"
                            .. members[2].endpoint.host
                            .. ":"
                            .. tostring(members[2].endpoint.port),
                        tostring(members[3].id)
                            .. ":"
                            .. members[3].name
                            .. ":"
                            .. members[3].endpoint.host
                            .. ":"
                            .. tostring(members[3].endpoint.port),
                    },
                    "|"
                ),
            }
        )
    end
)

tested.test(
    "config keeps descriptor order, rejects duplicate names and absent locals",
    function()
        local sparse = cluster_config.parse_jsonl(
            '{"id":303,"name":"n3","host":"127.0.0.1","port":7003}\n'
                .. '{"id":11,"name":"n1","host":"127.0.0.1","port":7001}\n'
                .. '{"id":22,"name":"n2","host":"127.0.0.1","port":7002}\n'
        )
        local members = config.members("n1", sparse)
        local duplicate_names = fails(
            function(): boolean
                config.members(
                    "n1",
                    {
                        {id = 11, name = "n1", host = "127.0.0.1", port = 7001},
                        {id = 22, name = "n1", host = "127.0.0.1", port = 7002},
                        {id = 33, name = "n3", host = "127.0.0.1", port = 7003},
                    } as {cluster_config.ClusterNode}
                )
                return true
            end
        )
        local absent = fails(
            function(): boolean config.members("n4", nodes()); return true end
        )
        tested.assert(
            {
                given = "a non-lexically-ordered sparse descriptor and invalid membership shapes",
                should = "preserve line order as the genesis succession and reject the rest before sockets",
                expected = "303:11:22:true:true",
                actual = tostring(members[1].id)
                    .. ":"
                    .. tostring(members[2].id)
                    .. ":"
                    .. tostring(members[3].id)
                    .. ":"
                    .. tostring(duplicate_names)
                    .. ":"
                    .. tostring(absent),
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
    "membership fingerprint has one canonical ordered SHA-256 representation",
    function()
        local members = config.members("n1", nodes())
        local changed = config.members(
            "n1",
            cluster_config.parse_jsonl(
                '{"id":11,"name":"n1","host":"127.0.0.1","port":7001}\n'
                    .. '{"id":22,"name":"n2","host":"127.0.0.1","port":7002}\n'
                    .. '{"id":33,"name":"n3","host":"127.0.0.1","port":7004}\n'
            )
        )
        tested.assert(
            {
                given = "the documented domain-separated, length-delimited ordered (id, name, endpoint) membership bytes",
                should = "produce a stable SHA-256 short fingerprint and change with membership",
                expected = "792aaa6463f0cb42:true",
                actual = config.membership_fingerprint(members)
                    .. ":"
                    .. tostring(
                        config.membership_fingerprint(
                            members
                        ) ~= config.membership_fingerprint(changed)
                    ),
            }
        )
    end
)

tested.test(
    "transport envelopes every peer payload with fingerprint and authenticates sources",
    function()
        local fingerprint = "792aaa6463f0cb42"
        local envelope = transport.encode_peer(
            transport.PEER_VRR, fingerprint, "opaque vrr datagram"
        )
        local application = transport.encode_peer(
            transport.PEER_APPLICATION, fingerprint, "\1opaque application payload"
        )
        local valid, kind, received, payload = transport.decode_peer(envelope)
        local application_valid, application_kind, application_fingerprint, application_payload = transport.decode_peer(
            application
        )
        local invalid = transport.decode_peer("vrr datagram")
        tested.assert(
            {
                given = "opaque VRR and application peer traffic",
                should = "require a typed fingerprint envelope before source-authenticated delivery",
                expected = "true:\1:792aaa6463f0cb42:opaque vrr datagram:true:\2:792aaa6463f0cb42:\1opaque application payload:false:true:false",
                actual = tostring(valid)
                    .. ":"
                    .. kind
                    .. ":"
                    .. received
                    .. ":"
                    .. payload
                    .. ":"
                    .. tostring(application_valid)
                    .. ":"
                    .. application_kind
                    .. ":"
                    .. application_fingerprint
                    .. ":"
                    .. application_payload
                    .. ":"
                    .. tostring(invalid)
                    .. ":"
                    .. tostring(transport.source_is_member("127.0.0.1", 7002, nodes()))
                    .. ":"
                    .. tostring(
                        transport.source_is_member("127.0.0.1", 7999, nodes())
                    ),
            }
        )
    end
)

tested.test(
    "transport round-trips a correlated era+view redirect without a leader id",
    function()
        local message_id = string.rep("x", transport.MESSAGE_ID_BYTES)
        local redirect = transport.encode_not_leader(message_id, 7, 4294967295)
        local valid, received_id, era, view = transport.decode_not_leader(redirect)
        tested.assert(
            {
                given = "an era+view redirect inside the fingerprinted application envelope",
                should = "preserve canonical correlation and omit redundant leader identity",
                expected = "true:xxx:7:4294967295:25",
                actual = tostring(valid)
                    .. ":"
                    .. received_id:sub(1, 3)
                    .. ":"
                    .. tostring(era)
                    .. ":"
                    .. tostring(view)
                    .. ":"
                    .. tostring(#redirect),
            }
        )
    end
)

tested.test(
    "transport accepts only a redirect from its current forwarding destination",
    function()
        local valid = transport.valid_not_leader(22, 22)
        local wrong_source = transport.valid_not_leader(22, 11)
        local not_forwarded = transport.valid_not_leader(-1, 22)
        tested.assert(
            {
                given = "redirects from expected and unexpected member ids",
                should = "accept only source-correlated redirects; the leader id comes from the local view map",
                expected = "true:false:false",
                actual = tostring(valid)
                    .. ":"
                    .. tostring(wrong_source)
                    .. ":"
                    .. tostring(not_forwarded),
            }
        )
    end
)

tested.test(
    "a mismatched peer fingerprint permanently quarantines client operations",
    function()
        local guard = membership_guard.new("792aaa6463f0cb42")
        local matching = guard:accept_fingerprint("792aaa6463f0cb42")
        local mismatched = guard:accept_fingerprint("deadbeefdeadbeef")
        local after = guard:accept_fingerprint("792aaa6463f0cb42")
        tested.assert(
            {
                given = "a matching redirect followed by a valid-source different membership",
                should = "allow the first, then become dirty and reject every client operation",
                expected = "true:false:false:false",
                actual = tostring(matching)
                    .. ":"
                    .. tostring(mismatched)
                    .. ":"
                    .. tostring(after)
                    .. ":"
                    .. tostring(guard:allows_client_operation()),
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
