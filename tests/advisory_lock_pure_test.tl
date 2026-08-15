local config = require("config")
local membership_guard = require("membership_guard")
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
    "membership fingerprint has one canonical ordered SHA-256 representation",
    function()
        local members = config.members(
            "n1",
            {
                "n1=127.0.0.1:7001",
                "n2=127.0.0.1:7002",
                "n3=127.0.0.1:7003",
            }
        )
        local changed = config.members(
            "n1",
            {
                "n1=127.0.0.1:7001",
                "n2=127.0.0.1:7002",
                "n3=127.0.0.1:7004",
            }
        )
        tested.assert(
            {
                given = "the documented domain-separated, length-delimited membership bytes",
                should = "produce a stable SHA-256 short fingerprint and change with membership",
                expected = "fcd4b13820df1fc3:true",
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
        local fingerprint = "fcd4b13820df1fc3"
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
        local members = {
            "n1=127.0.0.1:7001",
            "n2=127.0.0.1:7002",
            "n3=127.0.0.1:7003",
        }
        tested.assert(
            {
                given = "opaque VRR and application peer traffic",
                should = "require a typed fingerprint envelope before source-authenticated delivery",
                expected = "true:\1:fcd4b13820df1fc3:opaque vrr datagram:true:\2:fcd4b13820df1fc3:\1opaque application payload:false:true:false",
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
    "transport round-trips a correlated view redirect without a leader index",
    function()
        local message_id = string.rep("x", transport.MESSAGE_ID_BYTES)
        local redirect = transport.encode_not_leader(message_id, 4294967295)
        local valid, received_id, view = transport.decode_not_leader(redirect)
        tested.assert(
            {
                given = "a view redirect inside the fingerprinted application envelope",
                should = "preserve canonical correlation and omit redundant leader identity",
                expected = "true:xxx:4294967295:21",
                actual = tostring(valid)
                    .. ":"
                    .. received_id:sub(1, 3)
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
        local valid = transport.valid_not_leader(1, 1)
        local wrong_source = transport.valid_not_leader(1, 0)
        local not_forwarded = transport.valid_not_leader(-1, 1)
        tested.assert(
            {
                given = "redirects from expected and unexpected UDP peers",
                should = "accept only source-correlated redirects; leader comes from the local view map",
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
        local guard = membership_guard.new("fcd4b13820df1fc3")
        local matching = guard:accept_fingerprint("fcd4b13820df1fc3")
        local mismatched = guard:accept_fingerprint("deadbeefdeadbeef")
        local after = guard:accept_fingerprint("fcd4b13820df1fc3")
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
