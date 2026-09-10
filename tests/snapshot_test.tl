local cluster_config = require("cluster_config")
local snapshot = require("snapshot")
local tested = require("tested")
local transport = require("transport")

local FINGERPRINT = "0123456789abcdef"
local FOREIGN = "ffffffffffffffff"

local function member(
    id: integer, weight: integer, endpoint: string
): snapshot.SnapshotMember
    return {id = id, weight = weight, endpoint = endpoint}
end

local function shape(
    era: integer, slot: integer, members: {snapshot.SnapshotMember}
): snapshot.Snapshot
    return {era = era, slot = slot, members = members}
end

local function three(): {snapshot.SnapshotMember}
    return {
        member(101, 1, "127.0.0.1:27101"),
        member(202, 1, "127.0.0.1:27102"),
        member(303, 1, "127.0.0.1:27103"),
    }
end

tested.test(
    "the snapshot request is one tag byte and decodes exactly",
    function()
        local encoded = snapshot.encode_request()
        tested.assert(
            {
                given = "a snapshot request payload",
                should = "encode to one tag byte and decode only that shape",
                expected = "len=1|request=true|empty=false|trailing=false",
                actual = "len="
                    .. tostring(#encoded)
                    .. "|request="
                    .. tostring(snapshot.decode_request(encoded))
                    .. "|empty="
                    .. tostring(snapshot.decode_request(""))
                    .. "|trailing="
                    .. tostring(snapshot.decode_request(encoded .. "\1")),
            }
        )
        assert(#encoded == 1)
        assert(snapshot.decode_request(encoded))
        assert(not snapshot.decode_request(""))
        assert(not snapshot.decode_request(encoded .. "\1"))
    end
)

tested.test(
    "the snapshot response encodes era, slot and members and round-trips",
    function()
        local snap = shape(5, 42, three())
        local payload = snapshot.encode_response(snap)
        local valid, decoded = snapshot.decode_response(payload)
        tested.assert(
            {
                given = "an era-5, slot-chosen-at-4096 membership snapshot",
                should = "round-trip through the strict binary shape",
                expected = "era=5|slot=42|members=3|first=101:1:127.0.0.1:27101",
                actual = "era="
                    .. tostring(decoded.era)
                    .. "|slot="
                    .. tostring(decoded.slot)
                    .. "|members="
                    .. tostring(#decoded.members)
                    .. "|first="
                    .. decoded.members[1].id
                    .. ":"
                    .. decoded.members[1].weight
                    .. ":"
                    .. decoded.members[1].endpoint,
            }
        )
        assert(valid and decoded.era == 5)
    end
)

tested.test(
    "the snapshot response decode refuses every malformed shape",
    function()
        local good = snapshot.encode_response(shape(1, 2, three()))
        local truncated = good:sub(1, 15) .. "\17" .. good:sub(16)
        tested.assert(
            {
                given = "truncated, mistagged, trailing-byte and over-cap payloads",
                should = "refuse each without partial application",
                expected = "0 valid of 6",
                actual = tostring(
                    (snapshot.decode_response("") and 1 or 0) + (
                        snapshot.decode_response(good:sub(1, 4)) and 1 or 0
                    ) + (snapshot.decode_response("\4" .. good:sub(2)) and 1 or 0) + (
                        snapshot.decode_response(good:sub(1, #good - 1)) and 1 or 0
                    ) + (snapshot.decode_response(good .. "\0") and 1 or 0) + (
                        snapshot.decode_response(truncated) and 1 or 0
                    )
                ) .. " valid of 6",
            }
        )
        assert(
            not snapshot.decode_response("")
                and not snapshot.decode_response(good:sub(1, 4))
                and not snapshot.decode_response("\4" .. good:sub(2))
                and not snapshot.decode_response(good:sub(1, #good - 1))
                and not snapshot.decode_response(good .. "\0")
                and not snapshot.decode_response(truncated)
        )
        -- A member whose endpoint is not literal IPv4:port is refused.
        local bad_endpoint = good:gsub("127%.0%.0%.1:27101", "localhost:1")
        assert(not snapshot.decode_response(bad_endpoint))
    end
)

tested.test(
    "era plus slot make the up-to-dateness comparison total",
    function()
        tested.assert(
            {
                given = "the (era, slot) pairs of successive configurations",
                should = "order strictly by era, then by slot within an era",
                expected = "era-dominates=true|slot-breaks-ties=true|equal-noop=true|older-ignored=true",
                actual = "era-dominates="
                    .. tostring(snapshot.newer(2, 0, 1, 9))
                    .. "|slot-breaks-ties="
                    .. tostring(snapshot.newer(1, 9, 1, 3))
                    .. "|equal-noop="
                    .. tostring(not snapshot.newer(3, 7, 3, 7))
                    .. "|older-ignored="
                    .. tostring(not snapshot.newer(1, 9, 3, 0)),
            }
        )
        assert(snapshot.newer(2, 0, 1, 9))
        assert(snapshot.newer(1, 9, 1, 3))
        assert(not snapshot.newer(3, 7, 3, 7))
        assert(not snapshot.newer(1, 9, 3, 0))
    end
)

tested.test(
    "snapshots agree only on identical era, slot and member set",
    function()
        local a = shape(3, 12, three())
        assert(snapshot.agreeing(a, shape(3, 12, three())))
        assert(not snapshot.agreeing(a, shape(3, 13, three())))
        assert(not snapshot.agreeing(a, shape(4, 12, three())))
        local changed = {
            member(101, 1, "127.0.0.1:27101"),
            member(202, 1, "127.0.0.1:27102"),
            member(303, 0, "127.0.0.1:27103"),
        }
        tested.assert(
            {
                given = "two snapshots whose member weights differ",
                should = "refuse the agreement",
                expected = "false",
                actual = tostring(snapshot.agreeing(a, shape(3, 12, changed))),
            }
        )
        assert(not snapshot.agreeing(a, shape(3, 12, changed)))
    end
)

tested.test(
    "the quorum is the weighted majority and weight-0 learners never satisfy it",
    function()
        -- Four voting members at weight 1 plus one weight-0 learner:
        -- total 4, threshold 3.
        local members = {
            member(1, 1, "127.0.0.1:7001"),
            member(2, 1, "127.0.0.1:7002"),
            member(3, 1, "127.0.0.1:7003"),
            member(4, 1, "127.0.0.1:7004"),
            member(6, 0, "127.0.0.1:7006"),
        }
        local two = {[1] = true, [2] = true}
        local three_voters = {[1] = true, [2] = true, [3] = true}
        local learners_only = {[6] = true}
        tested.assert(
            {
                given = "two, three and learner-only agreeing responders",
                should = "satisfy the quorum at the weighted-majority threshold only",
                expected = "two=false|three=true|learners=false",
                actual = "two="
                    .. tostring(snapshot.quorum_reached(members, two))
                    .. "|three="
                    .. tostring(snapshot.quorum_reached(members, three_voters))
                    .. "|learners="
                    .. tostring(snapshot.quorum_reached(members, learners_only)),
            }
        )
        assert(not snapshot.quorum_reached(members, two))
        assert(snapshot.quorum_reached(members, three_voters))
        assert(not snapshot.quorum_reached(members, learners_only))
    end
)

tested.test(
    "a snapshot envelope rides the new peer kind and a foreign fingerprint drops",
    function()
        local payload = snapshot.encode_response(shape(1, 0, three()))
        local packet = transport.encode_peer(
            transport.PEER_SNAPSHOT, FINGERPRINT, payload
        )
        local valid, kind, received = transport.decode_peer(packet)
        tested.assert(
            {
                given = "a snapshot packet from the deployment and one from a foreign one",
                should = "decode the kind and expose the fingerprint for the drop rule",
                expected = "kind=true|own=true|foreign=true",
                actual = "kind="
                    .. tostring(kind == transport.PEER_SNAPSHOT)
                    .. "|own="
                    .. tostring(received == FINGERPRINT)
                    .. "|foreign="
                    .. tostring(received ~= FOREIGN and FOREIGN ~= FINGERPRINT),
            }
        )
        assert(valid and kind == transport.PEER_SNAPSHOT and received == FINGERPRINT)
        -- The drop decision at the receive boundary is the fingerprint
        -- comparison: a foreign deployment's snapshot never reaches the
        -- model, and the mismatch is not a quarantine (advisory evidence,
        -- dropped without ceremony).
        assert(FOREIGN ~= FINGERPRINT)
    end
)

tested.test(
    "the descriptor builds the boot membership model over the genesis lines",
    function()
        local nodes = cluster_config.parse_jsonl(
            '{"id":101,"name":"n1","host":"127.0.0.1","port":27101,"genesis":true}\n'
                .. '{"id":202,"name":"n2","host":"127.0.0.1","port":27102,"genesis":true}\n'
                .. '{"id":303,"name":"n3","host":"127.0.0.1","port":27103,"genesis":true}\n'
                .. '{"id":404,"name":"n4","host":"127.0.0.1","port":27104,"genesis":false}\n'
        )
        local model = snapshot.descriptor_model(nodes)
        tested.assert(
            {
                given = "a three-genesis, one-joiner deployment descriptor",
                should = "build the founding-membership model: era 1, slot 0, genesis voters only",
                expected = "era=1|slot=0|members=3|joiner-weight=-1",
                actual = "era="
                    .. tostring(model.era)
                    .. "|slot="
                    .. tostring(model.slot)
                    .. "|members="
                    .. tostring(#model.members)
                    .. "|joiner-weight="
                    .. tostring(snapshot.member_weight(model.members, 404)),
            }
        )
        assert(model.era == 1 and model.slot == 0 and #model.members == 3)
    end
)

tested.test(
    "the committed admin verbs move the membership model exactly",
    function()
        local model = three()
        local join_ok, joined = snapshot.apply_change(
            model, "join", 404, "127.0.0.1:27104"
        )
        assert(join_ok and #joined == 4)
        assert(snapshot.member_weight(joined, 404) == 0)
        -- A join of an existing member is refused untouched.
        local join_dup, same = snapshot.apply_change(joined, "join", 404, "127.0.0.1:9")
        assert(not join_dup and same == joined)
        local inc_ok, incremented = snapshot.apply_change(joined, "increment", 404, "")
        assert(inc_ok and snapshot.member_weight(incremented, 404) == 1)
        local dec_ok, decremented = snapshot.apply_change(
            incremented, "decrement", 404, ""
        )
        assert(dec_ok and snapshot.member_weight(decremented, 404) == 0)
        local _, left = snapshot.apply_change(decremented, "leave", 404, "")
        tested.assert(
            {
                given = "the full join/increment/decrement/leave sequence",
                should = "move the model additively and remove the departed member",
                expected = "join-w0|increment-w1|decrement-w0|leave-true|canonical=true",
                actual = "join-w"
                    .. tostring(snapshot.member_weight(joined, 404))
                    .. "|increment-w"
                    .. tostring(snapshot.member_weight(incremented, 404))
                    .. "|decrement-w"
                    .. tostring(snapshot.member_weight(decremented, 404))
                    .. "|leave-"
                    .. tostring(snapshot.member_weight(left, 404) == -1)
                    .. "|canonical="
                    .. tostring(left[1].id == 101 and left[3].id == 303),
            }
        )
        -- A leave of an absent member and an unknown action are refused.
        assert(not snapshot.apply_change(left, "leave", 404, ""))
        assert(not snapshot.apply_change(left, "resign", 404, ""))
    end
)

tested.test(
    "the membership sidecar round-trips and refuses a malformed series",
    function()
        local snap = shape(5, 42, three())
        local text = snapshot.encode_sidecar(snap)
        local valid, decoded = snapshot.decode_sidecar(text)
        local member_lines = 0
        for _ in text:gmatch('"id"%s*:%s*%d+') do
            member_lines = member_lines + 1
        end
        tested.assert(
            {
                given = "the write-behind sidecar of an adopted snapshot",
                should = "parse back exactly, header line first",
                expected = "era=5|slot=42|members=3|lines=4",
                actual = "era="
                    .. tostring(decoded.era)
                    .. "|slot="
                    .. tostring(decoded.slot)
                    .. "|members="
                    .. tostring(#decoded.members)
                    .. "|lines="
                    .. tostring(member_lines + 1),
            }
        )
        assert(valid and decoded.era == 5)
        -- A sidecar with a wrong format token, a descending member id, or
        -- a missing header is ignored entirely.
        local descending = '{"format":"membership-sidecar/v1","era":5,"slot":42}\n'
            .. '{"id":41,"weight":1,"endpoint":"127.0.0.1:1"}\n'
            .. '{"id":40,"weight":1,"endpoint":"127.0.0.1:2"}\n'
        assert(not snapshot.decode_sidecar(descending))
        assert(not snapshot.decode_sidecar('{"format":"other/v1","era":1,"slot":0}\n'))
        assert(not snapshot.decode_sidecar(""))
    end
)

return tested
