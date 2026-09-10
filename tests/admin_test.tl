local admin = require("admin")
local tested = require("tested")

local UUID = "018f3f2a-7b1c-4c50-9f2a-3f4b5c6d7e8f"

tested.test(
    "admin decodes the four membership verbs strictly",
    function()
        local join_valid, join = admin.decode_verb(
            '{"action":"join","message_id":"'
                .. UUID
                .. '","id":400,"name":"n4","endpoint":"127.0.0.1:7004"}'
        )
        local increment_valid, increment = admin.decode_verb(
            '{"action":"increment","message_id":"' .. UUID .. '","id":400}'
        )
        local decrement_valid, decrement = admin.decode_verb(
            '{"action":"decrement","message_id":"' .. UUID .. '","id":400}'
        )
        local leave_valid, leave = admin.decode_verb(
            '{"action":"leave","message_id":"' .. UUID .. '","id":400}'
        )
        tested.assert(
            {
                given = "one well-formed verb per membership change",
                should = "decode each with its typed fields",
                expected = "join:400:n4:127.0.0.1:7004|increment:400|decrement:400|leave:400",
                actual = join.action
                    .. ":"
                    .. tostring(join.id)
                    .. ":"
                    .. join.name
                    .. ":"
                    .. join.endpoint
                    .. "|"
                    .. increment.action
                    .. ":"
                    .. tostring(increment.id)
                    .. "|"
                    .. decrement.action
                    .. ":"
                    .. tostring(decrement.id)
                    .. "|"
                    .. leave.action
                    .. ":"
                    .. tostring(leave.id),
            }
        )
        assert(join_valid and increment_valid and decrement_valid and leave_valid)
    end
)

tested.test(
    "admin refuses unknown actions, malformed ids, bad message ids and bad endpoints",
    function()
        local checks: {boolean} = {}
        local function refuses(json: string): boolean
            local valid = admin.decode_verb(json)
            return valid == false
        end
        table.insert(
            checks,
            refuses('{"action":"resize","message_id":"' .. UUID .. '","id":400}')
        )
        table.insert(
            checks, refuses('{"action":"join","message_id":"' .. UUID .. '","id":-1}')
        )
        table.insert(
            checks,
            refuses('{"action":"join","message_id":"' .. UUID .. '","id":4294967295}')
        )
        table.insert(checks, refuses('{"action":"join","message_id":"nope","id":400}'))
        table.insert(
            checks,
            refuses('{"action":"decrement","message_id":"' .. UUID .. '","id":x}')
        )
        table.insert(
            checks,
            refuses(
                '{"action":"join","message_id":"'
                    .. UUID
                    .. '","id":400,"name":"n4","endpoint":"localhost:7004"}'
            )
        )
        table.insert(
            checks,
            refuses(
                '{"action":"join","message_id":"'
                    .. UUID
                    .. '","id":400,"name":"n4!","endpoint":"127.0.0.1:7004"}'
            )
        )
        table.insert(
            checks,
            refuses(
                '{"action":"join","message_id":"' .. UUID .. '","id":400,"name":"n4"}'
            )
        )
        local all_refused = true
        for _, refused in ipairs(checks) do
            if not refused then
                all_refused = false
            end
        end
        tested.assert(
            {
                given = "verbs with unknown actions, out-of-domain ids, unparseable message ids, non-literal endpoints, invalid names, and a join missing its endpoint",
                should = "refuse each before any state moves",
                expected = "8:true",
                actual = tostring(#checks) .. ":" .. tostring(all_refused),
            }
        )
    end
)

tested.test(
    "the break op is a lock verb for the adapter, not an admin verb",
    function()
        -- The privileged break rides the lock path: the Teal edge must not
        -- classify it as a membership verb, or it would drive a
        -- reconfiguration instead of submitting the op to the adapter.
        local valid = admin.decode_verb(
            '{"op":"break","message_id":"'
                .. UUID
                .. '","client_id":42,"request_num":9,"lock_id":9001}'
        )
        tested.assert(
            {
                given = "a well-formed break op line",
                should = "decode as a lock verb, never as a membership verb",
                expected = "false",
                actual = tostring(valid),
            }
        )
    end
)

tested.test(
    "admin frames the join and leave broadcasts and decodes them losslessly",
    function()
        local join = admin.encode_join(400, "n4", "127.0.0.1:7004")
        local change_valid, change = admin.decode_change(join)
        local leave = admin.encode_leave(400)
        local leave_valid, leave_change = admin.decode_change(leave)
        tested.assert(
            {
                given = "one ADMIN payload per endpoint-moving membership change",
                should = "decode each with its typed change; the leave carries only the id",
                expected = "true:join:400:n4:127.0.0.1:7004:true:leave:400:",
                actual = tostring(change_valid)
                    .. ":"
                    .. change.kind
                    .. ":"
                    .. tostring(change.id)
                    .. ":"
                    .. change.name
                    .. ":"
                    .. change.endpoint
                    .. ":"
                    .. tostring(leave_valid)
                    .. ":"
                    .. leave_change.kind
                    .. ":"
                    .. tostring(leave_change.id)
                    .. ":"
                    .. leave_change.endpoint,
            }
        )
    end
)

tested.test(
    "admin drops malformed broadcast payloads and foreign application payloads",
    function()
        local truncated = admin.decode_change(admin.ADMIN_DIRECTION .. "J400")
        local unknown_kind = admin.decode_change(
            admin.ADMIN_DIRECTION .. "X\x00\x00\x00\x00"
        )
        local forward = admin.decode_change("\1opaque forward payload")
        local empty = admin.decode_change("")
        tested.assert(
            {
                given = "a truncated join, an unknown kind, a foreign application payload, and an empty one",
                should = "refuse each without partial application",
                expected = "false:false:false:false",
                actual = tostring((truncated))
                    .. ":"
                    .. tostring((unknown_kind))
                    .. ":"
                    .. tostring((forward))
                    .. ":"
                    .. tostring((empty)),
            }
        )
    end
)

tested.test(
    "admin builds the operator acknowledgment with its acceptance fact",
    function()
        local accepted = admin.encode_ack("join", 400)
        local accepted_decrement = admin.encode_ack("decrement", 400)
        local rejected_json = '{"action":"leave","id":400,"accepted":false}'
        tested.assert(
            {
                given = "an accepted join, an accepted decrement and the documented rejected-verb shape",
                should = "encode the ack with the action, the id and the acceptance fact",
                expected = 'true:{"action":"join","id":400,"accepted":true}:{"action":"decrement","id":400,"accepted":true}',
                actual = tostring(admin.encode_ack("join", 400) == accepted)
                    .. ":"
                    .. accepted
                    .. ":"
                    .. accepted_decrement,
            }
        )
        assert(#rejected_json > 0)
    end
)

tested.test(
    "admin encodes the honest deadline acknowledgment with its reason",
    function()
        tested.assert(
            {
                given = "a verb whose establishing commit did not land within the client deadline",
                should = "refuse it with a reason, never accept uncommitted state",
                expected = '{"action":"decrement","id":400,"accepted":false,"reason":"deadline"}',
                actual = admin.encode_deadline_ack("decrement", 400),
            }
        )
    end
)

tested.test(
    "the dedup cache drives a verb once and replays the first outcome for duplicates",
    function()
        local cache = admin.new_cache(8)
        local accepted_ack = '{"action":"decrement","id":400,"accepted":true}'
        local states = cache:seen(UUID) .. ":" .. tostring(cache:claim(UUID))
        local duplicate_while_in_flight = cache:seen(UUID)
        cache:settle(UUID, accepted_ack)
        local settled = cache:seen(UUID)
        local replayed, ack = cache:replay(UUID)
        -- A late settle (a duplicate drive's refusal) never overwrites the
        -- first outcome.
        cache:settle(UUID, '{"action":"decrement","id":400,"accepted":false}')
        local replayed_again, ack_again = cache:replay(UUID)
        tested.assert(
            {
                given = "one drive of a verb and two duplicates of its message_id arriving around it",
                should = "claim exactly once, report in_flight before the outcome, and replay the FIRST ack after it",
                expected = "new:true:in_flight:settled:true:"
                    .. accepted_ack
                    .. ":true:"
                    .. accepted_ack,
                actual = states
                    .. ":"
                    .. duplicate_while_in_flight
                    .. ":"
                    .. settled
                    .. ":"
                    .. tostring(replayed)
                    .. ":"
                    .. ack
                    .. ":"
                    .. tostring(replayed_again)
                    .. ":"
                    .. ack_again,
            }
        )
    end
)

tested.test(
    "a re-driven committed verb returns the ORIGINAL ack, not a fresh refusal",
    function()
        local cache = admin.new_cache(8)
        local original = '{"action":"increment","id":404,"accepted":true}'
        cache:claim(UUID)
        cache:settle(UUID, original)
        -- The re-drive reaches the cache settled: it replays the original
        -- ack instead of driving into the core's refusal.
        local state = cache:seen(UUID)
        local replayed, ack = cache:replay(UUID)
        tested.assert(
            {
                given = "a committed verb whose message_id is submitted again",
                should = "answer with the original accepted ack and never re-drive",
                expected = "settled:true:" .. original,
                actual = state .. ":" .. tostring(replayed) .. ":" .. ack,
            }
        )
    end
)

tested.test(
    "a released claim is drivable again and a settled outcome survives release",
    function()
        local cache = admin.new_cache(8)
        cache:claim(UUID)
        cache:release(UUID)
        local released_state = cache:seen(UUID)
        local reclaimed = cache:claim(UUID)
        cache:settle(UUID, '{"action":"join","id":404,"accepted":true}')
        cache:release(UUID)
        local replayed, ack = cache:replay(UUID)
        tested.assert(
            {
                given = "a NotPrimary drive (claim released) and then a settled outcome on the reclaimed claim",
                should = "allow the fresh drive after release and keep the settled ack after a settled release",
                expected = "new:true:true:{\"action\":\"join\",\"id\":404,\"accepted\":true}",
                actual = released_state
                    .. ":"
                    .. tostring(reclaimed)
                    .. ":"
                    .. tostring(replayed)
                    .. ":"
                    .. ack,
            }
        )
    end
)

tested.test(
    "the dedup cache evicts settled entries at its bound and never an in-flight drive",
    function()
        local cache = admin.new_cache(2)
        local first = "018f3f2a-7b1c-4c50-9f2a-3f4b5c6d7e80"
        local second = "018f3f2a-7b1c-4c50-9f2a-3f4b5c6d7e81"
        local third = "018f3f2a-7b1c-4c50-9f2a-3f4b5c6d7e82"
        local fourth = "018f3f2a-7b1c-4c50-9f2a-3f4b5c6d7e83"
        cache:claim(first)
        cache:settle(first, '{"action":"join","id":1,"accepted":true}')
        cache:claim(second)
        cache:claim(third)
        -- third's claim evicts first (oldest settled); second is in flight.
        local first_evicted = cache:seen(first)
        local second_kept = cache:seen(second)
        cache:claim(fourth)
        -- fourth's claim would exceed the bound, but the oldest entry
        -- (second) is in flight and is never evicted.
        local fourth_kept = cache:seen(fourth)
        local second_still_kept = cache:seen(second)
        tested.assert(
            {
                given = "a cache at its bound with an in-flight drive at the eviction head",
                should = "evict the oldest settled entry first and keep every in-flight drive",
                expected = "new:in_flight:in_flight:in_flight",
                actual = first_evicted
                    .. ":"
                    .. second_kept
                    .. ":"
                    .. fourth_kept
                    .. ":"
                    .. second_still_kept,
            }
        )
    end
)

tested.test(
    "the forwarder rotation advances in member order and wraps",
    function()
        local members = {100, 200, 300}
        local first = admin.next_in_rotation(members, -1)
        local second = admin.next_in_rotation(members, first)
        local third = admin.next_in_rotation(members, second)
        local wrapped = admin.next_in_rotation(members, third)
        local past_gap = admin.next_in_rotation(members, 150)
        local empty = admin.next_in_rotation({}, -1)
        tested.assert(
            {
                given = "the live member list in ascending order and one id per advance",
                should = "start at the smallest member, advance in order, wrap, skip past a gap, and yield -1 on an empty list",
                expected = "100:200:300:100:200:-1",
                actual = tostring(first)
                    .. ":"
                    .. tostring(second)
                    .. ":"
                    .. tostring(third)
                    .. ":"
                    .. tostring(wrapped)
                    .. ":"
                    .. tostring(past_gap)
                    .. ":"
                    .. tostring(empty),
            }
        )
    end
)

return tested
