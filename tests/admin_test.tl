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

return tested
