local tested = require("tested")

-- The one-outstanding law (docs/src/client-topology.md): a connection holds
-- one outstanding command's uuid, and a second command bearing a DIFFERENT
-- uuid is refused BY NAME rather than queued behind it. The refusal is a
-- safety property of the uuid-to-socket nexus, so the decision is a pure
-- function of the connection's outstanding uuid and the arriving command's.
tested.test(
    "one outstanding command per connection, and the second is refused by name",
    function()
        local transport = require("transport")
        local first = "01010101-0101-0101-0101-010101010101"
        local second = "02020202-0202-0202-0202-020202020202"

        tested.assert(
            {
                given = "a connection holding nothing",
                should = "admit the arriving command: nothing is outstanding to refuse",
                expected = "true",
                actual = tostring((transport.one_outstanding("", second))),
            }
        )

        tested.assert(
            {
                given = "a connection whose outstanding uuid IS the arriving uuid",
                should = "admit it: a retry of the command already in flight is not a second outstanding request",
                expected = "true",
                actual = tostring((transport.one_outstanding(second, second))),
            }
        )

        local admitted, refusal = transport.one_outstanding(first, second)
        tested.assert(
            {
                given = "a connection holding one uuid and a DIFFERENT uuid arriving",
                should = "refuse it, and refuse it BY NAME",
                expected = "false|request_outstanding",
                actual = table.concat({tostring(admitted), refusal}, "|"),
            }
        )

        tested.assert(
            {
                given = "the refusal name itself",
                should = "be the documented constant, spelled one way in the tree",
                expected = "request_outstanding",
                actual = transport.CLIENT_REQUEST_OUTSTANDING,
            }
        )
    end
)

tested.test(
    "the refusal answers on the wire in the protocol's own JSON shape",
    function()
        local transport = require("transport")
        tested.assert(
            {
                given = "a refused command's uuid",
                should = "answer with the documented refusal line naming that uuid",
                expected = '{"error":"request_outstanding","message_id":"02020202-0202-0202-0202-020202020202"}',
                actual = transport.encode_outstanding_refusal(
                    "02020202-0202-0202-0202-020202020202"
                ),
            }
        )
    end
)

return tested
