local tested = require("tested")
local transport = require("transport")

local LOW = 303
local BUMPED = 303 + 16777216
local ENDPOINT = "127.0.0.1:27103"

local function tables(): ({integer: string}, {string: integer})
    local endpoint_by_id: {integer: string} = {
        [101] = "127.0.0.1:27101",
        [202] = "127.0.0.1:27102",
        [303] = ENDPOINT,
    }
    local id_by_endpoint: {string: integer} = {
        ["127.0.0.1:27101"] = 101,
        ["127.0.0.1:27102"] = 202,
        [ENDPOINT] = 303,
    }
    return endpoint_by_id, id_by_endpoint
end

tested.test(
    "the reincarnation remap adds the bumped id's row and keeps the old row",
    function()
        local endpoint_by_id, id_by_endpoint = tables()
        local applied = transport.apply_reincarnation_remap(
            endpoint_by_id, id_by_endpoint, ENDPOINT, LOW, BUMPED
        )
        tested.assert(
            {
                given = "a Reincarnation(old, new) pair from the old identity's socket",
                should = "add the bumped id's row at the source endpoint and keep the old row",
                expected = "applied=true|both-rows=true",
                actual = "applied="
                    .. tostring(applied)
                    .. "|both-rows="
                    .. tostring(
                        endpoint_by_id[LOW] ~= nil and endpoint_by_id[BUMPED] ~= nil
                    ),
            }
        )
        assert(applied)
        -- Both rows resolve while the old identity is still named by the
        -- serving configuration: sends to either id reach the restarted
        -- process's socket, and the pre-eviction era's traffic is never
        -- unaddressable.
        assert(endpoint_by_id[LOW] == ENDPOINT)
        assert(endpoint_by_id[BUMPED] == ENDPOINT)
        -- Datagrams from the source socket are attributed to the new
        -- identity from here on.
        assert(id_by_endpoint[ENDPOINT] == BUMPED)
    end
)

tested.test(
    "the reincarnation remap is a one-time-per-pair entry ticket",
    function()
        local endpoint_by_id, id_by_endpoint = tables()
        assert(
            transport.apply_reincarnation_remap(
                endpoint_by_id, id_by_endpoint, ENDPOINT, LOW, BUMPED
            )
        )
        -- A repeated announcement of the same pair adds nothing.
        tested.assert(
            {
                given = "the same Reincarnation pair announced again",
                should = "refuse as a no-op",
                expected = "false",
                actual = tostring(
                    transport.apply_reincarnation_remap(
                        endpoint_by_id, id_by_endpoint, ENDPOINT, LOW, BUMPED
                    )
                ),
            }
        )
        assert(
            not transport.apply_reincarnation_remap(
                endpoint_by_id, id_by_endpoint, ENDPOINT, LOW, BUMPED
            )
        )
        -- The tables are unchanged by the no-op.
        assert(endpoint_by_id[LOW] == ENDPOINT)
        assert(endpoint_by_id[BUMPED] == ENDPOINT)
        assert(id_by_endpoint[ENDPOINT] == BUMPED)
    end
)

tested.test(
    "the reincarnation remap refuses degenerate and low-band pairs",
    function()
        local endpoint_by_id, id_by_endpoint = tables()
        -- A degenerate pair (old == new) is not a bump.
        tested.assert(
            {
                given = "a pair whose members are equal",
                should = "refuse",
                expected = "false",
                actual = tostring(
                    transport.apply_reincarnation_remap(
                        endpoint_by_id, id_by_endpoint, ENDPOINT, LOW, LOW
                    )
                ),
            }
        )
        -- A bumped id outside the high band is not a bump product.
        assert(
            not transport.apply_reincarnation_remap(
                endpoint_by_id, id_by_endpoint, ENDPOINT, LOW, 404
            )
        )
        -- The old row is untouched by every refusal.
        assert(endpoint_by_id[LOW] == ENDPOINT)
        assert(endpoint_by_id[404] == nil)
        assert(id_by_endpoint[ENDPOINT] == 303)
    end
)

tested.test(
    "the reincarnation remap keeps the old row until the identity is evicted",
    function()
        local endpoint_by_id, id_by_endpoint = tables()
        assert(
            transport.apply_reincarnation_remap(
                endpoint_by_id, id_by_endpoint, ENDPOINT, LOW, BUMPED
            )
        )
        -- Simulate the reincarnation forced steps having run their
        -- course: the leave's application broadcast removes the old row
        -- when the old identity is out of the serving configuration —
        -- and not before. Until that point, sends to the old id must
        -- resolve to the restarted process's socket.
        tested.assert(
            {
                given = "the remap applied and the old identity still served",
                should = "resolve sends to the old id at the restarted socket",
                expected = ENDPOINT,
                actual = endpoint_by_id[LOW],
            }
        )
        -- The eviction is the identity's exit: the leave path removes the
        -- old row (the server's leave broadcast does exactly this) while
        -- the new identity's row survives.
        endpoint_by_id[LOW] = nil
        assert(endpoint_by_id[BUMPED] == ENDPOINT)
        assert(endpoint_by_id[LOW] == nil)
    end
)

return tested
