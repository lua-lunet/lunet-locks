local tested = require("tested")
local transport = require("transport")

local LOW = 19857409
local BUMPED = LOW + 1
local ENDPOINT = "127.0.0.1:27103"

local function tables(): ({integer: string}, {string: integer})
    local endpoint_by_id: {integer: string} = {
        [6619137] = "127.0.0.1:27101",
        [13238273] = "127.0.0.1:27102",
        [LOW] = ENDPOINT,
    }
    local id_by_endpoint: {string: integer} = {
        ["127.0.0.1:27101"] = 6619137,
        ["127.0.0.1:27102"] = 13238273,
        [ENDPOINT] = LOW,
    }
    return endpoint_by_id, id_by_endpoint
end

tested.test(
    "the reincarnation remap adds the next life's row and keeps the old row",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the reincarnation remap is a one-time-per-pair entry ticket",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the reincarnation remap refuses pairs that are not a lawful next life",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the reincarnation remap keeps the old row until the identity is evicted",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

return tested
