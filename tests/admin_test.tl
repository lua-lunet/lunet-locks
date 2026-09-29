local tested = require("tested")

tested.test(
    "admin decodes the four membership verbs strictly",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "admin refuses unknown actions, malformed ids, bad message ids and bad endpoints",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the break op is a lock verb for the adapter, not an admin verb",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "admin frames the join and leave broadcasts and decodes them losslessly",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "admin drops malformed broadcast payloads and foreign application payloads",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "admin builds the operator acknowledgment with its acceptance fact",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "admin encodes the honest deadline acknowledgment with its reason",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the dedup cache drives a verb once and replays the first outcome for duplicates",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a re-driven committed verb returns the ORIGINAL ack, not a fresh refusal",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a released claim is drivable again and a settled outcome survives release",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the dedup cache evicts settled entries at its bound and never an in-flight drive",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the forwarder rotation advances in member order and wraps",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

return tested
