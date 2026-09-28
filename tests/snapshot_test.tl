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
        member(6619137, 1, "127.0.0.1:27101"),
        member(13238273, 1, "127.0.0.1:27102"),
        member(19857409, 1, "127.0.0.1:27103"),
    }
end

tested.test(
    "the snapshot request is one tag byte and decodes exactly",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the snapshot response encodes era, slot and members and round-trips",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the snapshot response decode refuses every malformed shape",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "era plus slot make the up-to-dateness comparison total",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "snapshots agree only on identical era, slot and member set",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the quorum is the weighted majority and weight-0 learners never satisfy it",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a snapshot envelope rides the new peer kind and a foreign fingerprint drops",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the descriptor builds the boot membership model over the genesis lines",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the committed admin verbs move the membership model exactly",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the membership sidecar round-trips and refuses a malformed series",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

return tested
