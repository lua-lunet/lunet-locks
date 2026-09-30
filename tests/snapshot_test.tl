local tested = require("tested")

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

tested.test(
    "the voting floor refuses a demotion or a departure that would leave fewer than three voters",
    function()
        local snapshot = require("snapshot")
        local Member = snapshot.SnapshotMember
        local Model = snapshot.Snapshot
        local function voter(id: integer, weight: integer): Member
            return {
                id = id,
                weight = weight,
                endpoint = "127.0.0.1:" .. tostring(7000 + id),
            }
        end
        local three: {Member} = {voter(1, 1), voter(2, 1), voter(3, 1)}
        local four: {Member} = {voter(1, 1), voter(2, 1), voter(3, 1), voter(4, 1)}
        local with_learner: {Member} = {
            voter(1, 1),
            voter(2, 1),
            voter(3, 1),
            voter(9, 0),
        }
        local function outcome(members: {Member}, action: string, id: integer): string
            local changed, updated = snapshot.apply_change(members, action, id, "")
            local model: Model = {era = 1, slot = 0, members = updated}
            return tostring(changed)
                .. "/"
                .. tostring(snapshot.voting_members(model.members))
        end
        tested.assert(
            {
                given = "three voters, a decrement and a leave of one of them",
                should = "refuse both and touch nothing: neither may cross the floor",
                expected = "false/3:false/3",
                actual = outcome(three, "decrement", 2)
                    .. ":"
                    .. outcome(three, "leave", 2),
            }
        )
        tested.assert(
            {
                given = "four voters, the same two departures",
                should = "accept both and contract to three, which stands above the floor",
                expected = "true/3:true/3",
                actual = outcome(four, "decrement", 4)
                    .. ":"
                    .. outcome(four, "leave", 4),
            }
        )
        tested.assert(
            {
                given = "three voters and one weight-0 learner, a leave of the learner",
                should = "accept and still count three: a learner is not a voter",
                expected = "true/3",
                actual = outcome(with_learner, "leave", 9),
            }
        )
        tested.assert(
            {
                given = "the voting counts against the declared minimum",
                should = "count positive weights only",
                expected = "3:3:3",
                actual = tostring(snapshot.voting_members(three))
                    .. ":"
                    .. tostring(snapshot.voting_members(with_learner))
                    .. ":"
                    .. tostring(snapshot.VOTING_MINIMUM),
            }
        )
    end
)

return tested
