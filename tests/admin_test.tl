local tested = require("tested")

tested.test(
    "the refused acknowledgment answers a floor-crossing verb with the voting-floor reason",
    function()
        local admin = require("admin")
        tested.assert(
            {
                given = "a leave the leader refused at admission",
                should = "answer with the exact refused shape: the host's own refusal, nothing entered the log",
                expected = '{"action":"leave","id":13238273,"accepted":false,"reason":"voting-floor"}',
                actual = admin.encode_refused_ack("leave", 13238273),
            }
        )
        tested.assert(
            {
                given = "a decrement the leader refused at admission",
                should = "answer with the same shape",
                expected = '{"action":"decrement","id":13238273,"accepted":false,"reason":"voting-floor"}',
                actual = admin.encode_refused_ack("decrement", 13238273),
            }
        )
        tested.assert(
            {
                given = "an unknown action",
                should = "raise: the encoder is strict, the same law as the deadline acknowledgment",
                expected = "false",
                actual = tostring(
                    pcall(function() admin.encode_refused_ack("destroy", 1) end)
                ),
            }
        )
    end
)

return tested
