local advisory_lock = require("advisory_lock")
local tested = require("tested")

local members = {"n1", "n2", "n3"}
local state_path = ".tmp/advisory-lock-ffi-test.nonce"

tested.test(
    "advisory lock wrapper loads, reports status, drains peer bytes, and closes twice",
    function()
        os.remove(state_path)
        local node = advisory_lock.new(members, "n1", state_path)
        local status = node:status()
        local mapped_leader = node:leader_for_epoch(status.epoch)
        local outputs = node:request(
            '{"op":"get","message_id":"00000000-0000-0000-0000-000000000001","client_id":1,"request_num":1,"lock_id":9001}'
        )
        local output = outputs[1]
        local closed_once = node:close()
        local closed_twice = node:close()
        os.remove(state_path)
        tested.assert(
            {
                given = "a fresh three-member node and a byte-counted GET envelope",
                should = "surface native status, opaque peer bytes, and idempotent close",
                expected = "normal:0:0:true:broadcast:true:true",
                actual = status.state
                    .. ":"
                    .. tostring(status.leader)
                    .. ":"
                    .. tostring(status.epoch)
                    .. ":"
                    .. tostring(mapped_leader == status.leader)
                    .. ":"
                    .. output.kind
                    .. ":"
                    .. tostring(closed_once)
                    .. ":"
                    .. tostring(closed_twice),
            }
        )
        tested.assert(
            {
                given = "a native peer output",
                should = "retain a 16-byte canonical message id and non-empty opaque bytes",
                expected = true,
                actual = #output.message_id == 16 and #output.bytes > 0,
            }
        )
    end
)

tested.test(
    "advisory lock wrapper maps malformed client envelopes to a useful native error",
    function()
        local path = ".tmp/advisory-lock-ffi-error-test.nonce"
        os.remove(path)
        local node = advisory_lock.new(members, "n1", path)
        local ok = pcall(function() node:request("{not json}") end)
        node:close()
        os.remove(path)
        tested.assert(
            {
                given = "malformed client JSON passed to the native adapter",
                should = "surface its mapped native rejection to the caller",
                expected = false,
                actual = ok,
            }
        )
    end
)

return tested
