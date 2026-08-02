local tested = require("tested")

local record Header
   tag: integer
   epoch: integer
   slot_hi: integer
   slot_lo: integer
end

local record Output
   kind: string
   replica: integer
   header: Header
   bytes: string
end

local record Node
   request: function(Node, string, string): {Output}
   receive: function(Node, integer, string): {Output}
   idle: function(Node): {Output}
   leader_timeout: function(Node): {Output}
   close: function(Node): boolean
end

local record Vrr
   new: function({string}, string): Node
end

local vrr = dofile("./build/vrr.lua") as Vrr
local members: {string} = { "a:1", "b:1", "c:1" }
local four_members: {string} = { "a:1", "b:1", "c:1", "d:1" }

local request = [[{"op":"set","message_id":"01010101-0101-0101-0101-010101010101","client_id":1,"request_num":1,"lock_id":7,"lease":{"lease_id":9,"holder":"02020202-0202-0202-0202-020202020202","expiry":500}}]]

tested.test("vrr ffi rejects invalid membership", function()
   local ok = pcall(function()
      vrr.new({ "b:1", "a:1", "c:1" }, "a:1")
   end)
   tested.assert({
      given = "an unsorted membership",
      should = "reject construction at the Rust boundary",
      expected = false,
      actual = ok,
   })
end)

tested.test("vrr ffi accepts an even four-node membership", function()
   local leader = vrr.new(four_members, "a:1")
   leader:close()
   tested.assert({
      given = "a sorted four-node fixed membership",
      should = "construct with strict-majority quorum support",
      expected = true,
      actual = true,
   })
end)

tested.test("vrr ffi rejects client bytes before mutating the log", function()
   local leader = vrr.new(members, "a:1")
   local malformed_ok = pcall(function()
      leader:request("100", "not json")
   end)
   local oversized = request:sub(1, -2)
      .. ',"padding":"' .. string.rep("x", 25000) .. '"}'
   local oversized_ok = pcall(function()
      leader:request("100", oversized)
   end)
   local prepared = leader:request("100", request)
   leader:close()
   tested.assert({
      given = "malformed and over-datagram client bytes",
      should = "reject both before the next valid request takes slot one",
      expected = true,
      actual = not malformed_ok
         and not oversized_ok
         and #prepared == 1
         and prepared[1].header.slot_lo == 1,
   })
end)

tested.test("vrr ffi completes normal operation and resends duplicate reply", function()
   local leader = vrr.new(members, "a:1")
   local backup = vrr.new(members, "b:1")

   local prepared = leader:request("100", request)
   local acked = backup:receive(0, prepared[1].bytes)
   local replied = leader:receive(1, acked[1].bytes)
   local duplicate = leader:request("100", request)

   tested.assert({
      given = "a SET accepted by a three-node epoch-zero leader",
      should = "cross PREPARE/PREPARE_OK and return the correlated JSON reply",
      expected = true,
      actual = #prepared == 1
         and prepared[1].kind == "broadcast"
         and prepared[1].header.tag == 0x10
         and prepared[1].header.epoch == 0
         and prepared[1].header.slot_hi == 0
         and prepared[1].header.slot_lo == 1
         and #acked == 1
         and acked[1].kind == "send"
         and acked[1].replica == 0
         and acked[1].header.tag == 0x11
         and #replied == 1
         and replied[1].kind == "reply"
         and replied[1].bytes:find("01010101%-0101%-0101%-0101%-010101010101") ~= nil
         and replied[1].bytes:find('"granted":true', 1, true) ~= nil
         and #duplicate == 1
         and duplicate[1].kind == "reply"
         and duplicate[1].bytes == replied[1].bytes,
   })

   leader:close()
   backup:close()
end)

tested.test("vrr ffi reconstructs and replays the exact result after epoch change", function()
   local old_leader = vrr.new(members, "a:1")
   local new_leader = vrr.new(members, "b:1")

   local prepared = old_leader:request("100", request)
   local acked = new_leader:receive(0, prepared[1].bytes)
   local original = old_leader:receive(1, acked[1].bytes)

   local old_change = old_leader:leader_timeout()
   local new_change = new_leader:leader_timeout()
   local report = old_leader:receive(1, new_change[1].bytes)
   new_leader:receive(0, old_change[1].bytes)
   local installed = new_leader:receive(0, report[1].bytes)

   local reconstructed = ""
   for _, output in ipairs(installed) do
      if output.kind == "reply" then
         reconstructed = output.bytes
      end
   end
   local duplicate = new_leader:request("100", request)

   old_leader:close()
   new_leader:close()
   tested.assert({
      given = "a committed request selected by the next epoch leader",
      should = "rebuild and replay the exact serialized result without re-admission",
      expected = true,
      actual = reconstructed == original[1].bytes
         and #duplicate == 1
         and duplicate[1].kind == "reply"
         and duplicate[1].bytes == reconstructed,
   })
end)

tested.test("vrr ffi uses explicit byte lengths", function()
   local leader = vrr.new(members, "a:1")
   local backup = vrr.new(members, "b:1")
   local prepared = leader:request("100", request)
   local corrupted = prepared[1].bytes .. "\0ignored"
   local ok = pcall(function()
      backup:receive(0, corrupted)
   end)
   leader:close()
   backup:close()
   tested.assert({
      given = "a valid message followed by an embedded NUL and trailing bytes",
      should = "pass the full byte length to Rust and reject it",
      expected = false,
      actual = ok,
   })
end)

return tested
