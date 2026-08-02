local tested = require("tested")

tested.test("no assertions means invalid", function()
   local x = 1
   x = x + 1
end)

return tested
