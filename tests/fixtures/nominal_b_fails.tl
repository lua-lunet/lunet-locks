local A = require("nominal_a")

local record B
   record Token
      e: integer
   end
end

local function wants_b(t: B.Token): B.Token
   return t
end

local x = wants_b(A.make())
print(x)
