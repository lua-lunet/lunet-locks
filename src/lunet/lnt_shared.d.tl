-- Type declarations for lunet.lnt_shared Lua FFI wrapper.
-- Covers the subset of the Dict API used by the scaffold.

local record lnt_shared
   record Dict
      incr: function(Dict, string, number, number, number): number
      get: function(Dict, string): number
   end

   open: function(string, integer): Dict
end

return lnt_shared
