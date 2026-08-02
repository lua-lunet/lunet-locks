local function no_annotation()
   return "x"
end

local function wants_string(x: string): string
   return x
end

local s = wants_string(no_annotation())
print(s)
