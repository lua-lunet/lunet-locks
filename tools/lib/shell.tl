-- Typed subprocess shells: the one place that runs shell commands, so
-- the exit-code normalization happens once and every caller gets a
-- typed result. The command runs through libc system() via the LuaJIT
-- FFI: LuaJIT's own os.execute returns a number in plain code but a
-- 5.2-style boolean under the Teal loader, which loses the child's exit
-- code — system() keeps it (the raw wait status: exit code << 8, or the
-- signal in the low byte).

local bit = require("bit")
local ffi = require("ffi")

ffi.cdef(
    [[
   int system(const char *command);
]]
)

local record LibC
    system: function(string): integer
end

local C = ffi.C as LibC

local record M
    status: function(string): integer
    run: function(string): boolean
    capture: function(string): string
    quote: function(string): string
end

function M.status(cmd: string): integer
    local n = C.system(cmd)
    if bit.band(n, 0x7f) == 0 then
        return bit.rshift(n, 8)
    end
    -- Killed by a signal: the shell convention maps it to 128+sig.
    return 128 + bit.band(n, 0x7f)
end

function M.run(cmd: string): boolean
    return M.status(cmd) == 0
end

function M.capture(cmd: string): string
    local pipe = io.popen(cmd)
    if not pipe then
        return ""
    end
    local out = pipe:read("*a") or ""
    pipe:close()
    return out
end

-- Single-quote a word for shell use: the only safe quoting for embedded
-- anything.
function M.quote(s: string): string
    return "'" .. s:gsub("'", "'\\''") .. "'"
end

return M
