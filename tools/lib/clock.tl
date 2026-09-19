-- Typed clocks: epoch and monotonic milliseconds, plus sleep. The log
-- lines carry epoch-millisecond ts= stamps, so every gate deadline and
-- latency math shares this one clock. LibC through the LuaJIT FFI: no
-- subprocess, no socket dependency.

local bit = require("bit")
local ffi = require("ffi")

ffi.cdef(
    [[
   struct timespec { long tv_sec; long tv_nsec; };
   int clock_gettime(int clk_id, struct timespec *tp);
   unsigned int sleep(unsigned int seconds);
]]
)

local record Timespec
    tv_sec: integer
    tv_nsec: integer
end

local record LibC
    clock_gettime: function(integer, any): integer
    sleep: function(integer): integer
end

local C = ffi.C as LibC

local ts = ffi.new("struct timespec[1]") as {Timespec}

-- CLOCK_REALTIME is 0 on both Linux and macOS.
local CLOCK_REALTIME = 0
-- CLOCK_MONOTONIC is 1 on Linux and 6 on macOS.
local CLOCK_MONO = (ffi.os == "OSX") and 6 or 1

local record M
    now_ms: function(): integer
    mono_ms: function(): integer
    sleep: function(number)
end

function M.now_ms(): integer
    C.clock_gettime(CLOCK_REALTIME, ts)
    return (
        tonumber(ts[0].tv_sec) * 1000 + math.floor(tonumber(ts[0].tv_nsec) / 1000000)
    ) as integer
end

function M.mono_ms(): integer
    C.clock_gettime(CLOCK_MONO, ts)
    return (
        tonumber(ts[0].tv_sec) * 1000 + math.floor(tonumber(ts[0].tv_nsec) / 1000000)
    ) as integer
end

function M.sleep(seconds: number)
    if seconds >= 1 then
        C.sleep(math.floor(seconds))
        seconds = seconds - math.floor(seconds)
    end
    if seconds > 0 then
        -- A busy sleep for the sub-second residue: the gates poll at
        -- 100-500 ms and a nanosecond-level wait is not worth another
        -- dependency.
        local deadline = M.mono_ms() + bit.tobit(seconds * 1000)
        while M.mono_ms() < deadline do
            -- busy spin
        end
    end
end

return M
