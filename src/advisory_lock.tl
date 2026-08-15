local ffi = require("ffi")

-- The uVRR v0.2.0 rebuild removes the previous alpha C ABI. The native
-- library is being rebuilt around the new core; the cdefs and wrappers return
-- with it. This module keeps only the library-loading skeleton so the rest of
-- the tree stays honest about where the native boundary lives.

local record DebugInfo
    source: string
end

local function load_native(): any
    local path = os.getenv("LUNET_ADVISORY_LOCK_LIB")
    if path == nil or path == "" then
        local suffix = ffi.os == "OSX" and "dylib" or "so"
        local source = (debug.getinfo(1, "S") as DebugInfo).source
        local dir = source:match("^@(.+)/[^/]+$") or "."
        path = dir
            .. "/../ext/advisory_lock/target/release/liblunet_advisory_lock."
            .. suffix
    end
    return ffi.load(path)
end

local native = load_native()

local record advisory_lock
end

local M: advisory_lock = {}

return M
