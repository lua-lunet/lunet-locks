-- tools/bootstrap.lua — the shared prelude for every Teal tooling entry.
-- Resolves the project-local .rocks tree (and the LuaRocks 5.1 paths for
-- C rocks like luasocket) without a pre-sourced LUA_PATH, activates the
-- Teal loader, and makes the repo's typed libraries requirable from any
-- cwd. Everything resolves from arg[0], never the cwd, so every entry
-- works under `env -u LUA_PATH -u LUA_CPATH` from any directory.

local entry_dir = arg[0]:match("^(.*)/[^/]*$") or "."

local function exists(path)
    local f = io.open(path, "rb")
    if f then
        f:close()
        return true
    end
    return false
end

local root = entry_dir
while root ~= "/" and not exists(root .. "/Makefile") do
    root = root:match("^(.*)/[^/]*$") or "/"
end

if not pcall(require, "tl") then
    local function lr(which)
        local pipe = io.popen(
            "luarocks --lua-version=5.1 path --lr-" .. which .. " 2>/dev/null"
        )
        local out = pipe and pipe:read("*l") or nil
        if pipe then
            pipe:close()
        end
        return out
    end
    package.path = root .. "/.rocks/share/lua/5.1/?.lua;" .. package.path
    local p, c = lr("path"), lr("cpath")
    if p and #p > 0 then
        package.path = p .. ";" .. package.path
    end
    if c and #c > 0 then
        package.cpath = c .. ";" .. package.cpath
    end
end

local ok, tl = pcall(require, "tl")
if not ok then
    io.stderr:write(
        "ERROR: Teal (tl) not installed for the LuaJIT 5.1 ABI. Run: make init\n"
    )
    os.exit(1)
end
tl.loader()

package.path = root
    .. "/?.lua;"
    .. root
    .. "/?.tl;"
    .. root
    .. "/tools/?.tl;"
    .. root
    .. "/tools/lib/?.tl;"
    .. package.path

return root
