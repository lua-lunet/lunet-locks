-- Typed log-gate machinery. The dated-log bug class dies here: a gate
-- never names a log file, never globs one — it reads the logs DIRECTORY
-- and selects every entry that carries the node-name prefix, so
-- `n1.log`, `n1.stderr`, and a rolled `n1.2026-09-18.log` are all in the
-- evidence set by construction.

local fs = require("tools.lib.fs")

local record M
    node_files: function(string, string): {string}
    node_logs: function(string, string): {string}
    files_for_all: function(string, {string}): {string}
    grep: function({string}, string): {string}
    count: function({string}, string): integer
    kv: function(string): {string: string}
    fields: function(string): {string}
end

-- Every entry under logs_dir whose name carries the node-name prefix:
-- `name`, `name.<anything>`, `name.log`, `name.2026-09-18.log`. A node
-- named `n1` must not swallow `n10`: the prefix boundary is the dot.
function M.node_files(logs_dir: string, name: string): {string}
    local entries, _err = fs.list_dir(logs_dir)
    local out: {string} = {}
    for _, entry in ipairs(entries) do
        if entry == name or entry:sub(1, #name + 1) == name .. "." then
            out[#out + 1] = fs.join(logs_dir, entry)
        end
    end
    return out
end

-- The node's actual log files (the appender output), as opposed to its
-- stderr capture: entries matching `name*.log*` — any extension that
-- starts with `log` covers `.log`, `.log.gz`, `.log.1`, and the dated
-- `n1.<date>.log` appender files.
function M.node_logs(logs_dir: string, name: string): {string}
    local entries, _err = fs.list_dir(logs_dir)
    local out: {string} = {}
    local prefix = name .. "."
    for _, entry in ipairs(entries) do
        local is_log = entry:match("%.log")
        if entry:sub(1, #prefix) == prefix and is_log then
            out[#out + 1] = fs.join(logs_dir, entry)
        end
    end
    return out
end

-- The whole evidence set for several nodes, deduplicated, sorted.
function M.files_for_all(logs_dir: string, names: {string}): {string}
    local seen: {string: boolean} = {}
    local out: {string} = {}
    for _, name in ipairs(names) do
        for _, path in ipairs(M.node_files(logs_dir, name)) do
            if not seen[path] then
                seen[path] = true
                out[#out + 1] = path
            end
        end
    end
    table.sort(out)
    return out
end

-- All lines from all files whose content matches the Lua pattern.
function M.grep(files: {string}, pattern: string): {string}
    local out: {string} = {}
    for _, path in ipairs(files) do
        local data = fs.read_all(path)
        for line in (data .. "\n"):gmatch("(.-)\n") do
            if line:match(pattern) then
                out[#out + 1] = line
            end
        end
    end
    return out
end

-- Count of matching lines across the file set.
function M.count(files: {string}, pattern: string): integer
    local n = 0
    for _, path in ipairs(files) do
        local data = fs.read_all(path)
        for line in (data .. "\n"):gmatch("(.-)\n") do
            if line:match(pattern) then
                n = n + 1
            end
        end
    end
    return n
end

-- Whitespace-separated fields of a line.
function M.fields(line: string): {string}
    local out: {string} = {}
    for field in line:gmatch("%S+") do
        out[#out + 1] = field
    end
    return out
end

-- The `key=value` tokens of a line as a map: the log line's structured
-- content, typed at the read site rather than parsed by awk.
function M.kv(line: string): {string: string}
    local out: {string: string} = {}
    for field in line:gmatch("%S+") do
        local k, v = field:match("^([^=]+)=(.*)$")
        if k and not out[k] then
            out[k] = v
        end
    end
    return out
end

return M
