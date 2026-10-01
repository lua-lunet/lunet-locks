-- Typed log-gate machinery. The dated-log bug class dies here: a gate
-- never names a log file, never globs one — it reads the logs DIRECTORY
-- and selects every entry that carries the node-name prefix, so
-- `n1.log`, `n1.stderr`, and a rolled `n1.2026-09-18.log` are all in the
-- evidence set by construction.
--
-- The node's lines are JSON objects, one per line (the observability
-- contract, `docs/src/test-scaffold.md`). `kv` reads an object's own
-- top-level members, so a gate reads `line.event`, `line.ts`,
-- `line.era`, `line.slot` rather than scraping `key=value` tokens off a
-- sentence. A line that is not a JSON object — the Lua host's stdout,
-- which the rigs capture beside the node's own log — falls back to the
-- `key=value` scan, so one reader serves both surfaces.
--
-- The reader is deliberately shallow: it reads a top-level object's
-- members and records a nested value as the empty string. The producer
-- is flat by construction (the tracing JSON formatter with its event
-- fields flattened into the root object, no nested member among them), so
-- a nested value is a shape the node does not emit; skipping it keeps the
-- remaining members readable either way.

local fs = require("tools.lib.fs")

local record M
    node_files: function(string, string): {string}
    node_logs: function(string, string): {string}
    files_for_all: function(string, {string}): {string}
    grep: function({string}, string): {string}
    count: function({string}, string): integer
    count_where: function({string}, string, {string: string}): integer
    event_lines: function({string}, string): {string}
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

-- Every line of every file, in file order.
local function all_lines(files: {string}): {string}
    local out: {string} = {}
    for _, path in ipairs(files) do
        local data = fs.read_all(path)
        for line in (data .. "\n"):gmatch("(.-)\n") do
            out[#out + 1] = line
        end
    end
    return out
end

-- All lines whose text matches the Lua pattern. The whole JSON line is
-- the haystack: a pattern names a member (`"event":"lease-grant"`) or a
-- free-text body, never a parsed value.
function M.grep(files: {string}, pattern: string): {string}
    local out: {string} = {}
    for _, line in ipairs(all_lines(files)) do
        if line:match(pattern) then
            out[#out + 1] = line
        end
    end
    return out
end

-- Count of matching lines across the file set.
function M.count(files: {string}, pattern: string): integer
    return #M.grep(files, pattern)
end

-- Count of lines carrying this `event` whose members also equal these.
-- The field predicate a gate states as data: the line's own name plus
-- the exact member values it is counting.
function M.count_where(
    files: {string},
    event: string,
    members: {string: string}
): integer
    local n = 0
    for _, line in ipairs(M.event_lines(files, event)) do
        local kv = M.kv(line)
        local matched = true
        for key, want in pairs(members) do
            if kv[key] ~= want then
                matched = false
                break
            end
        end
        if matched then
            n = n + 1
        end
    end
    return n
end

-- Every line whose `event` member is this name — the JSON line's own
-- identity, the way a text log's leading word was.
function M.event_lines(files: {string}, event: string): {string}
    local out: {string} = {}
    for _, line in ipairs(all_lines(files)) do
        if M.kv(line).event == event then
            out[#out + 1] = line
        end
    end
    return out
end

-- Whitespace-separated fields of a line.
function M.fields(line: string): {string}
    local out: {string} = {}
    for field in line:gmatch("%S+") do
        out[#out + 1] = field
    end
    return out
end

-- The scanner below, forward-declared: `M.kv` is the entry point the
-- gates call, and it reads through these.
local scan_object: function(string, integer, {string: string}): integer
local read_string: function(string, integer): string, integer
local read_value: function(string, integer): string, integer
local skip_nested: function(string, integer): integer

-- One line's structured content as a map, typed at the read site. A JSON
-- object contributes its own top-level members (numbers and booleans as
-- their JSON text); any other line contributes its `key=value` tokens.
function M.kv(line: string): {string: string}
    local members: {string: string} = {}
    if line:sub(1, 1) == "{" and scan_object(line, 1, members) > 0 then
        return members
    end
    local out: {string: string} = {}
    for field in line:gmatch("%S+") do
        local k, v = field:match("^([^=]+)=(.*)$")
        if k and not out[k] then
            out[k] = v
        end
    end
    return out
end

-- Index of the first non-space character at or after `i`.
local function skip_space(line: string, i: integer): integer
    local n = #line
    while i <= n and line:sub(i, i):match("%s") do
        i = i + 1
    end
    return i
end

-- Reads the JSON object at `i` into `out`; the index just past its
-- closing brace, or 0 when the text there is not an object.
function scan_object(
    line: string,
    i: integer,
    out: {string: string}
): integer
    i = skip_space(line, i)
    if line:sub(i, i) ~= "{" then
        return 0
    end
    i = skip_space(line, i + 1)
    if line:sub(i, i) == "}" then
        return i + 1
    end
    while true do
        if line:sub(i, i) ~= '"' then
            return 0
        end
        local key, after_key = read_string(line, i)
        if after_key == 0 then
            return 0
        end
        i = skip_space(line, after_key)
        if line:sub(i, i) ~= ":" then
            return 0
        end
        i = skip_space(line, i + 1)
        local value, after_value = read_value(line, i)
        if after_value == 0 then
            return 0
        end
        out[key] = value
        i = skip_space(line, after_value)
        local delimiter = line:sub(i, i)
        if delimiter == "," then
            i = i + 1
        elseif delimiter == "}" then
            return i + 1
        else
            return 0
        end
    end
end

-- The string starting at the opening quote, unescaped, and the index
-- just past its closing quote; 0 for the second when there is none.
function read_string(line: string, i: integer): string, integer
    local buffer: {string} = {}
    local n = #line
    local j = i + 1
    while j <= n do
        local c = line:sub(j, j)
        if c == "\\" then
            local escaped = line:sub(j + 1, j + 1)
            if escaped == "n" then
                buffer[#buffer + 1] = "\n"
            elseif escaped == "t" then
                buffer[#buffer + 1] = "\t"
            elseif escaped == "r" then
                buffer[#buffer + 1] = "\r"
            else
                buffer[#buffer + 1] = escaped
            end
            j = j + 2
        elseif c == '"' then
            return table.concat(buffer), j + 1
        else
            buffer[#buffer + 1] = c
            j = j + 1
        end
    end
    return "", 0
end

-- The value starting at `i` as its JSON text, and the index just past
-- it; 0 for the second when it does not read. A nested object or array
-- is skipped whole and records as the empty string.
function read_value(line: string, i: integer): string, integer
    local head = line:sub(i, i)
    if head == '"' then
        return read_string(line, i)
    end
    if head == "{" or head == "[" then
        return "", skip_nested(line, i)
    end
    local n = #line
    local j = i
    while j <= n do
        local c = line:sub(j, j)
        if c == "," or c == "}" then
            break
        end
        j = j + 1
    end
    local raw = line:sub(i, j - 1):gsub("^%s*(.-)%s*$", "%1")
    return raw, j
end

-- The index just past the `}` or `]` closing the container at `i`,
-- counting nesting; 0 when the container never closes.
function skip_nested(line: string, i: integer): integer
    local n = #line
    local depth = 0
    local j = i
    while j <= n do
        local c = line:sub(j, j)
        if c == '"' then
            local _text, after = read_string(line, j)
            if after == 0 then
                return 0
            end
            j = after
        elseif c == "{" or c == "[" then
            depth = depth + 1
            j = j + 1
        elseif c == "}" or c == "]" then
            depth = depth - 1
            if depth == 0 then
                return j + 1
            end
            j = j + 1
        else
            j = j + 1
        end
    end
    return 0
end

return M
