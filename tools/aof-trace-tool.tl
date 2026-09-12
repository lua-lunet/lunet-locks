-- aof-trace-tool.tl — the LuaJIT twin of aof-trace-tool.py: read the
-- standby AOF traces (vendored TigerBeetle 0.17.9 format) through the
-- C ABI, one envelope at a time. LuaJIT is the lean runtime: one reused
-- RECORD_MAX buffer, cdata uint64 clocks (exact past 2^53), no
-- per-record tables. The python twin is the reference; the summaries
-- must have PARITY (same counts, same shape). Sketch source: the
-- upstream reader gist (handover 337b313e, aof-reader-tool-sketch.md).
--
-- Run the parity test: LUA_PATH='./?.tl;./?.lua;<tl-path>;;' luajit -l tl \
--   -e 'require("tl").loader(); require("tools.test_aof_trace_tool")'
local ffi = require("ffi")

ffi.cdef([[
int32_t lunet_aof_iter_open(const char* path, size_t path_len, void** out);
int32_t lunet_aof_iter_next(void* it, char* out_data, size_t cap,
                            size_t* out_len, uint64_t* out_op);
void lunet_aof_iter_close(void* it);
int32_t lunet_aof_open(const char* path, size_t path_len, uint8_t force_flush, void** out);
int32_t lunet_aof_append(void* file, const char* data, size_t len, uint64_t* out_op);
int32_t lunet_aof_close(void* file);
]])

local RECORD_MAX = (1024 * 1024) - 256

-- The envelope markers (item22 M1). Unknown markers are rejected, never
-- guessed — the same rejection table the python twin enforces.
local MARKER_WIRE = 1
local MARKER_TIMEOUT = 2
local MARKER_TRANSITION = 3
local MARKER_OUTBOUND = 4

-- The uVRR wire tags (marker-1 payload header, big-endian).
local TAG_NAMES: {number: string} = {
    [2] = "2 Prepare",
    [3] = "3 PrepareOk",
    [4] = "4 Commit",
    [5] = "5 StartViewChange",
    [6] = "6 DoViewChange",
    [7] = "7 StartView",
    [8] = "8 PlannedViewChange",
    [9] = "9 GetState",
    [10] = "10 NewState",
    [13] = "13 Reincarnation",
}

local record Native
    lunet_aof_iter_open: function(cdata, number, {any}): number
    lunet_aof_iter_next: function(any, cdata, number, {any}, {any}): number
    lunet_aof_iter_close: function(any)
    lunet_aof_open: function(cdata, number, number, {any}): number
    lunet_aof_append: function(any, cdata, number, {any}): number
    lunet_aof_close: function(any)
end

local function default_lib(): string
    local env = os.getenv("AOF_LIB")
    if env ~= nil and env ~= "" then
        return env
    end
    local info = debug.getinfo(1, "S") as {source: string}
    local dir = info.source:match("^@(.+)/[^/]+$") or "."
    local suffix = ffi.os == "OSX" and "dylib" or "so"
    return dir .. "/../ext/lunet-locks-aof/zig/zig-out/lib/liblunet_locks_aof." .. suffix
end

local native = ffi.load(os.getenv("AOF_LIB") or default_lib()) as Native

-- The one reused read buffer (the memory trick: stream, never
-- materialise — a python list of 50k records costs the world; this
-- costs one MiB, and only what the analysis needs ever survives).
local buffer = ffi.new("char[?]", RECORD_MAX)
local len_out = ffi.new("size_t[1]")
local op_out = ffi.new("uint64_t[1]")

local record Summary
    files: number
    records: number
    by_marker: {string: number}
    tags: {string: number}
    first_ns: any
    last_ns: any
    span_s: string
    malformed: number
end

local function iter_open(path: string): any
    local out = ffi.new("void*[1]")
    local rc = native.lunet_aof_iter_open(path, #path, out)
    if rc ~= 0 then
        error("lunet_aof_iter_open rc=" .. tostring(rc))
    end
    return out[0]
end

-- Big-endian ns of an envelope record: exact via cdata uint64.
function ns_of(record: string): any
    local v = ffi.new("uint64_t", 0)
    local b = ffi.cast("const uint8_t*", record)
    for i = 1, 8 do
        v = v * 256 + b[i]
    end
    return v
end

local function bump(t: {string: number}, key: string, by: number)
    t[key] = (t[key] or 0) + by
end

local function summarize_one(path: string): Summary
    local by_marker: {string: number} = {}
    local tags: {string: number} = {}
    local first_ns: any = nil
    local last_ns: any = nil
    local records = 0
    local malformed = 0
    local it = iter_open(path)
    while true do
        -- rc: >0 = one record (the length is in len_out, NOT the return);
        -- 0 = EOF or torn tail; <0 = the named error code.
        local rc = native.lunet_aof_iter_next(it, buffer, RECORD_MAX, len_out, op_out)
        if rc <= 0 then
            native.lunet_aof_iter_close(it)
            break
        end
        local frame = ffi.string(buffer, len_out[0])
        records = records + 1
        local marker = frame:byte(1)
        if (marker ~= MARKER_WIRE and marker ~= MARKER_TIMEOUT
            and marker ~= MARKER_TRANSITION and marker ~= MARKER_OUTBOUND)
            or #frame < 9 then
            malformed = malformed + 1
        else
            bump(by_marker, tostring(marker), 1)
            local ns = ns_of(frame)
            if first_ns == nil or ns < first_ns then
                first_ns = ns
            end
            if last_ns == nil or ns > last_ns then
                last_ns = ns
            end
            if marker == MARKER_WIRE then
                if #frame < 21 then
                    malformed = malformed + 1
                else
                    -- payload starts at byte 10 (1-indexed): marker(1)+ns(8)
                    local tag = frame:byte(10) * 16777216
                        + frame:byte(11) * 65536
                        + frame:byte(12) * 256
                        + frame:byte(13)
                    local name = TAG_NAMES[tag]
                    if name ~= nil then
                        bump(tags, name, 1)
                    else
                        malformed = malformed + 1
                    end
                end
            end
        end
    end
    local span_s = "0.000000"
    if first_ns ~= nil and last_ns ~= nil then
        span_s = string.format("%.6f", tonumber(last_ns - first_ns) / 1e9)
    end
    return {
        files = 1,
        records = records,
        by_marker = by_marker,
        tags = tags,
        first_ns = first_ns,
        last_ns = last_ns,
        span_s = span_s,
        malformed = malformed,
    }
end

local function merge_key(total: {string: number}, part: {string: number}, key: string)
    local v = part[key]
    if v ~= nil then
        bump(total, key, v)
    end
end

local function summarize_series(dir_path: string): Summary
    local total: Summary = {
        files = 0,
        records = 0,
        by_marker = {},
        tags = {},
        first_ns = nil,
        last_ns = nil,
        span_s = "0.000000",
        malformed = 0,
    }
    local names: {string} = {}
    local p = io.popen("ls -1 " .. dir_path .. "/*.aof 2>/dev/null")
    for f in p:lines() do
        names[#names + 1] = f
    end
    p:close()
    table.sort(names)
    total.files = #names
    for _, f in ipairs(names) do
        local s = summarize_one(f)
        total.records = total.records + s.records
        total.malformed = total.malformed + s.malformed
        merge_key(total.by_marker, s.by_marker, "1")
        merge_key(total.by_marker, s.by_marker, "2")
        merge_key(total.by_marker, s.by_marker, "3")
        merge_key(total.by_marker, s.by_marker, "4")
        for k, v in pairs(s.tags) do
            bump(total.tags, k, v)
        end
        if s.first_ns ~= nil and (total.first_ns == nil or s.first_ns < total.first_ns) then
            total.first_ns = s.first_ns
        end
        if s.last_ns ~= nil and (total.last_ns == nil or s.last_ns > total.last_ns) then
            total.last_ns = s.last_ns
        end
    end
    if total.first_ns ~= nil then
        total.span_s = string.format("%.6f", tonumber(total.last_ns - total.first_ns) / 1e9)
    end
    return total
end

-- The JSON in a FIXED key order (the parity contract with the python
-- twin's sort_keys output). ns values are cdata uint64 → tostring keeps
-- full precision past 2^53.
local function json_escape(s: string): string
    return (s:gsub('\\', '\\\\'):gsub('"', '\\"'))
end

local function sorted_map_json(m: {string: number}): string
    local keys: {string} = {}
    for k, _ in pairs(m) do
        keys[#keys + 1] = k
    end
    table.sort(keys)
    local out: {string} = {}
    for _, k in ipairs(keys) do
        out[#out + 1] = ('"%s": %d'):format(json_escape(k), m[k])
    end
    return "{" .. table.concat(out, ", ") .. "}"
end

function summarize_series_json(dir_path: string): string
    local total = summarize_series(dir_path)
    return ('{"files": %d, "records": %d, "by_marker": %s, "tags": %s, '
        .. '"first_ns": %s, "last_ns": %s, "span_s": %s, "malformed": %d}')
        :format(total.files, total.records, sorted_map_json(total.by_marker),
            sorted_map_json(total.tags), tostring(total.first_ns),
            tostring(total.last_ns), total.span_s, total.malformed)
end

-- The test-fixture writer surface (the same C ABI the standby uses).
function open_writer(path: string): any
    local out = ffi.new("void*[1]")
    local rc = native.lunet_aof_open(path, #path, 0, out)
    if rc ~= 0 then
        error("lunet_aof_open rc=" .. tostring(rc))
    end
    return out[0]
end

function append(handle: any, data: string): number
    local op = ffi.new("uint64_t[1]")
    local rc = native.lunet_aof_append(handle, data, #data, op)
    if rc ~= 0 then
        error("lunet_aof_append rc=" .. tostring(rc))
    end
    return tonumber(op[0])
end

function close_writer(handle: any)
    local rc = native.lunet_aof_close(handle)
    if rc ~= 0 then
        error("lunet_aof_close rc=" .. tostring(rc))
    end
end

function fixture_dir(): string
    local tmpl = os.getenv("TMPDIR") or "/tmp"
    local name = tmpl .. "/aof-trace-tool-" .. tostring(os.time()) .. "-" .. tostring(math.random(100000))
    os.execute("mkdir -p " .. name)
    return name
end

function is_dir(path: string): boolean
    local p = io.open(path .. "/.", "r")
    if p ~= nil then
        p:close()
        return true
    end
    return false
end

local function main(argv: {string}): number
    if #argv < 2 then
        print("usage: aof-trace-tool.tl [--lib PATH] [--json] FILE_OR_DIR ...")
        return 2
    end
    for i = 2, #argv do
        local p = argv[i]
        if p ~= "--json" and p ~= "--lib" then
            print(summarize_series_json(p))
        end
    end
    return 0
end

return {
    iterate = nil,
    ns_of = ns_of,
    summarize = summarize_one,
    summarize_series = summarize_series,
    summarize_series_json = summarize_series_json,
    fixture_dir = fixture_dir,
    open_writer = open_writer,
    append = append,
    close_writer = close_writer,
    is_dir = is_dir,
    main = main,
}
