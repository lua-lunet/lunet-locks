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
--
-- The twins share two hard contracts (item03.5):
-- - --kind is repeatable AND space-joined, with UNION semantics
--   ("--kind hb-spacing --kind aof-noise" == "--kind hb-spacing aof-noise");
--   the special names "all" and "both" expand to their kind sets and
--   union with any named kinds.
-- - distribution means use one documented summation algorithm: Neumaier
--   compensated summation over the sorted values (compensated_sum).
--
-- The CLI is tools/tl-driver.lua — tl gen of this module alone is NOT
-- a CLI (the generated file only runs under the Teal loader and a
-- plain luajit run silently no-ops). Run it from the repo root:
--   LUA_PATH="./?.tl;./?.lua;.rocks/share/lua/5.1/?.lua;;" \
--     luajit -l tl tools/tl-driver.lua [--kind KIND ...] DIR OUT.jsonl
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
local MARKER_INTERVAL = 5

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

-- The sentinel a saturated field carries through to the JSON as null.
local JSON_NULL = "json-null"

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

-- One file's summary: the same shape the python twin's summarize()
-- produces (no "files" key).
function summarize_one_json(path: string): string
    local s = summarize_one(path)
    return ('{"records": %d, "by_marker": %s, "tags": %s, '
        .. '"first_ns": %s, "last_ns": %s, "span_s": %s, "malformed": %d}')
        :format(s.records, sorted_map_json(s.by_marker),
            sorted_map_json(s.tags), tostring(s.first_ns),
            tostring(s.last_ns), s.span_s, s.malformed)
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


-- -------------------------------------------------------------- export ----
-- The trace of what happened, as record tables: the ts at the AOF (the
-- standby's local ns clock), the ts at the leader (the phi trailer's
-- sent_at_ms, carried on every leader Commit), the command, the result.

local TRAILER_MAGIC = {0xc0, 0x0b}
local TRAILER_BYTES = 22

local SYS_NAMES: {number: string} = {
    [1] = "Void", [2] = "Init", [3] = "Increment", [4] = "Decrement",
    [5] = "Double", [6] = "Halve", [7] = "Join", [8] = "Leave", [9] = "Batch",
}

-- All byte readers are 1-INDEXED like the string: the ffi cast is
-- 0-indexed, so every lookup lands at `at - 1`.
local function be_u32(buf: string, at: number): number
    local b = ffi.cast("const uint8_t*", buf)
    return b[at - 1] * 16777216 + b[at] * 65536 + b[at + 1] * 256 + b[at + 2]
end

local function be_u64(buf: string, at: number): any
    local v = ffi.new("uint64_t", 0)
    local b = ffi.cast("const uint8_t*", buf)
    for i = 0, 7 do
        v = v * 256 + b[at - 1 + i]
    end
    return v
end

-- little-endian u64 from the tail (the trailer's sent_at_ms)
local function le_u64(buf: string, at: number): any
    local v = ffi.new("uint64_t", 0)
    local b = ffi.cast("const uint8_t*", buf)
    for i = 7, 0, -1 do
        v = v * 256 + b[at - 1 + i]
    end
    return v
end

local function split_trailer(frame: string): (string, any)
    if #frame >= TRAILER_BYTES then
        local m1 = frame:byte(#frame - TRAILER_BYTES + 1)
        local m2 = frame:byte(#frame - TRAILER_BYTES + 2)
        if m1 == TRAILER_MAGIC[1] and m2 == TRAILER_MAGIC[2] then
            return frame:sub(1, #frame - TRAILER_BYTES), le_u64(frame, #frame - 7)
        end
    end
    return frame, nil
end

-- One SystemOperation: (command string, bytes consumed) or (nil, 0).
local function parse_system(buf: string): (string, number)
    if #buf == 0 then
        return nil, 0
    end
    local disc = buf:byte(1)
    local name = SYS_NAMES[disc]
    if name == nil then
        return nil, 0
    end
    if disc == 1 or disc == 5 or disc == 6 then
        return name, 1
    end
    if disc == 3 or disc == 4 or disc == 8 then
        if #buf < 5 then
            return nil, 0
        end
        return ("%s node=%d"):format(name, be_u32(buf, 2)), 5
    end
    if disc == 7 then
        if #buf < 9 then
            return nil, 0
        end
        return ("Join node=%d position=%d"):format(be_u32(buf, 2), be_u32(buf, 6)), 9
    end
    if disc == 2 then
        if #buf < 5 then
            return nil, 0
        end
        local count = be_u32(buf, 2)
        if #buf < 5 + 4 * count then
            return nil, 0
        end
        local nodes: {string} = {}
        for i = 0, count - 1 do
            nodes[#nodes + 1] = tostring(be_u32(buf, 6 + 4 * i))
        end
        return ("Init order=%s"):format(table.concat(nodes, ",")), 5 + 4 * count
    end
    if disc == 9 then
        if #buf < 5 then
            return nil, 0
        end
        local count = be_u32(buf, 2)
        local cursor = 6
        local parts: {string} = {}
        for _ = 0, count - 1 do
            local sub, consumed = parse_system(buf:sub(cursor))
            if sub == nil then
                return nil, 0
            end
            parts[#parts + 1] = sub
            cursor = cursor + consumed
        end
        return ("Batch[%s]"):format(table.concat(parts, "; ")), cursor - 1
    end
    return nil, 0
end

-- One uVRR wire frame: (kind, tag, era, view, slot, entry, committed)
-- where entry is ("operation", payload-json) or ("system", command).
local function parse_wire(frame: string): (string, number, number, number, any, ({string, string} | {string, any}), any)
    if #frame < 21 then
        return nil, nil, nil, nil, nil, nil, nil
    end
    local tag = be_u32(frame, 1)
    local era = be_u32(frame, 5)
    local view = be_u32(frame, 9)
    local slot = be_u64(frame, 13)
    local disc = frame:byte(21)
    if disc ~= tag % 256 then
        return nil, nil, nil, nil, nil, nil, nil
    end
    if tag == 2 then
        if #frame < 21 + 8 + 4 + 1 then
            return nil, nil, nil, nil, nil, nil, nil
        end
        local entry_slot = be_u64(frame, 22)
        local entry_era = be_u32(frame, 30)
        local payload_disc = frame:byte(34)
        local entry: {string, string}
        local cursor = 35
        if payload_disc == 1 then
            if #frame < cursor + 16 + 4 then
                return nil, nil, nil, nil, nil, nil, nil
            end
            local jlen = be_u32(frame, cursor + 16)
            cursor = cursor + 16 + 4
            if #frame < cursor + jlen then
                return nil, nil, nil, nil, nil, nil, nil
            end
            entry = {"operation", frame:sub(cursor, cursor + jlen - 1)}
            cursor = cursor + jlen
        elseif payload_disc == 2 then
            local command, consumed = parse_system(frame:sub(cursor))
            if command == nil then
                return nil, nil, nil, nil, nil, nil, nil
            end
            entry = {"system", command}
            cursor = cursor + consumed
        else
            return nil, nil, nil, nil, nil, nil, nil
        end
        -- committed = the 8 bytes at cursor..cursor+7 (1-indexed inclusive)
        if #frame < cursor + 7 then
            return nil, nil, nil, nil, nil, nil, nil
        end
        local committed = be_u64(frame, cursor)
        _ = entry_slot
        _ = entry_era
        return "prepare", tag, era, view, slot, entry, committed
    elseif tag == 4 then
        if #frame < 21 + 8 - 1 then
            return nil, nil, nil, nil, nil, nil, nil
        end
        return "commit", tag, era, view, slot, nil, be_u64(frame, 22)
    elseif tag == 3 or tag == 5 or tag == 8 then
        return "empty", tag, era, view, slot, nil, nil
    end
    return "other", tag, era, view, slot, nil, nil
end

-- The lock-op fields out of the tagged JSON (the same shapes the python
-- twin's json.loads extracts; both tools read the same bytes).
-- an integral decimal token read back as exact uint64 cdata (past
-- 2^53 too: digit-by-digit, no double intermediate)
local function require_ffi_u64(s: string): any
    local sign = true
    local body = s
    if s:byte(1) == string.byte("-") then
        sign = false
        body = s:sub(2)
    end
    local v = ffi.new("uint64_t", 0)
    for i = 1, #body do
        v = v * 10 + (body:byte(i) - 48)
    end
    if not sign then
        v = -v
    end
    return v
end

local function json_num(json: string, key: string): any
    local raw = json:match('"' .. key .. '":%s*(-?%d+%.?%d*)')
    if raw ~= nil then
        -- integral tokens stay exact (and int-typed on the wire: the
        -- reference dumps them as python ints); fractional ride as floats
        if raw:match("^-?%d+$") ~= nil then
            return require_ffi_u64(raw)
        end
        return tonumber(raw)
    end
    -- the detector's bare `inf`/`nan` saturations are not JSON: null
    if json:match('"' .. key .. '":%s*[-+]?inf') ~= nil
        or json:match('"' .. key .. '":%s*[Nn]a[Nn]') ~= nil
        or json:match('"' .. key .. '":%s*null') ~= nil then
        return JSON_NULL
    end
    return nil
end

local function json_str(json: string, key: string): string
    return json:match('"' .. key .. '":%s*"([^"]*)"')
end

local LOCK_OPS: {string: boolean} = {
    ["get"] = true, ["set"] = true, ["release"] = true, ["break"] = true,
}

local function lock_fields(json: string): ({string: any} | nil)
    local op = json:match('"op":%s*"([a-z]+)"')
    if op == nil or LOCK_OPS[op] ~= true then
        return nil
    end
    return {
        op = op,
        lock_id = json_num(json, "lock_id"),
        message_id = json_str(json, "message_id"),
        client_id = json_num(json, "client_id"),
        request_num = json_num(json, "request_num"),
    }
end

local function json_payload(frame: string): string
    -- the envelope: marker(1) + ns(8) + payload → the JSON starts at 10
    return frame:sub(10)
end

-- The JSONL line for one event: fixed key order (the parity contract).
local function json_escape(s: string): string
    local out = s:gsub("\\", "\\\\")
    out = out:gsub('"', '\\"')
    return out
end

-- Nested aggregate serialisation: _stats tables and stall entries
-- carry their own key order in _o (the parity contract with the
-- python twin's dict construction order).
-- python-json-compatible float rendering: repr's shortest-round-trip
-- (integral floats keep the .0 the reference prints; %%g alone would
-- drop it and truncate the averaging fractions at 14 digits).
local function float_repr(v: number): string
    if v == math.floor(v) and math.abs(v) < 1e15 then
        return ("%d.0"):format(math.floor(v))
    end
    for p = 1, 17 do
        local s = string.format(("%." .. p .. "g"), v)
        if tonumber(s) == v then
            return s
        end
    end
    return string.format("%.17g", v)
end

local function scalar_json(key: string, v: any): string
    if v == JSON_NULL then
        return ('"%s": null'):format(key)
    elseif v == true or v == false then
        return ('"%s": %s'):format(key, tostring(v))
    elseif type(v) == "string" then
        return ('"%s": "%s"'):format(key, json_escape(v))
    elseif ffi.istype("uint64_t", v) then
        -- exact past 2^53: the cdata formats itself, no ULL suffix
        return ('"%s": %s'):format(key, string.format("%d", v))
    elseif type(v) == "number" then
        return ('"%s": %s'):format(key, float_repr(v))
    else
        return ('"%s": %s'):format(key, tostring(v))
    end
end

local function nested_json(v: any): string
    if type(v) ~= "table" then
        return tostring(v)
    end
    if v._o ~= nil then
        local parts: {string} = {}
        for _, key in ipairs(v._o as {string}) do
            if v[key] ~= nil then
                parts[#parts + 1] = scalar_json(key, v[key])
            end
        end
        return "{" .. table.concat(parts, ", ") .. "}"
    end
    -- an array of nested objects (the stall list)
    local parts: {string} = {}
    for _, e in ipairs(v) do
        parts[#parts + 1] = nested_json(e)
    end
    return "[" .. table.concat(parts, ", ") .. "]"
end

-- Per-kind key order: each mirrors the python twin's dict
-- construction order so a raw line diff is the parity check.
local DEFAULT_ORDER: {string} = {"kind", "aof_ns", "leader_ms", "slot",
                                 "result", "op", "lock_id", "message_id",
                                 "client_id", "request_num", "command",
                                 "phi", "now_ms", "prev_wait_ms",
                                 "next_wait_ms", "mean_interval_ms", "mean",
                                 "min_ms", "max_ms", "node", "era", "leader",
                                 "view", "addr", "holder", "dt_ms", "ts_ms",
                                 "sent_at_ms", "t_rel_ms", "t_fmt", "anchor",
                                 "last_holder", "last_renew_t_rel",
                                 "break_seen_t_rel", "next_acquire_t_rel",
                                 "next_holder", "takeover_ms", "dir",
                                 "source", "min_ms", "mean_ms", "p50_ms",
                                 "p95_ms", "max_ms", "count", "gaps",
                                 "stalls", "samples"}
local ROW_ORDER: {string: {string}} = {
    ["locks"] = {"kind", "aof_ns", "leader_ms", "slot", "result", "op",
                 "lock_id", "message_id", "client_id", "request_num",
                 "t_rel_ms", "t_fmt"},
    ["reconfig"] = {"kind", "command", "aof_ns", "leader_ms", "slot",
                    "result", "t_rel_ms", "t_fmt"},
    ["phi"] = {"kind", "aof_ns", "phi", "now_ms", "prev_wait_ms",
               "next_wait_ms", "leader", "era", "view", "mean",
               "mean_interval_ms", "min_ms", "max_ms", "t_rel_ms", "t_fmt"},
    ["phi-samples"] = {"kind", "aof_ns", "node", "era", "leader", "addr",
                       "dt_ms", "ts_ms", "phi", "sent_at_ms",
                       "t_rel_ms", "t_fmt"},
    ["locks-timeline"] = {"kind", "op", "holder", "lock_id", "ts_ms",
                          "aof_ns", "t_rel_ms", "t_fmt"},
    ["takeover"] = {"kind", "anchor", "last_holder", "last_renew_t_rel",
                    "break_seen_t_rel", "next_acquire_t_rel", "next_holder",
                    "takeover_ms"},
    ["anchor"] = {"kind", "anchor", "ts_ms", "t_rel_ms", "t_fmt"},
    ["hb-spacing"] = {"kind", "dir", "source", "era", "leader", "min_ms",
                      "mean_ms", "p50_ms", "p95_ms", "max_ms", "count",
                      "t_rel_ms", "t_fmt"},
    ["aof-noise"] = {"kind", "dir", "gaps", "stalls", "samples",
                     "t_rel_ms", "t_fmt"},
}

local function ev_json(e: {string: any}): string
    local parts: {string} = {}
    local order = ROW_ORDER[e.kind] or DEFAULT_ORDER
    for _, key in ipairs(order) do
        local v = e[key]
        if v ~= nil then
            if type(v) == "table" then
                parts[#parts + 1] = ('"%s": %s'):format(key, nested_json(v))
            else
                parts[#parts + 1] = scalar_json(key, v)
            end
        end
    end
    return "{" .. table.concat(parts, ", ") .. "}"
end

-- Parity with the python twin's wanted-set: "all" is the base kinds
-- only (the new kinds ride under their own names). item03.5: the kinds
-- string is a whitespace-joined UNION (the CLI accumulates repeatable
-- --kind values into it; "all"/"both" stay whole-string for the
-- library callers).
local function kind_wanted(kinds: string, kind: string): boolean
    if kinds == "all" then
        return kind == "locks" or kind == "reconfig" or kind == "phi"
            or kind == "phi-samples"
    end
    if kinds == "both" then
        return kind == "locks" or kind == "reconfig"
    end
    for token in kinds:gmatch("%S+") do
        if token == kind then
            return true
        end
    end
    return false
end

-- Forward declarations: the extras (timeline/hb-spacing/aof-noise) and
-- the anchored export read export_series, but export_series appends
-- their rows — declared here, defined below.
local timeline_rows
local hb_spacing_rows
local aof_noise_rows
local takeover_row
local anchor_rows_and_takeovers

local function export_series(dir_path: string, kinds: string, anchors: any, hb_ms: any): {{string: any}}
    local wanted_names: {string} = {}
    local p = io.popen("ls -1 " .. dir_path .. "/*.aof 2>/dev/null")
    for f in p:lines() do
        wanted_names[#wanted_names + 1] = f
    end
    p:close()
    table.sort(wanted_names)
    local events: {{string: any}} = {}
    local pending: {number: {string: any}} = {}
    for _, path in ipairs(wanted_names) do
        local it = iter_open(path)
        while true do
            local rc = native.lunet_aof_iter_next(it, buffer, RECORD_MAX, len_out, op_out)
            if rc <= 0 then
                native.lunet_aof_iter_close(it)
                break
            end
            local frame = ffi.string(buffer, len_out[0])
            local aof_ns = ns_of(frame)
            local marker = frame:byte(1)
            if marker == MARKER_TIMEOUT then
                if kind_wanted(kinds, "phi") then
                    local e: {string: any} = {kind = "phi", aof_ns = aof_ns}
                    local j = json_payload(frame)
                    -- the detector saturates: bare `inf` (not JSON) → null
                    e.phi = json_num(j, "phi")
                    e.now_ms = json_num(j, "now_ms")
                    e.prev_wait_ms = json_num(j, "prev_wait_ms")
                    e.next_wait_ms = json_num(j, "next_wait_ms")
                    e.mean_interval_ms = json_num(j, "mean_interval_ms")
                    e.min_ms = json_num(j, "min_ms")
                    e.max_ms = json_num(j, "max_ms")
                    -- the reference dumps the whole payload: the
                    -- emitters also write a bare `mean` alias
                    e.mean = json_num(j, "mean")
                    e.leader = json_num(j, "leader")
                    e.era = json_num(j, "era")
                    e.view = json_num(j, "view")
                    events[#events + 1] = e
                end
            elseif marker == MARKER_INTERVAL then
                if kind_wanted(kinds, "phi-samples") then
                    local e: {string: any} = {kind = "phi-samples", aof_ns = aof_ns}
                    local j = json_payload(frame)
                    e.node = json_num(j, "node")
                    e.era = json_num(j, "era")
                    e.leader = json_num(j, "leader")
                    e.addr = json_str(j, "addr")
                    e.dt_ms = json_num(j, "dt_ms")
                    e.ts_ms = json_num(j, "ts_ms")
                    e.phi = json_num(j, "phi")
                    e.sent_at_ms = json_num(j, "sent_at_ms")
                    events[#events + 1] = e
                end
            elseif marker == MARKER_WIRE then
                local wire_frame, leader_ms = split_trailer(json_payload(frame))
                local kind, _tag, _era, _view, slot, entry, committed = parse_wire(wire_frame)
                if kind == "prepare" and entry ~= nil then
                    if entry[1] == "operation" then
                        local fields = lock_fields(entry[2])
                        if fields ~= nil then
                            pending[tonumber(slot)] = fields
                        end
                    elseif entry[1] == "system" then
                        pending[tonumber(slot)] = {
                            kind = "reconfig",
                            command = entry[2],
                        }
                    end
                    local frontier = tonumber(committed)
                    for s, fields in pairs(pending) do
                        if s <= frontier then
                            pending[s] = nil
                            if fields.op ~= nil then
                                if kind_wanted(kinds, "locks") then
                                    local e: {string: any} = {
                                        kind = "locks", aof_ns = aof_ns,
                                        leader_ms = leader_ms,
                                        slot = require_ffi_u64(tostring(s)), result = "committed",
                                    }
                                    for k, v in pairs(fields) do
                                        e[k] = v
                                    end
                                    events[#events + 1] = e
                                end
                            elseif kind_wanted(kinds, "reconfig") then
                                local e: {string: any} = {
                                    aof_ns = aof_ns,
                                    leader_ms = leader_ms,
                                    slot = require_ffi_u64(tostring(s)), result = "committed",
                                }
                                for k, v in pairs(fields) do
                                    e[k] = v
                                end
                                events[#events + 1] = e
                            end
                        end
                    end
                elseif kind == "commit" then
                    local frontier = tonumber(committed)
                    for s, fields in pairs(pending) do
                        if s <= frontier then
                            pending[s] = nil
                            if fields.op ~= nil then
                                if kind_wanted(kinds, "locks") then
                                    local e: {string: any} = {
                                        kind = "locks", aof_ns = aof_ns,
                                        leader_ms = leader_ms,
                                        slot = require_ffi_u64(tostring(s)), result = "committed",
                                    }
                                    for k, v in pairs(fields) do
                                        e[k] = v
                                    end
                                    events[#events + 1] = e
                                end
                            elseif kind_wanted(kinds, "reconfig") then
                                local e: {string: any} = {
                                    aof_ns = aof_ns,
                                    leader_ms = leader_ms,
                                    slot = require_ffi_u64(tostring(s)), result = "committed",
                                }
                                for k, v in pairs(fields) do
                                    e[k] = v
                                end
                                events[#events + 1] = e
                            end
                        end
                    end
                end
            end
        end
    end
    if kind_wanted(kinds, "locks-timeline") then
        for _, r in ipairs(timeline_rows(dir_path) as {{string: any}}) do
            events[#events + 1] = r
        end
    end
    if kind_wanted(kinds, "hb-spacing") then
        for _, r in ipairs(hb_spacing_rows(dir_path) as {{string: any}}) do
            events[#events + 1] = r
        end
    end
    if kind_wanted(kinds, "aof-noise") then
        local floor = hb_ms
        if floor == nil then
            floor = 5.0
        end
        for _, r in ipairs(aof_noise_rows(dir_path, floor) as {{string: any}}) do
            events[#events + 1] = r
        end
    end
    if anchors ~= nil and type(anchors) == "table" and #anchors > 0 then
        events = anchor_rows_and_takeovers(events, anchors as {number})
    end
    return events
end

-- ------------------------------------------------- time columns (item03) ----
-- Readable times: an anchor is a kill/silence stamp in unix ms (the
-- acting host's date stamp), DIRECTLY comparable to a same-host AOF
-- envelope ns stamp. Every exported row gains t_rel_ms (relative to the
-- latest anchor at or before the row's ts; negative before the first
-- anchor) and t_fmt (HH:MM:SS.mmm of the row's ts, UTC).

local ABSENT = "absent"

-- Exact-enough ms: the whole-second part is well under 2^53, and the
-- sub-ms remainder rides in as its own fraction. All arithmetic on the
-- cdata uint64 stays exact (u64 div/mod); only the final conversion is
-- a double.
ns_to_ms = function(ns: any): number
    local rem = tonumber(ns % 1000000)
    local whole = tonumber((ns - ns % 1000000) / 1000000)
    return whole + rem / 1e6
end

-- HH:MM:SS.mmm (UTC) of a unix-ms stamp, epoch arithmetic only.
local function fmt_ts_ms(ts_ms: any): string
    local m = tonumber(ts_ms)
    if m == nil then
        return ABSENT
    end
    if m < 0 then
        return "-" .. fmt_ts_ms(-m)
    end
    local mi = math.floor(m)
    local frac = mi % 1000
    local tod = math.floor(mi / 1000)
    return string.format("%02d:%02d:%02d.%03d",
        math.floor(tod / 3600) % 24, math.floor(tod / 60) % 60, tod % 60, frac)
end

-- The latest anchor at or before the ts; the first anchor before any ts
-- earlier than it (negative t_rel is legitimate and allowed).
local function anchor_of(ts_ms: number, anchors: {number}): number
    local chosen = anchors[1]
    for _, a in ipairs(anchors) do
        if a <= ts_ms then
            chosen = a
        else
            break
        end
    end
    return chosen
end

-- py's round(_, 3): half away from zero at the micro-second grain.
local function round3(v: number): number
    if v >= 0 then
        return math.floor(v * 1000 + 0.5) / 1000
    end
    return -math.floor(-v * 1000 + 0.5) / 1000
end

-- The distribution record: min/mean/p50/p95/max + count, nearest-rank
-- percentile over the sorted values (table.sort's contract is enough —
-- the sort is stable for the statistics read).
local STATS_ORDER: {string} = {"min_ms", "mean_ms", "p50_ms", "p95_ms",
                               "max_ms", "count"}

-- The one summation algorithm both twins share (item03.5): Neumaier
-- compensated summation over the SORTED values — the python twin's
-- reference behaviour (its _compensated_sum; CPython >= 3.12's sum()
-- already compensates, the explicit loop pins it version-independently).
-- Naive left-to-right accumulation drifted the aof-noise mean ~1e-12 ms
-- on the 28k-gap real-w1b trace; min/p50/p95/max never drifted (they
-- are picks, not sums).
local function compensated_sum(s: {number}): number
    local total = 0.0
    local corr = 0.0
    for _, v in ipairs(s) do
        local t = total + v
        if math.abs(total) >= math.abs(v) then
            corr = corr + ((total - t) + v)
        else
            corr = corr + ((v - t) + total)
        end
        total = t
    end
    return total + corr
end

local function stats(values: {number}): {string: any}
    local s: {number} = {}
    for _, v in ipairs(values) do
        s[#s + 1] = v
    end
    table.sort(s)
    local n = #s
    if n == 0 then
        return {min_ms = ABSENT, mean_ms = ABSENT, p50_ms = ABSENT,
                p95_ms = ABSENT, max_ms = ABSENT, count = ffi.new("uint64_t", 0),
                _o = STATS_ORDER}
    end
    return {min_ms = s[1], mean_ms = compensated_sum(s) / n,
            p50_ms = s[math.ceil(0.5 * n)],
            p95_ms = s[math.ceil(0.95 * n)],
            max_ms = s[n], count = ffi.new("uint64_t", n), _o = STATS_ORDER}
end

-- The row's best ts in unix ms, or nil when the row is an aggregate
-- summary with no single ts.
local function row_ts_ms(row: {string: any}): any
    local k = row.kind
    if k == "locks" or k == "locks-timeline" or k == "reconfig" then
        return ns_to_ms(row.aof_ns)
    elseif k == "phi" or k == "phi-samples" then
        if row.ts_ms ~= nil then
            return tonumber(row.ts_ms)
        end
        if row.now_ms ~= nil then
            return tonumber(row.now_ms)
        end
        if row.aof_ns ~= nil then
            return tonumber(ns_to_ms(row.aof_ns))
        end
    end
    return nil
end

-- One row per lock-operation event in trace order. set resolves to
-- acquire or renew against the holder in force; release/break clear it.
timeline_rows = function(dir_path: string): {{string: any}}
    local locks = export_series(dir_path, "locks")
    local rows: {{string: any}} = {}
    local tenure: {any: any} = {} -- lock_id -> holder in force
    for _, r in ipairs(locks) do
        local lid = tostring(r.lock_id)
        local holder = r.client_id
        local op: string
        if r.op == "set" then
            op = (tenure[lid] == holder) and "renew" or "acquire"
            tenure[lid] = holder
        elseif r.op == "release" then
            op = "release"
            tenure[lid] = nil
        elseif r.op == "break" then
            op = "break"
            tenure[lid] = nil
        else
            op = "get"
        end
        rows[#rows + 1] = {kind = "locks-timeline", op = op, holder = holder,
                           lock_id = r.lock_id, ts_ms = ns_to_ms(r.aof_ns),
                           aof_ns = r.aof_ns}
    end
    return rows
end

-- The per-anchor takeover summary: the last sign of the outgoing
-- holder, the server break if seen, and the next holder's acquire.
-- Any fact not in this anchor's window states "absent" (never guessed).
takeover_row = function(window_rows: {{string: any}}, anchor: number,
                        anchor_text: string): {string: any}
    local anchor_out = require_ffi_u64(anchor_text)
    local function exactly(v: any): any
        if v == nil then
            return ABSENT
        end
        return round3(v)
    end
    local heres: {{string: any}} = {}
    local brk: {{string: any}} = nil
    brk = {}
    for _, r in ipairs(window_rows) do
        if r.op == "acquire" or r.op == "renew" then
            heres[#heres + 1] = r
        elseif r.op == "break" then
            brk[#brk + 1] = r
        end
    end
    local last: {string: any}
    local nxt: {string: any}
    if #brk > 0 then
        local b = brk[1]
        -- the last holder fact BEFORE the break, then the first
        -- acquire AFTER it
        for _, r in ipairs(heres) do
            if r.ts_ms < b.ts_ms + 0.0 then
                last = r
            end
        end
        for _, r in ipairs(heres) do
            if r.op == "acquire" and r.ts_ms > b.ts_ms then
                if nxt == nil then
                    nxt = r
                end
            end
        end
    else
        -- No break row: the handover is a holder change (natural TTL
        -- out; server-side break not recorded in this window).
        for j = 2, #heres do
            if heres[j].holder ~= heres[j - 1].holder then
                last = heres[j - 1]
                nxt = heres[j]
                break
            end
        end
        if last == nil and #heres > 0 then
            last = heres[#heres]
        end
    end
    return {kind = "takeover", anchor = anchor_out,
            last_holder = (last ~= nil) and last.holder or ABSENT,
            last_renew_t_rel = (last ~= nil) and exactly(last.ts_ms - anchor) or ABSENT,
            break_seen_t_rel = (#brk > 0) and exactly(brk[1].ts_ms - anchor) or ABSENT,
            next_acquire_t_rel = (nxt ~= nil) and exactly(nxt.ts_ms - anchor) or ABSENT,
            next_holder = (nxt ~= nil) and nxt.holder or ABSENT,
            takeover_ms = (nxt ~= nil) and exactly(nxt.ts_ms - anchor) or ABSENT}
end

-- The anchored export: time columns on every row, anchor echoes first,
-- then takeover summary lines when the locks-timeline ran.
anchor_rows_and_takeovers = function(events: {{string: any}}, anchors_in: {number}): {{string: any}}
    local anchors: {number} = {}
    for _, a in ipairs(anchors_in) do
        anchors[#anchors + 1] = a
    end
    table.sort(anchors)
    local timed: {{string: any}} = {}
    for _, row in ipairs(events) do
        local ts = row_ts_ms(row)
        if ts == nil then
            row.t_rel_ms = ABSENT
            row.t_fmt = ABSENT
        else
            row.t_rel_ms = round3(ts - anchor_of(ts, anchors))
            row.t_fmt = fmt_ts_ms(ts)
        end
        timed[#timed + 1] = row
    end
    local out: {{string: any}} = {}
    for _, a in ipairs(anchors) do
        -- the anchor echoes as the int the reference keeps; the float
        -- rides on for the window arithmetic
        out[#out + 1] = {kind = "anchor", anchor = require_ffi_u64(tostring(a)),
                         ts_ms = a + 0.0,
                         t_rel_ms = 0.0, t_fmt = fmt_ts_ms(a)}
    end
    for _, row in ipairs(timed) do
        out[#out + 1] = row
    end
    local has_tl = false
    for _, row in ipairs(timed) do
        if row.kind == "locks-timeline" then
            has_tl = true
        end
    end
    if has_tl then
        local tl: {{string: any}} = {}
        for _, row in ipairs(timed) do
            if row.kind == "locks-timeline" then
                tl[#tl + 1] = row
            end
        end
        for i, a in ipairs(anchors) do
            local hi = anchors[i + 1]
            local window: {{string: any}} = {}
            for _, r in ipairs(tl) do
                if r.ts_ms >= a and (hi == nil or r.ts_ms < hi) then
                    window[#window + 1] = r
                end
            end
            out[#out + 1] = takeover_row(window, a, tostring(a))
        end
    end
    return out
end

-- little-endian u32 inside a record (the trailer's era/leader fields)
local function le_u32(buf: string, at: number): number
    local b = ffi.cast("const uint8_t*", buf)
    return b[at - 1] + b[at] * 256 + b[at + 1] * 65536 + b[at + 2] * 16777216
end

-- The dir's AOF series, sorted (file order = ns order contract).
local function aof_names(dir_path: string): {string}
    local names: {string} = {}
    local p = io.popen("ls -1 " .. dir_path .. "/*.aof 2>/dev/null")
    for f in p:lines() do
        names[#names + 1] = f
    end
    p:close()
    table.sort(names)
    return names
end

-- per (era, leader) heartbeat-spacing stats: the leader's own send
-- gaps (wire trailer sent_at_ms), arrived-gap dt_ms from marker-5.
hb_spacing_rows = function(dir_path: string): {{string: any}}
    local sent_by: {string: {number}} = {}
    local dt_by: {string: {number}} = {}
    for _, path in ipairs(aof_names(dir_path)) do
        local it = iter_open(path)
        while true do
            local rc = native.lunet_aof_iter_next(it, buffer, RECORD_MAX, len_out, op_out)
            if rc <= 0 then
                native.lunet_aof_iter_close(it)
                break
            end
            local frame = ffi.string(buffer, len_out[0])
            local marker = frame:byte(1)
            if marker == MARKER_WIRE and #frame >= TRAILER_BYTES + 9 then
                local _wf, sent = split_trailer(frame:sub(10))
                if sent ~= nil then
                    -- era then leader: the trailer's first 8 bytes
                    local era = le_u32(frame, #frame - 19)
                    local leader = le_u32(frame, #frame - 15)
                    local key = tostring(era) .. ":" .. tostring(leader)
                    local list = sent_by[key]
                    if list == nil then
                        list = {}
                        sent_by[key] = list
                    end
                    list[#list + 1] = tonumber(sent)
                end
            elseif marker == MARKER_INTERVAL then
                local j = json_payload(frame)
                local era = json_num(j, "era")
                local leader = json_num(j, "leader")
                local dt = json_num(j, "dt_ms")
                if dt ~= nil and dt ~= JSON_NULL then
                    -- u64 cdata tostrings as "33ULL": canonicalise the
                    -- key back to the plain integer text
                    local key = tostring(tonumber(era)) .. ":"
                        .. tostring(tonumber(leader))
                    local list = dt_by[key]
                    if list == nil then
                        list = {}
                        dt_by[key] = list
                    end
                    list[#list + 1] = tonumber(dt)
                end
            end
        end
    end
    local rows: {{string: any}} = {}
    local function emit(source: string, by: {string: {number}}, min_count: number, gapmode: boolean)
        local keys: {string} = {}
        for k, _ in pairs(by) do
            keys[#keys + 1] = k
        end
        table.sort(keys, function(x: string, y: string)
            local xe, xl = x:match("(%S+):(%S+)")
            local ye, yl = y:match("(%S+):(%S+)")
            local xe_n = tonumber(xe)
            local ye_n = tonumber(ye)
            if xe_n ~= nil and ye_n ~= nil and xe_n ~= ye_n then
                return xe_n < ye_n
            end
            local xl_n = tonumber(xl)
            local yl_n = tonumber(yl)
            if xl_n ~= nil and yl_n ~= nil then
                return xl_n < yl_n
            end
            return x < y
        end)
        for _, k in ipairs(keys) do
            local values = by[k]
            local stats_values = values
            if gapmode then
                -- wire: the gap series of consecutive sent_at_ms (only
                -- forward gaps count, exactly like the python twin)
                local gaps: {number} = {}
                for i = 2, #values do
                    local d = values[i] - values[i - 1]
                    if d > 0 then
                        gaps[#gaps + 1] = d + 0.0
                    end
                end
                stats_values = gaps
            end
            if #stats_values >= min_count then
                local era, leader = k:match("(%S+):(%S+)")
                local row: {string: any} = {kind = "hb-spacing", dir = dir_path,
                                            source = source,
                                            era = require_ffi_u64(era),
                                            leader = require_ffi_u64(leader)}
                local st = stats(stats_values)
                for _, key in ipairs(STATS_ORDER) do
                    row[key] = st[key]
                end
                rows[#rows + 1] = row
            end
        end
    end
    -- wire: skip a pair whose sent_at gap series is empty
    emit("wire", sent_by, 1, true)
    emit("sample", dt_by, 2, false)
    return rows
end

-- The AOF noise picture: the arrival-gap series between consecutive
-- marker-1 records (the envelope ns is stamped pre-queue, so
-- write-behind shows up here, not as latency), the marker-5 arrival
-- dt stats, and the ns-gap stalls over the floor min(hb_ms, 5p50).
aof_noise_rows = function(dir_path: string, hb_ms: number): {{string: any}}
    local ns_list: {any} = {}
    local dts: {number} = {}
    for _, path in ipairs(aof_names(dir_path)) do
        local it = iter_open(path)
        while true do
            local rc = native.lunet_aof_iter_next(it, buffer, RECORD_MAX, len_out, op_out)
            if rc <= 0 then
                native.lunet_aof_iter_close(it)
                break
            end
            local frame = ffi.string(buffer, len_out[0])
            local marker = frame:byte(1)
            if marker == MARKER_WIRE then
                ns_list[#ns_list + 1] = ns_of(frame)
            elseif marker == MARKER_INTERVAL then
                local j = json_payload(frame)
                local dt = json_num(j, "dt_ms")
                if dt ~= nil and dt ~= JSON_NULL then
                    dts[#dts + 1] = tonumber(dt)
                end
            end
        end
    end
    local row: {string: any} = {kind = "aof-noise", dir = dir_path}
    local gaps_ms: {number} = {}
    for i = 2, #ns_list do
        -- signed ns delta: the u64 envelope stamps can DECREASE across
        -- the rolled files (no b>a filter here, mirroring the twin),
        -- so compute the magnitude on cdata and carry the sign
        local a, b = ns_list[i - 1], ns_list[i]
        local d: any
        local sign = 1.0
        if b >= a then
            d = b - a
        else
            d = a - b
            sign = -1.0
        end
        gaps_ms[#gaps_ms + 1] = tonumber(d) / 1e6 * sign
    end
    if #gaps_ms > 0 then
        local st = stats(gaps_ms)
        row.gaps = st
        local thr = math.min(hb_ms, 5 * st.p50_ms)
        local stalls: {{string: any}} = {}
        for i, g in ipairs(gaps_ms) do
            if g > thr then
                stalls[#stalls + 1] = {a_ns = ns_list[i], b_ns = ns_list[i + 1],
                                       gap_ms = g, _o = {"a_ns", "b_ns", "gap_ms"}}
            end
        end
        row.stalls = stalls
    end
    if #dts > 0 then
        row.samples = stats(dts)
    end
    return {row}
end

local function export_to(out_path: string, series_dir: string, kinds: string,
                         anchors: any, hb_ms: any): number
    local events = export_series(series_dir, kinds, anchors, hb_ms)
    local handle = io.open(out_path, "w")
    if handle == nil then
        error("cannot write " .. out_path)
    end
    for _, e in ipairs(events) do
        handle:write(ev_json(e) .. "\n")
    end
    handle:close()
    return #events
end

local function main(argv: {string}): number
    -- Usage parity with the python twin: --lib is accepted (the cdylib
    -- is loaded at require time), --json, --kind KIND (repeatable AND
    -- space-joined, union semantics — one contract with the python
    -- twin), repeatable --anchor EPOCHMS, --hb-ms N, FILE_OR_DIR
    -- [OUT.jsonl].
    local follow_json = false
    local anchors: {number} = {}
    local hb_ms = 5.0
    local paths: {string} = {}
    local kind_parts: {string} = {}
    local i = 2
    while i <= #argv do
        local a = argv[i]
        if a == "--lib" then
            i = i + 2
        elseif a == "--json" then
            follow_json = true
            i = i + 1
        elseif a == "--kind" then
            for token in argv[i + 1]:gmatch("%S+") do
                if token == "all" then
                    kind_parts[#kind_parts + 1] = "locks"
                    kind_parts[#kind_parts + 1] = "reconfig"
                    kind_parts[#kind_parts + 1] = "phi"
                    kind_parts[#kind_parts + 1] = "phi-samples"
                elseif token == "both" then
                    kind_parts[#kind_parts + 1] = "locks"
                    kind_parts[#kind_parts + 1] = "reconfig"
                else
                    kind_parts[#kind_parts + 1] = token
                end
            end
            i = i + 2
        elseif a == "--anchor" then
            anchors[#anchors + 1] = tonumber(argv[i + 1])
            i = i + 2
        elseif a == "--hb-ms" then
            hb_ms = tonumber(argv[i + 1])
            i = i + 2
        else
            paths[#paths + 1] = a
            i = i + 1
        end
    end
    local export_kinds = #kind_parts > 0 and table.concat(kind_parts, " ") or "all"
    if #paths < 1 then
        print("usage: aof-trace-tool.tl [--lib PATH] [--json] "
            .. "[--kind KIND ...] [--anchor EPOCHMS ...] [--hb-ms N] "
            .. "FILE_OR_DIR [OUT.jsonl]")
        return 2
    end
    local export_out: (string | nil) = nil
    if #paths > 1 then
        local second = paths[2]
        local second_exists = is_dir(second)
        if not second_exists then
            local fh = io.open(second, "r")
            if fh ~= nil then
                second_exists = true
                fh:close()
            end
        end
        if not second_exists then
            export_out = second
            paths = {paths[1]}
        end
    end
    if export_out ~= nil then
        table.sort(anchors)
        for _, a in ipairs(anchors) do
            print("anchor " .. fmt_ts_ms(a))
        end
        local n = export_to(export_out, paths[1], export_kinds,
                            #anchors > 0 and anchors or nil, hb_ms)
        print(("exported %d events to %s"):format(n, export_out))
        return 0
    end
    for _, p in ipairs(paths) do
        if is_dir(p) then
            print(summarize_series_json(p))
        else
            print(summarize_one_json(p))
        end
    end
    return 0
end

return {
    iterate = nil,
    ns_of = ns_of,
    parse_wire = parse_wire,
    split_trailer = split_trailer,
    lock_fields = lock_fields,
    summarize = summarize_one,
    summarize_series = summarize_series,
    summarize_series_json = summarize_series_json,
    export_series = export_series,
    export_to = export_to,
    fixture_dir = fixture_dir,
    open_writer = open_writer,
    append = append,
    close_writer = close_writer,
    is_dir = is_dir,
    main = main,
}
