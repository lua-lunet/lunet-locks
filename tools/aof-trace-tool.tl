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
local function json_num(json: string, key: string): any
    local raw = json:match('"' .. key .. '":%s*(-?%d+%.?%d*)')
    if raw ~= nil then
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

local function ev_json(e: {string: any}): string
    local parts: {string} = {}
    local order: {string} = {"kind", "aof_ns", "leader_ms", "slot", "result",
                             "op", "lock_id", "message_id", "client_id",
                             "request_num", "command", "phi", "now_ms",
                             "prev_wait_ms", "next_wait_ms", "mean_interval_ms",
                             "min_ms", "max_ms", "node", "era", "leader",
                             "view", "addr", "dt_ms", "ts_ms"}
    for _, key in ipairs(order) do
        local v = e[key]
        if v ~= nil then
            if v == JSON_NULL then
                parts[#parts + 1] = ('"%s": null'):format(key)
            elseif v == true or v == false then
                parts[#parts + 1] = ('"%s": %s'):format(key, tostring(v))
            elseif type(v) == "string" then
                parts[#parts + 1] = ('"%s": "%s"'):format(key, json_escape(v))
            elseif ffi.istype("uint64_t", v) then
                -- exact past 2^53: the cdata formats itself, no ULL suffix
                parts[#parts + 1] = ('"%s": %s'):format(key, string.format("%d", v))
            else
                parts[#parts + 1] = ('"%s": %s'):format(key, tostring(v))
            end
        end
    end
    return "{" .. table.concat(parts, ", ") .. "}"
end

local function kind_wanted(kinds: string, kind: string): boolean
    if kinds == "all" then
        return true
    end
    if kinds == "both" then
        return kind == "locks" or kind == "reconfig"
    end
    return kinds == kind
end

local function export_series(dir_path: string, kinds: string): {{string: any}}
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
                                        slot = s, result = "committed",
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
                                    slot = s, result = "committed",
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
                                        slot = s, result = "committed",
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
                                    slot = s, result = "committed",
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
    return events
end

local function export_to(out_path: string, series_dir: string, kinds: string): number
    local events = export_series(series_dir, kinds)
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
