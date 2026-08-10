local bit = require("bit")
local telemetry_log = require("telemetry_log")
local tested = require("tested")

local function shell_ok(cmd: string): boolean
    local res: any = os.execute(cmd)
    if type(res) == "boolean" then
        return res as boolean
    end
    return (res as number) == 0
end

local function read_file(path: string): string
    local f = io.open(path, "rb")
    if f == nil then
        return ""
    end
    local data = f:read("*a")
    f:close()
    return data or ""
end

local function list_segments(dir: string): {string}
    local names: {string} = {}
    local p = io.popen("ls " .. dir .. " 2>/dev/null")
    if p ~= nil then
        for line in (p:read("*a") or ""):gmatch("[^\n]+") do
            if line:match("%.bin$") ~= nil then
                table.insert(names, line)
            end
        end
        p:close()
    end
    table.sort(names)
    return names
end

-- Independent decoder: hand-rolled here, sharing no codec code with
-- src/telemetry_log.tl, so a bug on one side cannot mask itself.
local function u16(d: string, p: integer): integer
    return d:byte(p) * 256 + d:byte(p + 1)
end

local function u32(d: string, p: integer): number
    return ((d:byte(p) * 256 + d:byte(p + 1)) * 256 + d:byte(p + 2)) * 256 + d:byte(
        p + 3
    )
end

local function u64(d: string, p: integer): number
    return u32(d, p) * 4294967296 + u32(d, p + 4)
end

-- Independent CRC-32C: per-bit loop (not table-driven), same reflected
-- Castagnoli polynomial 0x82F63B78. Returns unsigned.
local function crc32c_check(data: string): number
    local c = bit.tobit(0xFFFFFFFF)
    for i = 1, #data do
        c = bit.bxor(c, data:byte(i))
        for _ = 1, 8 do
            if bit.band(c, 1) ~= 0 then
                c = bit.bxor(bit.rshift(c, 1), 0x82F63B78)
            else
                c = bit.rshift(c, 1)
            end
        end
    end
    local u = bit.bxor(c, bit.tobit(0xFFFFFFFF))
    if u < 0 then
        u = u + 4294967296
    end
    return u
end

local record Rec
    kind: integer
    ts_ms: number
    lock_id: number
    fencing_token: number
    renew_count: integer
    held_gauge: integer
    name: string
end

-- Decodes one segment; returns the records, or nil + error on any format or
-- CRC violation (the CRC check is what the corruption test relies on).
local function decode_segment(data: string): ({Rec}, string)
    if #data < 24 or data:sub(1, 8) ~= "LLOCKTEL" then
        return nil, "bad header"
    end
    if u16(data, 9) ~= 1 then
        return nil, "bad version"
    end
    if data:sub(15, 24) ~= string.rep("\0", 10) then
        return nil, "reserved bytes not zero"
    end
    local records: {Rec} = {}
    local p = 25
    while p <= #data do
        if data:byte(p) ~= 0x4C then
            return nil, "bad record_magic at " .. tostring(p)
        end
        local len = u16(data, p + 1)
        if len < 37 or p + len - 1 > #data then
            return nil, "bad record_len at " .. tostring(p)
        end
        local body = data:sub(p, p + len - 5)
        local want_crc = u32(data, p + len - 4)
        if crc32c_check(body) ~= want_crc then
            return nil, "crc mismatch at " .. tostring(p)
        end
        local name_len = data:byte(p + 32)
        table.insert(
            records,
            {
                kind = data:byte(p + 3),
                ts_ms = u64(data, p + 4),
                lock_id = u32(data, p + 12),
                fencing_token = u64(data, p + 16),
                renew_count = u32(data, p + 24) as integer,
                held_gauge = u32(data, p + 28) as integer,
                name = data:sub(p + 33, p + 32 + name_len),
            }
        )
        p = p + len
    end
    return records, ""
end

local K = telemetry_log.KINDS

shell_ok("mkdir -p .tmp")

tested.test(
    "telemetry_log round-trips every record field through the binary segment",
    function()
        local dir = ".tmp/telemetry-test-roundtrip"
        shell_ok("rm -rf " .. dir .. " && mkdir -p " .. dir)
        local w = telemetry_log.new(dir, "n0", 42, 65536)
        local long_name = string.rep("x", 128)
        w:record(K.acquire, 1700000000123, 7, 99, 0, 1, "alpha")
        w:record(K.renew, 1700000000456, 7, 99, 3, 1, nil)
        w:record(K.release, 1700000000789, 7, 99, 3, 0, long_name)
        w:record(K.deny, 9007199254740991, 4294967295, 9007199254740000, 0, 0, "edge")
        w:close()
        local names = list_segments(dir)
        local records: {Rec} = {}
        local err = ""
        if #names == 1 then
            records, err = decode_segment(read_file(dir .. "/" .. names[1]))
        end
        local r = records[4]
        local actual = tostring(#names)
            .. "|"
            .. tostring(#records)
            .. "|"
            .. tostring(records[1].kind)
            .. ","
            .. string.format("%.0f", records[1].ts_ms)
            .. ","
            .. tostring(records[1].lock_id)
            .. ","
            .. string.format("%.0f", records[1].fencing_token)
            .. ","
            .. tostring(records[1].renew_count)
            .. ","
            .. tostring(records[1].held_gauge)
            .. ","
            .. records[1].name
            .. "|"
            .. tostring(records[2].kind)
            .. ","
            .. tostring(records[2].renew_count)
            .. ","
            .. tostring(#records[2].name)
            .. "|"
            .. tostring(#records[3].name)
            .. "|"
            .. string.format("%.0f", r.ts_ms)
            .. ","
            .. tostring(r.lock_id)
            .. ","
            .. string.format("%.0f", r.fencing_token)
            .. "|"
            .. err
        tested.assert(
            {
                given = "four records (incl. nil name, 128-byte name, and 2^53-edge u64 values)",
                should = "decode byte-for-byte with every field intact and CRCs valid",
                expected = "1|4|1,1700000000123,7,99,0,1,alpha|2,3,0|128|9007199254740991,4294967295,9007199254740000|",
                actual = actual,
            }
        )
    end
)

tested.test(
    "telemetry_log rotates at min-size with tmp+rename naming and >=min_bytes segments",
    function()
        local dir = ".tmp/telemetry-test-rotate"
        shell_ok("rm -rf " .. dir .. " && mkdir -p " .. dir)
        local min_bytes = 256
        local w = telemetry_log.new(dir, "n1", 1, min_bytes)
        for i = 1, 60 do
            w:record(
                K.acquire, 1700000000000 + i, i, i * 10, 0, 1, "lock-" .. tostring(i)
            )
        end
        w:close()
        local names = list_segments(dir)
        local pattern_ok = #names >= 3
        local size_ok = #names >= 3
        local prev_seq = 0
        for i, n in ipairs(names) do
            local node, seq = n:match("^telemetry%-(n1)%-%d+%-(%d%d%d%d%d%d)%.bin$")
            if node == nil then
                pattern_ok = false
            else
                local s = tonumber(seq) as integer
                if s ~= prev_seq + 1 then
                    pattern_ok = false
                end
                prev_seq = s
            end
            if i < #names and #read_file(dir .. "/" .. n) < min_bytes then
                size_ok = false
            end
        end
        local tmp_left = shell_ok("ls " .. dir .. "/*.tmp >/dev/null 2>&1")
        tested.assert(
            {
                given = "60 records with min_bytes=256",
                should = "produce >=3 sequentially-named .bin segments, each non-final >= min_bytes, no .tmp left",
                expected = "true|true|true|false",
                actual = tostring(pattern_ok)
                    .. "|"
                    .. tostring(size_ok)
                    .. "|"
                    .. tostring(#names >= 3)
                    .. "|"
                    .. tostring(tmp_left),
            }
        )
    end
)

tested.test(
    "telemetry_log CRC-32C trailer lets an independent decoder detect corruption",
    function()
        local dir = ".tmp/telemetry-test-crc"
        shell_ok("rm -rf " .. dir .. " && mkdir -p " .. dir)
        local w = telemetry_log.new(dir, "n2", 3, 65536)
        w:record(K.cas, 1700000001000, 5, 42, 0, 1, "victim")
        w:close()
        local name = list_segments(dir)[1]
        local path = dir .. "/" .. name
        local data = read_file(path)
        local mid = 24 + 10 -- inside the first record's u64 fields
        local corrupted = data:sub(1, mid - 1)
            .. string.char((data:byte(mid) + 1) % 256)
            .. data:sub(mid + 1)
        local f = io.open(path, "wb")
        local wrote = false
        if f ~= nil then
            f:write(corrupted)
            f:close()
            wrote = true
        end
        local _, err = decode_segment(read_file(path))
        tested.assert(
            {
                given = "a segment with one byte flipped mid-record",
                should = "be rejected by the independent decoder with a crc mismatch",
                expected = "true|crc mismatch",
                actual = tostring(wrote) .. "|" .. err:sub(1, 12),
            }
        )
    end
)

tested.test(
    "telemetry_log warns once and permanently disables on an unwritable target",
    function()
        local blocker = ".tmp/telemetry-test-blocker"
        shell_ok("rm -rf " .. blocker .. " && touch " .. blocker)
        local w = telemetry_log.new(blocker, "n3", 4, 256)
        w:record(K.acquire, 1700000002000, 1, 1, 0, 1, "x")
        local after_first = w.disabled
        w:record(K.renew, 1700000002001, 1, 1, 1, 1, "x")
        w:close()
        tested.assert(
            {
                given = "a writer pointed at a path that is a file, not a directory",
                should = "disable on first record, no-op afterwards, never raise",
                expected = "true|true|true",
                actual = tostring(after_first)
                    .. "|"
                    .. tostring(w.disabled)
                    .. "|"
                    .. tostring(#list_segments(blocker) == 0),
            }
        )
    end
)

tested.test(
    "telemetry_log emits the deterministic fixture segment set for the OpenResty reader",
    function()
        local dir = ".tmp/telemetry-fixture"
        shell_ok("rm -rf " .. dir .. " && mkdir -p " .. dir)
        local w = telemetry_log.new(dir, "t0", 1000, 512)
        local kinds = {K.acquire, K.renew, K.release, K.cas, K.expire, K.break_, K.deny}
        local n = 0
        for _ = 1, 3 do
            for _, kind in ipairs(kinds) do
                n = n + 1
                w:record(
                    kind,
                    1700000000000 + n * 1000,
                    100 + n,
                    5000 + n,
                    n % 4,
                    (n % 5) + 1,
                    "fixture-lock-" .. tostring(n)
                )
            end
        end
        w:record(K.release, 1700000099999, 999, 9999, 0, 0, nil)
        w:close()
        local names = list_segments(dir)
        local total = 0
        local ok = #names >= 1
        for _, name in ipairs(names) do
            local records, err = decode_segment(read_file(dir .. "/" .. name))
            if records == nil then
                ok = false
            else
                total = total + #records + #err * 0
            end
        end
        tested.assert(
            {
                given = "the .tmp/telemetry-fixture segment set (node t0, 3 records x 7 kinds + nil-name)",
                should = "be present and decode cleanly for item26 to copy",
                expected = "true|22|true",
                actual = tostring(ok)
                    .. "|"
                    .. tostring(total)
                    .. "|"
                    .. tostring(
                        names[1] ~= nil and names[1]:match("^telemetry%-t0%-") ~= nil
                    ),
            }
        )
    end
)

return tested
