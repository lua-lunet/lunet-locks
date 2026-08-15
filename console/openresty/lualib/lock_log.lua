-- Telemetry binary segment reader (docs/telemetry-binary-log.md).
--
-- Plain LuaJIT companion to src/telemetry_log.tl (the writer). No `ngx`
-- APIs, so it is unit-testable with the system luajit; inside OpenResty it
-- loads as an ordinary lualib module. All multi-byte integers are
-- big-endian; LuaJIT has no string.pack, so decoding uses string.byte
-- arithmetic, with u64 as hi * 2^32 + lo (exact under 2^53). CRC-32C
-- (Castagnoli, reflected, poly 0x82F63B78) is table-driven and matches the
-- writer bit-for-bit.
--
-- Tolerant reader semantics: telemetry, not a WAL. A bad header magic
-- yields an empty result plus a warning; records are walked by record_len;
-- on a CRC mismatch or implausible record_len the reader resyncs by
-- scanning forward for the next 0x4C byte that begins a valid record; a
-- truncated final record (torn tail) is normal and stops the walk quietly.
--
-- Directory listing is isolated in ONE injectable function
-- (`lock_log._listdir`, overridable per call) so unit tests can substitute
-- a fake lister; the default shells out to `ls -1` because luafilesystem
-- is not guaranteed to be installed.

local bit = require("bit")

local RECORD_MAGIC = 0x4C
local VERSION = 2
local HEADER_SIZE = 24
-- Fixed envelope: everything except the variable-length name and labels
-- payloads: magic(1)+len(2)+kind(1)+ts(8)+lock(4)+token(8)+renew(4)
-- +held(4)+expiry(8)+holder(16)+name_len(1)+labels_len(2)+crc(4).
local RECORD_FIXED_SIZE = 63
-- Byte offset (1-based, relative to record start) of name_len.
local NAME_LEN_OFF = 57

-- CRC-32C table (reflected Castagnoli). Identical constants to the writer.
local crc_table = {}
for i = 0, 255 do
    local c = i
    for _ = 1, 8 do
        if bit.band(c, 1) ~= 0 then
            c = bit.bxor(bit.rshift(c, 1), 0x82F63B78)
        else
            c = bit.rshift(c, 1)
        end
    end
    crc_table[i] = c
end

local function crc32c(data)
    local c = bit.tobit(0xFFFFFFFF)
    for i = 1, #data do
        c = bit.bxor(
            bit.rshift(c, 8), crc_table[bit.band(bit.bxor(c, data:byte(i)), 0xFF)]
        )
    end
    local u = bit.bxor(c, bit.tobit(0xFFFFFFFF))
    if u < 0 then
        u = u + 4294967296
    end
    return u
end

-- Big-endian decoders over string.byte arithmetic.
local function u16be(d, p)
    local b1, b2 = d:byte(p, p + 1)
    return b1 * 256 + b2
end

local function u32be(d, p)
    local b1, b2, b3, b4 = d:byte(p, p + 3)
    return ((b1 * 256 + b2) * 256 + b3) * 256 + b4
end

local function u64be(d, p)
    return u32be(d, p) * 4294967296 + u32be(d, p + 4)
end

local lock_log = {}

lock_log.VERSION = VERSION
lock_log.HEADER_SIZE = HEADER_SIZE
lock_log.crc32c = crc32c

-- Default directory lister: returns an array of bare file names in `dir`.
-- Injectable: pass `listdir` to list_segments/scan or replace
-- lock_log._listdir in tests.
function lock_log._listdir(dir)
    local fh = io.popen("ls -1 " .. dir .. " 2>/dev/null", "r")
    if fh == nil then
        return {}
    end
    local names = {}
    for line in fh:lines() do
        names[#names + 1] = line
    end
    fh:close()
    return names
end

-- Segment name: telemetry-<nodeName>-<epochMsAtOpen>-<seq6>.bin
-- The node name may itself contain dashes; the two trailing numeric groups
-- anchor the parse. *.tmp files and non-matching names are ignored.
local function parse_segment_name(name)
    if name:match("%.tmp$") then
        return nil
    end
    local node, opened_ms, seq = name:match("^telemetry%-(.-)%-(%d+)%-(%d%d%d%d%d%d)%.bin$")
    if node == nil then
        return nil
    end
    return node, tonumber(opened_ms), tonumber(seq)
end

-- lock_log.list_segments(dir [, listdir]) → array of
-- { path=, node=, opened_ms=, seq=, size= } sorted by (opened_ms, seq).
function lock_log.list_segments(dir, listdir)
    local ls = listdir or lock_log._listdir
    local segs = {}
    for _, name in ipairs(ls(dir)) do
        local node, opened_ms, seq = parse_segment_name(name)
        if node ~= nil then
            local path = dir .. "/" .. name
            local size = 0
            local fh = io.open(path, "rb")
            if fh ~= nil then
                size = fh:seek("end") or 0
                fh:close()
            end
            segs[#segs + 1] = {
                path = path,
                node = node,
                opened_ms = opened_ms,
                seq = seq,
                size = size,
            }
        end
    end
    table.sort(segs, function(a, b)
        if a.opened_ms ~= b.opened_ms then
            return a.opened_ms < b.opened_ms
        end
        return a.seq < b.seq
    end)
    return segs
end

-- Formats 16 raw bytes as a canonical dashed UUID string; returns nil for
-- the all-zero "no holder" encoding.
local function holder_string(bytes)
    if bytes == string.rep("\0", 16) then
        return nil
    end
    local hex = (bytes:gsub(".", function(c)
        return string.format("%02x", c:byte())
    end))
    return hex:sub(1, 8)
        .. "-"
        .. hex:sub(9, 12)
        .. "-"
        .. hex:sub(13, 16)
        .. "-"
        .. hex:sub(17, 20)
        .. "-"
        .. hex:sub(21, 32)
end

-- Splits a labels CSV payload into an array (empty payload → empty array).
-- Labels are CSV-safe by grammar (lowercase/digit/hyphen, no commas), so a
-- plain split is exact.
local function labels_array(csv)
    local out = {}
    if csv == "" then
        return out
    end
    for label in csv:gmatch("[^,]+") do
        out[#out + 1] = label
    end
    return out
end

-- Attempts to decode one record at byte offset `pos` (1-based) in `data`.
-- Returns record, next_pos on success; nil, "torn" when the record is
-- truncated by end-of-data; nil, "corrupt" on any validity failure.
local function try_record(data, pos)
    if pos > #data or data:byte(pos) ~= RECORD_MAGIC then
        return nil, "corrupt"
    end
    if pos + 2 > #data then
        return nil, "torn"
    end
    local record_len = u16be(data, pos + 1)
    if record_len < RECORD_FIXED_SIZE or record_len > 65535 then
        return nil, "corrupt"
    end
    local name_len_off = pos + NAME_LEN_OFF - 1
    if name_len_off > #data then
        return nil, "torn"
    end
    local name_len = data:byte(name_len_off)
    local labels_len_off = name_len_off + name_len + 1
    if labels_len_off + 1 > #data then
        return nil, "torn"
    end
    local labels_len = u16be(data, labels_len_off)
    if record_len ~= RECORD_FIXED_SIZE + name_len + labels_len then
        return nil, "corrupt"
    end
    local rec_end = pos + record_len - 1
    if rec_end > #data then
        return nil, "torn"
    end
    local body = data:sub(pos, rec_end - 4)
    local stored_crc = u32be(data, rec_end - 3)
    if crc32c(body) ~= stored_crc then
        return nil, "corrupt"
    end
    local record = {
        kind = data:byte(pos + 3),
        ts_ms = u64be(data, pos + 4),
        lock_id = u32be(data, pos + 12),
        fencing_token = u64be(data, pos + 16),
        renew_count = u32be(data, pos + 24),
        held_gauge = u32be(data, pos + 28),
        expiry_ms = u64be(data, pos + 32),
        holder = holder_string(data:sub(pos + 40, pos + 55)),
        name = data:sub(pos + NAME_LEN_OFF, pos + NAME_LEN_OFF + name_len - 1),
        labels = labels_array(data:sub(labels_len_off + 2, rec_end - 4)),
    }
    return record, rec_end + 1
end

-- lock_log.read_segment(path [, offset]) → header, records, warnings,
-- consumed, status.
--
-- offset is the absolute byte count already consumed by a previous read of
-- the same file (nil/0 = full read, header validated). With offset > 0 the
-- file is read from that point and the header is not re-checked — sealed
-- prefixes are immutable, so only the appended tail needs decoding.
-- consumed is the absolute byte count validly decoded after this call
-- (header + complete records; a torn tail does not advance it). status is
-- "ok", or "resume_corrupt" when the byte at offset does not begin a valid
-- record — the file was rewritten in place and the caller must fall back
-- to a clean full rescan.
function lock_log.read_segment(path, offset)
    offset = offset or 0
    local warnings = {}
    local header = nil
    local records = {}
    local fh, err = io.open(path, "rb")
    if fh == nil then
        warnings[#warnings + 1] = "cannot open " .. path .. ": " .. tostring(err)
        return header, records, warnings, offset, "ok"
    end
    if offset > 0 then
        fh:seek("set", offset)
    end
    local data = fh:read("*a")
    fh:close()
    local pos
    if offset == 0 then
        if #data < HEADER_SIZE or data:sub(1, 8) ~= "LLOCKTEL" then
            warnings[#warnings + 1] = path .. ": bad or missing header magic; segment ignored"
            return header, records, warnings, 0, "ok"
        end
        header = {
            version = u16be(data, 9),
            node_id = u32be(data, 11),
        }
        if header.version ~= VERSION then
            -- Pre-release format: no v1 compatibility. Reject the segment.
            warnings[#warnings + 1] = path
                .. ": unsupported version "
                .. header.version
                .. " (expected "
                .. VERSION
                .. "); segment ignored"
            return header, records, warnings, 0, "ok"
        end
        pos = HEADER_SIZE + 1
    else
        pos = 1
    end
    local first = true
    while pos <= #data do
        local record, next_or_why = try_record(data, pos)
        if record ~= nil then
            records[#records + 1] = record
            pos = next_or_why
            first = false
        elseif next_or_why == "torn" then
            -- Truncated final record (torn tail) is normal: stop quietly.
            break
        else
            if offset > 0 and first then
                -- The resume point is not a record boundary: the file was
                -- rewritten in place, so the retained prefix is suspect.
                warnings[#warnings + 1] = path
                    .. ": no valid record at resume offset "
                    .. offset
                    .. "; segment needs a full re-read"
                return header, records, warnings, offset, "resume_corrupt"
            end
            warnings[#warnings + 1] = string.format(
                "%s: corrupt record at offset %d; resyncing", path, offset + pos - 1
            )
            -- Scan forward for the next 0x4C that begins a valid record.
            local resumed = false
            local scan = pos + 1
            while scan <= #data do
                if data:byte(scan) == RECORD_MAGIC then
                    local r, np = try_record(data, scan)
                    if r ~= nil then
                        records[#records + 1] = r
                        pos = np
                        resumed = true
                        break
                    elseif np == "torn" then
                        -- 0x4C near the end with an incomplete record: torn tail.
                        return header, records, warnings, offset + scan - 1, "ok"
                    end
                end
                scan = scan + 1
            end
            if not resumed then
                break
            end
            first = false
        end
    end
    return header, records, warnings, offset + pos - 1, "ok"
end

-- lock_log.scan(dir [, listdir]) → records concatenated across segments in
-- (opened_ms, seq) order, plus the aggregate warnings array.
function lock_log.scan(dir, listdir)
    local all = {}
    local warnings = {}
    for _, seg in ipairs(lock_log.list_segments(dir, listdir)) do
        local _, records, w = lock_log.read_segment(seg.path)
        for _, r in ipairs(records) do
            all[#all + 1] = r
        end
        for _, msg in ipairs(w) do
            warnings[#warnings + 1] = msg
        end
    end
    return all, warnings
end

return lock_log
