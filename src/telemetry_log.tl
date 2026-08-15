-- Telemetry binary segment writer (docs/telemetry-binary-log.md).
--
-- Pure, dependency-free module (no `require("lunet")`) that appends compact
-- binary counter records to local segment files for the admin console's
-- co-located nginx edge to read directly off disk. All multi-byte integers
-- are big-endian. Runtime target is LuaJIT/Lua 5.1, so there is no
-- `string.pack` here: the byte codec is hand-rolled, and CRC-32C
-- (Castagnoli, reflected, poly 0x82F63B78) is table-driven over the LuaJIT
-- `bit` library declared in `src/bit.d.tl`.
--
-- Failure policy: this is telemetry, not a WAL. Any I/O error (including
-- disk full) writes exactly one warning to stderr and permanently disables
-- the writer; every public method then becomes a cheap no-op. The module
-- never raises across the service boundary.

local bit = require("bit")

-- One record's worth of fields (v2). expiry_ms/holder16/name/labels_csv
-- are optional and default to 0 / sixteen zero bytes / nil / "".
local record RecordArgs
    kind: integer
    ts_ms: number
    lock_id: number
    fencing_token: number
    renew_count: integer
    held_gauge: integer
    expiry_ms: number
    holder16: string
    name: string
    labels_csv: string
end

local record Writer
    dir: string
    node_name: string
    node_id: integer
    min_bytes: integer
    disabled: boolean
    warned: boolean
    seq: integer
    size: integer
    fh: FILE

    record: function(Writer, RecordArgs)
    close: function(Writer)
end

local Writer_mt: metatable<Writer> = {__index = Writer}

local RECORD_MAGIC = 0x4C
local VERSION = 2
local HEADER_SIZE = 24
-- Everything except the variable-length name and labels payload:
-- magic(1)+len(2)+kind(1)+ts(8)+lock(4)+token(8)+renew(4)+held(4)
-- +expiry(8)+holder(16)+name_len(1)+labels_len(2)+crc(4).
local RECORD_FIXED_SIZE = 63
local ZERO_HOLDER = string.rep("\0", 16)

local KINDS: {string: integer} = {
    acquire = 1,
    renew = 2,
    release = 3,
    cas = 4,
    expire = 5,
    break_ = 6,
    deny = 7,
}

-- Hand-rolled big-endian byte codec. u32/u64 inputs are Lua numbers
-- (doubles, exact to 2^53), so plain arithmetic avoids the signed-32-bit
-- semantics of the bit library.
local function u8(v: integer): string
    return string.char(v % 256)
end

local function u16be(v: integer): string
    return string.char(math.floor(v / 256) % 256, v % 256)
end

local function u32be(v: number): string
    local b4 = v % 256
    return string.char(
        math.floor(v / 16777216) % 256,
        math.floor(v / 65536) % 256,
        math.floor(v / 256) % 256,
        b4 as integer
    )
end

local function u64be(v: number): string
    local hi = math.floor(v / 4294967296)
    local lo = v % 4294967296
    return u32be(hi) .. u32be(lo)
end

-- CRC-32C (Castagnoli), reflected, polynomial 0x82F63B78. Table-driven;
-- bit-library intermediates are signed 32-bit but bitwise-correct.
local crc_table: {integer} = {}
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

-- Returns the CRC as an unsigned Lua number in [0, 2^32).
local function crc32c(data: string): number
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

local function header(node_id: integer): string
    return "LLOCKTEL" .. u16be(VERSION) .. u32be(node_id) .. string.rep("\0", 10)
end

-- Lua 5.1 file handles have no fsync; use it when the runtime provides one
-- (e.g. an FFI-extended handle), otherwise fall back to flush. Best-effort:
-- reports failure so the caller can disable the writer.
local record Syncable
    fsync: function(Syncable)
end

local function sync_file(fh: FILE): boolean
    local ok = pcall(
        function(): boolean
            local res: any = fh:flush()
            if res == nil or res == false then
                error("flush failed", 0)
            end
            local f = fh as any as Syncable
            if f.fsync ~= nil then
                f:fsync()
            end
            return true
        end
    )
    return ok
end

local function segment_name(node_name: string, epoch_ms: integer, seq: integer): string
    return string.format("telemetry-%s-%d-%06d.bin", node_name, epoch_ms, seq)
end

-- One warning per writer lifetime, then permanent disable. Never raises.
function Writer:fail(msg: string)
    if not self.warned then
        self.warned = true
        io.stderr:write("telemetry_log: " .. msg .. "; writer permanently disabled\n")
    end
    self.disabled = true
    if self.fh ~= nil then
        pcall(function() self.fh:close() end)
        self.fh = nil as FILE
    end
end

-- Opens the next segment atomically: write `<name>.bin.tmp`, fsync, rename
-- into place, fsync the header. Returns false (and disables) on any error.
function Writer:open_segment(): boolean
    self.seq = self.seq + 1
    local name = segment_name(self.node_name, os.time() * 1000, self.seq)
    local tmp_path = self.dir .. "/" .. name .. ".tmp"
    local final_path = self.dir .. "/" .. name
    local fh, err = io.open(tmp_path, "wb")
    if fh == nil then
        self:fail("open " .. tmp_path .. ": " .. tostring(err))
        return false
    end
    local ok, werr = fh:write(header(self.node_id))
    if ok == nil then
        pcall(function() fh:close() end)
        self:fail("write header " .. tmp_path .. ": " .. tostring(werr))
        return false
    end
    if not sync_file(fh) then
        pcall(function() fh:close() end)
        self:fail("fsync header " .. tmp_path)
        return false
    end
    local rok, rerr = os.rename(tmp_path, final_path)
    if not rok then
        pcall(function() fh:close() end)
        self:fail(
            "rename " .. tmp_path .. " -> " .. final_path .. ": " .. tostring(rerr)
        )
        return false
    end
    self.fh = fh
    self.size = HEADER_SIZE
    return true
end

function Writer.new(
    dir: string, node_name: string, node_id: integer, min_bytes: integer
): Writer
    local self: Writer = setmetatable({} as Writer, Writer_mt)
    self.dir = dir
    self.node_name = node_name
    self.node_id = node_id
    self.min_bytes = min_bytes
    self.disabled = false
    self.warned = false
    self.seq = 0
    self.size = 0
    self.fh = nil as FILE
    return self
end

-- Appends one record. `args.name` may be nil (name_len 0); `args.holder16`
-- is normalized to exactly 16 bytes (zero-padded/truncated, nil → all-zero).
-- Min-size rotation: once the open segment has reached min_bytes, the NEXT
-- record rotates it; a segment is never smaller than min_bytes unless the
-- writer closes early.
function Writer:record(args: RecordArgs)
    if self.disabled then
        return
    end
    if self.fh ~= nil and self.size >= self.min_bytes then
        if not sync_file(self.fh) then
            self:fail("fsync segment on rotate")
            return
        end
        pcall(function() self.fh:close() end)
        self.fh = nil as FILE
    end
    if self.fh == nil then
        if not self:open_segment() then
            return
        end
    end
    local nm = args.name or ""
    if #nm > 128 then
        nm = nm:sub(1, 128)
    end
    local labels = args.labels_csv or ""
    local holder = args.holder16 or ZERO_HOLDER
    if #holder < 16 then
        holder = holder .. string.rep("\0", 16 - #holder)
    elseif #holder > 16 then
        holder = holder:sub(1, 16)
    end
    local record_len = RECORD_FIXED_SIZE + #nm + #labels
    local body = u8(RECORD_MAGIC)
        .. u16be(record_len)
        .. u8(args.kind)
        .. u64be(args.ts_ms)
        .. u32be(args.lock_id)
        .. u64be(args.fencing_token)
        .. u32be(args.renew_count)
        .. u32be(args.held_gauge)
        .. u64be(args.expiry_ms or 0)
        .. holder
        .. u8(#nm)
        .. nm
        .. u16be(#labels)
        .. labels
    local bytes = body .. u32be(crc32c(body))
    local ok, werr = self.fh:write(bytes)
    if ok == nil then
        self:fail("write record: " .. tostring(werr))
        return
    end
    self.size = self.size + record_len
end

-- Flushes, fsyncs (best-effort), and closes. No-op when disabled.
function Writer:close()
    if self.disabled then
        return
    end
    if self.fh ~= nil then
        sync_file(self.fh)
        pcall(function() self.fh:close() end)
        self.fh = nil as FILE
    end
    self.disabled = true
end

return {
    Writer = Writer,
    new = Writer.new,
    RecordArgs = RecordArgs,
    KINDS = KINDS,
    crc32c = crc32c,
}
