-- Unit test for console/openresty/lualib/lock_log.lua (plain luajit, no
-- framework: assert + exit code). Run from anywhere; paths are resolved
-- relative to this script, and FIXTURES can override the fixture dir.
--
--   luajit console/tests/lock_log_test.lua
--   FIXTURES=/path/to/telemetry luajit console/tests/lock_log_test.lua

local script_dir = arg[0]:match("^(.*)/[^/]*$") or "."
package.path = script_dir .. "/../openresty/lualib/?.lua;" .. package.path

local lock_log = require("lock_log")

local FIXTURES = os.getenv("FIXTURES") or (script_dir .. "/fixtures/telemetry")
local TMP = os.getenv("LOCK_LOG_TEST_TMP") or (script_dir .. "/../.tmp-lock-log-test")

local failures = 0
local function check(cond, msg)
    if cond then
        print("ok - " .. msg)
    else
        failures = failures + 1
        print("FAIL - " .. msg)
    end
end

local function shell(cmd)
    local ok = os.execute(cmd)
    -- Lua 5.1 returns true/status, LuaJIT 5.1 semantics: 0 on success.
    return ok == true or ok == 0
end

-- ---- Fixture round-trip ---------------------------------------------------

local segs = lock_log.list_segments(FIXTURES)
check(#segs == 4, "fixture has 4 segments (got " .. #segs .. ")")
check(
    segs[1] ~= nil
        and segs[1].node == "t0"
        and segs[1].opened_ms == 1786405137000
        and segs[1].seq == 1,
    "first segment name parsed (node t0, opened_ms, seq 1)"
)
check(
    #segs == 4 and segs[1].seq == 1 and segs[2].seq == 2 and segs[3].seq == 3 and segs[4].seq == 4,
    "segments sorted by (opened_ms, seq)"
)
check(segs[4] ~= nil and segs[4].size == 321, "last segment size 321")

local header, records, warnings = lock_log.read_segment(segs[1].path)
check(header ~= nil and header.version == 2, "header version 2")
check(header ~= nil and header.node_id == 1000, "header node_id 1000")
check(#warnings == 0, "no warnings on clean segment 1")
check(#records == 6, "segment 1 has 6 records (got " .. #records .. ")")

local first = records[1]
check(first ~= nil and first.kind == 1, "first record kind acquire(1)")
check(first ~= nil and first.ts_ms == 1700000001000, "first record ts_ms")
check(first ~= nil and first.lock_id == 101, "first record lock_id")
check(first ~= nil and first.fencing_token == 5001, "first record fencing_token")
check(first ~= nil and first.renew_count == 1, "first record renew_count")
check(first ~= nil and first.held_gauge == 2, "first record held_gauge")
check(first ~= nil and first.name == "fixture-lock-1", "first record name")
check(first ~= nil and first.expiry_ms == 1700000091000, "first record expiry_ms (ts + 90s)")
check(
    first ~= nil and first.holder == "00000000-0000-0000-0000-000000000001",
    "first record holder decoded as dashed UUID"
)
check(
    first ~= nil
        and first.labels ~= nil
        and #first.labels == 2
        and first.labels[1] == "group-1"
        and first.labels[2] == "lock",
    "first record labels decoded as array"
)

-- A non-leased record (release, n=3): zero expiry/holder/labels decode to
-- 0/nil/{}.
local rel = records[3]
check(rel ~= nil and rel.kind == 3 and rel.expiry_ms == 0, "release record expiry 0")
check(rel ~= nil and rel.holder == nil, "release record holder nil (all-zero)")
check(rel ~= nil and rel.labels ~= nil and #rel.labels == 0, "release record labels empty")

local _, recs2 = lock_log.read_segment(segs[2].path)
local _, recs3 = lock_log.read_segment(segs[3].path)
local _, recs4 = lock_log.read_segment(segs[4].path)
check(#recs2 == 6, "segment 2 has 6 records (got " .. #recs2 .. ")")
check(#recs3 == 6, "segment 3 has 6 records (got " .. #recs3 .. ")")
check(#recs4 == 4, "segment 4 has 4 records (got " .. #recs4 .. ")")

local all, all_warnings = lock_log.scan(FIXTURES)
check(#all == 22, "scan yields 22 records (got " .. #all .. ")")
check(#all_warnings == 0, "scan yields no warnings on clean fixture")
check(all[1] ~= nil and all[1].name == "fixture-lock-1", "scan first record name")

local last = all[#all]
check(last ~= nil and last.kind == 3, "last record kind release(3)")
check(last ~= nil and last.ts_ms == 1700000099999, "last record ts_ms")
check(last ~= nil and last.lock_id == 999, "last record lock_id")
check(last ~= nil and last.fencing_token == 9999, "last record fencing_token")
check(last ~= nil and last.renew_count == 0, "last record renew_count")
check(last ~= nil and last.held_gauge == 0, "last record held_gauge")
check(last ~= nil and last.name == "", "last record has empty (nil) name")

-- Every record field against the deterministic generator pattern:
-- record n (1..21): kind=((n-1)%7)+1, ts=1700000000000+n*1000, lock=100+n,
-- token=5000+n, renew=n%4, held=(n%5)+1, name="fixture-lock-"..n; leased
-- kinds (acquire/renew/cas) also carry expiry=ts+90000, a holder UUID
-- ending in %02x(n), and labels {"group-(n%3)","lock"}.
local all_ok = #all == 22
if all_ok then
    for n = 1, 21 do
        local r = all[n]
        local leased = r.kind == 1 or r.kind == 2 or r.kind == 4
        local want_holder = leased
                and string.format("00000000-0000-0000-0000-0000000000%02x", n)
            or nil
        local labels_ok = false
        if leased then
            labels_ok = #r.labels == 2
                and r.labels[1] == "group-" .. tostring(n % 3)
                and r.labels[2] == "lock"
        else
            labels_ok = #r.labels == 0
        end
        if r.kind ~= ((n - 1) % 7) + 1
            or r.ts_ms ~= 1700000000000 + n * 1000
            or r.lock_id ~= 100 + n
            or r.fencing_token ~= 5000 + n
            or r.renew_count ~= n % 4
            or r.held_gauge ~= (n % 5) + 1
            or r.name ~= "fixture-lock-" .. tostring(n)
            or r.expiry_ms ~= (leased and (1700000000000 + n * 1000 + 90000) or 0)
            or r.holder ~= want_holder
            or not labels_ok
        then
            all_ok = false
            print("  mismatch at record " .. n)
            break
        end
    end
end
check(all_ok, "all 21 named records match the generator pattern")

-- ---- Torn tail ------------------------------------------------------------

check(shell("rm -rf " .. TMP .. " && mkdir -p " .. TMP), "tmp dir prepared")
check(
    shell("cp " .. segs[4].path .. " " .. TMP .. "/torn.bin"),
    "segment 4 copied"
)
local torn_path = TMP .. "/torn.bin"
local fh = assert(io.open(torn_path, "rb"))
local bytes = fh:read("*a")
fh:close()
fh = assert(io.open(torn_path, "wb"))
fh:write(bytes:sub(1, #bytes - 7))
fh:close()
local th, tr, tw = lock_log.read_segment(torn_path)
check(th ~= nil, "torn tail: header still decoded")
check(#tr == 3, "torn tail: 3 complete records survive (got " .. #tr .. ")")
check(#tw == 0, "torn tail: no warnings (got " .. #tw .. ")")
check(tr[3] ~= nil and tr[3].name == "fixture-lock-21", "torn tail: record 21 intact")

-- ---- Mid-record corruption + resync ----------------------------------------

local cfh = assert(io.open(segs[1].path, "rb"))
local cbytes = cfh:read("*a")
cfh:close()
-- Records 1-4 occupy 24 + 89+89+77+89 = 368 bytes; record 5 starts at byte
-- 369 (1-based) and its kind byte is at 372. Flip it: breaks the CRC but
-- leaves record_len valid.
local flip = 372
check(cbytes:byte(369) == 0x4C, "corrupt: record 5 magic located at byte 369")
local flipped = cbytes:sub(1, flip - 1)
    .. string.char((cbytes:byte(flip) + 1) % 256)
    .. cbytes:sub(flip + 1)
local cpath = TMP .. "/corrupt.bin"
cfh = assert(io.open(cpath, "wb"))
cfh:write(flipped)
cfh:close()
local ch, cr, cw = lock_log.read_segment(cpath)
check(ch ~= nil, "corrupt: header decoded")
check(#cw >= 1, "corrupt: CRC mismatch warned (got " .. #cw .. ")")
check(#cr == 5, "corrupt: 5 records recovered (got " .. #cr .. ")")
check(
    cr[5] ~= nil and cr[5].lock_id == 106 and cr[5].kind == 6,
    "corrupt: resync recovered record 6 after the flipped record 5"
)

-- ---- Version rejection -------------------------------------------------------

-- Same bytes, but with the header version flipped back to 1: the reader
-- must reject the segment with a warning (no v1 compat on this branch).
local vpath = TMP .. "/v1.bin"
local vfh = assert(io.open(vpath, "wb"))
vfh:write(cbytes:sub(1, 8) .. string.char(0, 1) .. cbytes:sub(11))
vfh:close()
local vh, vr, vw = lock_log.read_segment(vpath)
check(vh ~= nil and vh.version == 1, "version: header still decoded")
check(#vr == 0, "version: no records from a version-1 segment")
check(#vw == 1, "version: exactly one warning (got " .. #vw .. ")")

-- ---- Bad header magic ------------------------------------------------------

local bpath = TMP .. "/bad.bin"
local bfh = assert(io.open(bpath, "wb"))
bfh:write("NOTTELEM" .. string.rep("\0", 64))
bfh:close()
local bh, br, bw = lock_log.read_segment(bpath)
check(bh == nil, "bad magic: header nil")
check(#br == 0, "bad magic: no records")
check(#bw == 1, "bad magic: exactly one warning")

-- ---- list_segments ordering and .tmp filtering (fake lister) ---------------

local fake = function()
    return {
        "telemetry-n2-2000-000002.bin",
        "telemetry-n1-1000-000003.bin.tmp",
        "telemetry-n1-1000-000001.bin",
        "telemetry-n1-1000-000002.bin",
        "README.md",
        "telemetry-bad.bin",
        "telemetry-n1-1000-0001.bin",
    }
end
local ordered = lock_log.list_segments(TMP, fake)
check(#ordered == 3, "fake lister: 3 matching segments (got " .. #ordered .. ")")
check(
    #ordered == 3
        and ordered[1].seq == 1
        and ordered[2].seq == 2
        and ordered[3].opened_ms == 2000,
    "fake lister: sorted by (opened_ms, seq), .tmp and junk ignored"
)
check(
    #ordered == 3 and ordered[1].node == "n1" and ordered[3].node == "n2",
    "fake lister: node names parsed"
)

-- ---- Teardown ---------------------------------------------------------------

shell("rm -rf " .. TMP)

if failures > 0 then
    print(failures .. " failure(s)")
    os.exit(1)
end
print("all tests passed")
os.exit(0)
