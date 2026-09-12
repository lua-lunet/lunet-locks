-- The aof-trace-tool.tl tests: fixture round-trip through the same C ABI,
-- envelope parsing, summary counts, and the acceptance numbers from the
-- recorded nine-node traces. Run: luajit tools/test_aof_trace_tool.tl
local tool = require("tools.aof-trace-tool")

local passed = 0
local failed = 0
local function check(name: string, cond: boolean, detail: string)
    if cond then
        passed = passed + 1
    else
        failed = failed + 1
        print(("FAIL %s: %s"):format(name, detail))
    end
end

-- LuaJIT has no string.pack (5.1 ABI): the BE integers go out by hand.
local function be_u32(v: number): string
    return string.char(
        math.floor(v / 16777216) % 256,
        math.floor(v / 65536) % 256,
        math.floor(v / 256) % 256,
        v % 256)
end

local function be_u64(v: number): string
    local out: {string} = {}
    for i = 7, 0, -1 do
        out[#out + 1] = string.char(math.floor(v / 256 ^ i) % 256)
    end
    return table.concat(out)
end

local function envelope(marker: number, ns: number, payload: string): string
    return string.char(marker) .. be_u64(ns) .. payload
end

local function wire(tag: number, era: number, view: number, slot: number): string
    -- marker-1 payload: 20 B big-endian header + body discriminant
    return be_u32(tag) .. be_u32(era) .. be_u32(view) .. be_u64(slot)
        .. string.char(9) .. ("\0"):rep(8)
end

-- 1. fixture round-trip: write five records through the C ABI, read back.
local dir = tool.fixture_dir()
local path = dir .. "/fixture.aof"
local h = tool.open_writer(path)
tool.append(h, envelope(2, 1000, '{"phi":1.5}'))
tool.append(h, envelope(1, 2000, wire(4, 1, 0, 5)))
tool.append(h, envelope(3, 3000, '{"event":"teardown"}'))
tool.append(h, envelope(2, 4000, '{"phi":2.5}'))
tool.append(h, envelope(1, 5000, wire(2, 1, 0, 6)))
tool.close_writer(h)
local s = tool.summarize(path)
check("fixture-records", s.records == 5, tostring(s.records))
check("fixture-markers", s.by_marker["1"] == 2 and s.by_marker["2"] == 2
    and s.by_marker["3"] == 1, tostring(s.by_marker))
check("fixture-tags", s.tags["4 Commit"] == 1 and s.tags["2 Prepare"] == 1,
    tostring(s.tags))
check("fixture-ns", s.first_ns == 1000 and s.last_ns == 5000,
    tostring(s.first_ns) .. ".." .. tostring(s.last_ns))
check("fixture-clean", s.malformed == 0, tostring(s.malformed))

-- 2. big-endian ns header
check("ns-big-endian", tool.ns_of(envelope(1, 0x1122334455667788, "")) == 0x1122334455667788, "ns")

-- 3. unknown markers rejected, never guessed
local dir2 = tool.fixture_dir()
local h2 = tool.open_writer(dir2 .. "/fixture.aof")
tool.append(h2, string.char(9) .. be_u64(10) .. "junk")
tool.close_writer(h2)
local s2 = tool.summarize(dir2 .. "/fixture.aof")
check("unknown-marker", s2.records == 1 and s2.malformed == 1, tostring(s2))

-- 4. acceptance: every record of the recorded standby series is readable
local real = "tools/../.tmp/telemetry/nine-node-2026-09-12/w1b/aof"
local ok_real = tool.is_dir(real)
if ok_real then
    local sr = tool.summarize_series(real)
    check("real-records", sr.records == 49832, tostring(sr.records))
    check("real-wire", sr.by_marker["1"] == 33285, tostring(sr.by_marker))
    local tel = (sr.by_marker["2"] or 0) + (sr.by_marker["3"] or 0) + (sr.by_marker["4"] or 0)
    check("real-telemetry", tel == 16547, tostring(tel))
    check("real-clean", sr.malformed == 0, tostring(sr.malformed))
    -- 5. the parity contract: the summary must parse as JSON with the
    -- shared key set (the python twin's shape).
    local j = tool.summarize_series_json(real)
    check("parity-keys", j:find('"records"') and j:find('"by_marker"')
        and j:find('"first_ns"') and j:find('"last_ns"') and j:find('"span_s"'),
        j:sub(1, 60))
else
    print("SKIP: real rig traces not extracted (tools/../.tmp/telemetry/...)")
end

-- 6. the export: committed lock work + enacted reconfigs + phi + samples
local export_dir = tool.fixture_dir()
local hp = tool.open_writer(export_dir .. "/fixture.aof")
local function be32(v: number): string
    return string.char(math.floor(v / 16777216) % 256,
        math.floor(v / 65536) % 256, math.floor(v / 256) % 256, v % 256)
end
local function be64n(v: number): string
    local o: {string} = {}
    for i = 7, 0, -1 do
        o[#o + 1] = string.char(math.floor(v / 256 ^ i) % 256)
    end
    return table.concat(o)
end
local set_json = '{"op":"set","message_id":"11111111-2222-3333-4444-555555555555","client_id":1,"request_num":1,"lock_id":7}'
local prepare = be32(2) .. be32(4) .. be32(1) .. be64n(5)
    .. string.char(2) .. be64n(5) .. be32(4) .. string.char(1)
    .. be64n(0x1111) .. be64n(0x2222) .. be32(#set_json) .. set_json
    .. be64n(0)
local commit = be32(4) .. be32(4) .. be32(1) .. be64n(5) .. string.char(4) .. be64n(5)
-- trailer: magic(2 LE order bytes c0 0b) + era(4) + leader(4) + seq(4) + sent_at(8), little-endian
local trailer = string.char(0xc0, 0x0b) .. string.char(4, 0, 0, 0) .. string.char(33, 0, 0, 0)
    .. string.char(9, 0, 0, 0) .. be64n(1789214915000):reverse()
tool.append(hp, envelope(1, 2000, prepare))
tool.append(hp, envelope(1, 2500, commit .. trailer))
tool.append(hp, envelope(2, 3000, '{"phi":1.7,"now_ms":100,"prev_wait_ms":900,"next_wait_ms":1000}'))
tool.append(hp, envelope(5, 4000, '{"node":88,"era":4,"leader":33,"addr":"127.0.0.1:1","dt_ms":22,"ts_ms":1789214915000}'))
local join = be32(2) .. be32(4) .. be32(1) .. be64n(6)
    .. string.char(2) .. be64n(6) .. be32(4) .. string.char(2, 7)
    .. be32(77) .. be32(255) .. be64n(0)
local commit6 = be32(4) .. be32(4) .. be32(1) .. be64n(6) .. string.char(4) .. be64n(6)
tool.append(hp, envelope(1, 5000, join))
tool.append(hp, envelope(1, 5500, commit6 .. trailer))
tool.close_writer(hp)
local events = tool.export_series(export_dir, "all")
local kinds: {string} = {}
for _, e in ipairs(events) do
    kinds[#kinds + 1] = e.kind
end
check("export-kinds", #kinds == 4 and kinds[1] == "locks" and kinds[2] == "phi"
    and kinds[3] == "phi-samples" and kinds[4] == "reconfig",
    table.concat(kinds, ","))
local locks = 0
local reconf: string = ""
local phi_seen = false
local sample_seen = false
for _, e in ipairs(events) do
    if e.kind == "locks" then locks = locks + 1 end
    if e.kind == "reconfig" then reconf = e.command end
    if e.kind == "phi" then phi_seen = e.phi == 1.7 end
    if e.kind == "phi-samples" then sample_seen = e.dt_ms == 22 end
end
check("export-locks-count", locks == 1, tostring(locks))
check("export-reconfig", reconf == "Join node=77 position=255", reconf)
check("export-phi", phi_seen, "phi")
check("export-samples", sample_seen, "dt_ms")
local locks_only = tool.export_series(export_dir, "locks")
local both_only = tool.export_series(export_dir, "both")
check("export-filter", #locks_only == 1 and #both_only == 2,
    tostring(#locks_only) .. "/" .. tostring(#both_only))
-- the lock record carries BOTH clocks
local lock_ev = locks_only[1]
check("export-clocks", lock_ev.leader_ms == 1789214915000 and lock_ev.aof_ns == 2500,
    tostring(lock_ev.leader_ms) .. "/" .. tostring(lock_ev.aof_ns))

print(("aof-trace-tool.tl: %d passed, %d failed"):format(passed, failed))
os.exit(failed == 0 and 0 or 1)
