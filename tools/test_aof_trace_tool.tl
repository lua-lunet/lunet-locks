-- The aof-trace-tool.tl tests: fixture round-trip through the same C ABI,
-- envelope parsing, summary counts, and the acceptance numbers from the
-- recorded nine-node traces. Run: luajit tools/test_aof_trace_tool.tl
local ffi = require("ffi")
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

-- 7. anchored timeline export (item02 item03 mirror): locks-timeline +
-- takeover summaries, hb-spacing, aof-noise. One rich fixture: lock ops
-- on ONE lock (holder churn 1 -> 1 -> break -> 2 -> 3), leader trailers
-- with a sent_at_ms series, marker-5 samples with dt_ms, and a 9 ms ns
-- arrival stall. Mirrors the py AnchoredTimelineExport fixture 1:1.
local BASE_MS = 1789214915000
local BASE_NS = ffi.new("uint64_t", BASE_MS) * 1000000
local A1 = BASE_MS + 3   -- the renew's commit ts: last sign of holder 1
local A2 = BASE_MS + 15  -- next holder's acquire commit ts
local LE32 = function(v: number): string
    return string.char(v % 256, math.floor(v / 256) % 256,
        math.floor(v / 65536) % 256, math.floor(v / 16777216) % 256)
end
-- u64 from a cdata byte-exact: 8 subtraction-extraction rounds keep
-- bytes exact past 2^53 (the number path cannot).
local u64_bytes_le_orig = function(v: any): string
    local out: {string} = {}
    for i = 8, 1, -1 do
        local b = v % 256
        out[i] = string.char(tonumber(b))
        v = (v - b) / 256
    end
    return table.concat(out)
end
local u64_bytes_be = function(v: any): string
    local out: {string} = {}
    for i = 8, 1, -1 do
        local b = v % 256
        out[i] = string.char(tonumber(b))
        v = (v - b) / 256
    end
    return table.concat(out)
end
local function op_lock_json(op: string, holder: number): string
    local mid = ("23456789-1234-5678-9123-67890123456%d"):format(holder)
    return ('{"op":"%s","message_id":"%s","client_id":%d,'
        .. '"request_num":1,"lock_id":7}'):format(op, mid, holder)
end
local function operation_prepare(slot: number, era: number, view: number,
                                 lock_json: string, committed: number): string
    return be32(2) .. be32(era) .. be32(view) .. be64n(slot)
        .. string.char(2) .. be64n(slot) .. be32(era) .. string.char(1)
        .. be64n(slot) .. be64n(slot) .. be32(#lock_json) .. lock_json
        .. be64n(committed)
end
local function commit_frame(slot: number, era: number, view: number,
                            committed: number): string
    return be32(4) .. be32(era) .. be32(view) .. be64n(slot)
        .. string.char(4) .. be64n(committed)
end
local function trailer(era: number, leader: number, seq: number,
                       sent_at: number): string
    return string.char(0xc0, 0x0b) .. LE32(era) .. LE32(leader) .. LE32(seq)
        .. be64n(sent_at):reverse()
end
local function envelope_u64(marker: number, ns: any, payload: string): string
    return string.char(marker) .. u64_bytes_be(ns) .. payload
end
local timeline_dir = tool.fixture_dir()
local hpt = tool.open_writer(timeline_dir .. "/fixture.aof")
-- The wire trace, in ns order. The 4th delta is 9 ms: the stall.
local writes: {any} = {
    {BASE_NS + 0,          operation_prepare(5, 4, 1, op_lock_json("set", 1), 0)},
    {BASE_NS + 1e6,        commit_frame(5, 4, 1, 5) .. trailer(4, 33, 1, BASE_MS)},
    {BASE_NS + 2e6,        operation_prepare(6, 4, 1, op_lock_json("set", 1), 0)},
    {BASE_NS + 3e6,        commit_frame(6, 4, 1, 6) .. trailer(4, 33, 2, BASE_MS + 20)},
    {BASE_NS + 12e6,       operation_prepare(7, 4, 1, op_lock_json("break", 99), 0)},
    {BASE_NS + 13e6,       commit_frame(7, 4, 1, 7) .. trailer(4, 33, 3, BASE_MS + 45)},
    {BASE_NS + 14e6,       operation_prepare(8, 4, 1, op_lock_json("set", 2), 0)},
    {BASE_NS + 15e6,       commit_frame(8, 4, 1, 8) .. trailer(4, 33, 4, BASE_MS + 65)},
    {BASE_NS + 16e6,       operation_prepare(9, 4, 1, op_lock_json("set", 3), 0)},
    {BASE_NS + 17e6,       commit_frame(9, 4, 1, 9) .. trailer(4, 33, 5, BASE_MS + 80)},
}
for _, w in ipairs(writes) do
    tool.append(hpt, envelope_u64(1, w[1], w[2]))
end
tool.close_writer(hpt)
-- Telemetry in a later-sorted file keeps file order = ns order across
-- the dir (the export's ordering contract).
local h2t = tool.open_writer(timeline_dir .. "/g.aof")
tool.append(h2t, envelope_u64(2, BASE_NS + 18e6,
    '{"phi":1.7,"now_ms":100,"prev_wait_ms":900,"next_wait_ms":1000,'
    .. '"leader":33,"era":4,"view":1,"mean":22.0}'))
tool.append(h2t, envelope_u64(5, BASE_NS + 19e6,
    '{"node":88,"era":4,"leader":33,"addr":"127.0.0.1:1","dt_ms":21,"ts_ms":'
    .. tostring(BASE_MS) .. ',"phi":0.123,"sent_at_ms":'
    .. tostring(BASE_MS) .. '}'))
tool.append(h2t, envelope_u64(5, BASE_NS + 20e6,
    '{"node":88,"era":4,"leader":33,"addr":"127.0.0.1:1","dt_ms":23,"ts_ms":'
    .. tostring(BASE_MS + 20000) .. ',"phi":0.223,"sent_at_ms":'
    .. tostring(BASE_MS) .. '}'))
tool.close_writer(h2t)

-- Independent reference for t_fmt: divmod over the UTC day portion.
local function hhmmss(ms: number): string
    local secs = math.floor(ms / 1000)
    local hh = math.floor((secs % 86400) / 3600)
    local rem = secs % 3600
    local mm = math.floor(rem / 60)
    local ss = rem % 60
    local frac = ms % 1000
    return ("%02d:%02d:%02d.%03d"):format(hh, mm, ss, frac)
end

local tlkinds: {string} = {}
local tlout = tool.export_series(timeline_dir, "locks-timeline", {A1, A2})
for _, e in ipairs(tlout) do
    tlkinds[#tlkinds + 1] = e.kind
end
check("tl-anchor-echoes-first", #tlkinds == 9 and tlkinds[1] == "anchor"
    and tlkinds[2] == "anchor", table.concat(tlkinds, ","))
check("tl-timeline-ops", #tlout >= 7 and tlout[3].op == "acquire"
    and tlout[4].op == "renew" and tlout[5].op == "break"
    and tlout[6].op == "acquire" and tlout[7].op == "acquire",
    tostring(tlout[3]) .. "|" .. tostring(tlout[4]))
check("tl-timeline-holders", #tlout >= 7 and tlout[3].holder == 1
    and tlout[4].holder == 1 and tlout[5].holder == 99
    and tlout[6].holder == 2 and tlout[7].holder == 3,
    tostring(tlout[6]) .. "/" .. tostring(tlout[7]))
check("tl-timeline-ts", #tlout >= 7 and tlout[3].ts_ms == BASE_MS + 1
    and tlout[4].ts_ms == BASE_MS + 3 and tlout[5].ts_ms == BASE_MS + 13
    and tlout[6].ts_ms == BASE_MS + 15 and tlout[7].ts_ms == BASE_MS + 17,
    tostring(tlout[3] and tlout[3].ts_ms))
check("tl-anchor-echo-fields", #tlout >= 2 and tlout[1].anchor == A1
    and tlout[1].t_fmt == hhmmss(A1) and tlout[2].t_fmt == hhmmss(A2),
    tostring(tlout[1]))
local tc: {any} = {}
for _, e in ipairs(tlout) do
    if e.kind == "takeover" then
        tc[#tc + 1] = e
    end
end
check("tl-takeover-anchors", #tc == 2 and tc[1].anchor == A1
    and tc[2].anchor == A2, tostring(#tc))
check("tl-takeover-1-py-mirror", #tc >= 2 and tc[1].last_holder == 1
    and tc[1].last_renew_t_rel == 0.0 and tc[1].break_seen_t_rel == 10.0
    and tc[1].next_acquire_t_rel == "absent"
    and tc[1].next_holder == "absent" and tc[1].takeover_ms == "absent",
    tostring(tc[1]))
check("tl-takeover-2-py-mirror", #tc >= 2 and tc[2].last_holder == 2
    and tc[2].last_renew_t_rel == 0.0
    and tc[2].break_seen_t_rel == "absent"
    and tc[2].next_acquire_t_rel == 2.0 and tc[2].next_holder == 3
    and tc[2].takeover_ms == 2.0, tostring(tc[2]))

-- plates: from the py suite — anchors map onto every extra kind's ts.
local lk: {any} = {}
for _, e in ipairs(tool.export_series(timeline_dir, "locks", {A2, A1}) as {{string: any}}) do
    if e.kind == "locks" then
        lk[#lk + 1] = e
    end
end
local lkfirst = lk[1]
check("tl-anchors-add-time-columns", lkfirst ~= nil
    and lkfirst.t_rel_ms == -2.0
    and lkfirst.t_fmt == hhmmss(BASE_MS + 1),
    tostring(lkfirst and lkfirst.t_rel_ms) .. "/" .. tostring(lkfirst))
local smp: {any} = {}
for _, e in ipairs(tool.export_series(timeline_dir, "phi-samples", {A1}) as {{string: any}}) do
    if e.kind == "phi-samples" then
        smp[#smp + 1] = e
    end
end
local smprow = smp[1]
check("tl-anchors-sample-ts-rides-payload", smprow ~= nil
    and smprow.t_rel_ms == -3.0
    and smprow.t_fmt == hhmmss(BASE_MS),
    tostring(smprow and smprow.t_rel_ms) .. "/" .. tostring(smprow))
local ph: {any} = {}
for _, e in ipairs(tool.export_series(timeline_dir, "phi", {A1}) as {{string: any}}) do
    if e.kind == "phi" then
        ph[#ph + 1] = e
    end
end
local phrow = ph[1]
check("tl-anchors-phi-now-ms-fallback", phrow ~= nil
    and phrow.t_rel_ms == 100.0 - A1
    and phrow.t_fmt == hhmmss(100),
    tostring(phrow and phrow.t_rel_ms) .. "/" .. tostring(phrow))

-- no anchors, no relative columns (existing rows stay untouched)
local lkn = tool.export_series(timeline_dir, "locks")
check("tl-no-anchors-keeps-rows", lkn[1] ~= nil and lkn[1].t_rel_ms == nil
    and lkn[1].t_fmt == nil, tostring(lkn[1]))

-- hb-spacing stats per (era, leader): wire trailer + sample sources.
local sp = tool.export_series(timeline_dir, "hb-spacing")
local spwire: {any} = {}
local spsample: {any} = {}
for _, e in ipairs(sp) do
    if e.source == "wire" then
        spwire[#spwire + 1] = e
    elseif e.source == "sample" then
        spsample[#spsample + 1] = e
    end
end
check("tl-hb-spacing-wire", #spwire == 1 and spwire[1].era == 4
    and spwire[1].leader == 33 and spwire[1].min_ms == 15.0
    and spwire[1].mean_ms == 20.0 and spwire[1].p50_ms == 20.0
    and spwire[1].p95_ms == 25.0 and spwire[1].max_ms == 25.0
    and spwire[1].count == 4, tostring(spwire[1]))
check("tl-hb-spacing-sample", #spsample == 1 and spsample[1].era == 4
    and spsample[1].leader == 33 and spsample[1].count == 2
    and spsample[1].min_ms == 21.0 and spsample[1].mean_ms == 22.0
    and spsample[1].max_ms == 23.0, tostring(spsample[1]))

-- aof-noise: gap/arrival stats + the 9 ms stall (--hb-ms floor)
local noise = tool.export_series(timeline_dir, "aof-noise")
check("tl-aof-noise-one-row", #noise == 1 and noise[1] ~= nil
    and noise[1].kind == "aof-noise", tostring(#noise))
local gn = noise[1] ~= nil and noise[1].gaps or nil
local snv = noise[1] ~= nil and noise[1].samples or nil
check("tl-aof-noise-gaps", gn ~= nil and gn.count == 9 and gn.min_ms == 1.0
    and gn.max_ms == 9.0 and gn.p50_ms == 1.0 and gn.p95_ms == 9.0
    and gn.mean_ms == 17.0 / 9.0, tostring(gn))
check("tl-aof-noise-samples", snv ~= nil and snv.count == 2
    and snv.mean_ms == 22.0, tostring(snv))
local stalls = (noise[1] ~= nil) and noise[1].stalls or nil
local okstall = stalls ~= nil and #stalls == 1
    and stalls[1].a_ns == BASE_NS + 3e6
    and stalls[1].b_ns == BASE_NS + 12e6
    and stalls[1].gap_ms == 9.0
check("tl-aof-noise-stalls", okstall, tostring(stalls))

-- cli: the main verb writes the anchor echo and summary lines
local cli_err = ""
local cli_ok, cli_rc = pcall(function()
    local out_path = timeline_dir .. "/out.jsonl"
    local rc = tool.main({"prog", "--anchor", tostring(BASE_MS + 1),
        "--kind", "locks-timeline", timeline_dir, out_path})
    local lines: {string} = {}
    local fh = io.open(out_path, "r")
    if fh ~= nil then
        for line in fh:lines() do
            lines[#lines + 1] = line
        end
        fh:close()
    end
    cli_err = tostring(rc) .. " " .. tostring(lines[1])
    check("tl-cli-anchor-echo", rc == 0 and #lines > 0
        and lines[1]:find('"kind": "anchor"') ~= nil
        and lines[1]:find('"t_fmt": "' .. hhmmss(BASE_MS + 1) .. '"') ~= nil,
        cli_err)
end)
if not cli_ok then
    check("tl-cli-anchor-echo", false, "main errored: " .. tostring(cli_rc))
end

-- 9. item03.5: --kind is repeatable AND space-joined, union semantics —
-- ONE contract in both twins (the dry run proved the divergence: py
-- overwrote last-wins but split joined values, tl overwrote and
-- silently exported nothing for joined values).
local function read_lines(p: string): {string}
    local fh = io.open(p, "r")
    local out: {string} = {}
    if fh ~= nil then
        for line in fh:lines() do
            out[#out + 1] = line
        end
        fh:close()
    end
    return out
end
local oku, erru = pcall(function()
    local u1 = timeline_dir .. "/union1.jsonl"
    local rcu1 = tool.main({"prog", "--kind", "hb-spacing", "--kind", "aof-noise",
        timeline_dir, u1})
    local u2 = timeline_dir .. "/union2.jsonl"
    local rcu2 = tool.main({"prog", "--kind", "hb-spacing aof-noise", timeline_dir, u2})
    local l1 = read_lines(u1)
    local l2 = read_lines(u2)
    local n_hb, n_noise = 0, 0
    for _, l in ipairs(l1) do
        if l:find('"kind": "hb-spacing"', 1, true) ~= nil then
            n_hb = n_hb + 1
        end
        if l:find('"kind": "aof-noise"', 1, true) ~= nil then
            n_noise = n_noise + 1
        end
    end
    check("kind-union-repeatable", rcu1 == 0 and #l1 == 3
        and n_hb == 2 and n_noise == 1,
        tostring(rcu1) .. " lines=" .. tostring(#l1) .. " hb=" .. tostring(n_hb)
            .. " noise=" .. tostring(n_noise))
    local same = #l1 == #l2
    if same then
        for i = 1, #l1 do
            if l1[i] ~= l2[i] then
                same = false
            end
        end
    end
    check("kind-union-joined-matches-repeatable", rcu2 == 0 and same,
        "joined and repeatable are one contract")
end)
if not oku then
    check("kind-union-cli", false, "main errored: " .. tostring(erru))
end

-- 10. item03.5 large-scale twin parity: >=28,000 wire records, ns stamps
-- ~1.789e18 (> 2^53) — the real-w1b dry run drifted in aof-noise
-- gaps.mean_ms (the naive accumulation vs the reference's compensated
-- sum). The tl export of hb-spacing + aof-noise must be byte-identical
-- to the python twin's CLI export of the same fixture dir.
local LARGE_N = 28001
local LBASE_NS = 1789248826514115648 -- 256-aligned: exact as a double literal
local LBASE_MS = 1789248826514
local ldir = ".tmp/large-parity-tl"
os.execute("rm -rf " .. ldir)
os.execute("mkdir -p " .. ldir)
local hw = tool.open_writer(ldir .. "/w.aof")
local lns = ffi.new("uint64_t", LBASE_NS)
local lsent = LBASE_MS
for k = 0, LARGE_N - 1 do
    if k > 0 then
        local g = 100000 + (k * 2654435761) % 9900000
        if k % 4096 == 0 then
            g = g + 5008914641
        end
        lns = lns + g
        lsent = lsent + 5 + (k * 97) % 17
    end
    local frame = wire(4, 4, 1, 5) .. string.char(0xc0, 0x0b)
        .. LE32(4) .. LE32(44) .. LE32(k) .. be64n(lsent):reverse()
    tool.append(hw, envelope_u64(1, lns, frame))
end
tool.close_writer(hw)
local hg = tool.open_writer(ldir .. "/g.aof")
for i = 0, 511 do
    local dt = 20 + (i * 13 % 7) / 3
    local js = ('{"node":88,"era":4,"leader":44,"addr":"127.0.0.1:1",'
        .. '"dt_ms":%s,"ts_ms":%d,"phi":0.123,"sent_at_ms":%d}')
        :format(("%.17g"):format(dt), LBASE_MS + i * 7, LBASE_MS + i * 7)
    tool.append(hg, envelope_u64(5,
        ffi.new("uint64_t", LBASE_NS) + 1000000000 + i * 1000000, js))
end
tool.close_writer(hg)
local ltl_out = ldir .. "/tl.jsonl"
local rcl = tool.main({"prog", "--kind", "hb-spacing", "--kind", "aof-noise",
    "--hb-ms", "6000", ldir, ltl_out})
local ph3 = io.popen("command -v python3")
local has_py3 = (ph3:read("*l") or "") ~= ""
ph3:close()
if not has_py3 then
    print("SKIP: python3 not on PATH — the large-scale twin cross-check needs it")
end
if has_py3 then
    os.execute("python3 tools/aof-trace-tool.py --kind hb-spacing"
        .. " --kind aof-noise --hb-ms 6000 " .. ldir .. " " .. ldir
        .. "/py.jsonl > /dev/null 2>&1")
    local ltl = read_lines(ltl_out)
    local lpy = read_lines(ldir .. "/py.jsonl")
    local ident = #ltl == #lpy
    local firstdiff = "line counts " .. tostring(#ltl) .. "/" .. tostring(#lpy)
    if ident then
        for i = 1, #ltl do
            if ltl[i] ~= lpy[i] then
                ident = false
                firstdiff = "line " .. tostring(i) .. ":\ntl: "
                    .. ltl[i]:sub(1, 160) .. "\npy: " .. lpy[i]:sub(1, 160)
                break
            end
        end
    end
    check("large-parity-byte-identical", rcl == 0 and #ltl == 3 and ident, firstdiff)
    -- the exported aof-noise mean is the compensated (Neumaier) sum over
    -- the sorted gaps, NOT the naive accumulation the old twin shipped
    local mean_txt = (ltl[3] ~= nil)
        and (ltl[3]:match('"mean_ms": ([%d%.]+)') or "?") or "?"
    local exported_mean = tonumber(mean_txt)
    local gaps: {number} = {}
    for i = 1, LARGE_N - 1 do
        local g = 100000 + (i * 2654435761) % 9900000
        if i % 4096 == 0 then
            g = g + 5008914641
        end
        gaps[#gaps + 1] = g / 1e6
    end
    table.sort(gaps)
    local naive = 0.0
    local tot, cor = 0.0, 0.0
    for _, v in ipairs(gaps) do
        naive = naive + v
        local t = tot + v
        if math.abs(tot) >= math.abs(v) then
            cor = cor + ((tot - t) + v)
        else
            cor = cor + ((v - t) + tot)
        end
        tot = t
    end
    check("large-parity-compensated-mean", exported_mean == (tot + cor) / #gaps
        and exported_mean ~= naive / #gaps,
        "mean=" .. tostring(mean_txt) .. " naive=" .. tostring(naive / #gaps))
end

print(("aof-trace-tool.tl: %d passed, %d failed"):format(passed, failed))
os.exit(failed == 0 and 0 or 1)
