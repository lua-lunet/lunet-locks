-- The reconfiguration admin surface, pure and tested: the client admin
-- verbs over the NDJSON line protocol, the application-level ADMIN peer
-- payload that carries a membership change to the other replicas, and the
-- admin acknowledgment line.
--
-- Contract:
-- - The join/increment/decrement/leave verbs are operator-trusted at exactly
--   the same level as the lock verbs today: the TCP client channel has no
--   separate authentication, and any client that can acquire locks can
--   propose membership changes to the leader. This is a fact of the current
--   deployment surface, not a gap to be worked around here.
-- - A verb decodes strictly (unknown actions, malformed ids, bad message
--   ids and bad endpoints are refused, never guessed) and is leader-only:
--   a non-leader replica forwards the line through the existing
--   forward/redirect machinery, and the leader drives the reconfiguration
--   through the native ABI.
-- - The ADMIN peer payload exists for the two membership changes that move
--   an endpoint row: a join adds the joining member's addressing row at the
--   peers (additive and idempotent, safe before the commit because the core
--   sends nothing to a pre-commit non-member), and a leave removes the
--   departed member's rows after the commit. An increment or a decrement
--   moves no endpoint, so neither broadcasts anything: the peers learn
--   weight changes the same way they learn every committed operation, from
--   the replication stream. A leave requires the member to be at weight 0:
--   the operator's sequence is decrement, wait for the era to commit, then
--   leave.
-- - Every payload is length-delimited and strictly shaped; a payload that
--   does not decode is dropped by the peer, never applied partially.
-- - The host robustness for the verbs is pure here too. The dedup cache
--   correlates a driven verb by its message_id: the leader claims a verb
--   before driving it, settles the FIRST outcome (accepted, refused, or the
--   honest deadline ack), and replays that same ack for every duplicate —
--   no re-drive, no racing refusal ahead of the true acknowledgment. A
--   claim that came back NotPrimary is released, so the new leader drives
--   the verb fresh. The rotation helper gives the forwarder the same
--   bounded, ordered retry-over-members discipline the lock path gets from
--   the adapter: a redirect or a silent-leader window just advances the
--   rotation.

local record Verb
    action: string
    message_id: string
    id: integer
    name: string
    endpoint: string
end

local record MemberChange
    kind: string
    id: integer
    name: string
    endpoint: string
end

local JOIN_KIND = "J"
local LEAVE_KIND = "L"
local ADMIN_DIRECTION = "\4"
local MAX_U32 = 4294967294

local config = require("config")

local function valid_action(action: string): boolean
    return action == "join"
        or action == "increment"
        or action == "decrement"
        or action == "leave"
end

local function valid_uuid(message_id: string): boolean
    if #message_id ~= 36 then
        return false
    end
    local hex = message_id:gsub("-", "")
    if #hex ~= 32 then
        return false
    end
    return hex:find("[^0-9a-fA-F]") == nil
end

local function valid_member_id(text: string): boolean
    local id = tonumber(text)
    return id ~= nil
        and id >= 0
        and id <= MAX_U32
        and id == math.floor(id)
        and text:match("^%d+$") ~= nil
end

local function decode_verb(json: string): (boolean, Verb)
    local action = json:match('"action"%s*:%s*"([^"]*)"')
    if not valid_action(action) then
        return false,
            {
                action = "",
                message_id = "",
                id = 0,
                name = "",
                endpoint = "",
            }
    end
    local message_id = json:match('"message_id"%s*:%s*"([^"]*)"')
    local id_text = json:match('"id"%s*:%s*(%d+)')
    if message_id == nil or not valid_uuid(message_id) then
        return false,
            {
                action = "",
                message_id = "",
                id = 0,
                name = "",
                endpoint = "",
            }
    end
    if id_text == nil or not valid_member_id(id_text) then
        return false,
            {
                action = "",
                message_id = "",
                id = 0,
                name = "",
                endpoint = "",
            }
    end
    local name = ""
    local endpoint = ""
    if action == "join" then
        name = json:match('"name"%s*:%s*"([^"]*)"') or ""
        endpoint = json:match('"endpoint"%s*:%s*"([^"]*)"') or ""
        if name:match("^[%w_-]+$") == nil then
            return false,
                {
                    action = "",
                    message_id = "",
                    id = 0,
                    name = "",
                    endpoint = "",
                }
        end
        local ok = pcall(function() config.endpoint(endpoint) end)
        if not ok then
            return false,
                {
                    action = "",
                    message_id = "",
                    id = 0,
                    name = "",
                    endpoint = "",
                }
        end
    end
    return true,
        {
            action = action,
            message_id = message_id,
            id = tonumber(id_text) as integer,
            name = name,
            endpoint = endpoint,
        }
end

local function u32_bytes(value: integer): string
    return string.char(
        math.floor(value / 16777216) % 256,
        math.floor(value / 65536) % 256,
        math.floor(value / 256) % 256,
        value % 256
    )
end

local function parse_u32(bytes: string): integer
    return (
        (bytes:byte(1) or 0) * 16777216 + (bytes:byte(2) or 0) * 65536 + (
            bytes:byte(3) or 0
        ) * 256 + (bytes:byte(4) or 0)
    ) as integer
end

local function u16_bytes(value: integer): string
    return string.char(math.floor(value / 256) % 256, value % 256)
end

local function parse_u16(bytes: string): integer
    return ((bytes:byte(1) or 0) * 256 + (bytes:byte(2) or 0)) as integer
end

-- The ADMIN peer payload for a join: direction byte, join kind, the member
-- id, and the length-delimited name and endpoint the peers add to their
-- addressing tables.
local function encode_join(id: integer, name: string, endpoint: string): string
    assert(
        valid_member_id(tostring(id)) and name:match("^[%w_-]+$") ~= nil,
        "admin: invalid join member"
    )
    assert(#name <= 65535 and #endpoint <= 65535, "admin: member field too long")
    return ADMIN_DIRECTION
        .. JOIN_KIND
        .. u32_bytes(id)
        .. u16_bytes(#name)
        .. name
        .. u16_bytes(#endpoint)
        .. endpoint
end

-- The ADMIN peer payload for a leave: direction byte, leave kind, the id
-- whose addressing rows the peers remove.
local function encode_leave(id: integer): string
    assert(valid_member_id(tostring(id)), "admin: invalid leave member")
    return ADMIN_DIRECTION .. LEAVE_KIND .. u32_bytes(id)
end

-- Decodes an application payload that carries the ADMIN direction byte.
-- Returns whether the payload is an admin payload, and its typed change.
local function decode_change(payload: string): (boolean, MemberChange)
    local nothing = {kind = "", id = 0, name = "", endpoint = ""}
    if payload:sub(1, 1) ~= ADMIN_DIRECTION then
        return false, nothing
    end
    local kind = payload:sub(2, 2)
    if kind == JOIN_KIND then
        local rest = payload:sub(3)
        if #rest < 4 + 2 then
            return false, nothing
        end
        local id = parse_u32(rest:sub(1, 4))
        local name_len = parse_u16(rest:sub(5, 6))
        if #rest < 4 + 2 + name_len + 2 then
            return false, nothing
        end
        local name = rest:sub(7, 6 + name_len)
        local endpoint_len = parse_u16(rest:sub(7 + name_len, 8 + name_len))
        if #rest < 4 + 2 + name_len + 2 + endpoint_len then
            return false, nothing
        end
        local endpoint = rest:sub(9 + name_len, 8 + name_len + endpoint_len)
        if name:match("^[%w_-]+$") == nil then
            return false, nothing
        end
        local ok = pcall(function() config.endpoint(endpoint) end)
        if not ok then
            return false, nothing
        end
        return true, {kind = "join", id = id, name = name, endpoint = endpoint}
    elseif kind == LEAVE_KIND then
        local rest = payload:sub(3)
        if #rest < 4 then
            return false, nothing
        end
        local id = parse_u32(rest:sub(1, 4))
        return true, {kind = "leave", id = id, name = "", endpoint = ""}
    end
    return false, nothing
end

-- The acknowledgment line the leader answers an accepted admin verb with.
-- The reconfiguration is proposed at that point; the era advances at the
-- establishing operation's commit, which the client observes through the
-- replica's status.
local function encode_ack(action: string, id: integer): string
    assert(valid_action(action), "admin: invalid ack action")
    return '{"action":"' .. action .. '","id":' .. tostring(id) .. ',"accepted":true}'
end

-- The honest acknowledgment for a verb whose establishing commit did not
-- land within the client's deadline: refused with a reason, because the
-- verb may still commit later. The operator re-drives with a fresh
-- message_id; duplicates of the timed-out one replay this exact line from
-- the dedup cache, truthfully.
local function encode_deadline_ack(action: string, id: integer): string
    assert(valid_action(action), "admin: invalid ack action")
    return '{"action":"'
        .. action
        .. '","id":'
        .. tostring(id)
        .. ',"accepted":false,"reason":"deadline"}'
end

-- The verb dedup cache: correlates a driven verb by its message_id so the
-- leader answers a duplicate with the FIRST outcome instead of re-driving
-- it into a racing refusal. A claimed verb is in flight until its outcome
-- settles; a claim released on a NotPrimary redirect is drivable again.
local record CacheEntry
    settled: boolean
    ack: string
end

local record DedupCache
    entries: {string: CacheEntry}
    order: {string}
    limit: integer

    seen: function(self: DedupCache, message_id: string): string
    claim: function(self: DedupCache, message_id: string): boolean
    settle: function(self: DedupCache, message_id: string, ack: string)
    replay: function(self: DedupCache, message_id: string): (boolean, string)
    release: function(self: DedupCache, message_id: string)
end

local function cache_seen(self: DedupCache, message_id: string): string
    local entry = self.entries[message_id]
    if entry == nil then
        return "new"
    elseif entry.settled then
        return "settled"
    end
    return "in_flight"
end

local function cache_claim(self: DedupCache, message_id: string): boolean
    if self.entries[message_id] ~= nil then
        return false
    end
    self.entries[message_id] = {settled = false, ack = ""}
    table.insert(self.order, message_id)
    -- Bounded memory: the oldest settled entries yield their slots. A drive
    -- still in flight is never evicted, so a duplicate can never re-drive
    -- it and race a refusal ahead of the true ack.
    while #self.order > self.limit do
        local oldest = self.order[1]
        local entry = self.entries[oldest]
        if entry ~= nil and not entry.settled then
            break
        end
        table.remove(self.order, 1)
        self.entries[oldest] = nil
    end
    return true
end

-- Records the FIRST outcome only: a second settle of the same message_id
-- is ignored, so the replayed ack is always the original one.
local function cache_settle(self: DedupCache, message_id: string, ack: string)
    local entry = self.entries[message_id]
    if entry ~= nil and not entry.settled then
        entry.settled = true
        entry.ack = ack
    end
end

local function cache_replay(self: DedupCache, message_id: string): (boolean, string)
    local entry = self.entries[message_id]
    if entry ~= nil and entry.settled then
        return true, entry.ack
    end
    return false, ""
end

-- Drops an unsettled claim: the drive came back NotPrimary (nothing was
-- proposed by it), so the new leader must be able to drive the verb fresh.
local function cache_release(self: DedupCache, message_id: string)
    local entry = self.entries[message_id]
    if entry ~= nil and not entry.settled then
        self.entries[message_id] = nil
        for index, id in ipairs(self.order) do
            if id == message_id then
                table.remove(self.order, index)
                break
            end
        end
    end
end

local record cache_factory
    new: function(integer): DedupCache
end

local cache: cache_factory = {}

function cache.new(limit: integer): DedupCache
    assert(limit >= 1, "admin: cache limit must be positive")
    return {
        entries = {},
        order = {},
        limit = limit,
        seen = cache_seen,
        claim = cache_claim,
        settle = cache_settle,
        replay = cache_replay,
        release = cache_release,
    }
end

-- The forwarder rotation's next hop over the live member list: the
-- smallest listed id greater than `current`, wrapping to the smallest
-- listed id; `current < 0` starts the rotation. An empty list yields -1
-- (nothing to try). The caller supplies the list with its own id already
-- removed: a non-leader does not drive verbs, so forwarding to itself
-- would only loop.
local function next_in_rotation(ids: {integer}, current: integer): integer
    local chosen = -1
    for _, id in ipairs(ids) do
        if id > current and (chosen < 0 or id < chosen) then
            chosen = id
        end
    end
    if chosen < 0 and #ids > 0 then
        chosen = ids[1]
    end
    return chosen
end

return {
    Verb = Verb,
    MemberChange = MemberChange,
    ADMIN_DIRECTION = ADMIN_DIRECTION,
    decode_verb = decode_verb,
    encode_join = encode_join,
    encode_leave = encode_leave,
    decode_change = decode_change,
    encode_ack = encode_ack,
    encode_deadline_ack = encode_deadline_ack,
    new_cache = cache.new,
    next_in_rotation = next_in_rotation,
}
