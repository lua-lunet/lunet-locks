local MAX_DATAGRAM = 65507
local PEER_MAGIC = "\0LUNET_ADVISORY_LOCK_PEER\0"
local PEER_VRR = "\1"
local PEER_APPLICATION = "\2"
local FORWARD_REQUEST = "\1"
local FORWARD_RESPONSE = "\2"
local FORWARD_NOT_LEADER = "\3"
local MESSAGE_ID_BYTES = 16
local MAX_U32 = 4294967295
local MEMBERSHIP_FINGERPRINT_HEX_BYTES = 16
local PEER_HEADER_BYTES = #PEER_MAGIC + 1 + MEMBERSHIP_FINGERPRINT_HEX_BYTES

local function endpoint_key(host: string, port: integer): string
    return host .. ":" .. tostring(port)
end

local function source_is_member(host: string, port: integer, members: {string}): boolean
    local source = endpoint_key(host, port)
    for _, member in ipairs(members) do
        local _, address = member:match("^([^=]+)=(.+)$")
        if address == source then
            return true
        end
    end
    return false
end

local function valid_fingerprint(fingerprint: string): boolean
    return #fingerprint == MEMBERSHIP_FINGERPRINT_HEX_BYTES
        and fingerprint:find("[^0-9a-f]") == nil
end

local function encode_peer(kind: string, fingerprint: string, payload: string): string
    assert(
        kind == PEER_VRR or kind == PEER_APPLICATION,
        "transport: invalid peer packet kind"
    )
    assert(valid_fingerprint(fingerprint), "transport: invalid membership fingerprint")
    assert(
        #payload + PEER_HEADER_BYTES <= MAX_DATAGRAM,
        "transport: peer payload exceeds UDP limit"
    )
    return PEER_MAGIC .. kind .. fingerprint .. payload
end

local function decode_peer(packet: string): (boolean, string, string, string)
    if #packet < PEER_HEADER_BYTES or packet:sub(1, #PEER_MAGIC) ~= PEER_MAGIC then
        return false, "", "", ""
    end
    local kind = packet:sub(#PEER_MAGIC + 1, #PEER_MAGIC + 1)
    local fingerprint = packet:sub(
        #PEER_MAGIC + 2, #PEER_MAGIC + 1 + MEMBERSHIP_FINGERPRINT_HEX_BYTES
    )
    if
        (kind ~= PEER_VRR and kind ~= PEER_APPLICATION)
        or not valid_fingerprint(fingerprint)
    then
        return false, "", "", ""
    end
    return true, kind, fingerprint, packet:sub(PEER_HEADER_BYTES + 1)
end

-- A redirect is deliberately a small fixed-size application packet. It carries
-- no leader index: every fixed ordered membership maps the supplied era+view
-- to its leader through the local replica adapter.
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

local function encode_not_leader(
    message_id: string, era: integer, view: integer
): string
    assert(#message_id == MESSAGE_ID_BYTES, "transport: invalid redirect message ID")
    assert(
        era >= 0 and era <= MAX_U32 and era == math.floor(era),
        "transport: invalid redirect era"
    )
    assert(
        view >= 0 and view <= MAX_U32 and view == math.floor(view),
        "transport: invalid redirect view"
    )
    return FORWARD_NOT_LEADER .. message_id .. u32_bytes(era) .. u32_bytes(view)
end

local function decode_not_leader(payload: string): (boolean, string, integer, integer)
    if
        #payload ~= 1 + MESSAGE_ID_BYTES + 4 + 4
        or payload:sub(1, 1) ~= FORWARD_NOT_LEADER
    then
        return false, "", 0, 0
    end
    local era = parse_u32(payload:sub(2 + MESSAGE_ID_BYTES, 1 + MESSAGE_ID_BYTES + 4))
    local view = parse_u32(
        payload:sub(2 + MESSAGE_ID_BYTES + 4, 1 + MESSAGE_ID_BYTES + 4 + 4)
    )
    return true, payload:sub(2, 1 + MESSAGE_ID_BYTES), era, view
end

local function valid_not_leader(
    expected_source: integer, actual_source: integer
): boolean
    return expected_source >= 0 and actual_source == expected_source
end

local function feed_ndjson(
    buffer: string, chunk: string, max_size: integer
): ({string}, string)
    assert(
        max_size > 0 and max_size <= MAX_DATAGRAM, "transport: invalid message limit"
    )
    local pending = buffer .. chunk
    local messages: {string} = {}
    while true do
        local newline = pending:find("\n", 1, true)
        if newline == nil then
            assert(
                #pending <= max_size, "transport: incomplete request exceeds UDP limit"
            )
            return messages, pending
        end
        local message = pending:sub(1, newline - 1)
        pending = pending:sub(newline + 1)
        if message:sub(-1) == "\r" then
            message = message:sub(1, -2)
        end
        assert(
            #message > 0 and #message <= max_size,
            "transport: invalid NDJSON request size"
        )
        table.insert(messages, message)
    end
end

return {
    MAX_DATAGRAM = MAX_DATAGRAM,
    PEER_MAGIC = PEER_MAGIC,
    PEER_VRR = PEER_VRR,
    PEER_APPLICATION = PEER_APPLICATION,
    PEER_HEADER_BYTES = PEER_HEADER_BYTES,
    FORWARD_REQUEST = FORWARD_REQUEST,
    FORWARD_RESPONSE = FORWARD_RESPONSE,
    FORWARD_NOT_LEADER = FORWARD_NOT_LEADER,
    MESSAGE_ID_BYTES = MESSAGE_ID_BYTES,
    MEMBERSHIP_FINGERPRINT_HEX_BYTES = MEMBERSHIP_FINGERPRINT_HEX_BYTES,
    source_is_member = source_is_member,
    encode_peer = encode_peer,
    decode_peer = decode_peer,
    encode_not_leader = encode_not_leader,
    decode_not_leader = decode_not_leader,
    valid_not_leader = valid_not_leader,
    feed_ndjson = feed_ndjson,
}
