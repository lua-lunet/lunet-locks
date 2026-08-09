local MAX_DATAGRAM = 65507
local FORWARD_MAGIC = "\0LUNET_ADVISORY_LOCK_FORWARD\0"

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

local function encode_forward(payload: string): string
    assert(
        #payload + #FORWARD_MAGIC <= MAX_DATAGRAM,
        "transport: forwarding payload exceeds UDP limit"
    )
    return FORWARD_MAGIC .. payload
end

local function decode_forward(packet: string): (boolean, string)
    if packet:sub(1, #FORWARD_MAGIC) == FORWARD_MAGIC then
        return true, packet:sub(#FORWARD_MAGIC + 1)
    end
    return false, packet
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
    FORWARD_MAGIC = FORWARD_MAGIC,
    source_is_member = source_is_member,
    encode_forward = encode_forward,
    decode_forward = decode_forward,
    feed_ndjson = feed_ndjson,
}
