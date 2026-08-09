local record Endpoint
    host: string
    port: integer
end

local record Member
    name: string
    endpoint: Endpoint
end

-- Membership bytes are deliberately domain-separated and length-delimited.
-- The lexical member order is validated by `members` before this is called.
local MEMBERSHIP_DOMAIN = "lunet-advisory-lock/membership/v1\0"
local FINGERPRINT_HEX_BYTES = 16

local bit = require("bit")

local SHA256_K = {
    0x428A2F98,
    0x71374491,
    0xB5C0FBCF,
    0xE9B5DBA5,
    0x3956C25B,
    0x59F111F1,
    0x923F82A4,
    0xAB1C5ED5,
    0xD807AA98,
    0x12835B01,
    0x243185BE,
    0x550C7DC3,
    0x72BE5D74,
    0x80DEB1FE,
    0x9BDC06A7,
    0xC19BF174,
    0xE49B69C1,
    0xEFBE4786,
    0x0FC19DC6,
    0x240CA1CC,
    0x2DE92C6F,
    0x4A7484AA,
    0x5CB0A9DC,
    0x76F988DA,
    0x983E5152,
    0xA831C66D,
    0xB00327C8,
    0xBF597FC7,
    0xC6E00BF3,
    0xD5A79147,
    0x06CA6351,
    0x14292967,
    0x27B70A85,
    0x2E1B2138,
    0x4D2C6DFC,
    0x53380D13,
    0x650A7354,
    0x766A0ABB,
    0x81C2C92E,
    0x92722C85,
    0xA2BFE8A1,
    0xA81A664B,
    0xC24B8B70,
    0xC76C51A3,
    0xD192E819,
    0xD6990624,
    0xF40E3585,
    0x106AA070,
    0x19A4C116,
    0x1E376C08,
    0x2748774C,
    0x34B0BCB5,
    0x391C0CB3,
    0x4ED8AA4A,
    0x5B9CCA4F,
    0x682E6FF3,
    0x748F82EE,
    0x78A5636F,
    0x84C87814,
    0x8CC70208,
    0x90BEFFFA,
    0xA4506CEB,
    0xBEF9A3F7,
    0xC67178F2,
}

local function u32(value: number): integer
    return bit.tobit(value)
end

local function sha256_hex(input: string): string
    local length = #input
    local padded = input .. "\128"
    local zeroes = (56 - (#padded % 64)) % 64
    padded = padded .. string.rep("\0", zeroes)
    local high = math.floor(length / 536870912)
    local low = (length * 8) % 4294967296
    local function byte(value: number, shift: number): integer
        return (math.floor(value / (2 ^ shift)) % 256) as integer
    end
    padded = padded
        .. string.char(
            byte(high, 24),
            byte(high, 16),
            byte(high, 8),
            byte(high, 0),
            byte(low, 24),
            byte(low, 16),
            byte(low, 8),
            byte(low, 0)
        )

    local h0: integer = 0x6A09E667
    local h1: integer = 0xBB67AE85
    local h2: integer = 0x3C6EF372
    local h3: integer = 0xA54FF53A
    local h4: integer = 0x510E527F
    local h5: integer = 0x9B05688C
    local h6: integer = 0x1F83D9AB
    local h7: integer = 0x5BE0CD19
    for offset = 1, #padded, 64 do
        local words: {integer} = {}
        for index = 0, 15 do
            local start = offset + index * 4
            words[index + 1] = u32(
                (padded:byte(start) or 0) * 16777216 + (
                    padded:byte(start + 1) or 0
                ) * 65536 + (padded:byte(start + 2) or 0) * 256 + (
                    padded:byte(start + 3) or 0
                )
            )
        end
        for index = 17, 64 do
            local a = words[index - 15]
            local b = words[index - 2]
            local s0 = bit.bxor(bit.ror(a, 7), bit.ror(a, 18), bit.rshift(a, 3))
            local s1 = bit.bxor(bit.ror(b, 17), bit.ror(b, 19), bit.rshift(b, 10))
            words[index] = u32(words[index - 16] + s0 + words[index - 7] + s1)
        end
        local a, b, c, d = h0, h1, h2, h3
        local e, f, g, h = h4, h5, h6, h7
        for index = 1, 64 do
            local s1 = bit.bxor(bit.ror(e, 6), bit.ror(e, 11), bit.ror(e, 25))
            local choose = bit.bxor(bit.band(e, f), bit.band(bit.bnot(e), g))
            local temporary1 = u32(h + s1 + choose + SHA256_K[index] + words[index])
            local s0 = bit.bxor(bit.ror(a, 2), bit.ror(a, 13), bit.ror(a, 22))
            local majority = bit.bxor(bit.band(a, b), bit.band(a, c), bit.band(b, c))
            local temporary2 = u32(s0 + majority)
            h, g, f, e = g, f, e, u32(d + temporary1)
            d, c, b, a = c, b, a, u32(temporary1 + temporary2)
        end
        h0, h1, h2, h3 = u32(h0 + a), u32(h1 + b), u32(h2 + c), u32(h3 + d)
        h4, h5, h6, h7 = u32(h4 + e), u32(h5 + f), u32(h6 + g), u32(h7 + h)
    end
    return bit.tohex(h0)
        .. bit.tohex(h1)
        .. bit.tohex(h2)
        .. bit.tohex(h3)
        .. bit.tohex(h4)
        .. bit.tohex(h5)
        .. bit.tohex(h6)
        .. bit.tohex(h7)
end

local function ipv4(host: string): boolean
    local count = 0
    for part in host:gmatch("[^.]+") do
        if part:find("[^0-9]") ~= nil or #part == 0 or #part > 3 then
            return false
        end
        local value = tonumber(part)
        if
            value == nil
            or value < 0
            or value > 255
            or (#part > 1 and part:sub(1, 1) == "0")
        then
            return false
        end
        count = count + 1
    end
    return count == 4 and host:find("%.") ~= nil
end

local function endpoint(value: string): Endpoint
    local host, port_text = value:match("^(.-):([0-9]+)$")
    assert(
        host ~= nil and port_text ~= nil and ipv4(host),
        "config: endpoint must be literal IPv4:port"
    )
    local port = tonumber(port_text)
    assert(port ~= nil and port >= 1 and port <= 65535, "config: port must be 1..65535")
    return {host = host, port = port as integer}
end

local function member(value: string): Member
    local name, address = value:match("^([^=]+)=(.+)$")
    assert(
        name ~= nil and address ~= nil and name:match("^[%w_-]+$") ~= nil,
        "config: member must be name=IPv4:port"
    )
    return {name = name, endpoint = endpoint(address)}
end

local function members(local_name: string, values: {string}): {Member}
    assert(#values >= 3, "config: at least three members are required")
    local parsed: {Member} = {}
    local seen: {string: boolean} = {}
    local local_count = 0
    local previous = ""
    for _, value in ipairs(values) do
        local current = member(value)
        assert(not seen[current.name], "config: member names must be unique")
        assert(
            previous == "" or previous < current.name,
            "config: members must be lexically sorted"
        )
        seen[current.name] = true
        previous = current.name
        if current.name == local_name then
            local_count = local_count + 1
        end
        table.insert(parsed, current)
    end
    assert(local_count == 1, "config: --node must appear exactly once in membership")
    return parsed
end

local function membership_bytes(ordered: {Member}): string
    local encoded = MEMBERSHIP_DOMAIN .. tostring(#ordered) .. ":"
    for _, current in ipairs(ordered) do
        local host = current.endpoint.host
        local port = tostring(current.endpoint.port)
        encoded = encoded
            .. tostring(#current.name)
            .. ":"
            .. current.name
            .. tostring(#host)
            .. ":"
            .. host
            .. tostring(#port)
            .. ":"
            .. port
    end
    return encoded
end

local function membership_fingerprint(ordered: {Member}): string
    return sha256_hex(membership_bytes(ordered)):sub(1, FINGERPRINT_HEX_BYTES)
end

return {
    endpoint = endpoint,
    member = member,
    members = members,
    membership_bytes = membership_bytes,
    membership_fingerprint = membership_fingerprint,
    FINGERPRINT_HEX_BYTES = FINGERPRINT_HEX_BYTES,
}
