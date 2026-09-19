-- Base64 (RFC 4648) for the basic-auth header. No rock: ~25 typed lines
-- beat a dependency.

local bit = require("bit")

local record M
    encode: function(string): string
end

local ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"

local function sextet(n: integer, shift: integer): string
    local idx = bit.band(bit.rshift(n, shift), 63) + 1
    return ALPHABET:sub(idx, idx)
end

function M.encode(data: string): string
    local out: {string} = {}
    local len = #data
    local i = 1
    while i <= len do
        local b1 = data:byte(i)
        local b2 = data:byte(i + 1)
        local b3 = data:byte(i + 2)
        local n = b1 * 65536 + (b2 or 0) * 256 + (b3 or 0)
        local chunk = sextet(n, 18) .. sextet(n, 12)
        if b2 then
            chunk = chunk .. sextet(n, 6)
        else
            chunk = chunk .. "="
        end
        if b3 then
            local idx = bit.band(n, 63) + 1
            chunk = chunk .. ALPHABET:sub(idx, idx)
        else
            chunk = chunk .. "="
        end
        out[#out + 1] = chunk
        i = i + 3
    end
    return table.concat(out)
end

return M
