local record Endpoint
    host: string
    port: integer
end

local record Member
    name: string
    endpoint: Endpoint
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

return {endpoint = endpoint, member = member, members = members}
