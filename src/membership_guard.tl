local record Guard
    configured: string
    dirty: boolean

    accept_fingerprint: function(Guard, string): boolean
    allows_client_operation: function(Guard): boolean
end

local function accept_fingerprint(self: Guard, received: string): boolean
    if self.dirty or received ~= self.configured then
        self.dirty = true
        return false
    end
    return true
end

local function allows_client_operation(self: Guard): boolean
    return not self.dirty
end

local record membership_guard
    new: function(string): Guard
end

local M: membership_guard = {}

function M.new(configured: string): Guard
    assert(
        #configured == 16 and configured:find("[^0-9a-f]") == nil,
        "membership_guard: invalid configured fingerprint"
    )
    return {
        configured = configured,
        dirty = false,
        accept_fingerprint = accept_fingerprint,
        allows_client_operation = allows_client_operation,
    }
end

return M
