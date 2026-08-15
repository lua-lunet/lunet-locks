local ffi = require("ffi")

ffi.cdef(
    [[
    int32_t lunet_lock_node_new(
        size_t members_len, const uint8_t* members,
        size_t own_len, const uint8_t* own,
        size_t state_len, const uint8_t* state,
        void** out);
    void lunet_lock_node_free(void* node);
    int32_t lunet_lock_node_request(void* node, size_t json_len, const uint8_t* json);
    int32_t lunet_lock_node_receive(
        void* node, uint32_t from, size_t len, const uint8_t* data);
    int32_t lunet_lock_node_idle(void* node);
    int32_t lunet_lock_node_leader_timeout(void* node);
    int32_t lunet_lock_node_recover(void* node);
    int32_t lunet_lock_node_status(
        void* node, uint32_t* status, uint32_t* leader,
        uint32_t* era, uint32_t* view);
    int32_t lunet_lock_node_leader_for_view(
        void* node, uint32_t era, uint32_t view, uint32_t* leader);
    int32_t lunet_lock_node_next(
        void* node,
        uint32_t* kind, uint32_t* to,
        uint32_t* era, uint32_t* view,
        uint32_t* slot_hi, uint32_t* slot_lo,
        uint8_t* message_id,
        size_t capacity, size_t* len, uint8_t* data);
]]
)

local MAX_DATAGRAM = 65507
local MESSAGE_ID_BYTES = 16
local NOT_LEADER = -8

local errors: {integer: string} = {
    [-1] = "invalid argument",
    [-2] = "invalid configuration or recovery state",
    [-4] = "invalid client JSON",
    [-5] = "invalid replication message",
    [-6] = "message exceeds one UDP packet",
    [-7] = "lock service execution failed",
    [-8] = "not the leader",
    [-9] = "replica faulted",
    [-127] = "native panic",
}

local record Native
    lunet_lock_node_new: function(
        integer, any, integer, any, integer, any, {any}
    ): number
    lunet_lock_node_free: function(any)
    lunet_lock_node_request: function(any, integer, any): number
    lunet_lock_node_receive: function(any, integer, integer, any): number
    lunet_lock_node_idle: function(any): number
    lunet_lock_node_leader_timeout: function(any): number
    lunet_lock_node_recover: function(any): number
    lunet_lock_node_status: function(
        any, {number}, {number}, {number}, {number}
    ): number
    lunet_lock_node_leader_for_view: function(any, integer, integer, {number}): number
    lunet_lock_node_next: function(
        any,
        {number},
        {number},
        {number},
        {number},
        {number},
        {number},
        any,
        integer,
        {number},
        any
    ): number
end

local record DebugInfo
    source: string
end

local function load_native(): Native
    local path = os.getenv("LUNET_ADVISORY_LOCK_LIB")
    if path == nil or path == "" then
        local suffix = ffi.os == "OSX" and "dylib" or "so"
        local source = (debug.getinfo(1, "S") as DebugInfo).source
        local dir = source:match("^@(.+)/[^/]+$") or "."
        path = dir
            .. "/../ext/advisory_lock/target/release/liblunet_advisory_lock."
            .. suffix
    end
    return ffi.load(path) as Native
end

local native = load_native()

local record advisory_lock
    record Node
        _handle: any
        _kind: {number}
        _to: {number}
        _era: {number}
        _view: {number}
        _slot_hi: {number}
        _slot_lo: {number}
        _message_id: any
        _len: {number}
        _buffer: any
        _status: {number}
        _leader: {number}

        request: function(Node, string): boolean, string
        receive: function(Node, integer, string): boolean
        idle: function(Node): boolean
        leader_timeout: function(Node): boolean
        recover: function(Node): boolean
        status: function(Node): Status
        leader_for_view: function(Node, integer, integer): integer
        outputs: function(Node): {Output}
        close: function(Node): boolean
    end

    record Output
        kind: string
        replica: integer
        era: integer
        view: integer
        slot: integer
        message_id: string
        bytes: string
    end

    record Status
        state: string
        leader: integer
        era: integer
        view: integer
    end

    new: function({string}, string, string): Node
end

local Node = advisory_lock.Node
local Output = advisory_lock.Output
local Status = advisory_lock.Status

local function check(rc: integer): boolean
    if rc < 0 then
        error("advisory_lock: " .. (errors[rc] or ("native error " .. tostring(rc))), 3)
    end
    return true
end

local function output_kind(kind_number: integer): string
    if kind_number == 1 then
        return "send"
    elseif kind_number == 2 then
        return "reply"
    end
    error("advisory_lock: invalid output kind " .. tostring(kind_number), 3)
end

function Node:request(json: string): boolean, string
    local rc = (
        tonumber(native.lunet_lock_node_request(self._handle, #json, json)) or -127
    ) as integer
    if rc == NOT_LEADER then
        return nil, "not_leader"
    end
    return check(rc)
end

function Node:receive(from: integer, bytes: string): boolean
    return check(
        (
            tonumber(native.lunet_lock_node_receive(self._handle, from, #bytes, bytes))
            or -127
        ) as integer
    )
end

function Node:idle(): boolean
    return check(
        (tonumber(native.lunet_lock_node_idle(self._handle)) or -127) as integer
    )
end

function Node:leader_timeout(): boolean
    return check(
        (
            tonumber(native.lunet_lock_node_leader_timeout(self._handle)) or -127
        ) as integer
    )
end

function Node:recover(): boolean
    return check(
        (tonumber(native.lunet_lock_node_recover(self._handle)) or -127) as integer
    )
end

function Node:status(): Status
    check(
        (
            tonumber(
                native.lunet_lock_node_status(
                    self._handle, self._status, self._leader, self._era, self._view
                )
            ) or -127
        ) as integer
    )
    local state_number = (tonumber(self._status[0]) or 0) as integer
    local state: string
    if state_number == 0 then
        state = "normal"
    elseif state_number == 1 then
        state = "view_change"
    elseif state_number == 2 then
        state = "recovering"
    elseif state_number == 3 then
        state = "replaying"
    else
        error("advisory_lock: invalid native status " .. tostring(state_number), 2)
    end
    return {
        state = state,
        leader = (tonumber(self._leader[0]) or 0) as integer,
        era = (tonumber(self._era[0]) or 0) as integer,
        view = (tonumber(self._view[0]) or 0) as integer,
    }
end

function Node:leader_for_view(era: integer, view: integer): integer
    assert(era >= 0, "advisory_lock: era must be non-negative")
    assert(view >= 0, "advisory_lock: view must be non-negative")
    check(
        (
            tonumber(
                native.lunet_lock_node_leader_for_view(
                    self._handle, era, view, self._leader
                )
            ) or -127
        ) as integer
    )
    return (tonumber(self._leader[0]) or -1) as integer
end

function Node:outputs(): {Output}
    local outputs: {Output} = {}
    while true do
        local rc = (
            tonumber(
                native.lunet_lock_node_next(
                    self._handle,
                    self._kind,
                    self._to,
                    self._era,
                    self._view,
                    self._slot_hi,
                    self._slot_lo,
                    self._message_id,
                    MAX_DATAGRAM,
                    self._len,
                    self._buffer
                )
            ) or -127
        ) as integer
        check(rc)
        if rc == 0 then
            return outputs
        end
        local len = (tonumber(self._len[0]) or 0) as integer
        local slot_hi = (tonumber(self._slot_hi[0]) or 0) as integer
        local slot_lo = (tonumber(self._slot_lo[0]) or 0) as integer
        table.insert(
            outputs,
            {
                kind = output_kind((tonumber(self._kind[0]) or 0) as integer),
                replica = (tonumber(self._to[0]) or 0) as integer,
                era = (tonumber(self._era[0]) or 0) as integer,
                view = (tonumber(self._view[0]) or 0) as integer,
                slot = slot_hi * 4294967296 + slot_lo,
                message_id = ffi.string(self._message_id, MESSAGE_ID_BYTES),
                bytes = ffi.string(self._buffer, len),
            }
        )
    end
end

function Node:close(): boolean
    if self._handle ~= nil then
        ffi.gc(self._handle, nil)
        native.lunet_lock_node_free(self._handle)
        self._handle = nil
    end
    return true
end

local M: advisory_lock = {}

function M.new(members: {string}, own: string, state_path: string): Node
    assert(#members >= 3, "advisory_lock: at least three members are required")
    for _, member in ipairs(members) do
        assert(
            member:find("\0", 1, true) == nil, "advisory_lock: membership contains NUL"
        )
    end
    assert(own:find("\0", 1, true) == nil, "advisory_lock: own member contains NUL")
    assert(
        state_path ~= "" and state_path:find("\0", 1, true) == nil,
        "advisory_lock: invalid state path"
    )
    local packed = table.concat(members, "\0")
    local out = ffi.new("void*[1]") as {any}
    check(
        (
            tonumber(
                native.lunet_lock_node_new(
                    #packed, packed, #own, own, #state_path, state_path, out
                )
            ) or -127
        ) as integer
    )
    local handle: any = ffi.gc(out[0], native.lunet_lock_node_free)
    return {
        _handle = handle,
        _kind = ffi.new("uint32_t[1]") as {number},
        _to = ffi.new("uint32_t[1]") as {number},
        _era = ffi.new("uint32_t[1]") as {number},
        _view = ffi.new("uint32_t[1]") as {number},
        _slot_hi = ffi.new("uint32_t[1]") as {number},
        _slot_lo = ffi.new("uint32_t[1]") as {number},
        _message_id = ffi.new("uint8_t[?]", MESSAGE_ID_BYTES),
        _len = ffi.new("size_t[1]") as {number},
        _buffer = ffi.new("uint8_t[?]", MAX_DATAGRAM),
        _status = ffi.new("uint32_t[1]") as {number},
        _leader = ffi.new("uint32_t[1]") as {number},
        request = Node.request,
        receive = Node.receive,
        idle = Node.idle,
        leader_timeout = Node.leader_timeout,
        recover = Node.recover,
        status = Node.status,
        leader_for_view = Node.leader_for_view,
        outputs = Node.outputs,
        close = Node.close,
    }
end

return M
