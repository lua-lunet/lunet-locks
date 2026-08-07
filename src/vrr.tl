local ffi = require("ffi")

ffi.cdef(
    [[
   int32_t vrr_node_new(
      size_t members_len, const uint8_t* members,
      size_t own_len, const uint8_t* own,
      void** out);
   void vrr_node_free(void* node);
   int32_t vrr_node_request(
      void* node,
      size_t execution_time_len, const uint8_t* execution_time,
      size_t json_len, const uint8_t* json);
   int32_t vrr_node_receive(
      void* node, uint32_t from,
      size_t len, const uint8_t* data);
   int32_t vrr_node_idle(void* node);
   int32_t vrr_node_leader_timeout(void* node);
   int32_t vrr_node_recover(
      void* node, size_t nonce_len, const uint8_t* nonce);
   int32_t vrr_node_next(
      void* node,
      uint32_t* out_kind, uint32_t* out_to,
      uint32_t* out_tag, uint32_t* out_epoch,
      uint32_t* out_slot_hi, uint32_t* out_slot_lo,
      size_t capacity, size_t* out_len, uint8_t* out_data);
]]
)

local MAX_DATAGRAM = 65507

local errors: {integer: string} = {
    [-1] = "invalid argument",
    [-2] = "invalid membership",
    [-3] = "invalid unsigned integer",
    [-4] = "invalid client JSON",
    [-5] = "invalid VRR message",
    [-6] = "message exceeds one UDP packet",
    [-7] = "service execution failed",
    [-127] = "native panic",
}

local record Native
    vrr_node_new: function(integer, any, integer, any, {any}): number
    vrr_node_free: function(any)
    vrr_node_request: function(any, integer, any, integer, any): number
    vrr_node_receive: function(any, integer, integer, any): number
    vrr_node_idle: function(any): number
    vrr_node_leader_timeout: function(any): number
    vrr_node_recover: function(any, integer, any): number
    vrr_node_next: function(
        any,
        {number},
        {number},
        {number},
        {number},
        {number},
        {number},
        integer,
        {number},
        any
    ): number
end

local record DebugInfo
    source: string
end

local function load_native(): Native
    local path = os.getenv("LUNET_VRR_LIB")
    if path == nil or path == "" then
        local suffix = ffi.os == "OSX" and "dylib" or "so"
        local source = (debug.getinfo(1, "S") as DebugInfo).source
        local dir = source:match("^@(.+)/[^/]+$") or "."
        path = dir .. "/../ext/vrr/target/release/libvrr." .. suffix
    end
    return ffi.load(path) as Native
end

local native = load_native()

local record Header
    tag: integer
    epoch: integer
    slot_hi: integer
    slot_lo: integer
end

local record Output
    kind: string
    replica: integer
    header: Header
    bytes: string
end

local record Node
    _handle: any
    _kind: {number}
    _to: {number}
    _tag: {number}
    _epoch: {number}
    _slot_hi: {number}
    _slot_lo: {number}
    _len: {number}
    _buffer: any

    request: function(Node, string, string): {Output}
    receive: function(Node, integer, string): {Output}
    idle: function(Node): {Output}
    leader_timeout: function(Node): {Output}
    recover: function(Node, string): {Output}
    close: function(Node): boolean
end

local function check(rc: integer): boolean
    if rc < 0 then
        error("vrr: " .. (errors[rc] or ("native error " .. tostring(rc))), 3)
    end
    return true
end

local function drain(self: Node): {Output}
    local outputs: {Output} = {}
    while true do
        local rc = (
            tonumber(
                native.vrr_node_next(
                    self._handle,
                    self._kind,
                    self._to,
                    self._tag,
                    self._epoch,
                    self._slot_hi,
                    self._slot_lo,
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

        local kind_number = (tonumber(self._kind[0]) or 0) as integer
        local kind: string
        if kind_number == 1 then
            kind = "broadcast"
        elseif kind_number == 2 then
            kind = "send"
        elseif kind_number == 3 then
            kind = "reply"
        else
            error("vrr: invalid output kind " .. tostring(kind_number), 2)
        end

        local len = (tonumber(self._len[0]) or 0) as integer
        table.insert(
            outputs,
            {
                kind = kind,
                replica = (tonumber(self._to[0]) or 0) as integer,
                header = {
                    tag = (tonumber(self._tag[0]) or 0) as integer,
                    epoch = (tonumber(self._epoch[0]) or 0) as integer,
                    slot_hi = (tonumber(self._slot_hi[0]) or 0) as integer,
                    slot_lo = (tonumber(self._slot_lo[0]) or 0) as integer,
                },
                bytes = ffi.string(self._buffer, len),
            }
        )
    end
end

function Node:request(execution_time: string, json: string): {Output}
    check(
        (
            tonumber(
                native.vrr_node_request(
                    self._handle, #execution_time, execution_time, #json, json
                )
            ) or -127
        ) as integer
    )
    return drain(self)
end

function Node:receive(from: integer, bytes: string): {Output}
    check(
        (
            tonumber(native.vrr_node_receive(self._handle, from, #bytes, bytes)) or -127
        ) as integer
    )
    return drain(self)
end

function Node:idle(): {Output}
    check((tonumber(native.vrr_node_idle(self._handle)) or -127) as integer)
    return drain(self)
end

function Node:leader_timeout(): {Output}
    check((tonumber(native.vrr_node_leader_timeout(self._handle)) or -127) as integer)
    return drain(self)
end

function Node:recover(nonce: string): {Output}
    check(
        (
            tonumber(native.vrr_node_recover(self._handle, #nonce, nonce)) or -127
        ) as integer
    )
    return drain(self)
end

function Node:close(): boolean
    if self._handle ~= nil then
        ffi.gc(self._handle, nil)
        native.vrr_node_free(self._handle)
        self._handle = nil
    end
    return true
end

local record vrr
    new: function({string}, string): Node
end

local M: vrr = {}

function M.new(members: {string}, own: string): Node
    for _, member in ipairs(members) do
        assert(member:find("\0", 1, true) == nil, "membership address contains NUL")
    end
    assert(own:find("\0", 1, true) == nil, "own address contains NUL")
    local packed = table.concat(members, "\0")
    local out = ffi.new("void*[1]") as {any}
    check(
        (
            tonumber(native.vrr_node_new(#packed, packed, #own, own, out)) or -127
        ) as integer
    )

    local handle: any = ffi.gc(out[0], native.vrr_node_free)
    return {
        _handle = handle,
        _kind = ffi.new("uint32_t[1]") as {number},
        _to = ffi.new("uint32_t[1]") as {number},
        _tag = ffi.new("uint32_t[1]") as {number},
        _epoch = ffi.new("uint32_t[1]") as {number},
        _slot_hi = ffi.new("uint32_t[1]") as {number},
        _slot_lo = ffi.new("uint32_t[1]") as {number},
        _len = ffi.new("size_t[1]") as {number},
        _buffer = ffi.new("uint8_t[?]", MAX_DATAGRAM),
        request = Node.request,
        receive = Node.receive,
        idle = Node.idle,
        leader_timeout = Node.leader_timeout,
        recover = Node.recover,
        close = Node.close,
    }
end

return M
