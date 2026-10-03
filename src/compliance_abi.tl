-- The Compliance ABI bindings: the nine `unsafe_*` exports of the
-- `compatibility_suite` build shape, plus the ordinary drive verbs the
-- executor needs to move the same node (`docs/src/compliance-abi.md`).
--
-- These symbols do not exist in a production library. Requiring this
-- module is therefore requiring a `compatibility_suite` build: there is
-- no runtime flag, no fallback path, and nothing here a default shape
-- could satisfy. The library is resolved from `LUNET_ADVISORY_LOCK_LIB`
-- exactly as `src/advisory_lock.tl` resolves it, so the gate points this
-- module at the compliance shape and a production boot never loads it.
--
-- Every vector read follows the pull-style buffer contract of
-- `lunet_lock_node_fault` (`ffi.rs:3451-3478`): a sizing call with a
-- null buffer answers `TOO_LARGE` and the length it needs, and nothing
-- allocates across the boundary.

local ffi = require("ffi")

ffi.cdef(
    [[
    int32_t lunet_lock_node_unsafe_open_compliance(
        size_t members_len, const uint8_t* members,
        size_t own_len, const uint8_t* own,
        size_t state_len, const uint8_t* state,
        uint64_t primary_timeout,
        void** out);
    int32_t lunet_lock_node_unsafe_set_compliance_clock(
        void* node, uint64_t at);
    int32_t lunet_lock_node_unsafe_propose_opaque(
        void* node, uint64_t id_msb, uint64_t id_lsb,
        size_t payload_len, const uint8_t* payload);
    int32_t lunet_lock_node_unsafe_reconfigure_opaque(
        void* node, size_t op_json_len, const uint8_t* op_json);
    int32_t lunet_lock_node_unsafe_frontiers(
        void* node, uint64_t* accepted, uint64_t* committed,
        uint64_t* applied);
    int32_t lunet_lock_node_unsafe_journal_entries(
        void* node, uint8_t* out_data, size_t capacity, size_t* out_len);
    int32_t lunet_lock_node_unsafe_membership(
        void* node, size_t* out_count, uint32_t* out_ids,
        uint64_t* out_weights, size_t capacity);
    int32_t lunet_lock_node_unsafe_witnesses(
        void* node, size_t* out_count, uint32_t* out_ids, size_t capacity);
    int32_t lunet_lock_node_unsafe_marker_log(
        void* node, uint8_t* out_data, size_t capacity, size_t* out_len);

    void lunet_lock_node_free(void* node);
    int32_t lunet_lock_node_receive(
        void* node, uint32_t from, size_t len, const uint8_t* data);
    int32_t lunet_lock_node_idle(void* node);
    int32_t lunet_lock_node_recover(void* node);
    int32_t lunet_lock_node_stop(void* node);
    int32_t lunet_lock_node_own_id(void* node, uint32_t* id);
    int32_t lunet_lock_node_status(
        void* node, uint32_t* status, uint32_t* leader,
        uint32_t* era, uint32_t* view);
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
local OK = 0
local TOO_LARGE = -6
local OUTPUT_SEND = 1

local errors: {integer: string} = {
    [-1] = "invalid argument",
    [-2] = "invalid configuration or recovery state",
    [-4] = "invalid client JSON",
    [-5] = "invalid replication message",
    [-6] = "message exceeds one UDP packet",
    [-7] = "lock service execution failed",
    [-8] = "not the leader",
    [-9] = "replica faulted",
    [-10] = "node stopped",
    [-127] = "native panic",
}

local record Native
    lunet_lock_node_unsafe_open_compliance: function(
        integer, any, integer, any, integer, any, integer, {any}
    ): number
    lunet_lock_node_unsafe_set_compliance_clock: function(any, integer): number
    lunet_lock_node_unsafe_propose_opaque: function(
        any, integer, integer, integer, any
    ): number
    lunet_lock_node_unsafe_reconfigure_opaque: function(any, integer, any): number
    lunet_lock_node_unsafe_frontiers: function(
        any, {number}, {number}, {number}
    ): number
    lunet_lock_node_unsafe_journal_entries: function(
        any, any, integer, {number}
    ): number
    lunet_lock_node_unsafe_membership: function(
        any, {number}, any, any, integer
    ): number
    lunet_lock_node_unsafe_witnesses: function(any, {number}, any, integer): number
    lunet_lock_node_unsafe_marker_log: function(any, any, integer, {number}): number
    lunet_lock_node_free: function(any)
    lunet_lock_node_receive: function(any, integer, integer, any): number
    lunet_lock_node_idle: function(any): number
    lunet_lock_node_recover: function(any): number
    lunet_lock_node_stop: function(any): number
    lunet_lock_node_own_id: function(any, {number}): number
    lunet_lock_node_status: function(
        any, {number}, {number}, {number}, {number}
    ): number
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
        local suffix = (ffi.os == "OSX") and "dylib" or "so"
        local source = (debug.getinfo(1, "S") as DebugInfo).source
        local dir = source:match("^@(.+)/[^/]+$") or "."
        path = dir
            .. "/../ext/advisory_lock/target/release/liblunet_advisory_lock."
            .. suffix
    end
    return ffi.load(path) as Native
end

local native = load_native()

local record Output
    kind: integer
    to: integer
    bytes: string
end

local record compliance_abi
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
        _word: {number}
        _leader: {number}
        _era_out: {number}
        _view_out: {number}
        _accepted: {number}
        _committed: {number}
        _applied: {number}

        set_clock: function(Node, integer)
        propose_opaque: function(Node, integer, integer, string)
        reconfigure_opaque: function(Node, string)
        frontiers: function(Node): integer, integer, integer
        journal_entries: function(Node): string
        membership: function(Node): {integer}, {integer}
        witnesses: function(Node): {integer}
        marker_log: function(Node): string
        receive: function(Node, integer, string)
        idle: function(Node)
        recover: function(Node)
        stop: function(Node): boolean
        own_id: function(Node): integer
        status: function(Node): integer, integer, integer
        outputs: function(Node): {Output}
        free: function(Node): boolean
    end

    OUTPUT_SEND: integer

    open: function(string, string, string, integer): Node, string
end

local Node = compliance_abi.Node

local function check(rc: integer, what: string): integer
    if rc < 0 then
        error(
            "compliance_abi: "
                .. what
                .. ": "
                .. (errors[rc] or ("native error " .. tostring(rc))),
            3
        )
    end
    return rc
end

-- A NUL-terminated payload out on the pull-style contract: the sizing
-- call first, then a buffer the library's own reported length sizes.
-- A sizing call answers `TOO_LARGE` and the length it needs, or `OK`
-- and nothing for a caller that needs no room. Neither is a refusal.
local function sizing(rc: integer, what: string)
    if rc ~= OK and rc ~= TOO_LARGE then
        check(rc, what)
    end
end

local function copy_out(read: function(any, integer, {number}): number): string
    local needed = ffi.new("size_t[1]") as {number}
    sizing((read(nil, 0, needed)) as integer, "a sizing call")
    local size = tonumber(needed[0]) as integer
    if size == 0 then
        return ""
    end
    local data = ffi.new("uint8_t[?]", size + 1) as any
    check((read(data, size + 1, needed)) as integer, "a payload read")
    return ffi.string(data, size) as string
end

function Node:set_clock(at: integer)
    check(
        (
            tonumber(
                native.lunet_lock_node_unsafe_set_compliance_clock(self._handle, at)
            ) or -127
        ) as integer,
        "the executor clock"
    )
end

function Node:propose_opaque(msb: integer, lsb: integer, payload: string)
    check(
        (
            tonumber(
                native.lunet_lock_node_unsafe_propose_opaque(
                    self._handle, msb, lsb, #payload, payload
                )
            ) or -127
        ) as integer,
        "an opaque proposal"
    )
end

function Node:reconfigure_opaque(json: string)
    check(
        (
            tonumber(
                native.lunet_lock_node_unsafe_reconfigure_opaque(
                    self._handle, (#json) as integer, json
                )
            ) or -127
        ) as integer,
        "an opaque reconfiguration"
    )
end

function Node:frontiers(): integer, integer, integer
    check(
        (
            tonumber(
                native.lunet_lock_node_unsafe_frontiers(
                    self._handle, self._accepted, self._committed, self._applied
                )
            ) or -127
        ) as integer,
        "the frontiers"
    )
    return (tonumber(self._accepted[0]) or 0) as integer,
        (tonumber(self._committed[0]) or 0) as integer,
        (tonumber(self._applied[0]) or 0) as integer
end

function Node:journal_entries(): string
    return copy_out(
        function(data: any, capacity: integer, length: {number}): number
            return (
                tonumber(
                    native.lunet_lock_node_unsafe_journal_entries(
                        self._handle, data, capacity, length
                    )
                ) or -127
            ) as integer
        end
    )
end

function Node:marker_log(): string
    return copy_out(
        function(data: any, capacity: integer, length: {number}): number
            return (
                tonumber(
                    native.lunet_lock_node_unsafe_marker_log(
                        self._handle, data, capacity, length
                    )
                ) or -127
            ) as integer
        end
    )
end

-- The folded configuration's record: the succession order and each
-- member's weight, in the same order. A node holding no era record
-- reports zero members, which is the empty record, not a refusal.
function Node:membership(): {integer}, {integer}
    local count = ffi.new("size_t[1]") as {number}
    sizing(
        (
            tonumber(
                native.lunet_lock_node_unsafe_membership(
                    self._handle, count, nil, nil, 0
                )
            ) or -127
        ) as integer,
        "a membership count"
    )
    local total = tonumber(count[0]) as integer
    if total == 0 then
        return {}, {}
    end
    local ids = ffi.new("uint32_t[?]", total) as {number}
    local weights = ffi.new("uint64_t[?]", total) as {number}
    check(
        (
            tonumber(
                native.lunet_lock_node_unsafe_membership(
                    self._handle, count, ids, weights, total
                )
            ) or -127
        ) as integer,
        "a membership read"
    )
    local out_ids: {integer} = {}
    local out_weights: {integer} = {}
    for index = 0, total - 1 do
        table.insert(out_ids, tonumber(ids[index]) as integer)
        table.insert(out_weights, tonumber(weights[index]) as integer)
    end
    return out_ids, out_weights
end

function Node:witnesses(): {integer}
    local count = ffi.new("size_t[1]") as {number}
    sizing(
        (
            tonumber(
                native.lunet_lock_node_unsafe_witnesses(self._handle, count, nil, 0)
            ) or -127
        ) as integer,
        "a witness count"
    )
    local total = tonumber(count[0]) as integer
    if total == 0 then
        return {}
    end
    local ids = ffi.new("uint32_t[?]", total) as {number}
    check(
        (
            tonumber(
                native.lunet_lock_node_unsafe_witnesses(self._handle, count, ids, total)
            ) or -127
        ) as integer,
        "a witness read"
    )
    local out: {integer} = {}
    for index = 0, total - 1 do
        table.insert(out, tonumber(ids[index]) as integer)
    end
    return out
end

function Node:receive(from: integer, bytes: string)
    check(
        (
            tonumber(
                native.lunet_lock_node_receive(
                    self._handle, from, (#bytes) as integer, bytes
                )
            ) or -127
        ) as integer,
        "a delivery"
    )
end

function Node:idle()
    check(
        (tonumber(native.lunet_lock_node_idle(self._handle)) or -127) as integer,
        "a timer event"
    )
end

function Node:recover()
    check(
        (tonumber(native.lunet_lock_node_recover(self._handle)) or -127) as integer,
        "a recovery announcement"
    )
end

function Node:stop(): boolean
    return check(
        (tonumber(native.lunet_lock_node_stop(self._handle)) or -127) as integer,
        "the graceful stop"
    ) == OK
end

function Node:own_id(): integer
    check(
        (tonumber(native.lunet_lock_node_own_id(self._handle, self._word)) or -127)
            as integer,
        "the own identity"
    )
    return (tonumber(self._word[0]) or -1) as integer
end

-- The status's snapshot word, its era and its view. The word is the
-- core's own numbering (`progress.rs:95-103`) and the caller renders it:
-- `src/advisory_lock.tl`'s human-facing names are not the corpus's
-- grammar, so the compliance host never borrows them.
function Node:status(): integer, integer, integer
    check(
        (
            tonumber(
                native.lunet_lock_node_status(
                    self._handle,
                    self._word,
                    self._leader,
                    self._era_out,
                    self._view_out
                )
            ) or -127
        ) as integer,
        "the status"
    )
    return (tonumber(self._word[0]) or 0) as integer,
        (tonumber(self._era_out[0]) or 0) as integer,
        (tonumber(self._view_out[0]) or 0) as integer
end

-- Every queued output, in arrival order. The compliance executor queues
-- only unicast datagrams, and nothing answers an opaque proposal, so any
-- other output kind is an adapter bug and is refused.
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
        check(rc, "an output read")
        if rc == OK then
            return outputs
        end
        local kind = (tonumber(self._kind[0]) or 0) as integer
        if kind ~= OUTPUT_SEND then
            error(
                "compliance_abi: the compliance executor queues only unicast "
                    .. "datagrams, got output kind "
                    .. tostring(kind),
                2
            )
        end
        table.insert(
            outputs,
            {
                kind = kind,
                to = (tonumber(self._to[0]) or 0) as integer,
                bytes = ffi.string(
                    self._buffer, (tonumber(self._len[0]) or 0) as integer
                ) as string,
            }
        )
    end
end

function Node:free(): boolean
    if self._handle ~= nil then
        native.lunet_lock_node_free(self._handle)
        self._handle = nil
    end
    return true
end

-- The compliance constructor: the same marker store with the lock-event
-- journal disabled and the corpus's deterministic harness rules armed.
-- This is the only entry that arms them.
function compliance_abi.open(
    members: string, own: string, state_path: string, primary_timeout: integer
): Node, string
    local out = ffi.new("void*[1]") as {any}
    local rc = (
        tonumber(
            native.lunet_lock_node_unsafe_open_compliance(
                (#members) as integer,
                members,
                (#own) as integer,
                own,
                (#state_path) as integer,
                state_path,
                primary_timeout,
                out
            )
        ) or -127
    ) as integer
    if rc < 0 then
        return nil,
            "the compliance boot refused: "
                .. (errors[rc] or ("native error " .. tostring(rc)))
    end
    local node: Node = {
        _handle = out[0],
        _kind = ffi.new("uint32_t[1]") as {number},
        _to = ffi.new("uint32_t[1]") as {number},
        _era = ffi.new("uint32_t[1]") as {number},
        _view = ffi.new("uint32_t[1]") as {number},
        _slot_hi = ffi.new("uint32_t[1]") as {number},
        _slot_lo = ffi.new("uint32_t[1]") as {number},
        _message_id = ffi.new("uint8_t[?]", MESSAGE_ID_BYTES),
        _len = ffi.new("size_t[1]") as {number},
        _buffer = ffi.new("uint8_t[?]", MAX_DATAGRAM),
        _word = ffi.new("uint32_t[1]") as {number},
        _leader = ffi.new("uint32_t[1]") as {number},
        _era_out = ffi.new("uint32_t[1]") as {number},
        _view_out = ffi.new("uint32_t[1]") as {number},
        _accepted = ffi.new("uint64_t[1]") as {number},
        _committed = ffi.new("uint64_t[1]") as {number},
        _applied = ffi.new("uint64_t[1]") as {number},
        set_clock = Node.set_clock,
        propose_opaque = Node.propose_opaque,
        reconfigure_opaque = Node.reconfigure_opaque,
        frontiers = Node.frontiers,
        journal_entries = Node.journal_entries,
        membership = Node.membership,
        witnesses = Node.witnesses,
        marker_log = Node.marker_log,
        receive = Node.receive,
        idle = Node.idle,
        recover = Node.recover,
        stop = Node.stop,
        own_id = Node.own_id,
        status = Node.status,
        outputs = Node.outputs,
        free = Node.free,
    }
    return node, nil
end

-- The two FFI calls with more than eight arguments are excluded from
-- tracing for the reason `src/advisory_lock.tl` states: LuaJIT's arm64
-- JIT compiles their on-stack argument area incorrectly under GC
-- pressure, which corrupts the callee's `out` pointer. The jit module is
-- a LuaJIT builtin with no Teal defs, so it is loaded dynamically.
local builtin = "ji" .. "t"
local ok, jit_any = pcall(require, builtin)
if ok then
    local record JitApi
        off: function(any, boolean)
    end
    local jit_module: JitApi = jit_any as JitApi
    jit_module.off(compliance_abi.open, true)
    jit_module.off(Node.outputs, true)
end

compliance_abi.OUTPUT_SEND = OUTPUT_SEND

return compliance_abi
