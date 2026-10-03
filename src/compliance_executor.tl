-- The compliance corpus's abstract host interface, over the Compliance
-- ABI (`docs/src/compliance-abi.md`).
--
-- This is the Lua host's counterpart of the reference executor in
-- `ext/advisory_lock/tests/compliance/mod.rs`. The Rust file is the
-- specification and this file is held to it operation for operation: the
-- same fourteen operations, the same queue discipline, the same bounded
-- settle and drain, the same per-node post record, and the same
-- rendering grammars. Where the reference reaches a
-- `lunet_advisory_lock::Node` method directly, this reaches the same
-- method across the C ABI; everything else — the tick bookkeeping, the
-- FIFO wire, the hexadecimal renderings, the marker grammar — is the
-- same derivation.
--
-- The determinism is the executor's own: a logical clock advanced once
-- per tick sweep and carried by every drive between those advances, no
-- wall clock anywhere, and each case's own marker-store files.
--
-- A case record arrives as JSON (the transport of §8 hands the corpus
-- over verbatim), so the operations are read from that document rather
-- than through a second vocabulary: there is no translation layer that
-- could paraphrase a case.

local abi = require("compliance_abi")
local json = require("compliance_json")

-- The settle bound: the bootstrap converges long before this, so a
-- script that cannot settle inside it is a corpus defect, not a slow
-- machine. The drain bound: the post-input cascade is finite and
-- deterministic.
local SETTLE_BOUND = 1000
local DRAIN_BOUND = 1000

-- The vector operations' fixed identity half: part of the fixture
-- (`docs/uvrr-host-compliance.md` §4), never zero, never privileged.
-- It rides the wire as `vect` in the first eight bytes big-endian.
local OP_MSB = 0x76656374

-- The core's own status numbering (`progress.rs:95-103`), rendered in
-- the corpus's vocabulary and never in this host's.
local STATUS_NAMES: {integer: string} = {
    [0] = "Normal",
    [1] = "ViewChange",
    [2] = "Restarting",
    [3] = "Replaying",
    [4] = "Joining",
}

local STATUS_NORMAL = 0

local record compliance_executor
    record Wire
        from: integer
        to: integer
        bytes: string
    end

    record Delivery
        from: string
        to: string
        wire: string
    end

    -- One node's full post record, every field the ABI can observe. An
    -- absent field is unconstrained: the capture of a node that is down
    -- carries its identity and its markers and nothing else.
    record Post
        node: string
        status: string | nil
        era: integer | nil
        view: integer | nil
        accepted: integer | nil
        committed: integer | nil
        applied: integer | nil
        journal: {string} | nil
        members: {string} | nil
        weights: {integer} | nil
        markers: {string} | nil
        witnesses: {string} | nil
    end

    -- One slot of the executor's cluster: the identity occupying it, the
    -- live node (or the down state), the descriptor pieces a reopen
    -- needs, and the boot-gate marker schedule. `markers` holds the
    -- schedule of a node that is down, frozen at the moment it went
    -- down; a slot whose schedule a restart moved to another identity
    -- carries none.
    record Slot
        id: integer
        node: abi.Node | nil
        name: string
        state_path: string
        members: string
        markers: {string}
    end

    -- The full capture: the delivered datagram sequence and the rendered
    -- per-node post-state, in the transport's shape.
    record Capture
        deliveries: {Delivery}
        post: {Post}
    end

    record Executor
        slots: {Slot}
        queue: {Wire}
        clock: integer
        nodes: integer
        genesis: string
        op_seq: integer
        root: string
        tag: string
        provision_timeout: integer

        apply: function(Executor, any): string | nil
        drain: function(Executor): {Delivery}, string
        post_of: function(Executor, integer): Post
        post_records: function(Executor): {Post}
        free: function(Executor)
    end

    SETTLE_BOUND: integer
    DRAIN_BOUND: integer

    provision: function(integer, integer, string, string): Executor, string
end

local Executor = compliance_executor.Executor
local Slot = compliance_executor.Slot

-- ----------------------------------------------------------------------
-- Identities, hex, and wire
-- ----------------------------------------------------------------------

-- The identity half's parse failure: the pair is one-indexed in both
-- halves, so a zero half is no identity
-- (`docs/uvrr-io-obligations.md`, the boot-gate chapter §5).
local function half(text: string): integer, string
    if text:find("^%d+$") == nil or #text > 5 then
        return nil, "the identity half " .. text .. " is not a u16"
    end
    local value = tonumber(text) as integer
    if value > 65535 then
        return nil, "the identity half " .. text .. " is not a u16"
    end
    if value == 0 then
        return nil, "the identity half " .. text .. " is zero: no identity"
    end
    return value, nil
end

-- The packed identity an explicit `system:counter` pair names
-- (`NodeId::new`, `ext/uvrr-core/src/ids.rs:135-137`: MSB system
-- identifier, LSB crash counter).
local function identity(pair: string): integer, string
    local system_text, counter_text = pair:match("^([^:]+):([^:]+)$")
    if system_text == nil then
        return nil, "the identity " .. pair .. " is not the pair system:counter"
    end
    local system, failure = half(system_text)
    if system == nil then
        return nil, failure as string
    end
    local counter, refused = half(counter_text)
    if counter == nil then
        return nil, refused as string
    end
    return system * 65536 + counter, nil
end

-- The explicit pair a packed identity names.
local function pair_of(id: integer): string
    return tostring(math.floor(id / 65536)) .. ":" .. tostring(id % 65536)
end

-- The next life of the same system: the counter incremented. A counter
-- at the packing's last slot has no next life (`NodeId::next_life`,
-- `ext/uvrr-core/src/ids.rs:160-174`).
local function next_life(id: integer): integer, string
    if id % 65536 >= 65535 then
        return nil, "the identity space is spent"
    end
    return id + 1, nil
end

local function hex_encode(bytes: string): string
    local out: {string} = {}
    for index = 1, #bytes do
        out[index] = string.format("%02x", string.byte(bytes, index))
    end
    return table.concat(out)
end

local function hex_decode(text: string): string, string
    if #text % 2 ~= 0 then
        return nil, "odd hex length"
    end
    local out: {string} = {}
    for index = 1, #text, 2 do
        local byte = tonumber(string.sub(text, index, index + 1), 16)
        if byte == nil then
            return nil, "not hex"
        end
        out[(index + 1) // 2] = string.char(byte)
    end
    return table.concat(out), nil
end

-- ----------------------------------------------------------------------
-- The abstract operation, in the core's own encoding
-- ----------------------------------------------------------------------

-- The typed cluster operation a `SystemOperation` crosses the ABI as:
-- the core's own JSON (`configuration.rs:917-978`, no serde tag
-- attributes, so serde's default externally-tagged encoding). The names
-- are the core's, never the corpus's lowercase `kind`.
local CORE_NAMES: {string: string} = {
    increment = "Increment",
    decrement = "Decrement",
    leave = "Leave",
    join = "Join",
    nominate = "Nominate",
    batch = "Batch",
}

-- The `NodeId` an abstract operation's argument names, as the packed
-- number serde writes for the identity's newtype.
local function member_node(value: any, kind: string): integer, string
    local text = json.text_of(json.field(value, "node"))
    if text == nil then
        return nil, "the " .. kind .. " operation needs its node"
    end
    return identity(text)
end

local function system_operation(value: any): string, string
    local kind = json.text_of(json.field(value, "kind"))
    if kind == nil then
        return nil, "a typed cluster operation needs its kind"
    end
    if kind == "double" then
        return json.encode(json.text("Double")), nil
    elseif kind == "halve" then
        return json.encode(json.text("Halve")), nil
    elseif kind == "double" or kind == "halve" then
        return nil, "no such typed cluster operation: " .. kind
    elseif kind == "increment" or kind == "decrement" or kind == "leave" then
        local id, refused = member_node(value, kind)
        if id == nil then
            return nil, refused as string
        end
        return json.encode(
            json.object(
                {
                    {
                        key = CORE_NAMES[kind],
                        value = json.number(id),
                    },
                }
            )
        ), nil
    elseif kind == "join" then
        local id, refused = member_node(value, kind)
        if id == nil then
            return nil, refused as string
        end
        local position = json.number_of(json.field(value, "position"))
        if position == nil then
            return nil, "the join operation needs its position"
        end
        return json.encode(
            json.object(
                {
                    {
                        key = "Join",
                        value = json.object(
                            {
                                {key = "node", value = json.number(id)},
                                {key = "position", value = json.number(position)},
                            }
                        ),
                    },
                }
            )
        ), nil
    elseif kind == "nominate" then
        local from = json.number_of(json.field(value, "from"))
        local offset = json.number_of(json.field(value, "offset"))
        if from == nil or offset == nil then
            return nil, "the nominate operation needs its from and offset"
        end
        return json.encode(
            json.object(
                {
                    {
                        key = "Nominate",
                        value = json.object(
                            {
                                {key = "from", value = json.number(from)},
                                {key = "offset", value = json.number(offset)},
                            }
                        ),
                    },
                }
            )
        ), nil
    elseif kind == "batch" then
        local encoded: {any} = {}
        for _, op in ipairs(json.items_of(json.field(value, "ops"))) do
            local text, refused = system_operation(op)
            if text == nil then
                return nil, refused as string
            end
            local parsed, malformed = json.parse(text)
            if parsed == nil then
                return nil, malformed as string
            end
            table.insert(encoded, parsed)
        end
        return json.encode(
            json.object(
                {
                    {key = "Batch", value = json.array(encoded)},
                }
            )
        ), nil
    end
    return nil, "no such typed cluster operation: " .. kind
end

-- The core's own rendering of a typed cluster operation
-- (`ext/uvrr-core/src/conformance/corpus.rs:352-382`, `system_op_name`),
-- used for the journal's system entries.
local function system_op_name(value: any): string, string
    if json.is_text(value) then
        local tag = json.text_of(value)
        if tag == "Void" then
            return "void", nil
        elseif tag == "Double" then
            return "double", nil
        elseif tag == "Halve" then
            return "halve", nil
        end
        return nil, "no such typed cluster operation: " .. (tag as string)
    end
    local init = json.field(value, "Init")
    if init ~= nil then
        local order: {string} = {}
        for _, id in ipairs(json.items_of(json.field(init, "order"))) do
            local packed = json.number_of(id)
            if packed == nil then
                return nil, "an init journal entry carries no order"
            end
            table.insert(order, pair_of(packed as integer))
        end
        return "init order=" .. table.concat(order, ","), nil
    end
    for _, name in ipairs({"Increment", "Decrement", "Leave"}) do
        local wrapped = json.field(value, name)
        if wrapped ~= nil then
            local id = json.number_of(wrapped)
            if id == nil then
                return nil, "a " .. name .. " journal entry carries no node"
            end
            return string.lower(name) .. " " .. pair_of(id as integer), nil
        end
    end
    local join = json.field(value, "Join")
    if join ~= nil then
        local node = json.number_of(json.field(join, "node"))
        local position = json.number_of(json.field(join, "position"))
        if node == nil or position == nil then
            return nil, "a join journal entry is incomplete"
        end
        return "join " .. pair_of(node as integer) .. " at " .. tostring(position), nil
    end
    local nominate = json.field(value, "Nominate")
    if nominate ~= nil then
        local from = json.number_of(json.field(nominate, "from"))
        local offset = json.number_of(json.field(nominate, "offset"))
        if from == nil or offset == nil then
            return nil, "a nominate journal entry is incomplete"
        end
        return "nominate from=" .. tostring(from) .. " offset=" .. tostring(offset), nil
    end
    local batch = json.field(value, "Batch")
    if batch ~= nil then
        local pieces: {string} = {}
        for _, op in ipairs(json.items_of(batch)) do
            local text, refused = system_op_name(op)
            if text == nil then
                return nil, refused as string
            end
            table.insert(pieces, text)
        end
        return "batch [" .. table.concat(pieces, ", ") .. "]", nil
    end
    return nil, "no such typed cluster operation in the journal"
end

-- A journal entry's payload as corpus text: an operation's bytes ride
-- opaque, a system operation its own rendering
-- (`ext/uvrr-core/src/conformance/corpus.rs:407-412`, `render_payload`).
local function render_payload(entry: any): string, string
    local payload = json.field(entry, "payload")
    local operation = json.field(payload, "Operation")
    if operation ~= nil then
        local out: {string} = {}
        for _, byte in ipairs(json.items_of(json.field(operation, "payload"))) do
            local value = json.number_of(byte)
            if value == nil then
                return nil, "an operation payload byte is not a number"
            end
            table.insert(out, string.char(value as integer))
        end
        return table.concat(out), nil
    end
    local system = json.field(payload, "System")
    if system ~= nil then
        return system_op_name(system)
    end
    return nil, "a journal entry carries neither an operation nor a system payload"
end

-- ----------------------------------------------------------------------
-- The executor
-- ----------------------------------------------------------------------

local marker_lines: function(Slot): {string}

-- The store's own lines, in write order. A live node is read through
-- the ABI; a node that has gone down keeps the schedule frozen at the
-- moment it did.
function marker_lines(slot: Slot): {string}
    local out: {string} = {}
    if slot.node == nil then
        return out
    end
    for line in (slot.node as abi.Node):marker_log():gmatch("[^\n]+") do
        table.insert(out, line)
    end
    return out
end

-- The boot-gate marker schedule a known identity's post record carries:
-- the machine rounds of the slot's most recent gate, as
-- `<Marker>@<system>:<counter>` with the `drain` between the halt's
-- rounds. A line the store's reader does not understand is dropped, and
-- a slot whose schedule a restart moved away carries none.
local function render_markers(lines: {string}): {string}
    local out: {string} = {}
    for _, line in ipairs(lines) do
        if line == "drain" then
            table.insert(out, "drain")
        else
            local name, packed_text = line:match("^commit:([^@]+)@(.+)$")
            if name ~= nil and packed_text:find("^%d+$") ~= nil then
                table.insert(
                    out, name .. "@" .. pair_of(tonumber(packed_text) as integer)
                )
            end
        end
    end
    return out
end

-- A live node's schedule is read through the ABI; a node that has gone
-- down keeps the schedule frozen at the moment it did.
local function marker_states(slot: Slot): {string}
    if slot.node ~= nil then
        return render_markers(marker_lines(slot))
    end
    return render_markers(slot.markers)
end

-- Sets the executor's logical clock on the node about to be driven.
local function arm(slot: Slot, clock: integer)
    if slot.node ~= nil then
        (slot.node as abi.Node):set_clock(clock)
    end
end

-- Moves the node's queued outputs into the wire. Any output beyond the
-- send kind is an adapter bug under the compliance rules.
local function drain_outputs(executor: Executor, index: integer)
    local slot = executor.slots[index]
    if slot.node == nil then
        return
    end
    for _, output in ipairs((slot.node as abi.Node):outputs()) do
        if output.kind ~= abi.OUTPUT_SEND then
            error(
                "compliance_executor: the compliance executor queues only "
                    .. "unicast datagrams, got output kind "
                    .. tostring(output.kind),
                0
            )
        end
        table.insert(
            executor.queue, {from = slot.id, to = output.to, bytes = output.bytes}
        )
    end
end

-- The marker-store file a case gives one identity. The case's own tag
-- is part of the name, so two cases never share a marker's file and a
-- case needs no directory of its own.
local function state_path_of(root: string, tag: string, id: integer): string
    return root .. "/" .. tag .. "-" .. tostring(id) .. ".state"
end

local function slot_of(executor: Executor, id: integer): integer | nil
    for index, slot in ipairs(executor.slots) do
        if slot.id == id then
            return index
        end
    end
    return nil
end

-- Whether the network is quiet and every live node is `Normal`.
local function quiet_and_normal(executor: Executor): boolean
    if #executor.queue > 0 then
        return false
    end
    for _, slot in ipairs(executor.slots) do
        if slot.node ~= nil then
            local word = (slot.node as abi.Node):status()
            if word ~= STATUS_NORMAL then
                return false
            end
        end
    end
    return true
end

-- One timer event to every live node, in slot order, the clock advanced
-- once for the sweep.
local function tick_all(executor: Executor)
    executor.clock = executor.clock + 1
    for _, slot in ipairs(executor.slots) do
        if slot.node ~= nil then
            arm(slot, executor.clock)
            (slot.node as abi.Node):idle()
        end
    end
    for index = 1, #executor.slots do
        drain_outputs(executor, index)
    end
end

-- Delivers every queued datagram once, in queue order. A delivery to a
-- down node is popped and skipped: it stays named in the drain's list,
-- undeliverable.
local function deliver_all(
    executor: Executor, record: {compliance_executor.Delivery} | nil
): string | nil
    local steps = 0
    while #executor.queue > 0 do
        steps = steps + 1
        if steps > DRAIN_BOUND then
            return "deliver_all did not quiet inside the bound"
        end
        local wire = table.remove(executor.queue, 1)
        if record ~= nil then
            table.insert(
                record,
                {
                    from = pair_of(wire.from),
                    to = pair_of(wire.to),
                    wire = hex_encode(wire.bytes),
                }
            )
        end
        local found = slot_of(executor, wire.to)
        if found ~= nil then
            local index = found as integer
            local slot = executor.slots[index]
            if slot.node ~= nil then
                arm(slot, executor.clock)
                (slot.node as abi.Node):receive(wire.from, wire.bytes)
                drain_outputs(executor, index)
            end
        end
    end
    return nil
end

-- The post-input drain: delivers until quiet, recording the exact
-- delivery sequence. No timer event fires in the drain.
function Executor:drain(): {compliance_executor.Delivery}, string
    local out: {compliance_executor.Delivery} = {}
    for _ = 1, DRAIN_BOUND do
        if #self.queue == 0 then
            return out, nil
        end
        local failure = deliver_all(self, out)
        if failure ~= nil then
            return {}, failure
        end
    end
    return {}, "the drain did not quiet inside the bound"
end

local function live_slot(
    executor: Executor, id: integer, node: string, what: string
): integer, string
    local found = slot_of(executor, id)
    if found == nil then
        return nil, what .. ": no slot for " .. node
    end
    local index = found as integer
    if executor.slots[index].node == nil then
        return nil, what .. ": " .. node .. " is down"
    end
    return index, nil
end

-- Boots one node into a new slot. The descriptor pieces a reopen needs
-- are the caller's: a provision's genesis members, a restart's frozen
-- set, a joiner's genesis plus its own joiner row.
local function boot_slot(
    members: string, name: string, state_path: string, timeout: integer, id: integer
): Slot, string
    -- A boot refusal is a named outcome, not a raised error: the
    -- transport answers a case with a verdict, so the reason the cluster
    -- would not come up has to reach the client as text. This also covers
    -- the binding itself failing — a production library carries none of
    -- these symbols, and loading one says so here rather than crashing
    -- the connection.
    local opened, node, failure = pcall(abi.open, members, name, state_path, timeout)
    if not opened then
        return nil,
            "the compliance boot raised: " .. tostring(node) .. " [" .. name .. "]"
    end
    if node == nil then
        return nil, (failure or "the compliance boot refused") .. " [" .. name .. "]"
    end
    return {
        id = id,
        node = node,
        name = name,
        state_path = state_path,
        members = members,
        markers = {},
    }, nil
end

-- Applies one operation.
function Executor:apply(op: any): string | nil
    local name = json.text_of(json.field(op, "op"))
    if name == nil then
        return "an operation needs its op tag"
    end
    local function text(key: string): string
        local value = json.text_of(json.field(op, key))
        if value == nil then
            return nil
        end
        return value
    end
    -- The packed identity an operation's named node is, with the label
    -- the reference's refusal names.
    local function address(key: string, what: string): integer, string
        local label = text(key)
        if label == nil then
            return nil, what .. ": the operation needs its " .. key
        end
        local id, refused = identity(label)
        if id == nil then
            return nil, refused as string
        end
        return id, nil
    end
    if name == "provision" then
        return "provision is the setup's first operation, built by the executor"
    elseif name == "settle" then
        for _ = 1, SETTLE_BOUND do
            if quiet_and_normal(self) then
                return nil
            end
            tick_all(self)
            local broken = deliver_all(self, nil)
            if broken ~= nil then
                return broken
            end
        end
        return "settle did not converge inside the bound"
    elseif name == "propose" then
        local id, refused = address("node", "propose")
        if id == nil then
            return refused as string
        end
        local payload = text("payload")
        if payload == nil then
            return "propose: the operation needs its payload"
        end
        local index, no_slot = live_slot(
            self, id, json.text_of(json.field(op, "node")), "propose"
        )
        if index == nil then
            return no_slot as string
        end
        local slot = self.slots[index]
        arm(slot, self.clock)
        (slot.node as abi.Node):propose_opaque(OP_MSB, self.op_seq, payload)
        self.op_seq = self.op_seq + 1
        drain_outputs(self, index)
        return nil
    elseif name == "reconfigure" then
        local id, refused = address("node", "reconfigure")
        if id == nil then
            return refused as string
        end
        local index, no_slot = live_slot(
            self, id, json.text_of(json.field(op, "node")), "reconfigure"
        )
        if index == nil then
            return no_slot as string
        end
        local encoded, no_kind = system_operation(json.field(op, "system"))
        if encoded == nil then
            return "reconfigure: " .. (no_kind as string)
        end
        local slot = self.slots[index]
        arm(slot, self.clock)
        (slot.node as abi.Node):reconfigure_opaque(encoded)
        drain_outputs(self, index)
        return nil
    elseif name == "tick" then
        local id, refused = address("node", "tick")
        if id == nil then
            return refused as string
        end
        local index, no_slot = live_slot(
            self, id, json.text_of(json.field(op, "node")), "tick"
        )
        if index == nil then
            return no_slot as string
        end
        self.clock = self.clock + 1
        local slot = self.slots[index]
        arm(slot, self.clock)
        (slot.node as abi.Node):idle()
        drain_outputs(self, index)
        return nil
    elseif name == "tick_all" then
        tick_all(self)
        return nil
    elseif name == "deliver_all" then
        return deliver_all(self, nil)
    elseif name == "crash" then
        local id, refused = address("node", "crash")
        if id == nil then
            return refused as string
        end
        local found = slot_of(self, id)
        if found == nil then
            return "crash: no slot for " .. json.text_of(json.field(op, "node"))
        end
        local slot = self.slots[found as integer]
        if slot.node == nil then
            return "crash: "
                .. json.text_of(json.field(op, "node"))
                .. " is already down"
        end
        -- The schedule is read while the node still answers: a dropped
        -- node's store handle stays readable in the reference, and the
        -- frozen lines are what stands in for it here.
        slot.markers = marker_lines(slot)
        (slot.node as abi.Node):free()
        slot.node = nil
        return nil
    elseif name == "restart" then
        local id, refused = address("node", "restart")
        if id == nil then
            return refused as string
        end
        local kind = text("kind")
        if kind == nil then
            return "restart: the operation needs its kind"
        end
        local found = slot_of(self, id)
        if found == nil then
            return "restart: no slot for " .. json.text_of(json.field(op, "node"))
        end
        local index = found as integer
        if self.slots[index].node ~= nil then
            return "restart: "
                .. json.text_of(json.field(op, "node"))
                .. " is up; halt or crash it first"
        end
        local previous = self.slots[index]
        if kind == "clean" then
            local slot, refusal = boot_slot(
                previous.members,
                previous.name,
                previous.state_path,
                self.provision_timeout,
                id
            )
            if slot == nil then
                return "the clean restart boots: " .. (refusal as string)
            end
            local own = (slot.node as abi.Node):own_id()
            if own ~= id then
                (slot.node as abi.Node):free()
                return "the clean restart continued a different identity: "
                    .. pair_of(own)
                    .. " != "
                    .. pair_of(id)
            end
            slot.markers = marker_lines(slot)
            self.slots[index] = slot
            drain_outputs(self, index)
            return nil
        elseif kind == "crashed" then
            local bumped, spent = next_life(id)
            if bumped == nil then
                return spent as string
            end
            local slot, refusal = boot_slot(
                previous.members,
                previous.name,
                previous.state_path,
                self.provision_timeout,
                bumped as integer
            )
            if slot == nil then
                return "the crashed restart boots: " .. (refusal as string)
            end
            local life = bumped as integer
            local own = (slot.node as abi.Node):own_id()
            if own ~= life then
                (slot.node as abi.Node):free()
                return "the crashed restart bumped to a different identity: "
                    .. pair_of(own)
                    .. " != "
                    .. pair_of(life)
            end
            slot.markers = marker_lines(slot)
            table.insert(self.slots, slot)
            -- The bump's boot owns the new schedule; the superseded
            -- identity's record is empty.
            previous.markers = {}
            drain_outputs(self, #self.slots)
            return nil
        end
        return "no such restart kind: " .. kind
    elseif name == "halt" then
        local id, refused = address("node", "halt")
        if id == nil then
            return refused as string
        end
        local index, no_slot = live_slot(
            self, id, json.text_of(json.field(op, "node")), "halt"
        )
        if index == nil then
            return no_slot as string
        end
        local slot = self.slots[index]
        arm(slot, self.clock)
        if not (slot.node as abi.Node):stop() then
            return "halt: " .. json.text_of(json.field(op, "node")) .. " refused"
        end
        -- The halt's own `drain` marker lands before the schedule is
        -- frozen, so a halted node's post record carries it.
        slot.markers = marker_lines(slot)
        (slot.node as abi.Node):free()
        slot.node = nil
        return nil
    elseif name == "boot" then
        local id, refused = address("node", "boot")
        if id == nil then
            return refused as string
        end
        if slot_of(self, id) ~= nil then
            return "boot: "
                .. json.text_of(json.field(op, "node"))
                .. " already has a slot"
        end
        local label = "n" .. tostring(math.floor(id / 65536))
        local members = self.genesis .. "\0" .. tostring(id) .. ":" .. label .. ":j"
        local slot, refusal = boot_slot(
            members,
            label,
            state_path_of(self.root, self.tag, id),
            self.provision_timeout,
            id
        )
        if slot == nil then
            return "the fresh joiner boots: " .. (refusal as string)
        end
        slot.markers = marker_lines(slot)
        table.insert(self.slots, slot)
        drain_outputs(self, #self.slots)
        return nil
    elseif name == "announce" then
        local id, refused = address("node", "announce")
        if id == nil then
            return refused as string
        end
        local old = text("old")
        if old == nil then
            return "announce: the operation needs its old"
        end
        if identity(old) == nil then
            return "announce: " .. old .. " is not the pair system:counter"
        end
        local index, no_slot = live_slot(
            self, id, json.text_of(json.field(op, "node")), "announce"
        )
        if index == nil then
            return no_slot as string
        end
        local slot = self.slots[index]
        arm(slot, self.clock)
        (slot.node as abi.Node):recover()
        drain_outputs(self, index)
        return nil
    elseif name == "deliver" then
        local to_id, refused = address("to", "deliver")
        if to_id == nil then
            return refused as string
        end
        local from_id, broken = address("from", "deliver")
        if from_id == nil then
            return broken as string
        end
        local wire = text("wire")
        if wire == nil then
            return "deliver: the operation needs its wire"
        end
        local bytes, not_hex = hex_decode(wire)
        if bytes == nil then
            return "deliver: " .. (not_hex as string)
        end
        local found = slot_of(self, to_id)
        if found == nil then
            return "deliver: no slot for " .. json.text_of(json.field(op, "to"))
        end
        local index = found as integer
        local slot = self.slots[index]
        if slot.node == nil then
            return "the receiver is down"
        end
        arm(slot, self.clock)
        (slot.node as abi.Node):receive(from_id, bytes)
        drain_outputs(self, index)
        return nil
    elseif name == "gossip" then
        local wire = text("wire")
        if wire == nil then
            return "gossip: the operation needs its wire"
        end
        local bytes, not_hex = hex_decode(wire)
        if bytes == nil then
            return "gossip: " .. (not_hex as string)
        end
        local sender, refused = identity(tostring(self.nodes + 1) .. ":1")
        if sender == nil then
            return refused as string
        end
        for _, slot in ipairs(self.slots) do
            if slot.node ~= nil then
                arm(slot, self.clock)
                (slot.node as abi.Node):receive(sender, bytes)
            end
        end
        for index = 1, #self.slots do
            drain_outputs(self, index)
        end
        return nil
    end
    return "no such host operation: " .. name
end

-- One node's full post record, every field captured.
function Executor:post_of(id: integer): compliance_executor.Post
    local found = slot_of(self, id)
    if found == nil then
        return {node = ""}
    end
    local slot = self.slots[found as integer]
    if slot.node == nil then
        return {
            node = pair_of(id),
            markers = marker_states(slot),
        }
    end
    local node = slot.node as abi.Node
    local word, era, view = node:status()
    local accepted, committed, applied = node:frontiers()
    local ids, weights = node:membership()
    local members: {string} = {}
    for _, member in ipairs(ids) do
        table.insert(members, pair_of(member))
    end
    local entries, malformed = json.parse(node:journal_entries())
    if entries == nil then
        error(
            "compliance_executor: the journal is not JSON: " .. (malformed as string), 0
        )
    end
    local journal: {string} = {}
    for _, entry in ipairs(json.items_of(entries)) do
        local payload, refusal = render_payload(entry)
        if payload == nil then
            error("compliance_executor: " .. (refusal as string), 0)
        end
        table.insert(journal, payload)
    end
    local witnesses: {string} = {}
    for _, witness in ipairs(node:witnesses()) do
        table.insert(witnesses, pair_of(witness))
    end
    return {
        node = pair_of(id),
        status = STATUS_NAMES[word],
        era = era,
        view = view,
        accepted = accepted,
        committed = committed,
        applied = applied,
        journal = journal,
        members = members,
        weights = weights,
        markers = marker_states(slot),
        witnesses = witnesses,
    }
end

function Executor:post_records(): {compliance_executor.Post}
    local out: {compliance_executor.Post} = {}
    for _, slot in ipairs(self.slots) do
        table.insert(out, self:post_of(slot.id))
    end
    return out
end

function Executor:free()
    for _, slot in ipairs(self.slots) do
        if slot.node ~= nil then
            (slot.node as abi.Node):free()
            slot.node = nil
        end
    end
end

-- The abstract host interface over one provisioned cluster of adapter
-- nodes: one `open_compliance` per roster member over a fresh marker
-- store, the packed-vector identities, the case's primary-timeout knob,
-- and the unbounded suffix budget the compliance rules arm.
function compliance_executor.provision(
    nodes: integer, timeout: integer, root: string, tag: string
): Executor, string
    local entries: {string} = {}
    for index = 0, nodes - 1 do
        local id, failure = identity(tostring(index + 1) .. ":1")
        if id == nil then
            return nil, failure as string
        end
        table.insert(entries, tostring(id) .. ":n" .. tostring(index + 1))
    end
    local genesis = table.concat(entries, "\0")
    local slots: {Slot} = {}
    for index = 0, nodes - 1 do
        local id, failure = identity(tostring(index + 1) .. ":1")
        if id == nil then
            return nil, failure as string
        end
        local name = "n" .. tostring(index + 1)
        local slot, refusal = boot_slot(
            genesis, name, state_path_of(root, tag, id), timeout, id
        )
        if slot == nil then
            return nil,
                "node "
                    .. pair_of(id)
                    .. ": the provision boots: "
                    .. (refusal as string)
        end
        slot.markers = marker_lines(slot)
        table.insert(slots, slot)
    end
    return {
        slots = slots,
        queue = {},
        clock = 0,
        nodes = nodes,
        genesis = genesis,
        op_seq = 1,
        root = root,
        tag = tag,
        provision_timeout = timeout,
        apply = Executor.apply,
        drain = Executor.drain,
        post_of = Executor.post_of,
        post_records = Executor.post_records,
        free = Executor.free,
    }, nil
end

compliance_executor.SETTLE_BOUND = SETTLE_BOUND
compliance_executor.DRAIN_BOUND = DRAIN_BOUND

return compliance_executor
