-- The compliance corpus's transport host: the case endpoint and the
-- session endpoints of `docs/uvrr-host-compliance.md` §8, and the
-- verdict.
--
-- §8.3 is law here. A verdict is `pass` only when the host's OWN capture
-- matches the `expect` the request carried: `run_case` replays the case
-- against real adapter nodes through the Compliance ABI, captures what
-- they did, and `assert_expectation` compares that capture against the
-- named expectation field by field. There is no shortcut from a case id
-- to a verdict, no special-cased case, and no regenerated fixture: the
-- request's `expect` is read as data and compared against the capture,
-- and the comparison is the only thing that can produce `pass`.
--
-- A host that serves these endpoints conforms independently of the
-- client's implementation language, so this module is pure: the
-- routing is a function of (method, path, body) and needs no socket.
-- `src/compliance_server.tl` carries it on a socket.

local executor = require("compliance_executor")
local json = require("compliance_json")

local Capture = executor.Capture
local Delivery = executor.Delivery
local Executor = executor.Executor
local Post = executor.Post

local record compliance_host
    Host: Host
    render_capture: function(Capture): any
    assert_expectation: function(any, Capture): string | nil
    run_case: function(any, string, string): Capture, string
    host: function(string, string): Host
end

-- One named field's mismatch, or `nil` when the field passes or is
-- unnamed. An absent field on either side is unconstrained; a present
-- field on both sides is compared exactly.
local function field(captured: any, named: any, name: string): string | nil
    if captured == nil or named == nil then
        return nil
    end
    local mine = json.encode(captured)
    local theirs = json.encode(named)
    if mine ~= theirs then
        return name .. ": expected " .. theirs .. ", captured " .. mine
    end
    return nil
end

-- One delivery as the transport renders it.
local function render_delivery(delivery: Delivery): any
    return json.object(
        {
            {key = "from", value = json.text(delivery.from)},
            {key = "to", value = json.text(delivery.to)},
            {key = "wire", value = json.text(delivery.wire)},
        }
    )
end

local function strings(items: {string}): any
    local out: {any} = {}
    for _, item in ipairs(items) do
        table.insert(out, json.text(item))
    end
    return json.array(out)
end

local function numbers(items: {integer}): any
    local out: {any} = {}
    for _, item in ipairs(items) do
        table.insert(out, json.number(item))
    end
    return json.array(out)
end

-- One node's post record as the transport renders it. A field the
-- capture could not observe is absent rather than null: the capture of a
-- node that is down carries its identity and its markers and nothing
-- else, and an absent field is unconstrained on both sides.
local function render_post(post: Post): any
    local entries: {json.Entry} = {{key = "node", value = json.text(post.node)}}
    local function named(key: string, value: any)
        if value ~= nil then
            local entry: json.Entry = {key = key, value = value}
            table.insert(entries, entry)
        end
    end
    named("status", post.status ~= nil and json.text(post.status) or nil)
    named("era", post.era ~= nil and json.number(post.era) or nil)
    named("view", post.view ~= nil and json.number(post.view) or nil)
    named("accepted", post.accepted ~= nil and json.number(post.accepted) or nil)
    named("committed", post.committed ~= nil and json.number(post.committed) or nil)
    named("applied", post.applied ~= nil and json.number(post.applied) or nil)
    named("journal", post.journal ~= nil and strings(post.journal) or nil)
    named("members", post.members ~= nil and strings(post.members) or nil)
    named("weights", post.weights ~= nil and numbers(post.weights) or nil)
    named("markers", post.markers ~= nil and strings(post.markers) or nil)
    named("witnesses", post.witnesses ~= nil and strings(post.witnesses) or nil)
    return json.object(entries)
end

-- The full capture in the `expect` shape: the delivered datagram
-- sequence and the rendered per-node post-state.
local function render_capture(capture: Capture): any
    local deliveries: {any} = {}
    for _, delivery in ipairs(capture.deliveries) do
        table.insert(deliveries, render_delivery(delivery))
    end
    local post: {any} = {}
    for _, record in ipairs(capture.post) do
        table.insert(post, render_post(record))
    end
    return json.object(
        {
            {key = "deliveries", value = json.array(deliveries)},
            {key = "post", value = json.array(post)},
        }
    )
end

-- Asserts a captured expectation against a case's named expectation: the
-- delivery sequence exactly, then each named post field exactly, in the
-- order the reference compares them.
local function assert_expectation(case: any, capture: Capture): string | nil
    local rendered = render_capture(capture)
    local delivered = json.field(rendered, "deliveries")
    local named = json.field(json.field(case, "expect"), "deliveries")
    if json.encode(delivered) ~= json.encode(named) then
        return "deliveries: expected "
            .. json.encode(named)
            .. ", captured "
            .. json.encode(delivered)
    end
    for _, post in ipairs(
        json.items_of(json.field(json.field(case, "expect"), "post"))
    ) do
        local label = json.text_of(json.field(post, "node"))
        local found: Post | nil = nil
        for _, record in ipairs(capture.post) do
            if record.node == label then
                found = record
                break
            end
        end
        if found == nil then
            return "no post record for node " .. (label or "")
        end
        local mine = render_post(found as Post)
        local mismatch = field(
            json.field(mine, "status"), json.field(post, "status"), "status"
        ) or field(json.field(mine, "era"), json.field(post, "era"), "era")
            or field(json.field(mine, "view"), json.field(post, "view"), "view")
            or field(
                json.field(mine, "accepted"), json.field(post, "accepted"), "accepted"
            ) or field(
            json.field(mine, "committed"), json.field(post, "committed"), "committed"
        ) or field(json.field(mine, "applied"), json.field(post, "applied"), "applied")
            or field(
                json.field(mine, "journal"), json.field(post, "journal"), "journal"
            ) or field(
            json.field(mine, "members"), json.field(post, "members"), "members"
        ) or field(json.field(mine, "weights"), json.field(post, "weights"), "weights")
            or field(
                json.field(mine, "markers"), json.field(post, "markers"), "markers"
            ) or field(
            json.field(mine, "witnesses"), json.field(post, "witnesses"), "witnesses"
        )
        if mismatch ~= nil then
            return mismatch
        end
    end
    return nil
end

-- Runs a case's setup and input against real nodes, then drains and
-- captures the full expectation. The executor is the only thing that
-- touches protocol state, and it does so through the Compliance ABI.
local function run_case(case: any, root: string, tag: string): Capture, string
    local setup = json.items_of(json.field(case, "setup"))
    local first = setup[1]
    if json.text_of(json.field(first, "op")) ~= "provision" then
        return nil, "the setup must open with provision"
    end
    local nodes = json.number_of(json.field(first, "nodes"))
    local timeout = json.number_of(json.field(first, "timeout"))
    if nodes == nil or timeout == nil then
        return nil, "the provision operation needs its nodes and timeout"
    end
    local cluster, refusal = executor.provision(
        nodes as integer, timeout as integer, root, tag
    )
    if cluster == nil then
        return nil, refusal as string
    end
    for index = 2, #setup do
        local failure = (cluster as Executor):apply(setup[index])
        if failure ~= nil then
            (cluster as Executor):free()
            return nil, "the setup's operation refused: " .. failure
        end
    end
    local input = json.field(case, "input")
    local failure = (cluster as Executor):apply(input)
    if failure ~= nil then
        (cluster as Executor):free()
        return nil, "the input refused: " .. failure
    end
    local deliveries, broken = (cluster as Executor):drain()
    if broken ~= nil then
        (cluster as Executor):free()
        return nil, broken as string
    end
    local capture: Capture = {
        deliveries = deliveries,
        post = (cluster as Executor):post_records(),
    }
    (cluster as Executor):free()
    return capture, nil
end

-- ----------------------------------------------------------------------
-- The sessions and the routing
-- ----------------------------------------------------------------------

-- One open session: the cluster the provision built, and the id the
-- client drives it by. A session holds its own executor and its own
-- marker-store names, so one session never observes another's state
-- (§8.3).
local record Session
    cluster: Executor
end

local record Host
    root: string
    version: string
    sessions: {string: Session}
    opened: integer
    cases: integer
    verdicts: {string}
    take_verdicts: function(Host): {string}

    open_session: function(Host, string): string, string
    apply_operation: function(Host, string, string): any, string
    capture: function(Host, string): any, string
    run: function(Host, string): integer, string
    route: function(Host, string, string, string): integer, string
    close: function(Host)
end

local function answer(status: integer, body: any): integer, string
    return status, json.encode(body)
end

local function error_answer(status: integer, reason: string): integer, string
    return answer(status, json.object({{key = "error", value = json.text(reason)}}))
end

-- The case endpoint (§8.1). The verdict comes from comparing this host's
-- own capture against the `expect` the request carries, and the `expect`
-- in the answer is that same capture — never the request's copy.
function Host:run(body: string): integer, string
    local case, malformed = json.parse(body)
    if case == nil then
        return error_answer(400, malformed as string)
    end
    local id = json.text_of(json.field(case, "id"))
    local family = json.text_of(json.field(case, "family"))
    if id == nil or family == nil then
        return error_answer(400, "a case needs its id and family")
    end
    self.cases = self.cases + 1
    local capture, failure = run_case(
        case, self.root, self.version .. "-case-" .. tostring(self.cases)
    )
    local entries: {json.Entry} = {
        {key = "id", value = json.text(id)},
        {key = "family", value = json.text(family)},
    }
    if capture == nil then
        -- The case could not be replayed at all: a named verdict of
        -- `fail` with the refusal as the mismatch, so a client sees the
        -- host's own reason rather than a rewritten fixture.
        table.insert(
            self.verdicts, family .. " " .. id .. " fail: " .. (failure as string)
        )
        table.insert(entries, {key = "verdict", value = json.text("fail")})
        table.insert(entries, {key = "mismatch", value = json.text(failure as string)})
        table.insert(entries, {key = "expect", value = json.nothing()})
        return answer(200, json.object(entries))
    end
    local mismatch = assert_expectation(case, capture)
    table.insert(
        self.verdicts,
        (
            mismatch == nil and family .. " " .. id .. " pass"
            or (family .. " " .. id .. " fail: " .. mismatch)
        )
    )
    table.insert(
        entries,
        {key = "verdict", value = json.text(mismatch == nil and "pass" or "fail")}
    )
    if mismatch ~= nil then
        table.insert(entries, {key = "mismatch", value = json.text(mismatch)})
    end
    table.insert(entries, {key = "expect", value = render_capture(capture)})
    return answer(200, json.object(entries))
end

-- Opens a session: a provision is what begins a cluster, so the session
-- opens with the provision operation's own arguments.
function Host:open_session(body: string): string, string
    local request, malformed = json.parse(body)
    if request == nil then
        return nil, malformed as string
    end
    local nodes = json.number_of(json.field(request, "nodes"))
    local timeout = json.number_of(json.field(request, "timeout"))
    if nodes == nil or timeout == nil then
        return nil, "a session opens with the provision operation's nodes and timeout"
    end
    self.opened = self.opened + 1
    local id = "session-" .. tostring(self.opened)
    local cluster, refusal = executor.provision(
        nodes as integer, timeout as integer, self.root, self.version .. "-" .. id
    )
    if cluster == nil then
        return nil, refusal as string
    end
    self.sessions[id] = {cluster = cluster as Executor}
    return id, nil
end

function Host:apply_operation(id: string, body: string): any, string
    local session = self.sessions[id]
    if session == nil then
        return nil, "no such session"
    end
    local op, malformed = json.parse(body)
    if op == nil then
        return nil, malformed as string
    end
    local failure = session.cluster:apply(op)
    if failure ~= nil then
        return json.object(
            {
                {key = "ok", value = json.flag(false)},
                {key = "error", value = json.text(failure)},
            }
        ), nil
    end
    return json.object({{key = "ok", value = json.flag(true)}}), nil
end

-- The settling window of §8.2 made explicit: drain to quiet, bounded,
-- no wall-clock wait, and return the complete delivered set.
function Host:capture(id: string): any, string
    local session = self.sessions[id]
    if session == nil then
        return nil, "no such session"
    end
    local deliveries, failure = session.cluster:drain()
    if failure ~= nil then
        return nil, failure
    end
    local capture: Capture = {
        deliveries = deliveries,
        post = session.cluster:post_records(),
    }
    return json.object({{key = "expect", value = render_capture(capture)}}), nil
end

-- The health probe: the host's identity. `host` and `crate` name the
-- harness role and the corpus this transport serves, which is what the
-- committed suite asserts; `version` is this build's own stamp, and the
-- build's own name and feature shape ride beside it so the answer never
-- leaves a client guessing which implementation answered.
function Host:health(): any
    return json.object(
        {
            {key = "host", value = json.text("uvrr-conformance")},
            {key = "crate", value = json.text("uvrr-core")},
            {key = "version", value = json.text(self.version)},
            {key = "implementation", value = json.text("lua-lunet/lunet-locks")},
            {
                key = "feature_shape",
                value = json.text("production+compatibility_suite"),
            },
        }
    )
end

-- One request, one answer. The routing is a pure function of the method,
-- the path and the body: no socket state, no clock, nothing the client's
-- timing could reach.
function Host:route(method: string, path: string, body: string): integer, string
    if method == "GET" and path == "/health" then
        return answer(200, self:health())
    elseif method == "POST" and path == "/session" then
        local id, failure = self:open_session(body)
        if id == nil then
            return error_answer(400, failure as string)
        end
        return answer(200, json.object({{key = "session", value = json.text(id)}}))
    elseif method == "POST" and path == "/case" then
        return self:run(body)
    elseif method == "GET" and path == "/case" then
        return error_answer(405, "the case endpoint takes POST")
    elseif method == "GET" and path == "/" then
        return error_answer(405, "this host serves the compliance corpus endpoints")
    elseif path:match("^/session/([^/]+)/op$") ~= nil then
        local id = path:match("^/session/([^/]+)/op$") as string
        if method ~= "POST" then
            return error_answer(405, "an operation is applied with POST")
        end
        local payload, failure = self:apply_operation(id, body)
        if payload == nil then
            local status = 404
            if failure ~= "no such session" then
                status = 400
            end
            return error_answer(status, failure as string)
        end
        return answer(200, payload)
    elseif path:match("^/session/([^/]+)/capture$") ~= nil then
        local id = path:match("^/session/([^/]+)/capture$") as string
        if method ~= "GET" then
            return error_answer(405, "a capture is read with GET")
        end
        local payload, failure = self:capture(id)
        if payload == nil then
            return error_answer(404, failure as string)
        end
        return answer(200, payload)
    end
    return error_answer(404, "no such route: " .. method .. " " .. path)
end

-- The verdict lines since the last call, one per case, in arrival
-- order. The transport answers the client; this is the same verdict
-- written out for whoever reads the gate's log.
function Host:take_verdicts(): {string}
    local out = self.verdicts
    self.verdicts = {}
    return out
end

function Host:close()
    for _, session in pairs(self.sessions) do
        session.cluster:free()
    end
    self.sessions = {}
end

function compliance_host.host(root: string, version: string): Host
    return {
        root = root,
        version = version,
        sessions = {},
        opened = 0,
        cases = 0,
        verdicts = {},
        take_verdicts = Host.take_verdicts,
        open_session = Host.open_session,
        apply_operation = Host.apply_operation,
        capture = Host.capture,
        health = Host.health,
        run = Host.run,
        route = Host.route,
        close = Host.close,
    }
end

local M: compliance_host = {
    render_capture = render_capture,
    assert_expectation = assert_expectation,
    run_case = run_case,
    host = compliance_host.host,
}

return M
