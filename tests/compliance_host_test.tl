local tested = require("tested")

-- `tested.assert` refuses a nil on either side, so an absence is
-- asserted as its own rendering.
local function absent(value: any): string
    return tostring(value)
end

-- The verdict and the routing, on their own: no socket, no cluster, no
-- case replay. What is pinned here is §8.3 — a verdict is derived from
-- the host's OWN capture and from nothing else — and the status codes
-- the transport contract names.

local function a_case(expect: string): string
    return '{"id":"the-case","family":"agreement","setup":[{"op":'
        .. '"provision","nodes":3,"timeout":3}],"input":{"op":"settle"},'
        .. '"expect":'
        .. expect
        .. "}"
end

local function member(payload: any, key: string): string
    return require("compliance_json").text_of(
        require("compliance_json").field(payload, key)
    ) as string
end

tested.test(
    "a capture that meets the expectation passes, and says nothing more",
    function()
        local host = require("compliance_host")
        local capture = {
            deliveries = {},
            post = {
                {
                    node = "1:1",
                    status = "Normal",
                    era = 1,
                    view = 0,
                    accepted = 0,
                    committed = 0,
                    applied = 0,
                    journal = {},
                    members = {"1:1", "2:1", "3:1"},
                    weights = {1, 1, 1},
                    markers = {},
                    witnesses = {},
                },
            },
        }
        local case = require("compliance_json").parse(
            a_case(
                '{"deliveries":[],"post":[{"node":"1:1","status":"Normal",'
                    .. '"era":1,"view":0,"accepted":0,"committed":0,"applied":0,'
                    .. '"journal":[],"members":["1:1","2:1","3:1"],"weights":[1,1,1],'
                    .. '"markers":[],"witnesses":[]}]}'
            )
        )
        tested.assert(
            {
                given = "a capture identical to the expectation it is compared against",
                should = "name no field that differs",
                expected = "nil",
                actual = absent(host.assert_expectation(case, capture)),
            }
        )
    end
)

tested.test(
    "the first differing named field is the mismatch, in the transport's form",
    function()
        local host = require("compliance_host")
        local capture = {
            deliveries = {},
            post = {
                {
                    node = "1:1",
                    status = "Normal",
                    era = 1,
                    view = 0,
                    accepted = 0,
                    committed = 3,
                    applied = 0,
                    journal = {},
                    members = {"1:1"},
                    weights = {1},
                    markers = {},
                    witnesses = {},
                },
            },
        }
        local case = require("compliance_json").parse(
            a_case(
                '{"deliveries":[],"post":[{"node":"1:1","status":"ViewChange",'
                    .. '"committed":7}]}'
            )
        )
        tested.assert(
            {
                given = "a capture whose status and committed frontier both differ",
                should = "name the status first, and no other field",
                expected = 'status: expected "ViewChange", captured "Normal"',
                actual = host.assert_expectation(case, capture),
            }
        )
    end
)

tested.test(
    "a delivery the host made that the case names none of is a mismatch",
    function()
        local host = require("compliance_host")
        local capture = {
            deliveries = {
                {from = "1:1", to = "2:1", wire = "00ff"},
            },
            post = {},
        }
        local case = require("compliance_json").parse(
            a_case('{"deliveries":[],"post":[]}')
        )
        tested.assert(
            {
                given = "a capture carrying one delivery the expectation names none of",
                should = "name the delivery sequence, which is how silence is asserted",
                expected = 'deliveries: expected [], captured [{"from":"1:1","to":"2:1","wire":"00ff"}]',
                actual = host.assert_expectation(case, capture),
            }
        )
    end
)

tested.test(
    "an absent named field leaves the capture unconstrained on it",
    function()
        local host = require("compliance_host")
        local capture = {
            deliveries = {},
            post = {
                {
                    node = "1:1",
                    markers = {"Joining@1:1"},
                },
            },
        }
        local case = require("compliance_json").parse(
            '{"id":"the-case","family":"boot-gate","setup":[{"op":'
                .. '"provision","nodes":3,"timeout":3}],"input":{"op":"settle"},'
                .. '"expect":{"deliveries":[],"post":[{"node":"1:1",'
                .. '"status":"Normal","committed":99,"markers":["Joining@1:1"]}]}}'
        )
        tested.assert(
            {
                given = "the capture of a node that is down, against a post record naming its status",
                should = "compare only the markers: an absent field is unconstrained",
                expected = "nil",
                actual = absent(host.assert_expectation(case, capture)),
            }
        )
    end
)

tested.test(
    "a post record for a node the capture does not have is a mismatch",
    function()
        local host = require("compliance_host")
        local case = require("compliance_json").parse(
            a_case('{"deliveries":[],"post":[{"node":"9:9","status":"Normal"}]}')
        )
        tested.assert(
            {
                given = "an expectation naming a node the capture has no slot for",
                should = "name the node it has no record of",
                expected = "no post record for node 9:9",
                actual = host.assert_expectation(case, {deliveries = {}, post = {}}),
            }
        )
    end
)

tested.test(
    "the health probe names the harness role and this build's own identity",
    function()
        local host = require("compliance_host").host("/dev/null", "v9.9.9")
        local status, body = host:route("GET", "/health", "")
        local payload = require("compliance_json").parse(body)
        tested.assert(
            {
                given = "GET /health",
                should = "answer 200 naming the corpus host, this build, and its shape",
                expected = "200|uvrr-conformance|uvrr-core|v9.9.9|production+compatibility_suite",
                actual = tostring(status)
                    .. "|"
                    .. member(payload, "host")
                    .. "|"
                    .. member(payload, "crate")
                    .. "|"
                    .. member(payload, "version")
                    .. "|"
                    .. member(payload, "feature_shape"),
            }
        )
    end
)

tested.test(
    "the documented status codes answer exactly",
    function()
        local host = require("compliance_host").host("/dev/null", "v9.9.9")
        local routes: {{string, string, string, integer, string}} = {
            {"GET", "/case", "", 405, "the case endpoint takes POST"},
            {"GET", "/", "", 405, "this host serves the compliance corpus endpoints"},
            {"GET", "/session/no-such-session/capture", "", 404, "no such session"},
            {
                "POST",
                "/session/no-such-session/op",
                '{"op":"settle"}',
                404,
                "no such session",
            },
            {"POST", "/case", "not json at all", 400, "no JSON value at byte 1"},
            {"GET", "/nowhere", "", 404, "no such route: GET /nowhere"},
        }
        for _, route in ipairs(routes) do
            local status, body = host:route(route[1], route[2], route[3])
            tested.assert(
                {
                    given = route[1] .. " " .. route[2],
                    should = "answer " .. tostring(route[4]) .. " naming its refusal",
                    expected = tostring(route[4]) .. "|" .. route[5],
                    actual = tostring(status)
                        .. "|"
                        .. member(require("compliance_json").parse(body), "error"),
                }
            )
        end
    end
)

tested.test(
    "a case that cannot be replayed answers a verdict with the host's reason",
    function()
        local host = require("compliance_host").host("/dev/null", "v9.9.9")
        -- The provision cannot boot against /dev/null, so the replay is
        -- refused: the answer is still a verdict with a mismatch, never a
        -- rewritten fixture and never a `pass`.
        local status, body = host:route(
            "POST", "/case", a_case('{"deliveries":[],"post":[]}')
        )
        local payload = require("compliance_json").parse(body)
        tested.assert(
            {
                given = "a case whose cluster cannot boot",
                should = "answer 200, a fail verdict, and the host's own reason",
                expected = "200|fail|the-case|agreement",
                actual = tostring(status)
                    .. "|"
                    .. member(payload, "verdict")
                    .. "|"
                    .. member(payload, "id")
                    .. "|"
                    .. member(payload, "family"),
            }
        )
    end
)

return tested
