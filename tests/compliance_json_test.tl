local tested = require("tested")

-- `tested.assert` refuses a nil on either side, so an absence is
-- asserted as its own rendering.
local function absent(value: any): string
    return tostring(value)
end

-- The codec is the boundary the corpus crosses twice: it parses the case
-- a client posts and it encodes the capture the host answers with. Both
-- directions are pinned here, because a codec that drifts shows up in
-- the corpus as a case failing for the wrong reason.

tested.test(
    "the corpus's own case record parses and re-encodes to the same document",
    function()
        local json = require("compliance_json")
        local text = '{"id":"a-case","family":"agreement","setup":[{"op":'
            .. '"provision","nodes":3,"timeout":3},{"op":"settle"}],"input":'
            .. '{"op":"propose","node":"1:1","payload":"agreed"},"expect":'
            .. '{"deliveries":[{"from":"1:1","to":"2:1","wire":"00ff"}],'
            .. '"post":[{"node":"1:1","status":"Normal","weights":[1,1,1]}]}}'
        local value, failure = json.parse(text)
        local setup = json.items_of(json.field(value, "setup"))
        local post = json.items_of(json.field(json.field(value, "expect"), "post"))
        tested.assert(
            {
                given = "a case record in the transport's own shape",
                should = "parse whole, with every member the corpus names",
                expected = "agreement|a-case|provision|3|settle|1|agreed|Normal|3",
                actual = json.text_of(json.field(value, "family"))
                    .. "|"
                    .. json.text_of(json.field(value, "id"))
                    .. "|"
                    .. json.text_of(json.field(setup[1], "op"))
                    .. "|"
                    .. tostring(json.number_of(json.field(setup[1], "nodes")))
                    .. "|"
                    .. json.text_of(json.field(setup[2], "op"))
                    .. "|"
                    .. tostring(
                        #json.items_of(
                            json.field(json.field(value, "expect"), "deliveries")
                        )
                    ) .. "|"
                    .. json.text_of(json.field(json.field(value, "input"), "payload"))
                    .. "|"
                    .. json.text_of(json.field(post[1], "status"))
                    .. "|"
                    .. tostring(#json.items_of(json.field(post[1], "weights"))),
            }
        )
        tested.assert(
            {
                given = "the document",
                should = "parse with no refusal",
                expected = "nil",
                actual = absent(failure),
            }
        )
        tested.assert(
            {
                given = "the same record",
                should = "re-encode to the very bytes it arrived as",
                expected = text,
                actual = json.encode(value),
            }
        )
    end
)

tested.test(
    "an integer never travels as a decimal and a string escapes as JSON",
    function()
        local json = require("compliance_json")
        local value = json.object(
            {
                {key = "committed", value = json.number(3)},
                {key = "clause", value = json.text('a "quoted" \\ back\nline')},
            }
        )
        tested.assert(
            {
                given = "a committed frontier and a clause carrying JSON's own escapes",
                should = "encode as an integer and as JSON string escapes",
                expected = '{"committed":3,"clause":"a \\"quoted\\" \\\\ back\\nline"}',
                actual = json.encode(value),
            }
        )
    end
)

tested.test(
    "a member order is the order it was built in, never the hash order",
    function()
        local json = require("compliance_json")
        local text = '{"a":1,"b":2,"c":3,"d":4,"e":5,"f":6}'
        local stable = true
        for _ = 1, 64 do
            local value = json.parse(text)
            if json.encode(value) ~= text then
                stable = false
            end
        end
        tested.assert(
            {
                given = "the same object parsed and encoded sixty-four times",
                should = "keep its member order every time: the transport's determinism clause",
                expected = "true",
                actual = tostring(stable),
            }
        )
    end
)

tested.test(
    "an absent member and a null member are distinguishable",
    function()
        local json = require("compliance_json")
        local value = json.parse('{"present":null}')
        tested.assert(
            {
                given = "a member whose value is JSON null",
                should = "be present, so an absent field stays distinguishable",
                expected = "true",
                actual = tostring(json.field(value, "present") ~= nil),
            }
        )
        tested.assert(
            {
                given = "a member whose value is JSON null",
                should = "read as the null shape",
                expected = "true",
                actual = tostring(json.is_nothing(json.field(value, "present"))),
            }
        )
        tested.assert(
            {
                given = "a member the document does not carry",
                should = "read as absent",
                expected = "nil",
                actual = absent(json.field(value, "absent")),
            }
        )
    end
)

tested.test(
    "the escapes the corpus can carry, both ways",
    function()
        local json = require("compliance_json")
        local value, failure = json.parse(
            '"\\u0076\\u00e9\\ud83d\\ude00\\n\\t\\/\\\\\\""'
        )
        tested.assert(
            {
                given = "the document",
                should = "parse with no refusal",
                expected = "nil",
                actual = absent(failure),
            }
        )
        tested.assert(
            {
                given = "a surrogate pair, a Latin-1 code point and the short escapes",
                should = "decode to their UTF-8 bytes and the plain characters",
                expected = "v\195\169\240\159\152\128\n\t/\\\"",
                actual = json.text_of(value),
            }
        )
    end
)

tested.test(
    "a malformed body is refused with a reason, never half-parsed",
    function()
        local json = require("compliance_json")
        local bodies = {
            "",
            "not json at all",
            "{",
            '{"a":}',
            '{"a":1,}',
            "[1 2]",
            '{"a":1} trailing',
            '"unterminated',
            '{"a":"\\q"}',
            '"\\ud800"',
            '"\\u00zz"',
            '"\\udc00"',
        }
        for _, text in ipairs(bodies) do
            local value, failure = json.parse(text)
            tested.assert(
                {
                    given = "the malformed body " .. string.format("%q", text),
                    should = "parse to nothing",
                    expected = "nil",
                    actual = absent(value),
                }
            )
            tested.assert(
                {
                    given = "the malformed body " .. string.format("%q", text),
                    should = "name its refusal",
                    expected = "string",
                    actual = type(failure),
                }
            )
        end
    end
)

return tested
