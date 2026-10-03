-- The JSON codec the compliance host speaks.
--
-- The corpus is data on the wire, so the host both parses the case that
-- arrives as a request body and encodes the capture it answers with. Two
-- properties are load-bearing and the module is shaped for them:
--
-- - An object's members keep the order they were built in. The corpus
--   compares the rendered grammar literally and the transport's
--   determinism clause (`docs/uvrr-host-compliance.md` §8.3) asks for the
--   same capture on every run, so the encoding never depends on Lua's
--   hash order: an object is an ordered list of keyed members, not a
--   table.
-- - Every shape is tagged by its own `tag` field rather than inferred
--   from the Lua type, so a codec bug surfaces as a named shape
--   mismatch instead of a nil dereference three frames later.
--
-- Nothing here reaches for `lunet` or `ffi`: the codec is pure and lives
-- in the plain test suite.

local record compliance_json
    -- One object member: the key in write order and its value. An
    -- object is this list, never a Lua table, so the encoding never
    -- depends on hash order.
    record Entry
        key: string
        value: any
    end

    record Object
        tag: string
        entries: {compliance_json.Entry}
    end

    record Array
        tag: string
        items: {any}
    end

    record Text
        tag: string
        value: string
    end

    record Number
        tag: string
        value: number
    end

    record Flag
        tag: string
        value: boolean
    end

    record Nothing
        tag: string
    end

    object: function({compliance_json.Entry}): Object
    array: function({any}): Array
    text: function(string): Text
    number: function(number): Number
    flag: function(boolean): Flag
    nothing: function(): Nothing

    parse: function(string): any, string
    encode: function(any): string

    is_object: function(any): boolean
    is_array: function(any): boolean
    is_text: function(any): boolean
    is_number: function(any): boolean
    is_flag: function(any): boolean
    is_nothing: function(any): boolean

    field: function(any, string): any
    items_of: function(any): {any}
    text_of: function(any): string
    number_of: function(any): number
    flag_of: function(any): boolean
end

local Entry = compliance_json.Entry
local Object = compliance_json.Object
local Array = compliance_json.Array
local Text = compliance_json.Text
local Number = compliance_json.Number
local Flag = compliance_json.Flag
local Nothing = compliance_json.Nothing

function compliance_json.object(entries: {Entry}): Object
    return {tag = "object", entries = entries}
end

function compliance_json.array(items: {any}): Array
    return {tag = "array", items = items}
end

function compliance_json.text(value: string): Text
    return {tag = "string", value = value}
end

function compliance_json.number(value: number): Number
    return {tag = "number", value = value}
end

function compliance_json.flag(value: boolean): Flag
    return {tag = "boolean", value = value}
end

function compliance_json.nothing(): Nothing
    return {tag = "null"}
end

function compliance_json.is_object(value: any): boolean
    return type(value) == "table" and (value as Object).tag == "object"
end

function compliance_json.is_array(value: any): boolean
    return type(value) == "table" and (value as Array).tag == "array"
end

function compliance_json.is_text(value: any): boolean
    return type(value) == "table" and (value as Text).tag == "string"
end

function compliance_json.is_number(value: any): boolean
    return type(value) == "table" and (value as Number).tag == "number"
end

function compliance_json.is_flag(value: any): boolean
    return type(value) == "table" and (value as Flag).tag == "boolean"
end

function compliance_json.is_nothing(value: any): boolean
    return type(value) == "table" and (value as Nothing).tag == "null"
end

-- The member a key names, or `nil` when the key is absent. A present
-- member is never `nil` — JSON's own null is a value of its own — so
-- absence and null are distinguishable, and the corpus's optional post
-- fields are read this way: an absent field is unconstrained, never a
-- mismatch against a default.
function compliance_json.field(value: any, key: string): any
    if not compliance_json.is_object(value) then
        return nil
    end
    for _, entry in ipairs((value as Object).entries) do
        if entry.key == key then
            return entry.value
        end
    end
    return nil
end

function compliance_json.items_of(value: any): {any}
    if not compliance_json.is_array(value) then
        return {}
    end
    return (value as Array).items
end

function compliance_json.text_of(value: any): string
    if not compliance_json.is_text(value) then
        return nil
    end
    return (value as Text).value
end

function compliance_json.number_of(value: any): number
    if not compliance_json.is_number(value) then
        return nil
    end
    return (value as Number).value
end

function compliance_json.flag_of(value: any): boolean
    if not compliance_json.is_flag(value) then
        return nil
    end
    return (value as Flag).value
end

-- ----------------------------------------------------------------------
-- Encoding
-- ----------------------------------------------------------------------

-- The escape table for the encoder: the seven short forms plus the
-- control bytes that keep a JSON string printable ASCII. Anything else
-- rides as itself, which is what a UTF-8 clause's continuation bytes are.
local ESCAPES: {string: string} = {
    ['"'] = '\\"',
    ["\\"] = "\\\\",
    ["\b"] = "\\b",
    ["\f"] = "\\f",
    ["\n"] = "\\n",
    ["\r"] = "\\r",
    ["\t"] = "\\t",
}

local function quote(text: string): string
    local out = '"'
    for index = 1, #text do
        local byte = string.byte(text, index) as integer
        local short = ESCAPES[string.char(byte)]
        if short ~= nil then
            out = out .. short
        elseif byte < 0x20 or byte == 0x7f then
            out = out .. string.format("\\u%04x", byte)
        else
            out = out .. string.char(byte)
        end
    end
    return out .. '"'
end

-- A number is written as an integer when it is one. The corpus's
-- frontiers and journal slots are all integral and the client compares
-- them as JSON integers, so `3` never travels as `3.0`.
local function numeral(value: number): string
    if value ~= value or value == math.huge or value == -math.huge then
        error("compliance_json: a non-finite number has no JSON encoding", 0)
    end
    if math.floor(value) == value and math.abs(value) < 1e15 then
        return string.format("%d", value)
    end
    return string.format("%.17g", value)
end

local function encode_into(value: any): string
    if value == nil or compliance_json.is_nothing(value) then
        return "null"
    elseif compliance_json.is_object(value) then
        local pieces: {string} = {}
        for _, entry in ipairs((value as Object).entries) do
            table.insert(pieces, quote(entry.key) .. ":" .. encode_into(entry.value))
        end
        return "{" .. table.concat(pieces, ",") .. "}"
    elseif compliance_json.is_array(value) then
        local pieces: {string} = {}
        for _, item in ipairs((value as Array).items) do
            table.insert(pieces, encode_into(item))
        end
        return "[" .. table.concat(pieces, ",") .. "]"
    elseif compliance_json.is_text(value) then
        return quote((value as Text).value)
    elseif compliance_json.is_number(value) then
        return numeral((value as Number).value)
    elseif compliance_json.is_flag(value) then
        return (value as Flag).value and "true" or "false"
    end
    error("compliance_json: no JSON encoding for " .. type(value), 0)
end

function compliance_json.encode(value: any): string
    return encode_into(value)
end

-- ----------------------------------------------------------------------
-- Parsing
-- ----------------------------------------------------------------------

local WHITESPACE: {string: boolean} = {
    [" "] = true,
    ["\t"] = true,
    ["\n"] = true,
    ["\r"] = true,
}

local UNESCAPES: {string: string} = {
    ['"'] = '"',
    ["\\"] = "\\",
    ["/"] = "/",
    b = "\b",
    f = "\f",
    n = "\n",
    r = "\r",
    t = "\t",
}

-- The four UTF-8 encodings, by code point. A `\u` escape outside the
-- Basic Multilingual Plane arrives as a surrogate pair; a lone surrogate
-- is U+FFFD, the replacement character, because the corpus is data and a
-- malformed escape must not become a byte the encoder cannot frame.
local function utf8_encode(code: integer): string
    if code < 0 or code > 0x10ffff then
        return "\239\191\189"
    end
    if code < 0x80 then
        return string.char(code)
    elseif code < 0x800 then
        return string.char(0xc0 + math.floor(code / 0x40), 0x80 + code % 0x40)
    elseif code < 0x10000 then
        return string.char(
            0xe0 + math.floor(code / 0x1000),
            0x80 + math.floor(code / 0x40) % 0x40,
            0x80 + code % 0x40
        )
    end
    return string.char(
        0xf0 + math.floor(code / 0x40000),
        0x80 + math.floor(code / 0x1000) % 0x40,
        0x80 + math.floor(code / 0x40) % 0x40,
        0x80 + code % 0x40
    )
end

local function at(text: string, index: integer): string
    return string.sub(text, index, index)
end

-- The code point of a `\u` escape whose `u` sits at `index`, decoding a
-- surrogate pair as the single code point it names.
local function code_unit(text: string, index: integer): integer, string
    local digits = string.sub(text, index + 1, index + 4)
    if #digits < 4 or digits:find("^%x%x%x%x$") == nil then
        return nil, "a \\u escape needs four hex digits"
    end
    local value = tonumber(digits, 16) as integer
    if value >= 0xd800 and value <= 0xdbff then
        local low = string.sub(text, index + 7, index + 10)
        if
            string.sub(text, index + 5, index + 6) ~= "\\u"
            or low:find("^%x%x%x%x$") == nil
        then
            return nil, "a high surrogate escape is not followed by its low surrogate"
        end
        local tail = tonumber(low, 16) as integer
        if tail < 0xdc00 or tail > 0xdfff then
            return nil, "a high surrogate escape is not followed by its low surrogate"
        end
        return 0x10000 + (value - 0xd800) * 0x400 + (tail - 0xdc00), nil
    end
    if value >= 0xdc00 and value <= 0xdfff then
        return nil, "a low surrogate escape stands alone"
    end
    return value, nil
end

local parse_value: function(string, integer): any, integer, string

-- The JSON string opening at the quote `index`: the text, then the byte
-- after the closing quote.
local function parse_string(text: string, index: integer): string, integer, string
    if at(text, index) ~= '"' then
        return nil, index, "a string must open with a quote"
    end
    local out = ""
    local cursor = index + 1
    while true do
        local byte = string.byte(text, cursor)
        if byte == nil then
            return nil, index, "a string is not terminated"
        elseif byte == 0x22 then
            return out, cursor + 1, nil
        elseif byte == 0x5c then
            local escape = at(text, cursor + 1)
            local simple = UNESCAPES[escape]
            if simple ~= nil then
                out = out .. simple
                cursor = cursor + 2
            elseif escape == "u" then
                local value, failure = code_unit(text, cursor + 1)
                if value == nil then
                    return nil, index, failure as string
                end
                out = out .. utf8_encode(value)
                if value > 0xffff then
                    cursor = cursor + 12
                else
                    cursor = cursor + 6
                end
            else
                return nil, index, "no such string escape: \\" .. escape
            end
        elseif byte < 0x20 then
            return nil, index, "a raw control byte cannot stand in a string"
        else
            out = out .. string.char(byte)
            cursor = cursor + 1
        end
    end
end

local function skip_whitespace(text: string, index: integer): integer
    local cursor = index
    while WHITESPACE[at(text, cursor)] == true do
        cursor = cursor + 1
    end
    return cursor
end

-- The number grammar, read as far as it is legal to read and no further.
local function parse_number(text: string, index: integer): number, integer, string
    local start = index
    local cursor = index
    if at(text, cursor) == "-" then
        cursor = cursor + 1
    end
    local whole = string.match(text, "^%d+", cursor)
    if whole == nil then
        return nil, index, "a number needs its integer part"
    end
    cursor = cursor + #whole
    local rest = string.sub(text, cursor)
    local point = rest:match("^(%.%d+)")
    if point ~= nil then
        cursor = cursor + #point
        rest = string.sub(text, cursor)
    end
    local power = rest:match("^[eE][-+]?%d+")
    if power ~= nil then
        cursor = cursor + #power
    end
    local literal = string.sub(text, start, cursor - 1)
    local value = tonumber(literal)
    if value == nil then
        return nil, index, "not a JSON number: " .. literal
    end
    return value, cursor, nil
end

local function parse_array(text: string, index: integer): any, integer, string
    local items: {any} = {}
    local cursor = skip_whitespace(text, index + 1)
    if at(text, cursor) == "]" then
        return compliance_json.array(items), cursor + 1, nil
    end
    while true do
        local value, after, failure = parse_value(text, cursor)
        if failure ~= nil then
            return nil, index, failure
        end
        table.insert(items, value)
        cursor = skip_whitespace(text, after)
        local separator = at(text, cursor)
        if separator == "," then
            cursor = skip_whitespace(text, cursor + 1)
        elseif separator == "]" then
            return compliance_json.array(items), cursor + 1, nil
        else
            return nil, index, "an array member is followed by a comma or ]"
        end
    end
end

local function parse_object(text: string, index: integer): any, integer, string
    local entries: {Entry} = {}
    local cursor = skip_whitespace(text, index + 1)
    if at(text, cursor) == "}" then
        return compliance_json.object(entries), cursor + 1, nil
    end
    while true do
        local key, after, failure = parse_string(text, cursor)
        if failure ~= nil then
            return nil, index, failure
        end
        cursor = skip_whitespace(text, after)
        if at(text, cursor) ~= ":" then
            return nil, index, "an object member needs a colon after its name"
        end
        local value, past, broken = parse_value(text, cursor + 1)
        if broken ~= nil then
            return nil, index, broken
        end
        table.insert(entries, {key = key, value = value})
        cursor = skip_whitespace(text, past)
        local separator = at(text, cursor)
        if separator == "," then
            cursor = skip_whitespace(text, cursor + 1)
        elseif separator == "}" then
            return compliance_json.object(entries), cursor + 1, nil
        else
            return nil, index, "an object member is followed by a comma or }"
        end
    end
end

function parse_value(text: string, index: integer): any, integer, string
    local cursor = skip_whitespace(text, index)
    local head = at(text, cursor)
    if head == "{" then
        return parse_object(text, cursor)
    elseif head == "[" then
        return parse_array(text, cursor)
    elseif head == '"' then
        local value, after, failure = parse_string(text, cursor)
        if failure ~= nil then
            return nil, index, failure
        end
        return compliance_json.text(value), after, nil
    elseif head == "-" or (head ~= "" and head:find("%d") ~= nil) then
        local value, after, failure = parse_number(text, cursor)
        if failure ~= nil then
            return nil, index, failure
        end
        return compliance_json.number(value), after, nil
    elseif string.sub(text, cursor, cursor + 3) == "true" then
        return compliance_json.flag(true), cursor + 4, nil
    elseif string.sub(text, cursor, cursor + 4) == "false" then
        return compliance_json.flag(false), cursor + 5, nil
    elseif string.sub(text, cursor, cursor + 3) == "null" then
        return compliance_json.nothing(), cursor + 4, nil
    end
    return nil, index, "no JSON value at byte " .. tostring(cursor)
end

function compliance_json.parse(text: string): any, string
    local value, cursor, failure = parse_value(text, 1)
    if failure ~= nil then
        return nil, failure
    end
    cursor = skip_whitespace(text, cursor)
    if cursor <= #text then
        return nil, "trailing text after the JSON value at byte " .. tostring(cursor)
    end
    return value, nil
end

return compliance_json
