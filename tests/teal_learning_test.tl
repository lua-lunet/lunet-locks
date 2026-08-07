local tested = require("tested")

local function shell_ok(cmd: string): boolean
    local res: any = os.execute(cmd)
    if type(res) == "boolean" then
        return res as boolean
    end
    return (res as number) == 0
end

local function read_file(path: string): string
    local f = io.open(path, "r")
    if f == nil then
        return ""
    end
    local data = f:read("*a")
    f:close()
    return data or ""
end

local function run_capture(cmd: string, stem: string): (boolean, string)
    local out = ".tmp/" .. stem .. ".out"
    shell_ok("rm -f " .. out)
    local ok = shell_ok(cmd .. " > " .. out .. " 2>&1")
    return ok, read_file(out)
end

tested.test(
    "learning: Teal requires explicit return annotations",
    function()
        local ok, out = run_capture(
            ".rocks/bin/cyan check tests/fixtures/no_return_annotation_fails.tl",
            "learning-no-return"
        )
        tested.assert(
            {
                given = "function without an explicit return annotation",
                should = "fail type checking with 'expected 0 ()' semantics",
                expected = true,
                actual = (not ok)
                    and out:find("expected 0 %(%)") ~= nil
                    and out:find("wrong number of arguments") ~= nil,
            }
        )
    end
)

tested.test(
    "learning: map syntax is {K:V}, not bracket notation",
    function()
        local ok, out = run_capture(
            ".rocks/bin/cyan check tests/fixtures/map_syntax_passes.tl",
            "learning-map-syntax"
        )
        tested.assert(
            {
                given = "typed map fixture",
                should = "type check successfully using {string: Point}",
                expected = true,
                actual = ok and out:find("Type checked") ~= nil,
            }
        )
    end
)

tested.test(
    "learning: duplicated records are nominally distinct across modules",
    function()
        local ok, out = run_capture(
            ".rocks/bin/cyan check tests/fixtures/nominal_b_fails.tl -I tests/fixtures",
            "learning-nominal"
        )
        tested.assert(
            {
                given = "A.Token passed where B.Token is expected",
                should = "fail type checking as a nominal mismatch",
                expected = true,
                actual = (not ok) and out:find("A%.Token is not a B%.Token") ~= nil,
            }
        )
    end
)

tested.test(
    "learning: tested marks no-assert tests as invalid",
    function()
        local ok, out = run_capture(
            ".rocks/bin/tested tests/fixtures/tested_invalid_no_assert_fixture.tl",
            "learning-tested-invalid"
        )
        tested.assert(
            {
                given = "tested fixture with a test body that runs no assertions",
                should = "report invalid / no assertions run",
                expected = true,
                actual = (not ok)
                    and out:find("invalid") ~= nil
                    and out:find("No assertions run during test") ~= nil,
            }
        )
    end
)

return tested
