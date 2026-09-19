-- Typed gate helpers: deadline-bounded polling and loud failure. A gate
-- that finds nothing reports the condition it waited for and exits
-- nonzero — silence is the defect this library exists to prevent.

local clock = require("tools.lib.clock")

local record M
    fail: function(string)
    wait_for: function(integer, integer, function(): boolean, string): boolean, string
    sleep_s: function(number)
end

-- The one failure path: a clear message raised to the runner's guarded
-- body, which reports it on stderr and exits nonzero.
function M.fail(message: string)
    error("FAIL: " .. message, 0)
end

-- Polls `check` every `interval_ms` until it returns true or the
-- deadline passes. The check returns (done, status): `status` is the
-- gate's live context — the measured outcome on success, the missing
-- evidence on the loud failure. Never silently passes.
function M.wait_for(
    deadline_ms: integer, interval_ms: integer, check: function(): boolean, string
): boolean, string
    local last = ""
    while clock.now_ms() < deadline_ms do
        local done, report = check()
        if done then
            return true, report
        end
        last = report
        clock.sleep(interval_ms / 1000)
    end
    return false, last
end

function M.sleep_s(seconds: number)
    clock.sleep(seconds)
end

return M
