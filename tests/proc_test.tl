local tested = require("tested")
local proc = require("tools.lib.proc")

tested.test(
    "stop_all reaps every tracked process, not every second one",
    function()
        -- Three real sleepers. The removal-during-iteration bug skipped
        -- every second entry of the tracked table (a ipairs over a table
        -- that M.stop's untrack table.removes mid-walk), so of seven
        -- rig-spawned nodes exactly the even-indexed ones outlived their
        -- own cleanup. Three sleepers reproduce the skip: the second one
        -- survived stop_all before the fix.
        local sleepers = {}
        for i = 1, 3 do
            sleepers[i] = proc.spawn(
                {"/bin/sleep", "30"},
                "stopall-sleeper" .. tostring(i),
                {
                    out_path = ".tmp/proc-stopall-test-" .. tostring(i) .. ".out",
                    err_path = ".tmp/proc-stopall-test-" .. tostring(i) .. ".err",
                    combine = true,
                }
            )
        end
        for i, p in ipairs(sleepers) do
            tested.assert(
                {
                    given = "three spawned sleepers",
                    should = "sleeper " .. tostring(i) .. " be alive before the stop",
                    expected = "true",
                    actual = tostring(proc.alive(p)),
                }
            )
        end
        proc.stop_all()
        local states = {}
        for i, p in ipairs(sleepers) do
            states[i] = tostring(proc.alive(p))
        end
        tested.assert(
            {
                given = "three tracked sleepers, one stop_all",
                should = "every one of them be reaped, none skipped",
                expected = "false:false:false",
                actual = table.concat(states, ":"),
            }
        )
    end
)

return tested
