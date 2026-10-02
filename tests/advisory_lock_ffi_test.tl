local advisory_lock = require("advisory_lock")
local tested = require("tested")

tested.test(
    "the read-only console reports the build's own identity",
    function()
        local properties = advisory_lock.version_properties()
        local keys: {string} = {}
        for _, key in ipairs({"version", "sha", "dirty", "feature_shape"}) do
            local value = properties[key]
            assert(
                type(value) == "string" and value ~= "", "the console carries " .. key
            )
            table.insert(keys, key)
        end
        local feature = properties.feature_shape:match("^[^+]*") or "?"
        local dirty = tostring(properties.dirty == "true")
        local boolean = tostring(
            properties.dirty == "true" or properties.dirty == "false"
        )
        tested.assert(
            {
                given = "the build's own identity facts, as the console reports them",
                should = "the four keys, the feature shape named production first, dirty a boolean",
                expected = "4:production:true:true",
                actual = tostring(#keys)
                    .. ":"
                    .. feature
                    .. ":"
                    .. dirty
                    .. ":"
                    .. boolean,
            }
        )
    end
)

return tested
