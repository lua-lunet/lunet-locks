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
        local is_boolean = properties.dirty == "true" or properties.dirty == "false"
        -- The dirty stamp is a fact about the tree at build time, so its
        -- value is the build's and not the console's: the console's
        -- contract is that it arrives as one of the two booleans, and a
        -- clean-tree build is a correct build.
        local dirty = is_boolean and "a-boolean" or properties.dirty
        local boolean = tostring(is_boolean)
        tested.assert(
            {
                given = "the build's own identity facts, as the console reports them",
                should = "the four keys, the feature shape named production first, dirty a boolean",
                expected = "4:production:a-boolean:true",
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
