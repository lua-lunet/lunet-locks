local advisory_lock = require("advisory_lock")
local cluster_config = require("cluster_config")
local tested = require("tested")

-- A unique scratch marker path under the repo's .tmp: each test's node
-- boots on its own marker and its own WAL artefact.
local function scratch_state(name: string): string
    local path = ".tmp/teal-ffi/"
        .. name
        .. "-"
        .. tostring(os.time())
        .. "-"
        .. tostring(math.random(1000000))
        .. ".nonce"
    os.execute("mkdir -p .tmp/teal-ffi")
    return path
end

-- A three-member deployment descriptor: distinct provisioned
-- identities (the system half in the high sixteen bits, counter 1), the
-- genesis succession order.
local function members(): {cluster_config.ClusterNode}
    local nodes: {cluster_config.ClusterNode} = {}
    for index = 1, 3 do
        table.insert(
            nodes,
            {
                id = index * 65536 + 1,
                name = ({"a", "b", "c"})[index] as string,
                host = "127.0.0.1",
                port = 27200 + index,
                genesis = true,
            }
        )
    end
    return nodes
end

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

-- The state seam's FFI surface, against the real cdylib: the WAL store
-- mounts at the node's construction, a tick with nothing changed is a
-- no-op success, the per-stream counters read all zeros through
-- `state_status`, and the stop completes clean with the
-- submit-before-flush seat in place.
tested.test(
    "the state tick and the per-stream counters ride the FFI",
    function()
        local node = advisory_lock.new(members(), "a", scratch_state("tick"), "", 0)
        local ok, outcome = node:state_tick()
        tested.assert(
            {
                given = "a fresh node's state tick with nothing changed",
                should = "submit nothing and report success",
                expected = "true:",
                actual = tostring(ok) .. ":" .. (outcome or "?"),
            }
        )
        local counters, status_ok, status_error = node:state_status()
        tested.assert(
            {
                given = "a fresh WAL mount's per-stream status read",
                should = "answer with the counters, never raise",
                expected = "true:",
                actual = tostring(status_ok) .. ":" .. (status_error or "?"),
            }
        )
        tested.assert(
            {
                given = "a fresh WAL mount's per-stream counters",
                should = "read zero across both streams, nothing outstanding",
                expected = "0:0:0:0:0:0:0:0",
                actual = table.concat(
                    {
                        tostring(counters.ephemeral.completed_generations),
                        tostring(counters.ephemeral.completed_records),
                        tostring(counters.ephemeral.refusals),
                        tostring(counters.ephemeral.in_flight),
                        tostring(counters.durable.completed_generations),
                        tostring(counters.durable.completed_records),
                        tostring(counters.durable.refusals),
                        tostring(counters.durable.in_flight),
                    },
                    ":"
                ),
            }
        )
        tested.assert(
            {
                given = "a node stopped through the drain point",
                should = "stop clean with the tick-before-flush seat in place",
                expected = "true",
                actual = tostring(node:stop()),
            }
        )
    end
)

return tested
