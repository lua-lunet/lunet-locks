local cluster_config = require("cluster_config")
local config = require("config")
local membership_guard = require("membership_guard")
local tested = require("tested")
local transport = require("transport")

local function fails(f: function(): boolean): boolean
    local ok = pcall(f)
    return not ok
end

local function nodes(): {cluster_config.ClusterNode}
    return cluster_config.parse_jsonl(
        '{"id":720897,"name":"n1","host":"127.0.0.1","port":7001,"genesis":true}\n'
            .. '{"id":1441793,"name":"n2","host":"127.0.0.1","port":7002,"genesis":true}\n'
            .. '{"id":2162689,"name":"n3","host":"127.0.0.1","port":7003,"genesis":true}\n'
    )
end

tested.test(
    "config derives id-bearing members from the deployment descriptor",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "config keeps descriptor order, rejects duplicate names and absent locals",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "transport frames partial and multiple NDJSON requests without data loss",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "membership fingerprint has one canonical ordered SHA-256 representation",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "the deployment fingerprint is over the genesis membership and survives superset growth",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "transport envelopes every peer payload with fingerprint and authenticates sources",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "transport round-trips a correlated era+view redirect without a leader id",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "transport accepts only a redirect from its current forwarding destination",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

-- Big-endian wire encoders for the Reincarnation fixtures below: the
-- announcement frame the core packs is a 20-byte header (tag, era,
-- view, slot) + a 1-byte body discriminant + `old` + `new` +
-- `committed` + `prepared`.
local function be32(value: integer): string
    return string.char(
        math.floor(value / 16777216) % 256,
        math.floor(value / 65536) % 256,
        math.floor(value / 256) % 256,
        value % 256
    )
end

local function be64(value: integer): string
    return be32(math.floor(value / 4294967296)) .. be32(value % 4294967296)
end

local function announcement_frame(old: integer, new: integer): string
    return be32(13)
        .. be32(2)
        .. be32(1)
        .. be64(0)
        .. string.char(13)
        .. be32(old)
        .. be32(new)
        .. be64(2)
        .. be64(2)
end

tested.test(
    "transport decodes the Reincarnation pair from its exact 45-byte wire shape",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a decoded pair that is not a lawful next life never arms the reincarnation remap",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "a mismatched peer fingerprint permanently quarantines client operations",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "transport rejects oversized complete and incomplete NDJSON requests",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

tested.test(
    "transport frames an extended SET line with a full-cap identity intact",
    function()
        error(
            "EXPUNGED at the uvrr0_10_x frontier: tainted by the pre-0.10 world; re-authored in the arbitration"
        )
    end
)

return tested
