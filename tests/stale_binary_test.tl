local tested = require("tested")
local stale_binary = require("tools.lib.stale_binary")

-- The stale-binary rule's decision table, over measured facts rather than
-- over a real artifact: a lane never measures a stale artifact, and this
-- test is what keeps the table from losing a leg.
--
-- Two facts decide: the commit the artifact's stamp names ("" when it names
-- none) against the commit the tree names ("" when it names none), and the
-- newest source that feeds the artifact against the artifact's own mtime.

local HEAD = "528eeb419a02"

local function artifact(
    sha: string, dirty: string, mtime_s: integer, mtime_ns: integer
): stale_binary.Artifact
    local stamp: {string: string} = nil
    if sha ~= nil then
        stamp = {
            sha = sha,
            dirty = dirty,
            feature_shape = "production",
            version = "unknown",
        }
    end
    return {
        label = "the artifact",
        path = "/nowhere/the-artifact",
        stamp = stamp,
        mtime_s = mtime_s,
        mtime_ns = mtime_ns,
        note = "",
    }
end

local function profile(one: stale_binary.Artifact): stale_binary.Profile
    return {
        label = "the artifact",
        rebuild = "rebuild it",
        artifacts = {one},
        sources = {},
    }
end

local function tree(
    head: string, newest_s: integer, newest_ns: integer
): stale_binary.Tree
    return {
        head = head,
        newest_path = "src/lib.rs",
        newest_s = newest_s,
        newest_ns = newest_ns,
    }
end

-- The verdict as one word plus the leg that decided it, so a test asserts
-- both the outcome and which fact produced it.
local function decide(one: stale_binary.Artifact, measured: stale_binary.Tree): string
    local fresh, reason = stale_binary.verdict(profile(one), measured)
    if fresh then
        return "fresh"
    end
    if reason:find("built from commit") then
        return "identity"
    end
    if reason:find("is newer than the artifact") then
        return "age"
    end
    if reason:find("not built") then
        return "absent"
    end
    return "other:" .. reason
end

tested.test(
    "the identity leg refuses a stamp from another commit, even when the artifact is the newest file on disk",
    function()
        tested.assert(
            {
                given = "an artifact stamped with another commit, and no source newer than it",
                should = "refuse on the identity leg, naming both commits",
                expected = "identity",
                actual = decide(
                    artifact("395dd93ebd9f", "false", 2000, 0), tree(HEAD, 1000, 0)
                ),
            }
        )
    end
)

tested.test(
    "the identity leg refuses a stamp where the tree names no commit",
    function()
        tested.assert(
            {
                given = "an artifact stamped with a commit, in a tree that names none",
                should = "refuse on the identity leg: nothing can show them equal",
                expected = "identity",
                actual = decide(
                    artifact("395dd93ebd9f", "false", 2000, 0), tree("", 1000, 0)
                ),
            }
        )
    end
)

tested.test(
    "a stamp that names no commit is no better than no stamp at all",
    function()
        tested.assert(
            {
                given = "an artifact whose console reported sha=unknown, against a tree at HEAD",
                should = "pass on identity and let the age leg carry it",
                expected = "fresh",
                actual = decide(
                    artifact("unknown", "true", 2000, 0), tree(HEAD, 1000, 0)
                ),
            }
        )
    end
)

tested.test(
    "the age leg refuses a source edited after the build, at nanosecond resolution",
    function()
        tested.assert(
            {
                given = "an artifact stamped at HEAD whose newest source is one nanosecond newer",
                should = "refuse on the age leg",
                expected = "age",
                actual = decide(artifact(HEAD, "true", 2000, 0), tree(HEAD, 2000, 1)),
            }
        )
    end
)

tested.test(
    "the age leg carries the dirty tree: a dirty build of this commit with no source moved is fresh",
    function()
        tested.assert(
            {
                given = "an artifact stamped dirty at HEAD, every source older than it",
                should = "pass: the stamp names the commit and the age leg certifies the content",
                expected = "fresh",
                actual = decide(artifact(HEAD, "true", 2000, 0), tree(HEAD, 1000, 0)),
            }
        )
    end
)

tested.test(
    "an artifact with no identity console is judged on its age alone",
    function()
        tested.assert(
            {
                given = "an executable with no console, older than every source",
                should = "pass: the age leg is the whole of the rule for it",
                expected = "fresh",
                actual = decide(artifact(nil, nil, 2000, 0), tree(HEAD, 1000, 0)),
            }
        )
        tested.assert(
            {
                given = "the same executable with a source newer than it",
                should = "refuse on the age leg",
                expected = "age",
                actual = decide(artifact(nil, nil, 2000, 0), tree(HEAD, 3000, 0)),
            }
        )
    end
)

tested.test(
    "an artifact that is not built is refused, never booted",
    function()
        tested.assert(
            {
                given = "no artifact on disk at all",
                should = "refuse, and say it is not built",
                expected = "absent",
                actual = decide(artifact(HEAD, "false", -1, -1), tree(HEAD, 1000, 0)),
            }
        )
    end
)

tested.test(
    "a tree with no checkout of its own is judged on the age leg alone",
    function()
        tested.assert(
            {
                given = "an extracted release archive: no HEAD, no stamp, no source newer",
                should = "pass: neither side names a commit, so neither can be behind the other",
                expected = "fresh",
                actual = decide(artifact(nil, nil, 2000, 0), tree("", -1, -1)),
            }
        )
    end
)

return tested
