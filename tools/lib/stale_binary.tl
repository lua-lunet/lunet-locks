-- The stale-binary rule: a lane never measures a stale artifact
-- (docs/src/test-scaffold.md). One check, every lane: the lane says which
-- artifact it is about to boot, and this library decides whether that
-- artifact can be booted at all. A lane that does not ask never gets to
-- boot one, and a lane that asks and is told no stops there.
--
-- Two legs, both required:
--
--   IDENTITY  The artifact's build-time stamp names the commit it was built
--             from. `ext/advisory_lock/src/info.rs` compiles
--             `LUNET_INFO_VERSION`, `LUNET_INFO_SHA`, `LUNET_INFO_DIRTY` and
--             `LUNET_INFO_FEATURES` into the library, and
--             `lunet_lock_version_properties` is the read-only console that
--             hands them out, so the question is asked of the artifact and
--             not of a side file that could disagree with it. The tree's HEAD
--             names the commit it is. Two stamps that name the same commit
--             pass; two that name no commit at all pass (a release tarball
--             and a Docker build context carry no `.git`, so neither side can
--             name one and neither can be behind the other); a stamp and a
--             HEAD that name DIFFERENT commits, or a stamp that names a
--             commit where the tree names none, refuse. A missing stamp is
--             not a failure - see AGE - but it is always announced.
--
--   AGE       No source file that feeds the artifact is newer than the
--             artifact. "Feeds" is every tracked and untracked-not-ignored
--             file under the crate's own source roots, so build outputs and
--             tool caches (all gitignored) can never raise a false alarm.
--             AGE is a conservative superset of cargo's own mtime
--             fingerprint: anything cargo would rebuild, AGE calls stale, and
--             a source file that has not moved since the build is the only
--             way AGE passes.
--
-- AGE is what carries the dirty tree, and it is why a dirty tree is not a
-- special case here. A build stamped `dirty=true` names its commit but not
-- its content, so IDENTITY cannot certify it and there is no third fact to
-- ask for: the stamp is one bool and one hash. The gates stamp dirty by
-- design and run on dirty trees constantly, so refusing every dirty build
-- would make the rule refuse every local run and prove nothing. What a dirty
-- build actually needs is the one question the stamp cannot answer - has a
-- source file moved since the build - and AGE answers it. A dirty stamp is
-- therefore annotated, never refused; AGE refuses the dirty build whose
-- source has moved, and passes the one whose source has not.
--
-- AGE also carries the artifacts that carry no console at all. The
-- lease-sequencer binaries are executables and expose no identity surface, so
-- AGE is the whole of their rule, which is the honest fallback: it is the
-- same comparison the CDylib's age leg makes, minus the commit cross-check
-- the console would have allowed.

local ffi = require("ffi")
local fs = require("tools.lib.fs")
local gate = require("tools.lib.gate")
local shell = require("tools.lib.shell")

ffi.cdef(
    [[
   int32_t lunet_lock_version_properties(uint8_t* data, size_t capacity, size_t* len);
]]
)

local record Native
    lunet_lock_version_properties: function(any, integer, {number}): number
end

-- One artifact, already asked: where it is, what it says about itself, and
-- when it was last written. `stamp` is nil when the artifact carries no
-- identity console.
local record Artifact
    label: string
    path: string
    stamp: {string: string}
    mtime_s: integer
    mtime_ns: integer
    note: string
end

-- The tree an artifact is booted from, already measured: the commit it names
-- (`""` when it names none) and the newest file that feeds the artifact.
local record Tree
    head: string
    newest_path: string
    newest_s: integer
    newest_ns: integer
end

-- What one lane is about to boot: the artifacts, the repo-relative source
-- roots that feed them, and the command that rebuilds them. The rebuild
-- command is named in the refusal so the operator's next keystroke is the
-- fix, not a search.
local record Profile
    label: string
    rebuild: string
    artifacts: {Artifact}
    sources: {string}
end

local record M
    Artifact: Artifact
    Profile: Profile
    Tree: Tree
    verdict: function(Profile, Tree): boolean, string
    cdylib: function(string): Profile
    lease_node: function(string): Profile
    lease_ladder: function(string): Profile
    standby_stack: function(string): Profile
    snapshot_tools: function(string): Profile
    lock_feed: function(string): Profile
    image: function(string): Profile
    check: function(string, Profile): integer
end

local CDYLIB_SOURCES = {
    "ext/advisory_lock",
    "ext/lunet-locks-aof",
    ".cargo",
    "rust-toolchain.toml",
}

-- The same roots plus the example crate that links the CDylib statically.
local LEASE_SOURCES = {
    "examples/lease-sequencer",
    "ext/advisory_lock",
    "ext/lunet-locks-aof",
    ".cargo",
    "rust-toolchain.toml",
}

-- The console feed's own roots: its crate, the vendored cargo config and
-- the toolchain pin.
local FEED_SOURCES = {
    "ext/lock_feed",
    ".cargo",
    "rust-toolchain.toml",
}

local CDYLIB_REBUILD = "cargo build --release --manifest-path ext/advisory_lock/Cargo.toml"
local LEASE_REBUILD = "cargo build --release --manifest-path examples/lease-sequencer/Cargo.toml"

-- Ask one artifact what it is. `nil` when it carries no identity console: a
-- plain executable that cannot be dlopen'ed, a library built before the
-- console existed, or a path that is not there at all. A missing console is
-- not a failure here; the age leg is the whole of the rule for such an
-- artifact, and the refusal text says so out loud.
local function ask(path: string): {string: string}
    if not fs.exists(path) then
        return nil
    end
    local opened: {any} = {pcall(ffi.load, path)}
    if not opened[1] or opened[2] == nil then
        return nil
    end
    local handle = opened[2] as Native
    local needed = ffi.new("size_t[1]") as {number}
    local probe: {any} = {
        pcall(
            function(): integer
                return (
                    tonumber(handle.lunet_lock_version_properties(nil, 0, needed))
                    or -127
                ) as integer
            end
        ),
    }
    if not probe[1] or (tonumber(needed[0]) or 0) < 1 then
        return nil
    end
    local size = needed[0] as integer
    local data = ffi.new("uint8_t[?]", size + 1) as any
    local read: {any} = {
        pcall(
            function(): integer
                return (
                    tonumber(
                        handle.lunet_lock_version_properties(data, size + 1, needed)
                    ) or -127
                ) as integer
            end
        ),
    }
    if not read[1] then
        return nil
    end
    local properties: {string: string} = {}
    for line in (ffi.string(data, size) as string):gmatch("[^\n]+") do
        local key, value = line:match("^([^=]+)=(.*)$")
        if key ~= nil and value ~= nil then
            properties[key] = value
        end
    end
    return properties
end

-- A binary artifact: no console, so the stamp is nil and the age leg carries
-- it whole. Its own mtime is read the same way as any other file's.
local function binary(label: string, path: string): Artifact
    local mtime_s, mtime_ns = fs.mtime(path)
    return {
        label = label,
        path = path,
        stamp = nil,
        mtime_s = mtime_s,
        mtime_ns = mtime_ns,
        note = "",
    }
end

-- A CDylib: asked for its stamp, and stamped with its own mtime when it has
-- one to give.
local function library(label: string, path: string): Artifact
    local mtime_s, mtime_ns = fs.mtime(path)
    return {
        label = label,
        path = path,
        stamp = ask(path),
        mtime_s = mtime_s,
        mtime_ns = mtime_ns,
        note = "",
    }
end

local function soext(): string
    local suffix = "so"
    if ffi.os == "OSX" then
        suffix = "dylib"
    end
    return suffix
end

-- Where the example crate's release binaries land. Every lane that boots one
-- of them boots it from here.
local function lease_dir(root: string): string
    return fs.join(root, "examples/lease-sequencer/target/release")
end

-- The tree's commit, twelve hex digits to match the stamp's own width, or
-- `""` where there is no checkout to ask (a release tarball, an extracted
-- release archive).
local function head_sha(root: string): string
    local out = shell.capture(
        "git -C " .. shell.quote(root) .. " rev-parse --short=12 HEAD 2>/dev/null"
    )
    return out:match("^%s*(%x+)%s*$") or ""
end

-- The newest file under each source root, as tracked plus
-- untracked-not-ignored: the files a build can read. A source root that is
-- itself a checkout (the `ext/lunet-locks-aof` submodule) is asked where it
-- lives, because the parent index names a submodule as one gitlink and
-- would hide every file inside it.
local function newest_source(root: string, sources: {string}): string, integer, integer
    local best_path = ""
    local best_s = -1
    local best_ns = -1
    for _, source in ipairs(sources) do
        local cwd = root
        if fs.is_dir(fs.join(root, source, ".git")) then
            cwd = fs.join(root, source)
        end
        local out = shell.capture(
            "git -C "
                .. shell.quote(cwd)
                .. " ls-files -z --cached --others --exclude-standard -- "
                .. shell.quote(source)
                .. " 2>/dev/null"
        )
        for entry in out:gmatch("([^%z]+)") do
            if entry ~= "" then
                local path = fs.join(root, entry)
                local mtime_s, mtime_ns = fs.mtime(path)
                if mtime_s > best_s or (mtime_s == best_s and mtime_ns > best_ns) then
                    best_path = entry
                    best_s = mtime_s
                    best_ns = mtime_ns
                end
            end
        end
    end
    return best_path, best_s, best_ns
end

-- A modification time as UTC, to the nanosecond. Two moments a nanosecond
-- apart are the whole difference between "built from this tree" and "behind
-- it", so the diagnostic prints both moments exactly as they were compared.
local function moment(seconds: integer, nanoseconds: integer): string
    if seconds < 0 then
        return "(absent)"
    end
    local fraction = string.format("%09d", nanoseconds)
    return (os.date("!%Y-%m-%dT%H:%M:%SZ", tonumber(seconds)) as string)
        .. "."
        .. fraction
end

-- RFC 3339 to epoch seconds, in Lua, so the image's creation time needs no
-- `date` and no per-platform branch. Nanoseconds are dropped: Docker's clock
-- has one-second resolution here, and a source edited in the same second the
-- image was built counts as newer, which is the safe direction.
local function rfc3339_seconds(stamp: string): integer
    local year, month, day, hour, minute, second = stamp:match(
        "^(%d%d%d%d)%-(%d%d)%-(%d%d)T(%d%d):(%d%d):(%d%d)"
    )
    if year == nil then
        return -1
    end
    return os.time(
        {
            year = tonumber(year) as integer,
            month = tonumber(month) as integer,
            day = tonumber(day) as integer,
            hour = tonumber(hour) as integer,
            min = tonumber(minute) as integer,
            sec = tonumber(second) as integer,
        }
    ) as integer
end

local function measure_tree(root: string, sources: {string}): Tree
    local newest_path, newest_s, newest_ns = newest_source(root, sources)
    return {
        head = head_sha(root),
        newest_path = newest_path,
        newest_s = newest_s,
        newest_ns = newest_ns,
    }
end
-- The commit an artifact's stamp names, or `""` when it names none: the
-- console's own `unknown` is a fact the build could not reach, not a commit,
-- and is treated exactly like a missing console.
local function stamped(artifact: Artifact): string
    if artifact.stamp == nil then
        return ""
    end
    local sha = artifact.stamp.sha or ""
    if sha == "unknown" then
        return ""
    end
    return sha
end

-- The whole rule, as one pure comparison over measured facts. Every lane
-- reaches this and nothing else decides whether an artifact may be booted.
function M.verdict(profile: Profile, tree: Tree): boolean, string
    for _, artifact in ipairs(profile.artifacts) do
        if artifact.mtime_s < 0 then
            return false,
                "the artifact is not built: " .. artifact.path .. " is not there"
        end
        if tree.head ~= "" and stamped(artifact) ~= "" then
            local stamp_sha = stamped(artifact)
            if stamp_sha ~= tree.head then
                return false,
                    "the artifact was built from commit "
                        .. stamp_sha
                        .. " and the tree is at "
                        .. tree.head
            end
        elseif tree.head == "" and stamped(artifact) ~= "" then
            return false,
                "the artifact was built from commit "
                    .. stamped(artifact)
                    .. " and this tree names no commit to compare it against"
        end
        local newer = tree.newest_s > artifact.mtime_s
            or (
                tree.newest_s == artifact.mtime_s and tree.newest_ns > artifact.mtime_ns
            )
        if newer and tree.newest_path ~= "" then
            return false,
                "the source "
                    .. tree.newest_path
                    .. " is newer than the artifact it builds"
        end
    end
    return true, ""
end

-- The refusal, verbatim: the artifact's own identity, the tree's identity,
-- the leg that failed, the two times compared, and the rebuild. Nothing is
-- left for the operator to infer.
local function refusal(profile: Profile, tree: Tree, reason: string): string
    local lines: {string} = {
        "STALE BINARY: refusing to boot " .. profile.label,
    }
    for _, artifact in ipairs(profile.artifacts) do
        lines[#lines + 1] = "  artifact  " .. artifact.path
        if artifact.stamp == nil then
            lines[#lines + 1] = "  stamp     none: this artifact carries no identity console,"
                .. " so only its age can be checked"
        else
            lines[#lines + 1] = "  stamp     sha="
                .. (artifact.stamp.sha or "unknown")
                .. " dirty="
                .. (artifact.stamp.dirty or "unknown")
                .. " feature_shape="
                .. (artifact.stamp.feature_shape or "unknown")
                .. " version="
                .. (artifact.stamp.version or "unknown")
        end
        if artifact.note ~= "" then
            lines[#lines + 1] = "            " .. artifact.note
        end
    end
    if tree.head == "" then
        lines[#lines + 1] = "  tree      no commit: this tree names no HEAD to compare against"
    else
        lines[#lines + 1] = "  tree      HEAD=" .. tree.head
    end
    lines[#lines + 1] = "  reason    " .. reason
    lines[#lines + 1] = "  newest source "
        .. (tree.newest_path == "" and "(none tracked)" or tree.newest_path)
        .. " at "
        .. moment(tree.newest_s, tree.newest_ns)
    for _, artifact in ipairs(profile.artifacts) do
        lines[#lines + 1] = "            "
            .. artifact.label
            .. " at "
            .. moment(artifact.mtime_s, artifact.mtime_ns)
    end
    lines[#lines + 1] = "  rebuild   " .. profile.rebuild
    return table.concat(lines, "\n")
end

-- The pass annotation, one line, naming the identity the artifact carries so
-- that every lane log records which artifact the measurement is about. A
-- dirty stamp is called out: it names a commit and not its content, and the
-- age leg is what stands behind it.
local function annotation(profile: Profile, tree: Tree): string
    local first = profile.artifacts[1]
    local stamp = first.stamp
    local identity = "no identity console (the age leg carries it)"
    local dirty = false
    if stamp ~= nil then
        identity = "sha="
            .. (stamp.sha or "unknown")
            .. " dirty="
            .. (stamp.dirty or "unknown")
            .. " feature_shape="
            .. (stamp.feature_shape or "unknown")
        dirty = stamp.dirty == "true"
    end
    local line = "stale-binary: "
        .. profile.label
        .. " is fresh: "
        .. identity
        .. "; newest source "
        .. (tree.newest_path == "" and "(none tracked)" or tree.newest_path)
        .. " is older than the artifact"
    if dirty then
        line = line
            .. "; the stamp names a dirty build of that commit, so the age leg"
            .. " is what certifies it"
    end
    return line
end

function M.check(root: string, profile: Profile): integer
    local tree = measure_tree(root, profile.sources)
    local fresh, reason = M.verdict(profile, tree)
    if not fresh then
        gate.fail(refusal(profile, tree, reason))
    end
    io.stderr:write(annotation(profile, tree) .. "\n")
    return 0
end

-- The CDylib the Lua service loads: `src/advisory_lock.tl` resolves it from
-- `LUNET_ADVISORY_LOCK_LIB` when that is set and from its own directory
-- otherwise, so the check resolves it the same way and asks the same file
-- the service will open.
function M.cdylib(root: string): Profile
    local path = os.getenv("LUNET_ADVISORY_LOCK_LIB")
    if path == nil or path == "" then
        path = fs.join(
            root, "ext/advisory_lock/target/release/liblunet_advisory_lock." .. soext()
        )
    end
    return {
        label = "the advisory_lock cdylib",
        rebuild = CDYLIB_REBUILD,
        artifacts = {library("the advisory_lock cdylib", path)},
        sources = CDYLIB_SOURCES,
    }
end

-- The rig's node binaries: the release node and the control client. The rig
-- never spawns the traffic generator.
function M.lease_node(root: string): Profile
    local dir = lease_dir(root)
    return {
        label = "the lease-sequencer node binaries",
        rebuild = LEASE_REBUILD,
        artifacts = {
            binary("the lease-sequencer node", fs.join(dir, "lease-sequencer")),
            binary("the lease client", fs.join(dir, "lease-client")),
        },
        sources = LEASE_SOURCES,
    }
end

-- The scenario lanes' ladder binaries: the node, the traffic generator and
-- the control client, all three booted by softball2 and polite2.
function M.lease_ladder(root: string): Profile
    local profile = M.lease_node(root)
    profile.label = "the lease-sequencer ladder binaries"
    profile.artifacts[#profile.artifacts + 1] = binary(
        "the load generator", fs.join(lease_dir(root), "lease-load")
    )
    return profile
end

-- The rig's standby stack adds one binary from one more crate: lock-feed
-- serves the standby's AOF series to the console, and no other rig entry
-- boots it, so it belongs to the standby's artifact set and to nobody's
-- else.
function M.standby_stack(root: string): Profile
    local profile = M.lease_node(root)
    profile.label = "the lease-sequencer standby stack"
    profile.rebuild = LEASE_REBUILD
        .. " && cargo build --release --manifest-path ext/lock_feed/Cargo.toml"
    profile.sources = {
        "ext/lock_feed",
        "examples/lease-sequencer",
        "ext/advisory_lock",
        "ext/lunet-locks-aof",
        ".cargo",
        "rust-toolchain.toml",
    }
    profile.artifacts[#profile.artifacts + 1] = binary(
        "the console feed", fs.join(root, "ext/lock_feed/target/release/lock-feed")
    )
    return profile
end

-- The snapshot acceptance run's two tools: the flight-tape reader, from the
-- example crate, and the admin nuke, from the adapter crate.
function M.snapshot_tools(root: string): Profile
    local dir = lease_dir(root)
    return {
        label = "the snapshot acceptance tools",
        rebuild = LEASE_REBUILD
            .. " && cargo build --release --manifest-path ext/advisory_lock/Cargo.toml",
        artifacts = {
            binary("the flight-tape reader", fs.join(dir, "skaffold_flight_tape")),
            binary(
                "the marker admin tool",
                fs.join(root, "ext/advisory_lock/target/release/lunet_locks_nuke")
            ),
        },
        sources = LEASE_SOURCES,
    }
end

-- The console's feed, at whatever path the console's own configuration
-- names (`FEED_BIN`, or the console's debug build by default). A lane whose
-- artifact path comes from its configuration rather than from a convention
-- this module owns declares its profile here, so the check stays the one
-- check.
function M.lock_feed(path: string): Profile
    return {
        label = "the console feed",
        rebuild = "cargo build --manifest-path ext/lock_feed/Cargo.toml",
        artifacts = {binary("the console feed", path)},
        sources = FEED_SOURCES,
    }
end

-- The Docker lane's artifact is the IMAGE, not a file on the host: the image
-- is what gets booted, and its creation time is the only host-side fact that
-- dates it. The identity leg cannot reach inside it either - the build
-- context is the committed tree with `.git` excluded, so the in-image build
-- of the CDylib stamps `sha=unknown` - which is exactly the case the age leg
-- exists for, and the note says so on every refusal.
local IMAGE_NOTE = "the image's cdylib is stamped sha=unknown: the build context"
    .. " is the committed tree with .git excluded, so the in-image build names"
    .. " no commit and only the age leg can certify it"

function M.image(name: string): Profile
    local out = shell.capture(
        "docker image inspect --format '{{.Id}} {{.Os}}/{{.Architecture}} {{.Created}}' "
            .. shell.quote(name)
            .. " 2>/dev/null"
    )
    local id, platform, created = out:match("^%s*(%S+)%s+(%S+)%s+(%S+)%s*$")
    local artifact: Artifact
    if id == nil then
        artifact = {
            label = "the image " .. name,
            path = name,
            stamp = nil,
            mtime_s = -1,
            mtime_ns = -1,
            note = IMAGE_NOTE,
        }
    else
        artifact = {
            label = "the image " .. name,
            path = name .. " (" .. id .. " on " .. platform .. ")",
            stamp = nil,
            mtime_s = rfc3339_seconds(created),
            mtime_ns = 0,
            note = IMAGE_NOTE,
        }
    end
    return {
        label = "the Docker image " .. name,
        rebuild = "make docker-build (docker build from the committed tree)",
        artifacts = {artifact},
        sources = CDYLIB_SOURCES,
    }
end

return M
