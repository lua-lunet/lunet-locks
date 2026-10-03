-- The upstream compliance corpus, run over the transport, as a gate.
--
-- The corpus is upstream data at its pinned tag (`ext/uvrr-core`), and
-- the `hurl` suite is its own driver: this module boots the Lua
-- compliance host, waits for its health probe, drives the suite against
-- it, and stops the host. Nothing here reads a case, compares an
-- expectation, or knows a verdict — that is the host's work over the
-- Compliance ABI, and the gate's only question is whether the suite
-- passes.
--
-- `contract.hurl` runs first: it covers the endpoints that replay no
-- case, so a transport fault names itself before any case is reached.
-- The ten `corpus-*.hurl` files follow in name order.
--
-- The host is a `compatibility_suite` build of the native library and is
-- never booted in production (`docs/src/compliance-abi.md`); the library
-- path is handed in so the caller names the shape.

local ffi = require("ffi")

ffi.cdef(
    [[
    int getpid(void);
]]
)

local record LibC
    getpid: function(): integer
end

local C = ffi.C as LibC

local client = require("tools.lib.client")
local clock = require("tools.lib.clock")
local fs = require("tools.lib.fs")
local proc = require("tools.lib.proc")
local shell = require("tools.lib.shell")

local record Options
    root: string
    shape: string
    lunet_run: string
    library_override: string
    library: string
    production_library: string
    port: integer
    suite: string
    run_dir: string
    log_dir: string
    ready_timeout_ms: integer
end

-- The Compliance ABI's nine exports. A tenth `unsafe_` symbol anywhere
-- in the production shape fails the production check below without being
-- named here.
local NINE_EXPORTS: {string} = {
    "lunet_lock_node_unsafe_open_compliance",
    "lunet_lock_node_unsafe_set_compliance_clock",
    "lunet_lock_node_unsafe_propose_opaque",
    "lunet_lock_node_unsafe_reconfigure_opaque",
    "lunet_lock_node_unsafe_frontiers",
    "lunet_lock_node_unsafe_journal_entries",
    "lunet_lock_node_unsafe_membership",
    "lunet_lock_node_unsafe_witnesses",
    "lunet_lock_node_unsafe_marker_log",
}

local record M
    run: function(Options): integer
    check_production_shape: function(Options): integer
    check_suite_shape: function(Options): boolean
    defaults: function(string): Options
    main: function(string, {string}): integer
end

-- The suite's own file list: the transport contract first, then the
-- corpus families. The names are the upstream suite's; nothing here
-- interprets a file's contents.
local SUITE_FILES: {string} = {
    "contract.hurl",
    "corpus-agreement.hurl",
    "corpus-boot-gate.hurl",
    "corpus-casting-vote.hurl",
    "corpus-fuse.hurl",
    "corpus-identity.hurl",
    "corpus-prepare-accept.hurl",
    "corpus-reconfiguration.hurl",
    "corpus-reincarnation-safety.hurl",
    "corpus-view-selection.hurl",
    "corpus-witness.hurl",
}

local function health(options: Options): string
    local body, failure = client.http_get(options.port, "/health", "")
    if failure ~= "" then
        return ""
    end
    return body
end

-- The host's health probe, polled until it answers or the boot window
-- closes. A host that died is reported against its own log, never as a
-- silent timeout.
local function await_host(options: Options, host: proc.Process): boolean, string
    local waited = 0
    local step = 50
    while waited < options.ready_timeout_ms do
        if not proc.alive(host) then
            return false, "the compliance host exited before its health probe answered"
        end
        if health(options) ~= "" then
            return true, nil
        end
        clock.sleep(step / 1000)
        waited = waited + step
    end
    return false,
        "the compliance host did not answer its health probe inside "
            .. tostring(options.ready_timeout_ms)
            .. "ms"
end

-- The pinned project-local Lunet runtime, the same one every other
-- service lane uses: a host `lunet-run` on PATH is never substituted.
local function pinned_runtime(root: string): string
    return root .. "/.lunet/v0.10.0/lunet-run"
end

-- The `compatibility_suite` library, built into its own target
-- directory so the production shape under `target/release` is never
-- replaced by the gated one.
local function suite_library(root: string): string
    local suffix = (ffi.os == "OSX") and "dylib" or "so"
    return root
        .. "/ext/advisory_lock/target/compliance/debug/liblunet_advisory_lock."
        .. suffix
end

local function parse_options(argv: {string}, root: string): Options
    local options = M.defaults(root)
    local index = 1
    while index <= #argv do
        local flag = argv[index]
        local value = argv[index + 1]
        assert(value ~= nil, "compliance_gate: missing value for " .. flag)
        if flag == "--shape" then
            options.shape = value
        elseif flag == "--port" then
            options.port = tonumber(value) as integer
        elseif flag == "--run-dir" then
            options.run_dir = value
            options.log_dir = value
        elseif flag == "--library" then
            options.library_override = value
        else
            error("compliance_gate: no such flag " .. flag, 0)
        end
        index = index + 2
    end
    return options
end

-- The production library this tree ships: the default feature shape,
-- built by `make ext` into the crate's own target directory.
local function production_library(root: string): string
    local suffix = (ffi.os == "OSX") and "dylib" or "so"
    return root .. "/ext/advisory_lock/target/release/liblunet_advisory_lock." .. suffix
end

-- The exported text symbols the platform's `nm` reports, with the
-- platform's own decoration removed (Mach-O prefixes every C symbol with
-- an underscore).
local function exported_symbols(library: string): {string}
    local table_text = shell.capture("nm -g " .. shell.quote(library) .. " 2>/dev/null")
    local symbols: {string} = {}
    for line in table_text:gmatch("[^\n]+") do
        local kind, name = line:match("%s(%a)%s+(_?[%w_]+)$")
        if kind ~= nil and (kind == "T" or kind == "W") then
            table.insert(symbols, (name:gsub("^_", "")))
        end
    end
    return symbols
end

local function contains(items: {string}, name: string): boolean
    for _, item in ipairs(items) do
        if item == name then
            return true
        end
    end
    return false
end

-- The locked door, re-proved from the gate: the production shape exports
-- no `unsafe_` symbol at all. The feature compiles them out, so this is
-- an absence in the symbol table and never a runtime refusal.
function M.check_production_shape(options: Options): integer
    local symbols = exported_symbols(options.production_library)
    if #symbols == 0 then
        io.stderr:write(
            "ERROR: nm reported no exported text symbol for the production library "
                .. options.production_library
                .. "; the door check would pass on an unread table\n"
        )
        return 1
    end
    local leaked: {string} = {}
    for _, symbol in ipairs(symbols) do
        if string.find(symbol, "unsafe_", 1, true) ~= nil then
            table.insert(leaked, symbol)
        end
    end
    if #leaked > 0 then
        io.stderr:write(
            "ERROR: the production build exports "
                .. tostring(#leaked)
                .. " `unsafe_` symbol(s): "
                .. table.concat(leaked, ", ")
                .. " — the compatibility_suite feature must compile the Compliance ABI out\n"
        )
        return 1
    end
    io.stdout:write(
        "compliance_gate: the production library exports "
            .. tostring(#symbols)
            .. " symbols and ZERO `unsafe_` ones: the locked door is shut\n"
    )
    return 0
end

-- The gated shape must carry all nine: the host binds them by name, and a
-- missing one is a refusal at boot rather than a silent gap.
function M.check_suite_shape(options: Options): boolean
    local symbols = exported_symbols(options.library)
    local missing: {string} = {}
    for _, export in ipairs(NINE_EXPORTS) do
        if not contains(symbols, export) then
            table.insert(missing, export)
        end
    end
    if #missing > 0 then
        io.stderr:write(
            "ERROR: the compatibility_suite library is missing "
                .. tostring(#missing)
                .. " of the nine Compliance exports: "
                .. table.concat(missing, ", ")
                .. "\n"
        )
        return false
    end
    io.stdout:write(
        "compliance_gate: the compatibility_suite library exports all nine `unsafe_` Compliance ABI entry points\n"
    )
    return true
end

function M.defaults(root: string): Options
    return {
        root = root,
        shape = "compatibility_suite",
        lunet_run = "",
        library_override = "",
        library = "",
        production_library = "",
        port = 8099,
        suite = root .. "/ext/uvrr-core/tests/hurl",
        run_dir = root .. "/.tmp/compliance",
        log_dir = root .. "/.tmp/compliance",
        ready_timeout_ms = 30000,
    }
end

-- This run's own directory under the scratch tree. A case's marker
-- store carries its boot classification durably, so a case must never
-- open one a previous run left behind: the process id names the run, and
-- the case number inside it names the case. The reference executor uses
-- the same pair.
local function run_directory(base: string): string
    return base .. "/run-" .. tostring(C.getpid())
end

function M.run(options: Options): integer
    -- The host's marker-store directory and this gate's logs share one
    -- tree: the scratch space the case marker files live in, which the
    -- same tooling already creates for every other run.
    options.run_dir = run_directory(options.run_dir)
    options.log_dir = options.run_dir
    assert(fs.mkdir_p(options.run_dir), "compliance_gate: the run directory creates")
    local host = proc.spawn(
        {
            options.lunet_run,
            options.root .. "/build/compliance_server.lua",
            "--host",
            "127.0.0.1",
            "--port",
            tostring(options.port),
            "--root",
            options.run_dir,
        },
        "compliance-host",
        {
            out_path = options.log_dir .. "/compliance-host.log",
            err_path = options.log_dir .. "/compliance-host.log",
            combine = true,
            env = {LUNET_ADVISORY_LOCK_LIB = options.library},
        }
    )
    local ready, refusal = await_host(options, host)
    if not ready then
        proc.stop(host, 5000)
        io.stderr:write(
            "ERROR: "
                .. (refusal as string)
                .. "; see "
                .. options.log_dir
                .. "/compliance-host.log\n"
        )
        return 1
    end
    local argv: {string} = {
        "hurl",
        "--test",
        "--variable",
        "base_url=http://127.0.0.1:" .. tostring(options.port),
    }
    for _, name in ipairs(SUITE_FILES) do
        table.insert(argv, options.suite .. "/" .. name)
    end
    local suite = proc.spawn(
        argv,
        "hurl-suite",
        {
            out_path = options.log_dir .. "/compliance-suite.log",
            err_path = options.log_dir .. "/compliance-suite.log",
            combine = true,
            env = {},
        }
    )
    local status = proc.wait(suite)
    proc.stop(host, 5000)
    if status ~= 0 then
        io.stderr:write(
            "ERROR: the compliance suite failed (exit "
                .. tostring(status)
                .. "); see "
                .. options.log_dir
                .. "/compliance-suite.log and "
                .. options.log_dir
                .. "/compliance-host.log\n"
        )
        return status
    end
    return 0
end

function M.main(root: string, argv: {string}): integer
    local options = parse_options(argv, root)
    options.lunet_run = pinned_runtime(root)
    options.production_library = production_library(root)
    options.library = options.library_override ~= "" and options.library_override
        or suite_library(root)
    if options.shape == "production" then
        return M.check_production_shape(options)
    end
    assert(
        options.shape == "compatibility_suite",
        "compliance_gate: no such shape " .. options.shape
    )
    if not M.check_suite_shape(options) then
        return 1
    end
    return M.run(options)
end

return M
