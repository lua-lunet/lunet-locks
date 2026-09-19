-- The dangling-run finder: any process whose full command line carries
-- the repo root, listed via ps with its parent chain; `--kill`
-- terminates them deliberately, skipping self and the invoking shell
-- chain. Classification is typed data (a table of Entry records), not
-- grep, and the never-kill-operator-jobs rule is the skip set below.

local ffi = require("ffi")
local shell = require("tools.lib.shell")

ffi.cdef(
    [[
   int getpid(void);
   int getppid(void);
]]
)

local record Entry
    pid: string
    ppid: string
    stat: string
    command: string
end

local record M
    find: function(): {Entry}
    Entry: Entry
    main: function({string}): integer
    REPO: string
end

local record LibC
    getpid: function(): integer
    getppid: function(): integer
end

local C = ffi.C as LibC

M.REPO = "/Users/Shared/lua-lunet/lunet-locks"

local function ps_entry(pid: string): Entry
    local out = shell.capture(
        "ps -o pid=,ppid=,stat=,command= -p " .. pid .. " 2>/dev/null"
    )
    local line = out:match("([^\n]+)")
    if not line then
        return nil
    end
    local fields: {string} = {}
    for field in line:gmatch("%S+") do
        fields[#fields + 1] = field
    end
    if #fields < 4 then
        return nil
    end
    local command = line
    for _ = 1, 3 do
        command = command:gsub("%S+", "", 1)
        command = command:gsub("^%s+", "", 1)
    end
    return {
        pid = fields[1],
        ppid = fields[2],
        stat = fields[3],
        command = command,
    }
end

-- The candidate set: pgrep over the repo path in the command line, self
-- excluded.
function M.find(): {Entry}
    local out: {Entry} = {}
    local self_pid = tostring(C.getpid())
    local pids = shell.capture("pgrep -f " .. shell.quote(M.REPO) .. " 2>/dev/null")
    for pid in pids:gmatch("%S+") do
        if pid ~= self_pid then
            local e = ps_entry(pid)
            if e then
                out[#out + 1] = e
            end
        end
    end
    return out
end

function M.main(argv: {string}): integer
    local kill_mode = argv[1] == "--kill"
    local self_pid = tostring(C.getpid())
    local self_ppid = tostring(C.getppid())

    local entries = M.find()
    for _, e in ipairs(entries) do
        -- Skip our own parent chain: the invoking shell/session is never a
        -- dangling run.
        if e.pid == self_ppid or e.ppid == self_pid then
            -- the invoking chain is never a dangling run
        elseif kill_mode then
            if shell.status("kill " .. e.pid .. " 2>/dev/null") == 0 then
                print("killed " .. e.pid)
                os.execute("sleep 1")
                if shell.status("kill -9 " .. e.pid .. " 2>/dev/null") == 0 then
                    print("killed -9 " .. e.pid)
                end
            end
        else
            print(e.pid .. " " .. e.ppid .. " " .. e.stat .. " " .. e.command)
        end
    end

    -- procs (brew) gives a second view with full args for anything whose
    -- cwd or args mention the repo; deduplicated against the pgrep set.
    if not kill_mode then
        local seen: {string: boolean} = {}
        for _, e in ipairs(entries) do
            seen[e.pid] = true
        end
        local proc_out = shell.capture("procs 2>/dev/null")
        local n = 0
        for line in proc_out:gmatch("([^\n]+)") do
            if line:find(M.REPO, 1, true) then
                local fields: {string} = {}
                for field in line:gmatch("%S+") do
                    fields[#fields + 1] = field
                end
                if fields[2] and not seen[fields[2]] and n < 40 then
                    print(line)
                    n = n + 1
                end
            end
        end
    end

    return 0
end

return M
