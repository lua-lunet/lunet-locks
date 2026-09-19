-- Typed process management: spawn with file redirects, liveness, TERM
-- stop with a KILL backstop, waitpid exit-code decoding. Every spawned
-- process is a Process record; the runners track them all and terminate
-- them on exit — nothing the tooling starts ever dangles.

local bit = require("bit")
local ffi = require("ffi")
local clock = require("tools.lib.clock")

ffi.cdef(
    [[
   typedef int pid_t;
   pid_t fork(void);
   int execvp(const char *file, char *const argv[]);
   void _exit(int status);
   int dup2(int oldfd, int newfd);
   int open(const char *path, int flags);
   int close(int fd);
   pid_t waitpid(pid_t pid, int *status, int options);
   int kill(pid_t pid, int sig);
   int setenv(const char *name, const char *value, int overwrite);
]]
)

local O_WRONLY = 0x0001
local O_TRUNC = 0x0400
local SIGTERM = 15
local SIGKILL = 9
local SIGSTOP = 17
local SIGCONT = 19
local WNOHANG = 1

local enum ProcState
    "starting"
    "running"
    "exited"
    "signalled"
    "failed"
end

local record LibC
    fork: function(): integer
    execvp: function(any, any): integer
    _exit: function(integer)
    dup2: function(integer, integer): integer
    open: function(string, integer): integer
    close: function(integer): integer
    waitpid: function(integer, any, integer): integer
    kill: function(integer, integer): integer
    setenv: function(string, string, integer): integer
end

local C = ffi.C as LibC

local record Process
    pid: integer
    name: string
    argv: {string}
    state: ProcState
    exit_code: integer
    signal: integer
    started_ms: integer
end

local record SpawnOpts
    out_path: string
    err_path: string
    combine: boolean
    env: {string: string}
end

local record M
    spawn: function({string}, string, SpawnOpts): Process
    alive: function(Process): boolean
    poll: function(Process): boolean
    wait: function(Process): integer
    kill: function(Process): boolean
    suspend: function(Process): boolean
    resume: function(Process): boolean
    stop: function(Process, integer): integer
    stop_all: function()
    tracked: {Process}
    untrack: function(Process)
    Process: Process
    SpawnOpts: SpawnOpts
    ProcState: ProcState
end

M.tracked = {}

function M.untrack(p: Process)
    for i, q in ipairs(M.tracked) do
        if q == p then
            table.remove(M.tracked, i)
            return
        end
    end
end

-- Fork/exec with redirects. The child opens nothing new: the parent
-- pre-creates the redirect files, the child only dup2's and execs — an
-- exec failure is a hard _exit(127), never a silent fallthrough.
-- The parent pre-creates every redirect file with plain io.open (the
-- umask's own mode; the LuaJIT arm64 FFI loses a variadic open's mode
-- argument), so the child opens O_WRONLY|O_TRUNC only — no O_CREAT, no
-- variadic mode argument anywhere.
function M.spawn(argv: {string}, name: string, opts: SpawnOpts): Process
    for _, path in ipairs({opts.out_path, opts.err_path}) do
        local f = io.open(path, "wb")
        if not f then
            error("cannot create redirect file " .. path)
        end
        f:close()
    end
    local out_fd = C.open(opts.out_path, bit.bor(O_WRONLY, O_TRUNC))
    local err_fd = opts.combine and out_fd
        or C.open(opts.err_path, bit.bor(O_WRONLY, O_TRUNC))

    -- The child's argv lands in cdata before fork: no Lua work happens
    -- between fork and exec.
    local argv_c: any = ffi.new("char*[?]", #argv + 1)
    local argv_c_arr = argv_c as {any}
    for i, v in ipairs(argv) do
        argv_c_arr[i - 1] = ffi.cast("char *", v)
    end

    local pid = C.fork()
    if pid < 0 then
        local p: Process = {
            pid = -1,
            name = name,
            argv = argv,
            state = "failed",
            exit_code = -1,
            signal = 0,
            started_ms = clock.now_ms(),
        }
        return p
    end

    if pid == 0 then
        for k, v in pairs(opts.env or {}) do
            C.setenv(k, v, 1)
        end
        C.dup2(out_fd, 1)
        C.dup2(err_fd, 2)
        if out_fd > 2 then
            C.close(out_fd)
        end
        if err_fd > 2 and err_fd ~= out_fd then
            C.close(err_fd)
        end
        C.execvp(argv_c_arr[0], argv_c)
        C._exit(127)
    end

    if out_fd > 2 then
        C.close(out_fd)
    end
    if err_fd > 2 and err_fd ~= out_fd then
        C.close(err_fd)
    end

    local p: Process = {
        pid = pid,
        name = name,
        argv = argv,
        state = "running",
        exit_code = 0,
        signal = 0,
        started_ms = clock.now_ms(),
    }
    M.tracked[#M.tracked + 1] = p
    return p
end

function M.alive(p: Process): boolean
    if p.state ~= "running" then
        return false
    end
    if C.kill(p.pid, 0) ~= 0 then
        return false
    end
    return true
end

-- Decodes a waitpid status into the record. LuaJIT is 5.1: a low byte
-- of zero means the process exited, otherwise it names the signal.
local function decode(p: Process, status: integer)
    if bit.band(status, 0x7f) == 0 then
        p.state = "exited"
        p.exit_code = bit.rshift(status, 8)
        p.signal = 0
    else
        p.state = "signalled"
        p.exit_code = 128 + bit.band(status, 0x7f)
        p.signal = bit.band(status, 0x7f)
    end
    M.untrack(p)
end

-- Non-blocking reap: true when the process has been reaped.
function M.poll(p: Process): boolean
    if p.state ~= "running" then
        return true
    end
    local status = ffi.new("int[1]") as {integer}
    local got = C.waitpid(p.pid, status, WNOHANG)
    if got == p.pid then
        decode(p, status[0])
        return true
    end
    return false
end

-- Blocking reap; returns the decoded exit code (128+signal for signals).
function M.wait(p: Process): integer
    if p.state ~= "running" then
        return p.exit_code
    end
    local status = ffi.new("int[1]") as {integer}
    C.waitpid(p.pid, status, 0)
    decode(p, status[0])
    return p.exit_code
end

function M.kill(p: Process): boolean
    return C.kill(p.pid, SIGKILL) == 0
end

-- The partition injection on a loopback rig: SIGSTOP freezes the member
-- whole (no sends, no receives, in-memory and durable state intact — the
-- partition-heal shape, no reboot, no identity bump); SIGCONT is the
-- heal and its backlog burst is the delayed-delivery data.
function M.suspend(p: Process): boolean
    return C.kill(p.pid, SIGSTOP) == 0
end

function M.resume(p: Process): boolean
    return C.kill(p.pid, SIGCONT) == 0
end

-- The polite stop: TERM, wait `grace_ms` for the exit, KILL only if the
-- process is still there, then reap. Never kills what it does not own —
-- every pid in a Process record came from our own fork.
function M.stop(p: Process, grace_ms: integer): integer
    if p.state ~= "running" then
        return p.exit_code
    end
    if C.kill(p.pid, SIGTERM) ~= 0 then
        -- Already gone; reap whatever is pending.
        if M.poll(p) then
            return p.exit_code
        end
    end
    local deadline = clock.mono_ms() + grace_ms
    while clock.mono_ms() < deadline do
        if M.poll(p) then
            return p.exit_code
        end
        clock.sleep(0.05)
    end
    if not M.poll(p) then
        C.kill(p.pid, SIGKILL)
        M.wait(p)
    end
    return p.exit_code
end

-- Reaps every tracked process through the polite stop: the runners'
-- unconditional cleanup on exit.
function M.stop_all()
    for _, p in ipairs(M.tracked) do
        M.stop(p, 1000)
    end
end

return M
