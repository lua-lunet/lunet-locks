-- Typed filesystem primitives: paths, file IO, directory walking. The
-- dated-log gate bug dies here structurally: gates read the logs
-- DIRECTORY via list_dir and match node-name prefixes, never a glob and
-- never an exact dated name.

local bit = require("bit")
local ffi = require("ffi")

-- struct stat and struct dirent layouts differ between macOS and Linux;
-- both are declared here in their platform's own shape, FULL layout —
-- a truncated struct lets lstat write past the cdata buffer.
if ffi.os == "OSX" then
    ffi.cdef(
        [[
      struct stat {
         unsigned int st_dev;
         unsigned short st_mode;
         unsigned short st_nlink;
         unsigned long long st_ino;
         unsigned int st_uid;
         unsigned int st_gid;
         unsigned int st_rdev;
         long long st_atime;
         long st_atime_nsec;
         long long st_mtime;
         long st_mtime_nsec;
         long long st_ctime;
         long st_ctime_nsec;
         long long st_birthtime;
         long st_birthtime_nsec;
         long long st_size;
         long long st_blocks;
         int st_blksize;
         unsigned int st_flags;
         unsigned int st_gen;
         unsigned int st_extsize;
         long long st_qspare[2];
      };
      struct dirent {
         unsigned long long d_ino;
         long long d_seekoff;
         unsigned short d_reclen;
         unsigned short d_namlen;
         unsigned char d_type;
         char d_name[256];
      };
   ]]
    )
else
    -- Linux's struct stat differs per CPU: the x86_64 ABI puts nlink
    -- before mode, the generic ABI (aarch64) puts mode before nlink. A
    -- truncated or misordered declaration makes lstat write real data
    -- into wrong offsets and every is_dir() reads garbage — the smoke's
    -- work-dir mkdir_p fails on the arm64 runners while amd64 sails.
    if ffi.arch == "x64" then
    ffi.cdef(
        [[
      struct stat {
         unsigned long st_dev;
         unsigned long st_ino;
         unsigned long st_nlink;
         unsigned int st_mode;
         unsigned int st_uid;
         unsigned int st_gid;
         int __pad0;
         unsigned long st_rdev;
         long st_size;
         long st_blksize;
         long st_blocks;
         long st_atime;
         long st_atime_nsec;
         long st_mtime;
         long st_mtime_nsec;
         long st_ctime;
         long st_ctime_nsec;
         long __unused[3];
      };
      struct dirent {
         unsigned long d_ino;
         long d_off;
         unsigned short d_reclen;
         unsigned char d_type;
         char d_name[256];
      };
   ]]
    )
    else
    ffi.cdef(
        [[
      struct stat {
         unsigned long st_dev;
         unsigned long st_ino;
         unsigned int st_mode;
         unsigned int st_nlink;
         unsigned int st_uid;
         unsigned int st_gid;
         int __pad0;
         unsigned long st_rdev;
         long st_size;
         long st_blksize;
         long st_blocks;
         long st_atime;
         long st_atime_nsec;
         long st_mtime;
         long st_mtime_nsec;
         long st_ctime;
         long st_ctime_nsec;
         long __unused[3];
      };
      struct dirent {
         unsigned long d_ino;
         long d_off;
         unsigned short d_reclen;
         unsigned char d_type;
         char d_name[256];
      };
   ]]
    )
    end
end

ffi.cdef(
    [[
   int lstat(const char *path, struct stat *buf);
   int chmod(const char *path, unsigned int mode);
   int unlink(const char *path);
   int rmdir(const char *path);
   int mkdir(const char *path, unsigned int mode);
   char *getcwd(char *buf, long size);
   typedef struct tooling_DIR tooling_DIR;
   tooling_DIR *opendir(const char *path);
   struct dirent *readdir(tooling_DIR *dir);
   int closedir(tooling_DIR *dir);
]]
)

local record Dirent
    d_name: string
end

local record Stat
    st_dev: integer
    st_mode: integer
    st_uid: integer
    st_gid: integer
    st_rdev: integer
    st_size: integer
end

local record LibC
    lstat: function(string, any): integer
    chmod: function(string, integer): integer
    unlink: function(string): integer
    rmdir: function(string): integer
    mkdir: function(string, integer): integer
    getcwd: function(any, integer): string
    opendir: function(string): any
    readdir: function(any): any
    closedir: function(any): integer
end

local C = ffi.C as LibC

local S_IFMT = 0xF000
local S_IFDIR = 0x4000
local S_IFLNK = 0xA000

local record M
    basename: function(string): string
    dirname: function(string): string
    join: function(...: string): string
    exists: function(string): boolean
    is_dir: function(string): boolean
    is_lnk: function(string): boolean
    mode_of: function(string): integer
    read_all: function(string): string, string
    write_all: function(string, string): boolean
    append_all: function(string, string): boolean
    mkdir_p: function(string): boolean
    copy_file: function(string, string): boolean
    copy_tree: function(string, string): boolean
    list_dir: function(string): {string}, string
    resolve: function(string): string
    remove_tree: function(string): boolean
    walk: function(string): {string}
    unique_sibling: function(string, string): string
end

function M.basename(path: string): string
    return path:match("[^/]+$") or path
end

function M.dirname(path: string): string
    local d = path:match("^(.*)/[^/]*$")
    if not d or d == "" then
        return "."
    end
    return d
end

function M.join(...: string): string
    local parts = {...}
    local out = ""
    for _, part in ipairs(parts) do
        if part == "" then
            -- skip
        elseif out == "" then
            out = part
        elseif out:sub(-1) == "/" then
            out = out .. part
        else
            out = out .. "/" .. part
        end
    end
    return out
end

local stat = ffi.new("struct stat[1]") as {Stat}

local function lstat_ok(path: string): boolean
    return C.lstat(path, stat) == 0
end

function M.exists(path: string): boolean
    return lstat_ok(path)
end

function M.is_dir(path: string): boolean
    if not lstat_ok(path) then
        return false
    end
    return bit.band(stat[0].st_mode, S_IFMT) == S_IFDIR
end

function M.is_lnk(path: string): boolean
    if not lstat_ok(path) then
        return false
    end
    return bit.band(stat[0].st_mode, S_IFMT) == S_IFLNK
end

function M.mode_of(path: string): integer
    if not lstat_ok(path) then
        return 0
    end
    return bit.band(stat[0].st_mode, 0xFFF)
end

-- Reads a whole file. A failure returns "" and the error string.
function M.read_all(path: string): string, string
    local f, err = io.open(path, "rb")
    if not f then
        return "", tostring(err)
    end
    local data = f:read("*a") or ""
    f:close()
    return data, ""
end

function M.write_all(path: string, data: string): boolean
    local f = io.open(path, "wb")
    if not f then
        return false
    end
    f:write(data)
    f:close()
    return true
end

function M.append_all(path: string, data: string): boolean
    local f = io.open(path, "ab")
    if not f then
        return false
    end
    f:write(data)
    f:close()
    return true
end

function M.mkdir_p(path: string): boolean
    local parts: {string} = {}
    for part in path:gmatch("[^/]+") do
        parts[#parts + 1] = part
    end
    local prefix = path:sub(1, 1) == "/" and "/" or ""
    for _, part in ipairs(parts) do
        prefix = prefix .. part .. "/"
        if not M.is_dir(prefix) then
            if C.mkdir(prefix, tonumber("755", 8)) ~= 0 and not M.is_dir(prefix) then
                return false
            end
        end
    end
    return true
end

-- Copies a regular file, preserving its mode.
function M.copy_file(src: string, dst: string): boolean
    local data, err = M.read_all(src)
    if data == "" and err ~= "" then
        return false
    end
    local f = io.open(dst, "wb")
    if not f then
        return false
    end
    f:write(data)
    f:close()
    C.chmod(dst, M.mode_of(src))
    return true
end

-- Copies a regular file or a whole directory tree (cp -pR). The
-- destination directory is created.
function M.copy_tree(src: string, dst: string): boolean
    if M.is_lnk(src) then
        return true
    end
    if not M.is_dir(src) then
        return M.copy_file(src, dst)
    end
    if not M.is_dir(dst) then
        C.mkdir(dst, M.mode_of(src))
    end
    local entries, _e = M.list_dir(src)
    for _, name in ipairs(entries) do
        if not M.copy_tree(M.join(src, name), M.join(dst, name)) then
            return false
        end
    end
    return true
end

-- Directory entries, sorted, without "." and "..". An unreadable
-- directory yields an empty list and the error string.
function M.list_dir(path: string): {string}, string
    local dir = C.opendir(path)
    if dir == nil then
        return {}, "cannot list " .. path
    end
    local out: {string} = {}
    while true do
        local raw = C.readdir(dir)
        if raw == nil then
            break
        end
        local ent = raw as Dirent
        local name = ffi.string(ent.d_name)
        if name ~= "." and name ~= ".." then
            out[#out + 1] = name
        end
    end
    C.closedir(dir)
    table.sort(out)
    return out, ""
end

-- Absolute, lexically normalized path (no symlink resolution).
function M.resolve(path: string): string
    if path:sub(1, 1) ~= "/" then
        local cwd = ffi.new("char[?]", 4096)
        local got = C.getcwd(cwd, 4096)
        path = (got ~= nil and ffi.string(got) or ".") .. "/" .. path
    end
    local out: {string} = {}
    for part in path:gmatch("[^/]+") do
        if part == "." then
            -- skip
        elseif part == ".." then
            out[#out] = nil
        else
            out[#out + 1] = part
        end
    end
    return "/" .. table.concat(out, "/")
end

-- Removes a directory tree (rm -rf in typed Lua). Refuses the root: a
-- wipe removes a run dir, never "/".
function M.remove_tree(path: string): boolean
    local abs = M.resolve(path)
    if abs == "/" then
        return false
    end
    if M.is_lnk(path) then
        return C.unlink(path) == 0
    end
    if M.is_dir(path) then
        local entries, _err = M.list_dir(path)
        for _, name in ipairs(entries) do
            if not M.remove_tree(M.join(path, name)) then
                return false
            end
        end
        return C.rmdir(path) == 0
    end
    if M.exists(path) then
        return C.unlink(path) == 0
    end
    return true
end

-- Every file path under a root, relative to the root, sorted. Symlinks
-- are listed but never descended into.
function M.walk(root_dir: string): {string}
    local out: {string} = {}
    local function visit(dir: string)
        local entries, _e = M.list_dir(dir)
        for _, name in ipairs(entries) do
            local full = M.join(dir, name)
            local rel = full:sub(#root_dir + 2)
            if M.is_dir(full) and not M.is_lnk(full) then
                visit(full)
            else
                out[#out + 1] = rel
            end
        end
    end
    if M.is_dir(root_dir) then
        visit(root_dir)
    end
    table.sort(out)
    return out
end

-- A unique staged sibling path next to `parent`: `prefix.NNNNN` with the
-- counter walked until the path is free (mktemp without a subprocess).
function M.unique_sibling(parent: string, prefix: string): string
    local n = 0
    while true do
        local candidate = M.join(parent, prefix .. "." .. string.format("%05d", n))
        if not M.exists(candidate) then
            return candidate
        end
        n = n + 1
    end
end

return M
