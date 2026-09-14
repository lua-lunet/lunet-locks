-- tl-driver.lua — the tracked CLI driver for tools/aof-trace-tool.tl.
-- `cyan build`/`tl gen` of the .tl alone is NOT a CLI: the generated
-- module only runs under the Teal loader, so piping a gen'd file to
-- luajit silently no-ops and costs operator time. This driver installs
-- the loader, requires the tool, and forwards the script args to its
-- main — the same argv contract `python3 tools/aof-trace-tool.py` has.
--
-- Run from the repo root (tl comes from the project-local .rocks tree):
--   LUA_PATH="./?.tl;./?.lua;.rocks/share/lua/5.1/?.lua;;" \
--     luajit -l tl tools/tl-driver.lua [--lib PATH] [--kind KIND ...]
--       [--anchor EPOCHMS ...] [--hb-ms N] FILE_OR_DIR [OUT.jsonl]
require("tl").loader()
local tool = require("tools.aof-trace-tool")
local argv = { "prog" }
for _, v in ipairs(arg) do
    argv[#argv + 1] = v
end
os.exit(tool.main(argv))
