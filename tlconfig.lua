-- Teal project config. Fields and semantics: see Cyan's docs
-- https://github.com/teal-language/cyan/blob/main/docs/tlconfig.md
return {
   source_dir = "src",
   build_dir = "build",

   -- source_dir is on the search path already. Harness modules loaded by
   -- scripts/ live outside it, so they need an explicit entry. Add "vendor"
   -- here too if/when plain-Lua modules are vendored.
   include_dir = { "scripts/lib", "tests" },

   -- lunet embeds LuaJIT (Lua 5.1). Keep generated code 5.1-clean and do
   -- not emit compat53 shims, since the runtime does not ship compat53.
   gen_target = "5.1",
   gen_compat = "off",
}
