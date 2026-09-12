//! The runtime lookup for the vendored TigerBeetle AOF cdylib: the zig
//! cdylib's install name is `@rpath/liblunet_locks_aof.dylib`, so the
//! binary needs an LC_RPATH (macOS) / RUNPATH (Linux) at the crate's
//! artifact directory, which the `links` contract propagates as the
//! `DEP_LUNET_LOCKS_AOF_METADATA` entries (one `lib_dir=<path>` per the
//! AOF crate's build script).

fn main() {
    if let Ok(metadata) = std::env::var("DEP_LUNET_LOCKS_AOF_METADATA")
        && let Some(lib_dir) = metadata
            .split_whitespace()
            .find_map(|entry| entry.strip_prefix("lib_dir=").map(|path| path.to_string()))
    {
        println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{lib_dir}");
    }
}
