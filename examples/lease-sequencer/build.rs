//! Two compile-time duties, no runtime behavior change:
//!
//! 1. The runtime lookup for the vendored TigerBeetle AOF cdylib: the
//!    zig cdylib's install name is `@rpath/liblunet_locks_aof.dylib`, so
//!    the binary needs an LC_RPATH (macOS) / RUNPATH (Linux) at the
//!    crate's artifact directory, which the `links` contract propagates
//!    as the `DEP_LUNET_LOCKS_AOF_METADATA` entries (one `lib_dir=<path>`
//!    per the AOF crate's build script).
//! 2. The Flight Recorder reader's commit facts: the extraction path
//!    (`skaffold_flight_tape`) refuses a recording whose commit the
//!    reading code is not — a recording is readable ONLY by code as-at
//!    its commit — so every build of this tree carries the hash it would
//!    check recordings against.

use std::process::Command;

fn main() {
    if let Ok(metadata) = std::env::var("DEP_LUNET_LOCKS_AOF_METADATA")
        && let Some(lib_dir) = metadata
            .split_whitespace()
            .find_map(|entry| entry.strip_prefix("lib_dir=").map(|path| path.to_string()))
    {
        println!("cargo:rustc-link-arg-bins=-Wl,-rpath,{lib_dir}");
    }

    let commit = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .expect("the reader names its commit; this tree is a git checkout")
            .stdout,
    )
    .expect("utf-8 commit hash")
    .trim()
    .to_string();
    println!("cargo:rustc-env=FLIGHT_COMMIT={commit}");
    let dirty = !Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .expect("git status")
        .stdout
        .is_empty();
    println!("cargo:rustc-env=FLIGHT_DIRTY={}", u8::from(dirty));
}
