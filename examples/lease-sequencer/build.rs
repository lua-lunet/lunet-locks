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

    let (commit, dirty) = head_state();
    println!("cargo:rustc-env=FLIGHT_COMMIT={commit}");
    println!("cargo:rustc-env=FLIGHT_DIRTY={}", u8::from(dirty));
}

/// The HEAD hash and whether the working tree carries uncommitted
/// changes.
///
/// Unavailable git (no checkout in the build environment) falls back to
/// the `"unknown"`/dirty shape instead of failing: the clean-commit
/// gate belongs to the adapter crate's build script (see
/// `ext/advisory_lock/build.rs`), which owns the feature flag this
/// reader annotates.
///
/// The Docker build context is the committed-tree snapshot with `.git`
/// excluded, so git is unavailable in-image. The fastbuild/release gate
/// asserts the clean-commit state before the build and hands the commit
/// it ships through `LUNET_LOCKS_HEAD`; that value wins over git when
/// set (and the gate is the authority — a change might be in flight in
/// the checking-out tree, which is exactly what the gate refuses).
fn head_state() -> (String, bool) {
    if let Some(commit) = std::env::var_os("LUNET_LOCKS_HEAD").and_then(|value| {
        let value = value
            .into_string()
            .expect("utf-8 commit hash")
            .trim()
            .to_string();
        if value.is_empty() { None } else { Some(value) }
    }) {
        return (commit, false);
    }

    let commit = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .map(|output| String::from_utf8(output.stdout).expect("utf-8 commit hash").trim().to_string())
        .expect("the flight reader names its commit; a build with git unavailable is a Docker context, which carries LUNET_LOCKS_HEAD");
    let dirty = !Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .map(|output| {
            !String::from_utf8(output.stdout)
                .expect("utf-8 git status")
                .is_empty()
        })
        .expect("git status");
    (commit, dirty)
}
