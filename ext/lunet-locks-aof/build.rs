//! Build script: compiles the vendored TigerBeetle AOF (Zig, the pinned
//! 0.14.1 toolchain wired through the repo's mise setup) into a cdylib and
//! links the Rust wrapper against it.
//!
//! Zig resolution order:
//! 1. `LUNET_LOCKS_AOF_ZIG` — an explicit zig binary path;
//! 2. `mise which zig` (the project's pinned toolchain, see mise.toml);
//! 3. plain `zig` from PATH (a mise-activated shell provides it).
//!
//! Incremental: `cargo:rerun-if-changed` covers the whole zig/ tree and the
//! build script itself; zig's own cache handles source-level increments.
use std::path::PathBuf;
use std::process::Command;

fn main() {
    let zig_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("zig");
    let out_dir = PathBuf::from(std::env::var("OUT_DIR").expect("OUT_DIR"));

    let zig = locate_zig();
    let optimize = if std::env::var("PROFILE").as_deref() == Ok("release") {
        "-Doptimize=ReleaseSafe"
    } else {
        "-Doptimize=Debug"
    };

    let status = Command::new(&zig)
        .current_dir(&zig_dir)
        .args([
            "build",
            optimize,
            "-Dtarget=native",
            "--prefix",
            out_dir.to_str().unwrap(),
        ])
        .status()
        .unwrap_or_else(|err| panic!("aof build: cannot run zig toolchain: {err}"));
    assert!(status.success(), "aof build: zig build failed");

    let lib_dir = out_dir.join("lib");

    println!("cargo:rustc-link-search=native={}", lib_dir.display());
    println!("cargo:rustc-link-lib=dylib=lunet_locks_aof");

    // The cdylib's install name is `@rpath/liblunet_locks_aof.dylib` (zig's
    // default), so every runtime consumer — the wrapper's own tests, the
    // lease-sequencer binary — needs the rpath pointing at the artifact.
    // `rustc-link-arg-tests` requires a test target; the package's tests are
    // integration tests, declared alongside this crate's manifests.
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_dir.display());

    // Downstream binaries (e.g. the lease-sequencer example) read this
    // metadata through the `links = "lunet_locks_aof"` contract as
    // DEP_LUNET_LOCKS_AOF_LIB_DIR and emit their own runtime rpath.
    println!("cargo:metadata=lib_dir={}", lib_dir.display());

    println!("cargo:rerun-if-changed={}", zig_dir.display());
    println!(
        "cargo:rerun-if-changed={}",
        zig_dir.join("build.zig").display()
    );
}

/// Resolve the zig binary: explicit override, then the mise-pinned
/// toolchain, then PATH.
fn locate_zig() -> PathBuf {
    if let Ok(path) = std::env::var("LUNET_LOCKS_AOF_ZIG") {
        let path = PathBuf::from(path);
        assert!(
            path.exists(),
            "LUNET_LOCKS_AOF_ZIG does not exist: {path:?}"
        );
        return path;
    }

    if let Ok(output) = Command::new("mise").arg("which").arg("zig").output() {
        if output.status.success() {
            let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if !path.is_empty() {
                return PathBuf::from(path);
            }
        }
    }

    PathBuf::from("zig")
}
