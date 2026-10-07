//! The build script stamps the build's identity facts and holds the
//! clean-commit guard.
//!
//! Every build carries `FLIGHT_COMMIT` (the HEAD hash) and `FLIGHT_DIRTY`
//! so a flight recording can name the exact code state it was recorded
//! by, and so the reader path can refuse a recording whose commit the
//! reading code is not.
//!
//! Every build ALSO carries the read-only console's facts
//! (`LUNET_INFO_*`): the release tag this build is, the commit, the
//! dirty flag, and the build's feature shape. They are stamped from the
//! same git probe, and a fact the probe cannot reach is stamped
//! `unknown` rather than guessed (`src/info.rs`).
//!
//! The `flight-recorder` feature holds a clean-commit guard: a
//! recording is readable only by the code as-at its commit — a
//! recording produced from an uncommitted tree names no code state at
//! all. A guard that fires is overridden by the feature's own
//! `FLIGHT_RECORDER_ALLOW_DIRTY=1`, which still stamps the build
//! `dirty: true` so every reader annotates it loudly.
//! An ordinary prod build (both features OFF) is never gated: dev trees
//! stay buildable.

use std::process::Command;

fn main() {
    let flight_feature = feature_on("FLIGHT_RECORDER");
    let (commit, dirty) = match head_state() {
        Some((commit, dirty)) => (commit, dirty),
        None => {
            for (on, feature) in [(flight_feature, "flight-recorder")] {
                if on {
                    panic!(
                        "{feature} build refuses to proceed: the git commit hash \
                         could not be read, so the build could not name its code \
                         state (a {feature} build must come from a clean commit)"
                    );
                }
            }
            ("unknown".to_string(), true)
        }
    };
    println!("cargo:rustc-env=FLIGHT_COMMIT={commit}");
    println!("cargo:rustc-env=FLIGHT_DIRTY={}", u8::from(dirty));

    let (version, sha) = version_facts(&commit);
    println!("cargo:rustc-env=LUNET_INFO_VERSION={version}");
    println!("cargo:rustc-env=LUNET_INFO_SHA={sha}");
    println!("cargo:rustc-env=LUNET_INFO_DIRTY={dirty}");
    println!(
        "cargo:rustc-env=LUNET_INFO_FEATURES={}",
        feature_shape(flight_feature)
    );

    guard_clean_commit(
        "FLIGHT_RECORDER_ALLOW_DIRTY",
        "flight-recorder",
        flight_feature,
        &commit,
        dirty,
    );
}

/// Whether the named cargo feature is on for this build.
fn feature_on(name: &str) -> bool {
    std::env::var(format!("CARGO_FEATURE_{name}"))
        .map(|value| value == "1")
        .unwrap_or(false)
}

/// The build's feature shape: `production`, then `+<feature>` for each
/// development feature it carries.
fn feature_shape(flight: bool) -> String {
    let mut shape = String::from("production");
    for (on, name) in [(flight, "flight-recorder")] {
        if on {
            shape.push('+');
            shape.push_str(name);
        }
    }
    shape
}

/// The release tag this build IS, and the short commit. A build whose
/// HEAD is not exactly at a tag carries no version: the nearest tag is
/// not this build, and reporting it would be a lie the `/info` console
/// could not defend. With no git facts at all (the Docker context, a
/// tarball) both are `unknown`.
fn version_facts(commit: &str) -> (String, String) {
    if commit == "unknown" {
        return (String::from("unknown"), String::from("unknown"));
    }
    let sha: String = commit.chars().take(12).collect();
    match git(&["describe", "--tags", "--exact-match"]) {
        Some(tag) if !tag.is_empty() => (tag, sha),
        _ => (String::from("unknown"), sha),
    }
}

/// The clean-commit guard, one per guarded feature. Off trees are never
/// gated.
fn guard_clean_commit(override_var: &str, feature: &str, on: bool, commit: &str, dirty: bool) {
    if !on || !dirty {
        return;
    }
    if std::env::var_os(override_var).is_none() {
        panic!(
            "{feature} build refuses a dirty tree: a {feature} artefact is \
             evidence about a commit, and a dirty tree has no such commit. \
             Commit (or stash with a todo to restore) first, or set \
             {override_var}=1 to override — the build is then stamped \
             dirty and every reader annotates it loudly."
        );
    }
    println!(
        "cargo:warning={feature} build is from a DIRTY tree ({commit}); the \
         build is stamped dirty and reads only through the override-annotated \
         path"
    );
}

/// One git query, trimmed. `None` when git is unavailable, this is not a
/// checkout, or the query fails.
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    String::from_utf8(output.stdout)
        .ok()
        .map(|text| text.trim().to_string())
}

/// The HEAD hash and whether the working tree carries uncommitted changes.
/// `None` when git is unavailable or this is not a git checkout.
///
/// The Docker build context is the committed-tree snapshot with `.git`
/// excluded, so git is unavailable in-image. The fastbuild/release gate
/// asserts the clean-commit state before the build and hands the commit
/// it ships through `LUNET_LOCKS_HEAD`; that value wins over git when
/// set (and the context-in-a-checkout git probe never sees dirt while a
/// change might be in flight — the gate is the authority, not a
/// potentially-dirty working tree).
fn head_state() -> Option<(String, bool)> {
    if let Some(commit) = std::env::var_os("LUNET_LOCKS_HEAD").and_then(|value| {
        let value = value
            .into_string()
            .expect("utf-8 commit hash")
            .trim()
            .to_string();
        if value.is_empty() { None } else { Some(value) }
    }) {
        return Some((commit, false));
    }
    let commit = git(&["rev-parse", "HEAD"])?;
    if commit.is_empty() {
        return None;
    }
    let status = Command::new("git")
        .args(["status", "--porcelain"])
        .output()
        .ok()?;
    let dirty = !status.stdout.is_empty();
    Some((commit, dirty))
}
