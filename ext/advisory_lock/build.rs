//! The build script stamps the Flight Recorder's commit facts and holds
//! the clean-commit guard.
//!
//! Every build carries `FLIGHT_COMMIT` (the HEAD hash) and `FLIGHT_DIRTY`
//! so a flight recording can name the exact code state it was recorded
//! by, and so the reader path can refuse a recording whose commit the
//! reading code is not.
//!
//! The clean-commit rule is enforced ONLY where it is owed: a
//! `flight-recorder` build (the feature flag set) MUST come from a clean
//! commit — a dirty tree fails the build with the escape hatch
//! `FLIGHT_RECORDER_ALLOW_DIRTY=1` (which still stamps the recording
//! `dirty: true` so every reader annotates it loudly). An ordinary prod
//! build (flag OFF) is never gated: dev trees stay buildable.

use std::process::Command;

fn main() {
    let flight_feature = std::env::var("CARGO_FEATURE_FLIGHT_RECORDER")
        .map(|value| value == "1")
        .unwrap_or(false);
    let (commit, dirty) = match head_state() {
        Some((commit, dirty)) => (commit, dirty),
        None => {
            if flight_feature {
                panic!(
                    "flight-recorder build refuses to proceed: the git commit hash \
                     could not be read, so the recording could not name its code \
                     state (a flight-recording build must come from a clean commit)"
                );
            }
            ("unknown".to_string(), true)
        }
    };
    println!("cargo:rustc-env=FLIGHT_COMMIT={commit}");
    println!("cargo:rustc-env=FLIGHT_DIRTY={}", u8::from(dirty));

    if flight_feature && dirty {
        if std::env::var_os("FLIGHT_RECORDER_ALLOW_DIRTY").is_none() {
            panic!(
                "flight-recorder build refuses a dirty tree: a flight recording \
                 is readable only by the code as-at its commit, and a dirty tree \
                 has no such commit. Commit (or stash with a todo to restore) \
                 first, or set FLIGHT_RECORDER_ALLOW_DIRTY=1 to override — the \
                 recording is then stamped dirty and every reader annotates it \
                 loudly."
            );
        }
        println!(
            "cargo:warning=flight-recorder build is from a DIRTY tree ({commit}); \
             the recording carries dirty=true and reads only through the \
             override-annotated path"
        );
    }
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
    let commit = String::from_utf8(
        Command::new("git")
            .args(["rev-parse", "HEAD"])
            .output()
            .ok()?
            .stdout,
    )
    .ok()?
    .trim()
    .to_string();
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
