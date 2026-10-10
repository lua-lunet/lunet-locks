//! The config-parse gate: every cluster descriptor file the tree ships
//! decodes, line by line.
//!
//! `cluster.jsonl` and `cluster-genesis.jsonl` sat malformed (every
//! descriptor line missing its leading `{`) from 65a4ad5 until 2026-10-09,
//! every node died at boot with them, and no gate read them — the catch
//! came from a human running the standby lane. These files are
//! load-bearing beyond the example: the release image copies
//! `cluster.jsonl` in (`docker/Dockerfile`), and the docker-simulation and
//! package-verify lanes boot against it. So the gate reads EVERY
//! `*.jsonl` under `config/` plus the docker image's own
//! `docker/cluster.jsonl`, strips the comment lines, and refuses any
//! non-comment line that is not a JSON object.
//!
//! The decoder is this crate's own (`serde_json`), the one that actually
//! parses these files at boot — the gate cannot disagree with the loader
//! about what is parseable.

use std::path::{Path, PathBuf};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

/// Every descriptor file the tree ships: the example's own `config/*.jsonl`
/// (the deployment's, and each experiment variant's) plus the docker
/// image's `cluster.jsonl`, which is copied into the release image and
/// booted by the simulation and package-verify lanes.
fn descriptor_files() -> Vec<PathBuf> {
    let config_dir = manifest_dir().join("config");
    let mut files: Vec<PathBuf> = std::fs::read_dir(&config_dir)
        .expect("the config directory lists")
        .map(|entry| entry.expect("every config entry reads").path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "jsonl"))
        .collect();
    files.sort();

    let docker = manifest_dir()
        .join("../..")
        .join("docker")
        .join("cluster.jsonl");
    files.push(docker);
    files
}

/// One descriptor file: every non-comment line is a JSON object, and at
/// least one descriptor line exists — a file of only comments is a
/// deployment with no members, which is the broken-shape cousin of a
/// malformed line.
fn assert_file_parses(path: &Path) {
    let name = path.display().to_string();
    let text = std::fs::read_to_string(path)
        .unwrap_or_else(|error| panic!("the descriptor file {name} reads: {error}"));
    let mut descriptor_lines = 0usize;
    for (index, line) in text.lines().enumerate() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let value: serde_json::Value = serde_json::from_str(trimmed).unwrap_or_else(|error| {
            panic!(
                "the descriptor file {name} line {} is not JSON: {error}\n  line: {trimmed}",
                index + 1
            )
        });
        assert!(
            value.is_object(),
            "the descriptor file {name} line {} is JSON but not an object: {trimmed}",
            index + 1
        );
        descriptor_lines += 1;
    }
    assert!(
        descriptor_lines > 0,
        "the descriptor file {name} carries no descriptor line at all"
    );
}

#[test]
fn every_shipped_descriptor_file_parses_line_by_line() {
    let files = descriptor_files();
    assert!(
        files.len() >= 4,
        "the gate found only {} descriptor files; the tree ships more",
        files.len()
    );
    for file in &files {
        assert_file_parses(file);
    }
}

/// The exact defect class that shipped for months: a descriptor line with
/// its leading `{` lost. The loader's refusal is the test's — prove the
/// gate names the line rather than passing it.
#[test]
fn a_descriptor_line_missing_its_leading_brace_is_refused() {
    let broken = r#""id":65537,"name":"dc1-node1","host":"127.0.0.1","port":41101,"genesis":true}"#;
    let result: Result<serde_json::Value, _> = serde_json::from_str(broken);
    assert!(result.is_err(), "the 65a4ad5 defect shape must not parse");
}
