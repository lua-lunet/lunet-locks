use std::path::{Path, PathBuf};

fn manifest_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

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

#[test]
fn a_descriptor_line_missing_its_leading_brace_is_refused() {
    let broken = r#""id":65537,"name":"dc1-node1","host":"127.0.0.1","port":41101,"genesis":true}"#;
    let result: Result<serde_json::Value, _> = serde_json::from_str(broken);
    assert!(result.is_err(), "the 65a4ad5 defect shape must not parse");
}
