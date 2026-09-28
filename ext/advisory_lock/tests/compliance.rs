//! The compliance suite's runner over OUR host: every case in the
//! vendored corpus (ext/uvrr-core/tests/compliance/corpus, the pinned
//! submodule tag), replayed through the abstract host interface — the
//! adapter's `Node` over the real marker store — and asserted against
//! the named expectations (`ext/uvrr-core/docs/uvrr-host-compliance.md`).

#[path = "compliance/mod.rs"]
mod compliance;

use compliance::{Case, assert_expectation, corpus_dir, run_case};
use std::fs;

/// The families the runner drives, smallest first (the ratchet: a family
/// enters this list when every case in it is green over our host).
const FAMILIES: &[&str] = &[
    "agreement",
    "reincarnation-safety",
    "identity",
    "fuse",
    "casting-vote",
    "reconfiguration",
    "view-selection",
    "witness",
    "boot-gate",
    "prepare-accept",
];

/// The corpus's case files, read from the submodule.
fn corpus() -> Vec<(String, Vec<Case>)> {
    let mut files: Vec<_> = fs::read_dir(corpus_dir())
        .expect("the corpus directory is present")
        .map(|entry| entry.expect("the corpus directory reads"))
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|e| e == "json"))
        .collect();
    files.sort();
    files
        .into_iter()
        .map(|path| {
            let text = fs::read_to_string(&path).expect("the corpus file reads");
            let name = path
                .file_stem()
                .and_then(|n| n.to_str())
                .expect("a corpus file is named")
                .to_string();
            let cases: Vec<Case> = serde_json::from_str(&text).expect("the corpus file parses");
            (name, cases)
        })
        .collect()
}

#[test]
fn the_corpus_passes() {
    let corpus = corpus();
    let families: Vec<_> = corpus
        .into_iter()
        .filter(|(name, _)| FAMILIES.contains(&name.as_str()))
        .collect();
    assert!(
        families.len() == FAMILIES.len(),
        "every named family has a corpus file: {FAMILIES:?}"
    );
    for (family, cases) in &families {
        assert!(!cases.is_empty(), "{family} has cases");
        for case in cases {
            assert_eq!(
                case.family, *family,
                "{}: the case names its family file",
                case.id
            );
            let captured = run_case(case).unwrap_or_else(|e| {
                panic!("{}: {}: the case refused to run: {e}", case.id, case.clause)
            });
            if let Err(mismatch) = assert_expectation(case, &captured) {
                panic!(
                    "{}: {}: {mismatch}\ncaptured: {:#?}",
                    case.id, case.clause, captured
                );
            }
        }
    }
}
