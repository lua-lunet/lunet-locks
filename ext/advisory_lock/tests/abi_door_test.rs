//! The locked door, PROVEN by the built library's symbol table rather
//! than asserted in a comment.
//!
//! The Compliance ABI's nine `lunet_lock_node_unsafe_*` exports are
//! behind the `compatibility_suite` cargo feature. This gate builds both
//! feature shapes, reads each cdylib's symbol table with the platform's
//! `nm`, and fails if
//!
//! - the DEFAULT (production) shape exports ANY `unsafe_` symbol — the
//!   feature must compile them out, not refuse them at runtime; or
//! - the `compatibility_suite` shape is missing any of the nine; or
//! - either shape is missing `lunet_lock_version_properties`, the
//!   read-only console every production build carries
//!   (docs/src/compliance-abi.md).
//!
//! Both shapes build into one target directory and each artifact is
//! copied aside as it is produced, so the gate pays for one dependency
//! build rather than two.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::process::Command;

/// The nine Compliance ABI exports. The list is the ABI's whole
/// surface; a tenth `unsafe_` symbol appearing anywhere fails the
/// default-shape check below without being named here.
const COMPLIANCE_EXPORTS: [&str; 9] = [
    "lunet_lock_node_unsafe_open_compliance",
    "lunet_lock_node_unsafe_set_compliance_clock",
    "lunet_lock_node_unsafe_propose_opaque",
    "lunet_lock_node_unsafe_reconfigure_opaque",
    "lunet_lock_node_unsafe_frontiers",
    "lunet_lock_node_unsafe_journal_entries",
    "lunet_lock_node_unsafe_membership",
    "lunet_lock_node_unsafe_witnesses",
    "lunet_lock_node_unsafe_marker_log",
];

/// The read-only console's single export. Present in EVERY shape: it is
/// the whole of what a production build adds, and it takes no node.
const CONSOLE_EXPORT: &str = "lunet_lock_version_properties";

/// The marker of a write-capable compliance symbol.
const UNSAFE_MARKER: &str = "unsafe_";

#[test]
fn the_compliance_abi_is_absent_from_the_production_build() {
    let built = build_both_shapes();

    let leaked: Vec<&String> = built
        .default_symbols
        .iter()
        .filter(|symbol| symbol.contains(UNSAFE_MARKER))
        .collect();
    assert!(
        leaked.is_empty(),
        "the default (production) build exports {} `{UNSAFE_MARKER}` symbol(s): \
         {leaked:?} — the compatibility_suite feature must compile the Compliance \
         ABI out, not refuse it at runtime (docs/src/compliance-abi.md). Every \
         exported symbol: {:?}",
        leaked.len(),
        built.default_symbols
    );
}

#[test]
fn the_compatibility_suite_build_carries_all_nine_exports() {
    let built = build_both_shapes();

    let missing: Vec<&str> = COMPLIANCE_EXPORTS
        .iter()
        .copied()
        .filter(|export| !built.suite_symbols.contains(&normalize(export)))
        .collect();
    assert!(
        missing.is_empty(),
        "the compatibility_suite build is missing {} of the nine Compliance \
         exports: {missing:?}; it exported: {:?}",
        missing.len(),
        built.suite_symbols
    );
}

#[test]
fn every_shape_carries_the_read_only_console() {
    let built = build_both_shapes();

    for (shape, symbols) in [
        ("default", &built.default_symbols),
        ("compatibility_suite", &built.suite_symbols),
    ] {
        assert!(
            symbols.contains(&normalize(CONSOLE_EXPORT)),
            "the {shape} build does not export `{CONSOLE_EXPORT}`; it exported: \
             {symbols:?}"
        );
    }
}

// ----------------------------------------------------------------------
// The build and the symbol table
// ----------------------------------------------------------------------

struct Built {
    default_symbols: BTreeSet<String>,
    suite_symbols: BTreeSet<String>,
}

/// Build both feature shapes once per test binary and read both symbol
/// tables. The builds are the expensive part, so the whole set is built
/// on the first call and the tables are read from there after.
fn build_both_shapes() -> &'static Built {
    use std::sync::OnceLock;
    static BUILT: OnceLock<Built> = OnceLock::new();
    BUILT.get_or_init(|| {
        let target = gate_target_dir();
        let default_library = target.join(shared_library_stem("default"));
        let suite_library = target.join(shared_library_stem("suite"));

        build(&target, &[]);
        let produced = produced_library(&target);
        copy_aside(&produced, &default_library);

        build(&target, &["compatibility_suite"]);
        let produced = produced_library(&target);
        copy_aside(&produced, &suite_library);

        Built {
            default_symbols: exported_symbols(&default_library),
            suite_symbols: exported_symbols(&suite_library),
        }
    })
}

/// This crate's own target directory, with the gate's subdirectory
/// beside the ordinary build output. Under `target/`, so it is gitignored
/// with the rest of the crate's build products.
fn gate_target_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join("abi-door")
}

/// `cargo build --lib` for one feature shape.
fn build(target: &Path, features: &[&str]) {
    let mut command = Command::new(cargo_program());
    command
        .arg("build")
        .arg("--lib")
        .arg("--manifest-path")
        .arg(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
        .env("CARGO_TARGET_DIR", target)
        // The overrides the Makefile's suite lane carries: a development
        // tree is dirty by construction, and the build script's
        // clean-commit guards would otherwise refuse the very shape this
        // gate must read.
        .env("COMPATIBILITY_SUITE_ALLOW_DIRTY", "1");
    if !features.is_empty() {
        command.arg("--features").arg(features.join(","));
    }
    let output = command
        .output()
        .unwrap_or_else(|error| panic!("the gate could not run cargo: {error}"));
    assert!(
        output.status.success(),
        "the {features:?} build failed, so its symbol table proves nothing:\n{}",
        String::from_utf8_lossy(&output.stderr)
    );
}

/// The cdylib `cargo` just produced in this target directory.
fn produced_library(target: &Path) -> PathBuf {
    let debug = target.join("debug");
    let entries = std::fs::read_dir(&debug)
        .unwrap_or_else(|error| panic!("the gate could not read {}: {error}", debug.display()));
    let mut found: Vec<PathBuf> = entries
        .map(|entry| entry.expect("the build directory entry reads").path())
        .filter(|path| {
            let name = path
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default();
            name.starts_with("liblunet_advisory_lock.")
                && matches!(
                    path.extension().and_then(|ext| ext.to_str()),
                    Some("dylib" | "so")
                )
        })
        .collect();
    found.sort();
    assert_eq!(
        found.len(),
        1,
        "the gate found {} cdylibs in {}, expected exactly one: {found:?}",
        found.len(),
        debug.display()
    );
    found.pop().expect("one cdylib")
}

/// The aside name for a copied library: the same extension, a distinct
/// stem, so the two shapes can never be confused for one another.
fn shared_library_stem(shape: &str) -> String {
    let extension = if cfg!(target_os = "macos") {
        "dylib"
    } else {
        "so"
    };
    format!("abi-door-{shape}.{extension}")
}

fn copy_aside(produced: &Path, aside: &Path) {
    std::fs::copy(produced, aside).unwrap_or_else(|error| {
        panic!(
            "the gate could not copy {} to {}: {error}",
            produced.display(),
            aside.display()
        )
    });
}

/// The names `nm` reports as globally defined text symbols, with the
/// platform's own decoration removed (a Mach-O leading underscore).
fn exported_symbols(library: &Path) -> BTreeSet<String> {
    let output = Command::new("nm")
        .arg(library)
        .output()
        .unwrap_or_else(|error| panic!("the gate could not run nm: {error}"));
    assert!(
        output.status.success(),
        "nm failed on {}:\n{}",
        library.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    let table = String::from_utf8_lossy(&output.stdout);
    let mut symbols = BTreeSet::new();
    for line in table.lines() {
        let fields: Vec<&str> = line.split_whitespace().collect();
        // "<addr> <type> <name>" for a defined symbol; "<type> <name>"
        // for an undefined one, which is never this library's export.
        let (kind, name) = match fields.as_slice() {
            [.., kind, name] if fields.len() >= 3 => (*kind, *name),
            [.., name] => ("U", *name),
            _ => continue,
        };
        if matches!(kind, "T" | "W") {
            symbols.insert(normalize(name));
        }
    }
    assert!(
        !symbols.is_empty(),
        "nm reported no defined text symbols for {} — the gate would pass on \
         an unread table",
        library.display()
    );
    symbols
}

/// Strip the platform's decoration from an export name: Mach-O prefixes
/// every C symbol with an underscore, ELF does not.
fn normalize(name: &str) -> String {
    name.strip_prefix('_').unwrap_or(name).to_string()
}

fn cargo_program() -> std::ffi::OsString {
    std::env::var_os("CARGO").unwrap_or_else(|| "cargo".into())
}
