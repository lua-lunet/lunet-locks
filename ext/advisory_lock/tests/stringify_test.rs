//! The operator's law on the human surfaces: every state a log, a
//! status report, or a trace prints is spelled with its name — never a
//! bare integer a human would have to memorise or look up. The numeric
//! words stay the comparison surface (the name is for humans only), and
//! every name comes from the const tables stated next to the numbering
//! they name.

use lunet_advisory_lock::journal;
use lunet_advisory_lock::{Node, OK, output_kind_name, replication_state_name};

/// The replication state words name themselves: the core's snapshot
/// numbering, unchanged, with the name stated beside it — and a word no
/// status encodes renders `invalid`, never a bare integer.
#[test]
fn replication_state_words_name_themselves() {
    assert_eq!(replication_state_name(0), "normal");
    assert_eq!(replication_state_name(1), "view_change");
    assert_eq!(replication_state_name(2), "restarting");
    assert_eq!(replication_state_name(3), "replaying");
    assert_eq!(replication_state_name(4), "joining");
    assert_eq!(replication_state_name(5), "invalid");
    assert_eq!(replication_state_name(u32::MAX), "invalid");
}

/// The queued output kinds name themselves.
#[test]
fn output_kinds_name_themselves() {
    assert_eq!(output_kind_name(1), "send");
    assert_eq!(output_kind_name(2), "reply");
    assert_eq!(output_kind_name(3), "unknown");
}

/// The journal kinds name themselves.
#[test]
fn journal_kinds_name_themselves() {
    assert_eq!(journal::kind_name(journal::KIND_HOLD), "hold");
    assert_eq!(journal::kind_name(journal::KIND_RENEW), "renew");
    assert_eq!(journal::kind_name(journal::KIND_RELEASE), "release");
    assert_eq!(journal::kind_name(journal::KIND_BREAK), "break");
    assert_eq!(journal::kind_name(0), "unknown");
}

/// A booted node's status carries the name with the word: the human
/// surface spells the state, the comparison surface stays the numeric
/// word, and the two can never disagree — the name is derived from the
/// word.
#[test]
fn the_booted_status_spells_its_state_name() {
    let dir = std::env::temp_dir().join(format!(
        "lunet-stringify-status-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut node = Node::open(
        &["655361:n1", "720897:n2", "786433:n3"].join("\0"),
        "n1",
        dir.join("state").to_str().unwrap(),
        None,
        0,
    )
    .expect("the node boots");
    assert_eq!(node.idle(), OK);
    let status = node.status();
    assert_eq!(status.state_name(), replication_state_name(status.state));
    assert_ne!(status.state_name(), "invalid");
    drop(node);
    let _ = std::fs::remove_dir_all(&dir);
}
