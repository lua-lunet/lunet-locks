//! The bench oracle: the CAS-chain evidence checks
//! (`docs/src/bench-harness.md`).
//!
//! Every bench node journals every committed lock transition it applies
//! (the LKE1 event journal, `ext/advisory_lock/src/journal.rs`): three
//! replicas applying one replicated log must produce one chain. The
//! oracle decodes each node's journal, compares the chains on their
//! semantic content (the apply timestamp is each node's own), checks
//! every lease a client observed appears in the chain in order, and
//! scans the node logs for the adapter's maybe-invariant voice. Any
//! violation is a run failure; there is no tolerance and no repair.

use std::fs;
use std::path::{Path, PathBuf};

/// The record kinds (the journal's `KIND_*`).
pub const KIND_HOLD: u8 = 1;
pub const KIND_RENEW: u8 = 2;
pub const KIND_RELEASE: u8 = 3;
pub const KIND_BREAK: u8 = 4;

const RECORD_MAGIC: &[u8; 4] = b"LKE1";
const RECORD_SIZE: usize = 61;
const RECORD_PAYLOAD_LEN: u32 = 53;

/// One decoded journal record's semantic content. The apply timestamp
/// is each node's own wall clock and is never compared.
#[derive(Debug, Clone)]
pub struct Transition {
    pub kind: u8,
    pub lock_id: u64,
    pub lease_id: u64,
    pub holder: [u8; 16],
    pub expiry: u64,
}

impl Transition {
    /// The deterministic comparison: the transition kind, the lock, the
    /// lease id, and the holder are the replicated content. The expiry
    /// is stamped from the executing node's own clock at apply
    /// (`locks.rs`'s `execution_time`) and the apply timestamp likewise;
    /// neither is consensus content, so neither is compared.
    fn same_content(&self, other: &Transition) -> bool {
        self.kind == other.kind
            && self.lock_id == other.lock_id
            && self.lease_id == other.lease_id
            && self.holder == other.holder
    }

    fn describe(&self) -> String {
        format!(
            "kind={} lock={} lease_id={} holder={} expiry={}",
            self.kind,
            self.lock_id,
            self.lease_id,
            self.holder
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>(),
            self.expiry
        )
    }
}

/// The CRC-32 (IEEE, reflected, polynomial 0xEDB88320) the journal
/// stamps over bytes `kind..=expiry`; the table is computed, matching
/// the writer's (`journal.rs`).
fn crc32_ieee(data: &[u8]) -> u32 {
    let mut table = [0u32; 256];
    for (index, slot) in table.iter_mut().enumerate() {
        let mut crc = index as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                0xEDB8_8320 ^ (crc >> 1)
            } else {
                crc >> 1
            };
        }
        *slot = crc;
    }
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc = (crc >> 8) ^ table[((crc ^ byte as u32) & 0xFF) as usize];
    }
    crc ^ 0xFFFF_FFFF
}

/// Decode one journal file's records. A truncated, malformed, or
/// crc-divergent record is an oracle failure: the chain is the
/// evidence, and broken evidence proves nothing.
pub fn decode_file(path: &Path) -> Result<Vec<Transition>, String> {
    let bytes = fs::read(path)
        .map_err(|error| format!("the journal file {} does not read: {error}", path.display()))?;
    if bytes.len() % RECORD_SIZE != 0 {
        return Err(format!(
            "the journal file {} has a trailing partial record ({} bytes)",
            path.display(),
            bytes.len()
        ));
    }
    let mut out = Vec::with_capacity(bytes.len() / RECORD_SIZE);
    for (index, record) in bytes.chunks_exact(RECORD_SIZE).enumerate() {
        if record[0..4] != RECORD_MAGIC[..] {
            return Err(format!(
                "the journal file {} record {index} has a bad magic",
                path.display()
            ));
        }
        if u32::from_be_bytes(record[4..8].try_into().expect("4 bytes")) != RECORD_PAYLOAD_LEN {
            return Err(format!(
                "the journal file {} record {index} has a bad payload length",
                path.display()
            ));
        }
        let stored_crc = u32::from_be_bytes(record[57..61].try_into().expect("4 bytes"));
        if crc32_ieee(&record[8..57]) != stored_crc {
            return Err(format!(
                "the journal file {} record {index} fails its crc",
                path.display()
            ));
        }
        out.push(Transition {
            kind: record[8],
            lock_id: u64::from_be_bytes(record[17..25].try_into().expect("8 bytes")),
            lease_id: u64::from_be_bytes(record[25..33].try_into().expect("8 bytes")),
            holder: record[33..49].try_into().expect("16 bytes"),
            expiry: u64::from_be_bytes(record[49..57].try_into().expect("8 bytes")),
        });
    }
    Ok(out)
}

/// The journal files of one node's directory in chain order: rolled
/// files by their first op, the open file (at most one) last.
fn journal_files(dir: &Path) -> Result<Vec<PathBuf>, String> {
    let mut rolled: Vec<(u64, PathBuf)> = Vec::new();
    let mut open: Vec<(u64, PathBuf)> = Vec::new();
    let entries = fs::read_dir(dir)
        .map_err(|error| format!("the journal dir {} does not read: {error}", dir.display()))?;
    for entry in entries {
        let entry = entry.map_err(|error| format!("the journal dir entry: {error}"))?;
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if let Some(rest) = name.strip_prefix("ev-open-") {
            let ts = rest
                .strip_suffix(".bin")
                .and_then(|stem| stem.parse::<u64>().ok())
                .unwrap_or(0);
            open.push((ts, entry.path()));
        } else if let Some(rest) = name.strip_prefix("ev-") {
            if name.ends_with(".meta") {
                continue;
            }
            let op_min = rest
                .split('-')
                .next()
                .and_then(|stem| stem.parse::<u64>().ok())
                .unwrap_or(0);
            rolled.push((op_min, entry.path()));
        }
    }
    rolled.sort_by_key(|(op_min, _)| *op_min);
    open.sort_by_key(|(ts, _)| *ts);
    Ok(rolled
        .into_iter()
        .chain(open)
        .map(|(_, path)| path)
        .collect())
}

/// One node's CAS chain: every journal file decoded in order.
pub fn node_chain(dir: &Path) -> Result<Vec<Transition>, String> {
    let mut chain = Vec::new();
    for file in journal_files(dir)? {
        chain.extend(decode_file(&file)?);
    }
    Ok(chain)
}

/// Check 1 — chain equality: every node's transition sequence is
/// identical. The error names the first divergence.
pub fn chains_agree(chains: &[(&str, Vec<Transition>)]) -> Result<(), String> {
    let Some((reference_name, reference)) = chains.first() else {
        return Err("the oracle was given no chains".to_string());
    };
    for (name, chain) in &chains[1..] {
        if chain.len() != reference.len() {
            return Err(format!(
                "chain length diverges: {reference_name} has {}, {name} has {}",
                reference.len(),
                chain.len()
            ));
        }
        for (index, (a, b)) in reference.iter().zip(chain.iter()).enumerate() {
            if !a.same_content(b) {
                return Err(format!(
                    "chains diverge at transition {index}: {reference_name} has {}, {name} has {}",
                    a.describe(),
                    b.describe()
                ));
            }
        }
    }
    Ok(())
}

/// Check 2 — client consistency: every lease a client observed through
/// one node's service (holder, lease_id) appears in the chain, in
/// observation order. A client seeing a state the chain does not
/// contain, or states out of the chain's order, is a divergence.
pub fn client_history_consistent(
    chain: &[Transition],
    history: &[([u8; 16], u64)],
) -> Result<(), String> {
    let mut cursor = 0;
    for (observation, (holder, lease_id)) in history.iter().enumerate() {
        let found = chain[cursor..]
            .iter()
            .position(|transition| {
                (transition.kind == KIND_HOLD || transition.kind == KIND_RENEW)
                    && transition.holder == *holder
                    && transition.lease_id == *lease_id
            })
            .map(|position| cursor + position);
        match found {
            Some(index) => cursor = index,
            None => {
                return Err(format!(
                    "client observation {observation} (lease_id {lease_id}) appears in the \
                     chain at no point at or after the previous observation"
                ));
            }
        }
    }
    Ok(())
}

/// Check 3 — the maybe-invariant voice: the adapter's `maybe_invariant!`
/// warns (release) or panics (debug) on a probationary-state violation.
/// Any occurrence in any node log fails the run.
pub fn logs_without_maybe_violations(logs: &[(&str, String)]) -> Result<(), String> {
    for (name, text) in logs {
        for (line, content) in text.lines().enumerate() {
            if content.contains("maybe-invariant violation") {
                return Err(format!(
                    "{name} log line {} carries a maybe-invariant violation: {content}",
                    line + 1
                ));
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn transition(kind: u8, lease_id: u64, holder_byte: u8) -> Transition {
        Transition {
            kind,
            lock_id: 0x0DDBA12,
            lease_id,
            holder: [holder_byte; 16],
            expiry: 10_000,
        }
    }

    #[test]
    fn the_crc_matches_the_ieee_reference_vector() {
        // The standard CRC-32/IEEE check value for "123456789".
        assert_eq!(crc32_ieee(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn identical_chains_agree() {
        let a = vec![transition(KIND_HOLD, 1, 1), transition(KIND_RENEW, 1, 1)];
        let b = a.clone();
        assert!(chains_agree(&[("n1", a), ("n2", b)]).is_ok());
    }

    #[test]
    fn a_divergent_transition_is_named() {
        let a = vec![transition(KIND_HOLD, 1, 1), transition(KIND_RENEW, 1, 1)];
        let mut b = a.clone();
        b[1] = transition(KIND_RENEW, 2, 2);
        let error = chains_agree(&[("n1", a), ("n2", b)]).expect_err("divergence");
        assert!(error.contains("transition 1"), "{error}");
    }

    #[test]
    fn a_length_divergence_is_named() {
        let a = vec![transition(KIND_HOLD, 1, 1)];
        let b = vec![transition(KIND_HOLD, 1, 1), transition(KIND_RENEW, 1, 1)];
        let error = chains_agree(&[("n1", a), ("n2", b)]).expect_err("divergence");
        assert!(error.contains("length"), "{error}");
    }

    #[test]
    fn an_observed_lease_must_be_in_the_chain() {
        let chain = vec![transition(KIND_HOLD, 1, 1), transition(KIND_HOLD, 2, 2)];
        let good = [([1u8; 16], 1u64), ([2u8; 16], 2u64)];
        assert!(client_history_consistent(&chain, &good).is_ok());
        let bad = [([9u8; 16], 9u64)];
        assert!(client_history_consistent(&chain, &bad).is_err());
        // Out of order: observation 2 precedes observation 1 in the chain.
        let reordered = [([2u8; 16], 2u64), ([1u8; 16], 1u64)];
        assert!(client_history_consistent(&chain, &reordered).is_err());
    }

    #[test]
    fn the_maybe_voice_fails_the_run() {
        let clean = [("n1", "ordinary line".to_string())];
        assert!(logs_without_maybe_violations(&clean).is_ok());
        let dirty = [(
            "n2",
            "WARN maybe-invariant violation: holder ghost".to_string(),
        )];
        let error = logs_without_maybe_violations(&dirty).expect_err("violation");
        assert!(error.contains("n2"), "{error}");
    }
}
