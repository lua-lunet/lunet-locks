use std::fs;
use std::path::{Path, PathBuf};

pub const KIND_HOLD: u8 = 1;
pub const KIND_RENEW: u8 = 2;
pub const KIND_RELEASE: u8 = 3;
pub const KIND_BREAK: u8 = 4;

const RECORD_MAGIC: &[u8; 4] = b"LKE1";
const RECORD_SIZE: usize = 61;
const RECORD_PAYLOAD_LEN: u32 = 53;

#[derive(Debug, Clone)]
pub struct Transition {
    pub kind: u8,
    pub lock_id: u64,
    pub lease_id: u64,
    pub holder: [u8; 16],
    pub expiry: u64,
}

impl Transition {
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

pub fn node_chain(dir: &Path) -> Result<Vec<Transition>, String> {
    let mut chain = Vec::new();
    for file in journal_files(dir)? {
        chain.extend(decode_file(&file)?);
    }
    Ok(chain)
}

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

pub fn logs_without_invariant_violations(logs: &[(&str, String)]) -> Result<(), String> {
    for (name, text) in logs {
        for (line, content) in text.lines().enumerate() {
            if content.contains("invariant violation") {
                return Err(format!(
                    "{name} log line {} carries an invariant violation: {content}",
                    line + 1
                ));
            }
        }
    }
    Ok(())
}
