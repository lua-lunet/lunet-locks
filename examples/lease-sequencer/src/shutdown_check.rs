//! The shutdown-restart consistency check (the snapshot rule's
//! verification half): the run's logs are cross-checked against the
//! run's durable markers, from a raw run directory or a
//! `snapshot_run.sh` archive alike.
//!
//! The stop path (the termination obligations' §2 write order) leaves
//! both a log record trail and a durable marker trail, and the two must
//! agree:
//!
//! * a node's last stop-path log record, when it is the flushed record
//!   (`stop: node drained and flushed`,
//!   `stop: drained and flushed; the next boot continues...`,
//!   `stop: drained and the drain proven; ...`), demands a final marker
//!   showing flushed or stopped at that identity: the superblock quorum
//!   classification and the single-file projection must agree in state
//!   and incarnation. A log claiming a clean flush whose copies do not
//!   show it is INCONSISTENCY, reported with the log file, line, the
//!   record's timestamp, and the copy states. An earlier stop cycle
//!   followed by a later life (a boot record after it) is superseded —
//!   a final marker a later life rewrote cannot contradict it.
//! * the reverse: a final marker showing flushed/stopped with no stop
//!   record anywhere in the node's logs is INCONSISTENCY.
//! * the stop path's records are ordered (stop-begin, drain, flushed)
//!   and inversions are flagged: records out of sequence within one
//!   stop cycle, a marker written before the stop began (the marker
//!   file's mtime — the tar entry's mtime for archives — against the
//!   begin record's timestamp), and any work the node logged after the
//!   persist order completed — what the runtime did after the stop
//!   path said it was done.
//!
//! The bin maps the verdict to its exit: 0 consistent, 1 findings, 2
//! usage or input errors.

use flate2::read::GzDecoder;
use lunet_locks_aof::marker::{self, Classified, MarkerState};
use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

/// The check's verdict: every node consistent, or findings exist.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Verdict {
    Consistent,
    Inconsistent,
}

const STOP_BEGIN_SPELLINGS: &[&str] = &[
    "stop: term received",
    "stop: sigterm received",
    "stop: sigint received",
    "stop: hup received",
    "stop: usr1 received",
    "stop: usr2 received",
    "sigterm: clean stop",
];

const DRAIN_SPELLINGS: &[&str] = &[
    "stop: drain point reached",
    "stop: the wire is closed, the in-memory state is final",
];

const FLUSHED_SPELLINGS: &[&str] = &[
    "stop: node drained and flushed",
    "stop: drained and flushed",
    "stop: drained and the drain proven",
];

const BOOT_SPELLINGS: &[&str] = &["membership model era=", "boot name=", "node provisioned"];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopKind {
    Begin,
    Drain,
    Flushed,
}

impl StopKind {
    fn word(self) -> &'static str {
        match self {
            StopKind::Begin => "stop-begin",
            StopKind::Drain => "drain",
            StopKind::Flushed => "flushed",
        }
    }

    fn rank(self) -> usize {
        match self {
            StopKind::Begin => 0,
            StopKind::Drain => 1,
            StopKind::Flushed => 2,
        }
    }
}

/// One stop-path log record: its kind, its line position, the raw line,
/// the explicit `ts=` on the record itself, and the timestamp carried
/// forward from the last explicit `ts=` line before it (the log is
/// append-ordered, so the carry is a lower bound for untimed records).
#[derive(Debug, Clone)]
struct StopRecord {
    kind: StopKind,
    line_no: usize,
    text: String,
    ts: Option<u64>,
    carried_ts: Option<u64>,
    carried_from: Option<usize>,
}

impl StopRecord {
    /// The record's best-known wall timestamp in ms: its own `ts=`, else
    /// the carried one, with the line the carry came from.
    fn known_ts(&self) -> Option<(u64, Option<usize>)> {
        match self.ts {
            Some(ts) => Some((ts, None)),
            None => self.carried_ts.map(|ts| (ts, self.carried_from)),
        }
    }
}

fn stop_kind(line: &str) -> Option<StopKind> {
    let lower = line.to_lowercase();
    if STOP_BEGIN_SPELLINGS
        .iter()
        .any(|needle| lower.contains(needle))
    {
        return Some(StopKind::Begin);
    }
    if DRAIN_SPELLINGS.iter().any(|needle| lower.contains(needle)) {
        return Some(StopKind::Drain);
    }
    if FLUSHED_SPELLINGS
        .iter()
        .any(|needle| lower.contains(needle))
    {
        return Some(StopKind::Flushed);
    }
    None
}

fn is_boot_record(line: &str) -> bool {
    let lower = line.to_lowercase();
    BOOT_SPELLINGS.iter().any(|needle| lower.contains(needle))
}

fn explicit_ts(line: &str) -> Option<u64> {
    for field in line.split_whitespace() {
        if let Some(value) = field.strip_prefix("ts=") {
            return value.parse::<u64>().ok();
        }
    }
    None
}

fn marker_word(state: MarkerState) -> &'static str {
    match state {
        MarkerState::Flushed => "flushed",
        MarkerState::Stopped => "stopped",
        MarkerState::Unflushed => "unflushed",
    }
}

/// The single-file projection line: `<incarnation>
/// <unflushed|stopped|flushed>`.
fn parse_single_file(text: &str) -> Option<(u64, &'static str)> {
    let line = text.trim().lines().next()?;
    let (incarnation_text, word) = line.split_once(' ')?;
    let incarnation = incarnation_text.parse::<u64>().ok()?;
    let word = match word {
        "flushed" => "flushed",
        "stopped" => "stopped",
        "unflushed" => "unflushed",
        _ => return None,
    };
    Some((incarnation, word))
}

/// The node identity a file name carries: the leading token before the
/// first dot (`n1.state`, `n1.nonce.superblock`,
/// `n1.2026-09-18.log` all name node `n1`).
fn node_token(file_name: &str) -> Option<&str> {
    let token = file_name.split('.').next()?;
    if token.is_empty() { None } else { Some(token) }
}

fn is_log_file_name(name: &str) -> bool {
    let lower = name.to_lowercase();
    [".log", ".nohup", ".out", ".err"]
        .iter()
        .any(|suffix| lower.ends_with(suffix))
}

/// One node's gathered evidence, grouped by the file names' identity
/// token.
#[derive(Default)]
struct NodeEvidence {
    logs: Vec<PathBuf>,
    state_bases: Vec<PathBuf>,
    sidecars: Vec<PathBuf>,
}

impl NodeEvidence {
    fn is_empty(&self) -> bool {
        self.logs.is_empty() && self.state_bases.is_empty() && self.sidecars.is_empty()
    }
}

fn collect_files(root: &Path, files: &mut Vec<PathBuf>, depth: u8) -> Result<(), String> {
    if depth > 8 {
        return Ok(());
    }
    let mut entries: Vec<_> = fs::read_dir(root)
        .map_err(|error| format!("walk {}: {error}", root.display()))?
        .filter_map(|entry| entry.ok())
        .collect();
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let meta = entry
            .metadata()
            .map_err(|error| format!("stat {}: {error}", path.display()))?;
        if meta.is_dir() {
            collect_files(&path, files, depth + 1)?;
        } else if meta.is_file() {
            files.push(path);
        }
    }
    Ok(())
}

/// One durable-state base's marker copies: the superblock's quorum
/// classification (or its failure), the single-file projection line,
/// and the marker's write time (the superblock file's mtime; the tar
/// entry's mtime for archives).
struct MarkerCopy {
    base: PathBuf,
    superblock: Option<PathBuf>,
    classified: Option<Classified>,
    classify_error: Option<i32>,
    single_file: Option<(u64, &'static str)>,
    mtime_ms: Option<u64>,
}

impl MarkerCopy {
    fn render(&self, root: &Path) -> String {
        let mut render = format!(
            "single-file {}",
            match self.single_file {
                Some((incarnation, word)) => format!("\"{incarnation} {word}\""),
                None => "absent".to_string(),
            }
        );
        if let Some(path) = &self.superblock {
            render.push_str(&format!(" | superblock {}", display_rel(path, root)));
        } else {
            render.push_str(" | superblock absent");
        }
        if let Some(classified) = self.classified {
            render.push_str(&format!(
                " classifies {} at incarnation {}",
                marker_word(classified.state),
                classified.incarnation,
            ));
        }
        render
    }
}

/// Runs the check against a raw run directory or a snapshot archive,
/// writing the rendered report to `out`.
pub fn check_shutdown_to(input: &Path, out: &mut dyn std::io::Write) -> Result<Verdict, String> {
    let report = check_shutdown(input)?;
    let _ = out.write_all(report.text.as_bytes());
    Ok(report.verdict)
}

/// The full report: the rendered text plus the verdict.
#[derive(Debug, Clone)]
pub struct Report {
    pub text: String,
    pub verdict: Verdict,
}

/// Runs the check and renders the report text. Archive inputs are
/// extracted to a temporary directory that is removed before returning.
pub fn check_shutdown(input: &Path) -> Result<Report, String> {
    let (root, extracted) = resolve_input(input)?;
    let root = root
        .canonicalize()
        .map_err(|error| format!("resolve {}: {error}", root.display()))?;
    let mut files = Vec::new();
    collect_files(&root, &mut files, 0)?;

    let mut nodes: BTreeMap<String, NodeEvidence> = BTreeMap::new();
    let mut superblocks: BTreeMap<PathBuf, PathBuf> = BTreeMap::new();
    for path in &files {
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        let lower = name.to_lowercase();
        let rel = display_rel(path, &root);
        if lower.ends_with(".superblock") {
            let base = root.join(rel.strip_suffix(".superblock").unwrap_or(&rel));
            superblocks.insert(base, path.clone());
            continue;
        }
        let token = node_token(name).map(str::to_string);
        if lower.ends_with(".membership") {
            if let Some(token) = &token {
                nodes
                    .entry(token.clone())
                    .or_default()
                    .sidecars
                    .push(path.clone());
            }
            continue;
        }
        if lower.starts_with("flight-") && lower.ends_with(".jsonl") {
            continue;
        }
        if is_log_file_name(name) {
            if let Some(token) = &token {
                nodes
                    .entry(token.clone())
                    .or_default()
                    .logs
                    .push(path.clone());
            }
            continue;
        }
        if lower.ends_with(".state") {
            if let Some(token) = &token {
                nodes
                    .entry(token.clone())
                    .or_default()
                    .state_bases
                    .push(path.clone());
            }
        }
    }
    // A durable-state base is any file a `.superblock` names as its
    // sibling, whatever its extension (`n1.nonce` beside
    // `n1.nonce.superblock`).
    for base in superblocks.keys() {
        let Some(token) = base
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(node_token)
            .map(str::to_string)
        else {
            continue;
        };
        let evidence = nodes.entry(token).or_default();
        if !evidence.state_bases.contains(base) {
            evidence.state_bases.push(base.clone());
        }
    }
    nodes.retain(|_token, evidence| !evidence.is_empty());

    let mut text = String::new();
    let mut inconsistent = false;
    let mut checked = 0usize;
    for (token, evidence) in &nodes {
        let findings = check_node(token, evidence, &root);
        checked += 1;
        if findings.is_empty() {
            text.push_str(&format!(
                "OK [{token}] consistent: {} log file(s), {} durable-state file(s), \
{} membership sidecar(s)\n",
                evidence.logs.len(),
                evidence.state_bases.len(),
                evidence.sidecars.len(),
            ));
        } else {
            inconsistent = true;
        }
        for finding in findings {
            text.push_str(&finding);
            text.push('\n');
        }
    }
    if nodes.is_empty() {
        text.push_str("check-shutdown: no nodes with logs or durable state found\n");
    }
    let verdict = if inconsistent {
        Verdict::Inconsistent
    } else {
        Verdict::Consistent
    };
    let verdict_word = match verdict {
        Verdict::Consistent => "CONSISTENT",
        Verdict::Inconsistent => "INCONSISTENT",
    };
    text.push_str(&format!(
        "check-shutdown: {checked} node(s) checked: {verdict_word}\n"
    ));
    if extracted.is_some() {
        let _ = fs::remove_dir_all(extracted.unwrap());
    }
    Ok(Report { text, verdict })
}

/// A run directory is taken raw; anything else is a snapshot archive,
/// extracted to a temporary directory so the superblock classifier sees
/// real files.
fn resolve_input(input: &Path) -> Result<(PathBuf, Option<PathBuf>), String> {
    if input.is_dir() {
        return Ok((input.to_path_buf(), None));
    }
    let is_gzip = fs::File::open(input)
        .and_then(|mut file| {
            let mut magic = [0u8; 2];
            Ok(file.read_exact(&mut magic).is_ok() && magic == [0x1f, 0x8b])
        })
        .unwrap_or(false);
    if !is_gzip {
        return Err(format!(
            "not a run directory and not a gzip archive: {}",
            input.display()
        ));
    }
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0);
    let target = std::env::temp_dir().join(format!(
        "skaffold-check-shutdown-{}-{nanos}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&target);
    fs::create_dir_all(&target).map_err(|error| format!("extract dir: {error}"))?;
    let file =
        fs::File::open(input).map_err(|error| format!("open {}: {error}", input.display()))?;
    let mut archive = tar::Archive::new(GzDecoder::new(file));
    archive
        .unpack(&target)
        .map_err(|error| format!("unpack {}: {error}", input.display()))?;
    Ok((target.clone(), Some(target)))
}

/// One node's whole check: the marker demand of its last stop record,
/// the reverse direction, the per-file stop-path ordering, and the
/// post-persist work scan.
fn check_node(token: &str, evidence: &NodeEvidence, root: &Path) -> Vec<String> {
    let mut findings = Vec::new();

    let mut logs = evidence.logs.clone();
    logs.sort();
    let mut parsed: Vec<(PathBuf, Vec<StopRecord>)> = Vec::new();
    for log in &logs {
        let content = match fs::read_to_string(log) {
            Ok(content) => content,
            Err(error) => {
                findings.push(format!(
                    "INCONSISTENCY [{token}] unreadable-log: {}: {error}",
                    display_rel(log, root)
                ));
                continue;
            }
        };
        parsed.push((log.clone(), parse_stop_records(&content)));
    }

    let mut bases = evidence.state_bases.clone();
    bases.sort();
    let marker_copies: Vec<MarkerCopy> = bases
        .iter()
        .map(|base| {
            let superblock = PathBuf::from(format!("{}.superblock", base.display()));
            let classified = if superblock.is_file() {
                marker::classify(&superblock)
            } else {
                Err(-1)
            };
            MarkerCopy {
                base: base.clone(),
                superblock: superblock.is_file().then_some(superblock.clone()),
                classified: classified.ok(),
                classify_error: if superblock.is_file() {
                    classified.err()
                } else {
                    None
                },
                single_file: fs::read_to_string(base)
                    .ok()
                    .and_then(|text| parse_single_file(&text)),
                mtime_ms: fs::metadata(&superblock)
                    .or_else(|_| fs::metadata(base))
                    .and_then(|meta| meta.modified())
                    .ok()
                    .and_then(|time| time.duration_since(UNIX_EPOCH).ok())
                    .map(|duration| duration.as_millis() as u64),
            }
        })
        .collect();

    // The node's last stop record across all its logs decides whether
    // the final marker is demanded: only a flushed last record can
    // contradict (or agree with) the marker as it stands.
    let mut last_stop: Option<(usize, StopRecord)> = None;
    for (log_index, (_log, records)) in parsed.iter().enumerate() {
        if let Some(record) = records.last() {
            last_stop = Some((log_index, record.clone()));
        }
    }
    let last_flush = parsed
        .iter()
        .enumerate()
        .rev()
        .find_map(|(log_index, (_log, records))| {
            records
                .iter()
                .rev()
                .find(|record| record.kind == StopKind::Flushed)
                .map(|record| (log_index, record.clone()))
        });
    // A later life (a boot record after the node's last flushed record,
    // in its log or any log after it) supersedes the stop cycle: the
    // final marker belongs to that later life, not to the flushed
    // record.
    let has_later_life = match &last_flush {
        None => false,
        Some((log_index, record)) => {
            let log = &logs[*log_index];
            let in_log = fs::read_to_string(log)
                .map(|content| content.lines().skip(record.line_no).any(is_boot_record))
                .unwrap_or(false);
            in_log
                || parsed[*log_index + 1..]
                    .iter()
                    .any(|(later_log, _records)| {
                        fs::read_to_string(later_log)
                            .map(|content| content.lines().any(is_boot_record))
                            .unwrap_or(false)
                    })
        }
    };
    let demands_flush = last_stop.as_ref().map(|(_index, record)| record.kind)
        == Some(StopKind::Flushed)
        && !has_later_life;

    for copy in &marker_copies {
        // The single-file projection is written only after the
        // authoritative quorum write succeeded: a flushed/stopped
        // projection above an unflushed quorum is an inconsistency, and
        // the two copies must name the same identity.
        if let (Some((single_incarnation, single_word)), Some(classified)) =
            (copy.single_file, copy.classified)
        {
            if (single_word == "flushed" || single_word == "stopped")
                && classified.state == MarkerState::Unflushed
            {
                findings.push(format!(
                    "INCONSISTENCY [{token}] projection-ahead-of-quorum: \
the single-file marker \"{} {}\" at {} stands over a superblock that \
classifies unflushed at incarnation {}",
                    single_incarnation,
                    single_word,
                    display_rel(&copy.base, root),
                    classified.incarnation,
                ));
            }
            if single_incarnation != classified.incarnation {
                findings.push(format!(
                    "INCONSISTENCY [{token}] identity-mismatch: the single-file \
marker names incarnation {single_incarnation} but the superblock names {} \
for {}",
                    classified.incarnation,
                    display_rel(&copy.base, root),
                ));
            }
        }
        if let Some(code) = copy.classify_error {
            findings.push(format!(
                "INCONSISTENCY [{token}] unreadable-superblock: {} failed \
classification (code {code})",
                display_rel(&copy.base, root),
            ));
        }
        // The reverse direction: the marker vouches for a flush, the
        // logs must carry the stop record.
        if let Some(classified) = copy.classified {
            if matches!(
                classified.state,
                MarkerState::Flushed | MarkerState::Stopped
            ) {
                let has_flushed = parsed.iter().any(|(_log, records)| {
                    records
                        .iter()
                        .any(|record| record.kind == StopKind::Flushed)
                });
                if !has_flushed {
                    findings.push(format!(
                        "INCONSISTENCY [{token}] marker-without-stop-record: the \
superblock classifies {} at incarnation {} for {} but no log carries a \
drained-and-flushed stop record for this node",
                        marker_word(classified.state),
                        classified.incarnation,
                        display_rel(&copy.base, root),
                    ));
                }
            }
        }
    }

    if demands_flush {
        // The flush claim's own evidence: the log record (file, line,
        // raw line) and its best-known timestamp, plus the marker
        // copies' states as the check found them.
        let log_record = last_flush.as_ref().map(|(log_index, record)| {
            let ts = match (record.ts, record.carried_ts) {
                (Some(ts), _) => format!("{ts}"),
                (None, Some(carried)) => format!(
                    "{carried} (carried from line {})",
                    record.carried_from.unwrap_or(record.line_no),
                ),
                (None, None) => "unknown".to_string(),
            };
            format!(
                " log record: {}: line {}: \"{}\" ts: {ts}",
                display_rel(&logs[*log_index], root),
                record.line_no,
                record.text,
            )
        });
        for copy in &marker_copies {
            let copies = format!(" copies: {}", copy.render(root));
            match (copy.classified, copy.classify_error) {
                (None, None) => findings.push(format!(
                    "INCONSISTENCY [{token}] log-claims-flush-no-superblock: the \
stop record below claims a clean flush but no superblock file was found \
for {}{copies}{}",
                    display_rel(&copy.base, root),
                    log_record.as_deref().unwrap_or(""),
                )),
                (Some(classified), _) if classified.state == MarkerState::Unflushed => {
                    findings.push(format!(
                        "INCONSISTENCY [{token}] log-claims-flush-marker-not-flushed: \
the stop record below claims a clean flush but the superblock classifies \
unflushed at incarnation {} for {}{copies}{}",
                        classified.incarnation,
                        display_rel(&copy.base, root),
                        log_record.as_deref().unwrap_or(""),
                    ));
                }
                _ => {}
            }
        }
    }

    // The ordering gate is per log: only the log holding the node's
    // last stop record scans for post-persist work, and only when the
    // demand applies (the flushed record is the node's final stop and
    // no later life rewrote the marker after it).
    let work_after_gate = if demands_flush {
        match &last_stop {
            Some((log_index, record)) if record.kind == StopKind::Flushed => {
                Some((*log_index, record.line_no))
            }
            _ => None,
        }
    } else {
        None
    };
    for (log_index, (log, records)) in parsed.iter().enumerate() {
        let gate = work_after_gate
            .and_then(|(gate_log, line_no)| (gate_log == log_index).then_some(line_no));
        findings.extend(ordering_findings(
            token,
            log,
            root,
            records,
            gate,
            &marker_copies,
        ));
    }
    findings
}

/// Parses one log's content into its stop records, carrying the last
/// explicit `ts=` forward as each untimed record's lower bound.
fn parse_stop_records(content: &str) -> Vec<StopRecord> {
    let mut records = Vec::new();
    let mut carried_ts: Option<u64> = None;
    let mut carried_from: Option<usize> = None;
    for (index, line) in content.lines().enumerate() {
        if let Some(ts) = explicit_ts(line) {
            carried_ts = Some(ts);
            carried_from = Some(index + 1);
        }
        if let Some(kind) = stop_kind(line) {
            records.push(StopRecord {
                kind,
                line_no: index + 1,
                text: line.to_string(),
                ts: explicit_ts(line),
                carried_ts,
                carried_from,
            });
        }
    }
    records
}

/// The stop path's order: within each stop cycle the begin record
/// precedes the drain record, the drain record precedes the flushed
/// record. When `work_after_line` names the flushed record's line (the
/// node's persist order), nothing but stop and boot records may follow
/// it, and the marker's write time may not precede the stop's begin.
fn ordering_findings(
    token: &str,
    log: &Path,
    root: &Path,
    records: &[StopRecord],
    work_after_line: Option<usize>,
    marker_copies: &[MarkerCopy],
) -> Vec<String> {
    let mut findings = Vec::new();

    let mut cycle: Vec<(usize, StopKind)> = Vec::new();
    for record in records {
        if record.kind == StopKind::Begin && !cycle.is_empty() {
            cycle.clear();
        }
        cycle.push((record.line_no, record.kind));
    }
    let ranks: Vec<usize> = cycle.iter().map(|(_line, kind)| kind.rank()).collect();
    let mut sorted = ranks.clone();
    sorted.sort_unstable();
    if ranks != sorted {
        let named: Vec<String> = cycle
            .iter()
            .map(|(line, kind)| format!("line {line}: {}", kind.word()))
            .collect();
        findings.push(format!(
            "INVERSION [{token}] stop-path-out-of-order in {}: {}",
            display_rel(log, root),
            named.join("; "),
        ));
    }

    let begin_ts = records
        .iter()
        .filter(|record| record.kind == StopKind::Begin)
        .filter_map(|record| record.known_ts().map(|(ts, _from)| ts))
        .last();

    if let Some(line_no) = work_after_line {
        let lines = fs::read_to_string(log)
            .map(|content| content.lines().map(str::to_string).collect::<Vec<_>>())
            .unwrap_or_default();
        for (index, line) in lines.iter().enumerate() {
            let position = index + 1;
            if position <= line_no || line.trim().is_empty() {
                continue;
            }
            if stop_kind(line).is_some() || is_boot_record(line) {
                continue;
            }
            findings.push(format!(
                "INVERSION [{token}] work-after-persist in {}: line {position} \
logged after the persist order completed: \"{line}\"",
                display_rel(log, root),
            ));
        }
        if let Some(begin_ts) = begin_ts {
            for copy in marker_copies {
                let Some(mtime_ms) = copy.mtime_ms else {
                    continue;
                };
                if (mtime_ms / 1000) < (begin_ts / 1000) {
                    findings.push(format!(
                        "INVERSION [{token}] marker-written-before-stop-began: the \
marker at {} carries mtime {}ms but the stop-begin record carries \
ts={begin_ts}ms",
                        display_rel(&copy.base, root),
                        mtime_ms,
                    ));
                }
            }
        }
    }
    findings
}

/// The path as the run's relative layout, forward-slash separated.
fn display_rel(path: &Path, root: &Path) -> String {
    path.strip_prefix(root)
        .map(|rel| rel.to_string_lossy().replace('\\', "/"))
        .unwrap_or_else(|_| path.to_string_lossy().replace('\\', "/"))
}
