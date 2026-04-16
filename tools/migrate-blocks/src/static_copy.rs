//! Static-file copier for reth datadirs.
//!
//! Reth stores bulk append-only chain data (headers, transactions, receipts)
//! in segmented NippyJar files under `<datadir>/static_files/`. Paradigm
//! v2.0.0 and bnb-chain v0.0.8 both use NippyJar format version 1, and the
//! Header row-level Compact encoding is stable, so we can migrate headers by
//! simply copying the segment triplets (`seg`, `seg.conf`, `seg.off`).
//!
//! Transactions and Receipts segments are also copied when requested, with
//! the caveat that the bnb-chain v0.0.8 `TransactionSigned` Compact layout
//! differs from paradigm v2.0.0's alloy envelope — copying the raw file will
//! trip when the target node's Execution stage tries to decode it, producing
//! a receipts-root mismatch at the affected block. That's self-verifying:
//! if it compiles through Execution, the bytes were compatible.
//!
//! The copy is parallelised with `rayon` across segments; within a segment,
//! the three files are copied sequentially.

use crate::Phase;
use alloy_primitives::BlockNumber;
use rayon::prelude::*;
use std::{
    fs,
    io,
    path::{Path, PathBuf},
};

/// What we discovered about a single NippyJar segment on disk.
#[derive(Debug, Clone)]
pub(crate) struct Segment {
    /// `headers`, `transactions`, `receipts`, ...
    pub kind: String,
    /// Inclusive range of block numbers the segment covers (derived from the
    /// filename tail `<start>_<end>`).
    pub start: BlockNumber,
    pub end: BlockNumber,
    /// Absolute path to the main data file (no `.conf`/`.off` suffix).
    pub data_path: PathBuf,
}

impl Segment {
    fn triplet(&self) -> [PathBuf; 3] {
        [
            self.data_path.clone(),
            self.data_path.with_extension("conf"),
            self.data_path.with_extension("off"),
        ]
    }

    fn file_name(&self) -> &str {
        self.data_path.file_name().and_then(|s| s.to_str()).unwrap_or_default()
    }
}

/// Scan the source datadir's static_files directory and return the set of
/// segments matching the requested phase.
pub(crate) fn discover_source_segments(
    source_datadir: &Path,
    phase: Phase,
) -> eyre::Result<Vec<Segment>> {
    let dir = resolve_static_files_dir(source_datadir);
    eyre::ensure!(
        dir.is_dir(),
        "source static_files dir not found at {} (tried <datadir>/static_files and <datadir>)",
        dir.display(),
    );

    let wanted_kinds: &[&str] = match phase {
        Phase::StaticHeaders | Phase::MdbxHeadersOnly => &["headers"],
        // bodies.rs filters down to just "transactions" before calling the
        // copier; we still expose the full set here so future phases can
        // pick up receipts without another code change.
        Phase::StaticBodies => &["headers", "transactions", "receipts"],
    };

    let mut segments = Vec::new();
    for entry in fs::read_dir(&dir)? {
        let entry = entry?;
        let path = entry.path();
        // We only touch the bare data file; `.conf` and `.off` are carried
        // along via `Segment::triplet`. Filter by extension (or lack
        // thereof) to avoid double-counting.
        let ext = path.extension().and_then(|s| s.to_str()).unwrap_or_default();
        if ext == "conf" || ext == "off" {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some(seg) = parse_segment_name(name, &path) else {
            continue;
        };
        if wanted_kinds.iter().any(|k| seg.kind == *k) {
            segments.push(seg);
        }
    }
    segments.sort_by(|a, b| (a.kind.clone(), a.start).cmp(&(b.kind.clone(), b.start)));
    Ok(segments)
}

/// Parse a filename of the form `static_file_<kind>_<start>_<end>`.
/// Returns `None` for anything that doesn't match.
fn parse_segment_name(name: &str, path: &Path) -> Option<Segment> {
    let rest = name.strip_prefix("static_file_")?;
    // Find the last two underscore-delimited integers.
    let (prefix_kind, end_str) = rest.rsplit_once('_')?;
    let (kind, start_str) = prefix_kind.rsplit_once('_')?;
    let start = start_str.parse().ok()?;
    let end = end_str.parse().ok()?;
    Some(Segment {
        kind: kind.to_string(),
        start,
        end,
        data_path: path.to_path_buf(),
    })
}

/// Copy the listed segments into `<dest_datadir>/static_files/`, parallelised
/// across segments. Returns the highest block number covered by any copied
/// headers segment (used to stamp `StageCheckpoints[Headers]`).
pub(crate) fn copy_segments(
    dest_datadir: &Path,
    segments: &[Segment],
) -> eyre::Result<CopyReport> {
    let dest_dir = dest_datadir.join("static_files");
    fs::create_dir_all(&dest_dir)?;

    let total_files = segments.len() * 3;
    tracing::info!(
        total_segments = segments.len(),
        total_files,
        "copying static_files"
    );

    let results: Vec<Result<u64, eyre::Report>> = segments
        .par_iter()
        .map(|seg| copy_segment_triplet(seg, &dest_dir))
        .collect();

    let mut bytes_total: u64 = 0;
    for r in results {
        bytes_total += r?;
    }

    let headers_tip = segments
        .iter()
        .filter(|s| s.kind == "headers")
        .map(|s| s.end)
        .max();

    Ok(CopyReport { bytes_copied: bytes_total, headers_tip })
}

pub(crate) struct CopyReport {
    pub bytes_copied: u64,
    pub headers_tip: Option<BlockNumber>,
}

fn copy_segment_triplet(seg: &Segment, dest_dir: &Path) -> eyre::Result<u64> {
    let mut bytes = 0u64;
    for src in seg.triplet() {
        if !src.exists() {
            // Some segments may not have a `.off` or `.conf` in edge cases
            // (e.g. still being written). Warn and move on.
            tracing::warn!(path = %src.display(), "segment sibling file missing, skipping");
            continue;
        }
        let file_name = src
            .file_name()
            .ok_or_else(|| eyre::eyre!("no file name: {}", src.display()))?;
        let dst = dest_dir.join(file_name);
        bytes += copy_file(&src, &dst)?;
    }
    tracing::debug!(segment = seg.file_name(), "copied");
    Ok(bytes)
}

/// Copy a single file using `std::fs::copy` — simple, uses the kernel's best
/// available path (copy_file_range on Linux, sendfile fallback, plain read/
/// write otherwise).
fn copy_file(src: &Path, dst: &Path) -> eyre::Result<u64> {
    match fs::copy(src, dst) {
        Ok(n) => Ok(n),
        Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
            eyre::bail!("{} already exists; refusing to overwrite", dst.display())
        }
        Err(e) => Err(eyre::eyre!("copy {} -> {}: {e}", src.display(), dst.display())),
    }
}

/// Prefer `<path>/static_files`; fall back to `<path>` itself if it already
/// points at a directory containing `static_file_*` entries.
fn resolve_static_files_dir(path: &Path) -> PathBuf {
    if path.join("static_files").is_dir() {
        path.join("static_files")
    } else {
        path.to_path_buf()
    }
}
