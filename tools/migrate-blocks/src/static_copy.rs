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

/// Copy a single file, using chunked parallel `copy_file_range` syscalls
/// for large files to saturate modern NVMe queue depth while keeping the
/// kernel zero-copy path. Small files fall through to `std::fs::copy`.
///
/// Why chunked parallel `copy_file_range` rather than pread/pwrite:
///  - `copy_file_range(2)` is the same syscall `std::fs::copy` lands on via
///    the kernel's fast path. It does the transfer entirely in-kernel —
///    source page cache → dest page cache, no userspace buffer, no extra
///    memcpy over DRAM.
///  - `std::fs::copy` serializes the whole file in one call. `copy_file_range`
///    accepts explicit `src_off`/`dst_off`, so we can invoke many in
///    parallel for disjoint offset ranges. Each call runs independently,
///    which stacks queue depth at the NVMe.
///  - Using pread+pwrite instead (as a prior iteration did) forces every
///    byte through userspace and ~doubles memory-bandwidth cost per copy —
///    that version measured *slower* than plain `fs::copy` on PCIe 5 SSDs.
///
/// `fallocate` pre-reserves the destination extent so concurrent
/// cross-offset writes don't race to extend the file.
fn copy_file(src: &Path, dst: &Path) -> eyre::Result<u64> {
    use std::os::fd::AsRawFd;

    const LARGE_FILE_THRESHOLD: u64 = 128 * 1024 * 1024; // 128 MB
    const CHUNK_SIZE: u64 = 128 * 1024 * 1024; // 128 MB per copy_file_range call

    // If the destination exists, it's almost certainly a genesis-stub written
    // by `reth-bsc init` (single-block segment, < 1 MB). The user is
    // expected to run `init` first so that genesis state (PlainAccountState,
    // Bytecodes, …) is populated; without that step BSC's pre-execution
    // returns 0 gas for every system tx in block 1 and unwinds the chain.
    // Once init has run, the stub headers/transactions/receipts segments at
    // range 0..=499999 must be replaced by our migrated, real-data versions.
    // Delete + log.
    if dst.exists() {
        let stub_size = dst.metadata().map(|m| m.len()).unwrap_or(0);
        tracing::warn!(
            path = %dst.display(),
            stub_size,
            "destination already exists (likely a `reth-bsc init` stub); deleting before copy"
        );
        fs::remove_file(dst)
            .map_err(|e| eyre::eyre!("remove existing {}: {e}", dst.display()))?;
    }

    let src_file = fs::File::open(src)
        .map_err(|e| eyre::eyre!("open source {}: {e}", src.display()))?;
    let size = src_file
        .metadata()
        .map_err(|e| eyre::eyre!("stat source {}: {e}", src.display()))?
        .len();

    // Small files: plain fs::copy — overhead of a par_iter costs more than
    // any queue-depth we'd gain.
    if size < LARGE_FILE_THRESHOLD {
        drop(src_file);
        return match fs::copy(src, dst) {
            Ok(n) => Ok(n),
            Err(e) => Err(eyre::eyre!("copy {} -> {}: {e}", src.display(), dst.display())),
        };
    }

    let dst_file = fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(dst)
        .map_err(|e| eyre::eyre!("create dest {}: {e}", dst.display()))?;
    preallocate(&dst_file, size);

    let src_fd = src_file.as_raw_fd();
    let dst_fd = dst_file.as_raw_fd();

    let num_chunks = size.div_ceil(CHUNK_SIZE);
    (0..num_chunks)
        .into_par_iter()
        .try_for_each(|i| -> eyre::Result<()> {
            let offset = i * CHUNK_SIZE;
            let len = (size - offset).min(CHUNK_SIZE);
            copy_range_exact(src_fd, dst_fd, offset, len).map_err(|e| {
                eyre::eyre!(
                    "copy_file_range {} -> {} @ offset {offset} len {len}: {e}",
                    src.display(),
                    dst.display(),
                )
            })
        })?;

    // Keep the file handles alive until every chunk has completed, otherwise
    // the raw fds we captured above would dangle.
    drop(dst_file);
    drop(src_file);

    Ok(size)
}

/// Issue `copy_file_range(2)` for an explicit [offset, offset+len) range on
/// both sides. If the kernel returns `EXDEV` (source and dest on different
/// filesystems — `copy_file_range` cannot bridge them without a filesystem
/// driver that supports it), transparently fall back to [`splice_range_exact`]
/// which routes through a pipe in-kernel and works across any fs combo.
///
/// Other short returns are handled by looping until the full range is copied
/// (the kernel is always allowed to report fewer bytes than we asked for).
fn copy_range_exact(src_fd: i32, dst_fd: i32, start: u64, remaining: u64) -> io::Result<()> {
    let mut src_off = start as libc::off64_t;
    let mut dst_off = start as libc::off64_t;
    let mut remaining = remaining;

    while remaining > 0 {
        // SAFETY: fds are valid for the lifetime of the caller's File
        // handles; offsets are in-range; len is size_t.
        let ret = unsafe {
            libc::copy_file_range(
                src_fd,
                &mut src_off,
                dst_fd,
                &mut dst_off,
                remaining as usize,
                0,
            )
        };
        if ret < 0 {
            let err = io::Error::last_os_error();
            // Common reasons to fall back:
            //  EXDEV  — cross-fs (we hit this on XFS-LVM → XFS-root)
            //  EINVAL — some fs drivers reject copy_file_range
            //  ENOSYS — older kernel
            if matches!(err.raw_os_error(), Some(libc::EXDEV) | Some(libc::EINVAL) | Some(libc::ENOSYS)) {
                // `src_off` / `dst_off` have been updated to reflect any
                // partial copy that already succeeded in prior iterations.
                return splice_range_exact(src_fd, dst_fd, src_off as u64, dst_off as u64, remaining);
            }
            return Err(err);
        }
        if ret == 0 {
            return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "copy_file_range EOF"));
        }
        remaining = remaining.saturating_sub(ret as u64);
    }
    Ok(())
}

/// Cross-filesystem fallback using `splice(2)` through an anonymous pipe.
/// Works across every Linux fs combo: splice pumps bytes source → pipe
/// (zero-copy where the fs supports page pinning) then pipe → dest (same).
/// We use a single pipe per call and keep it tight (16 MiB) — the kernel's
/// pipe buffer is the natural backpressure mechanism.
fn splice_range_exact(
    src_fd: i32,
    dst_fd: i32,
    mut src_off: u64,
    mut dst_off: u64,
    mut remaining: u64,
) -> io::Result<()> {
    // Create an O_CLOEXEC pipe dedicated to this splice chain.
    let mut pipe_fds: [libc::c_int; 2] = [-1, -1];
    // SAFETY: pipe2 initializes both fds on success.
    let rc = unsafe { libc::pipe2(pipe_fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if rc != 0 {
        return Err(io::Error::last_os_error());
    }
    let (pipe_r, pipe_w) = (pipe_fds[0], pipe_fds[1]);

    // Bump pipe capacity to reduce context-switch overhead. 16 MiB is the
    // standard large-pipe size; root usually gets it without sysctl tuning.
    unsafe {
        let _ = libc::fcntl(pipe_w, libc::F_SETPIPE_SZ, 1 << 24);
    }

    let result = (|| -> io::Result<()> {
        while remaining > 0 {
            // src -> pipe
            let to_pipe = {
                let mut off = src_off as libc::off64_t;
                // SAFETY: fds are valid; off is valid; nr bytes fits usize.
                let ret = unsafe {
                    libc::splice(
                        src_fd,
                        &mut off,
                        pipe_w,
                        std::ptr::null_mut(),
                        remaining as usize,
                        libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE,
                    )
                };
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
                if ret == 0 {
                    return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "splice src EOF"));
                }
                src_off = off as u64;
                ret as usize
            };

            // pipe -> dst
            let mut drained = 0usize;
            while drained < to_pipe {
                let mut off = dst_off as libc::off64_t;
                // SAFETY: as above.
                let ret = unsafe {
                    libc::splice(
                        pipe_r,
                        std::ptr::null_mut(),
                        dst_fd,
                        &mut off,
                        to_pipe - drained,
                        libc::SPLICE_F_MOVE | libc::SPLICE_F_MORE,
                    )
                };
                if ret < 0 {
                    return Err(io::Error::last_os_error());
                }
                if ret == 0 {
                    return Err(io::Error::new(io::ErrorKind::WriteZero, "splice dst short"));
                }
                drained += ret as usize;
                dst_off = off as u64;
            }

            remaining = remaining.saturating_sub(to_pipe as u64);
        }
        Ok(())
    })();

    // Always reap the pipe fds, even on error.
    unsafe {
        libc::close(pipe_r);
        libc::close(pipe_w);
    }
    result
}

/// Best-effort preallocation. Silently ignores any failure because this is
/// purely a perf hint.
fn preallocate(file: &fs::File, len: u64) {
    use std::os::fd::AsRawFd;
    // SAFETY: file's raw fd is valid for the lifetime of `file`.
    let _ = unsafe { libc::fallocate(file.as_raw_fd(), 0, 0, len as libc::off_t) };
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
