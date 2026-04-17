//! Pipeline orchestration: decide what to migrate based on the requested
//! phase and dispatch to the right codepath.
//!
//! Two paths today:
//!
//!  * **Static-file copy** (default, safest): reth v0.0.8 and paradigm
//!    v2.0.0 both use NippyJar format v1 with stable per-row Compact
//!    encoding (for headers at least), so we can copy segment files
//!    directly from `<from>/static_files/` to `<to>/static_files/` and
//!    stamp the stage checkpoints to match the highest copied block. This
//!    is what gets you from 0 to ~92M BSC blocks in ~minutes.
//!
//!  * **MDBX-headers-only** (legacy): the old block-by-block MDBX walker.
//!    Only useful for source datadirs that predate static files or where
//!    recent blocks are still in MDBX. Left in place so we don't lose the
//!    pipeline scaffolding if we need it later.

use crate::{
    reader::{open_source_db, reader_worker, SourceTip},
    static_copy,
    writer::{commit_checkpoints, open_dest_db, writer_loop, BlockPayload},
    Phase,
};
use crossbeam_channel::bounded;
use std::path::PathBuf;
use std::sync::Arc;

pub(crate) struct Context {
    pub from: PathBuf,
    pub to: PathBuf,
    pub workers: usize,
    pub from_block: u64,
    pub to_block: Option<u64>,
    pub reorder_window: usize,
    pub dry_run: bool,
    pub phase: Phase,
}

pub(crate) fn run(ctx: Context) -> eyre::Result<()> {
    match ctx.phase {
        Phase::StaticHeaders => run_static_copy(ctx),
        Phase::StaticBodies => run_bodies(ctx),
        Phase::MdbxHeadersOnly => run_mdbx_pipeline(ctx),
    }
}

fn run_bodies(ctx: Context) -> eyre::Result<()> {
    let cfg = crate::bodies::BodiesConfig {
        decode_samples: 2_000,
        root_samples: 200,
        phase: ctx.phase,
        to_block: ctx.to_block,
    };
    crate::bodies::run(&ctx.from, &ctx.to, cfg, ctx.dry_run)
}

/// Static-file phase: discover segments in the source datadir, parallel-copy
/// the matching files to the destination's static_files directory, then stamp
/// stage checkpoints.
fn run_static_copy(ctx: Context) -> eyre::Result<()> {
    tracing::info!(phase = ?ctx.phase, "static-copy phase");

    let mut segments = static_copy::discover_source_segments(&ctx.from, ctx.phase)?;
    if segments.is_empty() {
        eyre::bail!(
            "no static-file segments found under {} for phase {:?}",
            ctx.from.display(),
            ctx.phase,
        );
    }

    // Honor --to-block by dropping any segment whose end block exceeds it.
    // NippyJar segments aren't truncatable in-place, so the smallest unit we
    // can copy is one whole segment (typically 500k blocks). Anything past
    // --to-block is dropped wholesale.
    if let Some(cap) = ctx.to_block {
        let before = segments.len();
        segments.retain(|s| s.end <= cap);
        let after = segments.len();
        if before != after {
            tracing::info!(
                cap_block = cap,
                segments_kept = after,
                segments_dropped = before - after,
                "to_block filter applied to segments",
            );
        }
        if segments.is_empty() {
            eyre::bail!(
                "no segments fit under --to-block {cap}; smallest segment is 500k blocks. \
                 Use --to-block 499999 to copy just the first segment.",
            );
        }
    }

    // Group summary — headers_tip here is whatever the highest segment
    // covers; we stamp the checkpoint to that block regardless of
    // from_block / to_block, because static files are monolithic per
    // segment (no partial-copy without breaking the format).
    let mut kinds = std::collections::BTreeMap::<String, (u64, u64, usize)>::new();
    for s in &segments {
        let e = kinds.entry(s.kind.clone()).or_insert((s.start, s.end, 0));
        e.0 = e.0.min(s.start);
        e.1 = e.1.max(s.end);
        e.2 += 1;
    }
    for (kind, (start, end, count)) in &kinds {
        tracing::info!(kind, blocks_start = start, blocks_end = end, segments = count, "plan");
    }

    if ctx.dry_run {
        tracing::info!("dry-run: skipping file copy and checkpoint write");
        return Ok(());
    }

    // Rayon thread pool size follows --workers (single file copy is IO-bound,
    // so parallelism helps up to NVMe queue depth, say 8–16).
    rayon::ThreadPoolBuilder::new()
        .num_threads(ctx.workers)
        .build_global()
        .ok(); // err is fine if the global pool was already built elsewhere

    let t_start = std::time::Instant::now();
    let report = static_copy::copy_segments(&ctx.to, &segments)?;
    let elapsed = t_start.elapsed();
    tracing::info!(
        bytes_copied = report.bytes_copied,
        headers_tip = ?report.headers_tip,
        elapsed_secs = elapsed.as_secs_f64(),
        "static file copy complete",
    );

    // Stamp stage checkpoints. For static-headers we set Headers only;
    // for static-all we also stamp Bodies / SenderRecovery / TransactionLookup
    // to the same tip since those stages walk the same static-file ranges.
    let tip_block = report
        .headers_tip
        .ok_or_else(|| eyre::eyre!("no headers segments copied; refusing to stamp checkpoints"))?;

    let mut dst = open_dest_db(&ctx.to)?;
    reth_bsc::consensus::parlia::db::ensure_parlia_tables(&mut dst)
        .map_err(|e| eyre::eyre!("ensure_parlia_tables: {e}"))?;
    drop(dst);

    // Backfill the MDBX `HeaderNumbers` (hash → number) reverse index.
    // Without this, `provider.header(hash)` returns None for every block —
    // including genesis — and BSC's pre-execution parent lookup in
    // src/node/evm/pre_execution.rs:71 returns BlockExecutionError, which
    // the pipeline interprets as bad_block=Some(1) and unwinds every stage
    // to 0. See header_numbers.rs for the full story.
    crate::header_numbers::backfill(&ctx.to, 0, tip_block)?;

    let dst = open_dest_db(&ctx.to)?;
    commit_checkpoints(&dst, tip_block, /* tip_tx unused in static phases */ 0, ctx.phase)?;
    tracing::info!(
        tip_block,
        phase = ?ctx.phase,
        "stage checkpoints stamped; target node will resume Execution from genesis",
    );

    Ok(())
}

/// Legacy MDBX block-by-block pipeline. Retained for datadirs where headers
/// live in MDBX rather than static files.
fn run_mdbx_pipeline(ctx: Context) -> eyre::Result<()> {
    let src = Arc::new(open_source_db(&ctx.from)?);
    let SourceTip { tip_block, tip_tx } = crate::reader::discover_source_tip(&src)?;
    let to_block = ctx.to_block.unwrap_or(tip_block);
    eyre::ensure!(
        ctx.from_block <= to_block,
        "from_block ({}) > resolved to_block ({})",
        ctx.from_block,
        to_block,
    );
    tracing::info!(tip_block, tip_tx, to_block, "source tip discovered");

    if ctx.dry_run {
        tracing::info!("dry-run requested; exiting before any writes.");
        return Ok(());
    }

    let mut dst = open_dest_db(&ctx.to)?;
    reth_bsc::consensus::parlia::db::ensure_parlia_tables(&mut dst)
        .map_err(|e| eyre::eyre!("ensure_parlia_tables on destination: {e}"))?;
    let dst = Arc::new(dst);
    tracing::info!("destination initialized");

    let (tx, rx) = bounded::<BlockPayload>(ctx.reorder_window);

    let total = to_block - ctx.from_block + 1;
    let shard_size = total.div_ceil(ctx.workers as u64);
    let mut reader_handles = Vec::with_capacity(ctx.workers);
    for w in 0..ctx.workers {
        let start = ctx.from_block + (w as u64) * shard_size;
        if start > to_block {
            break;
        }
        let end = (start + shard_size - 1).min(to_block);
        let src = src.clone();
        let tx = tx.clone();
        let phase = ctx.phase;
        reader_handles.push(std::thread::Builder::new().name(format!("reader-{w}")).spawn(
            move || {
                if let Err(e) = reader_worker(src, start, end, phase, tx) {
                    tracing::error!(worker = w, ?e, "reader failed");
                }
            },
        )?);
    }
    drop(tx);

    let written =
        writer_loop(dst.clone(), rx, ctx.from_block, to_block, ctx.reorder_window)?;
    tracing::info!(written_blocks = written, "writer finished");

    for h in reader_handles {
        if let Err(e) = h.join() {
            tracing::error!("reader thread panicked: {e:?}");
        }
    }

    commit_checkpoints(&dst, to_block, tip_tx, ctx.phase)?;
    tracing::info!(to_block, tip_tx, phase = ?ctx.phase, "stage checkpoints written");

    Ok(())
}
