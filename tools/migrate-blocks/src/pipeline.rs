//! Pipeline orchestration: open source + dest, spawn readers, drive writer.
//!
//! Shape:
//!   readers (N) ──► reorder buffer (BTreeMap<block_num, Payload>) ──► writer (1)
//!
//! Each reader owns its own read-only MDBX tx on the source. Readers are
//! handed disjoint block-number ranges (contiguous shards); the writer pops
//! blocks from the reorder buffer in ascending block order and commits in
//! batches to the destination.

use crate::{
    reader::{open_source_db, reader_worker, SourceTip},
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
    // 1. Open source (read-only) and sanity-check it has the tables we expect.
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

    // 2. Open destination. `ensure_parlia_tables` makes sure our BSC-custom
    //    tables exist too, even though this pass doesn't use them — the target
    //    datadir will eventually be owned by the reth-bsc node.
    let mut dst = open_dest_db(&ctx.to)?;
    reth_bsc::consensus::parlia::db::ensure_parlia_tables(&mut dst)
        .map_err(|e| eyre::eyre!("ensure_parlia_tables on destination: {e}"))?;
    let dst = Arc::new(dst);
    tracing::info!("destination initialized");

    // 3. Bounded channel sized around the reorder window so readers don't run
    //    unboundedly ahead of the writer.
    let (tx, rx) = bounded::<BlockPayload>(ctx.reorder_window);

    // 4. Spawn readers. Each gets a contiguous shard of the block range.
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
    drop(tx); // When all reader senders drop, the writer's rx closes.

    // 5. Writer (single thread, strict in-order commit, batched).
    let written = writer_loop(dst.clone(), rx, ctx.from_block, to_block, ctx.reorder_window)?;
    tracing::info!(written_blocks = written, "writer finished");

    // 6. Ensure all readers exited cleanly.
    for h in reader_handles {
        if let Err(e) = h.join() {
            tracing::error!("reader thread panicked: {e:?}");
        }
    }

    // 7. Set StageCheckpoints so the target node skips Headers/Bodies/etc.
    commit_checkpoints(&dst, to_block, tip_tx, ctx.phase)?;
    tracing::info!(to_block, tip_tx, phase = ?ctx.phase, "stage checkpoints written");

    Ok(())
}
