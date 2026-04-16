//! v2 / destination-side writer. Single thread, strict ascending block order,
//! batched commits.

use crate::{reader::BodyPayload, Phase};
use alloy_consensus::Header;
use alloy_primitives::{BlockHash, BlockNumber};
use crossbeam_channel::Receiver;
use reth_db::{
    mdbx::{DatabaseArguments, DatabaseEnv},
    tables,
    transaction::{DbTx, DbTxMut},
    Database,
};
use reth_stages_types::{StageCheckpoint, StageId};
use std::{collections::BTreeMap, path::Path, sync::Arc};

/// Block payload crossing the reader→writer channel.
pub(crate) struct BlockPayload {
    pub number: BlockNumber,
    pub hash: BlockHash,
    pub header: Header,
    pub body: Option<BodyPayload>,
}

/// Open (or init) the destination reth v2.0.0 MDBX env.
///
/// Uses `init_db` so the default `Tables` schema is created first; caller is
/// expected to layer `ensure_parlia_tables` on top afterwards.
///
/// Accepts the top-level datadir and auto-creates the `db/` subdir (reth
/// convention), so users can pass `--to /mnt/bsc-data` rather than
/// `/mnt/bsc-data/db` and it just works.
pub(crate) fn open_dest_db(path: &Path) -> eyre::Result<DatabaseEnv> {
    let resolved = if path.join("mdbx.dat").is_file()
        || path.file_name().map(|n| n == "db").unwrap_or(false)
    {
        path.to_path_buf()
    } else {
        path.join("db")
    };
    std::fs::create_dir_all(&resolved)
        .map_err(|e| eyre::eyre!("mkdir {}: {e}", resolved.display()))?;
    tracing::info!(path = %resolved.display(), "opening (or initializing) dest MDBX env");
    reth_db::init_db(&resolved, DatabaseArguments::new(Default::default()))
        .map_err(|e| eyre::eyre!("init destination db: {e}"))
}

/// Writer loop: receive out-of-order blocks from the reorder buffer, commit in
/// strict ascending order from `first` to `last`.
///
/// Commit strategy: one RW tx per `COMMIT_BATCH` blocks. This balances MDBX
/// write-amplification against time-to-recovery if the migrator is
/// interrupted (restart picks up from the last committed checkpoint — see
/// `commit_checkpoints`).
pub(crate) fn writer_loop(
    db: Arc<DatabaseEnv>,
    rx: Receiver<BlockPayload>,
    first: BlockNumber,
    last: BlockNumber,
    reorder_window: usize,
) -> eyre::Result<u64> {
    const COMMIT_BATCH: u64 = 10_000;

    let mut pending: BTreeMap<BlockNumber, BlockPayload> = BTreeMap::new();
    let mut next_expected = first;
    let mut written: u64 = 0;
    let mut batch_start_block = first;
    let mut rw = db.tx_mut()?;

    loop {
        if next_expected > last {
            // All in-range blocks emitted — flush and exit.
            rw.commit()?;
            tracing::info!(written, "writer complete");
            break;
        }

        // Pull from reorder buffer first; only block on the channel if we'd
        // otherwise stall waiting for an out-of-order block.
        let payload = if let Some(p) = pending.remove(&next_expected) {
            p
        } else {
            match rx.recv() {
                Ok(p) if p.number == next_expected => p,
                Ok(p) => {
                    pending.insert(p.number, p);
                    if pending.len() > reorder_window * 2 {
                        eyre::bail!(
                            "reorder buffer overflowed ({} blocks pending, expected {next_expected}); \
                             a reader probably skipped a block or shards overlap wrong",
                            pending.len(),
                        );
                    }
                    continue;
                }
                Err(_) => {
                    // All readers done. If we still have the expected block
                    // somewhere in pending, that means a shard didn't cover
                    // it — error. Otherwise we've just reached end-of-stream
                    // early (bail loudly).
                    if let Some(p) = pending.remove(&next_expected) {
                        p
                    } else {
                        eyre::bail!(
                            "readers disconnected before block {next_expected} was emitted",
                        );
                    }
                }
            }
        };

        write_block(&rw, &payload)?;
        written += 1;
        next_expected += 1;

        // Commit periodically to bound memory + recovery window.
        if next_expected - batch_start_block >= COMMIT_BATCH {
            tracing::debug!(
                block = next_expected - 1,
                committed = next_expected - batch_start_block,
                "batch commit",
            );
            rw.commit()?;
            batch_start_block = next_expected;
            rw = db.tx_mut()?;
        }
    }

    Ok(written)
}

fn write_block(tx: &impl DbTxMut, p: &BlockPayload) -> eyre::Result<()> {
    // Headers + canonical mapping are required for every phase.
    tx.put::<tables::Headers>(p.number, p.header.clone())?;
    tx.put::<tables::HeaderNumbers>(p.hash, p.number)?;
    tx.put::<tables::CanonicalHeaders>(p.number, p.hash)?;

    if let Some(body) = &p.body {
        tx.put::<tables::BlockBodyIndices>(p.number, body.indices.clone())?;
        let mut tx_num = body.indices.first_tx_num;
        for signed in &body.transactions {
            tx.put::<tables::Transactions>(tx_num, signed.clone())?;
            // TransactionBlocks uses the LAST tx in the block as key → value =
            // block number. We write per-tx for simplicity; final write wins.
            // (Upstream populates only at the block boundary; both shapes
            // satisfy the lookup contract because the stage indexes by
            // "highest tx id in block".)
            tx_num += 1;
        }
        if body.indices.tx_count > 0 {
            let last_tx = body.indices.first_tx_num + body.indices.tx_count - 1;
            tx.put::<tables::TransactionBlocks>(last_tx, p.number)?;
        }
    }

    Ok(())
}

/// Record `StageCheckpoints` entries so a subsequent `reth-bsc node` run
/// treats the migrated range as already-done for Headers / Bodies /
/// SenderRecovery / TransactionLookup, and starts at Execution from block 0
/// on top of the migrated raw data.
///
/// NOTE: we deliberately do NOT write an Execution checkpoint. Leaving it
/// unset causes reth to execute every block from genesis upward, populating
/// state and Receipts through the normal stage path — which is the whole
/// point of this migration tier.
pub(crate) fn commit_checkpoints(
    db: &DatabaseEnv,
    tip_block: BlockNumber,
    tip_tx: u64,
    phase: Phase,
) -> eyre::Result<()> {
    let tx = db.tx_mut()?;

    // Every phase stamps Headers; static-all / mdbx-all additionally cover
    // the downstream append-only stages.
    put_checkpoint(&tx, StageId::Headers, tip_block)?;

    if matches!(phase, Phase::StaticBodies) {
        put_checkpoint(&tx, StageId::Bodies, tip_block)?;
        put_checkpoint(&tx, StageId::SenderRecovery, tip_block)?;
        put_checkpoint(&tx, StageId::TransactionLookup, tip_block)?;
        // Execution checkpoint intentionally unset — see doc comment above.
        let _ = tip_tx;
    }

    tx.commit()?;
    Ok(())
}

fn put_checkpoint(
    tx: &impl DbTxMut,
    stage: StageId,
    block: BlockNumber,
) -> eyre::Result<()> {
    let cp = StageCheckpoint::new(block);
    tx.put::<tables::StageCheckpoints>(stage.to_string(), cp)?;
    Ok(())
}
