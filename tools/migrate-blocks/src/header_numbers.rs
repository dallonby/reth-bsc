//! Backfill the MDBX `HeaderNumbers` table (hash → block number) from the
//! destination's headers static-file segments.
//!
//! ## Why this exists
//!
//! The static-file copy path migrates `Headers` (and `CanonicalHeaders` /
//! `HeaderTerminalDifficulties` inline) byte-for-byte. Those are number-keyed.
//! The reverse index `HeaderNumbers` (hash → number) lives only in MDBX and
//! is normally written by reth's `Headers` stage as it inserts each header.
//! We bypass the stage, so the reverse index is empty.
//!
//! Anything that calls `provider.header(hash)` — including BSC's pre-execution
//! parent-header lookup in `src/node/evm/pre_execution.rs` — does
//! `tx.get::<HeaderNumbers>(hash)` first. With an empty table the lookup
//! returns `None`, the BSC executor returns `BlockExecutionError::msg("Failed
//! to get parent header from global header reader")`, and the pipeline marks
//! `bad_block=Some(1)` — which unwinds *every* stage to 0, including Headers
//! and Bodies. That's a one-shot drain of the entire migration.
//!
//! ## How
//!
//! Read each header from the destination StaticFileProvider, hash it
//! (`hash_slow`, the keccak the reth provider uses), and bulk-insert
//! `(hash → number)` into MDBX. Hashing is parallelised with rayon because
//! it's the entire cost (the MDBX put is ~100 ns); for 92M headers on Ryzen
//! 9950X this is a couple of minutes.

use alloy_primitives::BlockNumber;
use rayon::prelude::*;
use reth_bsc::BscPrimitives;
use reth_db::{
    mdbx::DatabaseArguments,
    tables,
    transaction::{DbTx, DbTxMut},
    Database,
};
use reth_provider::{providers::StaticFileProvider, HeaderProvider};
use std::{
    path::{Path, PathBuf},
    time::Instant,
};

const HASH_BATCH: usize = 16_384;
const COMMIT_BATCH: usize = 250_000;

/// Backfill `HeaderNumbers` for blocks `[from, to]` (inclusive) using headers
/// already present in the destination's `static_files/`.
pub(crate) fn backfill(
    to_datadir: &Path,
    from: BlockNumber,
    to: BlockNumber,
) -> eyre::Result<u64> {
    let static_dir = resolve_static_files_dir(to_datadir);
    let db_dir = resolve_db_dir(to_datadir);

    eyre::ensure!(
        from <= to,
        "header_numbers backfill: from ({from}) > to ({to})",
    );

    let sfp = StaticFileProvider::<BscPrimitives>::read_only(&static_dir)
        .map_err(|e| eyre::eyre!("open dest static_files at {}: {e}", static_dir.display()))?;
    let db = reth_db::init_db(&db_dir, DatabaseArguments::new(Default::default()))
        .map_err(|e| eyre::eyre!("init dest db: {e}"))?;

    let total = to - from + 1;
    tracing::info!(
        from,
        to,
        total,
        hash_batch = HASH_BATCH,
        commit_batch = COMMIT_BATCH,
        "header_numbers: backfill starting",
    );

    let t0 = Instant::now();
    let mut written: u64 = 0;
    let mut committed_since_log: u64 = 0;
    let mut tx = db.tx_mut()?;
    let mut in_txn: usize = 0;

    let mut next = from;
    while next <= to {
        let batch_end = next.saturating_add(HASH_BATCH as u64 - 1).min(to);
        let nums: Vec<BlockNumber> = (next..=batch_end).collect();

        // Read headers in parallel; collecting (number, hash) pairs. Reads go
        // to mmap'd static files so this is CPU-bound on the keccak.
        let pairs: Vec<(BlockNumber, alloy_primitives::B256)> = nums
            .par_iter()
            .map(|&n| -> eyre::Result<_> {
                let h = sfp
                    .header_by_number(n)?
                    .ok_or_else(|| eyre::eyre!("header missing at block {n}"))?;
                Ok((n, h.hash_slow()))
            })
            .collect::<eyre::Result<Vec<_>>>()?;

        for (n, hash) in pairs {
            tx.put::<tables::HeaderNumbers>(hash, n)?;
            in_txn += 1;
            if in_txn >= COMMIT_BATCH {
                tx.commit()?;
                tx = db.tx_mut()?;
                in_txn = 0;
            }
        }
        let added = (batch_end - next + 1) as u64;
        written += added;
        committed_since_log += added;
        if committed_since_log >= 1_000_000 {
            let secs = t0.elapsed().as_secs_f64();
            tracing::info!(
                written,
                rate_per_sec = (written as f64 / secs) as u64,
                "header_numbers: progress",
            );
            committed_since_log = 0;
        }
        next = batch_end + 1;
    }
    tx.commit()?;
    tracing::info!(
        written,
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "header_numbers: backfill complete",
    );
    Ok(written)
}

fn resolve_db_dir(datadir: &Path) -> PathBuf {
    if datadir.join("db").is_dir() {
        datadir.join("db")
    } else {
        datadir.to_path_buf()
    }
}

fn resolve_static_files_dir(datadir: &Path) -> PathBuf {
    if datadir.join("static_files").is_dir() {
        datadir.join("static_files")
    } else {
        datadir.to_path_buf()
    }
}
