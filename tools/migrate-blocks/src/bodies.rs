//! Defensive bodies migration.
//!
//! **Goal:** produce a destination datadir whose bodies-layer tables and
//! static files are byte-for-byte what reth's staged-sync `Bodies` stage
//! would have written — without re-downloading 93M bodies from the network.
//!
//! ## Alignment with the Bodies stage
//!
//! Reth 2.0.0's `Bodies::execute` (see
//! `crates/stages/stages/src/stages/bodies.rs`) delegates all writes to
//! `DatabaseProvider::append_block_bodies`, which in turn does:
//!
//! | What Bodies stage writes          | Where                                  | Our approach                                          |
//! |------------------------------------|----------------------------------------|--------------------------------------------------------|
//! | Transactions                       | `static_files/transactions_*` segments | **raw-copy segment triplets** (byte-equivalent to stage-produced files; NippyJar v1 on both sides) |
//! | BlockBodyIndices                   | MDBX                                   | MDBX cursor-walk copy                                  |
//! | TransactionBlocks                  | MDBX                                   | MDBX cursor-walk copy                                  |
//! | Ommers / Withdrawals (via ChainStorageWriter = BscStorage) | MDBX | MDBX cursor-walk copy           |
//! | TransactionHashNumbers             | Written by TransactionLookup stage     | **We don't touch it** — let TransactionLookup rebuild |
//! | TransactionSenders                 | Written by SenderRecovery stage        | **We don't touch it** — let SenderRecovery re-derive  |
//!
//! The key observation: Bodies stage writes bytes via `StaticFileProviderRW
//! ::append_transaction`, which encodes each tx with Compact and appends to
//! the segment file. Our source datadir's segment files were produced by
//! the exact same code path at v0.0.8. So raw-copying them is equivalent to
//! invoking `append_transaction` for every tx — faster by orders of
//! magnitude, same bytes on disk.
//!
//! ## Two preflight gates
//!
//!  1. **Decode probe.** Read N sampled tx rows from the source
//!     `static_files/` via v2's `StaticFileProvider<BscPrimitives>`. Any
//!     decode error → byte-incompatibility → abort before touching the
//!     destination.
//!  2. **Root round-trip probe.** For M sampled blocks, reconstruct the
//!     `transactions_root` from source-side data (header from static files,
//!     tx list from static files, BlockBodyIndices from MDBX) and compare
//!     against the value stored in the header. A root mismatch rules out
//!     every subtle decode-succeeds-but-semantically-wrong scenario.
//!
//! ## Post-copy gate
//!
//! We re-run both gates against the **destination** after copying. Catches
//! copy-time corruption (disk errors, partial writes) before the user
//! invests 50+ hours in Execution stage.

use crate::Phase;
use alloy_consensus::{proofs::calculate_transaction_root, BlockHeader};
use alloy_primitives::{BlockNumber, TxNumber, B256};
use reth_bsc::BscPrimitives;
use reth_db::{
    cursor::{DbCursorRO, DbCursorRW},
    mdbx::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind},
    tables,
    transaction::{DbTx, DbTxMut},
    Database,
};
use reth_provider::{
    providers::StaticFileProvider, HeaderProvider, TransactionsProvider,
};
use std::{
    path::{Path, PathBuf},
    time::Instant,
};

/// Operational knobs for bodies migration.
pub(crate) struct BodiesConfig {
    /// Number of random tx rows to decode-probe before agreeing to migrate.
    pub decode_samples: usize,
    /// Number of blocks to fully reconstruct (tx list + transactions_root
    /// recompute) before agreeing to migrate.
    pub root_samples: usize,
    /// Phase (passed through so we stamp the right checkpoints at the end).
    pub phase: Phase,
    /// Optional cap on the highest block to migrate. NippyJar segments aren't
    /// truncatable, so segments whose `end > to_block` are dropped wholesale.
    pub to_block: Option<BlockNumber>,
}

impl Default for BodiesConfig {
    fn default() -> Self {
        Self {
            decode_samples: 2_000,
            root_samples: 200,
            phase: Phase::StaticBodies,
            to_block: None,
        }
    }
}

/// Detailed probe outcome; on any `failures` the migrator aborts.
pub(crate) struct PreflightReport {
    pub source_block_range: (BlockNumber, BlockNumber),
    pub source_tx_range: (TxNumber, TxNumber),
    pub decode_samples_checked: usize,
    pub root_samples_checked: usize,
    pub failures: Vec<String>,
}

impl PreflightReport {
    pub fn ok(&self) -> bool {
        self.failures.is_empty()
    }
}

/// Entry point for the `static-bodies` phase.
pub(crate) fn run(
    from_datadir: &Path,
    to_datadir: &Path,
    cfg: BodiesConfig,
    dry_run: bool,
) -> eyre::Result<()> {
    // Probe first — if this returns errors we do not touch the destination.
    let report = preflight(from_datadir, &cfg)?;
    print_report(&report);
    if !report.ok() {
        eyre::bail!(
            "bodies preflight failed ({} failures); refusing to migrate",
            report.failures.len(),
        );
    }

    if dry_run {
        tracing::info!("dry-run: preflight ok, skipping copy + checkpoint write");
        return Ok(());
    }

    // 1. Copy transactions static-file segments. (Headers assumed already
    //    migrated by a prior `static-headers` run; we don't re-copy them.)
    copy_transactions_segments(from_datadir, to_datadir, cfg.to_block)?;

    // 2. Copy the MDBX companion index tables verbatim. Encoding of these
    //    (u64/B256 keys, simple Compact-encoded values made of u64 / Vec<
    //    Header or Withdrawal>) is stable between v1 and v2.
    copy_mdbx_body_tables(from_datadir, to_datadir, cfg.to_block)?;

    // 3. Stamp StageCheckpoints. If --to-block was set, cap the tip we stamp
    //    so the bodies checkpoint matches the data we actually copied.
    let tip_block = match cfg.to_block {
        Some(cap) => report.source_block_range.1.min(cap),
        None => report.source_block_range.1,
    };
    stamp_checkpoints(to_datadir, tip_block, cfg.phase)?;

    // 4. Post-copy: re-run the probe against the DESTINATION. If this fails,
    //    something corrupted during the copy. Loud failure here is an early
    //    signal — better to catch it before the user spends 50h on Execution.
    tracing::info!("post-copy verification");
    let post = preflight(to_datadir, &cfg)?;
    print_report(&post);
    if !post.ok() {
        eyre::bail!(
            "post-copy verification failed ({} failures); DB is NOT trustworthy",
            post.failures.len(),
        );
    }

    Ok(())
}

/// Run the two-gate probe against a datadir.
fn preflight(datadir: &Path, cfg: &BodiesConfig) -> eyre::Result<PreflightReport> {
    let static_dir = resolve_static_files_dir(datadir);
    let db_dir = resolve_db_dir(datadir);

    // Source MDBX (read-only) — for BlockBodyIndices.
    let mdbx = DatabaseEnv::open(
        &db_dir,
        DatabaseEnvKind::RO,
        DatabaseArguments::new(Default::default()),
    )?;
    let mdbx_tx = mdbx.tx()?;

    // Source static files via v2's StaticFileProvider. Parameterized on
    // BscPrimitives because that's what decodes our TransactionSigned +
    // Header rows.
    let sfp = StaticFileProvider::<BscPrimitives>::read_only(&static_dir)
        .map_err(|e| eyre::eyre!("open static_files at {}: {e}", static_dir.display()))?;

    // Discover the block + tx ranges covered by the source.
    let (first_block, last_block) = discover_block_range(&sfp)?;
    let (first_tx, last_tx) = discover_tx_range(&sfp)?;

    tracing::info!(
        first_block,
        last_block,
        first_tx,
        last_tx,
        decode_samples = cfg.decode_samples,
        root_samples = cfg.root_samples,
        "preflight: opened source"
    );

    let mut failures = Vec::<String>::new();

    // Gate 1: Decode probe. Pick tx numbers deterministically spread across
    // [first_tx, last_tx]. Any failure here is fatal — don't even bother
    // with the root gate.
    let mut decode_checked = 0usize;
    let span = last_tx.saturating_sub(first_tx).max(1);
    for i in 0..cfg.decode_samples {
        let tx_num = first_tx + (span * i as u64) / cfg.decode_samples as u64;
        match sfp.transaction_by_id(tx_num) {
            Ok(Some(tx)) => {
                if tx.hash() == &B256::ZERO {
                    failures.push(format!("tx_num {tx_num}: zero tx_hash after decode"));
                }
                decode_checked += 1;
            }
            Ok(None) => {
                failures.push(format!("tx_num {tx_num}: not found in static files"));
            }
            Err(e) => {
                failures.push(format!("tx_num {tx_num}: decode error: {e}"));
                // Bail early on the first decode failure — if one row is
                // broken we can't trust the rest without per-row verification
                // which is impractical on 90M rows.
                break;
            }
        }
    }
    if !failures.is_empty() {
        return Ok(PreflightReport {
            source_block_range: (first_block, last_block),
            source_tx_range: (first_tx, last_tx),
            decode_samples_checked: decode_checked,
            root_samples_checked: 0,
            failures,
        });
    }

    // Gate 2: Root round-trip probe. Much more expensive per sample (reads
    // tx_count rows + rebuilds the Merkle root per block) so we do fewer.
    let block_span = last_block.saturating_sub(first_block).max(1);
    let mut root_checked = 0usize;
    for i in 0..cfg.root_samples {
        let block = first_block + (block_span * i as u64) / cfg.root_samples as u64;

        let indices = match mdbx_tx.get::<tables::BlockBodyIndices>(block) {
            Ok(Some(b)) => b,
            Ok(None) => {
                failures.push(format!("block {block}: missing BlockBodyIndices"));
                continue;
            }
            Err(e) => {
                failures.push(format!("block {block}: MDBX read error: {e}"));
                continue;
            }
        };

        let header = match sfp.header_by_number(block) {
            Ok(Some(h)) => h,
            Ok(None) => {
                failures.push(format!("block {block}: header missing in static files"));
                continue;
            }
            Err(e) => {
                failures.push(format!("block {block}: header read error: {e}"));
                continue;
            }
        };

        let mut txs = Vec::with_capacity(indices.tx_count as usize);
        let mut load_ok = true;
        for tx_num in indices.first_tx_num..indices.first_tx_num + indices.tx_count {
            match sfp.transaction_by_id(tx_num) {
                Ok(Some(tx)) => txs.push(tx),
                Ok(None) => {
                    failures.push(format!(
                        "block {block}: tx_num {tx_num} (in range) missing in static files"
                    ));
                    load_ok = false;
                    break;
                }
                Err(e) => {
                    failures.push(format!("block {block}: tx_num {tx_num} decode error: {e}"));
                    load_ok = false;
                    break;
                }
            }
        }
        if !load_ok {
            continue;
        }

        let expected_root = header.transactions_root();
        let computed_root = calculate_transaction_root(&txs);
        if expected_root != computed_root {
            failures.push(format!(
                "block {block}: transactions_root mismatch (expected {expected_root:?}, \
                 computed {computed_root:?}, tx_count={})",
                indices.tx_count,
            ));
        }
        root_checked += 1;
    }

    Ok(PreflightReport {
        source_block_range: (first_block, last_block),
        source_tx_range: (first_tx, last_tx),
        decode_samples_checked: decode_checked,
        root_samples_checked: root_checked,
        failures,
    })
}

fn print_report(r: &PreflightReport) {
    tracing::info!(
        block_range = ?r.source_block_range,
        tx_range = ?r.source_tx_range,
        decode_samples = r.decode_samples_checked,
        root_samples = r.root_samples_checked,
        failures = r.failures.len(),
        "preflight report",
    );
    for f in &r.failures {
        tracing::error!(failure = %f);
    }
}

/// Copy the transactions static-file segments from source to dest. Uses the
/// same triplet-copy path as headers, filtered to kind = "transactions" and
/// (optionally) capped at `to_block`.
fn copy_transactions_segments(
    from_datadir: &Path,
    to_datadir: &Path,
    to_block: Option<BlockNumber>,
) -> eyre::Result<()> {
    let t0 = Instant::now();
    let segments =
        crate::static_copy::discover_source_segments(from_datadir, Phase::StaticBodies)?;
    let mut tx_segments: Vec<_> =
        segments.into_iter().filter(|s| s.kind == "transactions").collect();
    if let Some(cap) = to_block {
        tx_segments.retain(|s| s.end <= cap);
    }
    eyre::ensure!(
        !tx_segments.is_empty(),
        "no transactions segments found under {} (post --to-block filter)",
        from_datadir.display(),
    );
    let report = crate::static_copy::copy_segments(to_datadir, &tx_segments)?;
    tracing::info!(
        bytes_copied = report.bytes_copied,
        segments = tx_segments.len(),
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "transactions static-file segments copied",
    );
    Ok(())
}

/// MDBX-copy the bodies-related index tables from source to dest. Each table
/// is walked with a cursor on the source and written via `tx.put` on the
/// dest. Commits batch every N rows to bound memory. Optional `to_block` cap
/// stops the walk when keys (or values, for `TransactionBlocks`) exceed it.
fn copy_mdbx_body_tables(
    from_datadir: &Path,
    to_datadir: &Path,
    to_block: Option<BlockNumber>,
) -> eyre::Result<()> {
    let src_db = DatabaseEnv::open(
        &resolve_db_dir(from_datadir),
        DatabaseEnvKind::RO,
        DatabaseArguments::new(Default::default()),
    )?;
    let dst_db = reth_db::init_db(
        resolve_db_dir(to_datadir),
        DatabaseArguments::new(Default::default()),
    )
    .map_err(|e| eyre::eyre!("init dest db: {e}"))?;

    // Block-keyed tables: stop once the key exceeds the cap.
    copy_table_block_keyed::<tables::BlockBodyIndices>(
        &src_db,
        &dst_db,
        "BlockBodyIndices",
        to_block,
    )?;
    // BlockOmmers and BlockWithdrawals are typically sparse on BSC (no
    // ommers; withdrawals only post-Shanghai) but the stage expects them
    // present for blocks that have them. Copy verbatim.
    copy_table_block_keyed::<tables::BlockOmmers>(&src_db, &dst_db, "BlockOmmers", to_block)?;
    copy_table_block_keyed::<tables::BlockWithdrawals>(
        &src_db,
        &dst_db,
        "BlockWithdrawals",
        to_block,
    )?;
    // TransactionBlocks: key = TxNumber, value = BlockNumber. Cap on value.
    copy_transaction_blocks(&src_db, &dst_db, to_block)?;

    // TransactionSenders: key = TxNumber, value = Address. Cap on key — need
    // to translate the block cap to a tx cap via BlockBodyIndices[cap_block].
    // This produces the data that SenderRecovery would otherwise spend time
    // re-deriving via ECDSA from every transaction signature.
    let tx_cap_exclusive = match to_block {
        Some(b) => Some(tx_number_upper_exclusive_for_block(&src_db, b)?),
        None => None,
    };
    copy_transaction_senders(&src_db, &dst_db, tx_cap_exclusive)?;

    Ok(())
}

/// Given a block number cap, return `first_tx_num + tx_count` for that block
/// — i.e. the exclusive upper bound on `TxNumber`s we want to keep. Used to
/// cap `TransactionSenders` walks. Handles empty blocks (tx_count = 0)
/// naturally: the return value equals the next block's first_tx_num, so
/// a strict `<` comparison keeps nothing past the cap.
fn tx_number_upper_exclusive_for_block(
    src: &DatabaseEnv,
    cap_block: BlockNumber,
) -> eyre::Result<TxNumber> {
    let tx = src.tx()?;
    let indices = tx
        .get::<tables::BlockBodyIndices>(cap_block)?
        .ok_or_else(|| eyre::eyre!("cap block {cap_block} has no BlockBodyIndices on source"))?;
    Ok(indices.first_tx_num + indices.tx_count)
}

/// Copy a table whose key is a `BlockNumber`. Stops when the key exceeds
/// `cap` (if provided). Uses `cursor.append()` — destination must start
/// empty for this table.
fn copy_table_block_keyed<T: reth_db::table::Table<Key = BlockNumber>>(
    src: &DatabaseEnv,
    dst: &DatabaseEnv,
    label: &str,
    cap: Option<BlockNumber>,
) -> eyre::Result<()>
where
    T::Value: Clone,
{
    let t0 = Instant::now();
    let mut dst_tx = dst.tx_mut()?;
    let mut dst_cursor = dst_tx.cursor_write::<T>()?;
    let mut rows: u64 = 0;
    let mut in_batch: usize = 0;
    let mut rows_since_refresh: u64 = 0;
    let mut resume_from: Option<BlockNumber> = None;

    'outer: loop {
        let src_tx = src.tx()?;
        let mut cursor = src_tx.cursor_read::<T>()?;
        let mut entry = match resume_from {
            None => cursor.first()?,
            Some(k) => cursor.seek(k)?,
        };
        while let Some((k, v)) = entry {
            if cap.map_or(false, |c| k > c) {
                break 'outer;
            }
            dst_cursor.append(k, &v)?;
            rows += 1;
            in_batch += 1;
            rows_since_refresh += 1;
            if in_batch >= WRITE_BATCH {
                drop(dst_cursor);
                dst_tx.commit()?;
                dst_tx = dst.tx_mut()?;
                dst_cursor = dst_tx.cursor_write::<T>()?;
                in_batch = 0;
                tracing::info!(
                    table = label,
                    rows,
                    elapsed_secs = t0.elapsed().as_secs_f64(),
                    rate_per_sec = (rows as f64 / t0.elapsed().as_secs_f64()) as u64,
                    "progress",
                );
            }
            if rows_since_refresh >= READ_REFRESH_ROWS {
                resume_from = Some(k + 1);
                rows_since_refresh = 0;
                continue 'outer;
            }
            entry = cursor.next()?;
        }
        break;
    }

    drop(dst_cursor);
    dst_tx.commit()?;
    tracing::info!(
        table = label,
        rows,
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "copied",
    );
    Ok(())
}

/// How many rows to copy inside a single source read transaction before
/// releasing it and starting a new one. libmdbx kills read transactions
/// older than ~5 minutes (to stop them pinning the GC), so for tables with
/// billions of rows we MUST refresh.
const READ_REFRESH_ROWS: u64 = 10_000_000;

/// Rows per destination write transaction commit. Larger = fewer commit
/// round-trips = higher throughput. Each write tx accumulates dirty pages
/// until commit; 1M rows × ~40 B/row = ~40 MB dirty data per batch, well
/// under MDBX's soft limits and easily held in RAM.
const WRITE_BATCH: usize = 1_000_000;

/// `TransactionBlocks` is keyed by `TxNumber` with value `BlockNumber`.
/// We stop when the *value* (block) exceeds `cap`. Since the table is
/// monotonic in tx_num — and each block's tx range is contiguous — once a
/// row's block exceeds the cap, no later row can be in range either.
///
/// Uses `cursor.append()` on the destination (MDBX `WriteFlags::APPEND`) —
/// the bulk-load fast path for strictly-increasing keys. Skips the full
/// btree traversal on every insert and appends straight to the rightmost
/// leaf. A 5-10× speedup over plain `tx.put()` on ordered input.
///
/// Destination MUST start empty for this table (wipe + re-init between runs);
/// `append` fails if a key isn't strictly greater than the last inserted key.
fn copy_transaction_blocks(
    src: &DatabaseEnv,
    dst: &DatabaseEnv,
    cap: Option<BlockNumber>,
) -> eyre::Result<()> {
    let label = "TransactionBlocks";
    let t0 = Instant::now();
    let mut dst_tx = dst.tx_mut()?;
    let mut dst_cursor = dst_tx.cursor_write::<tables::TransactionBlocks>()?;
    let mut rows: u64 = 0;
    let mut in_batch: usize = 0;
    let mut rows_since_refresh: u64 = 0;
    let mut resume_from: Option<TxNumber> = None;

    'outer: loop {
        let src_tx = src.tx()?;
        let mut cursor = src_tx.cursor_read::<tables::TransactionBlocks>()?;
        let mut entry = match resume_from {
            None => cursor.first()?,
            Some(k) => cursor.seek(k)?,
        };

        while let Some((k, v)) = entry {
            if cap.map_or(false, |c| v > c) {
                break 'outer;
            }
            dst_cursor.append(k, &v)?;
            rows += 1;
            in_batch += 1;
            rows_since_refresh += 1;
            if in_batch >= WRITE_BATCH {
                drop(dst_cursor);
                dst_tx.commit()?;
                dst_tx = dst.tx_mut()?;
                dst_cursor = dst_tx.cursor_write::<tables::TransactionBlocks>()?;
                in_batch = 0;
                tracing::info!(
                    table = label,
                    rows,
                    elapsed_secs = t0.elapsed().as_secs_f64(),
                    rate_per_sec = (rows as f64 / t0.elapsed().as_secs_f64()) as u64,
                    "progress",
                );
            }
            if rows_since_refresh >= READ_REFRESH_ROWS {
                resume_from = Some(k + 1);
                rows_since_refresh = 0;
                continue 'outer;
            }
            entry = cursor.next()?;
        }
        break;
    }

    drop(dst_cursor);
    dst_tx.commit()?;
    tracing::info!(
        table = label,
        rows,
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "copied",
    );
    Ok(())
}

/// `TransactionSenders` is keyed by `TxNumber` with value `Address`. We stop
/// when the key exceeds `tx_cap_exclusive` (if set). Assumption: the v1
/// source snapshot stores senders in this MDBX table — a v2 snapshot would
/// instead put them in `static_file_transaction_senders_*` segments and this
/// phase would be a no-op (or need a different copy path). The destination
/// must also be initialized as v1 (`reth-bsc init --storage.v2 false`);
/// otherwise reth 2.0 ignores MDBX `TransactionSenders` on a v2 datadir.
///
/// Uses `cursor.append()` + periodic read-tx refresh, same fast-path pattern
/// as `copy_transaction_blocks`. Destination must start empty for this table.
fn copy_transaction_senders(
    src: &DatabaseEnv,
    dst: &DatabaseEnv,
    tx_cap_exclusive: Option<TxNumber>,
) -> eyre::Result<()> {
    let label = "TransactionSenders";
    let t0 = Instant::now();
    let mut dst_tx = dst.tx_mut()?;
    let mut dst_cursor = dst_tx.cursor_write::<tables::TransactionSenders>()?;
    let mut rows: u64 = 0;
    let mut in_batch: usize = 0;
    let mut rows_since_refresh: u64 = 0;
    let mut resume_from: Option<TxNumber> = None;

    'outer: loop {
        let src_tx = src.tx()?;
        let mut cursor = src_tx.cursor_read::<tables::TransactionSenders>()?;
        let mut entry = match resume_from {
            None => cursor.first()?,
            Some(k) => cursor.seek(k)?,
        };

        while let Some((k, v)) = entry {
            if tx_cap_exclusive.map_or(false, |c| k >= c) {
                break 'outer;
            }
            dst_cursor.append(k, &v)?;
            rows += 1;
            in_batch += 1;
            rows_since_refresh += 1;
            if in_batch >= WRITE_BATCH {
                drop(dst_cursor);
                dst_tx.commit()?;
                dst_tx = dst.tx_mut()?;
                dst_cursor = dst_tx.cursor_write::<tables::TransactionSenders>()?;
                in_batch = 0;
                tracing::info!(
                    table = label,
                    rows,
                    elapsed_secs = t0.elapsed().as_secs_f64(),
                    rate_per_sec = (rows as f64 / t0.elapsed().as_secs_f64()) as u64,
                    "progress",
                );
            }
            if rows_since_refresh >= READ_REFRESH_ROWS {
                resume_from = Some(k + 1);
                rows_since_refresh = 0;
                continue 'outer;
            }
            entry = cursor.next()?;
        }
        break;
    }

    drop(dst_cursor);
    dst_tx.commit()?;
    tracing::info!(
        table = label,
        rows,
        elapsed_secs = t0.elapsed().as_secs_f64(),
        "copied",
    );
    Ok(())
}

/// Stamp `StageCheckpoints` for every stage whose data the bodies phase
/// migrated. `TransactionLookup` is still left unset — that's stage 11 and
/// we haven't wired `TransactionHashNumbers` migration yet.
fn stamp_checkpoints(to_datadir: &Path, tip_block: BlockNumber, _phase: Phase) -> eyre::Result<()> {
    use reth_stages_types::{StageCheckpoint, StageId};

    let db = reth_db::init_db(
        resolve_db_dir(to_datadir),
        DatabaseArguments::new(Default::default()),
    )
    .map_err(|e| eyre::eyre!("init dest db for checkpoint stamp: {e}"))?;
    let tx = db.tx_mut()?;
    let cp = StageCheckpoint::new(tip_block);
    tx.put::<tables::StageCheckpoints>(StageId::Bodies.to_string(), cp)?;
    tx.put::<tables::StageCheckpoints>(StageId::SenderRecovery.to_string(), cp)?;
    tx.commit()?;
    tracing::info!(
        stages = "Bodies, SenderRecovery",
        tip_block,
        "stage checkpoints stamped; TransactionLookup left for the node",
    );
    Ok(())
}

/// Discover the block range via StaticFileProvider. We query the headers
/// segment metadata by asking for header at block 0 and walking the tip
/// from the provider's internal segment registry.
fn discover_block_range(
    sfp: &StaticFileProvider<BscPrimitives>,
) -> eyre::Result<(BlockNumber, BlockNumber)> {
    // Block 0 must exist if headers were migrated.
    let _genesis = sfp
        .header_by_number(0)?
        .ok_or_else(|| eyre::eyre!("no header at block 0 in source static_files"))?;
    // `StaticFileProvider` Derefs to `StaticFileProviderInner`, which owns
    // the segment registry — so we call the method directly on `sfp`.
    let tip = sfp
        .get_highest_static_file_block(reth_static_file_types::StaticFileSegment::Headers)
        .ok_or_else(|| eyre::eyre!("no headers segments registered"))?;
    Ok((0, tip))
}

fn discover_tx_range(
    sfp: &StaticFileProvider<BscPrimitives>,
) -> eyre::Result<(TxNumber, TxNumber)> {
    // Ask for tx_id 0. If static_file_transactions segments exist, this
    // resolves to the first tx.
    let _zero = sfp
        .transaction_by_id(0)?
        .ok_or_else(|| eyre::eyre!("no transaction at tx_id 0 in source static_files"))?;
    let tip_tx = sfp
        .get_highest_static_file_tx(reth_static_file_types::StaticFileSegment::Transactions)
        .ok_or_else(|| eyre::eyre!("no transactions segments registered"))?;
    Ok((0, tip_tx))
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
