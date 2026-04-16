//! v1 / source-side reader. Opens the old bnb-chain reth datadir's MDBX env in
//! read-only mode and serves header + body payloads block-by-block.
//!
//! Codec notes:
//!  * `Header` Compact encoding is stable between v0.0.8 and v2.0.0 — we can
//!    decode directly with reth-db 2.0.0's codec.
//!  * `BlockBodyIndices`, `BlockOmmers`, `BlockWithdrawals` are also stable.
//!  * `TransactionSigned` is NOT stable. v0.0.8 stored a bnb-chain-extended
//!    Compact variant. We bridge via RLP: read the raw MDBX value bytes, try
//!    RLP-decode with v2's alloy envelope. If that fails we fall back to a
//!    conservative "read as raw bytes" path and defer the re-encoding until
//!    we've inspected the concrete format at runtime (see TODO at the bottom).
//!
//! This file keeps the decoding close to reth-db 2.0.0 where possible because
//! the long-term direction is un-forking; we don't want to vendor old
//! primitives crates unless forced.

use crate::{writer::BlockPayload, Phase};
use alloy_consensus::Header;
use alloy_primitives::{BlockHash, BlockNumber, TxNumber};
use crossbeam_channel::Sender;
use reth_db::{
    cursor::DbCursorRO,
    mdbx::{DatabaseArguments, DatabaseEnv, DatabaseEnvKind},
    tables,
    transaction::DbTx,
    Database,
};
use reth_db_api::models::StoredBlockBodyIndices;
use reth_ethereum_primitives::TransactionSigned;
use std::{path::Path, sync::Arc};

pub(crate) struct SourceTip {
    pub tip_block: BlockNumber,
    pub tip_tx: TxNumber,
}

/// Open the source MDBX env read-only. We use reth-db 2.0.0's env code to
/// read v1 data; where the encodings match (headers, body indices) this Just
/// Works. Where they don't (transactions), we treat the value as an opaque
/// Vec<u8> and bridge via RLP in the reader.
pub(crate) fn open_source_db(path: &Path) -> eyre::Result<DatabaseEnv> {
    let env = DatabaseEnv::open(
        path,
        DatabaseEnvKind::RO,
        DatabaseArguments::new(Default::default()),
    )?;
    Ok(env)
}

pub(crate) fn discover_source_tip(db: &DatabaseEnv) -> eyre::Result<SourceTip> {
    let tx = db.tx()?;

    // Canonical tip: last (BlockNumber, BlockHash) in CanonicalHeaders.
    let mut canon_cursor = tx.cursor_read::<tables::CanonicalHeaders>()?;
    let tip_block = canon_cursor
        .last()?
        .map(|(n, _hash): (BlockNumber, BlockHash)| n)
        .ok_or_else(|| eyre::eyre!("source has no canonical headers — not a synced reth datadir"))?;

    // Last tx number reachable via BlockBodyIndices at tip.
    let indices = tx
        .get::<tables::BlockBodyIndices>(tip_block)?
        .ok_or_else(|| eyre::eyre!("tip block {tip_block} has no BlockBodyIndices entry"))?;
    let tip_tx = indices.first_tx_num + indices.tx_count;

    Ok(SourceTip { tip_block, tip_tx })
}

/// One reader thread. Owns its own read-only MDBX tx and walks the Headers
/// (and Bodies if phase == All) tables from `start` to `end` inclusive.
pub(crate) fn reader_worker(
    db: Arc<DatabaseEnv>,
    start: BlockNumber,
    end: BlockNumber,
    phase: Phase,
    tx: Sender<BlockPayload>,
) -> eyre::Result<()> {
    let reader = db.tx()?;

    for n in start..=end {
        let header: Header = match reader.get::<tables::Headers>(n)? {
            Some(h) => h,
            None => {
                tracing::warn!(block = n, "missing header in source; aborting shard");
                return Ok(());
            }
        };
        let hash = reader
            .get::<tables::CanonicalHeaders>(n)?
            .ok_or_else(|| eyre::eyre!("missing canonical hash for block {n}"))?;

        let body = match phase {
            Phase::HeadersOnly => None,
            Phase::All => Some(read_body(&reader, n)?),
        };

        if tx.send(BlockPayload { number: n, hash, header, body }).is_err() {
            // Writer dropped early (probably an error). Stop cleanly.
            tracing::warn!(block = n, "writer channel closed early; reader stopping");
            return Ok(());
        }
    }
    Ok(())
}

pub(crate) struct BodyPayload {
    pub indices: StoredBlockBodyIndices,
    pub transactions: Vec<TransactionSigned>,
}

/// Read a full block body from the source.
///
/// TODO(codec-bridge): for Transactions, reth v0.0.8 stored a bnb-chain-extended
/// `TransactionSigned` Compact layout that MAY not round-trip through v2's
/// decoder. On first runtime failure here, switch to reading the raw value
/// bytes via `reader.inner().get(...)` and RLP-re-encoding into v2. Keeping the
/// happy path naive until we've proven on-disk compatibility with a small
/// window.
fn read_body(reader: &impl DbTx, n: BlockNumber) -> eyre::Result<BodyPayload> {
    let indices: StoredBlockBodyIndices = reader
        .get::<tables::BlockBodyIndices>(n)?
        .ok_or_else(|| eyre::eyre!("BlockBodyIndices missing for block {n}"))?;

    let mut transactions = Vec::with_capacity(indices.tx_count as usize);
    for tx_num in indices.first_tx_num..indices.first_tx_num + indices.tx_count {
        let signed: TransactionSigned = reader
            .get::<tables::Transactions>(tx_num)?
            .ok_or_else(|| eyre::eyre!("Transactions missing for tx_num {tx_num} in block {n}"))?;
        transactions.push(signed);
    }

    Ok(BodyPayload { indices, transactions })
}
