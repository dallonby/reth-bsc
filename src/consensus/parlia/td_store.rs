//! Local total-difficulty accumulator.
//!
//! reth 2.0 dropped `HeaderProvider::header_td*` / `ConsensusEngineHandle::query_td`
//! / `PeerInfo.best_td` / `NewBlockMessage.td` post-merge — there's no TD
//! anywhere on mainnet-ethereum paths. BSC still uses TD for Parlia
//! fork-choice (comparing incoming vs canonical) and for the vote-broadcast
//! delta-TD gating in the bsc/2 protocol, so we need to keep our own
//! accumulator. This module is the store: `BscHeaderTd` is the on-disk MDBX
//! table, [`TdStore`] is the process-wide accessor that seeds genesis on
//! first init and writes TD for each validated header.
//!
//! Write path (consensus / block-import): every accepted block computes
//! `td(block) = td(parent) + block.difficulty` and persists it.
//!
//! Read path (fork-choice, miner broadcast, vote-gating): look up by hash.
//! Side-chains get populated as their headers are validated, mirroring how
//! Parlia handles them pre-port (they ride through the same validation API).
use super::db::{BscHeaderTd, StoredTd};
use alloy_consensus::BlockHeader;
use alloy_primitives::{BlockHash, U256};
use reth_db::cursor::DbCursorRO;
use reth_db::transaction::{DbTx, DbTxMut};
use reth_db::{Database, DatabaseEnv};
use std::sync::OnceLock;

static TD_STORE: OnceLock<TdStore> = OnceLock::new();

/// Process-wide handle to the TD accumulator. Lightweight: holds a cloned
/// `DatabaseEnv` (which is internally an `Arc<Environment>`).
pub struct TdStore {
    db: DatabaseEnv,
}

impl TdStore {
    /// Register the global store. Call once from main after `ensure_parlia_tables`.
    /// Idempotent — later calls are silently ignored (first initialization wins).
    pub fn init(db: DatabaseEnv) {
        let _ = TD_STORE.set(TdStore { db });
    }

    /// Read a TD by block hash, returning `None` if unknown. Unknown is
    /// treated as "TD gating disabled for this peer/header" by callers.
    pub fn get(hash: &BlockHash) -> Option<U256> {
        let store = TD_STORE.get()?;
        let tx = store.db.tx().ok()?;
        tx.get::<BscHeaderTd>(*hash).ok().flatten().map(|s| s.0)
    }

    /// Convenience: look up canonical TD at a block number by iterating the
    /// table. Used by miner broadcast where the caller only has the number.
    /// Linear scan over (number, hash) pairs is fine for occasional calls —
    /// callers that know the hash should use [`TdStore::get`] instead.
    pub fn best_by_number(number: u64) -> Option<U256> {
        let _ = TD_STORE.get()?;
        let header = crate::shared::get_canonical_header_by_number(number)?;
        Self::get(&header.hash_slow())
    }

    /// Persist TD for a hash. Called by consensus after
    /// `validate_header_against_parent` for the child header. Idempotent —
    /// overwriting with the same value is a no-op; mismatching writes (rare
    /// reorg re-validation) just refresh the value.
    pub fn put(hash: BlockHash, td: U256) {
        let Some(store) = TD_STORE.get() else { return };
        if let Ok(tx) = store.db.tx_mut() {
            let _ = tx.put::<BscHeaderTd>(hash, StoredTd(td));
            let _ = tx.commit();
        }
    }

    /// Ensure genesis TD is seeded. Genesis has no parent, so
    /// `td(genesis) = genesis.difficulty` (0 on BSC mainnet but we source it
    /// from the canonical header anyway to stay generic). Call once at
    /// startup after [`TdStore::init`].
    pub fn seed_genesis() {
        let Some(store) = TD_STORE.get() else { return };
        let Ok(tx) = store.db.tx() else { return };
        // Walk cursor to first entry; if any TD is stored, assume genesis
        // was already seeded on a previous run.
        if let Ok(mut cursor) = tx.cursor_read::<BscHeaderTd>() {
            if let Ok(Some(_)) = cursor.first() {
                return;
            }
        }
        drop(tx);
        let Some(genesis) = crate::shared::get_canonical_header_by_number(0) else {
            return;
        };
        let td = U256::from(genesis.difficulty);
        Self::put(genesis.hash_slow(), td);
    }
}

/// Write td(child) = td(parent) + child.difficulty. Silently no-ops when
/// td(parent) is unknown so stale or side-chain validations don't fail —
/// the read path just returns None for those hashes, which matches how
/// vote-broadcast and fork-choice treat the stubbed case.
pub fn record_child<H: BlockHeader>(child_hash: BlockHash, child: &H, parent_hash: BlockHash) {
    let Some(parent_td) = TdStore::get(&parent_hash) else {
        return;
    };
    let td = parent_td + child.difficulty();
    TdStore::put(child_hash, td);
}
