use reth_db::table::Table;
use alloy_primitives::BlockHash;

use super::snapshot::Snapshot;

// NOTE: In bnb-chain's vendored reth fork, `reth_db::models::ParliaSnapshotBlob`
// was defined in the reth-db crate itself. Paradigm's upstream reth-db has no
// such type, so the Snapshot value is stored directly: `Snapshot` already
// impls `Compress`/`Decompress` with `Compressed = Vec<u8>`, which matches the
// on-disk representation used by the bnb-chain fork.

/// Table: epoch boundary block number (u64) -> compressed snapshot bytes.
#[derive(Debug)]
pub struct ParliaSnapshots;

impl Table for ParliaSnapshots {
    const NAME: &'static str = "ParliaSnapshots";
    const DUPSORT: bool = false;
    type Key = u64;
    type Value = Snapshot;
}

/// Table: epoch boundary block hash (BlockHash) -> compressed snapshot bytes.
#[derive(Debug)]
pub struct ParliaSnapshotsByHash;

impl Table for ParliaSnapshotsByHash {
    const NAME: &'static str = "ParliaSnapshotsByHash";
    const DUPSORT: bool = false;
    type Key = BlockHash;
    type Value = Snapshot;
}