use reth_db::table::{Table, TableInfo};
use reth_db::tables::TableSet;
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

/// Object-safe table-info wrapper for BSC's custom Parlia tables.
///
/// reth 2.0's [`reth_db::tables::TableSet`] yields `Box<dyn TableInfo>`; the
/// upstream `tables!` macro generates these automatically via an enum, but we
/// add our two custom tables by hand and expose them via [`ParliaTables`] so
/// callers can register them on top of the default schema via
/// `DatabaseEnv::create_tables_for::<ParliaTables>()`.
#[derive(Debug, Clone, Copy)]
struct ParliaTableInfo {
    name: &'static str,
}

impl TableInfo for ParliaTableInfo {
    fn name(&self) -> &'static str {
        self.name
    }

    fn is_dupsort(&self) -> bool {
        false
    }
}

/// `TableSet` carrying BSC's Parlia tables so they can be registered against
/// an already-opened `DatabaseEnv` (e.g. in tests that use upstream `init_db`).
/// Production callers should add this after `init_db` via
/// `DatabaseEnv::create_tables_for::<ParliaTables>()`.
#[derive(Debug, Default)]
pub struct ParliaTables;

impl TableSet for ParliaTables {
    fn tables() -> Box<dyn Iterator<Item = Box<dyn TableInfo>>> {
        Box::new(
            [
                Box::new(ParliaTableInfo { name: ParliaSnapshots::NAME }) as Box<dyn TableInfo>,
                Box::new(ParliaTableInfo { name: ParliaSnapshotsByHash::NAME })
                    as Box<dyn TableInfo>,
            ]
            .into_iter(),
        )
    }
}