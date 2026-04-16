use reth_codecs::{Compact, DecompressError};
use reth_db::table::{Compress, Decompress, Table, TableInfo};
use reth_db::tables::TableSet;
use reth_db::{DatabaseEnv, DatabaseError};
use alloy_primitives::{BlockHash, U256};

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

/// Newtype around [`U256`] so we can implement [`Compress`] / [`Decompress`]
/// for it at the table-value layer without orphan-impl pain. Fixed 32-byte
/// big-endian encoding — no need for the variable-length Compact scheme since
/// TD is always 32 bytes anyway. Serde derive is required by reth_db's `Value`
/// bound but never actually invoked on disk (Compress uses to_be_bytes).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Serialize, serde::Deserialize)]
pub struct StoredTd(pub U256);

impl From<U256> for StoredTd {
    fn from(value: U256) -> Self {
        Self(value)
    }
}

impl From<StoredTd> for U256 {
    fn from(value: StoredTd) -> Self {
        value.0
    }
}

impl Compress for StoredTd {
    type Compressed = Vec<u8>;

    fn compress(self) -> Self::Compressed {
        self.0.to_be_bytes::<32>().to_vec()
    }

    fn compress_to_buf<B: bytes::BufMut + AsMut<[u8]>>(&self, buf: &mut B) {
        let bytes = self.0.to_be_bytes::<32>();
        buf.put_slice(&bytes);
    }
}

impl Decompress for StoredTd {
    fn decompress(value: &[u8]) -> Result<Self, DecompressError> {
        if value.len() != 32 {
            return Err(DecompressError::new(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("StoredTd expects 32 bytes, got {}", value.len()),
            )));
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(value);
        Ok(Self(U256::from_be_bytes(arr)))
    }
}

// Compact impl so StoredTd satisfies the `Value` bound reth_db places on
// table values. 32-byte fixed layout means we don't benefit from Compact,
// but the trait is required.
impl Compact for StoredTd {
    fn to_compact<B: bytes::BufMut + AsMut<[u8]>>(&self, buf: &mut B) -> usize {
        let bytes = self.0.to_be_bytes::<32>();
        buf.put_slice(&bytes);
        32
    }

    fn from_compact(buf: &[u8], _len: usize) -> (Self, &[u8]) {
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&buf[..32]);
        (Self(U256::from_be_bytes(arr)), &buf[32..])
    }
}

/// Table: block hash -> total difficulty.
///
/// BSC keeps using total difficulty for Parlia fork-choice and vote-broadcast
/// gating even though reth 2.0 dropped `HeaderProvider::header_td*` post-merge.
/// We maintain our own accumulator here. Keyed by hash (not number) so the
/// canonical-vs-side-chain distinction is handled trivially — two chains can
/// carry the same block number but never the same hash.
#[derive(Debug)]
pub struct BscHeaderTd;

impl Table for BscHeaderTd {
    const NAME: &'static str = "BscHeaderTd";
    const DUPSORT: bool = false;
    type Key = BlockHash;
    type Value = StoredTd;
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
                Box::new(ParliaTableInfo { name: BscHeaderTd::NAME }) as Box<dyn TableInfo>,
            ]
            .into_iter(),
        )
    }
}

/// Create BSC's Parlia tables on the already-opened node DB if they don't
/// already exist. Idempotent: MDBX's CREATE flag returns the existing handle
/// on the second run.
///
/// Upstream `init_db` (which runs inside `run_with_components`) only creates
/// the tables in the default `Tables` enum. We need to add ours on top before
/// the first Parlia snapshot read, otherwise MDBX returns NOTFOUND (-30798).
///
/// Take `&mut DatabaseEnv` so `create_and_track_tables_for` can both create
/// the on-disk tables and extend the cached DBI map; this is called from main
/// before the builder clones the DB Arc into components.
pub fn ensure_parlia_tables(db: &mut DatabaseEnv) -> Result<(), DatabaseError> {
    db.create_and_track_tables_for::<ParliaTables>()
}