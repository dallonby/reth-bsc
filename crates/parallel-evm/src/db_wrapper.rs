//! A [`revm::Database`] wrapper that reads through multi-version memory.
//!
//! One `DbWrapper` instance per transaction execution per worker. Each read:
//!
//! 1. Looks up the location in [`MvMemory`] using the reader's `tx_idx`.
//!    - Found as `Versioned`: return the versioned value and log the read
//!      origin for later validation.
//!    - Found as `Estimate`: surface a [`DbError::Blocked`] so the caller can
//!      park this tx on the blocker. The EVM will propagate this out through
//!      its `ExecuteEvm::Error`; the worker unpacks it.
//!    - Not found: fall through to the caller-supplied [`Storage`] and log
//!      the read as `Storage` origin.
//!
//! 2. Records every read into an owned [`ReadLogBuilder`]. When the tx
//!    finishes, `into_read_log()` yields the log for the scheduler's
//!    [`ReadSetStore`].
//!
//! # Why own the builder instead of borrowing
//!
//! Revm's `Evm` consumes the `Database` by value — it lives inside the
//! context. If the `ReadLogBuilder` was `&mut` borrowed from the worker, the
//! lifetime would imprison the whole Evm. Owning the builder inside the
//! wrapper lets revm consume us freely; after the EVM is dropped, whoever
//! extracted the `DbWrapper` back (or an `Arc`-shared reference) can finalize
//! the log.
//!
//! # Error mapping
//!
//! `DbError` is intentionally minimal — the two cases that matter for the
//! scheduler are "blocked on another tx" and "storage failure". Anything
//! else (a storage impl returning an exotic error) gets stringified.

use crate::{
    mv_memory::{MemoryLocation, MemoryValue, MvMemory, ReadOutcome, TxIdx},
    read_set::{ReadLog, ReadOrigin},
    storage::Storage,
};
use alloy_primitives::{Address, B256, U256};
use parking_lot::Mutex;
use revm::{bytecode::Bytecode, state::AccountInfo};
use std::{fmt, sync::Arc};
use thiserror::Error;

/// Error type surfaced by the wrapper's [`revm::Database`] impl.
///
/// Blocked reads are a signal to the scheduler, not a failure; everything
/// else is propagated as a fatal or tx-level error per revm's normal
/// contracts.
#[derive(Debug, Error, Clone, PartialEq)]
pub enum DbError {
    /// A read hit an `Estimate` marker: the writer at `blocking_tx_idx` is
    /// mid-re-execution and the value is not trustworthy. The caller parks
    /// on the blocker; revm surfaces this via `EVMError::Database` up to
    /// the worker loop.
    #[error("read blocked on tx {blocking_tx_idx} (mid-reexecution)")]
    Blocked {
        /// `tx_idx` of the writer we're parked on.
        blocking_tx_idx: TxIdx,
    },

    /// Underlying storage failed. The original error is stringified because
    /// [`Storage::Error`] is generic and we need `DbError` to be
    /// object-safe / simply typed.
    #[error("storage: {0}")]
    Storage(String),

    /// A value found in `MvMemory` did not match the kind expected by this
    /// read (e.g. `basic()` looked up and got a `Storage` value). This
    /// indicates an internal bug; shouldn't happen if writes are classified
    /// correctly.
    #[error("internal: location/value kind mismatch: {0}")]
    KindMismatch(&'static str),
}

// revm's Database trait requires Self::Error: DBErrorMarker — a zero-cost
// marker so the error type can be routed through the EVM's generic error
// machinery. Opting in is just an empty impl.
impl revm::database::DBErrorMarker for DbError {}

/// Shared handle to a read log. The worker holds a clone outside the EVM
/// so it can retrieve the log after the EVM (and the `DbWrapper` it
/// consumed) are dropped.
///
/// Using `Arc<Mutex<Vec<ReadLog>>>` instead of raw ownership side-steps a
/// thorny revm issue: `BscEvm` (and many other chain-specific variants)
/// don't expose an `into_inner()` to recover the consumed `Database`, so
/// owned per-wrapper state would be unreachable after the EVM is dropped.
/// The `Mutex` is single-thread contention (only the worker running this
/// tx touches it) — parking_lot handles that in tens of nanoseconds.
pub type ReadLogHandle = Arc<Mutex<Vec<ReadLog>>>;

/// Make a fresh empty handle. Workers allocate one per tx execution,
/// clone into the wrapper, then extract after `VmBuilder::transact`.
pub fn new_read_log_handle() -> ReadLogHandle {
    Arc::new(Mutex::new(Vec::new()))
}

/// Wraps the caller's `Storage` + [`MvMemory`] for a single tx execution.
///
/// Constructed fresh per tx, passed into revm by value, dropped when revm
/// is done.
pub struct DbWrapper<'a, S: Storage> {
    storage: &'a S,
    mv_memory: &'a MvMemory,
    reader_tx_idx: TxIdx,
    /// Shared handle to the read log. See [`ReadLogHandle`].
    read_log: ReadLogHandle,
}

impl<'a, S: Storage + fmt::Debug> fmt::Debug for DbWrapper<'a, S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DbWrapper")
            .field("reader_tx_idx", &self.reader_tx_idx)
            .field("reads", &self.read_log.lock().len())
            .finish()
    }
}

impl<'a, S: Storage> DbWrapper<'a, S> {
    /// Create a wrapper for transaction `reader_tx_idx`. The `read_log`
    /// handle is shared with the caller; workers call
    /// [`new_read_log_handle`] to allocate one, pass a clone here, keep
    /// the other clone to retrieve results after the EVM is dropped.
    pub fn new(
        storage: &'a S,
        mv_memory: &'a MvMemory,
        reader_tx_idx: TxIdx,
        read_log: ReadLogHandle,
    ) -> Self {
        Self {
            storage,
            mv_memory,
            reader_tx_idx,
            read_log,
        }
    }

    /// Internal helper: look up `location` through MvMemory + Storage,
    /// recording the read origin. Returns the raw `MemoryValue` for the
    /// caller (basic/storage/code-by-hash) to convert to its expected type.
    fn read_through(
        &mut self,
        location: MemoryLocation,
    ) -> Result<Option<MemoryValue>, DbError> {
        match self.mv_memory.read(&location, self.reader_tx_idx) {
            ReadOutcome::Versioned { writer, value } => {
                self.read_log.lock().push(ReadLog {
                    location,
                    origin: ReadOrigin::Versioned(writer),
                });
                Ok(Some(value))
            }
            ReadOutcome::Blocked { blocking_tx_idx } => {
                // DO NOT record a read log entry for a blocked read — the tx
                // will abort without finishing, and any recorded reads
                // wouldn't reflect the tx's real post-block behavior.
                Err(DbError::Blocked { blocking_tx_idx })
            }
            ReadOutcome::NotFound => {
                self.read_log.lock().push(ReadLog {
                    location,
                    origin: ReadOrigin::Storage,
                });
                Ok(None)
            }
        }
    }
}

impl<'a, S: Storage> revm::Database for DbWrapper<'a, S> {
    type Error = DbError;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, DbError> {
        let loc = MemoryLocation::Basic(address);
        match self.read_through(loc)? {
            Some(MemoryValue::Basic(info)) => Ok(Some(info)),
            Some(MemoryValue::SelfDestructed) => Ok(None),
            Some(MemoryValue::Storage(_)) => Err(DbError::KindMismatch(
                "basic() found Storage value in MvMemory",
            )),
            Some(MemoryValue::Code(_)) => Err(DbError::KindMismatch(
                "basic() found Code value in MvMemory",
            )),
            None => self
                .storage
                .basic(address)
                .map_err(|e| DbError::Storage(e.to_string())),
        }
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, DbError> {
        let loc = MemoryLocation::Code(code_hash);
        match self.read_through(loc)? {
            Some(MemoryValue::Code(bytecode)) => Ok(bytecode),
            Some(MemoryValue::Basic(_)) => Err(DbError::KindMismatch(
                "code_by_hash() found Basic value in MvMemory",
            )),
            Some(MemoryValue::Storage(_)) => Err(DbError::KindMismatch(
                "code_by_hash() found Storage value in MvMemory",
            )),
            Some(MemoryValue::SelfDestructed) => Err(DbError::KindMismatch(
                "code_by_hash() found SelfDestructed in MvMemory",
            )),
            None => self
                .storage
                .code_by_hash(code_hash)
                .map_err(|e| DbError::Storage(e.to_string())),
        }
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, DbError> {
        let loc = MemoryLocation::Storage(address, index);
        match self.read_through(loc)? {
            Some(MemoryValue::Storage(value)) => Ok(value),
            Some(MemoryValue::Basic(_)) => Err(DbError::KindMismatch(
                "storage() found Basic value in MvMemory",
            )),
            Some(MemoryValue::Code(_)) => Err(DbError::KindMismatch(
                "storage() found Code value in MvMemory",
            )),
            // A self-destructed account's storage reads as zero — EVM sees
            // a freshly-empty account.
            Some(MemoryValue::SelfDestructed) => Ok(U256::ZERO),
            None => self
                .storage
                .storage(address, index)
                .map_err(|e| DbError::Storage(e.to_string())),
        }
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, DbError> {
        // Block hashes are historical / immutable — no MvMemory tracking.
        // We also don't record them in the read log: they don't vary based
        // on intra-block tx writes, so validation wouldn't find them stale.
        self.storage
            .block_hash(number)
            .map_err(|e| DbError::Storage(e.to_string()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv_memory::Version;
    use alloy_primitives::{address, b256};
    use revm::Database;
    use std::collections::HashMap;
    use std::convert::Infallible;

    /// Tiny in-memory Storage for tests. Holds `HashMap`s keyed by the kind
    /// of read. `Infallible` error to keep callers simple.
    #[derive(Default, Debug)]
    struct Mem {
        basic: HashMap<Address, AccountInfo>,
        storage: HashMap<(Address, U256), U256>,
        code: HashMap<B256, Bytecode>,
        blocks: HashMap<u64, B256>,
    }

    impl Storage for Mem {
        type Error = Infallible;

        fn basic(&self, address: Address) -> Result<Option<AccountInfo>, Infallible> {
            Ok(self.basic.get(&address).cloned())
        }
        fn code_by_hash(&self, h: B256) -> Result<Bytecode, Infallible> {
            Ok(self.code.get(&h).cloned().unwrap_or_default())
        }
        fn storage(&self, a: Address, s: U256) -> Result<U256, Infallible> {
            Ok(self.storage.get(&(a, s)).copied().unwrap_or(U256::ZERO))
        }
        fn block_hash(&self, n: u64) -> Result<B256, Infallible> {
            Ok(self.blocks.get(&n).copied().unwrap_or_default())
        }
    }

    fn info(balance: u128, nonce: u64) -> AccountInfo {
        AccountInfo {
            balance: U256::from(balance),
            nonce,
            code_hash: b256!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            account_id: None,
            code: None,
        }
    }

    const ADDR_A: Address = address!("0x000000000000000000000000000000000000000a");
    const ADDR_B: Address = address!("0x000000000000000000000000000000000000000b");

    /// Test-helper: build a DbWrapper and its shared read-log handle.
    fn make_db<'a>(
        mem: &'a Mem,
        mv: &'a MvMemory,
        tx_idx: TxIdx,
    ) -> (DbWrapper<'a, Mem>, ReadLogHandle) {
        let h = new_read_log_handle();
        (DbWrapper::new(mem, mv, tx_idx, h.clone()), h)
    }

    /// Drain the log from the handle for inspection.
    fn drain(h: &ReadLogHandle) -> Vec<crate::read_set::ReadLog> {
        std::mem::take(&mut *h.lock())
    }

    #[test]
    fn basic_falls_through_to_storage_when_mv_empty() {
        let mut mem = Mem::default();
        mem.basic.insert(ADDR_A, info(100, 3));
        let mv = MvMemory::new();
        let (mut db, h) = make_db(&mem, &mv, 5);

        let got = db.basic(ADDR_A).unwrap().unwrap();
        assert_eq!(got.balance, U256::from(100));
        assert_eq!(got.nonce, 3);

        let log = drain(&h);
        assert_eq!(log.len(), 1);
        assert!(matches!(log[0].origin, ReadOrigin::Storage));
    }

    #[test]
    fn basic_prefers_mv_memory_over_storage() {
        let mut mem = Mem::default();
        mem.basic.insert(ADDR_A, info(100, 3));
        let mv = MvMemory::new();
        mv.write(
            MemoryLocation::Basic(ADDR_A),
            Version { tx_idx: 2, incarnation: 0 },
            MemoryValue::Basic(info(999, 7)),
        );

        let (mut db, h) = make_db(&mem, &mv, 5);
        let got = db.basic(ADDR_A).unwrap().unwrap();
        assert_eq!(got.balance, U256::from(999));
        assert_eq!(got.nonce, 7);

        let log = drain(&h);
        assert_eq!(log.len(), 1);
        assert!(matches!(
            log[0].origin,
            ReadOrigin::Versioned(Version { tx_idx: 2, incarnation: 0 })
        ));
    }

    #[test]
    fn storage_returns_zero_when_nothing_written() {
        let mem = Mem::default();
        let mv = MvMemory::new();
        let (mut db, h) = make_db(&mem, &mv, 5);

        let v = db.storage(ADDR_A, U256::from(42)).unwrap();
        assert_eq!(v, U256::ZERO);

        let log = drain(&h);
        assert_eq!(log.len(), 1);
        assert!(matches!(log[0].origin, ReadOrigin::Storage));
    }

    #[test]
    fn storage_reads_mv_version_when_available() {
        let mem = Mem::default();
        let mv = MvMemory::new();
        mv.write(
            MemoryLocation::Storage(ADDR_A, U256::from(1)),
            Version { tx_idx: 3, incarnation: 2 },
            MemoryValue::Storage(U256::from(42)),
        );
        let (mut db, h) = make_db(&mem, &mv, 10);
        let v = db.storage(ADDR_A, U256::from(1)).unwrap();
        assert_eq!(v, U256::from(42));

        let log = drain(&h);
        assert_eq!(log.len(), 1);
        assert!(matches!(
            log[0].origin,
            ReadOrigin::Versioned(Version { tx_idx: 3, incarnation: 2 })
        ));
    }

    #[test]
    fn blocked_read_surfaces_error_and_does_not_log() {
        let mem = Mem::default();
        let mv = MvMemory::new();
        mv.write(
            MemoryLocation::Basic(ADDR_A),
            Version { tx_idx: 2, incarnation: 0 },
            MemoryValue::Basic(info(100, 0)),
        );
        assert!(mv.mark_estimate(MemoryLocation::Basic(ADDR_A), 2));

        let (mut db, h) = make_db(&mem, &mv, 5);
        let err = db.basic(ADDR_A).unwrap_err();
        assert_eq!(
            err,
            DbError::Blocked {
                blocking_tx_idx: 2,
            }
        );

        // Blocked reads are NOT logged.
        assert!(drain(&h).is_empty());
    }

    #[test]
    fn self_destructed_in_mv_is_none_for_basic_zero_for_storage() {
        let mem = Mem::default();
        let mv = MvMemory::new();
        mv.write(
            MemoryLocation::Basic(ADDR_A),
            Version { tx_idx: 2, incarnation: 0 },
            MemoryValue::SelfDestructed,
        );
        let (mut db, _h) = make_db(&mem, &mv, 5);
        assert!(db.basic(ADDR_A).unwrap().is_none());

        mv.write(
            MemoryLocation::Storage(ADDR_A, U256::from(1)),
            Version { tx_idx: 2, incarnation: 0 },
            MemoryValue::SelfDestructed,
        );
        let v = db.storage(ADDR_A, U256::from(1)).unwrap();
        assert_eq!(v, U256::ZERO);
    }

    #[test]
    fn code_by_hash_falls_through_to_storage() {
        let mut mem = Mem::default();
        let h = b256!("0x1111111111111111111111111111111111111111111111111111111111111111");
        let code = Bytecode::new_raw(vec![0x00u8].into());
        mem.code.insert(h, code.clone());
        let mv = MvMemory::new();
        let (mut db, _log) = make_db(&mem, &mv, 5);
        let got = db.code_by_hash(h).unwrap();
        assert_eq!(got, code);
    }

    #[test]
    fn block_hash_goes_straight_to_storage_and_is_not_logged() {
        let mut mem = Mem::default();
        let h = b256!("0x2222222222222222222222222222222222222222222222222222222222222222");
        mem.blocks.insert(100, h);
        let mv = MvMemory::new();
        let (mut db, log) = make_db(&mem, &mv, 5);
        let got = db.block_hash(100).unwrap();
        assert_eq!(got, h);
        assert!(drain(&log).is_empty());
    }

    #[test]
    fn kind_mismatch_surfaces_as_internal_error() {
        let mem = Mem::default();
        let mv = MvMemory::new();
        mv.write(
            MemoryLocation::Basic(ADDR_B),
            Version { tx_idx: 1, incarnation: 0 },
            MemoryValue::Storage(U256::from(1)),
        );
        let (mut db, _h) = make_db(&mem, &mv, 5);
        let err = db.basic(ADDR_B).unwrap_err();
        assert!(matches!(err, DbError::KindMismatch(_)));
    }

    #[test]
    fn multiple_reads_accumulate_in_log() {
        let mem = Mem::default();
        let mv = MvMemory::new();
        mv.write(
            MemoryLocation::Basic(ADDR_A),
            Version { tx_idx: 1, incarnation: 0 },
            MemoryValue::Basic(info(100, 0)),
        );

        let (mut db, h) = make_db(&mem, &mv, 5);
        let _ = db.basic(ADDR_A).unwrap();
        let _ = db.basic(ADDR_B).unwrap();
        let _ = db.storage(ADDR_A, U256::from(9)).unwrap();

        let log = drain(&h);
        assert_eq!(log.len(), 3);
        assert!(matches!(
            log[0].origin,
            ReadOrigin::Versioned(Version { tx_idx: 1, incarnation: 0 })
        ));
        assert!(matches!(log[1].origin, ReadOrigin::Storage));
        assert!(matches!(log[2].origin, ReadOrigin::Storage));
    }
}
