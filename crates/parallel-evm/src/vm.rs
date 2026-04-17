//! Caller-supplied hook for actually running a transaction in their EVM.
//!
//! `parallel-evm` is EVM-variant-agnostic. Ethereum mainnet, BSC, Optimism,
//! revmc-JIT'd variants — all plug in via this trait. The worker loop hands
//! a fresh [`DbWrapper`] and a transaction to the `VmBuilder`; the impl
//! builds its chain-specific `Evm`, runs `transact`, and returns the
//! outcome.
//!
//! # Why not just `dyn Fn`
//!
//! We need the impl to be `Send + Sync` so workers can share a reference to
//! it across threads. A trait object with chain-specific associated types
//! (`Tx`, `HaltReason`) keeps the API strongly typed from the caller's side
//! without forcing us to erase into `Box<dyn ...>`.
//!
//! # What the impl must do
//!
//! 1. Construct an `Evm` around the provided `DbWrapper`. For plain
//!    Ethereum this is `revm::Context::mainnet().with_db(db).build_mainnet()`;
//!    for BSC it's the chain's own builder; for a revmc-JIT'd variant the
//!    builder caches/loads compiled bytecode and plugs it in at the
//!    interpreter boundary.
//!
//! 2. Call `evm.transact(tx)` (or equivalent).
//!
//! 3. Extract the read log from the `DbWrapper` after the EVM is dropped.
//!    The idiom is: unpack the Evm back into its context, pull the DB out,
//!    call `db.into_read_log()`.
//!
//! 4. Map revm's error types onto our [`TransactOutcome`]:
//!    - `EVMError::Database(DbError::Blocked { blocking_tx_idx })` →
//!      [`TransactOutcome::Blocked`]
//!    - `EVMError::Database(DbError::Storage(..))` or `::KindMismatch(..)` →
//!      [`TransactOutcome::StorageError`]
//!    - Any other revm error (transaction validation, header validation,
//!      custom) → [`TransactOutcome::ValidationError`]
//!    - `Ok(result_and_state)` → [`TransactOutcome::Completed`]

use crate::{
    db_wrapper::DbWrapper,
    mv_memory::TxIdx,
    read_set::ReadLog,
    storage::Storage,
};
use revm::{
    context::{result::ResultAndState, BlockEnv},
    primitives::hardfork::SpecId,
};

/// Outcome returned by [`VmBuilder::transact`].
///
/// Generic over the halt reason so chains with custom halt variants (OP,
/// BSC) can surface them unchanged to the caller.
#[derive(Debug)]
pub enum TransactOutcome<H> {
    /// EVM ran to completion (success, revert, or halt). Caller records the
    /// write set to `MvMemory` and moves on.
    Completed {
        /// revm's full result (receipt-relevant data + state diff).
        result_and_state: ResultAndState<H>,
    },
    /// A read hit an `Estimate` marker in MvMemory. The worker parks this
    /// tx on `blocking_tx_idx` via `StatusVector::add_dependent` and moves
    /// on to another task.
    Blocked {
        /// The tx this one is parked on.
        blocking_tx_idx: TxIdx,
    },
    /// Underlying storage failed. Fatal — the worker aborts the whole
    /// parallel run and the caller falls back to sequential.
    StorageError(String),
    /// The transaction itself is invalid (bad signature, insufficient
    /// funds, nonce mismatch, etc.). The worker records this as a
    /// failed-tx result; the block still completes.
    ///
    /// The string captures revm's error; we avoid forcing a concrete
    /// type because every chain has slightly different validation variants.
    ValidationError(String),
}

/// Full result of a single `transact` call, including the accumulated read
/// log. The worker feeds the log into the scheduler's [`ReadSetStore`] for
/// the validation phase.
#[derive(Debug)]
pub struct TransactResult<H> {
    pub outcome: TransactOutcome<H>,
    pub reads: Vec<ReadLog>,
}

/// Caller-implemented hook that wraps a revm variant.
///
/// Thread-safe (`Send + Sync`): one instance is shared across all worker
/// threads of a [`crate::ParallelExecutor::execute`] call. Implementations
/// should be cheap to clone if any per-worker state is needed; the default
/// assumption is that the builder itself is stateless and the EVM is
/// constructed fresh per tx.
pub trait VmBuilder: Send + Sync {
    /// Chain's transaction-env type. For vanilla Ethereum, `revm::context::
    /// TxEnv`; for BSC, `BscTxEnv`; for OP, `OpTxEnv`.
    ///
    /// Must be `Sync` because the full `txs: &[Tx]` slice is shared across
    /// worker threads for index-based access; individual txs are consumed
    /// by reference so immutability is enough for safety.
    type Tx: Clone + Send + Sync;

    /// Chain's halt-reason type. Often `revm::context::result::HaltReason`
    /// for mainnet, or a chain-specific variant.
    type HaltReason: Send + std::fmt::Debug;

    /// Execute `tx` against the supplied `DbWrapper`.
    ///
    /// # Contract
    ///
    /// - The impl MUST consume `db` through its EVM — no borrowing past
    ///   the EVM's lifetime.
    /// - The impl MUST extract `db.into_read_log()` and return it in the
    ///   `reads` field, regardless of success/failure. A blocked or
    ///   errored tx still has a partial log that the caller discards.
    /// - Errors from revm MUST be mapped onto the four [`TransactOutcome`]
    ///   variants.
    fn transact<S: Storage>(
        &self,
        db: DbWrapper<'_, S>,
        block_env: &BlockEnv,
        spec_id: SpecId,
        tx: &Self::Tx,
    ) -> TransactResult<Self::HaltReason>;
}
