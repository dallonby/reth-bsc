//! BSC adapter for [`parallel_evm`].
//!
//! Implements [`parallel_evm::VmBuilder`] over BSC's `BscEvm` variant, so a
//! caller can drop `ParallelExecutor<BscVmBuilder>` into the block-execution
//! path and get parallel tx execution without changing anything about the
//! Parlia/system-tx hooks that live outside the tx loop.
//!
//! # Scope of this module
//!
//! - `BscVmBuilder`: the VmBuilder impl.
//! - `RethStorage`: a thin adapter that implements `parallel_evm::Storage`
//!   over any reth `StateProviderBox` so the parallel executor can read
//!   through MDBX/static files unchanged.
//!
//! Not in this module: the integration into `BscBlockExecutor` itself.
//! That lives behind a CLI flag and requires careful handling of BSC's
//! pre/post-execution hooks (snapshot updates, validator rewards, system
//! txs). The integration sketch is at the bottom of this doc comment.
//!
//! # Wiring status (2026-04-17)
//!
//! The `--bsc.parallel-execute` CLI flag is live: it sets a process-wide
//! atomic in [`crate::shared`], which `BscEvmConfig::context_for_block`
//! reads and propagates through `BscBlockExecutionCtx::parallel` on every
//! non-speculative block. `BscBlockExecutor::apply_pre_execution_changes`
//! logs a `debug!` line when the flag is active.
//!
//! Not yet wired: the actual parallel branch that replaces the default
//! tx-iteration loop with a `ParallelExecutor` call. That requires (a) a
//! concrete `Storage` impl over the executor's `State<DB>` (the existing
//! `RethStorage<R>` stub is a placeholder), and (b) a serial commit phase
//! that applies Hertz patches, `on_state` hooks, receipt building, and
//! state commits in tx order. Until that lands, flag-on behaves
//! identically to flag-off (serial loop) with a per-block log marker.
//!
//! # Integration sketch
//!
//! Inside `BscBlockExecutor::execute_transactions` (or in a new
//! `execute_one_parallel` method), guarded by a runtime flag:
//!
//! 1. Partition block txs into user-txs (non-system) and system-tx
//!    candidates. System txs go to the existing serial post-execution
//!    handler; don't parallelize those.
//! 2. Wrap the executor's state DB in `RethStorage`.
//! 3. Build `BscVmBuilder` with the current chain spec + factory.
//! 4. `ParallelExecutor::new(cfg, vm_builder).execute(&storage,
//!    block_env, spec, user_txs)` — returns per-tx results.
//! 5. For each result in tx order: if `Ok(rs)`, commit `rs.state` to the
//!    executor's `State<DB>` via `DatabaseCommit::commit`, then build a
//!    receipt from `rs.result` via the executor's existing receipt
//!    builder. If `Err(msg)`, record a failed tx (empty receipt).
//! 6. Continue with the serial post-execution path (system txs, validator
//!    rewards, Feynman init, snapshot write, genesis-1 contract deploy).
//!
//! Any fallback condition (storage error, re-execution budget exceeded)
//! the parallel executor reports via `Err(parallel_evm::Error)` — the
//! adapter should fall back to the pure-serial path for that block.

use crate::{
    chainspec::BscChainSpec,
    evm::transaction::BscTxEnv,
    hardforks::bsc::BscHardfork,
    node::evm::BscEvmFactory,
};
use alloy_primitives::{Address, B256, U256};
use parallel_evm::{DbError, DbWrapper, Storage, TransactOutcome, VmBuilder};
use reth_chainspec::EthChainSpec;
use reth_evm::{EvmEnv, EvmFactory};
use revm::{
    bytecode::Bytecode,
    context::{result::{EVMError, HaltReason}, BlockEnv, CfgEnv},
    state::AccountInfo,
    DatabaseRef, ExecuteEvm,
};
use std::{marker::PhantomData, sync::Arc};

/// `parallel_evm::Storage` adapter that borrows any `&DB: DatabaseRef`.
///
/// This is the production-path adapter we hand to `ParallelExecutor`.
/// Crucially, `State<DB: DatabaseRef>` itself implements `DatabaseRef`
/// via cache→bundle→underlying-DB lookups *without* mutating its cache,
/// which is exactly what we need: parallel workers read through the
/// executor's current `&State<DB>` and observe the `apply_pre_execution
/// _changes` mutations without any locking. Writes are never routed
/// through `Storage`; they're accumulated in `parallel_evm`'s MvMemory
/// and serialised back to the executor's `State<DB>` after the parallel
/// phase finishes.
///
/// The `'a` lifetime keeps us honest: the adapter cannot outlive the
/// `&State<DB>` it borrows. Scoped worker threads inside
/// `ParallelExecutor::execute` join before returning, so the borrow is
/// safely bounded to a single `execute_block` invocation.
#[derive(Debug)]
pub struct RefStorage<'a, DB: DatabaseRef> {
    db: &'a DB,
    _marker: PhantomData<&'a ()>,
}

impl<'a, DB: DatabaseRef> RefStorage<'a, DB> {
    pub fn new(db: &'a DB) -> Self {
        Self { db, _marker: PhantomData }
    }
}

impl<'a, DB> Storage for RefStorage<'a, DB>
where
    DB: DatabaseRef + std::fmt::Debug + Send + Sync + 'a,
    DB::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = DB::Error;

    fn basic(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        DatabaseRef::basic_ref(self.db, address)
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        DatabaseRef::code_by_hash_ref(self.db, code_hash)
    }

    fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        DatabaseRef::storage_ref(self.db, address, slot)
    }

    fn block_hash(&self, number: u64) -> Result<B256, Self::Error> {
        DatabaseRef::block_hash_ref(self.db, number)
    }
}

/// VmBuilder over BSC's revm variant.
///
/// Owns a chain spec + the default `BscEvmFactory`. Per-tx the `transact`
/// method builds a fresh `CfgEnv` (carrying the `BscHardfork`),
/// instantiates a `BscEvm`, runs the tx, and maps revm's errors onto our
/// `TransactOutcome`.
#[derive(Debug, Clone)]
pub struct BscVmBuilder {
    chain_spec: Arc<BscChainSpec>,
    factory: BscEvmFactory,
}

impl BscVmBuilder {
    pub fn new(chain_spec: Arc<BscChainSpec>) -> Self {
        Self {
            chain_spec,
            factory: BscEvmFactory::default(),
        }
    }

    pub fn chain_spec(&self) -> &Arc<BscChainSpec> {
        &self.chain_spec
    }
}

impl VmBuilder for BscVmBuilder {
    type Tx = BscTxEnv;
    type HaltReason = HaltReason;
    type Spec = BscHardfork;

    fn transact<S: Storage>(
        &self,
        db: DbWrapper<'_, S>,
        block_env: &BlockEnv,
        spec: &BscHardfork,
        tx: &BscTxEnv,
    ) -> TransactOutcome<HaltReason> {
        // Build the cfg env carrying this block's spec and chain id.
        // NOTE: the Spec type held by CfgEnv is BSC's `BscHardfork`, not
        // revm's `SpecId` — this is how BSC surfaces its extra forks
        // (Luban, Feynman, etc.) to the handler.
        let cfg_env = CfgEnv::new_with_spec(spec.clone())
            .with_chain_id(self.chain_spec.chain().id());

        let evm_env = EvmEnv {
            cfg_env,
            block_env: block_env.clone(),
        };

        // Construct the evm wrapping our DbWrapper. BscEvmFactory handles
        // the fiddly inspector/trace flags; we always request non-trace
        // here (parallel execution is not instrumented).
        //
        // Read logging happens automatically through the DbWrapper's
        // shared `ReadLogHandle` — we don't need to extract the DB from
        // the EVM after transact returns.
        let mut evm = self.factory.create_evm(db, evm_env);

        match evm.transact(tx.clone()) {
            Ok(result_and_state) => TransactOutcome::Completed { result_and_state },
            Err(EVMError::Database(DbError::Blocked { blocking_tx_idx })) => {
                TransactOutcome::Blocked { blocking_tx_idx }
            }
            Err(EVMError::Database(DbError::Storage(msg))) => {
                TransactOutcome::StorageError(msg)
            }
            Err(EVMError::Database(DbError::KindMismatch(msg))) => {
                TransactOutcome::StorageError(format!("db kind mismatch: {msg}"))
            }
            Err(EVMError::Transaction(tx_err)) => {
                TransactOutcome::ValidationError(format!("{tx_err:?}"))
            }
            Err(EVMError::Header(err)) => {
                TransactOutcome::ValidationError(format!("header: {err:?}"))
            }
            Err(EVMError::Custom(msg)) => TransactOutcome::ValidationError(msg),
        }
    }
}

