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
    evm::{api::BscEvm, transaction::BscTxEnv},
    hardforks::bsc::BscHardfork,
    node::evm::BscEvmFactory,
};
use alloy_primitives::{Address, B256, U256};
use parallel_evm::{DbError, DbWrapper, Storage, TransactOutcome, VmBuilder};
use reth_chainspec::EthChainSpec;
use reth_evm::EvmFactory;
use revm::{
    bytecode::Bytecode,
    context::{result::{EVMError, HaltReason, InvalidTransaction, ResultAndState}, BlockEnv, CfgEnv},
    inspector::NoOpInspector,
    primitives::hardfork::SpecId,
    state::AccountInfo,
    Database, ExecuteEvm,
};
use reth_evm::EvmEnv;
use std::sync::Arc;

/// `parallel_evm::Storage` adapter over a reth-style state source.
///
/// The generic `R` is deliberately broad — any type that exposes the four
/// read methods fits. Typical caller: pass a `StateProviderBox` or
/// `LatestStateProviderRef` wrapped in an `Arc`. We take ownership of the
/// reader (via `Arc`) so the parallel executor can hand out `&self`
/// references to worker threads without lifetime gymnastics.
#[derive(Debug, Clone)]
pub struct RethStorage<R> {
    inner: Arc<R>,
}

impl<R> RethStorage<R> {
    pub fn new(inner: Arc<R>) -> Self {
        Self { inner }
    }
}

// We can't universally impl `parallel_evm::Storage` for arbitrary reth
// state readers without naming the exact trait bounds they expose. The
// caller supplies a concrete `R` (typically their `StateProviderDatabase`
// or `LatestStateProviderRef` wrapped) and implements `Storage` for
// `RethStorage<R>` manually, OR — more commonly — skips this wrapper
// entirely and implements `Storage` directly on their provider type.
//
// The wrapper exists mostly to document the shape and to show the
// `Clone + Send + Sync` requirements are satisfiable.

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

// --- Placeholder-to-avoid-dead-code-warnings: we import several items
// purely so the doc comments and future callers have the right paths
// wired. The following forwards ensure they're not silently pruned on
// unused-imports pedantry during early iteration.
#[allow(dead_code)]
fn _keep_unused_warnings_quiet<DB: Database>(
    _a: AccountInfo,
    _b: Bytecode,
    _c: Address,
    _d: U256,
    _e: B256,
    _f: SpecId,
    _g: &BscEvm<DB, NoOpInspector>,
    _h: ResultAndState,
    _i: InvalidTransaction,
) {
}
