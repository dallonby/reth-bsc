//! BSC batch executor that branches per-block into the parallel path.
//!
//! Reth's [`reth_evm::execute::Executor`] trait is the hook that its
//! `ExecutionStage` drives during pipeline sync: the stage calls
//! `batch_executor(db)` once, then `execute_one(&block)` per block, and
//! finally `into_state().take_bundle()` + `provider.write_state(...)`
//! at end-of-batch.
//!
//! The upstream default impl ([`reth_evm::BasicBlockExecutor`]) is
//! fine for the serial path but can't see our
//! [`super::parallel::LayeredStorage`] — for that we need per-block
//! access to the accumulating `State<DB>`'s `bundle_state`. This file
//! forks `BasicBlockExecutor` for BSC so each `execute_one` call can:
//!
//! 1. Check the global `--bsc.parallel-execute` flag.
//! 2. On the parallel branch, snapshot the state's current
//!    `bundle_state` (a `BundleState::clone`, see cost note below),
//!    build a concrete `BscBlockExecutor` via the factory path (NOT
//!    via `ConfigureEvm::executor_for_block`, which erases the concrete
//!    type behind `impl BlockExecutorFor`), and call
//!    [`crate::node::evm::executor::BscBlockExecutor::execute_block_parallel`].
//! 3. On the serial branch, mirror `BasicBlockExecutor::execute_one`
//!    verbatim.
//!
//! After either branch the caller merges transitions into the bundle
//! the same way `BasicBlockExecutor` does.
//!
//! # Cost of per-block bundle snapshots
//!
//! `BundleState: Clone` is a deep copy. For a pipeline batch with
//! N blocks and an average of K transitions per block, the bundle
//! grows to ~N*K entries and we clone it N times → O(N²*K) total
//! allocator work per batch. For reth's default batch size (a few
//! thousand blocks, a few hundred transitions each) this is tens of
//! MB total — painful but survivable. The natural Phase 3 optimization
//! is an `Arc<BundleState>` with copy-on-write via `Arc::make_mut`
//! after the parallel workers join; deferred until we have perf
//! numbers that motivate it.
//!
//! # Why not replace `ExecutionStage` entirely?
//!
//! Considered. The ExecutionStage fork would pull in a lot of reth-
//! specific scaffolding (ExExManagerHandle, thresholds, checkpointing)
//! for marginal benefit over intercepting at the `Executor` trait.
//! Keeping the stage means reth's existing stage metrics, ExEx
//! notifications, and commit/batch thresholds stay in play unchanged.
//!
//! # Factory access
//!
//! The parallel path needs a `Send + Sync` state-provider spawner for
//! per-worker tx allocation. Reth's `ExecutionStage` doesn't thread a
//! factory through `batch_executor(db)` — we can't get one at
//! construction time. Instead the node sets a global spawner via
//! [`crate::shared::set_parallel_state_spawner`] in its
//! `on_component_initialized` hook; we read it here per-block. If the
//! global hasn't been initialized but the flag is on (e.g. in unit
//! tests that construct a BscBatchExecutor directly), we warn and
//! fall back to the serial path.

use std::sync::Arc;

use super::{config::BscEvmConfig, executor::BscBlockExecutor};
use crate::node::primitives::BscPrimitives;
use alloy_evm::{
    block::{BlockExecutor as _, BlockExecutorFactory},
    Evm as _,
};
use reth_evm::{
    execute::{BlockExecutionError, Executor},
    ConfigureEvm, Database, OnStateHook,
};
use reth_primitives_traits::{NodePrimitives, RecoveredBlock};
use reth_provider::BlockExecutionResult;
use revm::{
    database::{states::bundle_state::BundleRetention, State},
    Database as _RevmDatabase,
};
use tracing::{debug, warn};

/// Executor that reth's `ExecutionStage` drives, with a per-block
/// branch into the parallel-evm path when the `--bsc.parallel-execute`
/// flag is on.
///
/// Mirrors [`reth_evm::BasicBlockExecutor`]'s shape (a single
/// `State<DB>` across the whole batch, merging transitions per block)
/// so the rest of ExecutionStage — checkpointing, metrics, batch
/// thresholding, ExEx notifications — keeps working unchanged.
pub struct BscBatchExecutor<DB> {
    evm_config: BscEvmConfig,
    state: State<DB>,
}

impl<DB: Database> BscBatchExecutor<DB> {
    /// Create a new batch executor over the given DB.
    ///
    /// Constructs the wrapping `State<DB>` with `bundle_update` mode,
    /// matching `BasicBlockExecutor::new` exactly.
    pub fn new(evm_config: BscEvmConfig, db: DB) -> Self {
        let state = State::builder().with_database(db).with_bundle_update().build();
        Self { evm_config, state }
    }

    /// Dispatch a single block through the parallel path.
    ///
    /// Sequence:
    /// 1. Build a concrete `BscBlockExecutor` via direct factory
    ///    dispatch (the trait method `executor_for_block` erases the
    ///    concrete type behind `impl BlockExecutorFor`, and our
    ///    `execute_block_parallel` is inherent, not trait-defined).
    /// 2. Drive pre-execution changes on the executor. These include
    ///    system-contract upgrades at block begin and Prague's
    ///    `apply_blockhashes_contract_call` — both of which write to
    ///    the state's cache/transitions.
    /// 3. Merge transitions into the bundle. This is the critical
    ///    step for parallel correctness: without it, the snapshot in
    ///    step 4 would NOT reflect any pre-exec writes, and parallel
    ///    workers would read pre-patch state for any contract the
    ///    current block's pre-exec upgraded.
    /// 4. Snapshot the bundle state.
    /// 5. Dispatch `execute_block_parallel(spawner, snapshot, txs)`,
    ///    which runs the tx phase in parallel, commits serially, and
    ///    finishes with post-exec (system txs, validator rewards,
    ///    etc.) on the main thread.
    fn try_execute_one_parallel(
        &mut self,
        block: &RecoveredBlock<<BscPrimitives as NodePrimitives>::Block>,
        spawner: &Arc<dyn super::parallel::ParallelStateSpawner + Send + Sync + 'static>,
    ) -> Result<
        BlockExecutionResult<<BscPrimitives as NodePrimitives>::Receipt>,
        BlockExecutionError,
    > {
        let evm = self
            .evm_config
            .evm_for_block(&mut self.state, block.header())
            .map_err(BlockExecutionError::other)?;
        let ctx = self
            .evm_config
            .context_for_block(block.sealed_block())
            .map_err(BlockExecutionError::other)?;

        let mut executor: BscBlockExecutor<'_, _, _, _> = self
            .evm_config
            .executor_factory
            .create_executor(evm, ctx);

        // Step 2: run pre-execution changes on the executor. Writes
        // land in `self.state` (through the `&mut` inside `evm`).
        executor.apply_pre_execution_changes()?;

        // Step 3: fold pre-exec writes into the bundle. Without this,
        // the snapshot in step 4 would miss them.
        //
        // We can't call `self.state.merge_transitions(...)` here
        // because `self.state` is borrowed mutably through `executor.evm`.
        // Instead, go through the executor's `db_mut` which returns
        // the same `&mut State<DB>`.
        executor
            .evm_mut()
            .db_mut()
            .merge_transitions(BundleRetention::Reverts);

        // Step 4: snapshot. This IS a deep clone of BundleState; see
        // the cost-model note in this module's docs.
        let bundle_snapshot = executor.evm().db().bundle_state.clone();

        // Step 5: run user/system tx phase in parallel + post-exec.
        executor.execute_block_parallel(
            &**spawner,
            &bundle_snapshot,
            block.transactions_recovered(),
        )
    }
}

/// Blanket-ish `Executor` impl for the BSC batch path.
///
/// The bounds look intimidating, but they're just what
/// `BscEvmConfig` already needs for any block execution — the trait-
/// method dispatch on `executor_factory.create_executor` requires that
/// the resulting `BscBlockExecutor`'s `BlockExecutor` impl type-checks.
/// Easier to express: whenever `&'a mut State<DB>` can back a
/// `BscBlockExecutor`, we can impl `Executor` for this batch wrapper.
impl<DB> Executor<DB> for BscBatchExecutor<DB>
where
    DB: Database,
    <DB as _RevmDatabase>::Error: Send + Sync + 'static,
{
    type Primitives = BscPrimitives;
    type Error = BlockExecutionError;

    fn execute_one(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
    ) -> Result<
        BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>,
        Self::Error,
    > {
        let want_parallel = crate::shared::is_parallel_execute_enabled();
        if want_parallel {
            if let Some(spawner) = crate::shared::get_parallel_state_spawner() {
                let spawner = spawner.clone();
                let block_number = block.header().number;
                match self.try_execute_one_parallel(block, &spawner) {
                    Ok(result) => {
                        self.state.merge_transitions(BundleRetention::Reverts);
                        debug!(
                            target: "bsc::executor::parallel",
                            block_number,
                            "parallel execute_one completed"
                        );
                        return Ok(result);
                    }
                    Err(err) => {
                        // Parallel path bubbled a BlockExecutionError.
                        // Note: execute_block_parallel already handles
                        // its own internal serial fallbacks (Hertz
                        // patches, parallel-executor errors, per-tx
                        // Err results); an error reaching us here
                        // means even the serial fallback inside that
                        // method failed — surface it.
                        return Err(err);
                    }
                }
            } else {
                // Flag on but no spawner registered — typical in unit
                // tests that don't go through the full node launch.
                // Fall through to the serial path.
                warn!(
                    target: "bsc::executor::parallel",
                    "parallel-execute flag on but no state spawner registered; \
                     running serially. Set the spawner in the node's \
                     on_component_initialized hook."
                );
            }
        }

        // Serial path: mirror BasicBlockExecutor::execute_one exactly.
        let executor = self
            .evm_config
            .executor_for_block(&mut self.state, block)
            .map_err(BlockExecutionError::other)?;
        let result = executor.execute_block(block.transactions_recovered())?;
        self.state.merge_transitions(BundleRetention::Reverts);
        Ok(result)
    }

    fn execute_one_with_state_hook<H>(
        &mut self,
        block: &RecoveredBlock<<Self::Primitives as NodePrimitives>::Block>,
        state_hook: H,
    ) -> Result<
        BlockExecutionResult<<Self::Primitives as NodePrimitives>::Receipt>,
        Self::Error,
    >
    where
        H: OnStateHook + 'static,
    {
        // State hooks are incompatible with the parallel-commit phase
        // (hook would fire on the batched commit, not per-tx-in-
        // block-order). Always go serial here; reth's stage uses this
        // path only for tracing/debug flows where correctness trumps
        // throughput anyway.
        let executor = self
            .evm_config
            .executor_for_block(&mut self.state, block)
            .map_err(BlockExecutionError::other)?
            .with_state_hook(Some(Box::new(state_hook)));
        let result = executor.execute_block(block.transactions_recovered())?;
        self.state.merge_transitions(BundleRetention::Reverts);
        Ok(result)
    }

    fn into_state(self) -> State<DB> {
        self.state
    }

    fn size_hint(&self) -> usize {
        self.state.bundle_state.size_hint()
    }
}

