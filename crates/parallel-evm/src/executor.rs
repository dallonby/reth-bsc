//! The public parallel block executor.
//!
//! # Shape
//!
//! ```no_run
//! # use parallel_evm::{ParallelExecutor, Config};
//! # fn example<VB: parallel_evm::VmBuilder, S: parallel_evm::Storage>(
//! #     vm: VB, storage: &S,
//! #     block_env: revm::context::BlockEnv,
//! #     spec: VB::Spec,
//! #     txs: Vec<VB::Tx>,
//! # ) -> parallel_evm::Result<()> {
//! let executor = ParallelExecutor::new(Config::default(), vm);
//! let output = executor.execute(storage, block_env, spec, txs)?;
//! for (idx, result) in output.results.iter().enumerate() {
//!     match result {
//!         Ok(r) => { /* apply r.state, build receipt from r.result */ }
//!         Err(msg) => { /* tx was invalid; skip */ }
//!     }
//! }
//! # Ok(()) }
//! ```
//!
//! # Per-block flow
//!
//! 1. Empty / small blocks fall through to a sequential path (avoids
//!    worker-spawn overhead for inputs that can't benefit).
//! 2. Spawn `config.workers` scoped threads. Each drains the scheduler.
//! 3. On the main thread, wait for all workers to join.
//! 4. If a worker signalled a fatal error (storage failure, budget
//!    exhausted), surface it.
//! 5. Otherwise collect per-tx results in index order. The caller commits
//!    the resulting state diffs to their own DB.

use crate::{
    error::{Error, Result},
    scheduler::Scheduler,
    storage::Storage,
    vm::{TransactOutcome, VmBuilder},
    worker::{commit_writes_to_mv, worker_loop, ResultSlots, WorkerShared},
};
use parking_lot::Mutex;
use revm::{
    context::{result::ResultAndState, BlockEnv},
};
use std::{
    sync::atomic::{AtomicU32, Ordering},
    thread,
};

/// Tuning knobs for the parallel executor. Defaults are chosen to be safe
/// on unknown workloads; callers with workload knowledge should override.
#[derive(Debug, Clone)]
pub struct Config {
    /// Number of worker threads. Must be non-zero.
    ///
    /// Rule of thumb: match to physical cores. Beyond that the scheduler
    /// contention (CAS on `execution_idx`) starts eating any benefit. On
    /// Zen 5 16-core (CCD0), 8-16 is a reasonable range.
    pub workers: usize,

    /// Cap on re-executions per transaction. A single tx re-executing more
    /// than this indicates a pathological dependency chain; the executor
    /// returns `Error::ReexecutionBudgetExceeded` and the caller falls
    /// back to sequential. Low-conflict BSC blocks usually re-execute 0-2
    /// times per tx; setting this to 16 leaves headroom while still
    /// catching livelocks.
    pub max_reexecutions_per_tx: u32,

    /// Below this many transactions, the parallel path has too much
    /// overhead to win. Execute sequentially.
    pub min_txs_for_parallel: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            workers: num_cpus_approx(),
            max_reexecutions_per_tx: 16,
            min_txs_for_parallel: 4,
        }
    }
}

/// Best-effort core count. Falls back to 8 if the platform query fails.
fn num_cpus_approx() -> usize {
    std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(8)
}

/// Per-tx result: either a successful execution (revm's `ResultAndState`,
/// which encodes success/revert/halt internally) or a tx-level validation
/// failure (stringified error).
///
/// Indexed by position in the block.
pub type TxResult<H> = std::result::Result<ResultAndState<H>, String>;

/// Output of executing a block in parallel.
#[derive(Debug)]
pub struct Output<H> {
    /// Per-tx results in block order. Index `i` is the result of the
    /// `i`-th input tx.
    pub results: Vec<TxResult<H>>,
    /// Diagnostics. Not required for correctness.
    pub stats: Stats,
}

/// Summary stats for a single `execute()` call.
#[derive(Debug, Default, Clone, Copy)]
pub struct Stats {
    /// Number of transactions processed.
    pub num_txs: usize,
    /// Sum of re-executions across all transactions. `== num_txs` means
    /// every tx executed exactly once (ideal parallelism). Higher means
    /// validation aborted some attempts; a value several times `num_txs`
    /// signals a high-conflict block.
    pub total_executions: u64,
    /// Whether the parallel path was skipped because the block was too
    /// small (see [`Config::min_txs_for_parallel`]).
    pub ran_sequential: bool,
}

/// The parallel block executor. Owns its configuration and the
/// chain-specific `VmBuilder`; re-usable across blocks (stateless).
pub struct ParallelExecutor<VB: VmBuilder> {
    config: Config,
    vm_builder: VB,
}

impl<VB: VmBuilder> ParallelExecutor<VB> {
    pub fn new(config: Config, vm_builder: VB) -> Self {
        Self { config, vm_builder }
    }

    /// Read-only access to the config (useful for adapters that expose
    /// tuning through their own CLI).
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Execute a block of transactions in parallel.
    ///
    /// `block_env` and `spec_id` are passed to every tx's EVM instance
    /// unchanged. `txs` are executed in input order for the purposes of
    /// conflict resolution (Block-STM serialisability).
    pub fn execute<S: Storage>(
        &self,
        storage: &S,
        block_env: BlockEnv,
        spec: VB::Spec,
        txs: Vec<VB::Tx>,
    ) -> Result<Output<VB::HaltReason>> {
        if self.config.workers == 0 {
            return Err(Error::ZeroWorkers);
        }

        let num_txs = txs.len();

        // Short-circuit: empty blocks.
        if num_txs == 0 {
            return Ok(Output {
                results: Vec::new(),
                stats: Stats { num_txs: 0, ..Default::default() },
            });
        }

        // Short-circuit: tiny blocks run sequentially.
        if num_txs < self.config.min_txs_for_parallel {
            return self.execute_sequential(storage, &block_env, &spec, &txs);
        }

        // Parallel path.
        let scheduler = Scheduler::new(num_txs);
        let mut results: ResultSlots<VB::HaltReason> = Vec::with_capacity(num_txs);
        for _ in 0..num_txs {
            results.push(Mutex::new(None));
        }
        let reexec_counts: Vec<AtomicU32> =
            (0..num_txs).map(|_| AtomicU32::new(0)).collect();
        let fatal: Mutex<Option<Error>> = Mutex::new(None);

        let shared = WorkerShared {
            scheduler: &scheduler,
            vm_builder: &self.vm_builder,
            storage,
            block_env: &block_env,
            spec: &spec,
            txs: &txs,
            results: &results,
            reexec_counts: &reexec_counts,
            fatal: &fatal,
            max_reexecutions: self.config.max_reexecutions_per_tx,
        };

        // Scoped threads so `shared` can borrow from the stack without
        // Arc. `shared` carries only borrowed references; nothing to
        // move.
        thread::scope(|s| {
            for _ in 0..self.config.workers {
                s.spawn(|| worker_loop(&shared));
            }
        });

        // Check for a fatal error surfaced by any worker.
        if let Some(err) = fatal.into_inner() {
            return Err(err);
        }

        // Extract results in index order.
        let mut out_results = Vec::with_capacity(num_txs);
        for (idx, slot) in results.into_iter().enumerate() {
            let value = slot
                .into_inner()
                .ok_or_else(|| Error::Storage(format!("tx {idx} never produced a result")))?;
            out_results.push(value);
        }

        let total_executions: u64 =
            reexec_counts.iter().map(|c| c.load(Ordering::Relaxed) as u64).sum();
        Ok(Output {
            results: out_results,
            stats: Stats {
                num_txs,
                total_executions,
                ran_sequential: false,
            },
        })
    }

    /// Sequential fallback — used for blocks below
    /// [`Config::min_txs_for_parallel`]. Runs each tx serially against an
    /// empty [`MvMemory`] so writes from one tx are visible to the next,
    /// matching parallel semantics.
    fn execute_sequential<S: Storage>(
        &self,
        storage: &S,
        block_env: &BlockEnv,
        spec: &VB::Spec,
        txs: &[VB::Tx],
    ) -> Result<Output<VB::HaltReason>> {
        use crate::{
            db_wrapper::{new_read_log_handle, DbWrapper},
            mv_memory::{MvMemory, Version},
        };

        let mv = MvMemory::new();
        let mut results = Vec::with_capacity(txs.len());
        for (idx, tx) in txs.iter().enumerate() {
            let handle = new_read_log_handle();
            let db = DbWrapper::new(storage, &mv, idx, handle);
            let outcome = self.vm_builder.transact(db, block_env, spec, tx);
            match outcome {
                TransactOutcome::Completed { result_and_state } => {
                    // Write-through so tx idx+1 sees this tx's writes.
                    commit_writes_to_mv(
                        &mv,
                        Version { tx_idx: idx, incarnation: 0 },
                        &result_and_state.state,
                    );
                    results.push(Ok(result_and_state));
                }
                TransactOutcome::Blocked { .. } => {
                    // Can't happen in sequential mode (nothing to be
                    // Blocked on), but surface it as a storage-level
                    // error if we somehow see it.
                    return Err(Error::Storage(
                        "sequential path hit an unexpected Blocked read".into(),
                    ));
                }
                TransactOutcome::StorageError(msg) => return Err(Error::Storage(msg)),
                TransactOutcome::ValidationError(msg) => results.push(Err(msg)),
            }
        }
        Ok(Output {
            results,
            stats: Stats {
                num_txs: txs.len(),
                total_executions: txs.len() as u64,
                ran_sequential: true,
            },
        })
    }
}
