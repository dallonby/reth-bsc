//! Worker loop: claim tasks from the scheduler, execute/validate, repeat.
//!
//! One worker per thread. Each worker:
//!
//! 1. Calls `scheduler.next_task()` until it returns `None` (block done).
//! 2. For `Execute(version)`: builds a fresh [`DbWrapper`], invokes the
//!    [`VmBuilder`], routes the outcome to one of finish_execution / abort /
//!    fallback.
//! 3. For `Validate(version)`: snapshots the tx's read log, replays it
//!    against current [`MvMemory`], calls `finish_validation(aborted)`.
//!
//! # Fatal errors
//!
//! A shared `fatal` slot is set when a worker discovers an unrecoverable
//! condition (storage failure, re-execution budget exhausted). Every worker
//! checks `fatal` on each loop iteration and exits immediately if set, so
//! the parallel run terminates cleanly and the caller can fall back to
//! sequential execution. The first worker to set `fatal` wins; others leave
//! it alone.

use crate::{
    db_wrapper::DbWrapper,
    error::Error,
    mv_memory::{MemoryLocation, MemoryValue, MvMemory, ReadOutcome, TxIdx, Version},
    read_set::{ReadLog, ReadOrigin},
    scheduler::{Scheduler, Task},
    storage::Storage,
    vm::{TransactOutcome, VmBuilder},
};
use parking_lot::Mutex;
use revm::{
    context::{result::ResultAndState, BlockEnv},
    primitives::hardfork::SpecId,
    state::{Account, EvmState},
};
use std::sync::atomic::{AtomicU32, Ordering};

/// Per-tx result slot. Each slot is written exactly once (the final,
/// validated incarnation's result) and read once at block end.
pub(crate) type ResultSlots<H> = Vec<Mutex<Option<Result<ResultAndState<H>, String>>>>;

/// Shared state used by `worker_loop`. Held on the stack of
/// [`crate::ParallelExecutor::execute`]; workers borrow it for the duration
/// of the parallel phase.
pub(crate) struct WorkerShared<'a, VB: VmBuilder, S: Storage> {
    pub scheduler: &'a Scheduler,
    pub vm_builder: &'a VB,
    pub storage: &'a S,
    pub block_env: &'a BlockEnv,
    pub spec_id: SpecId,
    pub txs: &'a [VB::Tx],
    /// Per-tx `Mutex<Option<_>>`: slot for the final execution result. On
    /// re-execution, the slot is overwritten by the latest incarnation.
    pub results: &'a ResultSlots<VB::HaltReason>,
    /// Per-tx re-execution counter. When this exceeds `max_reexecutions_per_tx`
    /// the worker sets `fatal` and the parallel run bails.
    pub reexec_counts: &'a [AtomicU32],
    /// Shared fatal-error slot. First writer wins; once set, all workers
    /// exit on next loop iteration.
    pub fatal: &'a Mutex<Option<Error>>,
    /// Re-execution budget per tx (Config::max_reexecutions_per_tx).
    pub max_reexecutions: u32,
}

/// Main per-thread entry point. Blocks until the scheduler declares the
/// block complete or `fatal` is set.
pub(crate) fn worker_loop<VB, S>(shared: &WorkerShared<'_, VB, S>)
where
    VB: VmBuilder,
    S: Storage,
{
    loop {
        if shared.fatal.lock().is_some() {
            return;
        }
        let Some(task) = shared.scheduler.next_task() else {
            return;
        };
        match task {
            Task::Execute(version) => handle_execute(shared, version),
            Task::Validate(version) => handle_validate(shared, version),
        }
    }
}

fn handle_execute<VB, S>(shared: &WorkerShared<'_, VB, S>, version: Version)
where
    VB: VmBuilder,
    S: Storage,
{
    // Enforce re-execution budget. Fetch-add returns the OLD value; we
    // compare before incrementing the next attempt.
    let prior = shared.reexec_counts[version.tx_idx].fetch_add(1, Ordering::Relaxed);
    if prior >= shared.max_reexecutions {
        let mut slot = shared.fatal.lock();
        if slot.is_none() {
            *slot = Some(Error::ReexecutionBudgetExceeded {
                tx_idx: version.tx_idx,
                budget: shared.max_reexecutions,
            });
        }
        // Release the scheduler slot so other workers don't hang waiting.
        // `abort_in_flight_execution` does the right bookkeeping — the
        // chosen blocker is arbitrary (0); the point is just to decrement
        // num_active_tasks. A subsequent worker that inspects `fatal`
        // will exit anyway.
        shared.scheduler.abort_in_flight_execution(version, version.tx_idx);
        return;
    }

    let db = DbWrapper::new(shared.storage, shared.scheduler.mv_memory(), version.tx_idx);
    let result = shared
        .vm_builder
        .transact(db, shared.block_env, shared.spec_id, &shared.txs[version.tx_idx]);

    // Record read log (even partial) so validation can run. For
    // Blocked outcomes the log is intentionally empty; storing an empty
    // log is fine because the tx will re-execute before its read-set
    // matters.
    shared.scheduler.read_sets().replace(version.tx_idx, result.reads);

    match result.outcome {
        TransactOutcome::Completed { result_and_state } => {
            // Push writes into MvMemory, then install the write set.
            let write_locations = commit_writes_to_mv(
                shared.scheduler.mv_memory(),
                version,
                &result_and_state.state,
            );
            let added_new = shared
                .scheduler
                .install_write_set(version.tx_idx, write_locations);

            // Store the successful result.
            *shared.results[version.tx_idx].lock() = Some(Ok(result_and_state));

            shared.scheduler.finish_execution(version, added_new);
        }
        TransactOutcome::Blocked { blocking_tx_idx } => {
            // Undo the re-execution counter bump — this attempt didn't
            // really execute, so it shouldn't count against the budget.
            shared.reexec_counts[version.tx_idx].fetch_sub(1, Ordering::Relaxed);
            shared
                .scheduler
                .abort_in_flight_execution(version, blocking_tx_idx);
        }
        TransactOutcome::StorageError(msg) => {
            let mut slot = shared.fatal.lock();
            if slot.is_none() {
                *slot = Some(Error::Storage(msg));
            }
            shared
                .scheduler
                .abort_in_flight_execution(version, version.tx_idx);
        }
        TransactOutcome::ValidationError(msg) => {
            // Tx-level failure: record as invalid, no writes to MvMemory.
            *shared.results[version.tx_idx].lock() = Some(Err(msg));
            shared.scheduler.install_write_set(version.tx_idx, vec![]);
            shared.scheduler.finish_execution(version, false);
        }
    }
}

fn handle_validate<VB, S>(shared: &WorkerShared<'_, VB, S>, version: Version)
where
    VB: VmBuilder,
    S: Storage,
{
    let reads = shared.scheduler.read_sets().snapshot(version.tx_idx);
    let ok = validate_read_set(&reads, shared.scheduler.mv_memory(), version.tx_idx);
    shared.scheduler.finish_validation(version, !ok);
}

/// Check every recorded read against the current state of MvMemory.
/// Returns `true` if all reads still resolve to the same origin; `false`
/// if any read is stale.
pub(crate) fn validate_read_set(reads: &[ReadLog], mv: &MvMemory, tx_idx: TxIdx) -> bool {
    for log in reads {
        match mv.read(&log.location, tx_idx) {
            ReadOutcome::Versioned { writer, .. } => {
                if log.origin != ReadOrigin::Versioned(writer) {
                    return false;
                }
            }
            ReadOutcome::NotFound => {
                if log.origin != ReadOrigin::Storage {
                    return false;
                }
            }
            ReadOutcome::Blocked { .. } => {
                // An earlier tx's write is now an Estimate; our read is
                // definitively stale.
                return false;
            }
        }
    }
    true
}

/// Extract all written locations from an EVM state diff and stage them in
/// MvMemory. Returns the list of locations touched — fed to
/// [`Scheduler::install_write_set`] for re-incarnation cleanup accounting.
///
/// # Classification
///
/// - Any account in `state` with `status.is_selfdestructed()`: write
///   `MemoryLocation::Basic(addr)` = `MemoryValue::SelfDestructed`.
/// - Otherwise, any `account.is_touched()` (EIP-161: account info may
///   have changed): write `MemoryLocation::Basic(addr)` =
///   `MemoryValue::Basic(info)`.
/// - On account creation with code: write `MemoryLocation::Code(code_hash)`
///   = `MemoryValue::Code(bytecode)` so later txs can resolve a CALL to
///   the new contract.
/// - Each `storage` slot where `is_changed()`: write
///   `MemoryLocation::Storage(addr, slot)` = `MemoryValue::Storage(value)`.
pub(crate) fn commit_writes_to_mv(
    mv: &MvMemory,
    version: Version,
    state: &EvmState,
) -> Vec<MemoryLocation> {
    let mut locations = Vec::new();
    for (addr, account) in state.iter() {
        write_account(mv, version, *addr, account, &mut locations);
    }
    locations
}

fn write_account(
    mv: &MvMemory,
    version: Version,
    addr: alloy_primitives::Address,
    account: &Account,
    locations: &mut Vec<MemoryLocation>,
) {
    if account.is_selfdestructed() {
        let loc = MemoryLocation::Basic(addr);
        mv.write(loc, version, MemoryValue::SelfDestructed);
        locations.push(loc);
        // Selfdestructed accounts also "clear" their storage — but we
        // don't need to explicitly delete each slot from MvMemory. A later
        // read through MvMemory sees the SelfDestructed marker on Basic;
        // storage reads on a self-destructed account return zero (handled
        // in the DbWrapper).
        return;
    }

    if !account.is_touched() {
        // Not touched — no state change at all (revm marks cold-only
        // reads without modifications).
        return;
    }

    // Basic (info) write.
    let loc = MemoryLocation::Basic(addr);
    mv.write(loc, version, MemoryValue::Basic(account.info.clone()));
    locations.push(loc);

    // New contract with code: also stage the bytecode by hash so later
    // CALLs to this address can resolve the code.
    if account.is_created() {
        if let Some(bytecode) = &account.info.code {
            let code_loc = MemoryLocation::Code(account.info.code_hash);
            mv.write(code_loc, version, MemoryValue::Code(bytecode.clone()));
            locations.push(code_loc);
        }
    }

    // Storage slots.
    for (slot, storage_slot) in account.storage.iter() {
        if storage_slot.is_changed() {
            let s_loc = MemoryLocation::Storage(addr, *slot);
            mv.write(
                s_loc,
                version,
                MemoryValue::Storage(storage_slot.present_value),
            );
            locations.push(s_loc);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv_memory::{MvMemory, Version};
    use crate::read_set::ReadLog;
    use alloy_primitives::{address, U256};

    #[test]
    fn validate_empty_read_set_is_ok() {
        let mv = MvMemory::new();
        assert!(validate_read_set(&[], &mv, 5));
    }

    #[test]
    fn validate_versioned_read_against_unchanged_mv_is_ok() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!(
            "0x0000000000000000000000000000000000000001"
        ));
        let v = Version { tx_idx: 2, incarnation: 0 };
        mv.write(loc, v, MemoryValue::Storage(U256::ZERO));
        // (intentional MemoryValue::Storage — validator doesn't inspect
        // value, only version.)

        let reads = vec![ReadLog {
            location: loc,
            origin: ReadOrigin::Versioned(v),
        }];
        assert!(validate_read_set(&reads, &mv, 5));
    }

    #[test]
    fn validate_versioned_read_fails_when_writer_version_changes() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!(
            "0x0000000000000000000000000000000000000001"
        ));
        mv.write(
            loc,
            Version { tx_idx: 2, incarnation: 0 },
            MemoryValue::Storage(U256::ZERO),
        );
        // Now a newer incarnation replaces it. The read log's observed
        // version is stale.
        mv.write(
            loc,
            Version { tx_idx: 2, incarnation: 1 },
            MemoryValue::Storage(U256::from(99)),
        );

        let reads = vec![ReadLog {
            location: loc,
            origin: ReadOrigin::Versioned(Version { tx_idx: 2, incarnation: 0 }),
        }];
        assert!(!validate_read_set(&reads, &mv, 5));
    }

    #[test]
    fn validate_storage_read_fails_when_earlier_tx_now_writes_there() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!(
            "0x0000000000000000000000000000000000000002"
        ));
        // Initially nothing at this location; reader recorded Storage
        // origin.
        let reads = vec![ReadLog {
            location: loc,
            origin: ReadOrigin::Storage,
        }];
        assert!(validate_read_set(&reads, &mv, 5));

        // An earlier tx writes here — reader's "Storage" origin is now stale.
        mv.write(
            loc,
            Version { tx_idx: 3, incarnation: 0 },
            MemoryValue::Storage(U256::from(5)),
        );
        assert!(!validate_read_set(&reads, &mv, 5));
    }

    #[test]
    fn validate_blocked_always_fails() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!(
            "0x0000000000000000000000000000000000000003"
        ));
        mv.write(
            loc,
            Version { tx_idx: 2, incarnation: 0 },
            MemoryValue::Storage(U256::ZERO),
        );
        assert!(mv.mark_estimate(loc, 2));

        let reads = vec![ReadLog {
            location: loc,
            origin: ReadOrigin::Versioned(Version { tx_idx: 2, incarnation: 0 }),
        }];
        assert!(!validate_read_set(&reads, &mv, 5));
    }
}
