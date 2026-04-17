//! Block-STM task scheduler (Gelashvili et al., 2022, §4).
//!
//! # Role
//!
//! The scheduler decides what each worker does next: run an execution of
//! transaction `i` at incarnation `n`, run a validation of `i`'s last
//! execution, or exit because the block is complete. It's the only piece of
//! the algorithm that touches the global "which txs are in flight" state;
//! everything else operates on `MvMemory` + per-tx structures.
//!
//! # Atomics (from paper §4.4)
//!
//! - `execution_idx`: next tx index a worker would try to execute. Fetch-add
//!   to claim. Pushed down (toward 0) when a validation aborts tx `i` — we
//!   need to re-execute `i`.
//!
//! - `validation_idx`: next tx index a worker would try to validate. Fetch-add
//!   to claim. Pushed down when an execution of tx `i` writes to a NEW
//!   location (compared to its previous incarnation) — every validation at
//!   `>= i` may now be stale.
//!
//! - `decrease_cnt`: incremented every time `execution_idx` or
//!   `validation_idx` is pushed down. Used by `next_task` to detect
//!   concurrent "index went down" races that would otherwise falsely
//!   conclude the block is finished.
//!
//! - `num_active_tasks`: in-flight task count. Incremented by
//!   `next_task` when it hands out a task, decremented by
//!   `finish_execution` / `finish_validation`.
//!
//! - `done_marker`: sticky flag set the first time we observe "all indices
//!   at ceiling AND no active tasks AND no concurrent index decrease". Once
//!   set, workers exit.
//!
//! # Ordering
//!
//! Using `SeqCst` throughout the first version. The scheduler is a small
//! fraction of total time vs. EVM execution; we'll tighten to `AcqRel` /
//! `Acquire` only after profiling shows it's a hot loop.

use crate::{
    mv_memory::{MvMemory, TxIdx, Version},
    read_set::ReadSetStore,
    status::StatusVector,
};
use parking_lot::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};

/// Work unit handed out by the scheduler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Task {
    /// Execute transaction `version.tx_idx` at `version.incarnation`.
    Execute(Version),
    /// Validate that transaction `version.tx_idx`'s last execution
    /// (at `version.incarnation`) still matches current state.
    Validate(Version),
}

/// Shared scheduler state. Clone-free: workers hold `Arc<Scheduler>`.
#[derive(Debug)]
pub struct Scheduler {
    /// Total txs in the block. Immutable after construction.
    num_txs: usize,

    /// Counter: next tx index to try executing. Monotonically non-decreasing
    /// under steady state; lowered when validation aborts push `tx_idx`
    /// back into the executable set.
    execution_idx: AtomicUsize,

    /// Counter: next tx index to try validating.
    validation_idx: AtomicUsize,

    /// Bumped whenever execution_idx or validation_idx is pushed down.
    /// `next_task` uses this to detect concurrent decreases and avoid a
    /// false "block is done" conclusion.
    decrease_cnt: AtomicU32,

    /// Tasks currently checked out to workers (execute or validate). Reaches
    /// zero only when the block is truly idle.
    num_active_tasks: AtomicU32,

    /// Sticky: once set, workers exit `next_task` with `None`.
    done_marker: AtomicBool,

    /// Per-tx phase vector.
    status: StatusVector,

    /// Per-tx read log for validation.
    read_sets: ReadSetStore,

    /// Multi-version memory — scheduler owns it because
    /// `mark_abort`/validate have to coordinate with it.
    mv_memory: MvMemory,

    /// Per-tx write set: the locations this tx wrote on its current
    /// incarnation. Used on re-incarnation to clean up stale writes from
    /// the previous attempt. Stored here (not in `MvMemory`) because it's
    /// keyed by tx, not by location.
    write_sets: Vec<Mutex<Vec<crate::mv_memory::MemoryLocation>>>,
}

impl Scheduler {
    /// Build a scheduler for a block of `num_txs` transactions.
    pub fn new(num_txs: usize) -> Self {
        let mut write_sets = Vec::with_capacity(num_txs);
        for _ in 0..num_txs {
            write_sets.push(Mutex::new(Vec::new()));
        }
        Self {
            num_txs,
            execution_idx: AtomicUsize::new(0),
            validation_idx: AtomicUsize::new(0),
            decrease_cnt: AtomicU32::new(0),
            num_active_tasks: AtomicU32::new(0),
            done_marker: AtomicBool::new(false),
            status: StatusVector::new(num_txs),
            read_sets: ReadSetStore::new(num_txs),
            mv_memory: MvMemory::new(),
            write_sets,
        }
    }

    /// Total number of transactions this scheduler coordinates.
    pub fn num_txs(&self) -> usize {
        self.num_txs
    }

    /// Borrow the multi-version memory for workers' DB-wrapper reads /
    /// writes. The scheduler guarantees only task-related mutations; the
    /// callers own read/write ops inline.
    pub fn mv_memory(&self) -> &MvMemory {
        &self.mv_memory
    }

    /// Borrow the read-set store so worker code can deposit a new read log
    /// on finish_execution.
    pub fn read_sets(&self) -> &ReadSetStore {
        &self.read_sets
    }

    /// Return the next task for a worker, or `None` when the block is done.
    ///
    /// Contract:
    /// - `None` is sticky (once returned, every subsequent call returns `None`
    ///   too) — workers can safely exit.
    /// - Does not block indefinitely. Spins briefly while checking for
    ///   concurrent decreases; returns as soon as a task is claimed or
    ///   done-ness is confirmed.
    pub fn next_task(&self) -> Option<Task> {
        if self.done_marker.load(Ordering::SeqCst) {
            return None;
        }
        loop {
            // Snapshot decrease_cnt before trying — used as the "nothing
            // changed while I looked" guard on the done check below.
            let start_decrease = self.decrease_cnt.load(Ordering::SeqCst);

            // 1. Prefer validation (validates what's executed, unblocks "done").
            let val_idx = self.validation_idx.load(Ordering::SeqCst);
            let exec_idx = self.execution_idx.load(Ordering::SeqCst);

            // A validation makes sense only when the target tx has been
            // executed at least once — i.e. val_idx < exec_idx. And we need
            // val_idx < num_txs to have something to claim.
            if val_idx < exec_idx && val_idx < self.num_txs {
                if let Some(task) = self.try_next_validation() {
                    return Some(task);
                }
            }

            // 2. Otherwise try execution.
            if self.execution_idx.load(Ordering::SeqCst) < self.num_txs {
                if let Some(task) = self.try_next_execution() {
                    return Some(task);
                }
            }

            // 3. Check done: all indices at ceiling, zero active, no
            //    concurrent decrease. Paper §4.4.
            if self.check_done(start_decrease) {
                return None;
            }

            // Something's in flight; another worker will unblock us. Spin.
            std::hint::spin_loop();
        }
    }

    /// Finish an execution task.
    ///
    /// `wrote_new_location` signals whether this execution wrote a location
    /// that the previous incarnation did NOT — used for paper's "re-cover
    /// downstream validations" push; subsumed by the unconditional push
    /// below but kept as a parameter so callers can still signal it (and we
    /// can tighten the push range later if profiling shows validation churn).
    ///
    /// After this call, the caller must obtain the next task via
    /// [`next_task`][Self::next_task]. No task is handed back directly;
    /// accounting stays simple because every task is exactly one
    /// `next_task` / `finish_*` pair.
    pub fn finish_execution(&self, version: Version, wrote_new_location: bool) {
        let dependents = self.status.finish_execution(version.tx_idx, version.incarnation);

        // Dependents need to re-execute. Wake them (flip to Ready), and
        // push execution_idx down to the earliest of them so the scheduler
        // picks them up.
        if !dependents.is_empty() {
            self.status.wake(&dependents);
            let min_dep = *dependents.iter().min().expect("non-empty");
            self.decrease_atomic(&self.execution_idx, min_dep);
        }

        // Crucial: push validation_idx down to THIS tx so next_task picks
        // up its validation. Without this, the paper's "fetch_add then
        // skip non-Executed" pattern loses the validation slot permanently
        // when our fetch_add happened while the tx was still Executing.
        //
        // `wrote_new_location` would nominally push to tx_idx + 1 (re-cover
        // downstream validations); tx_idx is strictly lower so we push
        // there, which covers both cases. Future tightening: if we can
        // distinguish the two paths cheaply, push to the larger of the two
        // minima to avoid wasted revalidations. Not a correctness concern.
        self.decrease_atomic(&self.validation_idx, version.tx_idx);
        let _ = wrote_new_location;

        // This task has been fully processed; let go of the in-flight slot.
        self.num_active_tasks.fetch_sub(1, Ordering::SeqCst);
    }

    /// Finish a validation task.
    ///
    /// `aborted` is the validator's verdict: `true` if any read was stale.
    /// On abort, this scheduler flips the tx's writes to `Estimate`, resets
    /// the tx to `Aborting`, and pushes `execution_idx` down so it gets
    /// re-executed.
    ///
    /// Callers obtain the next task via [`next_task`][Self::next_task].
    pub fn finish_validation(&self, version: Version, aborted: bool) {
        if aborted && self.status.try_abort(version.tx_idx, version.incarnation) {
            // The aborted incarnation's writes are now suspect; mark them
            // Estimate so later readers park on them instead of using a
            // stale value. `write_sets[tx_idx]` retains the list so the
            // next incarnation's `install_write_set` can reconcile.
            let writes = self.write_sets[version.tx_idx].lock().clone();
            for loc in &writes {
                self.mv_memory.mark_estimate(*loc, version.tx_idx);
            }
            // Tx needs a new incarnation.
            self.decrease_atomic(&self.execution_idx, version.tx_idx);
        }

        self.num_active_tasks.fetch_sub(1, Ordering::SeqCst);
    }

    /// Record the write set produced by an execution. Called by the worker
    /// AFTER the EVM finishes and BEFORE `finish_execution`. Return value
    /// indicates whether this write set added any NEW location vs. the
    /// previous incarnation's — the worker feeds that into
    /// `finish_execution`'s `wrote_new_location`.
    ///
    /// Also clears any previously-Estimated writes from the prior
    /// incarnation that the new one did NOT re-write — those locations
    /// reverted to "never written by this tx", so `MvMemory` must forget
    /// them.
    pub fn install_write_set(
        &self,
        tx_idx: TxIdx,
        new_writes: Vec<crate::mv_memory::MemoryLocation>,
    ) -> bool {
        use crate::mv_memory::MemoryLocation;
        let mut slot = self.write_sets[tx_idx].lock();
        let previous: Vec<MemoryLocation> = std::mem::take(&mut *slot);

        // Locations previously written but not re-written must be removed
        // (they were Estimates from the abort). Those reads had
        // "NotFound after cleanup" — still correct because validation
        // treats a read origin and current read consistently.
        let new_set: std::collections::HashSet<MemoryLocation> =
            new_writes.iter().copied().collect();
        for prev_loc in &previous {
            if !new_set.contains(prev_loc) {
                self.mv_memory.remove(*prev_loc, tx_idx);
            }
        }

        // Determine if any new location exists that wasn't in `previous`.
        let prev_set: std::collections::HashSet<MemoryLocation> =
            previous.iter().copied().collect();
        let added_new = new_writes.iter().any(|loc| !prev_set.contains(loc));

        *slot = new_writes;
        added_new
    }

    /// Claim the next execution slot.
    fn try_next_execution(&self) -> Option<Task> {
        let idx = self.execution_idx.fetch_add(1, Ordering::SeqCst);
        if idx >= self.num_txs {
            return None;
        }
        // begin_execution returns Some(inc) iff the phase was Ready or
        // Aborting; None means another worker already claimed it (or it's
        // Executed and waiting for validation).
        let inc = self.status.begin_execution(idx)?;
        self.num_active_tasks.fetch_add(1, Ordering::SeqCst);
        Some(Task::Execute(Version {
            tx_idx: idx,
            incarnation: inc,
        }))
    }

    /// Claim the next validation slot.
    fn try_next_validation(&self) -> Option<Task> {
        let idx = self.validation_idx.fetch_add(1, Ordering::SeqCst);
        if idx >= self.num_txs {
            return None;
        }
        // Only Executed txs can be validated. Phase read without taking
        // the execute-path lock path — begin_execution's CAS-equivalent
        // has already made `Executed(inc)` visible if an execution happened.
        match self.status.phase(idx) {
            crate::status::Phase::Executed(inc) => {
                self.num_active_tasks.fetch_add(1, Ordering::SeqCst);
                Some(Task::Validate(Version { tx_idx: idx, incarnation: inc }))
            }
            _ => None,
        }
    }

    /// Atomically lower `atom` to `new_val` if `new_val < current`. Bumps
    /// `decrease_cnt` iff the value was actually lowered.
    ///
    /// Equivalent to `fetch_min` (which AtomicUsize doesn't have natively).
    fn decrease_atomic(&self, atom: &AtomicUsize, new_val: usize) {
        let mut current = atom.load(Ordering::SeqCst);
        while new_val < current {
            match atom.compare_exchange(current, new_val, Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => {
                    self.decrease_cnt.fetch_add(1, Ordering::SeqCst);
                    return;
                }
                Err(actual) => current = actual,
            }
        }
    }

    /// Check whether the block is complete.
    fn check_done(&self, start_decrease: u32) -> bool {
        if self.num_active_tasks.load(Ordering::SeqCst) != 0 {
            return false;
        }
        if self.execution_idx.load(Ordering::SeqCst) < self.num_txs {
            return false;
        }
        if self.validation_idx.load(Ordering::SeqCst) < self.num_txs {
            return false;
        }
        // No one decreased an index while we looked — otherwise there's a
        // race where a just-aborted tx lowered the index and we'd miss it.
        if self.decrease_cnt.load(Ordering::SeqCst) != start_decrease {
            return false;
        }
        self.done_marker.store(true, Ordering::SeqCst);
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::mv_memory::Incarnation;

    fn ver(tx: TxIdx, inc: Incarnation) -> Version {
        Version {
            tx_idx: tx,
            incarnation: inc,
        }
    }

    #[test]
    fn empty_block_is_immediately_done() {
        let sch = Scheduler::new(0);
        assert!(sch.next_task().is_none());
        assert!(sch.next_task().is_none());
    }

    /// Drain a scheduler synchronously, running `handle` on each task. Test
    /// helper that exercises the full lifecycle as the public API expects.
    fn drain<F: FnMut(&Scheduler, Task)>(sch: &Scheduler, mut handle: F) -> Vec<Task> {
        let mut seen = Vec::new();
        while let Some(task) = sch.next_task() {
            seen.push(task);
            handle(sch, task);
        }
        seen
    }

    #[test]
    fn single_tx_executes_then_validates_then_done() {
        let sch = Scheduler::new(1);
        let seen = drain(&sch, |sch, task| match task {
            Task::Execute(v) => {
                sch.install_write_set(v.tx_idx, vec![]);
                sch.finish_execution(v, false);
            }
            Task::Validate(v) => {
                sch.finish_validation(v, false);
            }
        });
        assert_eq!(seen, vec![Task::Execute(ver(0, 0)), Task::Validate(ver(0, 0))]);
    }

    #[test]
    fn validation_abort_triggers_re_execution() {
        let sch = Scheduler::new(1);
        // Abort tx 0's first validation; it must re-execute at inc 1 and
        // re-validate. Then the scheduler terminates.
        let mut aborted_once = false;
        let seen = drain(&sch, |sch, task| match task {
            Task::Execute(v) => {
                sch.install_write_set(v.tx_idx, vec![]);
                sch.finish_execution(v, false);
            }
            Task::Validate(v) => {
                if !aborted_once && v.incarnation == 0 {
                    aborted_once = true;
                    sch.finish_validation(v, true); // abort
                } else {
                    sch.finish_validation(v, false);
                }
            }
        });
        // Expected trace: Execute(0,0) → Validate(0,0) (aborted)
        //                 → Execute(0,1) → Validate(0,1) (ok) → done.
        assert_eq!(
            seen,
            vec![
                Task::Execute(ver(0, 0)),
                Task::Validate(ver(0, 0)),
                Task::Execute(ver(0, 1)),
                Task::Validate(ver(0, 1)),
            ]
        );
    }

    #[test]
    fn two_independent_txs_complete() {
        let sch = Scheduler::new(2);
        let seen = drain(&sch, |sch, task| match task {
            Task::Execute(v) => {
                sch.install_write_set(v.tx_idx, vec![]);
                sch.finish_execution(v, false);
            }
            Task::Validate(v) => {
                sch.finish_validation(v, false);
            }
        });
        // Both txs executed exactly once and validated exactly once.
        let executes: Vec<_> = seen
            .iter()
            .filter_map(|t| match t {
                Task::Execute(v) => Some(v.tx_idx),
                _ => None,
            })
            .collect();
        let validates: Vec<_> = seen
            .iter()
            .filter_map(|t| match t {
                Task::Validate(v) => Some(v.tx_idx),
                _ => None,
            })
            .collect();
        assert_eq!(executes.len(), 2);
        assert_eq!(validates.len(), 2);
        let mut e_sorted = executes.clone();
        e_sorted.sort();
        assert_eq!(e_sorted, vec![0, 1]);
        let mut v_sorted = validates.clone();
        v_sorted.sort();
        assert_eq!(v_sorted, vec![0, 1]);
    }

    #[test]
    fn execution_wrote_new_location_forces_later_validations_redo() {
        use crate::mv_memory::MemoryLocation;
        use alloy_primitives::address;
        let loc = MemoryLocation::Basic(address!(
            "0x0000000000000000000000000000000000000001"
        ));

        let sch = Scheduler::new(2);

        // Force the specific interleaving: tx 0 CLAIMS execution (but
        // doesn't finish); tx 1 claims, executes, validates successfully;
        // THEN tx 0 finishes with a new write — which must push val_idx
        // back to 1 so tx 1 re-validates.
        //
        // Two workers acting sequentially via manual next_task calls.
        let claim_0 = sch.next_task().expect("claim tx 0");
        let claim_1 = sch.next_task().expect("claim tx 1");
        assert_eq!(claim_0, Task::Execute(ver(0, 0)));
        assert_eq!(claim_1, Task::Execute(ver(1, 0)));

        // Tx 1 finishes first (no writes).
        sch.install_write_set(1, vec![]);
        sch.finish_execution(ver(1, 0), false);

        // Tx 1 now validates (via next_task).
        let t = sch.next_task().expect("tx 1 validation");
        assert_eq!(t, Task::Validate(ver(1, 0)));
        sch.finish_validation(ver(1, 0), false);

        // Now tx 0 finishes — writes a new location that tx 1 may have
        // read (we don't model the read here; we're testing the scheduler
        // push behavior).
        let added = sch.install_write_set(0, vec![loc]);
        assert!(added);
        sch.finish_execution(ver(0, 0), added);

        // Drain remaining tasks. The scheduler should hand out both a
        // validation of tx 0 AND a re-validation of tx 1 (because tx 0's
        // write pushed validation_idx back down to tx_idx=0, which
        // re-covers 0 and 1).
        let remaining: Vec<Task> = {
            let mut v = Vec::new();
            while let Some(t) = sch.next_task() {
                v.push(t);
                if let Task::Validate(ver) = t {
                    sch.finish_validation(ver, false);
                }
            }
            v
        };

        let validations_of_1: usize = remaining
            .iter()
            .filter(|t| matches!(t, Task::Validate(v) if v.tx_idx == 1))
            .count();
        let validations_of_0: usize = remaining
            .iter()
            .filter(|t| matches!(t, Task::Validate(v) if v.tx_idx == 0))
            .count();

        assert!(
            validations_of_0 >= 1,
            "tx 0 must be validated; remaining trace: {remaining:?}"
        );
        assert!(
            validations_of_1 >= 1,
            "tx 1 must be re-validated after tx 0's write; remaining trace: {remaining:?}"
        );
    }

    #[test]
    fn many_independent_txs_complete_under_concurrent_workers() {
        use std::sync::Arc;
        use std::thread;

        const NUM_TXS: usize = 64;
        const NUM_WORKERS: usize = 8;

        let sch = Arc::new(Scheduler::new(NUM_TXS));

        let handles: Vec<_> = (0..NUM_WORKERS)
            .map(|_| {
                let sch = Arc::clone(&sch);
                thread::spawn(move || {
                    while let Some(task) = sch.next_task() {
                        match task {
                            Task::Execute(v) => {
                                sch.install_write_set(v.tx_idx, vec![]);
                                sch.finish_execution(v, false);
                            }
                            Task::Validate(v) => {
                                sch.finish_validation(v, false);
                            }
                        }
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        // Every tx is Executed; no lingering in-flight work.
        for i in 0..NUM_TXS {
            assert_eq!(
                sch.status.phase(i),
                crate::status::Phase::Executed(0),
                "tx {i} final phase"
            );
        }
    }
}
