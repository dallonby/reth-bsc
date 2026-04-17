//! Per-transaction status vector used by the scheduler.
//!
//! Split out from `scheduler.rs` because it's independent of task-assignment
//! policy and has its own test surface.
//!
//! # Phase lifecycle
//!
//! ```text
//!           ┌────────────────────────────────────────────┐
//!           │                                            │
//!           ▼                                            │
//!     ReadyToExecute                                     │
//!           │                                            │
//!   (next_execution task claimed)                        │
//!           ▼                                            │
//!       Executing(inc)                                   │
//!           │                                            │
//!   (finish_execution)                                   │
//!           ▼                                            │
//!       Executed(inc) ─────(try_validation_abort)───► Aborting(inc)
//!           │                                            │
//!   (finish_validation, valid)                    (scheduler assigns
//!           │                                      execution again)
//!           ▼                                            │
//!        ≡ still Executed ≡                              │
//!                                                        │
//!   validation failure eventually calls try_validation_abort ─┘
//! ```
//!
//! A successful validation does not change the phase — the tx stays
//! `Executed(inc)` and may be validated again later if any earlier tx
//! re-executes. Only a failed validation forces `Aborting(inc)`, which the
//! execution-task loop picks up and transitions to `Executing(inc+1)`.

use crate::mv_memory::Incarnation;
use parking_lot::Mutex;

/// Per-tx scheduling phase. Packed separately from the wait-list so the
/// scheduler can query status without holding the wait-list lock.
///
/// The `Incarnation` carried by `Executing` / `Executed` / `Aborting` is the
/// CURRENT attempt — it increments when transitioning Aborting -> Executing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Phase {
    /// The tx has not yet been executed, or its last incarnation was
    /// aborted and a new one has not yet been claimed.
    ///
    /// The only transition out is `begin_execution` (scheduler assigns a
    /// worker), which moves it to `Executing(next_incarnation)`.
    ReadyToExecute,

    /// A worker is currently running the tx at the given incarnation.
    Executing(Incarnation),

    /// Execution of the given incarnation has completed. The tx's write set
    /// is in MvMemory; its read set is in the scheduler's read-set store.
    /// The tx stays in this phase across successful validations — only a
    /// failed validation pushes it to `Aborting`.
    Executed(Incarnation),

    /// A validator concluded that the given incarnation's read set was
    /// stale. MvMemory will have had the tx's writes flipped to `Estimate`
    /// by the scheduler; the next `next_execution` claim transitions this
    /// back to `Executing(inc + 1)`.
    Aborting(Incarnation),
}

/// Full per-tx record: phase + the list of later txs parked on this one.
///
/// Wait-list: when worker W reads a location written by tx B and finds an
/// `Estimate` marker, W's tx A must not proceed until B re-executes. The
/// scheduler records A in B's dependents; when B's next incarnation finishes,
/// every dependent gets flipped back to `ReadyToExecute` and
/// `execution_idx` is lowered so the scheduler picks them up.
///
/// Both fields share a single lock because add-dependency and
/// finish-execution race on the same tx and the ordering matters: we must
/// never append a new dependent after finish-execution has already drained
/// the list, or the dependent will be permanently parked.
#[derive(Debug)]
pub struct TxState {
    phase: Phase,
    /// Indices of later txs parked on this tx. Drained when this tx finishes
    /// a successful incarnation.
    dependents: Vec<usize>,
}

impl TxState {
    pub fn new() -> Self {
        Self {
            phase: Phase::ReadyToExecute,
            dependents: Vec::new(),
        }
    }

    pub fn phase(&self) -> Phase {
        self.phase
    }
}

/// Collection of per-tx state, indexed by `tx_idx`. Each slot is guarded by
/// its own [`Mutex`] so concurrent operations on different txs proceed in
/// parallel. Operations on the *same* tx are serialised — this is deliberate
/// and necessary (see `TxState` rationale).
#[derive(Debug)]
pub struct StatusVector {
    txs: Vec<Mutex<TxState>>,
}

impl StatusVector {
    /// Allocate a status vector for a block of `len` transactions. All slots
    /// start in `ReadyToExecute`.
    pub fn new(len: usize) -> Self {
        let mut txs = Vec::with_capacity(len);
        for _ in 0..len {
            txs.push(Mutex::new(TxState::new()));
        }
        Self { txs }
    }

    /// Number of transactions tracked.
    pub fn len(&self) -> usize {
        self.txs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.txs.is_empty()
    }

    /// Inspect the current phase without mutating. Takes the slot lock
    /// briefly.
    pub fn phase(&self, tx_idx: usize) -> Phase {
        self.txs[tx_idx].lock().phase
    }

    /// Claim `tx_idx` for execution.
    ///
    /// Atomic transition. Returns `Some(incarnation_to_run)` iff the tx is
    /// claimable right now:
    /// - `ReadyToExecute` -> `Executing(0)`, returns `0`.
    /// - `Aborting(n)`    -> `Executing(n + 1)`, returns `n + 1`.
    ///
    /// Returns `None` if the tx is currently `Executing` or `Executed` — the
    /// caller must not run it concurrently.
    pub fn begin_execution(&self, tx_idx: usize) -> Option<Incarnation> {
        let mut state = self.txs[tx_idx].lock();
        match state.phase {
            Phase::ReadyToExecute => {
                state.phase = Phase::Executing(0);
                Some(0)
            }
            Phase::Aborting(prev) => {
                let next = prev + 1;
                state.phase = Phase::Executing(next);
                Some(next)
            }
            Phase::Executing(_) | Phase::Executed(_) => None,
        }
    }

    /// Mark execution complete. Transitions `Executing(inc) -> Executed(inc)`
    /// and drains + returns the list of txs parked on this one.
    ///
    /// Returns the dependents list. Returns an empty `Vec` if the phase was
    /// not `Executing(inc)` (e.g. aborted during execution) — the caller
    /// should still not drop dependents, but our scheduler gates execution
    /// transitions such that this shouldn't happen in practice; returning
    /// empty keeps the API total.
    pub fn finish_execution(&self, tx_idx: usize, inc: Incarnation) -> Vec<usize> {
        let mut state = self.txs[tx_idx].lock();
        if state.phase == Phase::Executing(inc) {
            state.phase = Phase::Executed(inc);
            std::mem::take(&mut state.dependents)
        } else {
            Vec::new()
        }
    }

    /// Attempt to abort `Executed(inc)` -> `Aborting(inc)` for validation.
    ///
    /// Returns `true` iff the transition happened. Failure cases:
    /// - Phase was not `Executed(inc)`: either the tx never executed, a
    ///   newer incarnation has already started, or another validator beat
    ///   us to the abort. In all cases the caller should stop (the block
    ///   state is already in the right shape).
    pub fn try_abort(&self, tx_idx: usize, inc: Incarnation) -> bool {
        let mut state = self.txs[tx_idx].lock();
        if state.phase == Phase::Executed(inc) {
            state.phase = Phase::Aborting(inc);
            true
        } else {
            false
        }
    }

    /// Record that `dependent_tx_idx` is parked on `blocker_tx_idx`.
    ///
    /// Atomic with respect to `finish_execution`: if the blocker has already
    /// finished its current incarnation (phase is `Executed`), returns
    /// `false` and does NOT add the dependent — the caller must retry the
    /// read instead of parking forever.
    pub fn add_dependent(&self, blocker_tx_idx: usize, dependent_tx_idx: usize) -> bool {
        let mut state = self.txs[blocker_tx_idx].lock();
        match state.phase {
            Phase::Executing(_) | Phase::Aborting(_) | Phase::ReadyToExecute => {
                state.dependents.push(dependent_tx_idx);
                true
            }
            Phase::Executed(_) => false,
        }
    }

    /// Flip every dependent back to `ReadyToExecute` so the scheduler
    /// re-picks them. Used by the scheduler after it drains a finished tx's
    /// dependents via [`finish_execution`].
    ///
    /// Dependents that were already beyond `ReadyToExecute` (e.g. they got
    /// unparked by a different blocker in the meantime) are left alone.
    pub fn wake(&self, dependents: &[usize]) {
        for &tx_idx in dependents {
            let mut state = self.txs[tx_idx].lock();
            // Only wake a tx that's still waiting. If it's already
            // Executing/Executed/Aborting someone else is driving it.
            if matches!(state.phase, Phase::ReadyToExecute) {
                // Nothing to do — already wake-able.
                continue;
            }
            // This path is reached when the dependent hadn't actually
            // started executing yet — `add_dependent` is called from inside
            // an executing tx's read path, so by definition the dependent
            // is also Executing at that moment. We flip it back so the
            // execution loop re-claims it fresh.
            //
            // A subtle point: if the dependent is mid-execution when we
            // wake, our flip to ReadyToExecute invalidates its current
            // incarnation. The dependent worker's finish_execution will no
            // longer match phase == Executing(inc), so it becomes a no-op
            // and its write-set landing in MvMemory is fine (it was
            // tentative and will be re-executed). Equivalent semantics to
            // aborting it.
            state.phase = Phase::ReadyToExecute;
            state.dependents.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_slots_are_ready() {
        let s = StatusVector::new(3);
        for i in 0..3 {
            assert_eq!(s.phase(i), Phase::ReadyToExecute);
        }
    }

    #[test]
    fn begin_execution_claims_once() {
        let s = StatusVector::new(1);
        assert_eq!(s.begin_execution(0), Some(0));
        assert_eq!(s.phase(0), Phase::Executing(0));
        // Second claim while Executing must fail.
        assert_eq!(s.begin_execution(0), None);
    }

    #[test]
    fn finish_execution_returns_dependents_and_transitions() {
        let s = StatusVector::new(3);
        s.begin_execution(1);
        assert!(s.add_dependent(1, 2));
        let deps = s.finish_execution(1, 0);
        assert_eq!(deps, vec![2]);
        assert_eq!(s.phase(1), Phase::Executed(0));
    }

    #[test]
    fn finish_execution_with_wrong_incarnation_is_noop() {
        let s = StatusVector::new(1);
        s.begin_execution(0);
        // Pretend caller claims inc 5 finished, but state says inc 0.
        let deps = s.finish_execution(0, 5);
        assert_eq!(deps, Vec::<usize>::new());
        assert_eq!(s.phase(0), Phase::Executing(0)); // unchanged
    }

    #[test]
    fn try_abort_only_succeeds_on_executed() {
        let s = StatusVector::new(1);
        // Not yet run: abort must fail.
        assert!(!s.try_abort(0, 0));

        s.begin_execution(0);
        // Still Executing: abort must fail.
        assert!(!s.try_abort(0, 0));

        s.finish_execution(0, 0);
        // Now Executed(0): abort succeeds exactly once.
        assert!(s.try_abort(0, 0));
        assert_eq!(s.phase(0), Phase::Aborting(0));

        // Second abort attempt on Aborting: fails.
        assert!(!s.try_abort(0, 0));
    }

    #[test]
    fn abort_then_reexecute_increments_incarnation() {
        let s = StatusVector::new(1);
        s.begin_execution(0);
        s.finish_execution(0, 0);
        s.try_abort(0, 0);
        // Next claim bumps incarnation.
        assert_eq!(s.begin_execution(0), Some(1));
        assert_eq!(s.phase(0), Phase::Executing(1));
    }

    #[test]
    fn add_dependent_rejected_after_blocker_finishes() {
        let s = StatusVector::new(3);
        s.begin_execution(1);
        s.finish_execution(1, 0);
        // Blocker (tx 1) already Executed → cannot park tx 2 on it.
        assert!(!s.add_dependent(1, 2));
    }

    #[test]
    fn wake_returns_dependents_to_ready() {
        let s = StatusVector::new(3);
        // tx 2 claims execution, will then park on tx 1.
        s.begin_execution(2);
        s.begin_execution(1);
        assert!(s.add_dependent(1, 2));
        // tx 2 is currently Executing(0). wake() should flip it back.
        s.wake(&[2]);
        assert_eq!(s.phase(2), Phase::ReadyToExecute);
    }

    #[test]
    fn wake_skips_already_ready_dependents() {
        let s = StatusVector::new(2);
        // tx 0 is ReadyToExecute — wake() should leave it alone and not
        // touch the dependents list.
        s.wake(&[0]);
        assert_eq!(s.phase(0), Phase::ReadyToExecute);
    }

    #[test]
    fn status_per_slot_independent_under_concurrency() {
        use std::sync::Arc;
        use std::thread;

        let s = Arc::new(StatusVector::new(100));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let s = Arc::clone(&s);
                thread::spawn(move || {
                    // Each thread drives a disjoint slice of txs through
                    // the lifecycle.
                    let start = t * 10;
                    let end = start + 10;
                    for i in start..end {
                        assert_eq!(s.begin_execution(i), Some(0));
                        let deps = s.finish_execution(i, 0);
                        assert!(deps.is_empty());
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        for i in 0..80 {
            assert_eq!(s.phase(i), Phase::Executed(0));
        }
        for i in 80..100 {
            assert_eq!(s.phase(i), Phase::ReadyToExecute);
        }
    }
}
