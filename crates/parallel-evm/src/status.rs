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

    /// Transition `Executing(inc)` -> `Aborting(inc)` for mid-execution
    /// aborts — specifically, the case where a worker hit a `Blocked` read
    /// in MvMemory and can't complete its incarnation. Re-execution picks
    /// up at `inc + 1` via the normal [`begin_execution`] path.
    ///
    /// Returns `true` iff the transition happened. Failure cases mirror
    /// [`try_abort`]: stale attempts to abort a phase that's moved on.
    pub fn abort_executing(&self, tx_idx: usize, inc: Incarnation) -> bool {
        let mut state = self.txs[tx_idx].lock();
        if state.phase == Phase::Executing(inc) {
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

    // Deliberately no `wake()` method here. Dependents drained by
    // [`finish_execution`] already have their phase set correctly by
    // whoever parked them:
    //
    // - The worker that hit a `Blocked` read called
    //   [`abort_executing`], so the dependent sits in `Aborting(inc)`.
    // - When the scheduler's `finish_execution` pushes `execution_idx`
    //   down to the min dependent, `next_task` re-picks the dependent and
    //   `begin_execution` transitions it to `Executing(inc + 1)`.
    //
    // An earlier iteration flipped dependents to `ReadyToExecute`, which
    // lost the incarnation counter and caused `MvMemory` to accumulate
    // stale writes. Never add that back without a very good reason.
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
    fn abort_executing_transitions_phase_but_preserves_inc() {
        let s = StatusVector::new(1);
        s.begin_execution(0);
        assert_eq!(s.phase(0), Phase::Executing(0));
        assert!(s.abort_executing(0, 0));
        assert_eq!(s.phase(0), Phase::Aborting(0));
        // A second abort on the same incarnation is a no-op.
        assert!(!s.abort_executing(0, 0));
        // begin_execution now bumps to inc 1.
        assert_eq!(s.begin_execution(0), Some(1));
    }

    #[test]
    fn abort_executing_rejects_non_executing_phases() {
        let s = StatusVector::new(1);
        // ReadyToExecute
        assert!(!s.abort_executing(0, 0));
        // After successful execute + try_abort → Aborting
        s.begin_execution(0);
        s.finish_execution(0, 0);
        s.try_abort(0, 0);
        assert!(!s.abort_executing(0, 0));
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
