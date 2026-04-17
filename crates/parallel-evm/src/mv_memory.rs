//! Multi-version in-memory store backing Block-STM.
//!
//! # Role in the algorithm
//!
//! Block-STM executes transactions in parallel against a shared, versioned
//! view of state. When worker A executes transaction `i`, it reads-through to
//! this structure; when it writes, it records the write keyed by
//! `(location, tx_idx, incarnation)`. Worker B executing transaction `j > i`
//! that reads the same location will see A's write (if it happened-before in
//! tx order), not the underlying database value.
//!
//! The shape of the problem is:
//!
//! - **Outer:** hash map `location → per-location history`. Locations are
//!   hashed and independent across txs, so a sharded concurrent hash (DashMap)
//!   removes almost all cross-thread contention here.
//!
//! - **Inner:** a sorted map `tx_idx → entry`. Reads need to find "the highest
//!   write with `tx_idx < me`"; writes need to replace-or-insert at their own
//!   tx_idx. `BTreeMap` with a `parking_lot::RwLock` gives us both in O(log n).
//!
//! # Invariants
//!
//! 1. There is at most one entry per `(location, tx_idx)` — a re-execution
//!    overwrites its own slot. Incarnation numbers encode the re-execution
//!    count.
//!
//! 2. A validator MUST observe `Estimate` entries during reads. If any read
//!    returns `Estimate`, the reading tx cannot proceed (its source of truth
//!    is being re-written). The scheduler parks the reader on the estimate's
//!    owner.
//!
//! # Not owned here
//!
//! Read-set tracking for validation lives in [`crate::read_set`] — this keeps
//! `MvMemory` focused on the concurrent map and keeps tests small. The
//! scheduler ([`crate::scheduler`]) owns task assignment.

use alloy_primitives::{Address, B256, U256};
use dashmap::DashMap;
use parking_lot::RwLock;
use revm::{bytecode::Bytecode, state::AccountInfo};
use std::collections::BTreeMap;

/// Position of a transaction in the block (0-based).
pub type TxIdx = usize;

/// Re-execution counter. Each time a tx is re-executed (because its read-set
/// was invalidated), its incarnation increments. Used to disambiguate writes
/// from different attempts at the same `tx_idx`.
pub type Incarnation = u32;

/// A specific (tx, attempt) pair. Two writes at the same `tx_idx` but different
/// incarnations are totally ordered: the later incarnation is the current one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Version {
    /// Position of the tx in the block.
    pub tx_idx: TxIdx,
    /// Which re-execution attempt produced this record.
    pub incarnation: Incarnation,
}

/// A location that can be independently read or written.
///
/// The granularity matches revm's database interface: we never read "half an
/// account" or "a range of storage". Serializing at this granularity is the
/// core assumption that lets us parallelize — txs that touch disjoint
/// `MemoryLocation`s cannot conflict.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum MemoryLocation {
    /// Account basics: balance, nonce, code_hash (not code itself).
    Basic(Address),
    /// Storage slot of an account.
    Storage(Address, U256),
    /// Contract code keyed by code hash. Rarely written after deploy but
    /// modeled uniformly here.
    Code(B256),
}

/// Value written to a [`MemoryLocation`].
#[derive(Debug, Clone)]
pub enum MemoryValue {
    Basic(AccountInfo),
    Storage(U256),
    Code(Bytecode),
    /// Account was removed by SELFDESTRUCT. Semantically distinct from "never
    /// existed" because it must invalidate subsequent reads of the same
    /// account.
    SelfDestructed,
}

/// A slot in the per-location history.
#[derive(Debug, Clone)]
pub enum MemoryEntry {
    /// A concrete write produced by `(tx_idx, incarnation)`.
    Write {
        incarnation: Incarnation,
        value: MemoryValue,
    },
    /// The previous write by this `tx_idx` was invalidated when the tx began a
    /// new incarnation. Any read that hits this marker MUST NOT consume the
    /// value — the writer is racing to replace it. The reader either
    /// (a) parks on this tx's completion, or (b) retries and reads through.
    Estimate,
}

/// Read-path outcome when looking up `location` from the perspective of a tx
/// at index `reader_tx_idx`.
#[derive(Debug, Clone)]
pub enum ReadOutcome {
    /// Found a `Write` from `writer` (tx_idx < reader). Carries the value.
    Versioned {
        writer: Version,
        value: MemoryValue,
    },
    /// Found an `Estimate` from `writer`. Reader must wait or retry; it must
    /// not use a value below this entry even if one exists.
    Blocked { blocking_tx_idx: TxIdx },
    /// No entry visible to this reader. Caller should fall through to the
    /// underlying storage.
    NotFound,
}

/// The multi-version memory. Safe to share across worker threads; no external
/// locking is required by callers.
#[derive(Debug, Default)]
pub struct MvMemory {
    /// Outer shard: 16-way by default (DashMap's default). 16 shards is
    /// overkill for small blocks but keeps contention bounded on 500-1000 tx
    /// blocks that touch hot DEX pools.
    data: DashMap<MemoryLocation, RwLock<BTreeMap<TxIdx, MemoryEntry>>>,
}

impl MvMemory {
    /// Construct an empty map. The structure grows as writes land.
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a write at `(tx_idx, incarnation)` for `location = value`.
    ///
    /// If a prior write by the same `tx_idx` existed (from an earlier
    /// incarnation), it's replaced. Writes from *different* `tx_idx` at the
    /// same location coexist — that is the whole point of the structure.
    pub fn write(&self, location: MemoryLocation, version: Version, value: MemoryValue) {
        let shard = self.data.entry(location).or_default();
        let mut inner = shard.value().write();
        inner.insert(
            version.tx_idx,
            MemoryEntry::Write {
                incarnation: version.incarnation,
                value,
            },
        );
    }

    /// Replace the `tx_idx`'s own write at `location` with an `Estimate`
    /// placeholder. Used when a tx enters a new incarnation: the previous
    /// incarnation's write must not be visible as a real value while the new
    /// incarnation is running.
    ///
    /// Returns `true` iff there was a prior write at this `(location, tx_idx)`.
    pub fn mark_estimate(&self, location: MemoryLocation, tx_idx: TxIdx) -> bool {
        let Some(shard) = self.data.get(&location) else {
            return false;
        };
        let mut inner = shard.value().write();
        match inner.get_mut(&tx_idx) {
            Some(entry) => {
                *entry = MemoryEntry::Estimate;
                true
            }
            None => false,
        }
    }

    /// Remove the `tx_idx`'s entry entirely at `location`. Used when a tx's
    /// re-execution produces a write-set strictly smaller than its previous
    /// incarnation's — the locations that are no longer written must be
    /// cleaned up.
    ///
    /// Returns `true` iff an entry existed.
    pub fn remove(&self, location: MemoryLocation, tx_idx: TxIdx) -> bool {
        let Some(shard) = self.data.get(&location) else {
            return false;
        };
        let mut inner = shard.value().write();
        inner.remove(&tx_idx).is_some()
    }

    /// Read `location` from the perspective of the tx at `reader_tx_idx`.
    ///
    /// The returned value comes from the highest `tx_idx < reader_tx_idx`
    /// entry. An `Estimate` at that position becomes [`ReadOutcome::Blocked`]
    /// and the reader MUST NOT look further back — doing so would silently
    /// use a stale write.
    pub fn read(&self, location: &MemoryLocation, reader_tx_idx: TxIdx) -> ReadOutcome {
        let Some(shard) = self.data.get(location) else {
            return ReadOutcome::NotFound;
        };
        let inner = shard.value().read();
        // `range(..reader_tx_idx).next_back()` gives us the highest key strictly
        // below the reader — exactly the Block-STM "version immediately before
        // me" semantic.
        let Some((&writer_tx_idx, entry)) = inner.range(..reader_tx_idx).next_back() else {
            return ReadOutcome::NotFound;
        };
        match entry {
            MemoryEntry::Write { incarnation, value } => ReadOutcome::Versioned {
                writer: Version {
                    tx_idx: writer_tx_idx,
                    incarnation: *incarnation,
                },
                value: value.clone(),
            },
            MemoryEntry::Estimate => ReadOutcome::Blocked {
                blocking_tx_idx: writer_tx_idx,
            },
        }
    }

    /// Number of distinct locations with at least one entry. For tests and
    /// diagnostics; not a hot-path operation.
    pub fn locations_len(&self) -> usize {
        self.data.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::{address, b256};

    fn info(balance: u128) -> AccountInfo {
        AccountInfo {
            balance: U256::from(balance),
            nonce: 0,
            code_hash: b256!("0x0000000000000000000000000000000000000000000000000000000000000000"),
            account_id: None,
            code: None,
        }
    }

    fn v(tx_idx: TxIdx, incarnation: Incarnation) -> Version {
        Version { tx_idx, incarnation }
    }

    #[test]
    fn empty_read_is_not_found() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        assert_matches::assert_matches!(mv.read(&loc, 5), ReadOutcome::NotFound);
    }

    #[test]
    fn single_write_is_visible_to_later_reader() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        mv.write(loc, v(2, 0), MemoryValue::Basic(info(100)));

        // Tx 3 should see tx 2's write.
        let got = mv.read(&loc, 3);
        match got {
            ReadOutcome::Versioned { writer, value } => {
                assert_eq!(writer, v(2, 0));
                assert_matches::assert_matches!(
                    value,
                    MemoryValue::Basic(AccountInfo { balance, .. }) if balance == U256::from(100)
                );
            }
            other => panic!("unexpected read outcome: {other:?}"),
        }
    }

    #[test]
    fn reader_does_not_see_its_own_write_or_later_writes() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        mv.write(loc, v(5, 0), MemoryValue::Basic(info(500)));
        mv.write(loc, v(10, 0), MemoryValue::Basic(info(1000)));

        // Tx 5 reading its own location must not see its own write.
        assert_matches::assert_matches!(mv.read(&loc, 5), ReadOutcome::NotFound);

        // Tx 7 reading should see tx 5's write, NOT tx 10's.
        match mv.read(&loc, 7) {
            ReadOutcome::Versioned { writer, .. } => assert_eq!(writer, v(5, 0)),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn reader_sees_latest_write_below_its_index() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        mv.write(loc, v(1, 0), MemoryValue::Basic(info(1)));
        mv.write(loc, v(3, 0), MemoryValue::Basic(info(3)));
        mv.write(loc, v(5, 0), MemoryValue::Basic(info(5)));

        match mv.read(&loc, 4) {
            ReadOutcome::Versioned { writer, .. } => assert_eq!(writer, v(3, 0)),
            other => panic!("unexpected: {other:?}"),
        }
        match mv.read(&loc, 100) {
            ReadOutcome::Versioned { writer, .. } => assert_eq!(writer, v(5, 0)),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn re_incarnation_replaces_previous_write() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        mv.write(loc, v(2, 0), MemoryValue::Basic(info(100)));
        mv.write(loc, v(2, 1), MemoryValue::Basic(info(200))); // re-execution

        match mv.read(&loc, 3) {
            ReadOutcome::Versioned { writer, value } => {
                assert_eq!(writer, v(2, 1), "reader must see the newer incarnation");
                assert_matches::assert_matches!(
                    value,
                    MemoryValue::Basic(AccountInfo { balance, .. }) if balance == U256::from(200)
                );
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn estimate_blocks_later_readers() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        mv.write(loc, v(2, 0), MemoryValue::Basic(info(100)));
        assert!(mv.mark_estimate(loc, 2));

        assert_matches::assert_matches!(
            mv.read(&loc, 5),
            ReadOutcome::Blocked { blocking_tx_idx: 2 }
        );
    }

    #[test]
    fn estimate_on_missing_entry_returns_false() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        // Location has other entries but not for tx_idx 4.
        mv.write(loc, v(2, 0), MemoryValue::Basic(info(100)));
        assert!(!mv.mark_estimate(loc, 4));
    }

    #[test]
    fn reader_with_estimate_not_blocked_by_earlier_writes() {
        // Scenario: tx 1 wrote, tx 3 wrote, then tx 3's write gets marked
        // Estimate. A reader at tx 5 must see the Estimate at tx 3, NOT fall
        // back to tx 1's write. Block-STM correctness depends on this — if we
        // returned tx 1's write, the reader would miss that tx 3 is in the
        // middle of re-executing and might produce a different value.
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        mv.write(loc, v(1, 0), MemoryValue::Basic(info(10)));
        mv.write(loc, v(3, 0), MemoryValue::Basic(info(30)));
        assert!(mv.mark_estimate(loc, 3));

        assert_matches::assert_matches!(
            mv.read(&loc, 5),
            ReadOutcome::Blocked { blocking_tx_idx: 3 }
        );
    }

    #[test]
    fn remove_clears_entry() {
        let mv = MvMemory::new();
        let loc = MemoryLocation::Basic(address!("0x0000000000000000000000000000000000000001"));
        mv.write(loc, v(2, 0), MemoryValue::Basic(info(100)));
        assert!(mv.remove(loc, 2));
        // After removal the location's entry is gone; reader falls through.
        assert_matches::assert_matches!(mv.read(&loc, 5), ReadOutcome::NotFound);
        // Removing again is a no-op.
        assert!(!mv.remove(loc, 2));
    }

    #[test]
    fn distinct_locations_do_not_interfere() {
        let mv = MvMemory::new();
        let a = MemoryLocation::Basic(address!("0x000000000000000000000000000000000000000a"));
        let b = MemoryLocation::Basic(address!("0x000000000000000000000000000000000000000b"));
        mv.write(a, v(2, 0), MemoryValue::Basic(info(10)));
        mv.write(b, v(2, 0), MemoryValue::Basic(info(20)));

        assert_eq!(mv.locations_len(), 2);
        match mv.read(&a, 3) {
            ReadOutcome::Versioned { value: MemoryValue::Basic(info), .. } => {
                assert_eq!(info.balance, U256::from(10));
            }
            other => panic!("unexpected: {other:?}"),
        }
        match mv.read(&b, 3) {
            ReadOutcome::Versioned { value: MemoryValue::Basic(info), .. } => {
                assert_eq!(info.balance, U256::from(20));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn concurrent_writes_to_different_locations_do_not_deadlock() {
        // Simple smoke test: many threads writing disjoint locations shouldn't
        // deadlock and should all land. No assertion about ordering — just
        // presence.
        use std::sync::Arc;
        use std::thread;

        let mv = Arc::new(MvMemory::new());
        let handles: Vec<_> = (0..8)
            .map(|thread_id| {
                let mv = Arc::clone(&mv);
                thread::spawn(move || {
                    for i in 0..100 {
                        let addr_bytes = {
                            let mut b = [0u8; 20];
                            b[0] = thread_id as u8;
                            b[1] = (i >> 8) as u8;
                            b[2] = i as u8;
                            b
                        };
                        let loc = MemoryLocation::Basic(Address::from(addr_bytes));
                        mv.write(loc, v(i as TxIdx, 0), MemoryValue::Basic(info(i as u128)));
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(mv.locations_len(), 8 * 100);
    }
}
