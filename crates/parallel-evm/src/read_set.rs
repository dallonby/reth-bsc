//! Per-transaction read-set recording, used for validation.
//!
//! During execution, every read performed by the EVM database wrapper is
//! logged here with the version it saw. During validation, we replay the
//! log and check every recorded read would still return the same version
//! today — if not, the tx is stale and must re-execute.
//!
//! # Sets, not maps
//!
//! A read set is an ordered `Vec<(Location, Origin)>`, not a `HashMap`:
//! an EVM execution can read the same location multiple times within one
//! tx (e.g. repeated SLOADs), and each read must see a consistent value
//! within that incarnation. The first read's origin is what matters for
//! validation — subsequent reads return the same observed version. In
//! practice, the DbWrapper caches within a single execution, so the read
//! set only captures the *first* read per location per incarnation anyway.
//!
//! # Re-incarnation discards the old read set
//!
//! Block-STM requires that a new incarnation's read set REPLACE the old
//! one — validation must reflect what this incarnation saw, not a previous
//! attempt's ghosts. Callers do this by calling [`ReadSetStore::replace`]
//! at the end of execution, which atomically swaps the stored vec.

use crate::mv_memory::{MemoryLocation, TxIdx, Version};
use parking_lot::Mutex;

/// Origin of a read, from the reader's point of view.
///
/// Validation compares a recorded [`ReadOrigin`] against the current state of
/// the multi-version memory. If they disagree, the read is stale and the tx's
/// incarnation must abort.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadOrigin {
    /// The read was served by MvMemory — some earlier tx wrote this location
    /// and we saw that write. Carries the version so validation can detect
    /// "a different tx now holds this slot" or "same tx but a newer
    /// incarnation wrote a different value".
    Versioned(Version),
    /// MvMemory had no entry for this location visible to the reader, so the
    /// read fell through to the caller's `Storage`. Validation checks that
    /// no earlier-tx writer has landed in the meantime — if one has, this
    /// origin is stale.
    Storage,
}

/// One entry in a tx's read set.
#[derive(Debug, Clone)]
pub struct ReadLog {
    pub location: MemoryLocation,
    pub origin: ReadOrigin,
}

/// Per-tx read log. Populated during execution; consumed during validation.
///
/// Indexed by tx_idx; slot-local `Mutex`es so concurrent txs do not contend.
/// Same tx's execution vs. validation DO contend, which is exactly what we
/// want: validation must not race a re-execution of the same tx.
#[derive(Debug)]
pub struct ReadSetStore {
    logs: Vec<Mutex<Vec<ReadLog>>>,
}

impl ReadSetStore {
    /// Allocate empty logs for a block of `len` transactions.
    pub fn new(len: usize) -> Self {
        let mut logs = Vec::with_capacity(len);
        for _ in 0..len {
            logs.push(Mutex::new(Vec::new()));
        }
        Self { logs }
    }

    pub fn len(&self) -> usize {
        self.logs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.logs.is_empty()
    }

    /// Replace tx `tx_idx`'s read log with `new_logs`, discarding whatever
    /// the previous incarnation recorded.
    pub fn replace(&self, tx_idx: TxIdx, new_logs: Vec<ReadLog>) {
        let mut slot = self.logs[tx_idx].lock();
        *slot = new_logs;
    }

    /// Snapshot tx `tx_idx`'s read log for validation. Returns a clone so the
    /// caller can iterate without holding the slot lock while they consult
    /// MvMemory — validation is already read-only on the read set.
    pub fn snapshot(&self, tx_idx: TxIdx) -> Vec<ReadLog> {
        self.logs[tx_idx].lock().clone()
    }
}

/// Build a read log incrementally on the execution path.
///
/// One of these per worker per tx. The worker records every read as it
/// happens, then hands the finished log to
/// [`ReadSetStore::replace`] on `finish_execution`.
///
/// Deliberately local state, not shared — contention-free.
#[derive(Debug, Default)]
pub struct ReadLogBuilder {
    entries: Vec<ReadLog>,
}

impl ReadLogBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a read.
    pub fn push(&mut self, location: MemoryLocation, origin: ReadOrigin) {
        self.entries.push(ReadLog { location, origin });
    }

    /// Number of reads recorded so far.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Consume the builder, returning the accumulated log.
    pub fn finish(self) -> Vec<ReadLog> {
        self.entries
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_primitives::address;

    fn loc(last_byte: u8) -> MemoryLocation {
        let mut b = [0u8; 20];
        b[19] = last_byte;
        MemoryLocation::Basic(alloy_primitives::Address::from(b))
    }

    #[test]
    fn builder_accumulates_in_order() {
        let mut b = ReadLogBuilder::new();
        b.push(loc(1), ReadOrigin::Storage);
        b.push(
            loc(2),
            ReadOrigin::Versioned(Version { tx_idx: 3, incarnation: 0 }),
        );
        let log = b.finish();
        assert_eq!(log.len(), 2);
        assert_eq!(log[0].location, loc(1));
        assert!(matches!(log[0].origin, ReadOrigin::Storage));
        assert!(matches!(
            log[1].origin,
            ReadOrigin::Versioned(Version { tx_idx: 3, incarnation: 0 })
        ));
    }

    #[test]
    fn store_replace_discards_previous() {
        let store = ReadSetStore::new(1);
        store.replace(
            0,
            vec![ReadLog {
                location: loc(1),
                origin: ReadOrigin::Storage,
            }],
        );
        assert_eq!(store.snapshot(0).len(), 1);
        // New incarnation — old log must be gone.
        store.replace(
            0,
            vec![ReadLog {
                location: loc(2),
                origin: ReadOrigin::Storage,
            }],
        );
        let snap = store.snapshot(0);
        assert_eq!(snap.len(), 1);
        assert_eq!(snap[0].location, loc(2));
    }

    #[test]
    fn store_per_tx_isolated() {
        use std::sync::Arc;
        use std::thread;

        let store = Arc::new(ReadSetStore::new(32));
        let handles: Vec<_> = (0..8)
            .map(|t| {
                let store = Arc::clone(&store);
                thread::spawn(move || {
                    for i in 0..4 {
                        let idx = t * 4 + i;
                        store.replace(
                            idx,
                            vec![ReadLog {
                                location: loc(idx as u8),
                                origin: ReadOrigin::Storage,
                            }],
                        );
                    }
                })
            })
            .collect();
        for h in handles {
            h.join().unwrap();
        }
        for idx in 0..32 {
            let snap = store.snapshot(idx);
            assert_eq!(snap.len(), 1);
            assert_eq!(snap[0].location, loc(idx as u8));
        }
    }

    // We piggyback one more test on Address construction so that the
    // `address!` macro import isn't leaked out of the module.
    #[test]
    fn address_macro_works() {
        let _ = address!("0x0000000000000000000000000000000000000001");
    }
}
