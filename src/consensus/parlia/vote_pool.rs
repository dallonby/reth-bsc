use dashmap::{DashMap, DashSet};
use lru::LruCache;
use once_cell::sync::Lazy;
use parking_lot::Mutex;
use std::{
    cmp::Reverse,
    collections::BinaryHeap,
    num::NonZero,
    sync::RwLock,
};

use alloy_primitives::{BlockNumber, B256};

use super::vote::{VoteData, VoteEnvelope};
use crate::metrics::BscVoteMetrics;
use crate::shared;

const LOWER_LIMIT_OF_VOTE_BLOCK_NUMBER: u64 = 256;
/// Size of the LRU cache for tracking finality notifications (matches geth's finalizedNotified)
const FINALIZED_NOTIFIED_CACHE_SIZE: usize = 21;

/// Priority queue wrapper for vote data, ordered by target_number (ascending).
///
/// The prune path is strictly serial (run at most once per canonical-head
/// update), so we guard this behind its own `parking_lot::Mutex` rather than
/// widening the DashMap lock footprint. Insert touches the queue only when
/// a new block hash is seen, not on every vote — minimal contention.
#[derive(Default)]
struct VotesPriorityQueue {
    heap: BinaryHeap<Reverse<VoteData>>,
}

impl VotesPriorityQueue {
    fn new() -> Self {
        Self { heap: BinaryHeap::new() }
    }

    fn push(&mut self, vote_data: VoteData) {
        self.heap.push(Reverse(vote_data));
    }

    fn pop(&mut self) -> Option<VoteData> {
        self.heap.pop().map(|Reverse(data)| data)
    }

    fn peek(&self) -> Option<&VoteData> {
        self.heap.peek().map(|Reverse(data)| data)
    }
}

impl PartialOrd for VoteData {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for VoteData {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.target_number.cmp(&other.target_number)
    }
}

/// Global in-memory pool of incoming Parlia votes.
///
/// reth 2.0 port: the old pool was a single `RwLock<VotePool>` which
/// serialized *every* vote read and write on one lock and cloned the full
/// `Vec<VoteEnvelope>` on every `fetch_*` / `get_votes` call. Under heavy vote
/// broadcast (and finality-check fanout in `maybe_notify_finality`) this was
/// a major contention point.
///
/// New shape: `received_votes` is a sharded `DashSet`, `cur_votes` is a
/// sharded `DashMap<B256, Vec<VoteEnvelope>>`, and only the priority queue
/// sits behind a small `Mutex` (used by `insert` on first-seen block and by
/// `prune` once per head update). Reads are lock-free per-bucket; writes
/// only contend within a single target_hash bucket.
static RECEIVED_VOTES: Lazy<DashSet<B256>> = Lazy::new(DashSet::new);
static CUR_VOTES: Lazy<DashMap<B256, Vec<VoteEnvelope>>> = Lazy::new(DashMap::new);
static CUR_VOTES_PQ: Lazy<Mutex<VotesPriorityQueue>> =
    Lazy::new(|| Mutex::new(VotesPriorityQueue::new()));

/// Global metrics for vote operations.
static VOTE_METRICS: Lazy<BscVoteMetrics> = Lazy::new(BscVoteMetrics::default);

/// LRU cache to track which blocks have already been notified for finality.
/// This prevents repeated update_forkchoice calls for the same block (matches geth's finalizedNotified).
static FINALIZED_NOTIFIED: Lazy<RwLock<LruCache<B256, ()>>> =
    Lazy::new(|| RwLock::new(LruCache::new(NonZero::new(FINALIZED_NOTIFIED_CACHE_SIZE).unwrap())));

/// Update vote pool size metric.
fn update_vote_pool_size_metric(size: usize) {
    VOTE_METRICS.vote_pool_size.set(size as f64);
    VOTE_METRICS.current_votes_count.set(size as f64);
}

/// Total votes currently held across all target hashes.
fn current_len() -> usize {
    CUR_VOTES.iter().map(|entry| entry.value().len()).sum()
}

/// Insert a single vote into the pool (deduplicated by hash). Lock footprint:
///   * `RECEIVED_VOTES.insert` — single-bucket DashSet write.
///   * On first-seen target hash: PQ mutex + one DashMap slot write.
///   * On subsequent votes for same target: only the DashMap slot write.
pub fn put_vote(vote: VoteEnvelope) {
    let vote_hash = vote.hash();
    // Dedup.
    if !RECEIVED_VOTES.insert(vote_hash) {
        return;
    }

    // Bump the received counter only on *actual* inserts (old code bumped on
    // duplicates too because the check was inside the lock scope).
    VOTE_METRICS.received_votes_total.increment(1);

    let target_hash = vote.data.target_hash;
    let vote_data = vote.data;

    // Push into the target-hash bucket; track whether this was the first vote
    // for this hash so we know whether to push onto the PQ.
    let (new_bucket, votes_for_block) = match CUR_VOTES.entry(target_hash) {
        dashmap::mapref::entry::Entry::Occupied(mut e) => {
            let v = e.get_mut();
            v.push(vote);
            (false, v.len())
        }
        dashmap::mapref::entry::Entry::Vacant(e) => {
            let v = e.insert(vec![vote]);
            (true, v.len())
        }
    };

    if new_bucket {
        CUR_VOTES_PQ.lock().push(vote_data);
    }

    update_vote_pool_size_metric(current_len());
    maybe_notify_finality(target_hash, votes_for_block);
}

/// Drain all pending votes. Clears each shared structure in turn; readers
/// between the clears see a partial state (pre-existing behaviour — the old
/// single-lock version was atomic only in the sense that nothing else could
/// interleave, but all observers still saw the same before/after jump).
pub fn drain() -> Vec<VoteEnvelope> {
    RECEIVED_VOTES.clear();
    *CUR_VOTES_PQ.lock() = VotesPriorityQueue::new();
    let mut all_votes = Vec::new();
    // Drain without materializing all (key, value) pairs at once: iterate
    // keys first, then remove them one at a time.
    let keys: Vec<B256> = CUR_VOTES.iter().map(|e| *e.key()).collect();
    for key in keys {
        if let Some((_, votes)) = CUR_VOTES.remove(&key) {
            all_votes.extend(votes);
        }
    }
    update_vote_pool_size_metric(0);
    all_votes
}

/// Snapshot all pending votes without removing them. This is still O(votes)
/// by necessity — callers expect an owned `Vec` — but each bucket clone is a
/// per-shard read, so the work parallelises instead of serialising on one
/// lock. Kept here for API compatibility; prefer [`with_votes_by_hash`] when
/// you only need to read one bucket.
pub fn get_votes() -> Vec<VoteEnvelope> {
    let mut all_votes = Vec::new();
    for entry in CUR_VOTES.iter() {
        all_votes.extend(entry.value().iter().cloned());
    }
    all_votes
}

/// Current number of queued votes.
pub fn len() -> usize {
    current_len()
}

/// Check if the pool is empty.
pub fn is_empty() -> bool {
    CUR_VOTES.is_empty()
}

/// Fetch votes by block hash. Clones the bucket under a per-shard read lock;
/// other shards remain unblocked.
pub fn fetch_vote_by_block_hash(block_hash: B256) -> Vec<VoteEnvelope> {
    CUR_VOTES.get(&block_hash).map(|entry| entry.value().clone()).unwrap_or_default()
}

/// Fetch votes by block hash and source block number. Same per-shard locking
/// as `fetch_vote_by_block_hash` — filter runs under the read guard so we
/// don't clone votes we'll immediately discard.
pub fn fetch_vote_by_block_hash_and_source_number(
    block_hash: B256,
    source_number: BlockNumber,
) -> Vec<VoteEnvelope> {
    CUR_VOTES
        .get(&block_hash)
        .map(|entry| {
            entry
                .value()
                .iter()
                .filter(|vote| vote.data.source_number == source_number)
                .cloned()
                .collect()
        })
        .unwrap_or_default()
}

/// Prune old votes based on the latest block number.
/// Removes votes where targetNumber + LOWER_LIMIT_OF_VOTE_BLOCK_NUMBER - 1 < latestBlockNumber
pub fn prune(latest_block_number: BlockNumber) {
    let mut pq = CUR_VOTES_PQ.lock();
    while let Some(vote_data) = pq.peek() {
        if vote_data.target_number + LOWER_LIMIT_OF_VOTE_BLOCK_NUMBER - 1 < latest_block_number {
            let vote_data = pq.pop().expect("peeked some");
            let block_hash = vote_data.target_hash;
            if let Some((_, votes)) = CUR_VOTES.remove(&block_hash) {
                for vote in votes {
                    RECEIVED_VOTES.remove(&vote.hash());
                }
            }
        } else {
            break;
        }
    }
    drop(pq);
    update_vote_pool_size_metric(current_len());
}

fn maybe_notify_finality(target_hash: B256, votes_for_block: usize) {
    // Check if we've already notified for this block (de-duplication)
    {
        let cache = FINALIZED_NOTIFIED.read().expect("finalized notified cache poisoned");
        if cache.peek(&target_hash).is_some() {
            return;
        }
    }

    let head_number = match shared::get_best_canonical_block_number() {
        Some(number) => number,
        None => return,
    };
    let head = match shared::get_canonical_header_by_number(head_number) {
        Some(header) => header,
        None => return,
    };
    if head.hash_slow() != target_hash {
        return;
    }

    let sp = match shared::get_snapshot_provider() {
        Some(provider) => provider,
        None => return,
    };
    let snap = match sp.snapshot_by_hash(&target_hash) {
        Some(snap) => snap,
        None => return,
    };
    if snap.validators.is_empty() {
        return;
    }

    let current_justified_number = snap.vote_data.target_number;
    if head.number == 0 || head.number - 1 != current_justified_number {
        return;
    }

    let quorum = usize::div_ceil(snap.validators.len() * 2, 3);
    if votes_for_block < quorum {
        return;
    }

    let eligible_votes = fetch_vote_by_block_hash(target_hash)
        .into_iter()
        .filter(|vote| {
            vote.data.source_number == current_justified_number
                && vote.data.target_number == head.number
        })
        .count();

    if eligible_votes < quorum {
        return;
    }

    // Mark as notified before sending to avoid duplicate notifications
    {
        let mut cache = FINALIZED_NOTIFIED.write().expect("finalized notified cache poisoned");
        cache.put(target_hash, ());
    }

    if let Some(engine) = shared::get_fork_choice_engine() {
        tokio::spawn(async move {
            let _ = engine.update_forkchoice(&head).await;
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::consensus::parlia::vote::{VoteAddress, VoteData, VoteEnvelope, VoteSignature};
    use alloy_primitives::B256;

    fn vote_with_source(target_hash: B256, source_number: u64, unique: u8) -> VoteEnvelope {
        let mut address = VoteAddress::default();
        address[0] = unique;
        let mut signature = VoteSignature::default();
        signature[0] = unique;
        VoteEnvelope {
            vote_address: address,
            signature,
            data: VoteData {
                source_number,
                source_hash: B256::from([source_number as u8; 32]),
                target_number: 100,
                target_hash,
            },
        }
    }

    #[test]
    fn fetch_votes_filters_by_source_number() {
        // Ensure global pool has a clean state across tests.
        let _ = drain();

        let target_hash = B256::from([0x11; 32]);
        let other_target_hash = B256::from([0x22; 32]);

        put_vote(vote_with_source(target_hash, 10, 1));
        put_vote(vote_with_source(target_hash, 11, 2));
        put_vote(vote_with_source(other_target_hash, 10, 3));

        let all_for_target = fetch_vote_by_block_hash(target_hash);
        assert_eq!(all_for_target.len(), 2);

        let source_10 = fetch_vote_by_block_hash_and_source_number(target_hash, 10);
        assert_eq!(source_10.len(), 1);
        assert_eq!(source_10[0].data.source_number, 10);

        let source_11 = fetch_vote_by_block_hash_and_source_number(target_hash, 11);
        assert_eq!(source_11.len(), 1);
        assert_eq!(source_11[0].data.source_number, 11);

        let source_12 = fetch_vote_by_block_hash_and_source_number(target_hash, 12);
        assert!(source_12.is_empty());

        let _ = drain();
    }
}
