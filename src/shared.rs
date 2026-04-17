use crate::consensus::parlia::SnapshotProvider;
use crate::node::engine_api::payload::BscPayloadTypes;
use crate::node::network::block_import::service::{IncomingBlock, IncomingMinedBlock};
use crate::node::network::BscNetworkPrimitives;
use crate::node::primitives::BscBlock;
use alloy_consensus::{BlockHeader, Header};
use alloy_rlp::Encodable;
use alloy_eips::BlockId;
use alloy_primitives::{B256, Bytes, U256};
use reth_ethereum_primitives::TransactionSigned;
use parking_lot::Mutex;
use reth_network::NetworkHandle;
use reth_network_api::PeerId;
use reth_payload_builder_primitives::Events;
use reth_provider::{BlockNumReader, HeaderProvider};
use schnellru::{ByLength, LruMap};
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::RwLock;
use std::sync::{Arc, OnceLock};
use tokio::sync::broadcast;
use tokio::sync::mpsc::UnboundedSender;
use alloy_rpc_types::{
    Block as RpcBlock, BlockOverrides, Header as RpcHeader, Receipt as RpcReceipt, Transaction as RpcTransaction, TransactionRequest as RpcTransactionRequest, state::StateOverride
};

/// Function type for HeaderProvider::header() access (by hash)
type HeaderByHashFn = Arc<dyn Fn(&B256) -> Option<Header> + Send + Sync>;

/// Function type for HeaderProvider::header_by_number() access (by number)  
type HeaderByNumberFn = Arc<dyn Fn(u64) -> Option<Header> + Send + Sync>;

/// Global shared access to the snapshot provider for RPC
static SNAPSHOT_PROVIDER: OnceLock<Arc<dyn SnapshotProvider + Send + Sync>> = OnceLock::new();

/// Global header provider function - HeaderProvider::header() by hash
static HEADER_BY_HASH_PROVIDER: OnceLock<HeaderByHashFn> = OnceLock::new();

/// Global header provider function - HeaderProvider::header_by_number() by number  
static HEADER_BY_NUMBER_PROVIDER: OnceLock<HeaderByNumberFn> = OnceLock::new();

/// Function type for BlockNumReader::best_block_number()
type BestBlockNumberFn = Arc<dyn Fn() -> Option<u64> + Send + Sync>;

/// Global best block number function
static BEST_BLOCK_NUMBER_PROVIDER: OnceLock<BestBlockNumberFn> = OnceLock::new();

/// Function type for best total difficulty (u128 approximation)
type BestTdFn = Arc<dyn Fn() -> Option<u128> + Send + Sync>;

/// Global best total difficulty provider
static BEST_TD_PROVIDER: OnceLock<BestTdFn> = OnceLock::new();

/// Global sender for submitting mined blocks to the import service
static BLOCK_IMPORT_MINED_SENDER: OnceLock<UnboundedSender<IncomingMinedBlock>> = OnceLock::new();

/// Global sender for submitting built payload to the import service
static BLOCK_IMPORT_SENDER: OnceLock<UnboundedSender<IncomingBlock>> = OnceLock::new();

/// Global local peer ID for network identification
static LOCAL_PEER_ID: OnceLock<PeerId> = OnceLock::new();

/// Global queue for bid packages (thread-safe)
static BID_PACKAGE_QUEUE: OnceLock<Arc<Mutex<VecDeque<crate::node::miner::bid_simulator::Bid>>>> =
    OnceLock::new();

/// Global network handle to interact with P2P (reth).
static NETWORK_HANDLE: OnceLock<NetworkHandle<BscNetworkPrimitives>> = OnceLock::new();

/// Global flag: run the user-tx phase of each block through the
/// parallel-evm Block-STM executor. Set once at node startup from the
/// `--bsc.parallel-execute` CLI flag; read by
/// `BscEvmConfig::context_for_block` on every block.
///
/// Uses an atomic bool so the read side is free; write side is one-shot
/// at startup.
static PARALLEL_EXECUTE_ENABLED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

/// Set the `--bsc.parallel-execute` flag at node startup. Idempotent.
pub fn set_parallel_execute_enabled(enabled: bool) {
    PARALLEL_EXECUTE_ENABLED.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

/// Read the parallel-execute flag. Called once per block from
/// `context_for_block`; cheap (relaxed atomic load).
#[inline]
pub fn is_parallel_execute_enabled() -> bool {
    PARALLEL_EXECUTE_ENABLED.load(std::sync::atomic::Ordering::Relaxed)
}

/// Diagnostic: force `parallel-evm`'s sequential path by passing
/// `min_txs_for_parallel = usize::MAX` to `ParallelConfig`. See the
/// flag doc on `BscCliArgs::parallel_force_sequential`.
static PARALLEL_FORCE_SEQUENTIAL: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_parallel_force_sequential(enabled: bool) {
    PARALLEL_FORCE_SEQUENTIAL.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn is_parallel_force_sequential() -> bool {
    PARALLEL_FORCE_SEQUENTIAL.load(std::sync::atomic::Ordering::Relaxed)
}

/// Diagnostic: log SYSTEM_ADDRESS balance after every commit_transaction
/// and at distribute_incoming. See `BscCliArgs::parallel_diagnostic_fee_trace`.
static PARALLEL_DIAGNOSTIC_FEE_TRACE: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

pub fn set_parallel_diagnostic_fee_trace(enabled: bool) {
    PARALLEL_DIAGNOSTIC_FEE_TRACE.store(enabled, std::sync::atomic::Ordering::Relaxed);
}

#[inline]
pub fn is_parallel_diagnostic_fee_trace() -> bool {
    PARALLEL_DIAGNOSTIC_FEE_TRACE.load(std::sync::atomic::Ordering::Relaxed)
}

/// Global state-provider spawner used by the parallel execution path.
///
/// Stashed here (not on `BscEvmConfig`) because reth's `ExecutionStage`
/// constructs executors via `ConfigureEvm::batch_executor(db)` which
/// only hands us `DB: Database` — there's no way to thread a
/// `ProviderFactory` through that trait-bounded surface. The node sets
/// this from `ctx.provider().clone()` in its `on_component_initialized`
/// hook, before the pipeline ever starts.
///
/// Using a global keeps the `batch_executor` return type unchanged and
/// keeps BscEvmConfig free of a life-cycle-coupled factory field. One
/// provider-per-node (which reth already enforces architecturally) is
/// fine for this use.
static PARALLEL_STATE_SPAWNER: OnceLock<
    Arc<dyn crate::node::evm::parallel::ParallelStateSpawner + Send + Sync + 'static>,
> = OnceLock::new();

/// Register a `ParallelStateSpawner` for the parallel execution path to
/// spawn per-worker `StateProviderBox` instances. Idempotent: silently
/// ignores re-registration attempts (same pattern as the other `OnceLock`
/// providers above).
pub fn set_parallel_state_spawner<S>(spawner: Arc<S>)
where
    S: crate::node::evm::parallel::ParallelStateSpawner + Send + Sync + 'static,
{
    // `Arc<S>` coerces to `Arc<dyn ParallelStateSpawner + Send + Sync>`
    // once we spell the target explicitly.
    let erased: Arc<
        dyn crate::node::evm::parallel::ParallelStateSpawner + Send + Sync + 'static,
    > = spawner;
    let _ = PARALLEL_STATE_SPAWNER.set(erased);
}

/// Read the globally-registered parallel state spawner. Returns
/// `Some` once the node has initialized components; `None` before.
pub fn get_parallel_state_spawner(
) -> Option<&'static Arc<dyn crate::node::evm::parallel::ParallelStateSpawner + Send + Sync + 'static>>
{
    PARALLEL_STATE_SPAWNER.get()
}

/// Global payload events broadcast sender
static PAYLOAD_EVENTS_TX: OnceLock<broadcast::Sender<Events<BscPayloadTypes>>> = OnceLock::new();
/// Broadcast channel for notifying about successfully imported block hashes
static IMPORTED_BLOCKS_TX: OnceLock<broadcast::Sender<B256>> = OnceLock::new();

/// Global MEV running status
static MEV_RUNNING: OnceLock<Arc<AtomicBool>> = OnceLock::new();
/// Global proxyed peer IDs list
static PROXYED_PEER_IDS: OnceLock<Vec<PeerId>> = OnceLock::new();

// Blob store handle from the transaction pool, stashed here so the MEV bid
// simulator can insert blob sidecars from replayed bids. reth 2.0 made
// `Pool::insert_blob` a private method on the concrete `Pool<V,T,S>` struct
// (not on the `TransactionPool` trait), so downstream code that only sees the
// trait can no longer reach it. Stashing the concrete blob store here is the
// minimum-invasive fix.
static BLOB_STORE: OnceLock<reth_transaction_pool::blobstore::DiskFileBlobStore> =
    OnceLock::new();

/// Register the pool's blob store. Called once from `BscPoolBuilder::build_pool`.
pub fn set_blob_store(
    store: reth_transaction_pool::blobstore::DiskFileBlobStore,
) -> Result<(), reth_transaction_pool::blobstore::DiskFileBlobStore> {
    BLOB_STORE.set(store)
}

/// Access the pool's blob store if initialized.
pub fn get_blob_store() -> Option<&'static reth_transaction_pool::blobstore::DiskFileBlobStore> {
    BLOB_STORE.get()
}

/// Current Parlia validator-set size, computed from the latest snapshot.
///
/// The bnb-chain fork piped this into reth's `EthApiBuilder` via a
/// `with_current_validators_len` hook for use inside the EthApi. reth 2.0
/// dropped that hook; we expose the same lookup here so BSC-specific RPCs
/// and internal consumers that need the value can call it directly. Returns
/// `None` until the snapshot provider and canonical-header provider are both
/// initialized, mirroring the bnb-chain behavior when a consumer queried
/// before the first sealed block.
pub fn current_validators_len() -> Option<usize> {
    let best_block = get_best_canonical_block_number()?;
    let header = get_canonical_header_by_number(best_block)?;
    let snapshot_provider = get_snapshot_provider()?;
    Some(snapshot_provider.snapshot_by_hash(&header.hash_slow())?.validators.len())
}

/// Set global imported blocks broadcast sender.
pub fn set_imported_blocks_tx(tx: broadcast::Sender<B256>) -> Result<(), broadcast::Sender<B256>> {
    IMPORTED_BLOCKS_TX.set(tx)
}

/// Get global imported blocks broadcast sender if initialized.
pub fn get_imported_blocks_tx() -> Option<&'static broadcast::Sender<B256>> {
    IMPORTED_BLOCKS_TX.get()
}

/// Set global proxyed peer IDs.
pub fn set_proxyed_peer_ids(peer_ids: Vec<PeerId>) -> Result<(), Vec<PeerId>> {
    PROXYED_PEER_IDS.set(peer_ids)
}

/// Get global proxyed peer IDs if initialized.
pub fn get_proxyed_peer_ids() -> Option<&'static Vec<PeerId>> {
    PROXYED_PEER_IDS.get()
}

/// Trait for fork choice engine operations that can be stored globally
pub trait ForkChoiceEngineTrait: Send + Sync {
    fn update_forkchoice<'a>(
        &'a self,
        header: &'a Header,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), crate::consensus::ParliaConsensusErr>>
                + Send
                + 'a,
        >,
    >;
    fn is_need_reorg<'a>(
        &'a self,
        incoming_header: &'a Header,
        current_header: &'a Header,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<bool, crate::consensus::ParliaConsensusErr>>
                + Send
                + 'a,
        >,
    >;
}

impl<P> ForkChoiceEngineTrait for crate::node::consensus::BscForkChoiceEngine<P>
where
    P: HeaderProvider<Header = Header> + BlockNumReader + Clone + Send + Sync,
{
    fn update_forkchoice<'a>(
        &'a self,
        header: &'a Header,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<(), crate::consensus::ParliaConsensusErr>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(self.update_forkchoice(header))
    }

    fn is_need_reorg<'a>(
        &'a self,
        incoming_header: &'a Header,
        current_header: &'a Header,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<bool, crate::consensus::ParliaConsensusErr>>
                + Send
                + 'a,
        >,
    > {
        Box::pin(self.is_need_reorg(incoming_header, current_header))
    }
}

/// Global fork choice engine instance
static FORK_CHOICE_ENGINE: OnceLock<Box<dyn ForkChoiceEngineTrait>> = OnceLock::new();

/// Trait for full block access (header + body + sidecars)
pub trait FullBlockProvider: Send + Sync {
    fn block_by_hash(&self, hash: &B256) -> Option<BscBlock>;
    fn block_by_number(&self, number: u64) -> Option<BscBlock>;
}

/// Global full block provider instance
static FULL_BLOCK_PROVIDER: OnceLock<Arc<dyn FullBlockProvider + Send + Sync>> = OnceLock::new();

/// In-memory cache for recently seen full blocks (hash -> block), and number -> hash mapping.
/// This allows answering range requests with full bodies if they were recently imported.
static BODY_CACHE: OnceLock<RwLock<BodyCache>> = OnceLock::new();

/// Max number of full blocks to store in the in-memory body cache
const BODY_CACHE_CAPACITY: usize = 512;

struct BodyCache {
    by_hash: LruMap<B256, BscBlock, ByLength>,
    by_number: LruMap<u64, B256, ByLength>,
}

impl Default for BodyCache {
    fn default() -> Self {
        Self {
            by_hash: LruMap::new(ByLength::new(BODY_CACHE_CAPACITY.try_into().unwrap())),
            by_number: LruMap::new(ByLength::new(BODY_CACHE_CAPACITY.try_into().unwrap())),
        }
    }
}

/// Store the snapshot provider globally
pub fn set_snapshot_provider(
    provider: Arc<dyn SnapshotProvider + Send + Sync>,
) -> Result<(), Arc<dyn SnapshotProvider + Send + Sync>> {
    SNAPSHOT_PROVIDER.set(provider)
}

/// Get the global snapshot provider
pub fn get_snapshot_provider() -> Option<&'static Arc<dyn SnapshotProvider + Send + Sync>> {
    SNAPSHOT_PROVIDER.get()
}

/// Store the header provider globally
/// Creates functions that directly call HeaderProvider::header() and HeaderProvider::header_by_number()
pub fn set_header_provider<T>(
    provider: Arc<T>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
where
    T: HeaderProvider<Header = Header> + BlockNumReader + Send + Sync + 'static,
{
    // Create function for header by hash
    let provider_clone = provider.clone();
    let header_by_hash_fn = Arc::new(move |block_hash: &B256| -> Option<Header> {
        match provider_clone.header(*block_hash) {
            Ok(Some(header)) => Some(header),
            _ => None,
        }
    });

    // Create function for header by number
    let provider_clone2 = provider.clone();
    let header_by_number_fn = Arc::new(move |block_number: u64| -> Option<Header> {
        match provider_clone2.header_by_number(block_number) {
            Ok(Some(header)) => Some(header),
            _ => None,
        }
    });

    // Set both functions
    HEADER_BY_HASH_PROVIDER.set(header_by_hash_fn).map_err(|_| "Failed to set hash provider")?;
    HEADER_BY_NUMBER_PROVIDER
        .set(header_by_number_fn)
        .map_err(|_| "Failed to set number provider")?;

    // Create function for best block number
    let provider_clone3 = provider.clone();
    let best_block_number_fn =
        Arc::new(move || -> Option<u64> { provider_clone3.best_block_number().ok() });
    BEST_BLOCK_NUMBER_PROVIDER
        .set(best_block_number_fn)
        .map_err(|_| "Failed to set best block number provider")?;

    // Create function for best total difficulty (u128 approximation).
    // reth 2.0 dropped HeaderProvider::header_td_by_number; we source TD from
    // the local accumulator maintained by consensus validation (see
    // crate::consensus::parlia::td_store). TD on BSC grows by ~1-2 per block,
    // so u128 is plenty (max u128 = 3.4e38, BSC TD at block 100M ≈ 2e8).
    let provider_clone4 = provider.clone();
    let best_td_fn = Arc::new(move || -> Option<u128> {
        let n = provider_clone4.best_block_number().ok()?;
        crate::consensus::parlia::td_store::TdStore::best_by_number(n).map(|td| td.to::<u128>())
    });
    BEST_TD_PROVIDER.set(best_td_fn).map_err(|_| "Failed to set best td provider")?;

    Ok(())
}

/// Get header by hash from the global header provider
/// Directly calls the stored HeaderProvider::header() function
pub fn get_canonical_header_by_hash_from_provider(block_hash: &B256) -> Option<Header> {
    let provider_fn = HEADER_BY_HASH_PROVIDER.get()?;
    provider_fn(block_hash)
}

/// Get header by number from the global header provider
/// Directly calls the stored HeaderProvider::header_by_number() function
pub fn get_canonical_header_by_number_from_provider(block_number: u64) -> Option<Header> {
    let provider_fn = HEADER_BY_NUMBER_PROVIDER.get()?;
    provider_fn(block_number)
}

/// Get header by hash - simplified interface
pub fn get_canonical_header_by_hash(block_hash: &B256) -> Option<Header> {
    get_canonical_header_by_hash_from_provider(block_hash)
}

/// Get header by number - simplified interface
pub fn get_canonical_header_by_number(block_number: u64) -> Option<Header> {
    get_canonical_header_by_number_from_provider(block_number)
}

/// Get the best block number from the global provider if initialized
pub fn get_best_canonical_block_number() -> Option<u64> {
    BEST_BLOCK_NUMBER_PROVIDER.get().and_then(|f| f())
}

/// Get the best total difficulty (u128 approximation) if available
pub fn get_best_canonical_td() -> Option<u128> {
    BEST_TD_PROVIDER.get().and_then(|f| f())
}

/// Store the block import sender globally. Returns an error if it was set before.
pub fn set_block_import_mined_sender(
    sender: UnboundedSender<IncomingMinedBlock>,
) -> Result<(), UnboundedSender<IncomingMinedBlock>> {
    BLOCK_IMPORT_MINED_SENDER.set(sender)
}

/// Get a reference to the global block import sender, if initialized.
pub fn get_block_import_mined_sender() -> Option<&'static UnboundedSender<IncomingMinedBlock>> {
    BLOCK_IMPORT_MINED_SENDER.get()
}

/// Store the block import sender globally. Returns an error if it was set before.
pub fn set_block_import_sender(
    sender: UnboundedSender<IncomingBlock>,
) -> Result<(), UnboundedSender<IncomingBlock>> {
    BLOCK_IMPORT_SENDER.set(sender)
}

/// Get a reference to the global block import sender, if initialized.
pub fn get_block_import_sender() -> Option<&'static UnboundedSender<IncomingBlock>> {
    BLOCK_IMPORT_SENDER.get()
}

/// Store the local peer ID globally. Returns an error if it was set before.
pub fn set_local_peer_id(peer_id: PeerId) -> Result<(), PeerId> {
    LOCAL_PEER_ID.set(peer_id)
}

/// Get the global local peer ID, or return a default PeerId if not set.
pub fn get_local_peer_id_or_default() -> PeerId {
    LOCAL_PEER_ID.get().cloned().unwrap_or_default()
}

/// Initialize the bid package queue (should be called once at startup)
pub fn init_bid_package_queue() {
    let _ = BID_PACKAGE_QUEUE.set(Arc::new(Mutex::new(VecDeque::new())));
}

/// Push a bid package to the global queue
pub fn push_bid_package(
    package: crate::node::miner::bid_simulator::Bid,
) -> Result<(), &'static str> {
    if let Some(queue) = BID_PACKAGE_QUEUE.get() {
        queue.lock().push_back(package);
        Ok(())
    } else {
        Err("Bid package queue not initialized")
    }
}

/// Pop a bid package from the global queueBid
pub fn pop_bid_package() -> Option<crate::node::miner::bid_simulator::Bid> {
    BID_PACKAGE_QUEUE.get().and_then(|queue| queue.lock().pop_front())
}

/// Get the count of pending bid packages in the queue
pub fn bid_package_queue_len() -> usize {
    BID_PACKAGE_QUEUE.get().map(|queue| queue.lock().len()).unwrap_or(0)
}

/// Store the reth `NetworkHandle` globally for dynamic peer actions.
pub fn set_network_handle(
    handle: NetworkHandle<BscNetworkPrimitives>,
) -> Result<(), NetworkHandle<BscNetworkPrimitives>> {
    NETWORK_HANDLE.set(handle)
}

/// Get a clone of the global network handle if available.
pub fn get_network_handle() -> Option<NetworkHandle<BscNetworkPrimitives>> {
    NETWORK_HANDLE.get().cloned()
}

/// Set global payload events broadcast sender.
pub fn set_payload_events_tx(
    tx: broadcast::Sender<Events<BscPayloadTypes>>,
) -> Result<(), broadcast::Sender<Events<BscPayloadTypes>>> {
    PAYLOAD_EVENTS_TX.set(tx)
}

/// Get global payload events broadcast sender if initialized.
pub fn get_payload_events_tx() -> Option<&'static broadcast::Sender<Events<BscPayloadTypes>>> {
    PAYLOAD_EVENTS_TX.get()
}

/// Store the fork choice engine globally.
///
/// This stores a `BscForkChoiceEngine` instance to provide global access for fork choice operations.
pub fn set_fork_choice_engine<P>(
    engine: crate::node::consensus::BscForkChoiceEngine<P>,
) -> Result<(), Box<dyn std::error::Error>>
where
    P: HeaderProvider<Header = Header> + BlockNumReader + Clone + Send + Sync + 'static,
{
    let boxed: Box<dyn ForkChoiceEngineTrait> = Box::new(engine);
    FORK_CHOICE_ENGINE.set(boxed).map_err(|_| "Failed to set fork choice engine")?;
    Ok(())
}

/// Get a reference to the global fork choice engine.
pub fn get_fork_choice_engine() -> Option<&'static dyn ForkChoiceEngineTrait> {
    FORK_CHOICE_ENGINE.get().map(|b| &**b)
}

/// Set the global full block provider
pub fn set_full_block_provider(
    provider: Arc<dyn FullBlockProvider + Send + Sync>,
) -> Result<(), Arc<dyn FullBlockProvider + Send + Sync>> {
    FULL_BLOCK_PROVIDER.set(provider)
}

/// Try to get a full block by hash from the global provider
pub fn get_full_block_by_hash(hash: &B256) -> Option<BscBlock> {
    FULL_BLOCK_PROVIDER.get().and_then(|p| p.block_by_hash(hash))
}

/// Try to get a full block by number from the global provider
pub fn get_full_block_by_number(number: u64) -> Option<BscBlock> {
    FULL_BLOCK_PROVIDER.get().and_then(|p| p.block_by_number(number))
}

/// A closure-based full block provider for easy integration.
pub struct ClosureFullBlockProvider<ByHash, ByNumber>
where
    ByHash: Fn(&B256) -> Option<BscBlock> + Send + Sync + 'static,
    ByNumber: Fn(u64) -> Option<BscBlock> + Send + Sync + 'static,
{
    by_hash: ByHash,
    by_number: ByNumber,
}

impl<ByHash, ByNumber> ClosureFullBlockProvider<ByHash, ByNumber>
where
    ByHash: Fn(&B256) -> Option<BscBlock> + Send + Sync + 'static,
    ByNumber: Fn(u64) -> Option<BscBlock> + Send + Sync + 'static,
{
    pub fn new(by_hash: ByHash, by_number: ByNumber) -> Self {
        Self { by_hash, by_number }
    }
}

impl<ByHash, ByNumber> FullBlockProvider for ClosureFullBlockProvider<ByHash, ByNumber>
where
    ByHash: Fn(&B256) -> Option<BscBlock> + Send + Sync + 'static,
    ByNumber: Fn(u64) -> Option<BscBlock> + Send + Sync + 'static,
{
    fn block_by_hash(&self, hash: &B256) -> Option<BscBlock> {
        (self.by_hash)(hash)
    }
    fn block_by_number(&self, number: u64) -> Option<BscBlock> {
        (self.by_number)(number)
    }
}

/// Helper to install a closure-based full block provider.
pub fn set_full_block_provider_from_closures<ByHash, ByNumber>(
    by_hash: ByHash,
    by_number: ByNumber,
) -> Result<(), Arc<dyn FullBlockProvider + Send + Sync>>
where
    ByHash: Fn(&B256) -> Option<BscBlock> + Send + Sync + 'static,
    ByNumber: Fn(u64) -> Option<BscBlock> + Send + Sync + 'static,
{
    set_full_block_provider(Arc::new(ClosureFullBlockProvider::new(by_hash, by_number)))
}

/// Inserts a full block into the in-memory body cache.
pub fn cache_full_block(block: BscBlock) {
    let cache = BODY_CACHE.get_or_init(|| RwLock::new(BodyCache::default()));
    if let Ok(mut guard) = cache.write() {
        let hash = block.header.hash_slow();
        let number = block.header.number();
        guard.by_number.insert(number, hash);
        guard.by_hash.insert(hash, block);
    }
}

/// Fetch a full block from the in-memory body cache by hash.
pub fn get_cached_block_by_hash(hash: &B256) -> Option<BscBlock> {
    let cache = BODY_CACHE.get_or_init(|| RwLock::new(BodyCache::default()));
    if let Ok(mut guard) = cache.write() {
        if let Some(block) = guard.by_hash.get(hash) {
            return Some(block.clone());
        }
    }
    None
}

/// Fetch a full block from the in-memory body cache by number.
pub fn get_cached_block_by_number(number: u64) -> Option<BscBlock> {
    let cache = BODY_CACHE.get_or_init(|| RwLock::new(BodyCache::default()));
    if let Ok(mut guard) = cache.write() {
        if let Some(h_ref) = guard.by_number.get(&number) {
            let h = *h_ref;
            if let Some(block) = guard.by_hash.get(&h) {
                return Some(block.clone());
            }
        }
    }
    None
}

/// Clear the body cache (primarily for testing)
#[cfg(test)]
pub fn clear_body_cache() {
    let cache = BODY_CACHE.get_or_init(|| RwLock::new(BodyCache::default()));
    if let Ok(mut guard) = cache.write() {
        *guard = BodyCache::default();
    }
}

// ============ MEV Running Status ============

/// Set global MEV running status (called by MevWorkWorker on startup)
pub fn set_mev_running(running: Arc<AtomicBool>) -> Result<(), Arc<AtomicBool>> {
    MEV_RUNNING.set(running)
}

/// Get global MEV running status
pub fn is_mev_running() -> bool {
    MEV_RUNNING.get().map(|status| status.load(Ordering::Relaxed)).unwrap_or(false)
}

// ============= IPC client ===============
pub static IPC_CLIENT: OnceLock<Arc<jsonrpsee::async_client::Client>> = OnceLock::new();

/// Set the IPC client
pub async fn set_ipc_client(path: String) -> Result<(), eyre::Error> {
    let client = reth_ipc::client::IpcClientBuilder::default()
        .build(&path)
        .await
        .map_err(|e| eyre::eyre!("Failed to build RPC client: {:?}", e))?;
    IPC_CLIENT.set(Arc::new(client)).map_err(|e| eyre::eyre!("Failed to set RPC client: {:?}", e))?;
    Ok(())
}

/// Get the IPC client
pub fn get_ipc_client() -> Option<Arc<jsonrpsee::async_client::Client>> {
    IPC_CLIENT.get().cloned()
}

/// Call the IPC client to get the result of an Ethereum call
/// This is a wrapper around the reth_rpc_eth_api::EthApiClient::call function
/// It takes a transaction request, a block ID, a state overrides, and a block overrides
/// It returns the result of the call as a Bytes object
pub async fn ipc_eth_call(
    req: RpcTransactionRequest,
    block_id: Option<BlockId>,
    state_overrides: Option<StateOverride>,
    block_overrides: Option<Box<BlockOverrides>>,
) -> Result<Bytes, eyre::Error> {
    let client = get_ipc_client().ok_or(eyre::eyre!("Failed to get RPC client"))?;
    reth_rpc_eth_api::EthApiClient::<
        RpcTransactionRequest,
        RpcTransaction,
        RpcBlock,
        RpcReceipt,
        RpcHeader,
        TransactionSigned,
    >::call(client.as_ref(), req, block_id, state_overrides, block_overrides)
    .await
    .map_err(|e| eyre::eyre!("failed to query chain id from healthy node: {e}"))
}

pub async fn ipc_estimate_gas(
    req: RpcTransactionRequest,
    block_id: Option<BlockId>,
    state_overrides: Option<StateOverride>,
) -> Result<U256, eyre::Error> {
    let client = get_ipc_client().ok_or(eyre::eyre!("Failed to get RPC client"))?;
    reth_rpc_eth_api::EthApiClient::<
        RpcTransactionRequest,
        RpcTransaction,
        RpcBlock,
        RpcReceipt,
        RpcHeader,
        TransactionSigned,
    >::estimate_gas(client.as_ref(), req, block_id, state_overrides)
    .await
    .map_err(|e| eyre::eyre!("failed to query chain id from healthy node: {e}"))
}

pub async fn ipc_send_transaction(
    req: RpcTransactionRequest,
) -> Result<B256, eyre::Error> {
    let client = get_ipc_client().ok_or(eyre::eyre!("Failed to get RPC client"))?;
    reth_rpc_eth_api::EthApiClient::<
        RpcTransactionRequest,
        RpcTransaction,
        RpcBlock,
        RpcReceipt,
        RpcHeader,
        TransactionSigned,
    >::send_transaction(client.as_ref(), req)
    .await
    .map_err(|e| eyre::eyre!("failed to query chain id from healthy node: {e}"))
}

/// Send a raw signed transaction via IPC (eth_sendRawTransaction)
pub async fn ipc_send_raw_transaction(
    tx: TransactionSigned,
)-> Result<B256, eyre::Error> {
    let client = get_ipc_client().ok_or(eyre::eyre!("Failed to get RPC client"))?;
    let mut buf = Vec::new();
    tx.encode(&mut buf);
    let bytes = Bytes::from(buf);
    reth_rpc_eth_api::EthApiClient::<
        RpcTransactionRequest,
        RpcTransaction,
        RpcBlock,
        RpcReceipt,
        RpcHeader,
        TransactionSigned,
    >::send_raw_transaction(client.as_ref(), bytes)
    .await
    .map_err(|e| eyre::eyre!("failed to query chain id from healthy node: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::BscBlockBody;
    use alloy_consensus::Header;

    fn mk_block(num: u64, parent: B256) -> BscBlock {
        let header = Header { parent_hash: parent, number: num, ..Default::default() };
        BscBlock {
            header,
            body: BscBlockBody {
                inner: reth_ethereum_primitives::BlockBody::default(),
                sidecars: None,
            },
        }
    }

    #[test]
    fn test_body_cache_put_and_get() {
        let genesis = mk_block(0, B256::ZERO);
        let ghash = genesis.header.hash_slow();
        cache_full_block(genesis.clone());
        assert_eq!(get_cached_block_by_hash(&ghash).unwrap().header.hash_slow(), ghash);
        assert_eq!(get_cached_block_by_number(0).unwrap().header.hash_slow(), ghash);
    }

    // Note: eviction behavior depends on access patterns; an exhaustive eviction
    // test would be flaky here without introspecting the LRU. The cache is covered
    // by basic put/get tests above.
}
