//! Speculative block prefetcher for MDBX page cache warming.
//!
//! # Why
//!
//! BSC's pipeline Execution stage is disk-I/O bound on random MDBX reads.
//! Each SLOAD triggers a single synchronous ~4 KiB read (~90 μs). The NVMe
//! drive can sustain 1 M+ random-read IOPS at high queue depth, but MDBX
//! issues reads at QD1 (one in flight at a time), so we observed ~6 K IOPS
//! out of ~1 M available — 99 % of drive capability idle.
//!
//! # How
//!
//! Run N parallel worker threads that speculatively execute *upcoming*
//! blocks against a fresh state provider. Every state read the speculative
//! execution performs issues a concurrent disk read, warming the OS page
//! cache ahead of where the main execution thread will need it. Workers
//! **discard** their results — we don't care that the speculative state is
//! slightly divergent from truth because we only care about the side effect
//! (page cache warming).
//!
//! # Safety
//!
//! Workers MUST NOT commit anything to MDBX. They construct a
//! [`BasicBlockExecutor`]-like wrapper with their own [`revm::State<DB>`]
//! and drop it without calling `into_state()` or committing. The database
//! is untouched; only the OS page cache gets side-effected.
//!
//! # Gating
//!
//! The prefetcher is only useful during pipeline sync. Once the node is
//! at tip (BSC block time ≈ 400 ms → ~2.5 blocks/sec live), speculative
//! work is wasted CPU. The coordinator task measures the rolling
//! blocks-per-second rate of the main thread and pauses workers when it
//! drops below [`PrefetchConfig::pause_bps_threshold`], resuming when it
//! rises above [`PrefetchConfig::activate_bps_threshold`].

use alloy_primitives::{Address, B256, U256};
use parking_lot::Mutex;
use revm::bytecode::Bytecode;
use revm::state::AccountInfo;
use revm::Database;
use std::{
    cell::Cell,
    collections::VecDeque,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, OnceLock,
    },
    thread,
    time::{Duration, Instant},
};

/// A `revm::Database` wrapper that returns `U256::MAX` balance for every
/// account read. **Speculative prefetch workers only** — this type would
/// silently corrupt state if used by the main execution thread.
///
/// Purpose: workers run transactions against stale latest state so that each
/// SLOAD/CALL reads the right MDBX pages to warm the OS page cache. Under
/// stale state, the sender's real balance may be too low to cover the tx
/// gas+value, causing revm's pre-execution balance check to reject the tx
/// and bail out before any interesting reads happen. Returning MAX balance
/// from `basic()` makes the check pass so the tx body actually executes.
///
/// Storage, code, and block hash reads pass through unchanged so they still
/// hit real MDBX pages.
///
/// # Panics
///
/// Construction (`new`) panics if called from a thread that has not been
/// marked via [`mark_thread_as_speculative_worker`]. This is a hard guard
/// against accidentally wiring the wrapper into a non-worker code path.
#[derive(Debug)]
pub struct InfiniteBalanceDb<D> {
    inner: D,
}

impl<D> InfiniteBalanceDb<D> {
    /// Wraps `inner`. Panics if the current thread is not a speculative
    /// prefetch worker. The panic is intentional: this wrapper silently
    /// lies about account balances and must NEVER be used on the main
    /// execution thread or any RPC / engine path.
    pub fn new(inner: D) -> Self {
        assert!(
            is_speculative_worker(),
            "InfiniteBalanceDb may only be constructed on speculative prefetch worker \
             threads. Constructing it anywhere else would silently corrupt state because \
             the wrapper returns U256::MAX balance for every account."
        );
        Self { inner }
    }
}

impl<D: Database> Database for InfiniteBalanceDb<D> {
    type Error = D::Error;

    fn basic(&mut self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // Debug-mode belt-and-braces: panic if somehow this method runs on a
        // non-worker thread (e.g. someone `mem::forgets` a worker-constructed
        // wrapper and passes it across threads). In release builds this check
        // is compiled out.
        debug_assert!(
            is_speculative_worker(),
            "InfiniteBalanceDb::basic called on non-worker thread"
        );
        // Read the real account (this triggers the MDBX read we actually
        // want for cache warming). Two cases:
        //
        //   - Account exists → rewrite balance to MAX so revm's pre-execution
        //     balance check passes regardless of stale state.
        //   - Account does NOT exist at the stale state snapshot (common for
        //     senders of txs in blocks far ahead of the latest committed
        //     state) → synthesize an empty account with MAX balance. Without
        //     this, revm sees None → treats as 0 balance → rejects the tx
        //     with LackOfFundForMaxFee before any interesting reads happen.
        let info = self.inner.basic(address)?;
        Ok(Some(match info {
            Some(mut a) => {
                a.balance = U256::MAX;
                a
            }
            None => AccountInfo {
                balance: U256::MAX,
                nonce: 0,
                code_hash: revm::primitives::KECCAK_EMPTY,
                account_id: None,
                code: None,
            },
        }))
    }

    fn code_by_hash(&mut self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        self.inner.code_by_hash(code_hash)
    }

    fn storage(&mut self, address: Address, index: U256) -> Result<U256, Self::Error> {
        self.inner.storage(address, index)
    }

    fn block_hash(&mut self, number: u64) -> Result<B256, Self::Error> {
        self.inner.block_hash(number)
    }
}

thread_local! {
    /// Set to `true` on speculative prefetch worker threads. Read by the
    /// BscEvmConfig / BscBlockExecutor code paths so they can skip every
    /// global-mutating step (validator cache writes, snapshot provider
    /// updates, progress publishing, system contract upgrades, reward
    /// distribution, etc.) and only execute transactions for their
    /// read-side-effect. Do NOT toggle on the main execution thread.
    static SPECULATIVE_WORKER: Cell<bool> = const { Cell::new(false) };

    /// Flag used by the main-execution thread's one-shot self-pin. Set
    /// to `true` after the first successful sched_setaffinity call so
    /// subsequent calls can skip.
    static MAIN_THREAD_PINNED: Cell<bool> = const { Cell::new(false) };
}

/// Returns true if the current thread is a speculative prefetch worker.
/// Callers can use this to gate global-mutating code paths.
#[inline]
pub fn is_speculative_worker() -> bool {
    SPECULATIVE_WORKER.with(|c| c.get())
}

fn mark_thread_as_speculative_worker() {
    SPECULATIVE_WORKER.with(|c| c.set(true));
}

/// A closure that speculatively executes the block at the given number.
///
/// The implementation must:
/// 1. Read the block (header + body + transactions) from local storage.
/// 2. Create a *fresh* latest state provider from the provider factory.
/// 3. Run the block through a block executor against that state provider.
/// 4. **Drop** the executor without committing — no writes to MDBX.
///
/// Any error (missing block, provider failure, execution divergence) must
/// be caught inside the closure and logged at trace/debug level. The
/// caller treats the closure as infallible because failure only means the
/// page cache didn't get warmed for that block — not a correctness issue.
pub type SpeculativeExecFn = Arc<dyn Fn(u64) + Send + Sync + 'static>;

/// Prefetcher configuration.
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Number of OS worker threads to spawn. Set 0 to disable the prefetcher
    /// entirely.
    pub worker_count: usize,
    /// How many blocks ahead of the main thread we speculatively execute.
    /// The job queue is bounded by this value.
    pub window_size: u64,
    /// Minimum blocks/sec of main-thread throughput to **activate** workers
    /// (from paused state). Chosen to be well above BSC live-mode rate
    /// (~2.5 bps) with hysteresis margin.
    pub activate_bps_threshold: f64,
    /// Minimum blocks/sec of main-thread throughput to **keep** workers
    /// active. Below this for the full rate window → pause.
    pub pause_bps_threshold: f64,
    /// Number of lightweight B-tree warm workers (0 = disabled).
    pub warm_worker_count: usize,
    /// How far ahead the warm workers look (in blocks). Should be larger
    /// than `window_size` since warm workers are much cheaper.
    pub warm_window_size: u64,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            worker_count: 24,
            window_size: 128,
            activate_bps_threshold: 15.0,
            pause_bps_threshold: 8.0,
            warm_worker_count: 16,
            warm_window_size: 1024,
        }
    }
}

/// Shared state accessed by the main thread, the coordinator, and all workers.
struct PrefetchState {
    config: PrefetchConfig,
    /// Block number of the latest block the MAIN execution thread has finished.
    /// Updated by [`publish_executed_block`] from inside the executor.
    latest_executed: AtomicU64,
    /// Whether workers are currently allowed to dequeue and run jobs.
    workers_active: AtomicBool,
    /// Job queue: block numbers to speculatively execute. Ordered ascending.
    queue: Mutex<VecDeque<u64>>,
    /// Job queue for lightweight B-tree warm workers. Looks further ahead
    /// than the speculative execution queue.
    warm_queue: Mutex<VecDeque<u64>>,
}

static PREFETCH_STATE: OnceLock<Arc<PrefetchState>> = OnceLock::new();

/// Called by the main execution thread after finishing each block. Updates
/// the atomic that the coordinator uses to compute the rate and that workers
/// use to skip stale jobs. Zero cost when the prefetcher was never initialized.
#[inline]
pub fn publish_executed_block(block_number: u64) {
    // Speculative workers also run block executors and go through
    // `finish()`, which calls this function. We must ignore those calls
    // otherwise the workers will clobber the main thread's progress tracker.
    if is_speculative_worker() {
        return;
    }
    // One-shot: first time the main execution thread calls this function,
    // pin it to a single CCD0 core so the kernel load balancer can't
    // migrate it away. Without this, main thread bounces between CCD0
    // cores (observed: alternating between 1 and 3) every few hundred
    // ms which cold-starts L1/L2 caches on each move.
    #[cfg(target_os = "linux")]
    pin_main_thread_once();
    if let Some(state) = PREFETCH_STATE.get() {
        state.latest_executed.store(block_number, Ordering::Relaxed);
    }
}

/// One-shot per-thread: pin the current thread to a single CCD0 core.
/// Idempotent via a thread-local flag. Called from `publish_executed_block`
/// which only runs on the main execution thread (speculative workers
/// early-return before reaching it).
#[cfg(target_os = "linux")]
fn pin_main_thread_once() {
    MAIN_THREAD_PINNED.with(|pinned| {
        if pinned.get() {
            return;
        }
        // Pick a specific logical core on CCD0. We use logical core 1 —
        // it's on CCD0 (physical core 1) and not core 0 (which tends to
        // get more kernel interrupts on Linux).
        const TARGET_CORE: usize = 1;
        let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
        unsafe {
            libc::CPU_ZERO(&mut set);
            libc::CPU_SET(TARGET_CORE, &mut set);
        }
        let rc = unsafe {
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set)
        };
        if rc == 0 {
            tracing::info!(
                target: "bsc::prefetcher",
                core = TARGET_CORE,
                "pinned main execution thread to single CCD0 core"
            );
        } else {
            let err = std::io::Error::last_os_error();
            tracing::warn!(
                target: "bsc::prefetcher",
                core = TARGET_CORE,
                error = %err,
                "failed to pin main execution thread"
            );
        }
        // Mark as pinned either way; we don't want to retry on every call.
        pinned.set(true);
    });
}

/// Temporarily reset the current thread's CPU affinity to all online
/// cores, run the closure, then re-pin the main thread back to its
/// single CCD0 core.
///
/// The parallel-evm worker threads spawned inside the closure inherit
/// the caller's affinity at `thread::spawn` time. Without this helper,
/// they'd all land on the single core that
/// [`pin_main_thread_once`] pins us to — so 16 workers would
/// serialise on one core and the parallel path would deliver ~0x
/// speedup over serial.
///
/// Non-linux targets are a no-op — the `pin_main_thread_once` helper
/// is already gated to `target_os = "linux"`, so there's no pinning
/// on other platforms and nothing to reset.
#[cfg(target_os = "linux")]
pub fn with_all_cores_affinity<R>(f: impl FnOnce() -> R) -> R {
    // Snapshot: build an "all online cores" cpu_set_t and set it as the
    // current thread's affinity. Save the previous set so we can restore.
    let nproc = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) }.max(1) as usize;
    let mut prev: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    let have_prev = unsafe {
        libc::CPU_ZERO(&mut prev);
        libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut prev) == 0
    };

    let mut all: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe {
        libc::CPU_ZERO(&mut all);
        for cpu in 0..nproc.min(libc::CPU_SETSIZE as usize) {
            libc::CPU_SET(cpu, &mut all);
        }
        libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &all);
    }

    // Thread-local flag used by `pin_main_thread_once` — clear it so the
    // FIRST post-closure `publish_executed_block` call re-pins us.
    MAIN_THREAD_PINNED.with(|p| p.set(false));

    let result = f();

    // Restore. Prefer the snapshotted prev set; if getaffinity failed
    // (never seen in practice on Linux), fall back to re-pinning via
    // the existing helper.
    if have_prev {
        unsafe {
            libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &prev);
        }
        // Whatever prev was, assume it represented an intentional pin —
        // mark the thread-local as "pinned" so pin_main_thread_once
        // doesn't fight it on the next block boundary.
        MAIN_THREAD_PINNED.with(|p| p.set(true));
    } else {
        pin_main_thread_once();
    }

    result
}

/// No-op version for non-Linux. Just runs the closure.
#[cfg(not(target_os = "linux"))]
pub fn with_all_cores_affinity<R>(f: impl FnOnce() -> R) -> R {
    f()
}

/// Initialize the prefetcher and spawn the worker threads and coordinator task.
///
/// Should be called at most once, after the node builder has materialized
/// the provider factory and EVM config. If `config.worker_count == 0` the
/// prefetcher is disabled and this call is a no-op.
pub fn init(
    config: PrefetchConfig,
    speculative_execute: SpeculativeExecFn,
    block_warm: Option<BlockWarmFn>,
) {
    if config.worker_count == 0 && config.warm_worker_count == 0 {
        tracing::info!(target: "bsc::prefetcher", "prefetcher disabled (all worker counts = 0)");
        return;
    }

    let state = Arc::new(PrefetchState {
        config: config.clone(),
        latest_executed: AtomicU64::new(0),
        workers_active: AtomicBool::new(false),
        queue: Mutex::new(VecDeque::with_capacity(config.window_size as usize * 2)),
        warm_queue: Mutex::new(VecDeque::with_capacity(config.warm_window_size as usize * 2)),
    });

    if PREFETCH_STATE.set(state.clone()).is_err() {
        tracing::warn!(target: "bsc::prefetcher", "prefetcher already initialized; ignoring");
        return;
    }

    for worker_id in 0..config.worker_count {
        let state = state.clone();
        let exec = speculative_execute.clone();
        thread::Builder::new()
            .name(format!("bsc-prefetch-{}", worker_id))
            .spawn(move || worker_loop(worker_id, state, exec))
            .expect("failed to spawn prefetcher worker");
    }

    if let Some(warm) = block_warm {
        for worker_id in 0..config.warm_worker_count {
            let state = state.clone();
            let warm = warm.clone();
            thread::Builder::new()
                .name(format!("bsc-warm-{}", worker_id))
                .spawn(move || warm_worker_loop(worker_id, state, warm))
                .expect("failed to spawn warm worker");
        }
    }

    // Coordinator is async because its work is mostly waiting.
    tokio::spawn(coordinator_loop(state));

    tracing::info!(
        target: "bsc::prefetcher",
        spec_workers = config.worker_count,
        warm_workers = config.warm_worker_count,
        spec_window = config.window_size,
        warm_window = config.warm_window_size,
        activate_bps = config.activate_bps_threshold,
        pause_bps = config.pause_bps_threshold,
        "speculative block prefetcher initialized"
    );
}

/// Pin the current thread to the same CCD as the main execution thread
/// (CCD0: logical cores 0-7 and SMT siblings 16-23 on Zen 5 16-core).
///
/// Why same CCD: speculative workers are I/O bound (each uses ~4% CPU).
/// They're not competing with the main thread for compute — they're
/// prefetching state pages. When workers run on the SAME CCD as the main
/// thread, their reads populate the shared 32 MB L3, so main thread gets
/// L3 hits on subsequent accesses instead of DRAM-latency fetches. This
/// is the whole point of a prefetcher: warm the cache level the consumer
/// uses.
///
/// Putting workers on CCD1 (the previous pin) meant their reads warmed a
/// DIFFERENT L3 that main thread couldn't see, defeating most of the
/// benefit.
#[cfg(target_os = "linux")]
fn pin_worker_to_ccd0(worker_id: usize) {
    let num_cpus = num_cpus_online();
    let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
    unsafe { libc::CPU_ZERO(&mut set) };
    let mut count = 0;
    for cpu in 0..num_cpus {
        // CCD0 physical range (0..=7) + CCD0 SMT siblings (16..=23) on
        // Zen 5 16-core. On other topologies this falls back to whatever
        // cores exist in that range.
        if (0..8).contains(&cpu) || (16..24).contains(&cpu) {
            if cpu < num_cpus {
                unsafe { libc::CPU_SET(cpu, &mut set) };
                count += 1;
            }
        }
    }
    if count == 0 {
        tracing::warn!(
            target: "bsc::prefetcher",
            worker_id,
            num_cpus,
            "no CCD0 cores available; leaving worker unpinned"
        );
        return;
    }
    let rc = unsafe {
        libc::sched_setaffinity(
            0, // current thread
            std::mem::size_of::<libc::cpu_set_t>(),
            &set,
        )
    };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        tracing::warn!(
            target: "bsc::prefetcher",
            worker_id,
            error = %err,
            "sched_setaffinity failed"
        );
    }
}

#[cfg(target_os = "linux")]
fn num_cpus_online() -> usize {
    unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) as usize }
}

fn worker_loop(worker_id: usize, state: Arc<PrefetchState>, exec: SpeculativeExecFn) {
    // Mark this OS thread as a speculative worker so every code path inside
    // the BSC block executor can gate global mutations (validator cache,
    // snapshot provider, progress publishing, etc.).
    mark_thread_as_speculative_worker();

    // Pin to CCD0 (same CCD as the main execution thread). Workers are
    // I/O bound and using very little CPU, so they don't meaningfully
    // compete with main thread for compute. They DO populate CCD0's
    // shared 32 MB L3 when they read state pages, which is exactly what
    // we want the main thread to find hot on its next access.
    //
    // (Earlier iteration pinned workers to CCD1 for "isolation". That was
    // backwards: it gave workers their own separate L3 that main thread
    // couldn't see, so the prefetcher warmed the wrong cache.)
    #[cfg(target_os = "linux")]
    pin_worker_to_ccd0(worker_id);

    tracing::debug!(target: "bsc::prefetcher", worker_id, "worker started");

    loop {
        // Paused: sleep a bit and recheck.
        if !state.workers_active.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(200));
            continue;
        }

        // Pop next job.
        let block_num = state.queue.lock().pop_front();
        let Some(block_num) = block_num else {
            thread::sleep(Duration::from_millis(2));
            continue;
        };

        // Skip stale jobs: main thread already finished this block.
        if block_num <= state.latest_executed.load(Ordering::Relaxed) {
            continue;
        }

        // Run speculative execution. The closure is responsible for catching
        // and logging its own errors — we treat it as infallible here.
        exec(block_num);
    }
}

/// A closure that performs lightweight B-tree warming for a block.
///
/// Instead of full EVM execution, this reads:
/// 1. Each transaction's `to` address → basic_account() (warms account B-tree)
/// 2. Each transaction's `from` address → basic_account()
/// 3. Each access_list item → basic_account() + storage() for each key
///
/// This is ~100x cheaper than full execution but warms the critical B-tree
/// pages that the main thread will need.
pub type BlockWarmFn = Arc<dyn Fn(u64) + Send + Sync + 'static>;

fn warm_worker_loop(worker_id: usize, state: Arc<PrefetchState>, warm: BlockWarmFn) {
    mark_thread_as_speculative_worker();

    tracing::debug!(target: "bsc::prefetcher", worker_id, "warm worker started");

    loop {
        if !state.workers_active.load(Ordering::Relaxed) {
            thread::sleep(Duration::from_millis(200));
            continue;
        }

        let block_num = state.warm_queue.lock().pop_front();
        let Some(block_num) = block_num else {
            thread::sleep(Duration::from_millis(2));
            continue;
        };

        if block_num <= state.latest_executed.load(Ordering::Relaxed) {
            continue;
        }

        warm(block_num);
    }
}

async fn coordinator_loop(state: Arc<PrefetchState>) {
    // Ring buffer of recent (sample_time, latest_executed) pairs.
    const WINDOW: Duration = Duration::from_secs(10);
    let sample_interval = Duration::from_secs(1);
    let mut samples: VecDeque<(Instant, u64)> = VecDeque::with_capacity(16);
    let mut next_to_schedule: u64 = 0;
    let mut next_warm_schedule: u64 = 0;

    loop {
        tokio::time::sleep(sample_interval).await;

        let now = Instant::now();
        let latest = state.latest_executed.load(Ordering::Relaxed);

        samples.push_back((now, latest));
        while let Some(&(t, _)) = samples.front() {
            if now.duration_since(t) > WINDOW {
                samples.pop_front();
            } else {
                break;
            }
        }

        // Rolling blocks-per-second over the sample window.
        let bps = if samples.len() >= 2 {
            let &(t_first, b_first) = samples.front().unwrap();
            let elapsed = now.duration_since(t_first).as_secs_f64();
            if elapsed > 0.0 {
                (latest.saturating_sub(b_first) as f64) / elapsed
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Hysteresis.
        let was_active = state.workers_active.load(Ordering::Relaxed);
        let now_active = if was_active {
            bps >= state.config.pause_bps_threshold
        } else {
            bps >= state.config.activate_bps_threshold
        };
        if now_active != was_active {
            state.workers_active.store(now_active, Ordering::Relaxed);
            tracing::info!(
                target: "bsc::prefetcher",
                bps = format!("{:.1}", bps),
                active = now_active,
                latest,
                "prefetcher state changed"
            );
        }

        if !now_active {
            // Drain queues and reset scheduler cursors so we don't replay stale
            // work when we reactivate.
            state.queue.lock().clear();
            state.warm_queue.lock().clear();
            next_to_schedule = 0;
            next_warm_schedule = 0;
            continue;
        }

        // First activation or we've drifted: reseed the scheduling cursors.
        if next_to_schedule <= latest {
            next_to_schedule = latest + 1;
        }
        if next_warm_schedule <= latest {
            // Warm workers start AFTER the speculative window — no point
            // warming pages that spec workers will warm via full execution.
            next_warm_schedule = latest + state.config.window_size + 1;
        }

        // Refill speculative execution queue.
        let window_end = latest + state.config.window_size;
        {
            let mut q = state.queue.lock();
            while let Some(&front) = q.front() {
                if front <= latest {
                    q.pop_front();
                } else {
                    break;
                }
            }
            while next_to_schedule <= window_end && q.len() < state.config.window_size as usize {
                q.push_back(next_to_schedule);
                next_to_schedule += 1;
            }
        }

        // Refill warm queue (further ahead than spec queue).
        if state.config.warm_worker_count > 0 {
            let warm_end = latest + state.config.window_size + state.config.warm_window_size;
            let mut wq = state.warm_queue.lock();
            while let Some(&front) = wq.front() {
                if front <= latest {
                    wq.pop_front();
                } else {
                    break;
                }
            }
            while next_warm_schedule <= warm_end
                && wq.len() < state.config.warm_window_size as usize
            {
                wq.push_back(next_warm_schedule);
                next_warm_schedule += 1;
            }
        }
    }
}
