//! BSC adapter for [`parallel_evm`].
//!
//! Implements [`parallel_evm::VmBuilder`] over BSC's `BscEvm` variant, so a
//! caller can drop `ParallelExecutor<BscVmBuilder>` into the block-execution
//! path and get parallel tx execution without changing anything about the
//! Parlia/system-tx hooks that live outside the tx loop.
//!
//! # Scope of this module
//!
//! - `BscVmBuilder`: the VmBuilder impl.
//! - `RethStorage`: a thin adapter that implements `parallel_evm::Storage`
//!   over any reth `StateProviderBox` so the parallel executor can read
//!   through MDBX/static files unchanged.
//!
//! Not in this module: the integration into `BscBlockExecutor` itself.
//! That lives behind a CLI flag and requires careful handling of BSC's
//! pre/post-execution hooks (snapshot updates, validator rewards, system
//! txs). The integration sketch is at the bottom of this doc comment.
//!
//! # Wiring status (2026-04-17)
//!
//! Landed in this series of commits:
//!
//! - `--bsc.parallel-execute` CLI flag sets a process-wide atomic that
//!   `BscEvmConfig::context_for_block` reads into `BscBlockExecutionCtx
//!   ::parallel` on every non-speculative block.
//! - [`RefStorage`] implements [`parallel_evm::Storage`] over any
//!   `&DB: DatabaseRef` — including revm's `State<DB: DatabaseRef>`
//!   itself, whose cache-first DatabaseRef impl lets parallel workers
//!   observe `apply_pre_execution_changes` mutations lock-free.
//! - `BscBlockExecutor::execute_block` override with `ctx.parallel`
//!   runtime branch (scaffold, falls through to serial).
//! - `HertzPatchManager::has_patch(&tx_hash)` helper for fallback
//!   detection.
//!
//! **An earlier attempt landed `BscBlockExecutor::execute_block_parallel
//! <DB>` with method-level bounds `EVM::DB = &'a mut State<DB>`, `DB:
//! DatabaseRef + Send + Sync`. It was REVERTED — the bounds couldn't
//! be satisfied at production call sites because reth's concrete DB
//! chain `StateProviderDatabase<LatestStateProviderRef<'_, Database
//! Provider<TX, N>>>` requires `TX: DbTx + Sync`, and `DbTx: Debug +
//! Send` by design (MDBX transactions are intentionally not `Sync`).
//! The compile-check test was a false positive on a generic DB.**
//!
//! # The correct design (next session)
//!
//! Reth's parallel-state primitive is `ConsistentDbView<Factory>` —
//! factory IS `Send + Sync`, each worker calls `view.provider_ro()`
//! for its own fresh tx. The correct `execute_block_parallel`
//! signature accepts a factory instead of relying on `&State<DB>`:
//!
//! ```ignore
//! pub fn execute_block_parallel<F>(
//!     mut self,
//!     view: &ConsistentDbView<F>,
//!     transactions: impl IntoIterator<Item = impl ExecutableTx<Self>>,
//! ) -> Result<BlockExecutionResult<R::Receipt>, BlockExecutionError>
//! where
//!     F: DatabaseProviderFactory + Send + Sync + Clone + 'static,
//!     // existing BscBlockExecutor bounds
//! {
//!     // 1. apply_pre_execution_changes (mutates self.evm.db())
//!     // 2. Snapshot current bundle_state IMMUTABLY from self.evm.db()
//!     //    (this requires concrete-type access — see step 0 below)
//!     // 3. Materialise txs; user/system split; Hertz detection
//!     // 4. Build LayeredStorage{bundle_snapshot, view}:
//!     //    - basic(addr): check bundle_snapshot first, else
//!     //      view.provider_ro()?.basic_account(addr)
//!     //    - similar for storage, code_by_hash, block_hash
//!     // 5. ParallelExecutor::execute(&layered, ...)
//!     // 6. Serial commit via self.commit_transaction (unchanged)
//!     // 7. System tail + apply_post_execution_changes
//! }
//! ```
//!
//! Step 0 (prerequisite): BscBlockExecutor cannot access
//! `self.evm.db().bundle_state` through its generic `E::DB` bound.
//! Options: (a) tighten `BscBlockExecutor`'s `E::DB` via a NEW
//! construction path (e.g., `BscEvmConfig::create_parallel_executor`
//! inherent method with tight bounds, not the trait method); (b) pass
//! the bundle snapshot in as an argument at `execute_block_parallel`
//! entry; (c) fork `BscBlockExecutor` into a parallel-specific wrapper
//! type with concrete `DB` generic.
//!
//! (b) is cleanest — keeps BscBlockExecutor generic, the caller
//! (custom ExecutionStage or payload validator) captures the bundle
//! snapshot from `State<DB>` BEFORE handing it to the method.
//!
//! # Integration points for activation
//!
//! Both sync paths eventually call `ConfigureEvm`-provided executors:
//!
//! - **Staged sync**: reth's `ExecutionStage` → `batch_executor(db)`
//!   → `BasicBlockExecutor::execute_one` → `executor_for_block(...)
//!   .execute_block(...)`. To activate parallel, replace with a
//!   custom stage in our node builder that holds a
//!   `ConsistentDbView`, iterates blocks, and calls
//!   `execute_block_parallel(view, txs)` instead.
//!
//! - **Live sync**: reth's `EngineTree::payload_validator::execute_
//!   block` creates executor via `create_executor` and iterates txs
//!   with streaming receipts. Parallel integration either (i) accepts
//!   loss of receipt streaming for parallel blocks or (ii) requires a
//!   custom `PayloadValidator` impl.
//!
//! For a focused follow-up session, start with (staged sync only) —
//! smaller scope, validates the architecture. Live sync activation
//! becomes a follow-up once staged-sync perf numbers justify.
//!
//! Next — the substantive work to turn the scaffold into a real
//! `ParallelExecutor` dispatch. Done in order because step 1 is the
//! actual blocker:
//!
//! 1. **Trait-bound wall.** Attempted in this session: tightening
//!    `E::DB: DatabaseRef + Send + Sync` on the `BlockExecutor` impl
//!    and propagating through `ConfigureEvm::create_executor` /
//!    `create_block_builder`. The propagation fails with E0276: reth's
//!    `ConfigureEvm` trait fixes `DB: StateDB + 'a` on
//!    `create_executor`, so our impl cannot add stricter bounds without
//!    modifying reth itself. The rollback is in git history.
//!
//!    Consequence: `ParallelExecutor` cannot be called with
//!    `RefStorage::new(self.evm.db())` from inside
//!    `BlockExecutor::execute_block`, because we have no way to assert
//!    `E::DB: DatabaseRef` at that call site.
//!
//!    The bigger insight follow-up investigation surfaced: reth's
//!    pipeline execution stage **batches** blocks — `State<DB>.bundle
//!    _state` accumulates transitions across many blocks before the
//!    single end-of-batch commit. So an "external state provider at
//!    parent's block number" is NOT a valid parallel-read source
//!    during pipeline sync: it misses every in-batch committed block's
//!    changes. The only correct read source for parallel is the
//!    executor's own `&State<DB>` (whose DatabaseRef impl reads cache
//!    → bundle → underlying-DB in order) — which is exactly the access
//!    our bounds don't permit.
//!
//!    ## The deeper wall (investigated further 2026-04-17)
//!
//!    Attempted minimal reth fork: tightening
//!    `BasicBlockExecutor::Executor<DB>` to require `DB: DatabaseRef +
//!    Send + Sync` with std::error::Error bounds on `DB::Error`. reth-
//!    evm compiles clean with that local change (the narrow sync-path
//!    tightening doesn't affect block-builder / payload paths that go
//!    through `CachedReadsDbMut`). But the workspace check surfaces
//!    propagation to `StateProviderBox = Box<dyn StateProvider + Send
//!    + 'static>` — no `Sync`. ef-tests and exex backfill construct
//!    `StateProviderDatabase(Box<dyn StateProvider + Send>)` which
//!    doesn't satisfy the new `Sync` requirement.
//!
//!    Adding `Sync` to the typealias cascades: `StateProvider` itself
//!    lacks `Sync` supertrait, `DatabaseProvider<TX>` only `Sync` if
//!    `TX: Sync`, and `pub trait DbTx: Debug + Send` is fundamentally
//!    not `Sync` — **MDBX transactions can be moved between threads
//!    but not concurrently shared**. This is a deliberate reth/MDBX
//!    design property, not a trait-declaration oversight. Reth's own
//!    parallel-state answer is `ConsistentDbView<Factory>`: a
//!    Send+Sync factory that hands each worker its own fresh tx — but
//!    that model doesn't compose with `parallel-evm`'s shared-
//!    `&Storage` interface.
//!
//!    ## Options left (none cheap)
//!
//!    - **(a) Batch-execution parallel across blocks.** Use
//!      `ConsistentDbView` directly, each worker processes an entire
//!      independent block. Limited applicability — most real BSC
//!      blocks are dependent on their immediate predecessor's state —
//!      and the existing speculative-prefetcher already covers the
//!      "warm multiple blocks on separate threads" axis.
//!
//!    - **(b) Bundle-overlay storage + ConsistentDbView.** Layered
//!      read source: capture `self.evm.db().bundle_state` immutably at
//!      execute_block entry (so batch-correctness is preserved), fall
//!      through to a `ConsistentDbView`-spawned per-worker tx for
//!      committed state. Requires either (i) upstream RFC adding a
//!      bundle-state accessor to the `StateDB` trait, or (ii)
//!      refactoring `BscBlockExecutor` to hold `&mut State<DB>`
//!      concretely instead of a generic `E::DB` — multi-file surgery
//!      that trades generic flexibility for concrete access.
//!
//!    - **(c) Pause parallel activation, keep the scaffold.** RefStorage
//!      + BscVmBuilder generic + execute_block runtime branch + CLI
//!      flag are all genuinely useful infrastructure for a future
//!      paradigm-upstream RFC or BscBlockExecutor refactor. Redirect
//!      this session's remaining energy to other reth-bsc priorities.
//!
//!    **Call:** (c). The combination of reth's trait-bound architecture
//!    and MDBX's non-Sync tx design means **no single small change
//!    unlocks in-block parallel** — (a) is narrow-use, (b) is a
//!    refactor-sized project. The scaffold stays in place for the next
//!    focused session to pick up with either an upstream paradigm PR
//!    or an in-crate BscBlockExecutor concrete-type refactor. Flag-on
//!    is byte-identical to flag-off today; zero runtime risk.
//! 2. In the parallel branch, collect txs into `Vec<(BscTxEnv,
//!    Recovered<TransactionSigned>)>`. `BscTxEnv::from_recovered_tx` is
//!    already available; the Recovered form is rebuilt from the
//!    `ExecutableTx`'s `tx()`/`signer()` via `Recovered::new_unchecked`.
//! 3. Split user txs from system txs (via
//!    `is_system_transaction(&signed, signer, beneficiary)`) — system
//!    txs take the existing serial path through
//!    `execute_transaction_with_result_closure`.
//! 4. Detect Hertz-patched tx hashes up front and force serial for the
//!    whole block if any are present. Patch entries are in
//!    `super::patch::{MAINNET,CHAPEL}_PATCHES_BEFORE_TX`; expose a
//!    `has_patch(tx_hash)` helper on `HertzPatchManager` for the check.
//! 5. Call `ParallelExecutor::execute(&RefStorage::new(self.evm.db()),
//!    block_env.clone(), spec, user_tx_envs)`. Drop the storage borrow
//!    before entering the serial commit phase.
//! 6. Serial commit phase: for each tx in block order, convert
//!    `TxResult::Ok(ResultAndState)` into `BscTxResult` and call
//!    `self.commit_transaction(...)` — which already handles on_state
//!    hook, receipt builder, gas/blob accounting, and state commit.
//!    Hertz patches stay no-ops via the existing `patch_before/after_tx`
//!    calls (they'll never apply because step 4 forced serial).
//! 7. On any `parallel_evm::Error` (budget exceeded, storage failure),
//!    abort to a pure-serial retry of the block. Block-STM is
//!    deterministic, so any state-root divergence during validation is
//!    a real bug — bubble it up immediately.
//!
//! # Integration sketch
//!
//! Inside `BscBlockExecutor::execute_transactions` (or in a new
//! `execute_one_parallel` method), guarded by a runtime flag:
//!
//! 1. Partition block txs into user-txs (non-system) and system-tx
//!    candidates. System txs go to the existing serial post-execution
//!    handler; don't parallelize those.
//! 2. Wrap the executor's state DB in `RethStorage`.
//! 3. Build `BscVmBuilder` with the current chain spec + factory.
//! 4. `ParallelExecutor::new(cfg, vm_builder).execute(&storage,
//!    block_env, spec, user_txs)` — returns per-tx results.
//! 5. For each result in tx order: if `Ok(rs)`, commit `rs.state` to the
//!    executor's `State<DB>` via `DatabaseCommit::commit`, then build a
//!    receipt from `rs.result` via the executor's existing receipt
//!    builder. If `Err(msg)`, record a failed tx (empty receipt).
//! 6. Continue with the serial post-execution path (system txs, validator
//!    rewards, Feynman init, snapshot write, genesis-1 contract deploy).
//!
//! Any fallback condition (storage error, re-execution budget exceeded)
//! the parallel executor reports via `Err(parallel_evm::Error)` — the
//! adapter should fall back to the pure-serial path for that block.

use crate::{
    chainspec::BscChainSpec,
    evm::transaction::BscTxEnv,
    hardforks::bsc::BscHardfork,
    node::evm::BscEvmFactory,
};
use alloy_consensus::constants::KECCAK_EMPTY;
use alloy_primitives::{Address, B256, U256};
use parallel_evm::{DbError, DbWrapper, Storage, TransactOutcome, VmBuilder};
use reth_chainspec::EthChainSpec;
use reth_evm::{EvmEnv, EvmFactory};
use reth_provider::{ProviderError, StateProviderFactory};
use revm::{
    bytecode::Bytecode,
    context::{result::{EVMError, HaltReason}, BlockEnv, CfgEnv},
    database::BundleState,
    state::AccountInfo,
    DatabaseRef, ExecuteEvm,
};
use std::{marker::PhantomData, sync::Arc};

/// `parallel_evm::Storage` adapter that borrows any `&DB: DatabaseRef`.
///
/// This is the production-path adapter we hand to `ParallelExecutor`.
/// Crucially, `State<DB: DatabaseRef>` itself implements `DatabaseRef`
/// via cache→bundle→underlying-DB lookups *without* mutating its cache,
/// which is exactly what we need: parallel workers read through the
/// executor's current `&State<DB>` and observe the `apply_pre_execution
/// _changes` mutations without any locking. Writes are never routed
/// through `Storage`; they're accumulated in `parallel_evm`'s MvMemory
/// and serialised back to the executor's `State<DB>` after the parallel
/// phase finishes.
///
/// The `'a` lifetime keeps us honest: the adapter cannot outlive the
/// `&State<DB>` it borrows. Scoped worker threads inside
/// `ParallelExecutor::execute` join before returning, so the borrow is
/// safely bounded to a single `execute_block` invocation.
#[derive(Debug)]
pub struct RefStorage<'a, DB: DatabaseRef> {
    db: &'a DB,
    _marker: PhantomData<&'a ()>,
}

impl<'a, DB: DatabaseRef> RefStorage<'a, DB> {
    pub fn new(db: &'a DB) -> Self {
        Self { db, _marker: PhantomData }
    }
}

impl<'a, DB> Storage for RefStorage<'a, DB>
where
    DB: DatabaseRef + std::fmt::Debug + Send + Sync + 'a,
    DB::Error: std::error::Error + Send + Sync + 'static,
{
    type Error = DB::Error;

    fn basic(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        DatabaseRef::basic_ref(self.db, address)
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        DatabaseRef::code_by_hash_ref(self.db, code_hash)
    }

    fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        DatabaseRef::storage_ref(self.db, address, slot)
    }

    fn block_hash(&self, number: u64) -> Result<B256, Self::Error> {
        DatabaseRef::block_hash_ref(self.db, number)
    }
}

/// Abstracts the "spawn a fresh per-worker state provider" operation so
/// [`LayeredStorage`] can carry a trait object.
///
/// Any `StateProviderFactory + Send + Sync` satisfies this via the
/// blanket impl. The indirection exists because we stash a concrete
/// factory in a global (see `crate::shared::set_parallel_state_spawner`)
/// and want to avoid propagating a generic parameter through
/// [`crate::node::evm::executor::BscBlockExecutor::execute_block_parallel`]
/// down to every caller in the stage pipeline.
pub trait ParallelStateSpawner: Send + Sync {
    /// Spawn a fresh `StateProviderBox` for a worker thread. The
    /// returned provider is `Send`-only (MDBX txs are never `Sync`);
    /// the caller is responsible for keeping it on a single thread.
    fn spawn_state(
        &self,
    ) -> reth_provider::ProviderResult<reth_provider::StateProviderBox>;
}

impl<F> ParallelStateSpawner for F
where
    F: StateProviderFactory + Send + Sync + ?Sized,
{
    fn spawn_state(
        &self,
    ) -> reth_provider::ProviderResult<reth_provider::StateProviderBox> {
        self.latest()
    }
}

/// `parallel_evm::Storage` adapter that layers an immutable `BundleState`
/// snapshot over a Send+Sync state-provider spawner.
///
/// # Why not `ConsistentDbView` or a plain `&DB: DatabaseRef`
///
/// - `ConsistentDbView<Factory>` was the initial design target, but its
///   `provider_ro()` performs a tip-consistency check that fails with
///   `ConsistentViewError::Syncing` during pipeline staged-sync — the
///   very path where parallel execution needs to run. So we bind on
///   [`ParallelStateSpawner`] (blanket-impl'd for `StateProviderFactory`)
///   directly and accept that live-sync callers would add their own
///   consistency layer on top if needed.
/// - `&DB: DatabaseRef` (what `RefStorage` uses) fails at the production
///   monomorphization because reth's `StateProviderDatabase<LatestStateProviderRef
///   <'_, DatabaseProvider<TX>>>` can't be `Sync` — MDBX txs are Send-only
///   by design. The factory-based approach sidesteps this: each worker
///   calls `spawner.spawn_state()` from its own thread to get a fresh
///   `StateProviderBox`, which only needs `Send`.
///
/// # Layered read pattern
///
/// 1. **Bundle overlay** (`bundle_snapshot`): captures in-batch committed
///    transitions. During pipeline sync, reth's `ExecutionStage` batches
///    many blocks into a single commit — a plain "latest" state provider
///    would miss every intra-batch change. The caller snapshots
///    `State<DB>::bundle_state` immutably before calling
///    `execute_block_parallel` and passes it here as `bundle_snapshot`.
/// 2. **StateProvider fallback** (`spawner.spawn_state()`): for any read
///    the bundle doesn't answer, spawn a fresh per-worker state provider
///    and delegate.
///
/// # Cost model (Phase 2)
///
/// Phase 2 spawns a fresh `StateProviderBox` on EVERY fallback read.
/// That's one MDBX tx + trait-object boxing per non-bundle read. This
/// is painful for perf but trivially correct. Phase 3 optimization slot:
/// add a `ThreadLocal<RefCell<Option<StateProviderBox>>>` so each worker
/// thread spawns at most one provider per `execute_block_parallel`
/// invocation.
///
/// # Lifetime contract
///
/// The `'a` lifetime binds both borrows to a single
/// `execute_block_parallel` call. `ParallelExecutor::execute` uses scoped
/// threads that join before return, so the borrows are safely bounded.
pub struct LayeredStorage<'a, S: ?Sized> {
    /// Immutable snapshot of in-batch committed state.
    bundle: &'a BundleState,
    /// Factory-equivalent for spawning per-worker state providers.
    spawner: &'a S,
}

impl<'a, S: ?Sized> LayeredStorage<'a, S> {
    /// Construct a new layered storage over the given bundle snapshot and
    /// spawner reference.
    pub const fn new(bundle: &'a BundleState, spawner: &'a S) -> Self {
        Self { bundle, spawner }
    }
}

impl<'a, S: ?Sized> std::fmt::Debug for LayeredStorage<'a, S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayeredStorage")
            .field("bundle_accounts", &self.bundle.state().len())
            .field("bundle_contracts", &self.bundle.contracts.len())
            .finish_non_exhaustive()
    }
}

impl<'a, S: ?Sized> Storage for LayeredStorage<'a, S>
where
    S: ParallelStateSpawner,
{
    type Error = ProviderError;

    fn basic(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error> {
        // Layer 1: bundle overlay. A destroyed account reads as
        // non-existent; a bundle account with Some(info) wins outright.
        // For entries with None info (loaded-not-existing), we fall
        // through — the DB-backed read yields the correct answer.
        if let Some(bundle_acct) = self.bundle.account(&address) {
            if bundle_acct.status.was_destroyed() {
                return Ok(None);
            }
            if let Some(info) = &bundle_acct.info {
                return Ok(Some(info.clone()));
            }
        }

        // Layer 2: spawn a per-call state provider and read through.
        let sp = self.spawner.spawn_state()?;
        let Some(acct) = sp.basic_account(&address)? else {
            return Ok(None);
        };
        let code_hash = acct.bytecode_hash.unwrap_or(KECCAK_EMPTY);
        // Leave `code` unpopulated; revm's EVM loads bytecode lazily via
        // `code_by_hash` when a CALL lands. Avoids an unconditional DB
        // fetch on every account read.
        Ok(Some(AccountInfo {
            balance: acct.balance,
            nonce: acct.nonce,
            code_hash,
            code: None,
            account_id: None,
        }))
    }

    fn code_by_hash(&self, code_hash: B256) -> Result<Bytecode, Self::Error> {
        // Layer 1: bundle-local contracts (any CREATE/CREATE2 in the
        // batch landed here).
        if let Some(bc) = self.bundle.bytecode(&code_hash) {
            return Ok(bc);
        }

        // Layer 2: DB. An unknown code hash at this point indicates a
        // real bug (e.g. tx references a code hash we've never observed);
        // surface as an error so the parallel phase aborts and the caller
        // falls back to serial for this block.
        let sp = self.spawner.spawn_state()?;
        // reth wraps revm's `Bytecode` in its own newtype for codec
        // reasons; unwrap via the public `.0` field.
        sp.bytecode_by_hash(&code_hash)?
            .map(|b| b.0)
            .ok_or(ProviderError::StateForHashNotFound(code_hash))
    }

    fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error> {
        // Layer 1: bundle overlay. Three cases:
        //   1. Bundle has this slot → return its present_value.
        //   2. Bundle knows the account's storage is fully rebuilt
        //      (destroyed-then-reused): missing slot = 0.
        //   3. Otherwise fall through — the pre-existing on-disk value
        //      still applies.
        if let Some(bundle_acct) = self.bundle.account(&address) {
            if let Some(slot_entry) = bundle_acct.storage.get(&slot) {
                return Ok(slot_entry.present_value);
            }
            if bundle_acct.status.is_storage_known() {
                return Ok(U256::ZERO);
            }
        }

        // Layer 2: DB.
        let sp = self.spawner.spawn_state()?;
        Ok(sp.storage(address, B256::from(slot))?.unwrap_or(U256::ZERO))
    }

    fn block_hash(&self, number: u64) -> Result<B256, Self::Error> {
        // Block hashes are not layered by bundle state — always go to DB.
        // EVM semantics: unknown blocks read as zero.
        let sp = self.spawner.spawn_state()?;
        Ok(sp.block_hash(number)?.unwrap_or(B256::ZERO))
    }
}


/// VmBuilder over BSC's revm variant.
///
/// Owns a chain spec + the default `BscEvmFactory`. Per-tx the `transact`
/// method builds a fresh `CfgEnv` (carrying the `BscHardfork`),
/// instantiates a `BscEvm`, runs the tx, and maps revm's errors onto our
/// `TransactOutcome`.
///
/// Generic over the chain-spec type so the builder can be instantiated
/// from inside `BscBlockExecutor` (whose `Spec` is a generic parameter).
/// The only chain-spec method actually used at runtime is
/// `EthChainSpec::chain().id()` for the CfgEnv chain_id. Default type
/// parameter `Arc<BscChainSpec>` preserves the original ergonomics for
/// callers that already hold one.
#[derive(Debug, Clone)]
pub struct BscVmBuilder<CS = Arc<BscChainSpec>> {
    chain_spec: CS,
    factory: BscEvmFactory,
}

impl<CS> BscVmBuilder<CS> {
    pub fn new(chain_spec: CS) -> Self {
        Self {
            chain_spec,
            factory: BscEvmFactory::default(),
        }
    }

    pub fn chain_spec(&self) -> &CS {
        &self.chain_spec
    }
}

impl<CS> VmBuilder for BscVmBuilder<CS>
where
    CS: EthChainSpec + Clone + std::fmt::Debug + Send + Sync + 'static,
{
    type Tx = BscTxEnv;
    type HaltReason = HaltReason;
    type Spec = BscHardfork;

    fn transact<S: Storage>(
        &self,
        db: DbWrapper<'_, S>,
        block_env: &BlockEnv,
        spec: &BscHardfork,
        tx: &BscTxEnv,
    ) -> TransactOutcome<HaltReason> {
        // Build the cfg env carrying this block's spec and chain id.
        // NOTE: the Spec type held by CfgEnv is BSC's `BscHardfork`, not
        // revm's `SpecId` — this is how BSC surfaces its extra forks
        // (Luban, Feynman, etc.) to the handler.
        let cfg_env = CfgEnv::new_with_spec(spec.clone())
            .with_chain_id(self.chain_spec.chain().id());

        let evm_env = EvmEnv {
            cfg_env,
            block_env: block_env.clone(),
        };

        // Construct the evm wrapping our DbWrapper. BscEvmFactory handles
        // the fiddly inspector/trace flags; we always request non-trace
        // here (parallel execution is not instrumented).
        //
        // Read logging happens automatically through the DbWrapper's
        // shared `ReadLogHandle` — we don't need to extract the DB from
        // the EVM after transact returns.
        let mut evm = self.factory.create_evm(db, evm_env);

        match evm.transact(tx.clone()) {
            Ok(result_and_state) => TransactOutcome::Completed { result_and_state },
            Err(EVMError::Database(DbError::Blocked { blocking_tx_idx })) => {
                TransactOutcome::Blocked { blocking_tx_idx }
            }
            Err(EVMError::Database(DbError::Storage(msg))) => {
                TransactOutcome::StorageError(msg)
            }
            Err(EVMError::Database(DbError::KindMismatch(msg))) => {
                TransactOutcome::StorageError(format!("db kind mismatch: {msg}"))
            }
            Err(EVMError::Transaction(tx_err)) => {
                TransactOutcome::ValidationError(format!("{tx_err:?}"))
            }
            Err(EVMError::Header(err)) => {
                TransactOutcome::ValidationError(format!("header: {err:?}"))
            }
            Err(EVMError::Custom(msg)) => TransactOutcome::ValidationError(msg),
        }
    }
}

