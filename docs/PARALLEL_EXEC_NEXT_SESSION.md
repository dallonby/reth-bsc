# Handoff: parallel-evm integration into BSC executor

## Context for the next agent

You are continuing work on integrating our portable `parallel-evm` Block-STM
crate (lives at `crates/parallel-evm/`, 54 tests green) into `reth-bsc`'s
block execution path behind the `--bsc.parallel-execute` CLI flag. This
work has had multiple prior sessions; read `src/node/evm/parallel.rs` module
doc first — it carries the complete investigation history and the architectural
correction you're implementing.

**You are picking up AFTER a revert.** The prior agent landed
`BscBlockExecutor::execute_block_parallel<DB>` with method-level bounds
`EVM::DB = &'a mut State<DB>, DB: Database + DatabaseRef + Send + Sync`.
The compile-check passed on a generic DB but the bounds are unsatisfiable
at production call sites because `pub trait DbTx: Debug + Send` — MDBX
transactions are intentionally not `Sync`. The method was removed in commit
`01e69bb` with a full rationale.

Do NOT re-attempt the same approach. The correct answer uses reth's
`ConsistentDbView<Factory>` primitive.

## What's already in place (compile-clean, don't break)

- `--bsc.parallel-execute` CLI flag + env var + global atomic in `src/shared.rs`
- `BscBlockExecutionCtx::parallel` field set by `context_for_block` (never by
  miner/live-engine paths — those always serial)
- `BscVmBuilder<CS>` in `src/node/evm/parallel.rs` — generic over chain-spec,
  implements `parallel_evm::VmBuilder` over `BscEvmFactory`
- `RefStorage<'a, DB>` in `src/node/evm/parallel.rs` — useful PATTERN but
  won't work in production (same Sync issue). Keep the type, use as reference
  when designing `LayeredStorage`.
- `HertzPatchManager::has_patch(&tx_hash)` helper in `src/node/evm/patch/mod.rs`
  for the parallel branch's fallback-to-serial detection
- `BscBlockExecutor::execute_block` runtime-branch scaffold that logs when
  `ctx.parallel` is set and falls through to serial (byte-identical to
  flag-off today). Replace this scaffold with real dispatch.

## The correct architecture

### Parallel state access via ConsistentDbView

Reth's `ConsistentDbView<Factory>` (in `reth-provider::ConsistentDbView`) is
the ONLY Send+Sync-safe concurrent-state-access primitive in reth:

- `Factory: DatabaseProviderFactory + Send + Sync + Clone + 'static`
- `view.provider_ro() -> ProviderResult<Factory::Provider>` — each caller gets
  a **fresh** read transaction; no cross-thread tx sharing

Parallel-evm's `Storage` trait is shared (`&S`). Each worker will need to
call `view.provider_ro()` internally to get its own tx.

### Layered storage for batch correctness

reth's pipeline `ExecutionStage` batches blocks — `State<DB>.bundle_state`
accumulates transitions across multiple blocks before the end-of-batch
commit. An external StateProvider at `latest()` would miss every in-batch
committed block's changes.

The solution: `LayeredStorage<'a, F>` layers:

1. **Immutable snapshot of `bundle_state`** captured at `execute_block_parallel`
   entry (reflects in-batch commits up to current block's parent)
2. **Per-worker ConsistentDbView provider** for committed state (reflects
   state as of the batch-start snapshot)

Reads check layer 1 first (bundle overlay), then layer 2 (per-worker provider).

### Correct signature

```rust
pub fn execute_block_parallel<F>(
    mut self,
    view: &ConsistentDbView<F>,
    transactions: impl IntoIterator<Item = impl ExecutableTx<Self>>,
) -> Result<BlockExecutionResult<R::Receipt>, BlockExecutionError>
where
    F: reth_provider::DatabaseProviderFactory + Send + Sync + Clone + 'static,
    // existing BscBlockExecutor where-clause bounds
```

Note: NO `DB: Sync` bound. The `view` parameter is Send+Sync; individual
worker txs are Send-only (created per-worker).

## The prerequisite problem: accessing bundle_state

`BscBlockExecutor::execute_block_parallel` needs to capture `self.evm.db()
.bundle_state` immutably at entry. `self.evm.db()` returns `&E::DB` where
`E::DB` is `alloy_evm::block::StateDB`-bounded — doesn't expose
`bundle_state`.

Three options (pick one):

**(a) Pass bundle snapshot as argument.** The caller (custom ExecutionStage
or payload validator) already holds `&mut State<DB>` at the pipeline level.
They can capture a reference to `bundle_state` BEFORE calling
`execute_block_parallel` and pass it in. Cleanest — `BscBlockExecutor` stays
generic.

```rust
pub fn execute_block_parallel<F>(
    mut self,
    view: &ConsistentDbView<F>,
    bundle_snapshot: &BundleState,
    transactions: impl IntoIterator<Item = impl ExecutableTx<Self>>,
) -> ...
```

**(b) BscEvmConfig::create_parallel_executor inherent method.** New method
with concrete DB bounds that constructs BscBlockExecutor AND captures a
closure over the `State<DB>` it was given. Closure returns the bundle
snapshot. Stored as an `Option<Box<dyn ...>>` field on BscBlockExecutor.
More machinery, but `BscBlockExecutor::execute_block_parallel` doesn't need
the snapshot passed in.

**(c) Fork BscBlockExecutor into a concrete-DB wrapper.** New type
`BscParallelBlockExecutor<'a, DB, Spec, R>` that holds `&mut State<DB>`
concretely. Most invasive — lots of code duplication unless shared logic
is factored out.

**Recommendation: (a).** Minimal, clean, defers the abstraction choice
until we know what works.

## Step-by-step implementation plan

### Phase 1: Core dispatch (this session's goal)

1. In `src/node/evm/parallel.rs`, design `LayeredStorage<'a, F>`:
   - Holds `bundle_snapshot: &'a BundleState` + `view: &'a ConsistentDbView<F>`
   - Impls `parallel_evm::Storage`:
     - `basic(addr)`: check bundle first, else `view.provider_ro()?`-via-
       `StateProviderDatabase::basic_ref(addr)`
     - Similar delegation for `storage`, `code_by_hash`, `block_hash`
   - Watch out: spawning a fresh provider on every read is expensive. Consider
     thread-local caching of the provider handle (each worker thread gets
     one). `thread_local!` or pass a per-worker provider via another layer.

2. In `src/node/evm/executor.rs`, add `BscBlockExecutor::execute_block_parallel`
   as an inherent method with signature from option (a) above. Pipeline:
   - `apply_pre_execution_changes()` — unchanged
   - Materialize txs into user/system via `is_system_transaction`
   - `HertzPatchManager::has_patch` check — fall back to serial if any match
   - Build `LayeredStorage { bundle_snapshot, view }`
   - `ParallelExecutor::execute(&layered, block_env, spec, user_tx_envs)`
   - Serial commit via `self.commit_transaction(BscTxResult{..})` per result
     (existing method — preserves on_state hook, gas, receipts, DatabaseCommit)
   - System tx tail — existing serial loop
   - `apply_post_execution_changes()` — unchanged

3. Run `cargo check -p reth_bsc` to confirm clean. Run `cargo test -p
   parallel-evm --release` to confirm the crate stays green.

### Phase 2: Staged-sync activation

4. In `src/main.rs` / `src/node/mod.rs`, customise the pipeline stages
   (reth exposes via `StageSet` / ComponentsBuilder). Define
   `BscParallelExecutionStage<E>` mirroring reth's `ExecutionStage<E>` but:
   - Holds a `ConsistentDbView<Factory>` built from the node's provider
   - When `ctx.parallel` is set on a block, captures
     `&self.executor.db.bundle_state` and calls
     `executor.execute_block_parallel(&view, bundle_snapshot, txs)`
   - Otherwise falls back to reth's default `execute_one`

   Register this stage in place of reth's default. Reth's `ExecutionStages`
   set can be replaced; look at how `BscNode::components_builder` is
   structured in `src/node/mod.rs`.

5. E2E test: run a short staged-sync with `--bsc.parallel-execute` against
   a small block range. Verify state root matches serial baseline (Block-STM
   is deterministic; any divergence = bug).

### Phase 3: Live-sync activation (deferred)

Live sync goes through `reth-engine-tree::payload_validator::execute_block`
which iterates txs with streaming receipts. Options:

- Accept loss of receipt streaming for parallel-enabled blocks (simplest)
- Provide a custom PayloadValidator impl (larger lift)

Defer until Phase 2 has perf numbers justifying the Phase 3 complexity.

## Non-negotiables

- **Determinism.** Block-STM is deterministic (Aptos paper §4). Any
  state-root divergence from serial is a bug; don't hide it.
- **Hertz patch fallback.** Blocks with patched tx hashes MUST fall back
  to serial — parallel reads see pre-patch state and would diverge.
- **System tx ordering.** System txs stay serial in the tail; don't
  parallelise validator rewards.
- **No `DB: Sync` bounds.** If you find yourself adding `Sync` to a
  database type, stop — you're repeating the prior session's mistake.
  Use `ConsistentDbView<Factory>` instead.
- **Tests stay green.** `cargo test -p parallel-evm --release` (54 tests),
  `cargo test -p reth_bsc --lib` (192 tests — there's one test-ordering
  flake `node::consensus::tests::finalized_with_larger_validator_set`
  that's pre-existing, passes in isolation, ignore it).

## Key files and line pointers

- `src/node/evm/parallel.rs` — module doc has full history + design for
  the redesign. Add `LayeredStorage` here.
- `src/node/evm/executor.rs` — `BscBlockExecutor` + its inherent impls and
  BlockExecutor trait impl. Line 1050-ish has the scaffold `execute_block`
  override with runtime branch. Add `execute_block_parallel` as an inherent
  method around line 400 (before the `impl BlockExecutor for` block).
  `BscTxResult` struct is defined at the top for the serial commit path.
- `src/node/evm/config.rs` — `BscEvmConfig::context_for_block` sets
  `ctx.parallel` from the shared atomic. `create_executor` at line 220 is
  the trait method (don't tighten its bounds — that's the wall).
- `src/node/evm/patch/mod.rs` — `HertzPatchManager::has_patch` at the bottom
- `src/shared.rs` — flag atomic + getters
- `crates/parallel-evm/src/` — the portable crate. `Storage` trait in
  `storage.rs`, `VmBuilder` in `vm.rs`, `ParallelExecutor::execute` in
  `executor.rs`. Don't modify these; they're stable.

## Reth version pinning

Reth is pinned to `v2.0.0` via git tag in Cargo.toml workspace deps. The
prior sessions investigated forking reth but the MDBX tx non-Sync wall
means any reth fork would cascade too far. The in-crate approach (no reth
fork) is the right path.

## Commit log context

Recent commits (top = most recent):

```
01e69bb revert(bsc): remove execute_block_parallel — bounds unsatisfiable at prod call sites
b01cba9 docs(bsc): MDBX tx non-Sync is the architectural wall for parallel
468e926 docs(bsc): batch-execution finding supersedes prior parallel roadmap
c55dffc docs(bsc): document trait-bound wall + next-session strategies for parallel
5137de5 docs(bsc): note bound-propagation blocker at top of parallel roadmap
a5a5b22 refactor(bsc): generalise BscVmBuilder over chain-spec type
1aa8c8e feat(bsc): scaffold execute_block override with parallel branch hook
db77bf2 feat(bsc): RefStorage adapter over &DB: DatabaseRef for parallel-evm
62e0e51 feat(bsc): --bsc.parallel-execute CLI flag + ctx plumbing
69bdf86 fix(system-contracts): use STAKE_HUB_ABI_PARSED for removeNodeIDs lookup in test
```

Read `01e69bb`'s commit message carefully — it's the most condensed
statement of what went wrong and why.

## Your first move

Don't start coding immediately. Do this first:

1. Read `src/node/evm/parallel.rs` module doc top-to-bottom.
2. Read commit `01e69bb` message (`git show 01e69bb`).
3. Skim `reth-provider::ConsistentDbView` source
   (`/home/david/Source/reth/crates/storage/provider/src/providers/
   consistent_view.rs`).
4. Confirm your understanding of the MDBX-tx-non-Sync constraint BEFORE
   writing any `where DB: ...` bounds.

Then design `LayeredStorage` on paper and confirm it with the user before
implementing. The user's memory profile says autonomous mode but this
architecture has had enough wrong turns that a brief design confirmation
is warranted.

Good luck. The portable crate is solid; the BSC wiring is the last mile.
