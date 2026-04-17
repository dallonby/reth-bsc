# Handoff: parallel-evm integration into BSC executor — Phase 4

## Status (Phase 3 E2E PASSED, 2026-04-17)

Parallel execution is **correct** against real BSC mainnet data.
Three phases shipped; this doc is the Phase 4 (perf + live-sync)
entry point.

- **Phase 1** (`90e2525`): `LayeredStorage` + `BscBlockExecutor::execute_block_parallel`.
- **Phase 2** (`0f12c2f`): wired into staged sync via `BscBatchExecutor`.
- **Phase 3 fixes & E2E** (`6bbe629`, `8944352`, `b77b3b1`):
  - Speculative-worker exclusion.
  - State<DB> cache pre-warm before parallel commit.
  - Single-revert-per-block fix (keep pre-exec transitions unmerged).
- **E2E**: 499,999 BSC mainnet blocks synced through the parallel
  path. All 13 staged-sync stages completed clean. **`MerkleExecute`
  passed**, meaning the computed state root matches the BSC header's
  stored state_root at every single block. Byte-identical to serial.

Branch: `main` on `reth-bsc-upstream`.

## Architectural summary (current wiring)

Data flow when `--bsc.parallel-execute` is on:

```
reth Pipeline
  └─ ExecutionStage<BscEvmConfig>
      └─ evm_config.batch_executor(db)
          └─ BscBatchExecutor<DB>              ← src/node/evm/batch.rs
              └─ execute_one(&block)
                  ├─ [flag off OR is_speculative_worker]
                  │    └─ executor_for_block(...).execute_block(txs)   [serial]
                  └─ [flag on + spawner registered + not speculative]
                      └─ try_execute_one_parallel(block, &spawner)
                          ├─ create_executor(evm, ctx)   → BscBlockExecutor
                          ├─ executor.apply_pre_execution_changes()
                          ├─ bundle_snapshot = bundle.clone()
                          │  + apply_transitions_and_create_reverts(
                          │        transition_state.clone(),
                          │        BundleRetention::PlainState  ← throwaway reverts
                          │    )
                          │  // pre-exec transitions stay in REAL state.transition_state;
                          │  // one final merge at end of execute_one folds everything.
                          └─ executor.execute_block_parallel(&spawner, &bundle_snapshot, txs)
                              ├─ LayeredStorage { bundle: &snapshot, spawner }
                              ├─ parallel_evm::ParallelExecutor::execute(&layered, ...)
                              ├─ [per user-tx commit]
                              │    ├─ for addr in result.state.keys():
                              │    │     self.evm.db_mut().basic(addr)?;   ← cache pre-warm
                              │    └─ self.commit_transaction(BscTxResult { .. })
                              ├─ [per system-tx] self.system_txs.push(..)
                              └─ self.apply_post_execution_changes()
              // execute_one then: self.state.merge_transitions(Reverts)  ← SINGLE revert entry
```

Three serial-fallback triggers inside `execute_block_parallel`:
1. Any user-tx hash matches `HertzPatchManager::has_patch`
2. `parallel_evm::Error` (budget exhausted, storage failure)
3. Any per-tx `Err` in the parallel output

## Non-negotiables (do not regress)

- **Determinism.** Block-STM is deterministic. Any state-root
  divergence is a bug.
- **Single revert per block.** Don't call `merge_transitions` on the
  real `state.bundle_state` between pre-exec and end-of-block.
  Apply pre-exec transitions to a throwaway CLONE of the bundle
  with `BundleRetention::PlainState`. The real transitions stay in
  `state.transition_state` and get folded by the end-of-block merge
  into one revert entry. Getting this wrong causes
  `MDBX_EKEYMISMATCH` on `StorageChangeSets` at batch flush.
- **State<DB> cache pre-warm.** Before each parallel commit, drive
  `self.evm.db_mut().basic(addr)` for every address in the tx's
  state map. Without this, `State<DB>::commit` panics "All accounts
  should be present inside cache" on the first user tx.
- **Speculative-worker exclusion.** `BscBatchExecutor::execute_one`
  checks `!is_speculative_worker()` before dispatching parallel.
- **No `DB: Sync` bounds.** MDBX txs are Send-only by design.
  Per-worker state providers are spawned via `ParallelStateSpawner`.
- **Hertz patch fallback.** Blocks with patched tx hashes must run
  serial.
- **System tx ordering.** System txs stay serial; don't parallelise
  validator rewards.

## Phase 4: perf optimization + live sync

Three independent tracks, pick in priority order based on where
profiling data points.

### Track A — thread-local provider cache

**Problem.** Every `LayeredStorage` read that misses the bundle
spawns a fresh `StateProviderBox` via `spawner.spawn_state()`. That's
one MDBX tx + `Box<dyn>` allocation **per read**.

**Fix.** Add a `ThreadLocal<RefCell<Option<StateProviderBox>>>` to
`LayeredStorage`, initialized lazily on first miss per worker thread.
parallel-evm uses `std::thread::scope` which spawns fresh OS threads
per `execute()` call, so thread-local state is correctly isolated
per-block.

**Watch for.** The provider holds a non-Sync MDBX read tx; the
thread_local guarantees single-thread access. Don't leak the
provider across `execute_block_parallel` calls (fresh threads per
call handle that naturally, but verify if parallel-evm ever switches
to a thread pool).

**File.** `src/node/evm/parallel.rs::LayeredStorage`.

**Expected gain.** 2–10x throughput on storage-heavy blocks (post-
block 20M on BSC mainnet).

### Track B — Arc<BundleState> copy-on-write

**Problem.** `try_execute_one_parallel` clones the bundle + applies
pre-exec transitions to the clone every block. For an N-block batch
with K cumulative transitions, that's O(N²·K) allocator work per
batch. Tens of MB for reth's default batch sizes.

**Fix.** Replace `bundle.clone()` with `Arc<BundleState>` + COW via
`Arc::make_mut` after parallel workers join. Workers hold
`Arc<BundleState>`; main thread regains exclusive access via
`Arc::make_mut` only if needed.

**Watch for.** `BundleState` has internal maps with undefined
iteration order. Ensure no hash-ordering dependencies leak.

**File.** `src/node/evm/parallel.rs::LayeredStorage`,
`src/node/evm/executor.rs::execute_block_parallel` (signature
change to take `Arc<BundleState>`).

**Expected gain.** Removes the per-block bundle clone. Measurable on
any batch > 1000 blocks.

### Track C — live-sync activation

**Problem.** Parallel only runs in pipeline sync (via the
`BscBatchExecutor` path). Live engine-tree blocks go through
`reth-engine-tree::payload_validator::execute_block`, which bypasses
`batch_executor`.

**Fix.** Custom `PayloadValidator` impl that routes to
`execute_block_parallel`. This is where `ConsistentDbView<Factory>`
IS the right primitive (live sync isn't staged-sync, so the
tip-consistency check is wanted).

**Watch for.** Payload validators stream receipts — parallel-commit
would have to either give up streaming or produce them in-order.
Accept loss of streaming for parallel-enabled blocks as a v1 cut.

**File.** new `src/node/engine_api/parallel_validator.rs` or
similar; register via node builder.

**Expected gain.** Removes the "parallel only during pipeline sync"
limitation — makes the flag useful for validators post-catchup.

## How to run the E2E validation (for reference)

Test datadir is at `/mnt/blockchain/bsc-testing` (owned by `reth`
user, user is in the `reth` group). Setup recipe in project memory.

To re-run execution from a clean slate without re-migrating:
```bash
sudo -u reth ./target/release/reth-bsc stage drop \
    --datadir /mnt/blockchain/bsc-testing --chain bsc execution

sudo -u reth ./target/release/reth-bsc node \
    --datadir /mnt/blockchain/bsc-testing --chain bsc \
    --bsc.parallel-execute --debug.max-block 499999 --debug.terminate
```

Pass/fail signal: does `MerkleExecute` stage complete? Pass = state
root matches BSC header commitment on every block. Fail = specific
block errors out with a state root mismatch (actionable, bisectable).

Do NOT point the node at `/mnt/blockchain/bsc-v2` — that's the
production datadir.

## Source pointers

| File | Role |
|------|------|
| `src/node/evm/parallel.rs` | `ParallelStateSpawner` trait, `LayeredStorage`, `BscVmBuilder`, `RefStorage` (legacy) |
| `src/node/evm/executor.rs` | `BscBlockExecutor` + inherent `execute_block_parallel` (caller drives pre-exec) |
| `src/node/evm/batch.rs` | `BscBatchExecutor<DB>` — `Executor<DB>` impl, the wiring point |
| `src/node/evm/config.rs` | `batch_executor` override; `context_for_block` sets `ctx.parallel` |
| `src/shared.rs` | `PARALLEL_EXECUTE_ENABLED`, `PARALLEL_STATE_SPAWNER` globals |
| `src/node/consensus.rs` | Registers spawner in `BscConsensusBuilder::build_consensus` |
| `crates/parallel-evm/` | Portable Block-STM crate. DO NOT MODIFY. |

## Commits (most recent first)

```
b77b3b1 fix(bsc): single revert entry per block by keeping pre-exec transitions unmerged
33298ee docs(bsc): note the State<DB> cache pre-warm as a non-negotiable
8944352 fix(bsc): pre-load State<DB> cache before parallel commit
6bbe629 fix(bsc): exclude speculative workers from parallel batch path
8b326a0 chore(bsc): Phase 3 handoff doc + batch.rs Arc-clone micro-opt
0f12c2f feat(bsc): Phase 2 parallel execution wired into staged sync
90e2525 feat(bsc): Phase 1 parallel execution dispatch via LayeredStorage
```
