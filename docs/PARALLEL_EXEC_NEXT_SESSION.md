# Handoff: parallel-evm integration into BSC executor — Phase 3

## Status (Phase 2 complete, 2026-04-17)

Two prior phases shipped; this doc is the Phase 3 entry point.

- **Phase 1** (commit `90e2525`): `LayeredStorage` + inherent method
  `BscBlockExecutor::execute_block_parallel` that runs the tx phase
  through `parallel-evm::ParallelExecutor` and falls back to serial
  on three triggers (Hertz patches, parallel executor error, any
  per-tx `Err` in output).
- **Phase 2** (commit `0f12c2f`): wired the parallel method into
  reth's staged-sync pipeline by overriding
  `ConfigureEvm::batch_executor` to return a custom `BscBatchExecutor`.
  Pre-exec sequencing corrected so parallel workers see pre-exec
  writes via the bundle overlay. Global `PARALLEL_STATE_SPAWNER` set
  during component init from `ctx.provider().clone()`.

Branch: `main` on `reth-bsc-upstream`. 192/192 lib tests + 54/54
parallel-evm tests green, release build clean.

## Architectural summary (for the next agent)

Data flow when `--bsc.parallel-execute` is on:

```
reth Pipeline
  └─ ExecutionStage<BscEvmConfig>
      └─ evm_config.batch_executor(db)
          └─ BscBatchExecutor<DB>              ← src/node/evm/batch.rs
              └─ execute_one(&block)
                  ├─ [flag off] executor_for_block(...).execute_block(txs)  [serial, BasicBlockExecutor-equivalent]
                  └─ [flag on]  try_execute_one_parallel(block, &spawner)
                      ├─ create_executor(evm, ctx)   → BscBlockExecutor    ← src/node/evm/executor.rs
                      ├─ executor.apply_pre_execution_changes()
                      ├─ state.merge_transitions(Reverts)                   ← CRITICAL: folds pre-exec into bundle
                      ├─ snapshot = state.bundle_state.clone()
                      └─ executor.execute_block_parallel(&spawner, &snapshot, txs)
                          ├─ LayeredStorage { bundle: &snapshot, spawner: &spawner }  ← src/node/evm/parallel.rs
                          ├─ parallel_evm::ParallelExecutor::execute(&layered, ..., user_tx_envs)
                          ├─ [per-tx commit] self.commit_transaction(BscTxResult { .. })
                          ├─ [system-tx buffer] self.system_txs.push(..)
                          └─ self.apply_post_execution_changes()  [system tx replay + validator rewards]
```

Three serial-fallback triggers inside `execute_block_parallel`:
1. Any user-tx hash matches `HertzPatchManager::has_patch`
2. `parallel_evm::Error` (budget exhausted, storage failure)
3. Any per-tx `Err` in the parallel output

All three re-run the block serially from the tx loop (pre-exec is
already done by the caller).

## Non-negotiables (do not regress)

- **Determinism.** Block-STM is deterministic (Aptos paper §4); parallel
  and serial must produce byte-identical state roots on the happy path.
- **Pre-exec merge timing.** The bundle snapshot MUST include the
  current block's pre-exec writes. Skipping the `merge_transitions`
  call in `BscBatchExecutor::try_execute_one_parallel` will silently
  diverge on system-contract upgrade blocks.
- **No `DB: Sync` bounds.** MDBX transactions are Send-only by design.
  Per-worker state providers are spawned via the
  `ParallelStateSpawner` trait, which the node registers from
  `ctx.provider().clone()`.
- **Hertz patch fallback.** Blocks with patched tx hashes must run
  serial.
- **System tx ordering.** System txs stay serial; validator rewards
  never parallelise.

## Phase 3: E2E validation

**The first real correctness test.** All prior validation is static
(192 lib tests don't exercise the live parallel dispatch path).

Steps:
1. Sync a small BSC mainnet range (e.g. blocks 0..1_000_000) with
   `--bsc.parallel-execute` OFF. Capture the final state root.
2. Reset the DB and re-sync the same range with the flag ON. Compare
   state roots.
3. Any divergence is a bug. Primary suspects in decreasing likelihood:
   - Bundle overlay miss case (incorrect `AccountStatus` branch in
     `LayeredStorage::basic` or `::storage`). See the match on
     `was_destroyed()` / `is_storage_known()` in parallel.rs.
   - Pre-exec write invisible to workers. `merge_transitions` in
     `try_execute_one_parallel` must happen AFTER
     `apply_pre_execution_changes` and BEFORE `bundle_state.clone`.
   - `is_system_transaction` misclassifying a user tx as system (or
     vice versa). The user-tx/system-tx split must match the serial
     path exactly.
4. Capture a perf number for the parallel vs serial ratio on the
   same range. That's the input for Phase 3 optimization decisions.

Recommended commands (adapt to your env):
```bash
# Serial baseline (populate a reference DB)
cargo run --release -p reth_bsc -- node --datadir /tmp/reth-bsc-serial \
    --chain bsc --max-block 1000000

# Parallel run into a fresh DB
cargo run --release -p reth_bsc -- node --datadir /tmp/reth-bsc-parallel \
    --chain bsc --max-block 1000000 --bsc.parallel-execute

# Compare state roots
cargo run --release -p reth_bsc -- db --datadir /tmp/reth-bsc-serial \
    stats --table HashedAccounts | grep state_root
cargo run --release -p reth_bsc -- db --datadir /tmp/reth-bsc-parallel \
    stats --table HashedAccounts | grep state_root
```

Reminder from memory: the user doesn't want `-v`/`-vv` on reth-bsc
node commands. Default verbosity is enough.

## After E2E passes

Three parallel tracks, unblocked:

- **Perf optimization.** Thread-local provider cache in
  `LayeredStorage` so each worker spawns one `StateProviderBox` per
  block instead of per read. Expected 2-10x throughput improvement on
  storage-heavy blocks.
- **Bundle clone optimization.** `Arc<BundleState>` with
  copy-on-write via `Arc::make_mut` after workers join; removes the
  O(N²*K) per-batch allocator work.
- **Live sync activation.** Custom `PayloadValidator` to run parallel
  on live-synced blocks. Needs a different state-access primitive
  (ConsistentDbView becomes appropriate here, since live sync isn't
  in pipeline mode).

Pick based on profile data from Phase 3 validation.

## Source pointers

| File | Role |
|------|------|
| `src/node/evm/parallel.rs` | `ParallelStateSpawner` trait, `LayeredStorage`, `BscVmBuilder`, `RefStorage` (legacy) |
| `src/node/evm/executor.rs` | `BscBlockExecutor` + inherent `execute_block_parallel` |
| `src/node/evm/batch.rs` | `BscBatchExecutor<DB>` — the Executor impl that wires into reth's stage |
| `src/node/evm/config.rs` | `batch_executor` override returning `BscBatchExecutor`; `context_for_block` sets `ctx.parallel` |
| `src/node/evm/patch/mod.rs` | `HertzPatchManager::has_patch` |
| `src/shared.rs` | `PARALLEL_EXECUTE_ENABLED` atomic, `PARALLEL_STATE_SPAWNER` global |
| `src/node/consensus.rs` | Registers spawner in `BscConsensusBuilder::build_consensus` |
| `crates/parallel-evm/` | Portable Block-STM crate, 54 tests green. DO NOT MODIFY. |

## Commits

```
0f12c2f feat(bsc): Phase 2 parallel execution wired into staged sync
90e2525 feat(bsc): Phase 1 parallel execution dispatch via LayeredStorage
01e69bb revert(bsc): remove execute_block_parallel — bounds unsatisfiable   [pre-Phase-1 dead end]
```

## Your first move

1. Read `git show 0f12c2f` for the Phase 2 rationale (especially the
   pre-exec sequencing fix).
2. Read `src/node/evm/batch.rs` top-to-bottom (~250 lines) — it's the
   smallest unit of code that describes the integration.
3. Set up a scratch BSC environment for E2E validation. Small ranges
   first (10k blocks, then 100k) to catch divergence early.

Determinism means any single-block divergence is reproducible and
diagnosable. Good luck.
