# parallel-evm

Chain-agnostic parallel EVM block executor for revm-based runtimes,
implementing Block-STM (Gelashvili et al., 2022).

## What this is

A portable library that takes a block's transactions, a state source, and
a caller-supplied EVM variant, and executes the transactions in parallel
across worker threads — producing per-tx results that are semantically
identical to sequential execution.

## What this is not

- **Not BSC-specific.** No BSC, reth, or any chain-specific types in the
  public API. Callers implement two trait objects (`Storage`, `VmBuilder`)
  and everything else is `revm`'s own types (`BlockEnv`, `AccountInfo`,
  `ResultAndState`, …).
- **Not a block builder.** This crate runs transactions; it doesn't
  assemble receipts, charge validators, write snapshots, or any of the
  chain-specific hooks around the tx loop. Those remain the caller's
  responsibility before and after `ParallelExecutor::execute`.
- **Not a replacement for the sequential path.** Blocks below a
  configurable threshold, or blocks that hit the re-execution budget,
  return via a clean error signal so the caller can fall back to its
  sequential executor unchanged.

## Design goals (recap)

| Goal | Concrete consequence |
|---|---|
| Portable | No BSC/reth in public API. Surface types are revm's own. |
| Composable with `revmc` JIT | Caller's `VmBuilder::transact` picks any revm variant, including a revmc-compiled one, at the point of EVM construction. |
| Correct | 7 differential tests: parallel = sequential across workloads (disjoint, conflicting, RMW chains) and across worker counts (1, 2, 4, 8, 16). |
| Clean fallback | Any fatal condition (storage failure, re-execution budget exhausted) returns `Err(parallel_evm::Error)` — partial state is discarded; caller's sequential path is authoritative. |

## API surface

```rust
use parallel_evm::{Config, ParallelExecutor};

let executor = ParallelExecutor::new(
    Config { workers: 8, max_reexecutions_per_tx: 16, min_txs_for_parallel: 4 },
    my_vm_builder, // impl parallel_evm::VmBuilder
);

let output = executor.execute(
    &my_storage,  // impl parallel_evm::Storage
    block_env,    // revm's BlockEnv
    spec,         // VB::Spec — SpecId for mainnet, BscHardfork for BSC, etc.
    txs,          // Vec<VB::Tx>
)?;

for (i, r) in output.results.iter().enumerate() {
    match r {
        Ok(res) => { /* apply res.state to caller DB; build receipt from res.result */ }
        Err(msg) => { /* tx-level invalid (nonce, balance, etc.); skip */ }
    }
}
```

## Module map

```
src/
  lib.rs          // public API, re-exports
  error.rs        // Error / Result
  executor.rs     // ParallelExecutor, Config, Output, Stats
  worker.rs       // per-thread task-handling loop + write commit + validation
  scheduler.rs    // Block-STM §4 scheduler (execution_idx, validation_idx, decrease_cnt)
  status.rs       // per-tx phase vector + dependency tracking
  mv_memory.rs    // multi-version concurrent memory
  read_set.rs     // per-tx read-log
  db_wrapper.rs   // revm::Database impl reading through MV memory; shared ReadLogHandle
  storage.rs      // Storage trait the caller implements
  vm.rs           // VmBuilder trait the caller implements + TransactOutcome
tests/
  correctness.rs  // differential: parallel == sequential under multiple workloads
```

## Key invariants worth knowing

1. **Read log extraction via `Arc<Mutex>` handle.** Chain-specific EVM
   variants (BSC, OP) typically don't expose an `into_inner()` to recover
   the `Database` after `transact`. Rather than require it, we share an
   `Arc<Mutex<Vec<ReadLog>>>` between the `DbWrapper` and the worker.
   Negligible contention (single-thread per tx), but makes the integration
   trivially possible against any revm variant.

2. **`abort_in_flight_execution` for mid-execution Blocked reads.** When
   a worker's read hits an `Estimate` marker, the tx transitions
   `Executing(inc) → Aborting(inc)` and is added to the blocker's
   dependent list. When the blocker finishes, it lowers `execution_idx`
   so the scheduler re-claims the dependent; `begin_execution` then
   transitions `Aborting(inc) → Executing(inc + 1)`. This preserves the
   incarnation counter which keeps MvMemory write-cleanup correct.

3. **`finish_execution` unconditionally pushes `validation_idx` down.**
   The Block-STM paper pushes only on new writes; we push every time so
   that even a `fetch_add(validation_idx)` that raced past a still-
   executing tx gets reclaimed for validation. Modest cost, prevents a
   livelock we saw in early iteration.

## Correctness tests

`tests/correctness.rs` runs each scenario through both sequential and
parallel paths using the same `FakeVm` (declarative read/write specs),
then asserts `state_to_canonical(seq) == state_to_canonical(par)` after
merging tx state deltas in order. Scenarios:

- Empty block
- Single-tx (exercises sequential fallback below threshold)
- 16 disjoint txs (ideal parallelism)
- Chained conflicts A→B→C→D (moderate serialisation)
- 32 txs different storage slots of same address (high per-address contention)
- 8-tx RMW chain (each reads prior, writes next)
- Concurrency sweep: 1, 2, 4, 8, 16 workers all must match sequential

## Composing with revmc

The `VmBuilder::transact` method is a pure EVM-construction hook.
Concretely, a `VmBuilder` that uses `revmc`'s JIT-compiled bytecode
instead of the interpreter is straightforward: construct a revm `Evm`
with the JIT-provided instruction set, hand the `DbWrapper` to it
unchanged. Reth already wires revmc this way via its `ConfigureEvm`
trait; in our boundary, the revmc piece is an implementation detail
inside `VmBuilder`.

Stacked speedup (block-STM × revmc): historically 2-3× × 2-3× = 4-9×
over sequential interpreter on compute-heavy workloads.
