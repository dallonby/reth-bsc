# Week-away status summary

Written on 2026-04-17, autonomous work from start of the week.

## What shipped while you were away

Seven commits on `main` (top first):

```
36144d8 feat(bsc): BscVmBuilder adapter; refactor read-log to shared handle (day 6)
9faadc6 test(parallel-evm): differential correctness suite + FakeVm harness (day 5)
...     feat(parallel-evm): DbWrapper, VmBuilder, ParallelExecutor (days 3-4)
...     feat(parallel-evm): scheduler + status vector + read-set + storage trait (day 2)
...     feat(parallel-evm): new crate scaffold + MvMemory (Block-STM day 1)
ba48cf2 perf(executor): speculative block prefetcher + skip_bls_verify for pipeline
02ad687 perf(executor): cache JSON ABI parses as LazyLock statics
47cf41f feat(migrate-blocks): full-chain sender migration, read-tx refresh, APPEND fast path
ebf251a fix(snapshot-db): ensure parlia tables exist on the snapshot MDBX env
```

## parallel-evm crate (`crates/parallel-evm/`)

**54 tests green** (46 unit + 7 integration + 1 doctest). Crate compiles
clean against revm 36 — same version reth-bsc uses.

Public surface: `ParallelExecutor` + `Config` + `Storage` trait +
`VmBuilder` trait. No BSC, no reth in the public API. See
`crates/parallel-evm/README.md` for design/usage.

**What's tested end-to-end** (`tests/correctness.rs`):

- Parallel produces identical final state to sequential across six
  workloads: empty, 1-tx, 16 disjoint, chained conflicts, 32 txs on
  same-address different-slot, RMW chain.
- Deterministic across worker counts (1, 2, 4, 8, 16).

**Key design calls made** (all documented in code comments):

1. Read log lives in `Arc<Mutex<Vec<ReadLog>>>` handle shared between
   DbWrapper and the worker. The chain-specific EVM doesn't need to
   expose `into_inner()` — we don't recover the DB, we pull the log from
   our retained handle. See `src/db_wrapper.rs`.

2. `finish_execution` unconditionally pushes `validation_idx` down to
   the tx's idx. Paper only pushes on new-location writes; our push is
   broader (cost: some wasted revalidations) but closes a livelock in
   the `fetch_add-then-skip` scheduler pattern.

3. `abort_in_flight_execution` for mid-execution Blocked reads —
   transitions `Executing(inc) → Aborting(inc)`, parks on blocker, and
   lowers `execution_idx`. Incarnation is preserved across re-entry so
   MvMemory cleanup is correct (an earlier `wake()` that flipped to
   `Ready` leaked incarnations; removed).

## BSC adapter (`src/node/evm/parallel.rs`)

Ships `BscVmBuilder` implementing `parallel_evm::VmBuilder` over
`BscEvmFactory`. Given a `DbWrapper`, it builds a `CfgEnv` with
`BscHardfork`, instantiates `BscEvm` via the factory, runs `transact`,
maps `EVMError` onto `TransactOutcome` variants.

**Compile clean**, **not yet wired into `BscBlockExecutor`**. The
integration sketch is in the module doc comment; see the "Integration
sketch" section. Summary:

1. Inside `BscBlockExecutor::execute_transactions` (or a new sibling
   method), gated by `--bsc.parallel-execute` flag:
   - Split block txs into user-txs (non-system) and system-tx candidates
   - Wrap executor's state DB in a `Storage` adapter
   - `ParallelExecutor::new(cfg, BscVmBuilder::new(spec)).execute(...)`
   - For each result: commit state to executor's DB, build receipt
   - Continue to serial post-execution (system txs, rewards, snapshots,
     genesis-1 contract deploy, Feynman init)
2. On `Err(parallel_evm::Error)`: fall back to serial loop for that
   block. Re-execution budget exhaustion is the main live trigger;
   storage failures should be fatal.

This is a ~1-2 day piece of careful surgery that I deliberately left
for your return — the parallel-evm crate is the portable unlock, and
the BSC wiring benefits from fresh eyes on the Parlia lifecycle hooks
(snapshot provider interactions, validator cache, Hertz patches at
specific blocks, etc.).

**Where to look:**

- `src/node/evm/parallel.rs` — adapter code + integration sketch in doc
  comment
- `src/node/evm/executor.rs` — existing `BscBlockExecutor` for the
  hooks you'll thread around the parallel call
- `src/node/evm/pre_execution.rs`, `src/node/evm/post_execution.rs` —
  the hooks themselves (stay serial)

## Other work that landed earlier in the week

`perf(executor): speculative block prefetcher + skip_bls_verify` — the
~2.5× throughput win you saw (983 Mgas/s → 2.52 Ggas/s). Ported from
bnb-chain/reth-bsc with tweaks: default workers dropped to 16 to match
CCD0 SMT count on 9950X; `skip_bls_verify` flag added for pipeline re-
execution of finalized blocks.

`perf(executor): cache JSON ABI parses as LazyLock statics` — the ~25%
CPU we spotted in profiling. `SystemContract::new` was
`serde_json::from_str`ing four ABI JSON blobs per block; now parsed once
at crate scope.

`feat(migrate-blocks): full-chain sender migration` — `TransactionSenders`
copy (439 GiB) added to the migrator, with MDBX APPEND fast path (~4×
throughput from 1.2M → 5M rows/sec) and read-tx refresh loops to dodge
MDBX's 5-minute long-lived-tx timeout.

`fix(snapshot-db): ensure parlia tables exist` — the bug that was
blowing up genesis snapshot persist on a fresh v2 datadir. One-liner
fix in `create_snapshot_provider`.

## Next steps (in priority order)

1. **Wire ParallelExecutor into BscBlockExecutor.** Sketch is in
   `src/node/evm/parallel.rs` doc comment. Add `--bsc.parallel-execute`
   CLI flag. Fall back to serial on `Err`.
2. **Validate against real BSC blocks.** Run parallel + serial side-by-
   side on blocks 1-1000, compare state roots. Block-STM is
   deterministic; any divergence is a bug.
3. **Tune defaults.** `max_reexecutions_per_tx = 16` and
   `min_txs_for_parallel = 4` are starting points. Real BSC blocks may
   want different values.
4. **Performance measurement.** Once wired: expected 2-3× over the
   current 2.52 Ggas/s single-threaded, so 5-7 Ggas/s on Ryzen 9950X.
   Stacked with revmc JIT (separate integration) that could hit
   10-20 Ggas/s.
5. **Document `RethStorage` adapter.** The sketched wrapper in
   `parallel.rs` is minimal; real integration needs an impl that reads
   from reth's `StateProviderDatabase` or `LatestStateProviderRef`.

## Tests you can run

```bash
cargo test -p parallel-evm            # 54 tests, should all pass
cargo check --release -p reth_bsc     # BSC compiles with adapter
cargo test --release                  # full workspace
```

Enjoy the holiday. 🌞
