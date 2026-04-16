# reth-bsc-migrate-blocks

Migrate a reth v0.0.8 / bnb-chain-fork BSC datadir into a paradigm reth
v2.0.0 datadir **without re-downloading 90M+ blocks** from the network.

Moves the append-only tier of chain data (block headers, transactions, and
the MDBX companion index tables they need) and leaves everything else —
state, receipts, history indices, trie — for reth to rebuild via the normal
staged-sync `Execution` pipeline on top of the migrated data.

**Typical outcome on BSC mainnet (~93M blocks, 2.85 TB of transactions):**
download phase cuts from ~1–3 days to ~30–60 min. Execution still dominates
at ~50–80 h.

## Safety model

Two preflight gates run **before any destination write**, and again on the
destination after the copy:

1. **Decode probe.** Sample ~2000 transactions from the source static_files
   via v2.0.0's `StaticFileProvider<BscPrimitives>::transaction_by_id`. Any
   decode failure — including silent field-order drift between the v0.0.8
   `TransactionSigned` Compact layout and paradigm's alloy
   `EthereumTxEnvelope<TxEip4844>` — aborts the run.

2. **Root round-trip probe.** Sample ~200 blocks. For each, load header
   from source static_files, BlockBodyIndices from source MDBX, pull the
   `tx_count` transactions by id, recompute `transactions_root` via
   `alloy_consensus::proofs::calculate_transaction_root`, compare against
   the value stored in the header. Catches semantic inconsistency even when
   the bytes happen to parse.

If both gates pass on both sides, the destination is trustworthy enough
that Execution replaying the chain against it will either succeed or (if
there is still corruption we missed) fail loud at the first bad block via
a `receipts_root` mismatch — not silently.

## Alignment with reth's Bodies stage

We mirror the write path of `DatabaseProvider::append_block_bodies` (see
`reth/crates/storage/provider/src/providers/database/provider.rs:3500`):

| What the Bodies stage writes           | Our migration                                                         |
|----------------------------------------|------------------------------------------------------------------------|
| Transactions static files              | Raw-copy segment triplets (`.seg` + `.off` + `.conf`) — byte-equivalent to calling `StaticFileProviderRW::append_transaction` for every tx, since NippyJar format is version 1 on both sides |
| `BlockBodyIndices` (MDBX)              | Cursor-walk copy from source                                           |
| `TransactionBlocks` (MDBX)             | Cursor-walk copy from source                                           |
| `BlockOmmers` / `BlockWithdrawals`     | Cursor-walk copy from source (both sparse on BSC — ommers is always empty under Parlia) |
| `TransactionHashNumbers`               | **Skipped on purpose** — TransactionLookup stage rebuilds it           |
| `TransactionSenders`                   | **Skipped on purpose** — SenderRecovery stage re-derives it            |

Headers follow the same principle — copy the `static_files/headers_*`
segments verbatim since Header Compact encoding has been stable across
every reth version on both sides.

## Why copy files instead of re-writing them

Same codec writes produced the source data as would produce the
destination — so raw-copying the three-file segment triplet is identical
on-disk to calling `append_transaction` 90M+ times, just faster by orders
of magnitude. We use:

1. `copy_file_range(2)` for each 128 MB offset-aligned chunk, dispatched in
   parallel via rayon. In-kernel transfer, no userspace bounce.
2. `splice(2)` through a 16 MiB pipe as a cross-filesystem fallback — the
   kernel returns `EXDEV` from `copy_file_range` for some fs-to-fs combos
   (e.g. XFS-on-LVM to XFS-on-different-mount) even when both are XFS.
3. `fallocate(2)` to pre-reserve destination extents so concurrent
   cross-offset writes don't race to extend the file.

## Phases

### `--phase static-headers`

Copy `static_files/static_file_headers_*` segment triplets to the dest and
stamp `StageCheckpoints[Headers]` to the highest covered block. Fast
(minutes on any NVMe) — headers total is modest even on a full BSC archive
(~65 GB across 186 segments on mainnet).

Safe to run on its own: header Compact encoding has been stable since
before BSC even existed.

### `--phase static-bodies`

Full bodies-layer migration with both preflight gates:

1. Preflight decode probe (~2000 samples) — aborts if any tx row fails to
   decode under v2.0.0's codec.
2. Preflight root round-trip probe (~200 blocks) — aborts on any
   `transactions_root` mismatch.
3. Copy `static_files/static_file_transactions_*` segments.
4. Cursor-copy `BlockBodyIndices`, `TransactionBlocks`, `BlockOmmers`,
   `BlockWithdrawals` from source MDBX → dest MDBX.
5. Stamp `StageCheckpoints[Bodies]` to the source-MDBX tip (which is the
   canonical bound on "bodies are complete through block N" for the target).
6. Post-copy gate: rerun both preflight probes **against the destination**.
   Catches copy-time corruption before the user invests 50+ hours in
   Execution.

Assumes headers are already in place (run `static-headers` first).

### `--phase mdbx-headers-only` (legacy)

Block-by-block MDBX-table walker from the initial prototype, retained for
datadirs that pre-date static files or have headers still in MDBX. Not the
common path on BSC but useful as a fallback.

## Usage

Source and destination paths can point at either the reth datadir root or
the `db/` subdir — the tool auto-resolves.

### Full headers → bodies flow (typical)

```bash
# 1. Set up destination
sudo mkdir -p /mnt/fast-ssd/bsc-v2
sudo chown -R reth:reth /mnt/fast-ssd/bsc-v2

# 2. Headers first (safe, fast)
sudo -u reth ./target/release/reth-bsc-migrate-blocks \
    --from /mnt/fast-ssd/legacy-bsc/datadir \
    --to   /mnt/fast-ssd/bsc-v2 \
    --phase static-headers

# 3. Bodies — runs preflight before writing
sudo -u reth ./target/release/reth-bsc-migrate-blocks \
    --from /mnt/fast-ssd/legacy-bsc/datadir \
    --to   /mnt/fast-ssd/bsc-v2 \
    --phase static-bodies

# 4. Start the node — picks up SenderRecovery + Execution + trie stages from
#    the migrated data, no download needed for headers or bodies up to the
#    migrated tip.
sudo -u reth ./target/release/reth-bsc node \
    --chain bsc \
    --datadir /mnt/fast-ssd/bsc-v2 \
    --engine.parallel-sparse-trie \
    --http --http.addr 127.0.0.1
```

### Dry-run first (recommended)

```bash
sudo -u reth ./target/release/reth-bsc-migrate-blocks \
    --from /mnt/fast-ssd/legacy-bsc/datadir \
    --to   /mnt/fast-ssd/bsc-v2 \
    --phase static-bodies \
    --dry-run
```

Runs both preflight gates against the source. Writes nothing. Exits loud
on any failure — which tells you up front whether the codec bridge works
for your specific v1 data before you commit to a multi-TB copy.

### Flags

| Flag                  | Default          | Notes                                                                |
|-----------------------|------------------|----------------------------------------------------------------------|
| `--from`              | required         | v1 datadir root or `db/` subdir                                      |
| `--to`                | required         | v2 datadir root (dir will be created)                                |
| `--phase`             | `static-headers` | One of `static-headers`, `static-bodies`, `mdbx-headers-only`        |
| `--workers`           | `min(2×cores,64)`| Rayon pool size. PCIe 5 NVMes want 64+.                              |
| `--reorder-window`    | 4096             | Legacy MDBX-pipeline tuning, unused in static phases                 |
| `--from-block`        | 0                | Lower bound (legacy MDBX pipeline only)                              |
| `--to-block`          | source tip       | Upper bound (legacy MDBX pipeline only)                              |
| `--dry-run`           | false            | Preflight-only. Writes nothing.                                      |

## Limitations

### What gets migrated

- Headers (segments + checkpoint)
- Bodies: Transactions, BlockBodyIndices, TransactionBlocks, BlockOmmers, BlockWithdrawals + checkpoint

### What does NOT get migrated

- **Receipts** — regenerated by Execution stage
- **State** (PlainAccountState, PlainStorageState, Bytecodes) — regenerated by Execution
- **Changesets + history indices** (AccountChangeSets, StorageChangeSets, AccountsHistory, StoragesHistory) — regenerated from Execution output
- **Trie** (HashedAccounts, HashedStorages, AccountsTrie, StoragesTrie) — regenerated by HashState + Merkle stages
- **TransactionHashNumbers** — regenerated by TransactionLookup stage (fast, ~1 h on BSC)
- **TransactionSenders** — regenerated by SenderRecovery stage (ECDSA-bound, ~1–2 h)

### Known caveats

- **Not resumable.** A crashed migration must be restarted from scratch
  against a clean dest. `init_db` on a partial dest is idempotent-ish for
  the MDBX side, but the stage checkpoints would be half-stamped — just
  delete the dest and retry.
- **Source node must be offline.** MDBX RO mode still requires write access
  to `mdbx.lck`; running a v1 node concurrently can cause lock contention.
- **Cross-filesystem performance varies.** `copy_file_range` won't use
  reflinks cross-mount; falls back to `splice(2)` automatically.
- **`--phase mdbx-headers-only` uses block-by-block decode/reencode** which
  is ~100× slower than the static-copy phases. Only use if the source has
  headers in MDBX rather than static files.

## Architecture

```
tools/migrate-blocks/src/
├── main.rs          # CLI, phase dispatch
├── pipeline.rs      # Orchestration per phase
├── static_copy.rs   # File-level copy: discover segments, parallel copy_file_range / splice
├── bodies.rs        # Defensive bodies migration: preflight, MDBX copy, stamp, post-verify
├── reader.rs        # Legacy: MDBX-side block-by-block reader + reorder pipeline
└── writer.rs        # Legacy: MDBX-side writer + stage checkpoint stamping
```

The `static_copy` path is the fast one and handles both headers and the
bulk of bodies. `bodies.rs` layers preflight + MDBX-companion-table copy
on top of `static_copy` for the `static-bodies` phase. The `reader`/`writer`
legacy pipeline is kept for the `mdbx-headers-only` fallback.
