//! Migrate append-only chain data (Headers + Bodies + Transactions + Senders +
//! TransactionHashNumbers) from a reth v0.0.8 / bnb-chain-fork BSC datadir into
//! a fresh paradigm reth v2.0.0 datadir.
//!
//! The downstream node then skips directly to Execution from genesis — we set
//! the appropriate `StageCheckpoints` entries (Headers, Bodies, SenderRecovery,
//! TransactionLookup) to the migrated tip, leave Execution unset, and every
//! stage after Execution will backfill from the migrated raw data when the node
//! starts.
//!
//! Safety model (see also: project memory):
//!  * `Header` Compact encoding is stable between v0.0.8 and v2.0.0.
//!  * Body index / ommers / withdrawals tables are stable.
//!  * `TransactionSigned` Compact encoding is NOT stable — v0.0.8 used reth's
//!    custom enum; v2.0.0 uses alloy's `EthereumTxEnvelope<TxEip4844>`. Bridged
//!    via RLP (EIP-2718 envelope, canonical both sides).
//!  * `Receipt` encoding is not migrated here (Receipts are rebuilt by
//!    Execution stage when the node runs).
//!
//! Pipeline shape: many v1 readers (disjoint block ranges) → reorder buffer →
//! single v2 writer. See design notes in project memory.

#![deny(clippy::disallowed_methods)]

use clap::Parser;
use std::path::PathBuf;
use tracing_subscriber::{fmt, prelude::*, EnvFilter};

mod bodies;
mod pipeline;
mod reader;
mod static_copy;
mod writer;

/// CLI surface for the migrator.
#[derive(Parser, Debug)]
#[command(
    author,
    version,
    about = "Migrate BSC reth v0.0.8 → v2.0.0 without re-downloading Headers + Bodies",
    long_about = None,
)]
struct Cli {
    /// Source: existing reth v0.0.8 / bnb-chain BSC datadir (read-only).
    #[arg(long, value_name = "PATH")]
    from: PathBuf,

    /// Destination: fresh paradigm reth v2.0.0 datadir (must not contain prior
    /// sync state; we init it here).
    #[arg(long, value_name = "PATH")]
    to: PathBuf,

    /// Number of parallel reader threads. Default = num_cpus, cap 16.
    #[arg(long, default_value = "0")]
    workers: usize,

    /// Start block (inclusive). Default 0.
    #[arg(long, default_value = "0")]
    from_block: u64,

    /// End block (inclusive). Default: auto-detect from source tip.
    #[arg(long)]
    to_block: Option<u64>,

    /// Size of the in-flight reorder window (blocks). Larger uses more memory
    /// but lets readers run further ahead of the writer.
    #[arg(long, default_value = "4096")]
    reorder_window: usize,

    /// Dry-run: open both DBs, report discovered ranges, exit without writing.
    #[arg(long)]
    dry_run: bool,

    /// Phase to run. Default: `static-headers` — copy static_files headers
    /// segments and stamp `StageCheckpoints[Headers]`. `static-all` extends
    /// to transactions + receipts segments. `mdbx-headers-only` is the
    /// legacy per-block MDBX iteration (rarely useful).
    #[arg(long, default_value = "static-headers")]
    phase: Phase,
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Phase {
    /// Copy `static_files/headers_*` segments + set StageCheckpoints[Headers].
    /// Safest tier: NippyJar format is v1 in both reth versions and Header
    /// Compact encoding is stable, so a raw file copy works.
    StaticHeaders,
    /// Defensive bodies migration: runs a two-gate preflight (decode probe
    /// + transactions_root round-trip) against the source, aborts if either
    /// gate fails, then copies `static_files/transactions_*` + MDBX body
    /// index tables (BlockBodyIndices / TransactionBlocks / BlockOmmers /
    /// BlockWithdrawals) and stamps StageCheckpoints[Bodies]. Assumes
    /// `static-headers` has already been run against the same destination.
    StaticBodies,
    /// Legacy: iterate MDBX Headers table block-by-block (only useful if the
    /// source datadir is pre-static-files or has unsegmented headers in
    /// MDBX). Not the common path on BSC.
    MdbxHeadersOnly,
}

fn main() -> eyre::Result<()> {
    tracing_subscriber::registry()
        .with(fmt::layer())
        .with(EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")))
        .init();

    let cli = Cli::parse();
    let workers = if cli.workers == 0 {
        std::thread::available_parallelism().map(|n| n.get().min(16)).unwrap_or(4)
    } else {
        cli.workers
    };

    tracing::info!(
        from = %cli.from.display(),
        to = %cli.to.display(),
        workers,
        from_block = cli.from_block,
        to_block = ?cli.to_block,
        reorder_window = cli.reorder_window,
        phase = ?cli.phase,
        dry_run = cli.dry_run,
        "reth-bsc-migrate-blocks starting",
    );

    let ctx = pipeline::Context {
        from: cli.from,
        to: cli.to,
        workers,
        from_block: cli.from_block,
        to_block: cli.to_block,
        reorder_window: cli.reorder_window,
        dry_run: cli.dry_run,
        phase: cli.phase,
    };

    pipeline::run(ctx)
}
