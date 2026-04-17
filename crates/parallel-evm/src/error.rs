//! Error types for the parallel executor.
//!
//! Public errors are deliberately chain-agnostic: any chain-specific failure
//! (invalid tx type, parlia breach, etc.) surfaces through the EVM's own
//! `ExecuteEvm` error channel, not through this type.

use thiserror::Error;

/// Errors raised by the parallel block executor during setup or coordination.
/// EVM-level errors (halts, reverts, transaction invalidity) flow through
/// per-tx [`revm::context_interface::result::ExecutionResult`] values, not
/// through this type — that is consistent with how sequential revm works.
#[derive(Debug, Error)]
pub enum Error {
    /// A worker thread panicked. The parallel run is unrecoverable; caller
    /// should fall back to sequential execution (or surface the crash).
    #[error("worker thread panicked")]
    WorkerPanic,

    /// A transaction was re-executed more than the configured budget allows.
    /// Block-STM re-executes transactions whose read-set was invalidated by a
    /// later validation; a runaway here usually means a pathological
    /// tx-dependency chain and the caller should fall back to serial.
    #[error("transaction {tx_idx} hit re-execution budget ({budget})")]
    ReexecutionBudgetExceeded {
        /// Transaction index (position in the block, zero-based).
        tx_idx: usize,
        /// Configured budget that was exceeded.
        budget: u32,
    },

    /// The underlying storage returned an error. The error message is
    /// preserved verbatim to avoid losing root-cause context; we don't wrap
    /// the `Storage::Error` associated type to keep this enum object-safe.
    #[error("storage error: {0}")]
    Storage(String),

    /// Requested `workers = 0`; the executor requires at least one thread.
    #[error("worker count must be non-zero")]
    ZeroWorkers,
}

/// Convenience alias used throughout the crate.
pub type Result<T> = std::result::Result<T, Error>;
