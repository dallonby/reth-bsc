//! # parallel-evm
//!
//! Chain-agnostic parallel EVM executor for revm-based runtimes, implementing
//! the Block-STM algorithm (Gelashvili et al., 2022).
//!
//! ## Design goals
//!
//! - **Portable.** No chain-specific logic in this crate. The caller supplies
//!   transactions, a state source, and a block env; the crate returns per-tx
//!   execution results plus a merged state diff. Pre/post-block hooks —
//!   validator rewards, system txs, snapshot updates, Parlia for BSC,
//!   coinbase lazy-update for Ethereum, etc. — remain the caller's
//!   responsibility.
//!
//! - **Native revm.** Public types mirror revm's own types (`BlockEnv`,
//!   `AccountInfo`, `Bytecode`, `ExecutionResult`). No bespoke adapter
//!   traits beyond what's strictly needed to pump custom EVM variants
//!   through the worker pool.
//!
//! - **Composable with `revmc`.** The caller's EVM variant — including a
//!   JIT-compiled one via `revmc` — plugs in via the `VmBuilder` trait with
//!   no extra wrappers. That makes the stack:
//!
//!   ```text
//!   [ caller's block executor (BSC, Eth, rbuilder) ]
//!   [ parallel-evm (this crate)                    ]
//!   [ revm + (optional) revmc-compiled bytecode    ]
//!   ```
//!
//! ## Status
//!
//! Day 1: multi-version memory + tests. Scheduler, workers, and public
//! `ParallelExecutor` land in subsequent steps. The API below will grow; no
//! stable promises until `0.1.0` ships.

pub mod error;
pub mod mv_memory;
pub mod read_set;
pub mod scheduler;
pub mod status;

pub use error::{Error, Result};
pub use mv_memory::{
    Incarnation, MemoryEntry, MemoryLocation, MemoryValue, MvMemory, ReadOutcome, TxIdx, Version,
};
pub use read_set::{ReadLog, ReadLogBuilder, ReadOrigin, ReadSetStore};
pub use scheduler::{Scheduler, Task};
pub use status::{Phase, StatusVector, TxState};
