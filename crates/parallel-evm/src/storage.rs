//! The caller-side state interface.
//!
//! `parallel-evm` can't open MDBX, static files, or any specific state
//! backend directly — that would drag in reth or chain-specific deps and
//! break the "portable" principle. Instead, consumers implement this trait
//! over whatever state source they have: reth's `StateProviderDatabase`, a
//! test in-memory map, revm's `CacheDB`, a fork state for rbuilder, etc.
//!
//! # Thread-safety contract
//!
//! `Storage` must be `Send + Sync`. Worker threads read through it
//! concurrently during the parallel phase. Writes NEVER go through this
//! trait — everything goes through `MvMemory` until block finalization, at
//! which point the caller commits the merged state to their own backend on
//! the main thread.
//!
//! # Errors
//!
//! Implementations may fail on I/O. The executor surfaces those to the
//! caller via [`crate::Error::Storage`] — specific error types live on the
//! implementor side.

use alloy_primitives::{Address, B256, U256};
use revm::{bytecode::Bytecode, state::AccountInfo};

/// Read-only state access, implemented by the caller.
pub trait Storage: Send + Sync {
    /// Error type the implementor surfaces. Converted to a String at the
    /// `parallel-evm` boundary to keep [`crate::Error`] object-safe.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Look up account basics (balance, nonce, code_hash). Return `Ok(None)`
    /// for non-existent accounts (revm treats this as "empty account"); use
    /// `Err` only for true failures.
    fn basic(&self, address: Address) -> Result<Option<AccountInfo>, Self::Error>;

    /// Look up contract code by its hash. Return an error if the hash is
    /// expected to resolve but doesn't; EVM will panic downstream
    /// otherwise.
    fn code_by_hash(&self, code_hash: B256) -> Result<Bytecode, Self::Error>;

    /// Look up a storage slot. Missing slots are `Ok(U256::ZERO)` — storage
    /// defaults to zero, it's not an error.
    fn storage(&self, address: Address, slot: U256) -> Result<U256, Self::Error>;

    /// Look up a historical block hash (BLOCKHASH opcode, EIP-2935). The
    /// hash must exist for block numbers within the recent 256 range of the
    /// currently-executing block; older reads may return `Ok(B256::ZERO)`
    /// matching EVM semantics.
    fn block_hash(&self, number: u64) -> Result<B256, Self::Error>;
}
