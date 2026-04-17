//! End-to-end correctness: parallel execution must produce the same
//! per-tx results and merged state as sequential execution.
//!
//! These tests stand up a toy `VmBuilder` that doesn't use a real revm
//! instruction interpreter; instead it simulates reads and writes through
//! the `DbWrapper` API. This exercises the scheduler, MV memory,
//! validation, re-execution, commit path — everything — without pulling
//! in a chain-specific EVM.
//!
//! The shape is the same as what real BSC / mainnet / OP integrations
//! will look like: a `VmBuilder` that takes a `DbWrapper`, reads, writes,
//! returns a `TransactResult`.

use alloy_primitives::{address, Address, B256, U256};
use parallel_evm::{
    Config, DbError, DbWrapper, ParallelExecutor, Storage, TransactOutcome, TransactResult,
    TxResult, VmBuilder,
};
use revm::{
    bytecode::Bytecode,
    context::{
        result::{ExecutionResult, Output as EvmOutput, ResultAndState, ResultGas, SuccessReason},
        BlockEnv,
    },
    primitives::{hardfork::SpecId, Bytes},
    state::{Account, AccountInfo, AccountStatus, EvmState, EvmStorageSlot},
    Database,
};
use std::{collections::HashMap, convert::Infallible};

// ---- Fake Storage for the tests ----

#[derive(Default, Debug)]
struct Mem {
    basic: HashMap<Address, AccountInfo>,
    storage: HashMap<(Address, U256), U256>,
    code: HashMap<B256, Bytecode>,
}

impl Storage for Mem {
    type Error = Infallible;
    fn basic(&self, a: Address) -> Result<Option<AccountInfo>, Infallible> {
        Ok(self.basic.get(&a).cloned())
    }
    fn code_by_hash(&self, h: B256) -> Result<Bytecode, Infallible> {
        Ok(self.code.get(&h).cloned().unwrap_or_default())
    }
    fn storage(&self, a: Address, s: U256) -> Result<U256, Infallible> {
        Ok(self.storage.get(&(a, s)).copied().unwrap_or(U256::ZERO))
    }
    fn block_hash(&self, _n: u64) -> Result<B256, Infallible> {
        Ok(B256::ZERO)
    }
}

// ---- Fake tx: declarative read/write set ----

#[derive(Clone, Debug)]
struct FakeTx {
    /// Addresses (Basic) and (addr, slot) pairs (Storage) to read.
    reads: Vec<Access>,
    /// Account writes with optional storage slot mutations.
    writes: Vec<Write>,
}

#[derive(Clone, Debug)]
enum Access {
    Basic(Address),
    Storage(Address, U256),
}

#[derive(Clone, Debug)]
struct Write {
    addr: Address,
    new_balance: U256,
    new_nonce: u64,
    storage: Vec<(U256, U256)>, // (slot, new value)
}

// ---- The VmBuilder under test ----

struct FakeVm;

impl VmBuilder for FakeVm {
    type Tx = FakeTx;
    type HaltReason = ();

    fn transact<S: Storage>(
        &self,
        mut db: DbWrapper<'_, S>,
        _block_env: &BlockEnv,
        _spec_id: SpecId,
        tx: &Self::Tx,
    ) -> TransactResult<Self::HaltReason> {
        // Perform all reads first. Any Blocked read short-circuits with
        // the partial read log discarded.
        for access in &tx.reads {
            let read_result: Result<(), DbError> = match access {
                Access::Basic(addr) => db.basic(*addr).map(|_| ()),
                Access::Storage(addr, slot) => db.storage(*addr, *slot).map(|_| ()),
            };
            if let Err(DbError::Blocked { blocking_tx_idx }) = read_result {
                return TransactResult {
                    outcome: TransactOutcome::Blocked { blocking_tx_idx },
                    reads: db.into_read_log(),
                };
            } else if let Err(e) = read_result {
                return TransactResult {
                    outcome: TransactOutcome::StorageError(e.to_string()),
                    reads: db.into_read_log(),
                };
            }
        }

        // Compose the state delta from declared writes.
        let mut state: EvmState = HashMap::default();
        for w in &tx.writes {
            let info = AccountInfo {
                balance: w.new_balance,
                nonce: w.new_nonce,
                code_hash: B256::ZERO,
                account_id: None,
                code: None,
            };
            let mut account = Account {
                info: info.clone(),
                original_info: Box::new(info),
                storage: revm::state::EvmStorage::default(),
                status: AccountStatus::Touched,
                transaction_id: 0,
            };
            for (slot, val) in &w.storage {
                account.storage.insert(
                    *slot,
                    EvmStorageSlot {
                        original_value: U256::ZERO,
                        present_value: *val,
                        transaction_id: 0,
                        is_cold: false,
                    },
                );
            }
            state.insert(w.addr, account);
        }

        let result = ExecutionResult::Success {
            reason: SuccessReason::Return,
            gas: ResultGas::default(),
            logs: vec![],
            output: EvmOutput::Call(Bytes::new()),
        };
        TransactResult {
            outcome: TransactOutcome::Completed {
                result_and_state: ResultAndState { result, state },
            },
            reads: db.into_read_log(),
        }
    }
}

// ---- Helpers ----

fn block_env() -> BlockEnv {
    BlockEnv::default()
}

const A: Address = address!("0x000000000000000000000000000000000000000a");
const B: Address = address!("0x000000000000000000000000000000000000000b");
const C: Address = address!("0x000000000000000000000000000000000000000c");
const D: Address = address!("0x000000000000000000000000000000000000000d");

fn basic_write(addr: Address, balance: u128, nonce: u64) -> Write {
    Write {
        addr,
        new_balance: U256::from(balance),
        new_nonce: nonce,
        storage: vec![],
    }
}

/// Run a block sequentially through the same FakeVm — the "truth" for
/// differential tests.
fn run_sequential(
    storage: &Mem,
    txs: &[FakeTx],
) -> Vec<TxResult<()>> {
    let exec = ParallelExecutor::new(
        Config {
            workers: 1,
            max_reexecutions_per_tx: 128,
            min_txs_for_parallel: usize::MAX, // force sequential path
        },
        FakeVm,
    );
    let out = exec
        .execute(storage, block_env(), SpecId::CANCUN, txs.to_vec())
        .expect("sequential execute");
    assert!(out.stats.ran_sequential);
    out.results
}

fn run_parallel(storage: &Mem, txs: &[FakeTx], workers: usize) -> Vec<TxResult<()>> {
    let exec = ParallelExecutor::new(
        Config {
            workers,
            max_reexecutions_per_tx: 128,
            min_txs_for_parallel: 2,
        },
        FakeVm,
    );
    let out = exec
        .execute(storage, block_env(), SpecId::CANCUN, txs.to_vec())
        .expect("parallel execute");
    out.results
}

// Compare by the fields that matter. EvmState internally uses HashMap so
// iteration is non-deterministic; we compare by address.
fn state_to_canonical(
    results: &[TxResult<()>],
) -> Vec<(Address, U256, u64, Vec<(U256, U256)>)> {
    let mut final_state: HashMap<Address, Account> = HashMap::new();
    for r in results {
        if let Ok(rs) = r {
            for (addr, acct) in rs.state.iter() {
                final_state
                    .entry(*addr)
                    .and_modify(|e| {
                        e.info = acct.info.clone();
                        for (s, v) in acct.storage.iter() {
                            e.storage.insert(*s, v.clone());
                        }
                    })
                    .or_insert_with(|| acct.clone());
            }
        }
    }
    let mut rows: Vec<_> = final_state
        .into_iter()
        .map(|(addr, acct)| {
            let mut slots: Vec<(U256, U256)> = acct
                .storage
                .iter()
                .map(|(s, v)| (*s, v.present_value))
                .collect();
            slots.sort_by_key(|(s, _)| *s);
            (addr, acct.info.balance, acct.info.nonce, slots)
        })
        .collect();
    rows.sort_by_key(|(a, ..)| *a);
    rows
}

// ---------- Tests ----------

#[test]
fn empty_block_produces_empty_output() {
    let storage = Mem::default();
    let out = ParallelExecutor::new(Config::default(), FakeVm)
        .execute(&storage, block_env(), SpecId::CANCUN, Vec::<FakeTx>::new())
        .unwrap();
    assert!(out.results.is_empty());
    assert_eq!(out.stats.num_txs, 0);
}

#[test]
fn single_tx_runs_sequentially_below_threshold() {
    let storage = Mem::default();
    let txs = vec![FakeTx {
        reads: vec![Access::Basic(A)],
        writes: vec![basic_write(A, 100, 1)],
    }];
    let out = ParallelExecutor::new(Config::default(), FakeVm)
        .execute(&storage, block_env(), SpecId::CANCUN, txs)
        .unwrap();
    assert!(out.stats.ran_sequential);
    assert_eq!(out.results.len(), 1);
    assert!(out.results[0].is_ok());
}

#[test]
fn disjoint_txs_parallel_matches_sequential() {
    let storage = Mem::default();
    let txs: Vec<FakeTx> = (0..16u8)
        .map(|i| {
            let mut addr_bytes = [0u8; 20];
            addr_bytes[19] = i;
            let addr = Address::from(addr_bytes);
            FakeTx {
                reads: vec![Access::Basic(addr)],
                writes: vec![basic_write(addr, i as u128 * 10, i as u64)],
            }
        })
        .collect();

    let seq = run_sequential(&storage, &txs);
    let par = run_parallel(&storage, &txs, 8);
    assert_eq!(seq.len(), par.len());
    assert_eq!(state_to_canonical(&seq), state_to_canonical(&par));
}

#[test]
fn conflicting_reads_and_writes_parallel_matches_sequential() {
    // Tx 0 writes A.balance := 100
    // Tx 1 reads A, writes B.balance := (A's balance + 1) — we can't do
    // real arithmetic through FakeVm without smarter plumbing, so just
    // assert sequential == parallel state for the writes that DO happen.
    let storage = Mem::default();
    let txs = vec![
        FakeTx {
            reads: vec![],
            writes: vec![basic_write(A, 100, 0)],
        },
        FakeTx {
            reads: vec![Access::Basic(A)],
            writes: vec![basic_write(B, 200, 0)],
        },
        FakeTx {
            reads: vec![Access::Basic(A), Access::Basic(B)],
            writes: vec![basic_write(C, 300, 0)],
        },
        FakeTx {
            reads: vec![Access::Basic(C)],
            writes: vec![basic_write(D, 400, 0)],
        },
    ];

    let seq = run_sequential(&storage, &txs);
    let par = run_parallel(&storage, &txs, 8);
    assert_eq!(state_to_canonical(&seq), state_to_canonical(&par));
}

#[test]
fn many_txs_different_storage_slots_same_address_parallel_matches_sequential() {
    let storage = Mem::default();
    let txs: Vec<FakeTx> = (0..32u64)
        .map(|i| FakeTx {
            reads: vec![Access::Storage(A, U256::from(i))],
            writes: vec![Write {
                addr: A,
                new_balance: U256::from(1000),
                new_nonce: i,
                storage: vec![(U256::from(i), U256::from(i * 2))],
            }],
        })
        .collect();

    let seq = run_sequential(&storage, &txs);
    let par = run_parallel(&storage, &txs, 8);
    // All txs write the same address's info but different storage slots.
    // The final basic-info value is whatever the last-applied tx wrote;
    // state_to_canonical applies in tx order so both runs produce the
    // same final info.
    assert_eq!(state_to_canonical(&seq), state_to_canonical(&par));
}

#[test]
fn read_modify_write_across_txs_parallel_matches_sequential() {
    // A chain where each tx reads the previous tx's written address and
    // writes a new one. This forces serialisation — parallel execution
    // will hit repeated aborts + re-executions.
    let storage = Mem::default();
    let txs: Vec<FakeTx> = (0..8u8)
        .map(|i| {
            let mut prev_bytes = [0u8; 20];
            prev_bytes[19] = i;
            let prev = Address::from(prev_bytes);
            let mut next_bytes = [0u8; 20];
            next_bytes[19] = i + 1;
            let next = Address::from(next_bytes);
            FakeTx {
                reads: if i == 0 {
                    vec![]
                } else {
                    vec![Access::Basic(prev)]
                },
                writes: vec![basic_write(next, (i as u128 + 1) * 1000, 0)],
            }
        })
        .collect();

    let seq = run_sequential(&storage, &txs);
    let par = run_parallel(&storage, &txs, 4);
    assert_eq!(state_to_canonical(&seq), state_to_canonical(&par));
}

#[test]
fn concurrency_level_does_not_affect_final_state() {
    // Block-STM guarantees determinism regardless of thread count.
    let storage = Mem::default();
    let txs: Vec<FakeTx> = (0..32)
        .map(|i: u8| {
            let mut bytes = [0u8; 20];
            bytes[19] = i % 5; // heavy contention on 5 addresses
            let addr = Address::from(bytes);
            FakeTx {
                reads: vec![Access::Basic(addr)],
                writes: vec![basic_write(addr, i as u128, i as u64)],
            }
        })
        .collect();

    let seq = run_sequential(&storage, &txs);
    let canonical_seq = state_to_canonical(&seq);
    for workers in [1, 2, 4, 8, 16] {
        let par = run_parallel(&storage, &txs, workers);
        assert_eq!(
            state_to_canonical(&par),
            canonical_seq,
            "workers={workers} diverged from sequential"
        );
    }
}
