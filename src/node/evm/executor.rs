use super::patch::HertzPatchManager;
use crate::{
    consensus::{SYSTEM_ADDRESS, parlia::{Parlia, Snapshot, VoteAddress}}, evm::{precompiles, transaction::BscTxEnv}, hardforks::BscHardforks, metrics::{BscBlockchainMetrics, BscConsensusMetrics, BscExecutorMetrics, BscRewardsMetrics, BscVoteMetrics}, node::evm::config::BscExecutionSharedCtx, system_contracts::{
        SystemContract, feynman_fork::ValidatorElectionInfo, get_upgrade_system_contracts, is_system_transaction
    }
};
use alloy_consensus::{Header, Transaction, TxReceipt};
use alloy_eips::{eip7685::Requests, Encodable2718};
use alloy_evm::{block::{ExecutableTx, StateChangeSource, TxResult}, eth::receipt_builder::ReceiptBuilderCtx};
use alloy_primitives::{hex, uint, Address, U256, BlockNumber, Bytes};
use reth_chainspec::{EthChainSpec, EthereumHardforks, Hardforks};
use super::config::{BscBlockExecutionCtx, revm_spec_by_timestamp_and_block_number};
use reth_evm::{
    block::{BlockValidationError, CommitChanges},
    eth::receipt_builder::ReceiptBuilder,
    execute::{BlockExecutionError, BlockExecutor},
    system_calls::SystemCaller,
    Evm, FromRecoveredTx, FromTxWithEncoded, IntoTxEnv, OnStateHook, RecoveredTx,
};
use reth_ethereum_primitives::TransactionSigned;
use reth_provider::BlockExecutionResult;
// reth 2.0 relaxed BlockExecutorFactory::create_executor to take any DB: StateDB,
// so the executor can no longer require a concrete `&'a mut State<DB>` on the EVM.
// We lean on DatabaseCommit + DatabaseCommitExt (drain_balances / increment_balances)
// and `commit_iter` to make bytecode/nonce patches, instead of the State-specific
// `load_cache_account` / `apply_transition` pair we used before.
use revm::{
    context::{
        result::{ExecutionResult, ResultAndState},

    },
    context_interface::block::Block,
    state::{Account, AccountInfo, Bytecode},
    Database as _RevmDatabase, DatabaseCommit,
};
use tracing::{error, warn, info, debug, trace};
use alloy_eips::eip2935::{HISTORY_STORAGE_ADDRESS, HISTORY_STORAGE_CODE};
use alloy_primitives::keccak256;
use std::{collections::HashMap, sync::Arc};
use crate::consensus::parlia::SnapshotProvider;

/// Result of executing a single BSC transaction without committing state.
///
/// Carries the data `commit_transaction` needs after execution: the EVM
/// `ResultAndState`, the consensus `tx_type` for the receipt, the blob gas
/// used (Cancun+), and the tx hash (for observability).
#[derive(Debug)]
pub struct BscTxResult<H> {
    /// Result of the transaction execution.
    pub result: ResultAndState<H>,
    /// Type of the transaction (consensus tx type byte).
    pub tx_type: alloy_consensus::TxType,
    /// Blob gas used by the transaction (0 before Cancun).
    pub blob_gas_used: u64,
    /// Transaction hash, retained for trace/diagnostic context.
    pub tx_hash: alloy_primitives::B256,
}

impl<H> TxResult for BscTxResult<H> {
    type HaltReason = H;

    fn result(&self) -> &ResultAndState<Self::HaltReason> {
        &self.result
    }

    fn into_result(self) -> ResultAndState<Self::HaltReason> {
        self.result
    }
}

/// Helper type for the input of post execution.
#[allow(clippy::type_complexity)]
#[derive(Debug, Clone)]
pub(crate) struct InnerExecutionContext {
    pub(crate) current_validators: Option<(Vec<Address>, HashMap<Address, VoteAddress>)>,
    pub(crate) expected_turn_length: Option<u8>,
    pub(crate) max_elected_validators: Option<U256>,
    pub(crate) validators_election_info: Option<Vec<ValidatorElectionInfo>>,
    pub(crate) snap: Option<Snapshot>,
    pub(crate) header: Option<Header>,
    pub(crate) parent_header: Option<Header>,
}

pub struct BscBlockExecutor<'a, EVM, Spec, R: ReceiptBuilder>
where
    Spec: EthChainSpec,
{
    /// Reference to the specification object.
    pub(super) spec: Spec,
    /// Inner EVM.
    pub(super) evm: EVM,
    /// Gas used in the block.
    pub(super) gas_used: u64,
    /// Total blob gas used in the block.
    pub(super) blob_gas_used: u64,
    /// Receipts of executed transactions.
    pub(super) receipts: Vec<R::Receipt>,
    /// System txs
    pub(super) system_txs: Vec<R::Transaction>,
    /// Receipt builder.
    pub(super) receipt_builder: R,
    /// System contracts used to trigger fork specific logic.
    pub(super) system_contracts: SystemContract<Spec>,
    /// Hertz patch manager for compatibility.
    hertz_patch_manager: HertzPatchManager,
    /// Context for block execution.
    pub(super) ctx: BscBlockExecutionCtx<'a>,
    /// Utility to call system caller.
    pub(super) system_caller: SystemCaller<Spec>,
    /// Snapshot provider for accessing Parlia validator snapshots.
    pub(super) snapshot_provider: Option<Arc<dyn SnapshotProvider + Send + Sync>>,
    /// Parlia consensus instance.
    pub(crate) parlia: Arc<Parlia<Spec>>,
    /// Inner execution context.
    pub(super) inner_ctx: InnerExecutionContext,
    /// Shared context for block execution.
    pub(super) shared_ctx: BscExecutionSharedCtx,
    /// Consensus metrics for tracking block height and other consensus stats.
    pub(super) consensus_metrics: BscConsensusMetrics,
    /// Blockchain metrics for tracking receipts and block processing.
    pub(super) blockchain_metrics: BscBlockchainMetrics,
    /// Vote metrics for tracking attestation errors.
    pub(super) vote_metrics: BscVoteMetrics,
    /// Executor metrics for tracking block execution.
    pub(super) executor_metrics: BscExecutorMetrics,
    /// Rewards metrics for tracking reward distributions.
    pub(super) rewards_metrics: BscRewardsMetrics,
}

impl<'a, EVM, Spec, R: ReceiptBuilder> BscBlockExecutor<'a, EVM, Spec, R>
where
    EVM: Evm<
        DB: alloy_evm::block::StateDB + 'a,
        Tx: FromRecoveredTx<R::Transaction>
                + FromRecoveredTx<TransactionSigned>
                + FromTxWithEncoded<TransactionSigned>,
        BlockEnv = revm::context::BlockEnv,
    >,
    Spec: EthereumHardforks + BscHardforks + EthChainSpec + Hardforks + Clone + 'static,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt: TxReceipt>,
    <R as ReceiptBuilder>::Transaction: Unpin + From<TransactionSigned>,
    <EVM as alloy_evm::Evm>::Tx: FromTxWithEncoded<<R as ReceiptBuilder>::Transaction>,
    BscTxEnv: IntoTxEnv<<EVM as alloy_evm::Evm>::Tx>,
    R::Transaction: Into<TransactionSigned>,
{
    /// Creates a new BscBlockExecutor.
    pub(crate) fn new(
        evm: EVM,
        ctx: BscBlockExecutionCtx<'a>,
        shared_ctx: BscExecutionSharedCtx,
        spec: Spec,
        receipt_builder: R,
        system_contracts: SystemContract<Spec>,
    ) -> Self {
        let is_mainnet = spec.chain().id() == 56; // BSC mainnet chain ID
        let hertz_patch_manager = HertzPatchManager::new(is_mainnet);
        
        trace!("Succeed to new block executor, header: {:?}", ctx.header);
        if let Some(ref header) = ctx.header {
            crate::node::evm::util::HEADER_CACHE_READER.lock().unwrap().insert_header_to_cache(header.clone());
        } else if !ctx.is_miner { // miner has no current header.
            warn!("No header found in the context, block_number: {:?}", evm.block().number().to::<u64>());
        }

        let parlia = Arc::new(Parlia::new(Arc::new(spec.clone()), 200));
        let spec_clone = spec.clone();
        Self {
            spec,
            evm,
            gas_used: 0,
            blob_gas_used: 0,
            receipts: vec![],
            system_txs: vec![],
            receipt_builder,
            system_contracts,
            hertz_patch_manager,
            ctx,
            shared_ctx,
            system_caller: SystemCaller::new(spec_clone),
            snapshot_provider: crate::shared::get_snapshot_provider().cloned(),
            parlia,
            inner_ctx: InnerExecutionContext {
                current_validators: None,
                expected_turn_length: None,
                max_elected_validators: None,
                validators_election_info: None,
                snap: None,
                header: None,
                parent_header: None,
            },
            consensus_metrics: BscConsensusMetrics::default(),
            blockchain_metrics: BscBlockchainMetrics::default(),
            vote_metrics: BscVoteMetrics::default(),
            executor_metrics: BscExecutorMetrics::default(),
            rewards_metrics: BscRewardsMetrics::default(),
        }
    }

    /// Accumulate blob gas used for Cancun blocks.
    fn accumulate_blob_gas_used<T: Transaction>(&mut self, tx: &T) {
        if !BscHardforks::is_cancun_active_at_timestamp(
            &self.spec,
            self.evm.block().number().to::<u64>(),
            self.evm.block().timestamp().to::<u64>(),
        ) {
            return;
        }

        self.blob_gas_used =
            self.blob_gas_used.saturating_add(tx.blob_gas_used().unwrap_or_default());
    }

    /// Applies system contract upgrades if the Feynman fork is not yet active.
    fn upgrade_contracts(&mut self, block_number: BlockNumber, block_timestamp: u64, parent_timestamp: u64) -> Result<(), BlockExecutionError> {
        trace!(
            target: "bsc::executor::upgrade",
            block_number,
            block_timestamp,
            parent_timestamp,
            "Calling get_upgrade_system_contracts"
        );
        
        let contracts = get_upgrade_system_contracts(
            &self.spec,
            block_number,
            block_timestamp,
            parent_timestamp,
        )
        .map_err(|_| BlockExecutionError::msg("Failed to get upgrade system contracts"))?;

        for (address, maybe_code) in contracts {
            if let Some(code) = maybe_code {
                debug!(
                    target: "bsc::executor::upgrade",
                    block_number,
                    address = ?address,
                    code_len = code.len(),
                    "Upgrading system contract"
                );
                self.upgrade_system_contract(address, code)?;
            }
        }

        Ok(())
    }

    /// Mimics Geth-BSC's TryUpdateBuildInSystemContract function
    fn try_update_build_in_system_contract(&mut self, block_number: BlockNumber, block_timestamp: u64, parent_timestamp: u64, at_block_begin: bool) -> Result<(), BlockExecutionError> {
        if at_block_begin {
            // Upgrade system contracts before Feynman at block begin
            if !self.spec.is_feynman_active_at_timestamp(block_number, parent_timestamp) {
                trace!(
                    target: "bsc::executor::upgrade",
                    block_number,
                    parent_timestamp,
                    "Upgrading system contracts at block begin (before Feynman)"
                );
                self.upgrade_contracts(block_number, block_timestamp, parent_timestamp)?;
            }
            
            // HistoryStorageAddress is a special system contract in BSC, which can't be upgraded
            // This must be done at block begin when Prague activates
            if self.spec.is_prague_transition_at_block_and_timestamp(block_number, block_timestamp, parent_timestamp) {
                info!(
                    target: "bsc::executor::prague",
                    block_number,
                    block_timestamp,
                    "Deploying HistoryStorageAddress contract (Prague transition at block begin)"
                );
                self.apply_history_storage_account(block_number)?;
            }
        } else {
            // Upgrade system contracts after Feynman at block end
            if self.spec.is_feynman_active_at_timestamp(block_number, parent_timestamp) {
                trace!(
                    target: "bsc::executor::upgrade",
                    block_number,
                    parent_timestamp,
                    "Upgrading system contracts at block end (Feynman active)"
                );
                self.upgrade_contracts(block_number, block_timestamp, parent_timestamp)?;
            }
        }
        Ok(())
    }

    /// Initializes the feynman contracts
    fn initialize_feynman_contracts(
        &mut self,
        beneficiary: Address,
    ) -> Result<(), BlockExecutionError> {
        let txs = self.system_contracts.feynman_contracts_txs();
        for tx in txs {
            self.transact_system_tx(tx.into(), beneficiary)?;
        }
        Ok(())
    }

    /// Initializes the genesis contracts
    fn deploy_genesis_contracts(
        &mut self,
        beneficiary: Address,
    ) -> Result<(), BlockExecutionError> {
        let txs = self.system_contracts.genesis_contracts_txs();
        for  tx in txs {
            self.transact_system_tx(tx.into(), beneficiary)?;
        }
        Ok(())
    }

    /// Replaces the code of a system contract in state.
    fn upgrade_system_contract(
        &mut self,
        address: Address,
        code: Bytecode,
    ) -> Result<(), BlockExecutionError> {
        // Trait-only replacement for the old State::load_cache_account /
        // apply_transition pair: read the current AccountInfo via
        // Database::basic, patch code + code_hash, build a touched Account,
        // and flush via DatabaseCommit::commit_iter — which State<DB> routes
        // through its own transition machinery just like the old path did.
        let info = self
            .evm
            .db_mut()
            .basic(address)
            .map_err(BlockExecutionError::other)?
            .unwrap_or_default();
        let new_info = AccountInfo {
            code_hash: code.hash_slow(),
            code: Some(code),
            ..info
        };
        let mut account = Account::default();
        account.info = new_info;
        account.mark_touch();
        self.evm
            .db_mut()
            .commit_iter(&mut core::iter::once((address, account)));
        Ok(())
    }

    pub(crate) fn apply_history_storage_account(
        &mut self,
        block_number: BlockNumber,
    ) -> Result<bool, BlockExecutionError> {
        info!(
            target: "bsc::executor::prague",
            block_number,
            address = ?HISTORY_STORAGE_ADDRESS,
            "Deploying HistoryStorageAddress contract (Prague transition)"
        );

        // Same trait-only pattern as upgrade_system_contract above.
        let old_info = self
            .evm
            .db_mut()
            .basic(HISTORY_STORAGE_ADDRESS)
            .map_err(|err| {
                error!(
                    target: "bsc::executor::prague",
                    block_number,
                    error = ?err,
                    "Failed to load HistoryStorageAddress account",
                );
                BlockExecutionError::other(err)
            })?;
        debug!(
            target: "bsc::executor::prague",
            block_number,
            old_nonce = ?old_info.as_ref().map(|i| i.nonce),
            old_code_hash = ?old_info.as_ref().map(|i| i.code_hash),
            "HistoryStorageAddress account before deployment"
        );

        let new_info = AccountInfo {
            code_hash: keccak256(HISTORY_STORAGE_CODE.clone()),
            code: Some(Bytecode::new_raw(Bytes::from_static(&HISTORY_STORAGE_CODE))),
            nonce: 1_u64,
            balance: U256::ZERO,
            account_id: None,
        };
        let mut account = Account::default();
        account.info = new_info;
        account.mark_touch();
        self.evm
            .db_mut()
            .commit_iter(&mut core::iter::once((HISTORY_STORAGE_ADDRESS, account)));
        
        info!(
            target: "bsc::executor::prague",
            block_number,
            "Successfully deployed HistoryStorageAddress contract"
        );
        Ok(true)
    }

    /// Execute a block's user transactions in parallel via Block-STM.
    ///
    /// # Overview
    ///
    /// This is the parallel analog of `BlockExecutor::execute_block` for
    /// blocks flagged by `ctx.parallel` (set from
    /// `--bsc.parallel-execute`). System transactions and all pre/post
    /// execution hooks stay serial; only the user-transaction body runs
    /// under `parallel_evm::ParallelExecutor`.
    ///
    /// # Storage layer
    ///
    /// Parallel workers read through a [`LayeredStorage`](crate::node::evm::parallel::LayeredStorage):
    /// an immutable `bundle_snapshot` overlay (required for pipeline-sync
    /// batch correctness, where many blocks commit into a single
    /// end-of-batch flush) over a Send+Sync `StateProviderFactory` that
    /// spawns a fresh per-worker read transaction on each fallback read.
    /// Each worker tx is owned by one thread and never shared — so we do
    /// NOT require `DB: Sync` (MDBX transactions are not `Sync` by
    /// design; that was the wall that killed the prior attempt, see
    /// commit `01e69bb`).
    ///
    /// # Fallback rules
    ///
    /// Three serial-fallback triggers preserve correctness:
    ///
    /// 1. **Hertz patches.** Any tx whose hash matches a Hertz patch
    ///    entry forces the entire block to run serial. Parallel workers
    ///    would read pre-patch state and diverge from mainnet.
    /// 2. **Parallel executor error.** Any `parallel_evm::Error` (budget
    ///    exhausted, storage failure) aborts the parallel attempt and
    ///    re-runs the whole block serially. Since nothing was committed
    ///    during the parallel phase, fallback is clean.
    /// 3. **Per-tx validation error.** If any result in the parallel
    ///    output is `Err(_)`, re-run serially to let the existing serial
    ///    path either validate the rejection or produce a proper error.
    ///
    /// Block-STM is deterministic (Aptos paper §4): on a happy-path
    /// success, the state root must match the serial baseline byte-for-
    /// byte. Any divergence is a bug; fallback is NOT a correctness
    /// hedge — it's a targeted escape hatch for cases parallel mode
    /// can't yet handle (patches, engine-level abort conditions).
    ///
    /// # Caller contract
    ///
    /// `bundle_snapshot` must reflect state up to the **parent** block's
    /// end-of-batch bundle (NOT the current block's pre-execution
    /// mutations, which the bundle doesn't see). In pipeline sync, the
    /// caller (a future `BscParallelExecutionStage`) holds
    /// `&mut State<DB>` at the pipeline level and can snapshot
    /// `state.bundle_state` immediately before the per-block executor
    /// is constructed.
    ///
    /// `factory` is typically the node's `ProviderFactory<N>` clone —
    /// Send+Sync by construction. Per-worker `factory.latest()` calls
    /// produce a fresh `StateProviderBox` per read, which keeps the non-
    /// Sync MDBX tx contained to a single thread.
    pub fn execute_block_parallel<F>(
        mut self,
        factory: &F,
        bundle_snapshot: &revm::database::BundleState,
        transactions: impl IntoIterator<Item = impl ExecutableTx<Self>>,
    ) -> Result<BlockExecutionResult<R::Receipt>, BlockExecutionError>
    where
        F: reth_provider::StateProviderFactory + Send + Sync,
        Spec: std::fmt::Debug + Send + Sync,
        // The parallel executor produces revm's `HaltReason`; pin the EVM's
        // own HaltReason to match so the results type-check when fed into
        // `commit_transaction`. This holds for the production BSC path
        // (BscEvmFactory::HaltReason = HaltReason); the bound only rules
        // out exotic EVM configurations we don't ship.
        EVM: Evm<HaltReason = revm::context::result::HaltReason>,
    {
        use super::parallel::{BscVmBuilder, LayeredStorage};
        use alloy_consensus::transaction::SignerRecoverable;
        use parallel_evm::{Config as ParallelConfig, ParallelExecutor};
        use reth_primitives_traits::Recovered;

        self.apply_pre_execution_changes()?;

        let beneficiary = self.evm.block().beneficiary();
        let block_env = self.evm.block().clone();
        let block_number = block_env.number().to::<u64>();
        let timestamp = block_env.timestamp().to::<u64>();
        let hardfork = revm_spec_by_timestamp_and_block_number(
            self.spec.clone(),
            timestamp,
            block_number,
        );

        // Per-tx slot so we can multi-pass (hertz scan, parallel split,
        // serial-fallback). We hold the evm's tx_env for the serial path
        // AND a freshly-built BscTxEnv for the parallel path, because
        // `<EVM as Evm>::Tx` and `BscTxEnv` aren't the same type in the
        // generic — `BscTxEnv: IntoTxEnv<<EVM as Evm>::Tx>` is one-way.
        //
        // `Recovered` is materialized by cloning the signed tx + signer
        // out of whatever `ExecutableTx::Recovered` the caller gave us.
        // The clone is cheap (TransactionSigned is Arc-backed) and makes
        // the slot type concrete so we can feed it back into
        // `execute_transaction` on the serial-fallback paths.
        struct Slot<EvmTx> {
            tx_env: EvmTx,
            recovered: Recovered<TransactionSigned>,
            tx_hash: alloy_primitives::B256,
            tx_type: alloy_consensus::TxType,
            blob_gas_used: u64,
            is_system: bool,
        }

        let mut slots: Vec<Slot<<EVM as alloy_evm::Evm>::Tx>> = Vec::new();
        for tx in transactions {
            let (tx_env, recovered) = tx.into_parts();
            let signed: &TransactionSigned = recovered.tx();
            // `recovered` here is bound by `RecoveredTx<TransactionSigned>`,
            // whose `signer()` returns `&Address`. Deref to a value (Copy).
            let signer: Address = *recovered.signer();
            let tx_hash = signed.trie_hash();
            let tx_type = signed.tx_type();
            let blob_gas_used = signed.blob_gas_used().unwrap_or_default();
            let is_system = is_system_transaction(signed, signer, beneficiary);
            let concrete_recovered = Recovered::new_unchecked(signed.clone(), signer);
            slots.push(Slot {
                tx_env,
                recovered: concrete_recovered,
                tx_hash,
                tx_type,
                blob_gas_used,
                is_system,
            });
        }

        // Trigger (1): Hertz-patched tx anywhere in the block → full
        // serial fallback. System-tx hashes can't appear in the patch
        // set but the scan is cheap and handles both cases uniformly.
        let has_hertz = slots
            .iter()
            .any(|s| self.hertz_patch_manager.has_patch(&s.tx_hash));
        if has_hertz {
            tracing::debug!(
                target: "bsc::executor::parallel",
                block_number,
                "hertz-patched tx detected, running block serially"
            );
            for s in slots {
                self.execute_transaction((s.tx_env, s.recovered))?;
            }
            return self.apply_post_execution_changes();
        }

        // Split: build BscTxEnv list for user txs (the parallel input).
        // System txs are buffered via the existing serial path during
        // the commit loop below.
        let user_tx_envs: Vec<BscTxEnv> = slots
            .iter()
            .filter(|s| !s.is_system)
            .map(|s| {
                BscTxEnv::from_recovered_tx(s.recovered.tx(), s.recovered.signer())
            })
            .collect();

        // Pure-system-tx block → no parallel work; fall through serial.
        // (In practice this never happens on BSC, but keeps the logic
        // honest.)
        if user_tx_envs.is_empty() {
            for s in slots {
                self.execute_transaction((s.tx_env, s.recovered))?;
            }
            return self.apply_post_execution_changes();
        }

        // Build the parallel executor + layered storage. `BscVmBuilder`
        // is stateless (Clone) and the Config defaults to CPU-count
        // workers. If we later need workload-specific tuning we'll plumb
        // it through the CLI.
        let vm_builder = BscVmBuilder::new(self.spec.clone());
        let parallel = ParallelExecutor::new(ParallelConfig::default(), vm_builder);
        let layered = LayeredStorage::new(bundle_snapshot, factory);

        let output = match parallel.execute(
            &layered,
            block_env,
            hardfork,
            user_tx_envs,
        ) {
            Ok(o) => o,
            Err(err) => {
                // Trigger (2): parallel failure → serial fallback. No
                // state has been committed yet, so the fallback loop
                // starts from the same pre-exec state.
                tracing::warn!(
                    target: "bsc::executor::parallel",
                    block_number,
                    ?err,
                    "parallel execution failed, falling back to serial"
                );
                for s in slots {
                    self.execute_transaction((s.tx_env, s.recovered))?;
                }
                return self.apply_post_execution_changes();
            }
        };

        // Trigger (3): per-tx Err means the parallel executor rejected
        // a tx (validation or an unrecoverable storage fault). Re-run
        // serially — the serial path either produces the same rejection
        // with a proper error, or commits differently under Hertz-ish
        // edge cases we haven't catalogued yet.
        if output.results.iter().any(|r| r.is_err()) {
            tracing::warn!(
                target: "bsc::executor::parallel",
                block_number,
                "parallel output contained error results, falling back to serial"
            );
            for s in slots {
                self.execute_transaction((s.tx_env, s.recovered))?;
            }
            return self.apply_post_execution_changes();
        }

        // Happy path: commit in original block order. User txs take
        // their result from the parallel output (always `Ok(_)` by the
        // check above); system txs are buffered exactly like the serial
        // `execute_transaction_with_result_closure` does — the `finish`
        // phase replays them via the system-contract path.
        let mut parallel_iter = output.results.into_iter();
        for s in slots {
            if s.is_system {
                self.system_txs.push(s.recovered.tx().clone());
                continue;
            }
            let ras = parallel_iter
                .next()
                .expect("user-tx count mismatch between slots and parallel output")
                .expect("error results already filtered above");
            self.commit_transaction(BscTxResult {
                result: ras,
                tx_type: s.tx_type,
                blob_gas_used: s.blob_gas_used,
                tx_hash: s.tx_hash,
            })?;
        }

        self.apply_post_execution_changes()
    }

}

impl<'a, E, Spec, R> BlockExecutor for BscBlockExecutor<'a, E, Spec, R>
where
    E: Evm<
        DB: alloy_evm::block::StateDB + 'a,
        Tx: FromRecoveredTx<R::Transaction>
                + FromRecoveredTx<TransactionSigned>
                + FromTxWithEncoded<TransactionSigned>,
        BlockEnv = revm::context::BlockEnv,
    >,
    Spec: EthereumHardforks + BscHardforks + EthChainSpec + Hardforks + 'static,
    R: ReceiptBuilder<Transaction = TransactionSigned, Receipt: TxReceipt>,
    <R as ReceiptBuilder>::Transaction: Unpin + From<TransactionSigned>,
    <E as alloy_evm::Evm>::Tx: FromTxWithEncoded<<R as ReceiptBuilder>::Transaction>,
    BscTxEnv: IntoTxEnv<<E as alloy_evm::Evm>::Tx>,
    R::Transaction: Into<TransactionSigned>,
{
    type Transaction = TransactionSigned;
    type Receipt = R::Receipt;
    type Evm = E;
    type Result = BscTxResult<<E as Evm>::HaltReason>;

    fn apply_pre_execution_changes(&mut self) -> Result<(), BlockExecutionError> {
        let block_env = self.evm.block().clone();
        trace!(
            target: "bsc::executor", 
            block_id = %block_env.number(),
            is_miner = self.ctx.is_miner,
            "Start to apply_pre_execution_changes"
        );
        
        // Update current block height and header height metrics
        let block_number = block_env.number().to::<u64>();
        self.consensus_metrics.current_block_height.set(block_number as f64);

        // Speculative prefetch workers skip ALL Parlia validation, snapshot
        // writes, header/validator cache inserts, and system-contract upgrades.
        // The *only* thing a worker does is run the block's transactions so
        // state reads page-fault and warm the OS page cache for the main
        // thread. We bail out here before touching anything global.
        if self.ctx.speculative {
            return Ok(());
        }

        if self.ctx.parallel {
            tracing::debug!(
                target: "bsc::executor::parallel",
                block_number,
                "parallel-execute enabled for this block (serial fallback until branch is wired)"
            );
        }

        // pre check and prepare some intermediate data for commit parlia snapshot in finish function.
        if self.ctx.is_miner {
            self.prepare_new_block(&block_env)?;
        } else {
            self.check_new_block(&block_env)?;
        }
        
        // State clear flag is now handled by the EVM journal in `finalize()` based on the spec
        // (revm-database 12.0.0+); the database always applies post-EIP-161 commit semantics.
        let parent_timestamp = self.inner_ctx.parent_header.as_ref().unwrap().timestamp;
        self.try_update_build_in_system_contract(
            self.evm.block().number().to::<u64>(), 
            self.evm.block().timestamp().to::<u64>(), 
            parent_timestamp, 
            true
        )?;
     
        // Apply historical block hashes if Prague is active
        if self.spec.is_prague_active_at_block_and_timestamp(
            self.evm.block().number().to::<u64>(), 
            self.evm.block().timestamp().to::<u64>()
        ) {
            trace!(
                target: "bsc::executor::prague",
                block_number = self.evm.block().number().to::<u64>(),
                parent_hash = ?self.ctx.base.parent_hash,
                "Calling apply_blockhashes_contract_call (Prague active)"
            );
            self.system_caller
                .apply_blockhashes_contract_call(self.ctx.base.parent_hash, &mut self.evm)?;
        }

        Ok(())
    }

    fn execute_transaction_without_commit(
        &mut self,
        tx: impl ExecutableTx<Self>,
    ) -> Result<Self::Result, BlockExecutionError> {
        let (tx_env, tx) = tx.into_parts();

        let block_available_gas = self.evm.block().gas_limit() - self.gas_used;
        if tx.tx().gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: tx.tx().gas_limit(),
                block_available_gas,
            }
            .into());
        }

        let tx_hash = tx.tx().trie_hash();
        let block_number = self.evm.block().number().to::<u64>();
        let timestamp = self.evm.block().timestamp().to::<u64>();
        let spec = revm_spec_by_timestamp_and_block_number(self.spec.clone(), timestamp, block_number);
        let (to, selector, input_len) = {
            let to = tx.tx().to();
            let input = tx.tx().input();
            let selector = if input.len() >= 4 {
                Some(hex::encode(&input[..4]))
            } else {
                None
            };
            (to, selector, input.len())
        };

        precompiles::push_precompile_trace_context(precompiles::PrecompileTraceContext::from_parts(
            block_number,
            spec,
            false,
            Some(tx_hash),
            to,
            selector,
            input_len,
        ));
        struct PrecompileTracePopGuard;
        impl Drop for PrecompileTracePopGuard {
            fn drop(&mut self) {
                precompiles::pop_precompile_trace_context();
            }
        }
        let _precompile_trace_pop_guard = PrecompileTracePopGuard;

        let tx_type = tx.tx().tx_type();
        let blob_gas_used = tx.tx().blob_gas_used().unwrap_or_default();

        let result = self.evm
            .transact(tx_env)
            .map_err(|err| BlockExecutionError::evm(err, tx_hash))?;

        Ok(BscTxResult { result, tx_type, blob_gas_used, tx_hash })
    }

    fn commit_transaction(
        &mut self,
        output: Self::Result,
    ) -> Result<u64, BlockExecutionError> {
        let BscTxResult { result: ResultAndState { result, state }, tx_type, blob_gas_used, tx_hash: _ } = output;

        let mut temp_state = state.clone();
        temp_state.remove(&SYSTEM_ADDRESS);
        self.system_caller
            .on_state(StateChangeSource::Transaction(self.receipts.len()), &temp_state);

        let gas_used = result.gas_used();
        self.gas_used += gas_used;
        if BscHardforks::is_cancun_active_at_timestamp(
            &self.spec,
            self.evm.block().number().to::<u64>(),
            self.evm.block().timestamp().to::<u64>(),
        ) {
            self.blob_gas_used = self.blob_gas_used.saturating_add(blob_gas_used);
        }

        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx_type,
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));

        self.evm.db_mut().commit(state);

        Ok(gas_used)
    }

    fn execute_transaction_with_commit_condition(
        &mut self,
        tx: impl ExecutableTx<Self>,
        f: impl FnOnce(&ExecutionResult<<Self::Evm as Evm>::HaltReason>) -> CommitChanges,
    ) -> Result<Option<u64>, BlockExecutionError> {
        let output = self.execute_transaction_without_commit(tx)?;

        if !f(&output.result.result).should_commit() {
            return Ok(None);
        }

        let gas_used = self.commit_transaction(output)?;
        Ok(Some(gas_used))
    }

    fn execute_transaction_with_result_closure(
        &mut self,
        tx: impl ExecutableTx<Self>,
        f: impl for<'b> FnOnce(&'b ExecutionResult<<E as alloy_evm::Evm>::HaltReason>),
    ) -> Result<u64, BlockExecutionError> {
        let (tx_env, recovered) = tx.into_parts();

        let signer = recovered.signer();
        let signed_tx: &TransactionSigned = recovered.tx();
        let is_system = is_system_transaction(signed_tx, *signer, self.evm.block().beneficiary());
        if is_system {
            self.system_txs.push(signed_tx.clone());
            return Ok(0);
        }

        self.hertz_patch_manager.patch_before_tx(signed_tx, self.evm.db_mut())?;

        let block_available_gas = self.evm.block().gas_limit() - self.gas_used;
        if signed_tx.gas_limit() > block_available_gas {
            return Err(BlockValidationError::TransactionGasLimitMoreThanAvailableBlockGas {
                transaction_gas_limit: signed_tx.gas_limit(),
                block_available_gas,
            }
            .into());
        }
        let tx_hash = signed_tx.trie_hash();
        let block_number = self.evm.block().number().to::<u64>();
        let timestamp = self.evm.block().timestamp().to::<u64>();
        let spec = revm_spec_by_timestamp_and_block_number(self.spec.clone(), timestamp, block_number);
        let (to, selector, input_len) = {
            let to = signed_tx.to();
            let input = signed_tx.input();
            let selector = if input.len() >= 4 {
                Some(hex::encode(&input[..4]))
            } else {
                None
            };
            (to, selector, input.len())
        };

        precompiles::push_precompile_trace_context(precompiles::PrecompileTraceContext::from_parts(
            block_number,
            spec,
            false,
            Some(tx_hash),
            to,
            selector,
            input_len,
        ));
        struct PrecompileTracePopGuard;
        impl Drop for PrecompileTracePopGuard {
            fn drop(&mut self) {
                precompiles::pop_precompile_trace_context();
            }
        }
        let _precompile_trace_pop_guard = PrecompileTracePopGuard;

        let tx_ref = signed_tx.clone();
        let tx_type = tx_ref.tx_type();
        let result_and_state =
            self.evm.transact(tx_env).map_err(|err| BlockExecutionError::evm(err, tx_hash))?;
        let ResultAndState { result, state } = result_and_state;

        f(&result);

        let mut temp_state = state.clone();
        temp_state.remove(&SYSTEM_ADDRESS);
        self.system_caller.on_state(StateChangeSource::Transaction(self.receipts.len()), &temp_state);

        let gas_used = result.gas_used();
        self.gas_used += gas_used;
        self.accumulate_blob_gas_used(&tx_ref);
        self.receipts.push(self.receipt_builder.build_receipt(ReceiptBuilderCtx {
            tx_type,
            evm: &self.evm,
            result,
            state: &state,
            cumulative_gas_used: self.gas_used,
        }));
        self.evm.db_mut().commit(state);

        self.hertz_patch_manager.patch_after_tx(&tx_ref, self.evm.db_mut())?;

        Ok(gas_used)
    }


    fn finish(
        mut self,
    ) -> Result<(Self::Evm, BlockExecutionResult<R::Receipt>), BlockExecutionError> {
        let block_env = self.evm.block().clone();
        debug!(
            target: "bsc::executor",
            block_id = %block_env.number(),
            is_miner = self.ctx.is_miner,
            "Start to finish"
        );

        // Speculative prefetch workers: skip everything global-mutating. The
        // interesting MDBX reads already happened while the txs executed;
        // don't touch validator rewards, snapshot provider, system contracts,
        // progress metrics, or the prefetcher progress atomic (which is only
        // updated from the main thread).
        if self.ctx.speculative {
            return Ok((
                self.evm,
                BlockExecutionResult {
                    receipts: self.receipts,
                    requests: Requests::default(),
                    gas_used: self.gas_used,
                    blob_gas_used: self.blob_gas_used,
                },
            ));
        }

        let parent_timestamp = self.inner_ctx.parent_header.as_ref().unwrap().timestamp;
        self.try_update_build_in_system_contract(
            self.evm.block().number().to::<u64>(), 
            self.evm.block().timestamp().to::<u64>(), 
            parent_timestamp, 
            false
        )?;

        // Initialize Feynman contracts on transition block
        if self.spec.is_feynman_transition_at_timestamp(
            self.evm.block().number().to::<u64>(), 
            self.evm.block().timestamp().to::<u64>(), 
            parent_timestamp
        ) {
            info!(
                target: "bsc::executor::feynman",
                block_number = self.evm.block().number().to::<u64>(),
                "Initializing Feynman contracts"
            );
            self.initialize_feynman_contracts(self.evm.block().beneficiary())?;
        }

        // Deploy genesis contracts on Block 1
        if self.evm.block().number() == uint!(1U256) {
            info!(
                target: "bsc::executor::genesis",
                "Deploying genesis contracts on Block 1"
            );
            self.deploy_genesis_contracts(self.evm.block().beneficiary())?;
        }

        if self.ctx.is_miner {
            self.finalize_new_block(&self.evm.block().clone())?;
        } else {
            self.post_check_new_block(&self.evm.block().clone())?;
        }

        // Update receipt height metric
        let block_number = self.evm.block().number().to::<u64>();
        self.blockchain_metrics.current_receipt_height.set(block_number as f64);
        
        // Update block execution metrics
        self.executor_metrics.executed_blocks_total.increment(1);
        
        // Update block insert metrics
        // Calculate total transaction size in bytes (simplified estimation)
        // Each receipt contributes approximately: 
        // - Base tx overhead: ~100 bytes
        // - Per log: ~100 bytes (address + topics + data average)
        let tx_size_bytes: usize = self.receipts.iter()
            .map(|r| {
                let logs_count = r.logs().len();
                100 + logs_count * 100 // Base + logs estimation
            })
            .sum();
        self.blockchain_metrics.block_tx_size_bytes.set(tx_size_bytes as f64);
        
        // Calculate block receive time difference
        // This is the difference between current block timestamp and parent block timestamp
        let current_timestamp = self.evm.block().timestamp().to::<u64>();
        if let Some(parent_header) = &self.inner_ctx.parent_header {
            let parent_timestamp = parent_header.timestamp;
            let time_diff = (current_timestamp as i64) - (parent_timestamp as i64);
            self.blockchain_metrics.block_receive_time_diff_seconds.set(time_diff as f64);
        }
        
        // Note: For gas-related metrics, use reth's ExecutorMetrics:
        // - sync.execution.gas_used_histogram
        // - sync.execution.gas_per_second (can be converted to MGas/s)
        // - sync.execution.execution_duration

        // Publish the finished block number to the speculative prefetcher (if
        // enabled). This powers two things:
        //   1. Workers skip jobs they'd otherwise speculatively execute for
        //      blocks the main thread has already finished.
        //   2. The coordinator samples this atomic to compute rolling
        //      blocks-per-second and gates workers on/off accordingly.
        // No-op when the prefetcher was never initialized.
        crate::prefetcher::publish_executed_block(self.evm.block().number().to::<u64>());

        Ok((
            self.evm,
            BlockExecutionResult {
                receipts: self.receipts,
                requests: Requests::default(),
                gas_used: self.gas_used,
                blob_gas_used: self.blob_gas_used,
            },
        ))
    }

    fn set_state_hook(&mut self, hook: Option<Box<dyn OnStateHook>>) {
        self.system_caller.with_state_hook(hook);
    }

    fn evm_mut(&mut self) -> &mut Self::Evm {
        &mut self.evm
    }

    fn evm(&self) -> &Self::Evm {
        &self.evm
    }

    fn receipts(&self) -> &[Self::Receipt] {
        &self.receipts
    }

    fn execute_block(
        mut self,
        transactions: impl IntoIterator<Item = impl ExecutableTx<Self>>,
    ) -> Result<BlockExecutionResult<Self::Receipt>, BlockExecutionError>
    where
        Self: Sized,
    {
        self.apply_pre_execution_changes()?;

        // Parallel-branch scaffold. When `--bsc.parallel-execute` is on for a
        // full (non-speculative, non-miner) block, materialise the tx slice
        // and report it; this is the hook-point where `ParallelExecutor`
        // will slot in. Until the serial-commit phase lands, we still run
        // the stock per-tx loop below so behaviour is byte-identical to the
        // flag-off case.
        if self.ctx.parallel {
            let txs: Vec<_> = transactions.into_iter().collect();
            tracing::debug!(
                target: "bsc::executor::parallel",
                block_number = self.evm.block().number().to::<u64>(),
                num_txs = txs.len(),
                "execute_block hit parallel branch (scaffold — serial fallback)"
            );
            for tx in txs {
                self.execute_transaction(tx)?;
            }
        } else {
            for tx in transactions {
                self.execute_transaction(tx)?;
            }
        }

        self.apply_post_execution_changes()
    }
}
