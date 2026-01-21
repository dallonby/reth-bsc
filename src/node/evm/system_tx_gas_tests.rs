//! Tests for system transaction gas usage.

#[cfg(test)]
mod tests {
    use crate::chainspec::BscChainSpec;
    use crate::hardforks::bsc::BscHardfork;
    use crate::node::evm::config::BscEvmConfig;
    use crate::node::evm::factory::BscEvmFactory;
    use crate::system_contracts::{get_upgrade_system_contracts, SystemContract, SLASH_CONTRACT};
    use crate::evm::transaction::BscTxEnv;
    use alloy_consensus::Header;
    use alloy_genesis::Genesis;
    use alloy_primitives::{Address, B256};
    use reth_chainspec::{Chain, ChainSpec, ForkCondition, NamedChain};
    use reth_ethereum_forks::EthereumHardfork;
    use reth_evm::{ConfigureEvm, EvmFactory};
    use reth_revm::{db::{CacheDB, EmptyDB}, State};
    use revm::context::TxEnv;
    use revm::context_interface::result::ExecutionResult;
    use revm::primitives::U256;
    use revm::state::AccountInfo;
    use revm::ExecuteEvm;
    use std::sync::Arc;

    fn build_fermi_chainspec(fermi_time: u64) -> BscChainSpec {
        let genesis: Genesis = serde_json::from_str(include_str!("../../chainspec/genesis.json"))
            .expect("valid mainnet genesis json");

        let chain_spec = ChainSpec::builder()
            .chain(Chain::from_named(NamedChain::BinanceSmartChain))
            .genesis(genesis)
            // Minimal Ethereum hardfork set to enable London (required by BSC forks)
            .with_fork(EthereumHardfork::Frontier, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Homestead, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Tangerine, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::SpuriousDragon, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Byzantium, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Constantinople, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Petersburg, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Istanbul, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::MuirGlacier, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Berlin, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::London, ForkCondition::Block(0))
            .with_fork(EthereumHardfork::Cancun, ForkCondition::Timestamp(0))
            // Ensure an upgrade path that provides SlashContract code.
            .with_fork(BscHardfork::Euler, ForkCondition::Block(1))
            .with_fork(BscHardfork::Cancun, ForkCondition::Timestamp(0))
            // Activate Fermi at a known timestamp so we can force upgrades.
            .with_fork(BscHardfork::Fermi, ForkCondition::Timestamp(fermi_time))
            .build();

        BscChainSpec { inner: chain_spec }
    }

    fn apply_system_contract_upgrades(
        db: &mut CacheDB<EmptyDB>,
        upgrades: std::collections::HashMap<Address, Option<revm::state::Bytecode>>,
    ) {
        for (address, code) in upgrades {
            let Some(code) = code else { continue };
            let mut info = AccountInfo::default();
            info.code_hash = code.hash_slow();
            info.code = Some(code);
            info.nonce = 1;
            info.balance = U256::ZERO;
            db.insert_account_info(address, info);
        }
    }

    fn to_system_tx_env(
        tx: reth_primitives::Transaction,
        caller: Address,
        block_gas_limit: u64,
    ) -> BscTxEnv {
        match tx {
            reth_primitives::Transaction::Legacy(tx) => {
                let mut env = TxEnv::default();
                env.tx_type = 0;
                env.caller = caller;
                env.gas_limit = std::cmp::min(tx.gas_limit, block_gas_limit);
                env.gas_price = tx.gas_price;
                env.kind = tx.to;
                env.value = tx.value;
                env.data = tx.input;
                env.nonce = tx.nonce;
                env.chain_id = tx.chain_id;
                env.access_list = Default::default();
                env.gas_priority_fee = None;
                env.blob_hashes = Vec::new();
                env.max_fee_per_blob_gas = 0;
                env.authorization_list = Vec::new();
                BscTxEnv { base: env, is_system_transaction: true }
            }
            _ => panic!("slash tx should be legacy"),
        }
    }

    fn gas_used(result: &ExecutionResult) -> u64 {
        match result {
            ExecutionResult::Success { gas_used, .. } => *gas_used,
            ExecutionResult::Revert { gas_used, .. } => *gas_used,
            ExecutionResult::Halt { gas_used, .. } => *gas_used,
        }
    }

    fn execute_slash_gas(
        spec: Arc<BscChainSpec>,
        header: &Header,
        db: CacheDB<EmptyDB>,
        caller: Address,
        slash_target: Address,
    ) -> u64 {
        let evm_config = BscEvmConfig::new(spec.clone());
        let evm_env = evm_config.evm_env(header).expect("evm env");

        let state = State::builder().with_database(db).build();
        let mut evm = BscEvmFactory::default().create_evm(state, evm_env);

        let system_contracts = SystemContract::new(spec);
        let tx = system_contracts.slash(slash_target);
        let tx_env = to_system_tx_env(tx, caller, header.gas_limit);

        let result = evm.transact(tx_env).expect("evm transact");
        gas_used(&result.result)
    }

    #[test]
    fn fermi_slash_gas_is_deterministic() {
        let spec = Arc::new(build_fermi_chainspec(1));

        // Force the Fermi upgrade at block 1 / timestamp 1.
        let upgrades = get_upgrade_system_contracts(spec.as_ref(), 1, 1, 0)
            .expect("fermi upgrades");
        assert!(
            upgrades.contains_key(&SLASH_CONTRACT),
            "upgrade set should include slash contract code; got keys: {:?}",
            upgrades.keys().collect::<Vec<_>>()
        );

        let mut db = CacheDB::new(EmptyDB::default());
        apply_system_contract_upgrades(&mut db, upgrades);

        let caller = Address::from([0x11; 20]);
        let header = Header {
            number: 1,
            timestamp: 1,
            gas_limit: 30_000_000,
            beneficiary: caller,
            parent_hash: B256::ZERO,
            blob_gas_used: Some(0),
            excess_blob_gas: Some(0),
            ..Default::default()
        };

        let db1 = db.clone();
        let db2 = db.clone();

        let gas1 = execute_slash_gas(spec.clone(), &header, db1, caller, Address::from([0x22; 20]));
        let gas2 = execute_slash_gas(spec, &header, db2, caller, Address::from([0x22; 20]));

        println!("fermi slash gas_used: first={}, second={}", gas1, gas2);
        assert!(gas1 > 0, "slash tx should consume gas");
        assert_eq!(gas1, gas2, "system slash gas should be deterministic");
    }
}
