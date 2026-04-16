use alloy_eips::eip4895::Withdrawals;
use alloy_primitives::{Address, B256};
use alloy_rpc_types_engine::PayloadId;

// Local replacement for the former `EthPayloadBuilderAttributes` struct (and the
// `PayloadBuilderAttributes` trait) that reth 2.0 removed. The reth 2.0 payload
// pipeline composes attributes from raw `PayloadAttributes` + parent header at
// build time, but the BSC miner still needs a concrete validated-attributes type
// it can pass around between `bsc_miner`, `payload`, `util`, and `bid_simulator`.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BscPayloadBuilderAttributes {
    pub id: PayloadId,
    pub parent: B256,
    pub timestamp: u64,
    pub suggested_fee_recipient: Address,
    pub prev_randao: B256,
    pub withdrawals: Withdrawals,
    pub parent_beacon_block_root: Option<B256>,
}

impl BscPayloadBuilderAttributes {
    pub fn payload_id(&self) -> PayloadId {
        self.id
    }

    pub fn parent(&self) -> B256 {
        self.parent
    }

    pub fn timestamp(&self) -> u64 {
        self.timestamp
    }

    pub fn suggested_fee_recipient(&self) -> Address {
        self.suggested_fee_recipient
    }

    pub fn prev_randao(&self) -> B256 {
        self.prev_randao
    }

    pub fn parent_beacon_block_root(&self) -> Option<B256> {
        self.parent_beacon_block_root
    }

    pub fn withdrawals(&self) -> &Withdrawals {
        &self.withdrawals
    }
}
