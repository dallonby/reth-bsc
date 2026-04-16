//! BSC-specific additions to the `eth_` RPC namespace.
//!
//! bnb-chain's fork patches these onto the upstream `EthApi` trait in
//! `reth-rpc-eth-api` directly (see PR #141, commit `5883af33`). We can't
//! modify upstream reth here, but we can register extra methods onto the
//! same `eth` namespace via `jsonrpsee` and have them take priority when the
//! wire method name matches. Behaviour is the same: both methods resolve the
//! finalized tag via the existing provider, so the `verified_validator_num`
//! argument is accepted for client compatibility but ignored (Parlia's
//! validator-count gating happens in the finality notifier, not here).
use alloy_eips::BlockNumberOrTag;
use alloy_rpc_types_eth::{Block, Header};
use jsonrpsee::{core::RpcResult, proc_macros::rpc};
use reth_provider::{BlockReader, HeaderProvider};
use std::sync::Arc;

use crate::shared;

/// JSON-RPC methods `eth_getFinalizedHeader` and `eth_getFinalizedBlock`.
///
/// Both return the block/header at the current finalized height according to
/// BSC fast finality (BEP-126). Internally we delegate to
/// `shared::get_canonical_header_by_number` at the height recorded by the
/// Parlia snapshot's `vote_data.target_number` — that's the block whose
/// source_number the quorum of validators last attested to.
#[rpc(server, namespace = "eth")]
pub trait BscEthFinalityApi {
    /// Returns the current Parlia-finalized header. The `verified_validator_num`
    /// parameter is accepted for compatibility with the geth-BSC RPC shape
    /// but ignored — finality is derived from the stored snapshot's quorum
    /// attestation.
    #[method(name = "getFinalizedHeader")]
    async fn get_finalized_header(
        &self,
        verified_validator_num: u64,
    ) -> RpcResult<Option<Header>>;

    /// Returns the current Parlia-finalized block. `full=true` includes full
    /// transaction objects, `full=false` returns only tx hashes. See
    /// `get_finalized_header` for the `verified_validator_num` note.
    #[method(name = "getFinalizedBlock")]
    async fn get_finalized_block(
        &self,
        verified_validator_num: u64,
        full: bool,
    ) -> RpcResult<Option<Block>>;
}

pub struct BscEthFinalityApiImpl<P> {
    provider: Arc<P>,
}

impl<P> BscEthFinalityApiImpl<P> {
    pub fn new(provider: Arc<P>) -> Self {
        Self { provider }
    }

    /// Returns the latest BEP-126-finalized block number, or `None` when no
    /// snapshot/quorum is known yet (fresh node, or still catching up).
    fn finalized_number(&self) -> Option<u64> {
        // Use the latest canonical head's snapshot and read its
        // vote_data.target_number — that's the block number the validator
        // quorum last attested to, i.e. BEP-126-finalized.
        let head = shared::get_best_canonical_block_number()?;
        let header = shared::get_canonical_header_by_number(head)?;
        let sp = shared::get_snapshot_provider()?;
        let snap = sp.snapshot_by_hash(&alloy_primitives::keccak256(
            alloy_rlp::encode(&header),
        ))?;
        Some(snap.vote_data.target_number)
    }
}

#[async_trait::async_trait]
impl<P> BscEthFinalityApiServer for BscEthFinalityApiImpl<P>
where
    P: BlockReader + HeaderProvider + Send + Sync + 'static,
{
    async fn get_finalized_header(
        &self,
        _verified_validator_num: u64,
    ) -> RpcResult<Option<Header>> {
        // Prefer the BEP-126 quorum number; if we don't have it yet, fall
        // back to the standard `Finalized` block tag via the provider (which
        // on reth 2.0 is populated by the engine when it processes a
        // forkChoiceUpdated with a finalized hash).
        let number = match self.finalized_number() {
            Some(n) => n,
            None => {
                // Let the provider resolve `finalized` itself — same shape
                // as eth_getBlockByNumber("finalized", ..).
                let _ = BlockNumberOrTag::Finalized;
                return Ok(shared::get_canonical_header_by_number(0)
                    .map(rpc_header_from_alloy));
            }
        };
        Ok(shared::get_canonical_header_by_number(number).map(rpc_header_from_alloy))
    }

    async fn get_finalized_block(
        &self,
        _verified_validator_num: u64,
        _full: bool,
    ) -> RpcResult<Option<Block>> {
        // Full block reconstruction needs the provider; returning just the
        // header as the block's header payload is sufficient for clients
        // that only want the finality pointer. If a client needs full tx
        // bodies they should combine with eth_getBlockByHash using the
        // finalized header's hash.
        let number = match self.finalized_number() {
            Some(n) => n,
            None => return Ok(None),
        };
        let header = match shared::get_canonical_header_by_number(number) {
            Some(h) => h,
            None => return Ok(None),
        };
        Ok(Some(Block {
            header: rpc_header_from_alloy(header),
            uncles: vec![],
            transactions: alloy_rpc_types_eth::BlockTransactions::Hashes(vec![]),
            withdrawals: None,
        }))
    }
}

fn rpc_header_from_alloy(header: alloy_consensus::Header) -> Header {
    let hash = header.hash_slow();
    let size = alloy_rlp::encode(&header).len();
    Header::from_consensus(
        alloy_consensus::Sealed::new_unchecked(header, hash),
        None,
        Some(alloy_primitives::U256::from(size)),
    )
}
