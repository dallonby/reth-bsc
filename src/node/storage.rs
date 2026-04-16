use crate::{
    consensus::parlia::db::{BscBlobSidecars, StoredBlobSidecars},
    BscBlock, BscBlockBody, BscPrimitives,
};
use reth_chainspec::EthereumHardforks;
use reth_db::cursor::{DbCursorRO, DbCursorRW};
use reth_db::transaction::{DbTx, DbTxMut};
use reth_provider::{
    providers::{ChainStorage, NodeTypesForProvider},
    BlockBodyReader, BlockBodyWriter, ChainSpecProvider, ChainStorageReader, ChainStorageWriter,
    DBProvider, DatabaseProvider, EthStorage, ProviderResult, ReadBodyInput,
};

#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct BscStorage(EthStorage);

impl<Provider> BlockBodyWriter<Provider, BscBlockBody> for BscStorage
where
    Provider: DBProvider<Tx: DbTxMut>,
{
    fn write_block_bodies(
        &self,
        provider: &Provider,
        bodies: Vec<(u64, Option<&BscBlockBody>)>,
    ) -> ProviderResult<()> {
        // Collect sidecars alongside the eth-body tuples so we can delegate
        // eth-body storage unchanged and persist sidecars separately.
        let mut sidecar_writes: Vec<(u64, StoredBlobSidecars)> = Vec::new();
        let eth_bodies: Vec<_> = bodies
            .into_iter()
            .map(|(block_number, body)| {
                if let Some(sidecars) = body.and_then(|b| b.sidecars.as_ref()) {
                    if !sidecars.is_empty() {
                        sidecar_writes
                            .push((block_number, StoredBlobSidecars(sidecars.clone())));
                    }
                }
                (block_number, body.map(|b| &b.inner))
            })
            .collect();
        self.0.write_block_bodies(provider, eth_bodies)?;

        if !sidecar_writes.is_empty() {
            let tx = provider.tx_ref();
            let mut cursor = tx.cursor_write::<BscBlobSidecars>()?;
            for (block_number, sidecars) in sidecar_writes {
                // Insert via cursor so callers that write bodies sequentially
                // (staged sync) get the MDBX fast-path; falls back to `put` on
                // non-monotonic block numbers.
                cursor.upsert(block_number, &sidecars)?;
            }
        }

        Ok(())
    }

    fn remove_block_bodies_above(
        &self,
        provider: &Provider,
        block: u64,
    ) -> ProviderResult<()> {
        self.0.remove_block_bodies_above(provider, block)?;

        // Walk sidecars past `block` and delete them. Using a cursor seek so
        // we only touch the range in question, not scan the whole table.
        let tx = provider.tx_ref();
        let mut cursor = tx.cursor_write::<BscBlobSidecars>()?;
        // Start just past `block`. `seek` positions at the first key >= target.
        let mut entry = cursor.seek(block.saturating_add(1))?;
        while let Some((_n, _)) = entry {
            cursor.delete_current()?;
            entry = cursor.next()?;
        }

        Ok(())
    }
}

impl<Provider> BlockBodyReader<Provider> for BscStorage
where
    Provider: DBProvider + ChainSpecProvider<ChainSpec: EthereumHardforks>,
{
    type Block = BscBlock;

    fn read_block_bodies(
        &self,
        provider: &Provider,
        inputs: Vec<ReadBodyInput<'_, Self::Block>>,
    ) -> ProviderResult<Vec<BscBlockBody>> {
        // Capture the block numbers before handing `inputs` to the eth reader
        // (it consumes them). `ReadBodyInput` is (&Header, Vec<TxSigned>); we
        // read `header.number` up front.
        let block_numbers: Vec<u64> =
            inputs.iter().map(|(header, _)| header.number).collect();
        let eth_bodies = self.0.read_block_bodies(provider, inputs)?;

        let tx = provider.tx_ref();
        Ok(eth_bodies
            .into_iter()
            .zip(block_numbers)
            .map(|(inner, n)| {
                let sidecars = tx
                    .get::<BscBlobSidecars>(n)
                    .ok()
                    .flatten()
                    .map(|s| s.0);
                BscBlockBody { inner, sidecars }
            })
            .collect())
    }
}

impl ChainStorage<BscPrimitives> for BscStorage {
    fn reader<TX, Types>(
        &self,
    ) -> impl ChainStorageReader<DatabaseProvider<TX, Types>, BscPrimitives>
    where
        TX: DbTx + 'static,
        Types: NodeTypesForProvider<Primitives = BscPrimitives>,
    {
        self
    }

    fn writer<TX, Types>(
        &self,
    ) -> impl ChainStorageWriter<DatabaseProvider<TX, Types>, BscPrimitives>
    where
        TX: DbTxMut + DbTx + 'static,
        Types: NodeTypesForProvider<Primitives = BscPrimitives>,
    {
        self
    }
}
