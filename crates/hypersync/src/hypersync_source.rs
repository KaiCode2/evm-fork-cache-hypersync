use async_trait::async_trait;
use futures::StreamExt;
use hypersync_client::{Client, HeightStreamEvent, net_types::Query};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::{ChainDataSource, ChainHeightStream, SourceError, SourcePage};

/// Production chain-data boundary backed by Envio's native Rust client.
#[derive(Clone)]
pub struct HyperSyncDataSource {
    chain_id: u64,
    client: Client,
}

impl HyperSyncDataSource {
    /// Configure a HyperSync client for one chain.
    ///
    /// # Errors
    ///
    /// Returns [`SourceError::Request`] when the Envio client builder rejects
    /// the chain/token configuration.
    pub fn new(chain_id: u64, api_token: impl ToString) -> Result<Self, SourceError> {
        let client = Client::builder()
            .chain_id(chain_id)
            .api_token(api_token)
            .build()
            .map_err(|error| SourceError::request(error.to_string()))?;
        Ok(Self { chain_id, client })
    }

    /// Configured EVM chain id.
    pub fn chain_id(&self) -> u64 {
        self.chain_id
    }

    /// Subscribe to reconnecting HyperSync archive-height notifications.
    pub fn stream_height(&self) -> mpsc::Receiver<HeightStreamEvent> {
        self.client.stream_height()
    }
}

#[async_trait]
impl ChainDataSource for HyperSyncDataSource {
    async fn height(&self) -> Result<u64, SourceError> {
        self.client
            .get_height()
            .await
            .map_err(|error| SourceError::request(error.to_string()))
    }

    async fn query(&self, query: Query) -> Result<SourcePage, SourceError> {
        let response = self
            .client
            .get(&query)
            .await
            .map_err(|error| SourceError::request(error.to_string()))?;
        Ok(SourcePage::new(
            response.next_block,
            response.data.blocks.into_iter().flatten().collect(),
            response.data.logs.into_iter().flatten().collect(),
        )
        .with_archive_height(response.archive_height)
        .with_rollback_guard(response.rollback_guard))
    }

    fn height_stream(&self) -> Option<ChainHeightStream> {
        let updates = ReceiverStream::new(self.stream_height()).filter_map(|event| async move {
            match event {
                HeightStreamEvent::Height(height) => Some(height),
                HeightStreamEvent::Connected | HeightStreamEvent::Reconnecting { .. } => None,
            }
        });
        Some(Box::pin(updates))
    }
}
