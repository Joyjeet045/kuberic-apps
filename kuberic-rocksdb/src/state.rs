use std::sync::Arc;

use async_trait::async_trait;
use futures::{StreamExt, stream};
use kuberic_runtime::application::{OperationDataStream, StateProvider};
use kuberic_runtime::protocol::types::Epoch;
use kuberic_runtime::{Result, RuntimeError};

use crate::persistence::RocksState;

pub struct RocksStateProvider {
    state: Arc<RocksState>,
}

impl RocksStateProvider {
    pub fn new(state: Arc<RocksState>) -> Self {
        Self { state }
    }

    pub fn state(&self) -> &Arc<RocksState> {
        &self.state
    }
}

#[async_trait]
impl StateProvider for RocksStateProvider {
    async fn update_epoch(&self, epoch: Epoch, previous_epoch_last_lsn: i64) -> Result<()> {
        self.state
            .update_epoch(epoch, previous_epoch_last_lsn)
            .await
    }

    async fn last_committed_lsn(&self) -> Result<i64> {
        self.state.committed_lsn().await
    }

    async fn get_copy_context(&self) -> Result<OperationDataStream> {
        Ok(Box::pin(stream::empty()))
    }

    async fn get_copy_state(
        &self,
        up_to_lsn: i64,
        mut copy_context: OperationDataStream,
    ) -> Result<OperationDataStream> {
        if copy_context.next().await.is_some() {
            return Err(RuntimeError::Application(
                "kuberic-rocksdb does not use a copy context".into(),
            ));
        }
        let chunks = self.state.copy_chunks(up_to_lsn).await?;
        Ok(Box::pin(stream::iter(chunks.into_iter().map(Ok))))
    }

    async fn on_data_loss(&self) -> Result<bool> {
        Ok(false)
    }
}
