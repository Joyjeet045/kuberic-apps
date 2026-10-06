use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use kuberic_runtime::application::{OpenContext, RoleChange, StatefulServiceReplica};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::{AccessStatus, ReplicaRole};
use kuberic_runtime::replicator::stream::{OperationMetadata, OperationStream};
use kuberic_runtime::replicator::{
    DefaultReplicatorFactory, Replicator, ReplicatorSettings, StateReplicator,
    StatefulServicePartition,
};
use kuberic_runtime::{Result, RuntimeError};
use tokio::sync::RwLock;

use crate::state::{PageState, PageStateProvider};

pub struct PageService {
    state: Arc<PageState>,
    provider: Arc<PageStateProvider>,
    replication_address: String,
    service_address: String,
    state_replicator: RwLock<Option<Arc<dyn StateReplicator>>>,
    partition: RwLock<Option<StatefulServicePartition>>,
    role: Mutex<ReplicaRole>,
}

impl PageService {
    pub fn new(replication_address: impl Into<String>, service_address: impl Into<String>) -> Self {
        let state = Arc::new(PageState::default());
        Self {
            provider: Arc::new(PageStateProvider::new(state.clone())),
            state,
            replication_address: replication_address.into(),
            service_address: service_address.into(),
            state_replicator: RwLock::new(None),
            partition: RwLock::new(None),
            role: Mutex::new(ReplicaRole::None),
        }
    }

    pub async fn replace_page(&self, page: Bytes) -> Result<i64> {
        self.state_replicator
            .read()
            .await
            .as_ref()
            .ok_or(RuntimeError::NotOpen)?
            .replicate(page)
            .await
    }

    pub async fn page(&self) -> Result<Option<Bytes>> {
        let partition = self
            .partition
            .read()
            .await
            .clone()
            .ok_or(RuntimeError::NotOpen)?;
        let status = partition.get_read_status().await?;
        if status != AccessStatus::Granted {
            return Err(RuntimeError::ReadClosed(status));
        }
        Ok(self.state.page())
    }
}

#[async_trait]
impl StatefulServiceReplica for PageService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        *self.partition.write().await = Some(context.partition.clone());
        let interfaces = context
            .partition
            .with_factory(Arc::new(DefaultReplicatorFactory::new(self.state.clone())))
            .create_replicator(
                Some(self.provider.clone()),
                Some(ReplicatorSettings {
                    replication_address: self.replication_address.clone(),
                }),
            )
            .await?;
        let state_replicator = interfaces
            .state_replicator()
            .expect("default replicator exposes operation and copy streams");
        let replication = state_replicator.get_replication_stream().await?;
        let copy = state_replicator.get_copy_stream().await?;
        tokio::spawn(consume_stream(self.state.clone(), replication));
        tokio::spawn(consume_stream(self.state.clone(), copy));
        *self.state_replicator.write().await = Some(state_replicator);
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        *self.role.lock().unwrap() = role;
        Ok(RoleChange {
            service_address: (role == ReplicaRole::Primary).then(|| self.service_address.clone()),
        })
    }

    async fn close(&self) -> Result<()> {
        self.state_replicator.write().await.take();
        self.partition.write().await.take();
        *self.role.lock().unwrap() = ReplicaRole::None;
        Ok(())
    }

    fn abort(&self) {
        if let Ok(mut state_replicator) = self.state_replicator.try_write() {
            state_replicator.take();
        }
        if let Ok(mut partition) = self.partition.try_write() {
            partition.take();
        }
        if let Ok(mut role) = self.role.lock() {
            *role = ReplicaRole::None;
        }
    }
}

async fn consume_stream(state: Arc<PageState>, mut stream: OperationStream) {
    while let Ok(Some(operation)) = stream.get_operation().await {
        let result = match &operation.metadata {
            OperationMetadata::Replication { lsn, committed_lsn } => {
                state
                    .apply(kuberic_runtime::application::Operation {
                        lsn: *lsn,
                        committed_lsn: *committed_lsn,
                        data: operation.data.clone(),
                    })
                    .await
            }
            OperationMetadata::Copy { build_id, sequence } => {
                match state
                    .apply_copy_chunk(
                        build_id,
                        *sequence,
                        kuberic_runtime::application::CopyChunk {
                            data: operation.data.clone(),
                        },
                    )
                    .await
                {
                    Ok(()) => state.durable_progress().await,
                    Err(error) => Err(error),
                }
            }
            OperationMetadata::CopyComplete {
                build_id,
                up_to_lsn,
                committed_lsn,
            } => {
                state
                    .finish_copy(build_id, *up_to_lsn, *committed_lsn)
                    .await
            }
        };
        match result {
            Ok(progress) => {
                let _ = operation.acknowledge(progress);
            }
            Err(error) => {
                let _ = operation.reject(error);
            }
        }
    }
}
