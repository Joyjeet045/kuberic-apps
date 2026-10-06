use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use kuberic_runtime::application::{OpenContext, RoleChange, StatefulServiceReplica};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::ReplicaRole;
use kuberic_runtime::replicator::stream::{OperationMetadata, OperationStream};
use kuberic_runtime::replicator::{
    DefaultReplicatorFactory, Replicator, ReplicatorSettings, StateReplicator,
};
use kuberic_runtime::{Result, RuntimeError};
use tokio::sync::RwLock;

use crate::dictionary::StateManager;
use crate::state::{ReliableCollectionsProvider, ReliableCollectionsState};

pub struct ReliableCollectionsService {
    state: Arc<ReliableCollectionsState>,
    provider: Arc<ReliableCollectionsProvider>,
    manager: StateManager,
    replication_address: String,
    service_address: Option<String>,
    state_replicator: RwLock<Option<Arc<dyn StateReplicator>>>,
    streams: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    role: Mutex<ReplicaRole>,
}

impl ReliableCollectionsService {
    pub fn new(
        path: PathBuf,
        replication_address: impl Into<String>,
        service_address: Option<String>,
    ) -> crate::Result<Self> {
        let state = Arc::new(ReliableCollectionsState::open(path)?);
        Self::from_state(state, replication_address, service_address)
    }

    pub fn from_state(
        state: Arc<ReliableCollectionsState>,
        replication_address: impl Into<String>,
        service_address: Option<String>,
    ) -> crate::Result<Self> {
        let manager = StateManager::from_state(state.clone(), false)?;
        Ok(Self {
            provider: Arc::new(ReliableCollectionsProvider::new(state.clone())),
            state,
            manager,
            replication_address: replication_address.into(),
            service_address,
            state_replicator: RwLock::new(None),
            streams: Mutex::new(Vec::new()),
            role: Mutex::new(ReplicaRole::None),
        })
    }

    pub fn state_manager(&self) -> StateManager {
        self.manager.clone()
    }
}

#[async_trait]
impl StatefulServiceReplica for ReliableCollectionsService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
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
        let replication_task = tokio::spawn(consume_stream(self.state.clone(), replication));
        let copy_task = tokio::spawn(consume_stream(self.state.clone(), copy));
        self.manager
            .attach_runtime(context.partition, state_replicator.clone())
            .await;
        *self.state_replicator.write().await = Some(state_replicator);
        self.streams
            .lock()
            .map_err(|_| RuntimeError::Application("stream task mutex poisoned".into()))?
            .extend([replication_task, copy_task]);
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        *self
            .role
            .lock()
            .map_err(|_| RuntimeError::Application("role mutex poisoned".into()))? = role;
        Ok(RoleChange {
            service_address: (role == ReplicaRole::Primary)
                .then(|| self.service_address.clone())
                .flatten(),
        })
    }

    async fn close(&self) -> Result<()> {
        self.manager.detach_runtime().await;
        self.state_replicator.write().await.take();
        for task in self
            .streams
            .lock()
            .map_err(|_| RuntimeError::Application("stream task mutex poisoned".into()))?
            .drain(..)
        {
            task.abort();
        }
        *self
            .role
            .lock()
            .map_err(|_| RuntimeError::Application("role mutex poisoned".into()))? =
            ReplicaRole::None;
        Ok(())
    }

    fn abort(&self) {
        if let Ok(mut state_replicator) = self.state_replicator.try_write() {
            state_replicator.take();
        }
        if let Ok(mut streams) = self.streams.lock() {
            for task in streams.drain(..) {
                task.abort();
            }
        }
        if let Ok(mut role) = self.role.lock() {
            *role = ReplicaRole::None;
        }
        self.manager.try_detach_runtime();
    }
}

async fn consume_stream(state: Arc<ReliableCollectionsState>, mut stream: OperationStream) {
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
