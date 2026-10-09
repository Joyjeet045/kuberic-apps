use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use bytes::Bytes;
use kuberic_runtime::application::{
    CopyChunk, OpenContext, Operation, RoleChange, StatefulServiceReplica,
};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::{AccessStatus, FaultType, ReplicaRole};
use kuberic_runtime::replicator::{
    DefaultReplicatorFactory, Replicator, ReplicatorSettings, StateReplicator,
    StatefulServicePartition,
    stream::{OperationMetadata, OperationStream},
};
use kuberic_runtime::{Result, RuntimeError};

use crate::{BatchRequest, RocksState, batch::Envelope};

pub struct RocksService {
    state: Arc<RocksState>,
    replication_address: String,
    service_address: String,
    partition: Mutex<Option<StatefulServicePartition>>,
    replicator: Mutex<Option<Arc<dyn StateReplicator>>>,
    primary: Arc<AtomicBool>,
    failed: Arc<AtomicBool>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}

impl RocksService {
    pub fn new(
        state: Arc<RocksState>,
        replication_address: String,
        service_address: String,
    ) -> Self {
        Self {
            state,
            replication_address,
            service_address,
            partition: Mutex::new(None),
            replicator: Mutex::new(None),
            primary: Arc::new(AtomicBool::new(false)),
            failed: Arc::new(AtomicBool::new(false)),
            tasks: Mutex::new(Vec::new()),
        }
    }

    pub fn state(&self) -> &Arc<RocksState> {
        &self.state
    }

    fn partition(&self) -> Result<StatefulServicePartition> {
        if self.failed.load(Ordering::SeqCst) {
            return Err(RuntimeError::Application(
                "replica storage is fenced; restart or rebuild required".into(),
            ));
        }
        self.partition
            .lock()
            .expect("partition mutex poisoned")
            .clone()
            .ok_or(RuntimeError::NotOpen)
    }

    pub async fn write(&self, request: BatchRequest) -> Result<i64> {
        let bytes = Envelope::encode(request)
            .map_err(|error| RuntimeError::Application(error.to_string()))?;
        self.submit(bytes.into()).await
    }

    pub(crate) async fn submit(&self, bytes: Bytes) -> Result<i64> {
        let partition = self.partition()?;
        if !self.primary.load(Ordering::SeqCst) {
            return Err(RuntimeError::NotPrimary);
        }
        let status = partition.get_write_status().await?;
        if status != AccessStatus::Granted {
            return Err(RuntimeError::WriteClosed(status));
        }
        let replicator = self
            .replicator
            .lock()
            .expect("replicator mutex poisoned")
            .clone()
            .ok_or(RuntimeError::NotOpen)?;
        let lsn = replicator.replicate(bytes).await?;
        let progress = self.state.durable_progress().await?;
        if progress.committed_lsn < lsn {
            self.failed.store(true, Ordering::SeqCst);
            partition.report_fault(FaultType::Permanent).await?;
            return Err(RuntimeError::Application(
                "replication completed without durable application commitment".into(),
            ));
        }
        Ok(lsn)
    }

    pub async fn get(&self, key: Vec<u8>) -> Result<Option<Vec<u8>>> {
        let partition = self.partition()?;
        if !self.primary.load(Ordering::SeqCst) {
            return Err(RuntimeError::NotPrimary);
        }
        let status = partition.get_read_status().await?;
        if status != AccessStatus::Granted {
            return Err(RuntimeError::ReadClosed(status));
        }
        let value = self.state.get(key).await?;
        let status = partition.get_read_status().await?;
        if status != AccessStatus::Granted {
            return Err(RuntimeError::ReadClosed(status));
        }
        if !self.primary.load(Ordering::SeqCst) || self.failed.load(Ordering::SeqCst) {
            return Err(RuntimeError::NotPrimary);
        }
        Ok(value)
    }
}

#[async_trait]
impl StatefulServiceReplica for RocksService {
    async fn open(self: Arc<Self>, context: OpenContext) -> Result<Arc<dyn Replicator>> {
        self.state.initialize().await?;
        self.failed.store(false, Ordering::SeqCst);
        self.primary.store(false, Ordering::SeqCst);
        let partition = context.partition;
        *self.partition.lock().expect("partition mutex poisoned") = Some(partition.clone());
        let interfaces = partition
            .with_factory(Arc::new(DefaultReplicatorFactory::new(self.state.clone())))
            .create_replicator(
                Some(self.state.clone()),
                Some(ReplicatorSettings {
                    replication_address: self.replication_address.clone(),
                }),
            )
            .await?;
        let replicator = interfaces.state_replicator().ok_or_else(|| {
            RuntimeError::Application("default replicator has no state capability".into())
        })?;
        for stream in [
            replicator.get_copy_stream().await?,
            replicator.get_replication_stream().await?,
        ] {
            let task = tokio::spawn(consume(
                self.state.clone(),
                stream,
                partition.clone(),
                self.failed.clone(),
            ));
            self.tasks.lock().expect("task mutex poisoned").push(task);
        }
        *self.replicator.lock().expect("replicator mutex poisoned") = Some(replicator);
        Ok(interfaces.replicator())
    }

    async fn change_role(&self, role: ReplicaRole) -> Result<RoleChange> {
        self.primary.store(false, Ordering::SeqCst);
        if role == ReplicaRole::Primary {
            self.partition()?;
            self.state.durable_progress().await?;
            self.primary.store(true, Ordering::SeqCst);
        }
        Ok(RoleChange {
            service_address: (role == ReplicaRole::Primary).then(|| self.service_address.clone()),
        })
    }

    async fn close(&self) -> Result<()> {
        self.abort();
        self.state.close().await
    }

    fn abort(&self) {
        self.primary.store(false, Ordering::SeqCst);
        self.partition
            .lock()
            .expect("partition mutex poisoned")
            .take();
        self.replicator
            .lock()
            .expect("replicator mutex poisoned")
            .take();
        for task in self.tasks.lock().expect("task mutex poisoned").drain(..) {
            task.abort();
        }
    }
}

async fn consume(
    state: Arc<RocksState>,
    mut stream: OperationStream,
    partition: StatefulServicePartition,
    failed: Arc<AtomicBool>,
) {
    loop {
        let operation = match stream.get_operation().await {
            Ok(Some(operation)) => operation,
            Ok(None) => return,
            Err(error) => {
                report_failure(&partition, &failed, &error).await;
                return;
            }
        };
        let result = match &operation.metadata {
            OperationMetadata::Replication { lsn, committed_lsn } => {
                state
                    .apply(Operation {
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
                        CopyChunk {
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
                if let Err(error) = operation.acknowledge(progress) {
                    tracing::debug!(%error, "durable operation completed after its receiver closed");
                }
            }
            Err(error) => {
                report_failure(&partition, &failed, &error).await;
                if let Err(error) = operation.reject(error) {
                    tracing::debug!(%error, "failed operation receiver already closed");
                }
                return;
            }
        }
    }
}

async fn report_failure(
    partition: &StatefulServicePartition,
    failed: &AtomicBool,
    error: &RuntimeError,
) {
    failed.store(true, Ordering::SeqCst);
    tracing::error!(%error, "RocksDB stream failed; replica access fenced");
    if let Err(error) = partition.report_fault(FaultType::Permanent).await {
        tracing::error!(%error, "failed to report RocksDB storage fault");
    }
}
