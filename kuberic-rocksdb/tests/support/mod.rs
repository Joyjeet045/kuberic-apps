use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kuberic_rocksdb::{BatchRequest, Mutation, RocksService, RocksState};
use kuberic_runtime::application::OpenMode;
use kuberic_runtime::protocol::types::*;
use kuberic_runtime::testing::authority::AdmittedAuthority;
use kuberic_runtime::testing::copy::{BuildConfiguration, PrepareCopyRequest};
use kuberic_runtime::testing::effects::{RuntimeEffect, RuntimeEffectAction};
use kuberic_runtime::testing::hosting::PodRuntime;
use kuberic_runtime::testing::runtime_adapter::RuntimeAdapter;
use kuberic_runtime::testing::session::ProcessSession;
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use kuberic_runtime::testing::{InProcessTransport, Message, TransportError, TransportEvent};

pub fn run<F: Future<Output = ()>>(scenario: impl FnOnce() -> F + Send + 'static) {
    std::thread::Builder::new()
        .stack_size(8 * 1024 * 1024)
        .spawn(move || {
            tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .thread_stack_size(8 * 1024 * 1024)
                .enable_all()
                .build()
                .unwrap()
                .block_on(async {
                    tokio::time::timeout(Duration::from_secs(60), scenario())
                        .await
                        .expect("scenario timed out");
                });
        })
        .unwrap()
        .join()
        .unwrap();
}

pub fn request(key: &[u8], value: &[u8]) -> BatchRequest {
    BatchRequest {
        column_family: "default".into(),
        operations: vec![Mutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        }],
    }
}

pub struct Pod {
    pub runtime: Arc<PodRuntime>,
    pub application: Arc<RocksService>,
    pub state: Arc<RocksState>,
    pub store: Arc<SqliteStore>,
    pub identity: ReplicaIdentity,
    pub session: ProcessSession,
    root: PathBuf,
    replicas: u32,
}

impl Pod {
    pub async fn new(id: i64, root: PathBuf, replicas: u32) -> Self {
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("rocks-{id}")),
            agent_generation: AgentGeneration::new(format!("rocks-generation-{id}")),
        };
        let metadata = SqliteStore::metadata_database_path(&root);
        let store = Arc::new(if metadata.exists() {
            SqliteStore::open_existing(metadata, None).unwrap()
        } else {
            SqliteStore::create_authorized(
                metadata,
                AgentState::new(StorageIdentity {
                    schema_version: SCHEMA_VERSION,
                    resource_uid: ResourceUid::new("rocks-test"),
                    pod_uid: PodUid::new(identity.instance_id.as_str()),
                    pvc_uid: PvcUid::new(format!("rocks-pvc-{id}")),
                    initialization_id: InitializationId::new(format!("rocks-init-{id}")),
                    local_identity: identity.clone(),
                    effective_policy: EffectivePolicy::fixed(replicas, 30).unwrap(),
                }),
            )
            .unwrap()
        });
        let state = Arc::new(RocksState::deferred(root.join("application")));
        let application = Arc::new(RocksService::new(
            state.clone(),
            format!("in-process://rocks-{id}"),
            format!("http://rocks-{id}:8080"),
        ));
        let runtime = Arc::new(PodRuntime::new(
            identity.clone(),
            application.clone(),
            store.clone(),
        ));
        Self {
            runtime,
            application,
            state,
            store,
            identity,
            session: ProcessSession::new(),
            root,
            replicas,
        }
    }

    pub async fn effect(&self, action: RuntimeEffectAction) {
        self.try_effect(action).await.unwrap();
    }

    pub async fn try_effect(
        &self,
        action: RuntimeEffectAction,
    ) -> kuberic_runtime::host::Result<()> {
        let sequence = self.store.load_state().await?.next_effect_sequence;
        RuntimeAdapter::new(self.store.clone(), self.runtime.clone())
            .execute(RuntimeEffect {
                operation_id: OperationId::new(format!("rocks-effect-{sequence}")),
                sequence,
                action,
            })
            .await?;
        Ok(())
    }

    pub async fn grant(&self) {
        self.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await;
    }

    pub async fn shutdown(&self) {
        self.runtime.abort();
        self.state.close().await.unwrap();
    }

    pub async fn restart(self) -> Self {
        self.shutdown().await;
        let root = self.root.clone();
        let id = self.identity.replica_id.value();
        let replicas = self.replicas;
        drop(self);
        let pod = Self::new(id, root, replicas).await;
        let state = pod.store.load_state().await.unwrap();
        pod.runtime
            .reconstruct(
                OpenMode::Existing,
                state.role,
                state.read_status,
                state.write_status,
                None,
            )
            .await
            .unwrap();
        pod
    }
}

impl Drop for Pod {
    fn drop(&mut self) {
        self.runtime.abort();
    }
}

pub fn configuration(pods: &[&Pod], primary: &Pod, epoch: i64) -> ConfigurationDescriptor {
    ConfigurationDescriptor::new(
        Epoch::new(0, epoch),
        primary.identity.replica_id,
        pods.iter()
            .map(|pod| ConfigurationMember {
                identity: pod.identity.clone(),
                role: if pod.identity == primary.identity {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        u32::try_from(pods.len()).unwrap() / 2 + 1,
    )
}

pub fn authority(pod: &Pod, current: &ConfigurationDescriptor) -> AdmittedAuthority {
    AdmittedAuthority {
        local_identity: pod.identity.clone(),
        current_configuration: current.clone(),
        previous_configuration: None,
        transition_kind: None,
        switchover_handoff: None,
        secondary_removal: None,
        scale_up: None,
    }
}

pub async fn bootstrap(pods: &[&Pod]) -> ConfigurationDescriptor {
    let current = configuration(pods, pods[0], 1);
    for pod in pods {
        pod.effect(RuntimeEffectAction::Open(OpenMode::Existing))
            .await;
    }
    pods[0]
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
        .await;
    let mut builds = Vec::new();
    for target in &pods[1..] {
        target
            .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary))
            .await;
        let id = OperationId::new(format!("bootstrap-{}", target.identity.replica_id));
        let build = pods[0]
            .runtime
            .authorize_build(
                id.clone(),
                target.identity.clone(),
                BuildConfiguration::Bootstrap(current.clone()),
            )
            .await
            .unwrap();
        target
            .effect(RuntimeEffectAction::AdmitBuildAuthority(Box::new(build)))
            .await;
        let mut prepared = pods[0]
            .runtime
            .data_plane()
            .prepare_copy(PrepareCopyRequest {
                build_id: id.clone(),
                target: target.identity.clone(),
                configuration: BuildConfiguration::Bootstrap(current.clone()),
                copy_context: Box::pin(futures::stream::empty()),
            })
            .await
            .unwrap();
        let mut transport = InProcessTransport::new();
        for pod in [pods[0], *target] {
            transport
                .register(pod.runtime.clone(), pod.session.id().clone())
                .await
                .unwrap();
        }
        loop {
            let item = prepared.items.next().await.unwrap().unwrap();
            let final_item = item.final_item;
            let message = transport.bind(Message::Copy(item)).unwrap();
            let delivery = transport.enqueue(message).unwrap();
            tokio::time::timeout(Duration::from_secs(15), async {
                loop {
                    let events = transport.next().await.events;
                    if events.iter().any(|event| matches!(event, TransportEvent::Copied { delivery: id, .. } if *id == delivery)) {
                        return;
                    }
                    assert!(!events.iter().any(|event| matches!(event, TransportEvent::Rejected { .. })), "{events:?}");
                }
            }).await.expect("durable checkpoint copy ACK");
            if final_item {
                break;
            }
        }
        builds.push((*target, id));
    }
    for pod in pods {
        let mut admitted = authority(pod, &current);
        admitted.transition_kind = Some(TransitionKind::Bootstrap);
        pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(admitted)))
            .await;
        pod.effect(RuntimeEffectAction::ChangeRole(
            if pod.identity == pods[0].identity {
                ReplicaRole::Primary
            } else {
                ReplicaRole::ActiveSecondary
            },
        ))
        .await;
    }
    for (target, id) in builds {
        pods[0]
            .effect(RuntimeEffectAction::RetireBuild(id.clone()))
            .await;
        target.effect(RuntimeEffectAction::RetireBuild(id)).await;
    }
    pods[0].grant().await;
    current
}

pub struct Routes(tokio::task::JoinHandle<()>);

impl Routes {
    pub async fn stop(mut self) {
        self.0.abort();
        match (&mut self.0).await {
            Ok(()) => {}
            Err(error) if error.is_cancelled() => {}
            Err(error) => panic!("transport failed: {error}"),
        }
    }
}

impl Drop for Routes {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub async fn route(pods: &[&Pod]) -> Routes {
    let mut transport = InProcessTransport::new();
    for pod in pods {
        transport
            .register(pod.runtime.clone(), pod.session.id().clone())
            .await
            .unwrap();
    }
    Routes(tokio::spawn(async move {
        loop {
            for event in transport.next().await.events {
                if let TransportEvent::Rejected { error, .. } = event {
                    assert!(
                        matches!(error, TransportError::Unregistered(_)),
                        "transport rejected operation: {error}"
                    );
                }
            }
        }
    }))
}

pub async fn wait_applied(pods: &[&Pod], lsn: i64) {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let mut ready = true;
            for pod in pods {
                ready &= pod
                    .runtime
                    .snapshot()
                    .await
                    .verified_replication_lsn
                    .is_some_and(|value| value >= lsn);
            }
            if ready {
                return;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("durable secondary ACK");
}

pub async fn build_after_write(
    source: &Pod,
    target: &Pod,
    boundary: i64,
    peers: &[&Pod],
    routes: Routes,
) {
    target
        .effect(RuntimeEffectAction::Open(OpenMode::Existing))
        .await;
    target
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary))
        .await;
    let id = OperationId::new("live-checkpoint-build");
    let build = source
        .runtime
        .authorize_build(
            id.clone(),
            target.identity.clone(),
            BuildConfiguration::Current,
        )
        .await
        .unwrap();
    assert_eq!(build.replication_boundary_lsn, boundary);
    target
        .effect(RuntimeEffectAction::AdmitBuildAuthority(Box::new(build)))
        .await;
    let later = source
        .application
        .write(request(b"after-snapshot", b"catch-up"))
        .await
        .unwrap();
    wait_applied(peers, later).await;
    routes.stop().await;
    let mut prepared = source
        .runtime
        .data_plane()
        .prepare_copy(PrepareCopyRequest {
            build_id: id.clone(),
            target: target.identity.clone(),
            configuration: BuildConfiguration::Current,
            copy_context: Box::pin(futures::stream::empty()),
        })
        .await
        .unwrap();
    let mut transport = InProcessTransport::new();
    for pod in [source, target] {
        transport
            .register(pod.runtime.clone(), pod.session.id().clone())
            .await
            .unwrap();
    }
    loop {
        let item = tokio::time::timeout(Duration::from_secs(10), prepared.items.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let done = !item.snapshot_chunk && item.lsn >= later;
        let message = transport.bind(Message::Copy(item)).unwrap();
        let delivery = transport.enqueue(message).unwrap();
        loop {
            let events = transport.next().await.events;
            assert!(
                !events
                    .iter()
                    .any(|event| matches!(event, TransportEvent::Rejected { .. })),
                "{events:?}"
            );
            if events.iter().any(|event| matches!(event, TransportEvent::Copied { delivery: id, .. } if *id == delivery)) {
                break;
            }
        }
        if done {
            break;
        }
    }
    assert_eq!(
        target
            .state
            .get(b"before-snapshot".to_vec())
            .await
            .unwrap()
            .unwrap(),
        b"checkpoint"
    );
    assert!(
        target
            .application
            .get(b"before-snapshot".to_vec())
            .await
            .is_err()
    );
    use kuberic_runtime::engine::DurableState;
    assert_eq!(
        target.state.durable_progress().await.unwrap().applied_lsn,
        later
    );
    let retained = target
        .state
        .get_replication_operations(later, later)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    let original = source
        .state
        .get_replication_operations(later, later)
        .await
        .unwrap()
        .next()
        .await
        .unwrap()
        .unwrap();
    assert_eq!(retained, original);
    source.runtime.cancel_outbound_build(&id).await.unwrap();
}

pub async fn change_primary(
    pods: &[&Pod],
    previous: &ConfigurationDescriptor,
    target: &Pod,
    planned: bool,
    boundary: i64,
) -> (ConfigurationDescriptor, Routes) {
    let source = *pods
        .iter()
        .find(|pod| pod.identity.replica_id == previous.primary_id)
        .unwrap();
    let handoff = if planned {
        source
            .effect(RuntimeEffectAction::PrepareSwitchover {
                preparation_generation: u64::try_from(previous.epoch.configuration_number + 1)
                    .unwrap(),
                request_id: SwitchoverRequestId::new("rocks-handoff"),
                source: source.identity.clone(),
                target: target.identity.clone(),
                starting_configuration_id: previous.configuration_id.clone(),
                starting_epoch: previous.epoch,
            })
            .await;
        let handoff = source
            .store
            .load_state()
            .await
            .unwrap()
            .prepared_switchover
            .unwrap();
        assert_eq!(handoff.handoff_lsn, boundary);
        Some(handoff)
    } else {
        source.runtime.abort();
        None
    };
    assert!(
        source
            .application
            .write(request(b"stale", b"forbidden"))
            .await
            .is_err()
    );
    let current = configuration(pods, target, previous.epoch.configuration_number + 1);
    let survivors = pods
        .iter()
        .copied()
        .filter(|pod| planned || pod.identity != source.identity)
        .collect::<Vec<_>>();
    for pod in &survivors {
        let mut admitted = authority(pod, &current);
        admitted.previous_configuration = Some(previous.clone());
        admitted.transition_kind = Some(if planned {
            TransitionKind::PlannedSwitchover
        } else {
            TransitionKind::Failover
        });
        admitted.switchover_handoff = handoff.clone();
        pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(admitted)))
            .await;
        if !planned {
            pod.effect(RuntimeEffectAction::AuthorizeFailoverPrefix(boundary))
                .await;
        }
        if pod.identity != source.identity {
            pod.effect(RuntimeEffectAction::ChangeRole(
                if pod.identity == target.identity {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            ))
            .await;
        }
    }
    let peers = survivors
        .iter()
        .copied()
        .filter(|pod| pod.identity != source.identity)
        .collect::<Vec<_>>();
    let routes = route(&peers).await;
    for peer in peers.iter().filter(|pod| pod.identity != target.identity) {
        target
            .runtime
            .repair_peer(peer.identity.clone(), boundary.saturating_sub(1))
            .await
            .unwrap();
    }
    target.effect(RuntimeEffectAction::WaitForCatchup).await;
    target.grant().await;
    assert!(
        source
            .application
            .write(request(b"stale", b"forbidden"))
            .await
            .is_err()
    );
    if planned {
        assert_eq!(source.runtime.snapshot().await.role, ReplicaRole::Primary);
        source
            .effect(RuntimeEffectAction::ChangeRole(
                ReplicaRole::ActiveSecondary,
            ))
            .await;
    }
    routes.stop().await;
    let routes = route(&survivors).await;
    for peer in survivors
        .iter()
        .filter(|pod| pod.identity != target.identity)
    {
        target
            .runtime
            .repair_peer(peer.identity.clone(), boundary.saturating_sub(1))
            .await
            .unwrap();
    }
    target.effect(RuntimeEffectAction::WaitForCatchup).await;
    for pod in survivors {
        let mut admitted = authority(pod, &current);
        admitted.switchover_handoff = handoff.clone();
        pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(admitted)))
            .await;
    }
    target.grant().await;
    (current, routes)
}
