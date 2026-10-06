use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures::StreamExt;
use kuberic_rocksdb::{Mutation, RocksService, RocksState};
use kuberic_runtime::application::OpenMode;
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::OperationId;
use kuberic_runtime::protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy,
    Epoch, InitializationId, PodUid, PvcUid, ReplicaId, ReplicaIdentity, ReplicaInstanceId,
    ReplicaRole, ResourceUid, TransitionKind,
};
use kuberic_runtime::testing::authority::AdmittedAuthority;
use kuberic_runtime::testing::copy::{BuildConfiguration, PrepareCopyRequest};
use kuberic_runtime::testing::effects::{RuntimeEffect, RuntimeEffectAction};
use kuberic_runtime::testing::hosting::PodRuntime;
use kuberic_runtime::testing::runtime_adapter::RuntimeAdapter;
use kuberic_runtime::testing::session::ProcessSession;
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{AgentState, SCHEMA_VERSION, StorageIdentity};
use kuberic_runtime::testing::{InProcessTransport, Message, TransportEvent};

#[test]
fn primary_replicates_batch_to_secondary() {
    std::thread::Builder::new()
        .name("kuberic-rocksdb-runtime-multireplica".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(multireplica_happy_path());
        })
        .unwrap()
        .join()
        .unwrap();
}

async fn multireplica_happy_path() {
    let directory = tempfile::tempdir().unwrap();
    let primary = RocksPod::new(1, directory.path().join("one"), 2).await;
    let secondary = RocksPod::new(2, directory.path().join("two"), 2).await;
    bootstrap(&[&primary, &secondary]).await;

    assert_eq!(
        secondary
            .state
            .durable_progress()
            .await
            .unwrap()
            .applied_lsn,
        0
    );
    assert_eq!(secondary.state.get(b"copied").await.unwrap(), None);

    let routes = route(&[&primary, &secondary]).await;
    let lsn = tokio::time::timeout(
        Duration::from_secs(5),
        primary.application.write(vec![
            Mutation::Put {
                key: b"account".to_vec(),
                value: b"100".to_vec(),
            },
            Mutation::Merge {
                key: b"account".to_vec(),
                value: b"+25".to_vec(),
            },
            Mutation::Put {
                key: b"transient".to_vec(),
                value: b"delete-me".to_vec(),
            },
            Mutation::Delete {
                key: b"transient".to_vec(),
            },
        ]),
    )
    .await
    .expect("quorum write should complete")
    .unwrap();
    wait_applied(&[&secondary], lsn).await;

    assert_eq!(
        primary.state.get(b"account").await.unwrap(),
        Some(Bytes::from_static(b"100+25"))
    );
    assert_eq!(primary.state.get(b"transient").await.unwrap(), None);
    assert_eq!(
        secondary.state.get(b"account").await.unwrap(),
        Some(Bytes::from_static(b"100+25"))
    );
    assert_eq!(secondary.state.get(b"transient").await.unwrap(), None);

    let primary_progress = primary.state.durable_progress().await.unwrap();
    let secondary_progress = secondary.state.durable_progress().await.unwrap();
    assert_eq!(primary_progress.applied_lsn, lsn);
    assert_eq!(primary_progress.committed_lsn, lsn);
    assert_eq!(secondary_progress.applied_lsn, lsn);
    assert!(secondary_progress.committed_lsn <= lsn);

    drop(routes);
    primary.runtime.abort();
    secondary.runtime.abort();
}

struct RocksPod {
    runtime: Arc<PodRuntime>,
    application: Arc<RocksService>,
    state: Arc<RocksState>,
    store: Arc<SqliteStore>,
    identity: ReplicaIdentity,
    session: ProcessSession,
}

impl RocksPod {
    async fn new(id: i64, root: PathBuf, replicas: u32) -> Self {
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("rocks-{id}")),
            agent_generation: AgentGeneration::new(format!("rocks-generation-{id}")),
        };
        let store = Arc::new(
            SqliteStore::create_authorized(
                SqliteStore::metadata_database_path(&root),
                AgentState::new(StorageIdentity {
                    schema_version: SCHEMA_VERSION,
                    resource_uid: ResourceUid::new("rocks-multireplica-test"),
                    pod_uid: PodUid::new(format!("rocks-pod-{id}")),
                    pvc_uid: PvcUid::new(format!("rocks-pvc-{id}")),
                    initialization_id: InitializationId::new(format!("rocks-init-{id}")),
                    local_identity: identity.clone(),
                    effective_policy: EffectivePolicy::fixed(replicas, 30).unwrap(),
                }),
            )
            .unwrap(),
        );
        let state = Arc::new(RocksState::open(root.join("application")).unwrap());
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
        }
    }

    async fn effect(&self, action: RuntimeEffectAction) {
        let state = self.store.load_state().await.unwrap();
        RuntimeAdapter::new(self.store.clone(), self.runtime.clone())
            .execute(RuntimeEffect {
                operation_id: OperationId::new(format!(
                    "rocks-multireplica-effect-{}",
                    state.next_effect_sequence
                )),
                sequence: state.next_effect_sequence,
                action,
            })
            .await
            .unwrap();
    }

    async fn open(&self) {
        self.effect(RuntimeEffectAction::Open(OpenMode::Existing))
            .await;
    }
}

impl Drop for RocksPod {
    fn drop(&mut self) {
        self.runtime.abort();
    }
}

struct Routes {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Routes {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn route(pods: &[&RocksPod]) -> Routes {
    let mut transport = InProcessTransport::new();
    for pod in pods {
        transport
            .register(pod.runtime.clone(), pod.session.id().clone())
            .await
            .unwrap();
    }
    let task = tokio::spawn(async move {
        loop {
            for event in transport.next().await.events {
                if let TransportEvent::Rejected { error, .. } = event {
                    panic!("in-process transport rejected RocksDB message: {error}");
                }
            }
        }
    });
    Routes { task }
}

async fn wait_applied(pods: &[&RocksPod], lsn: i64) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let mut applied = true;
            for pod in pods {
                applied &= pod
                    .runtime
                    .snapshot()
                    .await
                    .verified_replication_lsn
                    .is_some_and(|verified| verified >= lsn);
            }
            if applied {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("secondary durably acknowledged the operation");
}

async fn bootstrap(pods: &[&RocksPod]) {
    let members = pods
        .iter()
        .map(|pod| pod.identity.clone())
        .collect::<Vec<_>>();
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        pods[0].identity.replica_id,
        members
            .iter()
            .enumerate()
            .map(|(index, identity)| ConfigurationMember {
                identity: identity.clone(),
                role: if index == 0 {
                    ReplicaRole::Primary
                } else {
                    ReplicaRole::ActiveSecondary
                },
            })
            .collect(),
        2,
    );
    for pod in pods {
        pod.open().await;
    }
    pods[0]
        .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::Primary))
        .await;
    let mut builds = Vec::new();
    for target in &pods[1..] {
        target
            .effect(RuntimeEffectAction::ChangeRole(ReplicaRole::IdleSecondary))
            .await;
        let build_id =
            OperationId::new(format!("bootstrap-{}", target.identity.replica_id.value()));
        let build = pods[0]
            .runtime
            .authorize_build(
                build_id.clone(),
                target.identity.clone(),
                BuildConfiguration::Bootstrap(configuration.clone()),
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
                build_id: build_id.clone(),
                target: target.identity.clone(),
                configuration: BuildConfiguration::Bootstrap(configuration.clone()),
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
            let last = item.final_item;
            let message = transport.bind(Message::Copy(item)).unwrap();
            let delivery = transport.enqueue(message).unwrap();
            tokio::time::timeout(Duration::from_secs(5), async {
                loop {
                    let events = transport.next().await.events;
                    if events.iter().any(|event| {
                        matches!(event, TransportEvent::Copied { delivery: id, .. } if *id == delivery)
                    }) {
                        return;
                    }
                    for event in events {
                        match event {
                            TransportEvent::Copied { .. } => {}
                            other => panic!("unexpected bootstrap copy event: {other:?}"),
                        }
                    }
                }
            })
            .await
            .expect("copy item should be durably accepted");
            if last {
                break;
            }
        }
        builds.push((target, build_id));
    }
    for (index, pod) in pods.iter().enumerate() {
        let authority = AdmittedAuthority {
            local_identity: pod.identity.clone(),
            current_configuration: configuration.clone(),
            previous_configuration: None,
            transition_kind: Some(TransitionKind::Bootstrap),
            switchover_handoff: None,
            secondary_removal: None,
            scale_up: None,
        };
        pod.effect(RuntimeEffectAction::AdmitAuthority(Box::new(authority)))
            .await;
        pod.effect(RuntimeEffectAction::ChangeRole(if index == 0 {
            ReplicaRole::Primary
        } else {
            ReplicaRole::ActiveSecondary
        }))
        .await;
    }
    for (target, build_id) in builds {
        pods[0]
            .effect(RuntimeEffectAction::RetireBuild(build_id.clone()))
            .await;
        target
            .effect(RuntimeEffectAction::RetireBuild(build_id))
            .await;
    }
    pods[0]
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        })
        .await;
}
