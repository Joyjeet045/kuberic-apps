use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use kuberic_reliable_collections::ReliableCollectionsService;
use kuberic_runtime::application::OpenMode;
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
fn client_transaction_commits_through_v2_runtime() {
    std::thread::Builder::new()
        .name("reliable-collections-runtime-happy-path".into())
        .stack_size(8 * 1024 * 1024)
        .spawn(|| {
            tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(client_happy_path());
        })
        .unwrap()
        .join()
        .unwrap();
}

#[test]
fn primary_replicates_transactions_to_secondary() {
    std::thread::Builder::new()
        .name("reliable-collections-runtime-multireplica".into())
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

async fn client_happy_path() {
    let directory = tempfile::tempdir().unwrap();
    let identity = ReplicaIdentity {
        replica_id: ReplicaId::new(1),
        instance_id: kuberic_runtime::protocol::types::ReplicaInstanceId::new("rc-1"),
        agent_generation: AgentGeneration::new("rc-generation-1"),
    };
    let store = Arc::new(
        SqliteStore::create_authorized(
            SqliteStore::metadata_database_path(directory.path()),
            AgentState::new(StorageIdentity {
                schema_version: SCHEMA_VERSION,
                resource_uid: ResourceUid::new("rc-test"),
                pod_uid: PodUid::new("rc-pod-1"),
                pvc_uid: PvcUid::new("rc-pvc-1"),
                initialization_id: InitializationId::new("rc-init-1"),
                local_identity: identity.clone(),
                effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
            }),
        )
        .unwrap(),
    );
    let application = Arc::new(
        ReliableCollectionsService::new(
            directory.path().join("collections"),
            "in-process://rc-1",
            Some("http://rc-1:8080".into()),
        )
        .unwrap(),
    );
    let runtime = Arc::new(PodRuntime::new(
        identity.clone(),
        application.clone(),
        store.clone(),
    ));
    let adapter = RuntimeAdapter::new(store.clone(), runtime.clone());

    execute(
        &adapter,
        &store,
        RuntimeEffectAction::Open(OpenMode::Existing),
    )
    .await;
    let configuration = ConfigurationDescriptor::new(
        Epoch::new(0, 1),
        identity.replica_id,
        vec![ConfigurationMember {
            identity: identity.clone(),
            role: ReplicaRole::Primary,
        }],
        1,
    );
    execute(
        &adapter,
        &store,
        RuntimeEffectAction::AdmitAuthority(Box::new(AdmittedAuthority {
            local_identity: identity,
            current_configuration: configuration,
            previous_configuration: None,
            transition_kind: None,
            switchover_handoff: None,
            secondary_removal: None,
            scale_up: None,
        })),
    )
    .await;
    execute(
        &adapter,
        &store,
        RuntimeEffectAction::ChangeRole(ReplicaRole::Primary),
    )
    .await;
    execute(
        &adapter,
        &store,
        RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::Granted,
        },
    )
    .await;

    let manager = application.state_manager();
    let accounts = manager
        .get_or_add_dictionary::<String, i64>("accounts")
        .await
        .unwrap();
    let audit = manager
        .get_or_add_dictionary::<String, String>("audit")
        .await
        .unwrap();
    let mut transaction = manager.create_transaction().await.unwrap();
    accounts
        .set(&mut transaction, &"alice".into(), &90)
        .unwrap();
    accounts.set(&mut transaction, &"bob".into(), &110).unwrap();
    audit
        .set(&mut transaction, &"last".into(), &"alice -> bob".into())
        .unwrap();
    let identity = transaction.id().clone();
    let version = transaction.commit().await.unwrap();
    assert_eq!(
        manager.committed_result(identity).await.unwrap(),
        Some(version)
    );
    let mut read = manager.create_transaction().await.unwrap();
    assert_eq!(accounts.get(&mut read, &"alice".into()).unwrap(), Some(90));
    assert_eq!(accounts.get(&mut read, &"bob".into()).unwrap(), Some(110));

    runtime.abort();
}

async fn execute(adapter: &RuntimeAdapter, store: &Arc<SqliteStore>, action: RuntimeEffectAction) {
    let state = store.load_state().await.unwrap();
    adapter
        .execute(RuntimeEffect {
            operation_id: OperationId::new(format!("rc-effect-{}", state.next_effect_sequence)),
            sequence: state.next_effect_sequence,
            action,
        })
        .await
        .unwrap();
}

async fn multireplica_happy_path() {
    let directory = tempfile::tempdir().unwrap();
    let primary = RcPod::new(1, directory.path().join("one"), 2).await;
    let secondary = RcPod::new(2, directory.path().join("two"), 2).await;
    bootstrap(&[&primary, &secondary]).await;
    let routes = route(&[&primary, &secondary]).await;

    let manager = primary.application.state_manager();
    let accounts = manager
        .get_or_add_dictionary::<String, i64>("accounts")
        .await
        .unwrap();
    let mut transaction = manager.create_transaction().await.unwrap();
    accounts
        .set(&mut transaction, &"replicated".into(), &42)
        .unwrap();
    let version = tokio::time::timeout(Duration::from_secs(5), transaction.commit())
        .await
        .expect("quorum commit should complete")
        .unwrap();
    wait_applied(&[&secondary], version.0).await;

    secondary
        .effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::Granted,
            write: AccessStatus::NotPrimary,
        })
        .await;
    let secondary_manager = secondary.application.state_manager();
    let mut read = secondary_manager.create_transaction().await.unwrap();
    let secondary_accounts = read
        .get_dictionary::<String, i64>("accounts")
        .unwrap()
        .unwrap();
    assert_eq!(
        secondary_accounts
            .get(&mut read, &"replicated".into())
            .unwrap(),
        Some(42)
    );
    drop(routes);
    primary.runtime.abort();
    secondary.runtime.abort();
}

struct RcPod {
    runtime: Arc<PodRuntime>,
    application: Arc<ReliableCollectionsService>,
    store: Arc<SqliteStore>,
    identity: ReplicaIdentity,
    session: ProcessSession,
}

impl RcPod {
    async fn new(id: i64, root: std::path::PathBuf, replicas: u32) -> Self {
        let identity = ReplicaIdentity {
            replica_id: ReplicaId::new(id),
            instance_id: ReplicaInstanceId::new(format!("rc-{id}")),
            agent_generation: AgentGeneration::new(format!("rc-generation-{id}")),
        };
        let store = Arc::new(
            SqliteStore::create_authorized(
                SqliteStore::metadata_database_path(&root),
                AgentState::new(StorageIdentity {
                    schema_version: SCHEMA_VERSION,
                    resource_uid: ResourceUid::new("rc-multireplica-test"),
                    pod_uid: PodUid::new(format!("rc-pod-{id}")),
                    pvc_uid: PvcUid::new(format!("rc-pvc-{id}")),
                    initialization_id: InitializationId::new(format!("rc-init-{id}")),
                    local_identity: identity.clone(),
                    effective_policy: EffectivePolicy::fixed(replicas, 30).unwrap(),
                }),
            )
            .unwrap(),
        );
        let application = Arc::new(
            ReliableCollectionsService::new(
                root.join("collections"),
                format!("in-process://rc-{id}"),
                Some(format!("http://rc-{id}:8080")),
            )
            .unwrap(),
        );
        let runtime = Arc::new(PodRuntime::new(
            identity.clone(),
            application.clone(),
            store.clone(),
        ));
        Self {
            runtime,
            application,
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
                    "rc-multireplica-effect-{}",
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

impl Drop for RcPod {
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

async fn route(pods: &[&RcPod]) -> Routes {
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
                    panic!("in-process transport rejected reliable-collections message: {error}");
                }
            }
        }
    });
    Routes { task }
}

async fn wait_applied(pods: &[&RcPod], lsn: i64) {
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

async fn bootstrap(pods: &[&RcPod]) {
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
