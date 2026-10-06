use std::sync::Arc;

use kuberic_reliable_collections::ReliableCollectionsService;
use kuberic_runtime::application::OpenMode;
use kuberic_runtime::protocol::types::{
    AccessStatus, AgentGeneration, ConfigurationDescriptor, ConfigurationMember, EffectivePolicy,
    Epoch, InitializationId, OperationId, PodUid, PvcUid, ReplicaId, ReplicaIdentity, ReplicaRole,
    ResourceUid,
};
use kuberic_runtime::testing::authority::AdmittedAuthority;
use kuberic_runtime::testing::effects::{RuntimeEffect, RuntimeEffectAction};
use kuberic_runtime::testing::hosting::PodRuntime;
use kuberic_runtime::testing::runtime_adapter::RuntimeAdapter;
use kuberic_runtime::testing::sqlite_store::SqliteStore;
use kuberic_runtime::testing::state::{AgentState, SCHEMA_VERSION, StorageIdentity};

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
