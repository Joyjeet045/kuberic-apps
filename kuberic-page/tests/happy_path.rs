use std::sync::Arc;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use kuberic_page::{PageService, page_router};
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
use tower::ServiceExt;

#[test]
fn client_can_write_and_read_the_single_page() {
    std::thread::Builder::new()
        .name("kuberic-page-happy-path".into())
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
        instance_id: kuberic_runtime::protocol::types::ReplicaInstanceId::new("page-1"),
        agent_generation: AgentGeneration::new("page-generation-1"),
    };
    let store = Arc::new(
        SqliteStore::create_authorized(
            SqliteStore::metadata_database_path(directory.path()),
            AgentState::new(StorageIdentity {
                schema_version: SCHEMA_VERSION,
                resource_uid: ResourceUid::new("page-test"),
                pod_uid: PodUid::new("page-pod-1"),
                pvc_uid: PvcUid::new("page-pvc-1"),
                initialization_id: InitializationId::new("page-init-1"),
                local_identity: identity.clone(),
                effective_policy: EffectivePolicy::fixed(1, 30).unwrap(),
            }),
        )
        .unwrap(),
    );
    let application = Arc::new(PageService::new(
        "in-process://page-1",
        "http://page-1:8080",
    ));
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

    let router = page_router(application);
    let response = router
        .clone()
        .oneshot(Request::get("/page").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::NOT_FOUND);

    let expected = b"<h1>Hello from Kuberic</h1>";
    let response = router
        .clone()
        .oneshot(
            Request::put("/page")
                .body(Body::from(expected.as_slice()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let response = router
        .oneshot(Request::get("/page").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        to_bytes(response.into_body(), usize::MAX).await.unwrap(),
        expected.as_slice()
    );

    runtime.abort();
}

async fn execute(adapter: &RuntimeAdapter, store: &Arc<SqliteStore>, action: RuntimeEffectAction) {
    let state = store.load_state().await.unwrap();
    adapter
        .execute(RuntimeEffect {
            operation_id: OperationId::new(format!("page-effect-{}", state.next_effect_sequence)),
            sequence: state.next_effect_sequence,
            action,
        })
        .await
        .unwrap();
}
