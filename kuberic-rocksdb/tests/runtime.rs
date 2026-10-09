mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use axum::body::{Body, to_bytes};
use axum::http::{Request, StatusCode};
use kuberic_rocksdb::{BatchRequest, Mutation, rocks_router};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::{AccessStatus, ReplicaRole};
use kuberic_runtime::testing::effects::RuntimeEffectAction;
use tower::ServiceExt;

use support::*;

#[test]
fn http_api_obeys_authority_validates_requests_and_recovers_after_restart() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let pod = Pod::new(1, root.path().join("one"), 1).await;
        let router = rocks_router(pod.application.clone());
        let response = router
            .clone()
            .oneshot(Request::put("/keys/a").body(Body::from("x")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        bootstrap(&[&pod]).await;
        let response = router
            .clone()
            .oneshot(Request::put("/keys/a").body(Body::from("Alice")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let receipt: serde_json::Value =
            serde_json::from_slice(&to_bytes(response.into_body(), 1024).await.unwrap()).unwrap();
        assert_eq!(receipt["lsn"], 1);
        let response = router
            .clone()
            .oneshot(Request::get("/keys/a").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(to_bytes(response.into_body(), 1024).await.unwrap(), "Alice");
        for (body, status) in [
            (
                r#"{"column_family":"unknown","operations":[{"op":"delete","key":[97]}]}"#,
                StatusCode::BAD_REQUEST,
            ),
            (
                r#"{"operations":[{"op":"ingest_sst","path":"x"}]}"#,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
            (
                r#"{"operations":[],"disableWAL":true}"#,
                StatusCode::UNPROCESSABLE_ENTITY,
            ),
        ] {
            let response = router
                .clone()
                .oneshot(
                    Request::post("/batch")
                        .header("content-type", "application/json")
                        .body(Body::from(body))
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.status(), status);
        }
        drop(router);
        let pod = pod.restart().await;
        assert_eq!(
            pod.application.get(b"a".to_vec()).await.unwrap().unwrap(),
            b"Alice"
        );
        let router = rocks_router(pod.application.clone());
        let response = router
            .clone()
            .oneshot(Request::delete("/keys/a").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let response = router
            .oneshot(Request::get("/keys/a").body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        pod.shutdown().await;
    });
}

#[test]
fn three_replicas_replicate_atomic_put_delete_and_merge_batches() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let a = Pod::new(1, root.path().join("a"), 3).await;
        let b = Pod::new(2, root.path().join("b"), 3).await;
        let c = Pod::new(3, root.path().join("c"), 3).await;
        bootstrap(&[&a, &b, &c]).await;
        let routes = route(&[&a, &b, &c]).await;
        let lsn = a
            .application
            .write(BatchRequest {
                column_family: "default".into(),
                operations: vec![
                    Mutation::Put {
                        key: b"k".to_vec(),
                        value: b"A".to_vec(),
                    },
                    Mutation::Merge {
                        key: b"k".to_vec(),
                        value: b"B".to_vec(),
                    },
                    Mutation::Put {
                        key: b"deleted".to_vec(),
                        value: b"x".to_vec(),
                    },
                    Mutation::Delete {
                        key: b"deleted".to_vec(),
                    },
                ],
            })
            .await
            .unwrap();
        wait_applied(&[&b, &c], lsn).await;
        assert_eq!(
            a.application.get(b"k".to_vec()).await.unwrap().unwrap(),
            b"AB"
        );
        assert_eq!(a.application.get(b"deleted".to_vec()).await.unwrap(), None);
        assert!(b.application.write(request(b"k", b"bad")).await.is_err());
        assert!(b.application.get(b"k".to_vec()).await.is_err());
        for pod in [&b, &c] {
            assert_eq!(pod.state.durable_progress().await.unwrap().applied_lsn, lsn);
        }
        routes.stop().await;
        for pod in [&a, &b, &c] {
            pod.shutdown().await;
        }
    });
}

#[test]
fn concurrent_client_writes_follow_the_returned_lsn_order() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let a = Pod::new(1, root.path().join("a"), 3).await;
        let b = Pod::new(2, root.path().join("b"), 3).await;
        let c = Pod::new(3, root.path().join("c"), 3).await;
        bootstrap(&[&a, &b, &c]).await;
        let routes = route(&[&a, &b, &c]).await;
        let mut tasks = Vec::new();
        for value in 0..12_u8 {
            let application = a.application.clone();
            tasks.push(tokio::spawn(async move {
                (
                    application.write(request(b"key", &[value])).await.unwrap(),
                    value,
                )
            }));
        }
        let mut receipts = BTreeMap::new();
        for task in tasks {
            let (lsn, value) = task.await.unwrap();
            assert!(receipts.insert(lsn, value).is_none());
        }
        assert_eq!(
            receipts.keys().copied().collect::<Vec<_>>(),
            (1..=12).collect::<Vec<_>>()
        );
        wait_applied(&[&b, &c], 12).await;
        assert_eq!(
            a.application.get(b"key".to_vec()).await.unwrap().unwrap(),
            vec![*receipts.last_key_value().unwrap().1]
        );
        routes.stop().await;
        for pod in [&a, &b, &c] {
            pod.shutdown().await;
        }
    });
}

#[test]
fn primary_crash_after_quorum_preserves_acknowledged_writes_on_promotion() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let a = Pod::new(1, root.path().join("a"), 3).await;
        let b = Pod::new(2, root.path().join("b"), 3).await;
        let c = Pod::new(3, root.path().join("c"), 3).await;
        let previous = bootstrap(&[&a, &b, &c]).await;
        let routes = route(&[&a, &b, &c]).await;
        let lsn = a
            .application
            .write(request(b"key", b"acknowledged"))
            .await
            .unwrap();
        wait_applied(&[&b, &c], lsn).await;
        routes.stop().await;
        let (_, routes) = change_primary(&[&a, &b, &c], &previous, &b, false, lsn).await;
        assert_eq!(
            b.application.get(b"key".to_vec()).await.unwrap().unwrap(),
            b"acknowledged"
        );
        assert_eq!(b.application.get(b"stale".to_vec()).await.unwrap(), None);
        let next = b
            .application
            .write(request(b"next", b"after-failover"))
            .await
            .unwrap();
        assert!(next > lsn);
        wait_applied(&[&c], next).await;
        routes.stop().await;
        for pod in [&a, &b, &c] {
            pod.shutdown().await;
        }
    });
}

#[test]
fn planned_switchover_fences_the_still_running_old_primary() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let a = Pod::new(1, root.path().join("a"), 3).await;
        let b = Pod::new(2, root.path().join("b"), 3).await;
        let c = Pod::new(3, root.path().join("c"), 3).await;
        let previous = bootstrap(&[&a, &b, &c]).await;
        let routes = route(&[&a, &b, &c]).await;
        let lsn = a
            .application
            .write(request(b"key", b"before-switch"))
            .await
            .unwrap();
        wait_applied(&[&b, &c], lsn).await;
        routes.stop().await;
        let (_, routes) = change_primary(&[&a, &b, &c], &previous, &b, true, lsn).await;
        assert_eq!(
            b.application.get(b"key".to_vec()).await.unwrap().unwrap(),
            b"before-switch"
        );
        assert_eq!(
            a.runtime.snapshot().await.role,
            ReplicaRole::ActiveSecondary
        );
        assert!(
            a.application
                .write(request(b"key", b"stale"))
                .await
                .is_err()
        );
        let lsn = b
            .application
            .write(request(b"key", b"after-switch"))
            .await
            .unwrap();
        wait_applied(&[&a, &c], lsn).await;
        assert!(
            a.try_effect(RuntimeEffectAction::AdmitAuthority(Box::new(authority(
                &a, &previous
            ))))
            .await
            .is_err()
        );
        routes.stop().await;
        for pod in [&a, &b, &c] {
            pod.shutdown().await;
        }
    });
}

#[test]
fn primary_crash_before_quorum_leaves_no_visible_uncommitted_write() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let a = Pod::new(1, root.path().join("a"), 3).await;
        let b = Pod::new(2, root.path().join("b"), 3).await;
        let c = Pod::new(3, root.path().join("c"), 3).await;
        bootstrap(&[&a, &b, &c]).await;
        let application = a.application.clone();
        let pending =
            tokio::spawn(async move { application.write(request(b"key", b"uncommitted")).await });
        tokio::time::timeout(Duration::from_secs(10), async {
            while a.state.durable_progress().await.unwrap().applied_lsn == 0 {
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        assert!(!pending.is_finished());
        assert_eq!(a.state.get(b"key".to_vec()).await.unwrap(), None);
        a.runtime.abort();
        assert!(pending.await.unwrap().is_err());
        a.state.close().await.unwrap();
        a.state.initialize().await.unwrap();
        assert_eq!(a.state.durable_progress().await.unwrap().committed_lsn, 0);
        assert_eq!(a.state.get(b"key".to_vec()).await.unwrap(), None);
        for pod in [&a, &b, &c] {
            pod.shutdown().await;
        }
    });
}

#[test]
fn primary_role_alone_does_not_grant_read_or_write_access() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let pod = Pod::new(1, root.path().join("one"), 1).await;
        bootstrap(&[&pod]).await;
        pod.effect(RuntimeEffectAction::SetAccessStatus {
            read: AccessStatus::ReconfigurationPending,
            write: AccessStatus::ReconfigurationPending,
        })
        .await;
        assert_eq!(pod.runtime.snapshot().await.role, ReplicaRole::Primary);
        assert!(pod.application.write(request(b"k", b"v")).await.is_err());
        assert!(pod.application.get(b"k".to_vec()).await.is_err());
        pod.shutdown().await;
    });
}

#[test]
fn new_replica_uses_physical_checkpoint_then_runtime_incremental_catchup() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let a = Pod::new(1, root.path().join("a"), 3).await;
        let b = Pod::new(2, root.path().join("b"), 3).await;
        let c = Pod::new(3, root.path().join("c"), 3).await;
        let d = Pod::new(4, root.path().join("d"), 3).await;
        bootstrap(&[&a, &b, &c]).await;
        let routes = route(&[&a, &b, &c]).await;
        let boundary = a
            .application
            .write(request(b"before-snapshot", b"checkpoint"))
            .await
            .unwrap();
        build_after_write(&a, &d, boundary, &[&b, &c], routes).await;
        for pod in [&a, &b, &c, &d] {
            pod.shutdown().await;
        }
    });
}

#[test]
fn restarted_secondary_recovers_durable_ack_and_continues_replication() {
    run(|| async {
        let root = tempfile::tempdir().unwrap();
        let a = Pod::new(1, root.path().join("a"), 3).await;
        let b = Pod::new(2, root.path().join("b"), 3).await;
        let c = Pod::new(3, root.path().join("c"), 3).await;
        bootstrap(&[&a, &b, &c]).await;
        let routes = route(&[&a, &b, &c]).await;
        let boundary = a
            .application
            .write(request(b"key", b"durable"))
            .await
            .unwrap();
        wait_applied(&[&b, &c], boundary).await;
        routes.stop().await;
        let b = tokio::time::timeout(Duration::from_secs(10), b.restart())
            .await
            .expect("secondary reconstructs its durable state");
        assert_eq!(
            b.state.durable_progress().await.unwrap().applied_lsn,
            boundary
        );
        let routes = route(&[&a, &b, &c]).await;
        a.runtime
            .repair_peer(b.identity.clone(), boundary.saturating_sub(1))
            .await
            .unwrap();
        let next = tokio::time::timeout(
            Duration::from_secs(10),
            a.application.write(request(b"key", b"after-restart")),
        )
        .await
        .expect("write after fresh-session repair")
        .unwrap();
        wait_applied(&[&b, &c], next).await;
        let (_, routes_after) = {
            routes.stop().await;
            let previous = configuration(&[&a, &b, &c], &a, 1);
            change_primary(&[&a, &b, &c], &previous, &b, false, next).await
        };
        assert_eq!(
            b.application.get(b"key".to_vec()).await.unwrap().unwrap(),
            b"after-restart"
        );
        routes_after.stop().await;
        for pod in [&a, &b, &c] {
            pod.shutdown().await;
        }
    });
}
