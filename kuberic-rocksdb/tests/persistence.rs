use bytes::Bytes;
use futures::{StreamExt, TryStreamExt, stream};
use kuberic_rocksdb::{BatchRequest, Mutation, RocksState};
use kuberic_runtime::application::{CopyChunk, Operation, StateProvider};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::{Epoch, OperationId};

fn put(key: &[u8], value: &[u8]) -> Mutation {
    Mutation::Put {
        key: key.to_vec(),
        value: value.to_vec(),
    }
}

fn operation(lsn: i64, committed_lsn: i64, operations: Vec<Mutation>) -> Operation {
    Operation {
        lsn,
        committed_lsn,
        data: BatchRequest {
            column_family: "default".into(),
            operations,
        }
        .encode()
        .unwrap(),
    }
}

async fn open(path: &std::path::Path) -> RocksState {
    let state = RocksState::deferred(path);
    state.initialize().await.unwrap();
    state
}

async fn install(source: &RocksState, target: &RocksState, lsn: i64, build: &OperationId) {
    let mut stream = source
        .get_copy_state(lsn, Box::pin(stream::empty()))
        .await
        .unwrap();
    let mut sequence = 0;
    while let Some(bytes) = stream.next().await {
        sequence += 1;
        let chunk = CopyChunk {
            data: bytes.unwrap(),
        };
        target
            .apply_copy_chunk(build, sequence, chunk.clone())
            .await
            .unwrap();
        assert!(
            target
                .verify_copy_chunk(build, sequence, &chunk)
                .await
                .unwrap()
        );
    }
    target.finish_copy(build, lsn, lsn).await.unwrap();
}

#[tokio::test]
async fn accepted_tail_is_durable_but_never_client_visible_until_commit() {
    let root = tempfile::tempdir().unwrap();
    let state = open(root.path()).await;
    let first = operation(1, 0, vec![put(b"key", b"one")]);
    state.apply(first.clone()).await.unwrap();
    assert_eq!(state.get(b"key".to_vec()).await.unwrap(), None);
    assert_eq!(state.durable_progress().await.unwrap().applied_lsn, 1);
    state.close().await.unwrap();

    let state = open(root.path()).await;
    assert!(state.verify_applied(&first).await.unwrap());
    assert_eq!(state.get(b"key".to_vec()).await.unwrap(), None);
    state.commit(1).await.unwrap();
    assert_eq!(state.get(b"key".to_vec()).await.unwrap().unwrap(), b"one");
    state
        .apply(operation(2, 1, vec![put(b"key", b"uncommitted")]))
        .await
        .unwrap();
    state.update_epoch(Epoch::new(0, 2), 1).await.unwrap();
    assert_eq!(state.get(b"key".to_vec()).await.unwrap().unwrap(), b"one");
    state.close().await.unwrap();
    let state = open(root.path()).await;
    assert_eq!(state.durable_progress().await.unwrap().committed_lsn, 1);
    assert_eq!(state.get(b"key".to_vec()).await.unwrap().unwrap(), b"one");
    state.close().await.unwrap();
}

#[tokio::test]
async fn batches_merges_deletes_and_progress_are_replayed_exactly_once() {
    let root = tempfile::tempdir().unwrap();
    let state = open(root.path()).await;
    let op = operation(
        1,
        0,
        vec![
            put(b"key", b"A"),
            Mutation::Merge {
                key: b"key".to_vec(),
                value: b"B".to_vec(),
            },
            Mutation::Merge {
                key: b"key".to_vec(),
                value: b"C".to_vec(),
            },
            put(b"deleted", b"x"),
            Mutation::Delete {
                key: b"deleted".to_vec(),
            },
            put(b"", b"empty-key"),
            put(b"progress", b"user-key-not-metadata"),
        ],
    );
    state.apply(op.clone()).await.unwrap();
    state.commit(1).await.unwrap();
    state.apply(op.clone()).await.unwrap();
    state.commit(1).await.unwrap();
    assert_eq!(state.get(b"key".to_vec()).await.unwrap().unwrap(), b"ABC");
    assert_eq!(state.get(b"deleted".to_vec()).await.unwrap(), None);
    assert_eq!(state.get(Vec::new()).await.unwrap().unwrap(), b"empty-key");
    assert_eq!(
        state.get(b"progress".to_vec()).await.unwrap().unwrap(),
        b"user-key-not-metadata"
    );
    state.close().await.unwrap();
    let state = open(root.path()).await;
    state.apply(op).await.unwrap();
    assert_eq!(state.get(b"key".to_vec()).await.unwrap().unwrap(), b"ABC");
    state.close().await.unwrap();
}

#[tokio::test]
async fn gaps_conflicts_invalid_watermarks_and_epoch_regressions_fail_explicitly() {
    let root = tempfile::tempdir().unwrap();
    let state = open(root.path()).await;
    assert!(
        state
            .apply(operation(2, 0, vec![put(b"k", b"v")]))
            .await
            .is_err()
    );
    assert!(
        state
            .apply(operation(1, 2, vec![put(b"k", b"v")]))
            .await
            .is_err()
    );
    assert!(
        state
            .apply(operation(1, -1, vec![put(b"k", b"v")]))
            .await
            .is_err()
    );
    state
        .apply(operation(1, 0, vec![put(b"k", b"v")]))
        .await
        .unwrap();
    assert!(
        state
            .apply(operation(1, 0, vec![put(b"k", b"different")]))
            .await
            .is_err()
    );
    assert!(
        state
            .apply(operation(1, 1, vec![put(b"k", b"v")]))
            .await
            .is_err()
    );
    assert!(state.commit(2).await.is_err());
    state.update_epoch(Epoch::new(0, 2), 0).await.unwrap();
    assert!(state.update_epoch(Epoch::new(0, 1), 0).await.is_err());
    assert!(state.update_epoch(Epoch::new(0, 2), -1).await.is_err());
    state.close().await.unwrap();
    let state = open(root.path()).await;
    assert!(state.update_epoch(Epoch::new(0, 1), 0).await.is_err());
    state.close().await.unwrap();
}

#[tokio::test]
async fn physical_checkpoint_is_frozen_repeatable_and_followed_by_exact_catchup() {
    let root = tempfile::tempdir().unwrap();
    let source = open(&root.path().join("source")).await;
    source
        .apply(operation(1, 0, vec![put(b"k", b"A")]))
        .await
        .unwrap();
    source.commit(1).await.unwrap();
    let original = source.checkpoint(1).await.unwrap();
    source
        .apply(operation(
            2,
            1,
            vec![Mutation::Merge {
                key: b"k".to_vec(),
                value: b"B".to_vec(),
            }],
        ))
        .await
        .unwrap();
    source.commit(2).await.unwrap();
    source
        .apply(operation(3, 2, vec![put(b"k", b"hidden")]))
        .await
        .unwrap();
    assert_eq!(source.checkpoint(1).await.unwrap(), original);
    source.close().await.unwrap();
    let source = open(&root.path().join("source")).await;
    assert_eq!(source.checkpoint(1).await.unwrap(), original);
    let target = open(&root.path().join("target")).await;
    let build = OperationId::new("copy/with/path-like-id");
    install(&source, &target, 1, &build).await;
    assert_eq!(target.get(b"k".to_vec()).await.unwrap().unwrap(), b"A");
    assert!(target.get_replication_operations(1, 1).await.is_err());
    let operations = source
        .get_replication_operations(2, 3)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    for op in operations {
        target.apply(op).await.unwrap();
    }
    assert_eq!(target.get(b"k".to_vec()).await.unwrap().unwrap(), b"AB");
    assert_eq!(target.durable_progress().await.unwrap().applied_lsn, 3);
    target.finish_copy(&build, 1, 1).await.unwrap();
    assert_eq!(target.durable_progress().await.unwrap().applied_lsn, 3);
    assert!(target.finish_copy(&build, 2, 2).await.is_err());
    source.close().await.unwrap();
    target.close().await.unwrap();
    let target = open(&root.path().join("target")).await;
    target.finish_copy(&build, 1, 1).await.unwrap();
    assert_eq!(target.get(b"k".to_vec()).await.unwrap().unwrap(), b"AB");
    target.close().await.unwrap();
}

#[tokio::test]
async fn historical_checkpoint_undo_handles_repeated_keys_and_uncommitted_suffixes() {
    let root = tempfile::tempdir().unwrap();
    let source = open(&root.path().join("source")).await;
    source
        .apply(operation(1, 0, vec![put(b"k", b"A"), put(b"gone", b"old")]))
        .await
        .unwrap();
    source.commit(1).await.unwrap();
    source
        .apply(operation(
            2,
            1,
            vec![
                Mutation::Delete {
                    key: b"gone".to_vec(),
                },
                put(b"k", b"B"),
                Mutation::Merge {
                    key: b"k".to_vec(),
                    value: b"C".to_vec(),
                },
                put(b"new", b"v"),
            ],
        ))
        .await
        .unwrap();
    source.commit(2).await.unwrap();
    source
        .apply(operation(3, 2, vec![put(b"k", b"pending")]))
        .await
        .unwrap();
    let target = open(&root.path().join("target")).await;
    install(&source, &target, 1, &OperationId::new("historical")).await;
    assert_eq!(target.get(b"k".to_vec()).await.unwrap().unwrap(), b"A");
    assert_eq!(target.get(b"gone".to_vec()).await.unwrap().unwrap(), b"old");
    assert_eq!(target.get(b"new".to_vec()).await.unwrap(), None);
    assert!(source.checkpoint(3).await.is_err());
    source.close().await.unwrap();
    target.close().await.unwrap();
}

#[tokio::test]
async fn copy_chunks_survive_restart_and_reject_conflicts_or_incomplete_completion() {
    let root = tempfile::tempdir().unwrap();
    let source = open(&root.path().join("source")).await;
    source
        .apply(operation(1, 0, vec![put(b"large", &vec![7; 150_000])]))
        .await
        .unwrap();
    source.commit(1).await.unwrap();
    let chunks = source
        .get_copy_state(1, Box::pin(stream::empty()))
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert!(chunks.len() > 1);
    let target = open(&root.path().join("target")).await;
    let build = OperationId::new("restart-copy");
    let first = CopyChunk {
        data: chunks[0].clone(),
    };
    assert!(
        target
            .apply_copy_chunk(&build, 2, first.clone())
            .await
            .is_err()
    );
    target
        .apply_copy_chunk(&build, 1, first.clone())
        .await
        .unwrap();
    target.close().await.unwrap();
    let target = open(&root.path().join("target")).await;
    assert!(target.verify_copy_chunk(&build, 1, &first).await.unwrap());
    target.apply_copy_chunk(&build, 1, first).await.unwrap();
    assert!(
        target
            .apply_copy_chunk(
                &build,
                1,
                CopyChunk {
                    data: Bytes::from_static(b"wrong")
                }
            )
            .await
            .is_err()
    );
    assert!(target.finish_copy(&build, 1, 1).await.is_err());
    for (index, bytes) in chunks.into_iter().enumerate().skip(1) {
        target
            .apply_copy_chunk(
                &build,
                u64::try_from(index).unwrap() + 1,
                CopyChunk { data: bytes },
            )
            .await
            .unwrap();
    }
    target.finish_copy(&build, 1, 1).await.unwrap();
    assert_eq!(
        target.get(b"large".to_vec()).await.unwrap().unwrap(),
        vec![7; 150_000]
    );
    source.close().await.unwrap();
    target.close().await.unwrap();
}

#[tokio::test]
async fn malformed_versions_profiles_batches_and_unsupported_paths_are_rejected() {
    let root = tempfile::tempdir().unwrap();
    let state = open(root.path()).await;
    let original = operation(1, 0, vec![put(b"k", b"v")]);
    for field in ["version", "profile", "checksum", "batch", "operations"] {
        let mut value: serde_json::Value = serde_json::from_slice(&original.data).unwrap();
        value[field] = match field {
            "version" => serde_json::json!(999),
            "profile" => serde_json::json!("different-engine"),
            "checksum" => serde_json::json!(0),
            "batch" => serde_json::json!([0, 1]),
            _ => serde_json::json!([{"op":"ingest_sst","path":"file.sst"}]),
        };
        assert!(
            state
                .apply(Operation {
                    data: serde_json::to_vec(&value).unwrap().into(),
                    ..original.clone()
                })
                .await
                .is_err()
        );
    }
    assert!(
        BatchRequest {
            column_family: "dynamic".into(),
            operations: vec![put(b"k", b"v")]
        }
        .encode()
        .is_err()
    );
    assert!(
        BatchRequest {
            column_family: "default".into(),
            operations: Vec::new()
        }
        .encode()
        .is_err()
    );
    assert!(
        serde_json::from_str::<BatchRequest>(r#"{"operations":[],"disableWAL":true}"#).is_err()
    );
    assert!(
        serde_json::from_str::<BatchRequest>(
            r#"{"operations":[],"transaction_mode":"write_prepared"}"#
        )
        .is_err()
    );
    assert_eq!(state.durable_progress().await.unwrap().applied_lsn, 0);
    state.close().await.unwrap();
}

#[tokio::test]
async fn established_unknown_or_corrupted_storage_does_not_initialize_silently() {
    let root = tempfile::tempdir().unwrap();
    std::fs::write(root.path().join("unexpected"), b"data").unwrap();
    assert!(!RocksState::is_fresh_empty(root.path()).unwrap());
    assert!(
        RocksState::deferred(root.path())
            .initialize()
            .await
            .is_err()
    );
    let good = root.path().join("good");
    let state = open(&good).await;
    state.close().await.unwrap();
    let catalog = rocksdb::DB::open_default(good.join("catalog")).unwrap();
    catalog.put(b"profile", b"wrong").unwrap();
    drop(catalog);
    assert!(RocksState::deferred(&good).initialize().await.is_err());
}

#[test]
fn abrupt_process_exit_preserves_synced_acceptance_and_commit_without_drop() {
    for mode in ["accepted", "committed"] {
        let root = tempfile::tempdir().unwrap();
        let result = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "crash_child", "--nocapture"])
            .env("KUBERIC_ROCKSDB_CRASH_ROOT", root.path())
            .env("KUBERIC_ROCKSDB_CRASH_MODE", mode)
            .status()
            .unwrap();
        assert_eq!(result.code(), Some(71));
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let state = open(root.path()).await;
            let progress = state.durable_progress().await.unwrap();
            assert_eq!(progress.applied_lsn, 1);
            assert_eq!(progress.committed_lsn, i64::from(mode == "committed"));
            assert_eq!(
                state.get(b"key".to_vec()).await.unwrap(),
                (mode == "committed").then(|| b"value".to_vec())
            );
            state.close().await.unwrap();
        });
    }
}

#[test]
fn crash_child() {
    let Some(root) = std::env::var_os("KUBERIC_ROCKSDB_CRASH_ROOT") else {
        return;
    };
    tokio::runtime::Runtime::new().unwrap().block_on(async {
        let state = open(std::path::Path::new(&root)).await;
        state
            .apply(operation(1, 0, vec![put(b"key", b"value")]))
            .await
            .unwrap();
        if std::env::var("KUBERIC_ROCKSDB_CRASH_MODE").unwrap() == "committed" {
            state.commit(1).await.unwrap();
        }
        std::process::exit(71);
    });
}
