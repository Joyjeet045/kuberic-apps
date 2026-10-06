use std::sync::Arc;

use bytes::Bytes;
use futures::{StreamExt, stream};
use kuberic_rocksdb::{Mutation, RocksState, RocksStateProvider};
use kuberic_runtime::RuntimeError;
use kuberic_runtime::application::{CopyChunk, Operation, StateProvider};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::OperationId;

fn operation(lsn: i64, committed_lsn: i64, mutations: Vec<Mutation>) -> Operation {
    Operation {
        lsn,
        committed_lsn,
        data: RocksState::encode_mutations(mutations).unwrap(),
    }
}

async fn collect_copy(mut stream: kuberic_runtime::application::OperationDataStream) -> Vec<Bytes> {
    let mut chunks = Vec::new();
    while let Some(chunk) = stream.next().await {
        chunks.push(chunk.unwrap());
    }
    chunks
}

#[tokio::test]
async fn apply_is_idempotent_and_rejects_lsn_conflicts_and_gaps() {
    let directory = tempfile::tempdir().unwrap();
    let state = RocksState::open(directory.path()).unwrap();
    let first = operation(
        1,
        0,
        vec![Mutation::Put {
            key: b"key".to_vec(),
            value: b"value".to_vec(),
        }],
    );
    let duplicate = first.clone();
    let conflict = operation(
        1,
        0,
        vec![Mutation::Put {
            key: b"key".to_vec(),
            value: b"different".to_vec(),
        }],
    );
    let gap = operation(
        3,
        0,
        vec![Mutation::Put {
            key: b"later".to_vec(),
            value: b"value".to_vec(),
        }],
    );

    assert_eq!(state.apply(first.clone()).await.unwrap().applied_lsn, 1);
    assert_eq!(state.apply(duplicate).await.unwrap().applied_lsn, 1);
    assert!(matches!(
        state.apply(conflict).await,
        Err(RuntimeError::AuthorityMismatch(_))
    ));
    assert!(matches!(
        state.apply(gap).await,
        Err(RuntimeError::Application(_))
    ));
    assert!(state.verify_applied(&first).await.unwrap());
}

#[tokio::test]
async fn commit_is_bounded_by_applied_progress() {
    let directory = tempfile::tempdir().unwrap();
    let state = RocksState::open(directory.path()).unwrap();

    assert!(state.commit(1).await.is_err());
    state
        .apply(operation(
            1,
            0,
            vec![Mutation::Put {
                key: b"key".to_vec(),
                value: b"value".to_vec(),
            }],
        ))
        .await
        .unwrap();
    assert!(state.commit(2).await.is_err());
    assert_eq!(state.commit(1).await.unwrap().committed_lsn, 1);
    assert_eq!(state.commit(0).await.unwrap().committed_lsn, 1);
}

#[tokio::test]
async fn restart_recovers_durable_progress_and_values() {
    let directory = tempfile::tempdir().unwrap();
    {
        let state = RocksState::open(directory.path()).unwrap();
        state
            .apply(operation(
                1,
                0,
                vec![Mutation::Put {
                    key: b"key".to_vec(),
                    value: b"value".to_vec(),
                }],
            ))
            .await
            .unwrap();
        state.commit(1).await.unwrap();
    }

    let reopened = RocksState::open(directory.path()).unwrap();
    assert_eq!(reopened.applied_lsn().await.unwrap(), 1);
    assert_eq!(reopened.committed_lsn().await.unwrap(), 1);
    assert_eq!(
        reopened.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"value"))
    );
}

#[tokio::test]
async fn copy_at_earlier_boundary_is_exact_and_repeatable_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let post_boundary = operation(
        2,
        1,
        vec![Mutation::Put {
            key: b"key".to_vec(),
            value: b"new".to_vec(),
        }],
    );
    let first_bytes = {
        let source = Arc::new(RocksState::open(directory.path()).unwrap());
        source
            .apply(operation(
                1,
                0,
                vec![Mutation::Put {
                    key: b"key".to_vec(),
                    value: b"old".to_vec(),
                }],
            ))
            .await
            .unwrap();
        source.commit(1).await.unwrap();
        source.apply(post_boundary.clone()).await.unwrap();
        let provider = RocksStateProvider::new(source.clone());
        let first = collect_copy(
            provider
                .get_copy_state(1, Box::pin(stream::empty()))
                .await
                .unwrap(),
        )
        .await;
        let second = collect_copy(
            provider
                .get_copy_state(1, Box::pin(stream::empty()))
                .await
                .unwrap(),
        )
        .await;
        assert_eq!(first, second);
        let retained = source
            .get_replication_operations(2, 2)
            .await
            .unwrap()
            .map(|operation| operation.unwrap())
            .collect::<Vec<_>>()
            .await;
        assert_eq!(retained, vec![post_boundary]);
        first
    };

    let reopened = Arc::new(RocksState::open(directory.path()).unwrap());
    let provider = RocksStateProvider::new(reopened);
    let after_restart = collect_copy(
        provider
            .get_copy_state(1, Box::pin(stream::empty()))
            .await
            .unwrap(),
    )
    .await;
    assert_eq!(first_bytes, after_restart);

    let target_directory = tempfile::tempdir().unwrap();
    let target = RocksState::open(target_directory.path()).unwrap();
    let build = OperationId::new("earlier-boundary");
    for (index, chunk) in after_restart.iter().enumerate() {
        target
            .apply_copy_chunk(
                &build,
                index as u64 + 1,
                CopyChunk {
                    data: chunk.clone(),
                },
            )
            .await
            .unwrap();
    }
    target.finish_copy(&build, 1, 1).await.unwrap();
    assert_eq!(
        target.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"old"))
    );
}

#[tokio::test]
async fn copy_chunks_verify_finish_retry_and_catch_up() {
    let source_directory = tempfile::tempdir().unwrap();
    let target_directory = tempfile::tempdir().unwrap();
    let source = Arc::new(RocksState::open(source_directory.path()).unwrap());
    let copied = operation(
        1,
        0,
        vec![Mutation::Put {
            key: b"key".to_vec(),
            value: b"copied".to_vec(),
        }],
    );
    let catch_up = operation(
        2,
        1,
        vec![Mutation::Merge {
            key: b"key".to_vec(),
            value: b"+tail".to_vec(),
        }],
    );
    source.apply(copied).await.unwrap();
    source.commit(1).await.unwrap();
    source.apply(catch_up.clone()).await.unwrap();
    let provider = RocksStateProvider::new(source.clone());
    let chunks = collect_copy(
        provider
            .get_copy_state(1, Box::pin(stream::empty()))
            .await
            .unwrap(),
    )
    .await;

    let target = RocksState::open(target_directory.path()).unwrap();
    let build = OperationId::new("copy-retry");
    for (index, chunk) in chunks.iter().enumerate() {
        let sequence = index as u64 + 1;
        let chunk = CopyChunk {
            data: chunk.clone(),
        };
        target
            .apply_copy_chunk(&build, sequence, chunk.clone())
            .await
            .unwrap();
        assert!(
            target
                .verify_copy_chunk(&build, sequence, &chunk)
                .await
                .unwrap()
        );
    }
    let first = target.finish_copy(&build, 1, 1).await.unwrap();
    let retry = target.finish_copy(&build, 1, 1).await.unwrap();
    assert_eq!(first, retry);
    assert_eq!(
        target.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"copied"))
    );
    target.apply(catch_up).await.unwrap();
    assert_eq!(
        target.get(b"key").await.unwrap(),
        Some(Bytes::from_static(b"copied+tail"))
    );
}

#[tokio::test]
async fn malformed_envelope_is_rejected_without_progress() {
    let directory = tempfile::tempdir().unwrap();
    let state = RocksState::open(directory.path()).unwrap();
    let malformed = Operation {
        lsn: 1,
        committed_lsn: 0,
        data: Bytes::from_static(b"not-a-valid-envelope"),
    };

    assert!(matches!(
        state.apply(malformed).await,
        Err(RuntimeError::Application(_))
    ));
    assert_eq!(state.applied_lsn().await.unwrap(), 0);
}
