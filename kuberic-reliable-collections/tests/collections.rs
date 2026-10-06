use std::sync::Arc;
use std::time::Duration;

use futures::{StreamExt, TryStreamExt};
use kuberic_reliable_collections::{
    Error, ReliableCollectionsProvider, ReliableCollectionsState, StateManager, TransactionOptions,
    log::TransactionLog,
};
use kuberic_runtime::StateProvider;
use kuberic_runtime::application::{CopyChunk, OperationDataStream};
use kuberic_runtime::engine::DurableState;
use kuberic_runtime::protocol::types::OperationId;

async fn open_manager(
    path: impl Into<std::path::PathBuf>,
) -> (Arc<ReliableCollectionsState>, StateManager) {
    let state = Arc::new(ReliableCollectionsState::open(path.into()).unwrap());
    let manager = StateManager::standalone(state.clone()).unwrap();
    (state, manager)
}

#[tokio::test]
async fn dictionary_operations_conflicts_and_provider_lifetimes() {
    let directory = tempfile::tempdir().unwrap();
    let (_, manager) = open_manager(directory.path().join("one")).await;
    let mut create = manager.create_transaction().await.unwrap();
    let accounts = create
        .get_or_add_dictionary::<String, i64>("accounts")
        .unwrap();
    let audit = create
        .get_or_add_dictionary::<String, String>("audit")
        .unwrap();
    assert!(accounts.insert(&mut create, &"alice".into(), &100).unwrap());
    assert!(!accounts.insert(&mut create, &"alice".into(), &999).unwrap());
    audit
        .set(&mut create, &"event".into(), &"created".into())
        .unwrap();
    assert_eq!(create.provider_names().unwrap(), ["accounts", "audit"]);
    assert_eq!(create.commit().await.unwrap().0, 1);

    let mut first = manager.create_transaction().await.unwrap();
    let mut second = manager.create_transaction().await.unwrap();
    accounts.set(&mut first, &"alice".into(), &90).unwrap();
    accounts.set(&mut second, &"alice".into(), &80).unwrap();
    audit
        .set(&mut first, &"event".into(), &"debited".into())
        .unwrap();
    let (first, second) = tokio::join!(first.commit(), second.commit());
    assert_ne!(first.is_ok(), second.is_ok());
    assert!(matches!(
        first.err().or(second.err()).unwrap(),
        Error::Conflict(_)
    ));

    let mut left = manager.create_transaction().await.unwrap();
    let mut right = manager.create_transaction().await.unwrap();
    accounts.set(&mut left, &"left".into(), &1).unwrap();
    accounts.set(&mut right, &"right".into(), &2).unwrap();
    left.commit().await.unwrap();
    right.commit().await.unwrap();

    let mut scan = manager.create_transaction().await.unwrap();
    assert!(accounts.entries(&mut scan).unwrap().len() >= 3);
    let mut insert = manager.create_transaction().await.unwrap();
    accounts.set(&mut insert, &"phantom".into(), &3).unwrap();
    insert.commit().await.unwrap();
    assert!(matches!(scan.commit().await, Err(Error::Conflict(_))));

    let mut operations = manager.create_transaction().await.unwrap();
    assert_eq!(
        accounts
            .get_or_add(&mut operations, &"left".into(), 99)
            .unwrap(),
        1
    );
    assert_eq!(
        accounts
            .add_or_update(&mut operations, &"left".into(), |value| value.unwrap() + 1)
            .unwrap(),
        2
    );
    assert!(
        !accounts
            .update(&mut operations, &"left".into(), &99, &4)
            .unwrap()
    );
    assert!(
        accounts
            .update(&mut operations, &"left".into(), &2, &4)
            .unwrap()
    );
    assert_eq!(
        accounts.remove(&mut operations, &"left".into()).unwrap(),
        Some(4)
    );
    accounts.clear(&mut operations).unwrap();
    assert!(accounts.entries(&mut operations).unwrap().is_empty());
    operations.commit().await.unwrap();

    let mut removal = manager.create_transaction().await.unwrap();
    assert!(
        removal
            .get_dictionary::<String, String>("accounts")
            .is_err()
    );
    assert!(removal.remove_provider("accounts").unwrap());
    let replacement = removal
        .get_or_add_dictionary::<String, i64>("accounts")
        .unwrap();
    assert!(accounts.set(&mut removal, &"stale".into(), &1).is_err());
    replacement.set(&mut removal, &"new".into(), &5).unwrap();
    removal.commit().await.unwrap();
}

#[tokio::test]
async fn read_skew_abort_timeout_tombstones_and_request_retries() {
    let directory = tempfile::tempdir().unwrap();
    let (_, manager) = open_manager(directory.path().join("one")).await;
    let values = manager
        .get_or_add_dictionary::<String, i64>("values")
        .await
        .unwrap();

    let mut first = manager.create_transaction().await.unwrap();
    let mut second = manager.create_transaction().await.unwrap();
    for transaction in [&mut first, &mut second] {
        assert!(!values.contains_key(transaction, &"left".into()).unwrap());
        assert!(!values.contains_key(transaction, &"right".into()).unwrap());
    }
    values.set(&mut first, &"left".into(), &1).unwrap();
    values.set(&mut second, &"right".into(), &1).unwrap();
    first.commit().await.unwrap();
    assert!(matches!(second.commit().await, Err(Error::Conflict(_))));

    let mut absent = manager.create_transaction().await.unwrap();
    assert_eq!(values.get(&mut absent, &"key".into()).unwrap(), None);
    let mut insert = manager.create_transaction().await.unwrap();
    values.set(&mut insert, &"key".into(), &1).unwrap();
    insert.commit().await.unwrap();
    let mut remove = manager.create_transaction().await.unwrap();
    values.remove(&mut remove, &"key".into()).unwrap();
    remove.commit().await.unwrap();
    assert!(matches!(absent.commit().await, Err(Error::Conflict(_))));

    let mut aborted = manager.create_transaction().await.unwrap();
    values.set(&mut aborted, &"aborted".into(), &1).unwrap();
    aborted.abort();
    let expired = manager
        .transaction_with_options(TransactionOptions {
            timeout: Duration::from_millis(1),
            ..Default::default()
        })
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    assert!(matches!(expired.commit().await, Err(Error::Expired)));

    let mut original = manager.create_transaction().await.unwrap();
    let identity = original.id().clone();
    values.set(&mut original, &"retry".into(), &7).unwrap();
    let version = original.commit().await.unwrap();
    let mut duplicate = manager
        .create_transaction()
        .await
        .unwrap()
        .with_identity(identity.clone())
        .unwrap();
    values.set(&mut duplicate, &"retry".into(), &7).unwrap();
    assert_eq!(duplicate.commit().await.unwrap(), version);
    assert_eq!(
        manager.committed_result(identity.clone()).await.unwrap(),
        Some(version)
    );
    let mut incompatible = manager
        .create_transaction()
        .await
        .unwrap()
        .with_identity(identity)
        .unwrap();
    values.set(&mut incompatible, &"retry".into(), &8).unwrap();
    assert!(matches!(
        incompatible.commit().await,
        Err(Error::DuplicateRequest)
    ));
    let mut read = manager.create_transaction().await.unwrap();
    assert_eq!(values.get(&mut read, &"aborted".into()).unwrap(), None);
    assert_eq!(values.get(&mut read, &"retry".into()).unwrap(), Some(7));
}

#[tokio::test]
async fn transaction_context_admission_is_bounded_and_released() {
    let directory = tempfile::tempdir().unwrap();
    let (_, manager) = open_manager(directory.path().join("one")).await;
    let mut transactions = Vec::new();
    for _ in 0..16 {
        transactions.push(manager.create_transaction().await.unwrap());
    }
    assert!(matches!(
        manager.create_transaction().await,
        Err(Error::ResourceExhausted)
    ));
    transactions.clear();
    assert!(manager.create_transaction().await.is_ok());
}

#[tokio::test]
async fn committed_progress_does_not_checkpoint_each_commit_and_recovers_after_restart() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let (state, manager) = open_manager(&path).await;
    let values = manager
        .get_or_add_dictionary::<String, i64>("values")
        .await
        .unwrap();
    let mut last_version = 0;
    for value in 0..20 {
        let mut transaction = manager.create_transaction().await.unwrap();
        values
            .set(&mut transaction, &format!("key-{value}"), &value)
            .unwrap();
        last_version = transaction.commit().await.unwrap().0;
    }
    let progress = state.durable_progress().await.unwrap();
    assert_eq!(progress.applied_lsn, last_version);
    assert_eq!(progress.committed_lsn, last_version);
    drop(manager);
    drop(state);

    let log = TransactionLog::open(path.clone()).unwrap();
    assert!(log.checkpoint_record().is_none());
    assert_eq!(log.records().len(), last_version as usize);
    drop(log);

    let (state, manager) = open_manager(&path).await;
    let progress = state.durable_progress().await.unwrap();
    assert_eq!(progress.applied_lsn, last_version);
    assert_eq!(progress.committed_lsn, last_version);
    assert_eq!(manager.applied_lsn().await.unwrap(), last_version);
}

#[tokio::test]
async fn copy_state_retries_frozen_boundary_after_later_commits_and_restart() {
    let directory = tempfile::tempdir().unwrap();
    let source_path = directory.path().join("source");
    let (source_state, manager) = open_manager(&source_path).await;
    let mut create = manager.create_transaction().await.unwrap();
    let left = create.get_or_add_dictionary::<String, i64>("left").unwrap();
    let right = create
        .get_or_add_dictionary::<String, i64>("right")
        .unwrap();
    left.set(&mut create, &"balance".into(), &90).unwrap();
    right.set(&mut create, &"balance".into(), &110).unwrap();
    let identity = create.id().clone();
    let version = create.commit().await.unwrap();
    assert_eq!(
        manager.committed_result(identity.clone()).await.unwrap(),
        Some(version)
    );
    let provider = ReliableCollectionsProvider::new(source_state.clone());
    let first = copy_bytes(&provider, version.0).await;
    let mut later = manager.create_transaction().await.unwrap();
    left.set(&mut later, &"after-boundary".into(), &1).unwrap();
    later.commit().await.unwrap();
    let second = copy_bytes(&provider, version.0).await;
    assert_eq!(first, second);
    drop(provider);
    drop(manager);
    drop(source_state);
    let reopened = Arc::new(ReliableCollectionsState::open(source_path.clone()).unwrap());
    let provider = ReliableCollectionsProvider::new(reopened);
    let restarted = copy_bytes(&provider, version.0).await;
    assert_eq!(first, restarted);
    drop(provider);
    let (_, manager) = open_manager(&source_path).await;

    let target_state =
        Arc::new(ReliableCollectionsState::open(directory.path().join("target")).unwrap());
    let build = OperationId::new("copy-build");
    target_state
        .apply_copy_chunk(&build, 1, CopyChunk { data: first.into() })
        .await
        .unwrap();
    let progress = target_state
        .finish_copy(&build, version.0, version.0)
        .await
        .unwrap();
    assert_eq!(progress.applied_lsn, version.0);
    assert_eq!(
        target_state
            .finish_copy(&build, version.0, version.0)
            .await
            .unwrap(),
        progress
    );
    let copied = StateManager::standalone(target_state).unwrap();
    let mut read = copied.create_transaction().await.unwrap();
    let copied_left = read.get_dictionary::<String, i64>("left").unwrap().unwrap();
    assert_eq!(
        copied_left.get(&mut read, &"balance".into()).unwrap(),
        Some(90)
    );

    let backup = directory.path().join("backup");
    manager.backup(backup.clone()).await.unwrap();
    let (_, restored) = open_manager(directory.path().join("restored")).await;
    restored.restore_backup(backup).await.unwrap();
    let mut restored_read = restored.create_transaction().await.unwrap();
    assert_eq!(
        restored_read.provider_names().unwrap(),
        vec!["left", "right"]
    );
}

#[tokio::test]
async fn retained_replication_ranges_are_contiguous_and_fail_closed_after_checkpoint() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("state");
    let (state, manager) = open_manager(&path).await;
    let values = manager
        .get_or_add_dictionary::<String, i64>("values")
        .await
        .unwrap();
    let mut last_version = 0;
    for value in 0..5 {
        let mut transaction = manager.create_transaction().await.unwrap();
        values
            .set(&mut transaction, &format!("key-{value}"), &value)
            .unwrap();
        last_version = transaction.commit().await.unwrap().0;
    }
    let operations = state
        .get_replication_operations(2, last_version)
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(
        operations
            .iter()
            .map(|operation| operation.lsn)
            .collect::<Vec<_>>(),
        (2..=last_version).collect::<Vec<_>>()
    );

    manager.checkpoint().await.unwrap();
    let Err(error) = state.get_replication_operations(1, 1).await else {
        panic!("expected retained replication range to fail after checkpoint");
    };
    assert!(
        error
            .to_string()
            .contains("predates retained checkpoint boundary"),
        "{error}"
    );
}

async fn copy_bytes(provider: &ReliableCollectionsProvider, up_to_lsn: i64) -> Vec<u8> {
    let empty: OperationDataStream = Box::pin(futures::stream::empty());
    let mut stream = provider.get_copy_state(up_to_lsn, empty).await.unwrap();
    let mut bytes = Vec::new();
    while let Some(chunk) = stream.next().await {
        bytes.extend_from_slice(&chunk.unwrap());
    }
    bytes
}
