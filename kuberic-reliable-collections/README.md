# Kuberic Reliable Collections

`kuberic-reliable-collections` ports the archived V1 Reliable Collections MVP
from `youyuanwu/kuberic#78` (preserved in Joyjeet045/kuberic at tag
`archive/pr-78-reliable-collections-v1`) to the current V2
`kuberic-runtime = 0.0.1` application model.

The crate provides:

- `StateManager` for optimistic transactions.
- `ReliableDictionary<K, V>` named providers with typed keys and values.
- `ReliableCollectionsService`, a `StatefulServiceReplica` that uses
  `DefaultReplicatorFactory`, consumes V2 replication and copy streams, and
  acknowledges only after local durable persistence.
- A transaction log used as the application-owned durable state under
  `engine::DurableState`.

## API

Values implement `ReliableValue`, which requires deterministic `serde`
serialization plus a stable `TYPE_ID` and schema `VERSION`. Built-in values are
`String`, `Vec<u8>`, `i64`, `u64`, and `bool`.

```rust,no_run
use kuberic_reliable_collections::{ReliableCollectionsService, Result};

async fn transfer(service: &ReliableCollectionsService) -> Result<()> {
    let manager = service.state_manager();
    let accounts = manager.get_or_add_dictionary::<String, i64>("accounts").await?;
    let audit = manager.get_or_add_dictionary::<String, String>("audit").await?;

    let mut tx = manager.create_transaction().await?;
    let alice = accounts.get_or_add(&mut tx, &"alice".to_owned(), 100)?;
    let bob = accounts.get_or_add(&mut tx, &"bob".to_owned(), 100)?;
    accounts.set(&mut tx, &"alice".to_owned(), &(alice - 10))?;
    accounts.set(&mut tx, &"bob".to_owned(), &(bob + 10))?;
    audit.set(&mut tx, &"last-transfer".to_owned(), &"alice -> bob: 10".to_owned())?;
    let id = tx.id().clone();
    let version = tx.commit().await?;
    assert_eq!(manager.committed_result(id).await?, Some(version));
    Ok(())
}
```

`ReliableDictionary` supports `get`, `contains_key`, `insert`, `set`,
`update`, `remove`, `get_or_add`, `add_or_update`, `clear`, and `entries`.
Provider creation and removal can be staged in the same transaction as data
changes. `get_dictionary` rejects incompatible value type IDs or versions, and
old handles stop working after a provider is removed and recreated.

## Isolation and retries

Transactions capture a repeatable begin-time registry snapshot and read their
own writes. Commit validates provider identities, per-key logical versions,
dictionary scan revisions, and registry enumeration revisions against the
current durable state. Delete tombstones retain versions so absent-key ABA is
detected. Conflicts return `Error::Conflict`; retry the application logic in a
new transaction.

Each commit, including read-only commits, is serialized as one checked envelope.
On a V2 primary, the envelope is passed once to `StateReplicator::replicate`.
The V2 engine durably applies primary writes locally before quorum completion;
secondary stream consumers persist before acknowledging. A successful commit
response requires local durable acceptance and quorum commit.
Transactional reads on a primary use the locally applied durable image. During
failover recovery that image may include records whose quorum confirmation is
still in flight or adopted from an election-safe prefix. A successful commit,
including a read-only commit, confirms the prefix observed by that transaction;
plain reads do not by themselves advance the confirmed boundary.

`TransactionId` supplies retry identity. The latest 1,024 outcomes are retained
through restart, checkpoint, copy, and backup. Reusing a retained request ID or
transaction ID with different mutations returns `Error::DuplicateRequest`.
`committed_result` returns `Error::UnconfirmedCommit` when an adopted result is
durable locally but not yet confirmed by the V2 committed prefix. Retrying the
identical transaction or committing a new read-only transaction advances the
confirmation record without reapplying old mutations.

## Durability, copy, checkpoint, and backup

The application-owned log stores checksummed records, drops only an incomplete
torn tail, rejects corrupt complete records, publishes checkpoints through a new
generation and atomic pointer, and uses an exclusive directory lock. The V2
runtime owns quorum ordering, write fencing, election-safe prefix settlement,
and retained catch-up delivery; the V1 epoch rollback callback is therefore not
reimplemented in the application layer.

`StateProvider::get_copy_state(up_to_lsn)` returns deterministic bytes for a
committed frozen boundary, including after checkpointing and restart. Requested
copy snapshots are persisted and carried into subsequent checkpoint generations;
they are not automatically pruned, so provision disk for retained copy snapshots.
Installing a new copy starts a new snapshot history rather than reusing cached
bytes from the replaced state. `finish_copy` installs a
complete registry snapshot atomically and is idempotent for the same build and
boundary. Committed progress is persisted as a small, generation-local durable progress record on
ordinary commits; it does not rewrite the full checkpoint or reclaim retained
operations. A newly installed generation cannot inherit a previous generation's
commit watermark, including a crash immediately after publication.
A full checkpoint is published only by explicit `checkpoint()` or
automatically when the next transaction would push retained log bytes beyond
half of the 64 MiB retained-log budget. Checkpoint publication uses the current
committed prefix and retains any unconfirmed suffix. `backup()` writes a local
atomic snapshot only when the applied and committed boundaries match.
`restore_backup()` is allowed before a service attaches the manager to a running
runtime.

## Limits and scope

- 16 outstanding transaction contexts.
- 60-second maximum transaction timeout.
- 1,024 dictionaries.
- 1,024 observed keys or staged changes per transaction.
- 64 KiB encoded keys.
- 1 MiB transaction envelopes.
- 8 MiB snapshots and copy images.
- 64 MiB retained log bytes.
- 1,024 retained retry outcomes.

Reliable queues, pessimistic locks, group commit, schema migration, external
backup storage, and cluster-wide restore orchestration are outside this MVP.
