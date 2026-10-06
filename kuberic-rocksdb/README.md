# Kuberic RocksDB

`kuberic-rocksdb` is a V2 Kuberic application crate that stores replicated key/value state in RocksDB. It supersedes `youyuanwu/kuberic#76`, archived at tag `archive/pr-76-rocksdb-v1` in `Joyjeet045/kuberic`, and ports the durable state-provider ideas to the published `kuberic-runtime = 0.0.1` interfaces.

## API

The library exposes:

- `RocksState`: a RocksDB-backed `engine::DurableState` implementation.
- `RocksStateProvider`: an `application::StateProvider` for copy, epoch and committed-progress callbacks.
- `RocksService`: a `StatefulServiceReplica` using `DefaultReplicatorFactory`.
- `rocksdb_router`: an axum router with:
  - `PUT /keys/{key}` with raw bytes in the body;
  - `GET /keys/{key}`;
  - `DELETE /keys/{key}`;
  - `POST /batch` with JSON such as:

```json
[
  { "type": "put", "key": "account", "value": "100" },
  { "type": "merge", "key": "account", "value": "+25" },
  { "type": "delete", "key": "obsolete" }
]
```

Application writes call `StateReplicator::replicate` with a versioned, checksummed mutation envelope. Reads are allowed only when the runtime partition read status is `Granted`.

## Durability and ordering

Each replicated operation is decoded into typed `Put`, `Delete` and deterministic `Merge` mutations, validated against canonical RocksDB `WriteBatch` bytes, then applied with RocksDB WAL enabled and `sync=true`. The same write batch also persists:

- the applied Kuberic LSN;
- committed progress carried by the operation;
- the retained operation bytes;
- undo history for every touched user key.

Repeated delivery of the same LSN is idempotent only when the retained bytes and committed watermark match. LSN gaps and reused LSNs with different bytes fail explicitly. Later `commit` calls can only advance up to applied progress.

The adapter uses the default column family only, bytewise keys, no compression and the deterministic `append-v1` merge operator. User keys are encoded under a private prefix so they cannot collide with adapter metadata.

## Copy, exact boundaries and restart

V2 freezes copy at a committed LSN boundary and sends operations above that boundary as retained catch-up. `RocksState` reconstructs the exact requested boundary from current RocksDB user values plus persisted undo history, encodes the deterministic snapshot as bounded chunks, and produces identical bytes for the same boundary across repeated calls and restarts.

On the target, copy chunks are staged durably under the build ID. Exact retries verify identical bytes. `finish_copy` validates the version, checksum, profile and LSN, creates a new RocksDB generation from the copied snapshot, syncs it, and atomically publishes the active-generation pointer. Retrying `finish_copy` with the same staged bytes returns the same durable progress.

The V1 epoch-rollback callback is not carried forward as application rollback. In V2, the runtime engine owns authority fencing, verified prefixes and retained catch-up. The RocksDB adapter persists monotonic epoch observation but keeps applied operations and committed progress separate, allowing the engine to select the committed boundary without application-side tail deletion.

## Limits and supported configuration

- Up to 1,024 mutations per operation.
- Up to 1 MiB encoded operation envelope.
- Up to 256 MiB encoded copy snapshot.
- Default column family only.
- No compression.
- Deterministic `append-v1` merge.
- Native RocksDB I/O in `DurableState` callbacks runs on blocking workers.

Unlike the archived V1 PR, V2 copy is a deterministic logical RocksDB key/value snapshot rather than a physical checkpoint file bundle. It preserves exact-boundary copy, checksummed chunks, atomic install and retained catch-up while fitting the `kuberic-runtime` V2 copy contract.

## Build prerequisites

The `rocksdb` Rust binding builds native C++ code and bindgen output. Linux CI installs:

```sh
sudo apt-get update -qq
sudo apt-get install -y -qq clang libclang-dev
```

The repository validation command also installs `protobuf-compiler` for existing workspace tests.

## Run

The binary is hosted with `host::ReplicaHost` and accepts the same environment variables as `kuberic-page`, including `KUBERIC_RESOURCE_UID`, `KUBERIC_REPLICA_ID`, `KUBERIC_POD_UID`, `KUBERIC_PVC_UID`, `KUBERIC_POD_IP`, `KUBERIC_NAMESPACE`, `KUBERIC_AGENT_BEARER_TOKEN` and `KUBERIC_DATA_ROOT`. Application data lives under `$KUBERIC_DATA_ROOT/application`; existing data selects `ApplicationStorageState::Established`.

## Test

```sh
cargo test -p kuberic-rocksdb --all-features
```

The tests use real RocksDB directories and cover idempotent apply, LSN conflicts, gap rejection, commit bounds, restart progress recovery, exact-boundary copy, chunk verification and retry, malformed envelopes, deterministic merge, reserved metadata isolation and an end-to-end `kuberic_runtime::testing` happy path through the HTTP router.
