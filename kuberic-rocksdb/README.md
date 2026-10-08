# kuberic-rocksdb

An experimental, single-primary RocksDB application using the published Kuberic
runtime, following the [page application's](../kuberic-page/README.md) host/API
pattern. Application-specific storage stays in this repository; no private
framework APIs or path dependencies are used in production.

## Build and validation

Use the repository's pinned toolchain. This crate requires Rust 1.88+, a C++
compiler, libclang with its resource headers, and `protoc`. On Debian/Ubuntu:

```sh
sudo apt-get install build-essential clang libclang-dev protobuf-compiler
cargo test --locked -p kuberic-rocksdb -- --test-threads=2
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

On Windows, install the Visual C++ build tools, Windows SDK, LLVM, and `protoc`.
If libclang is not discoverable, point `LIBCLANG_PATH` at the LLVM binary
directory. A standalone libclang DLL also needs the matching Clang resource
headers; set `BINDGEN_EXTRA_CLANG_ARGS` to `-I"<resource-headers-directory>"`.

The dependency is `kuberic-runtime =0.0.1`, the latest published runtime when
this application was added. CI independently validates the current framework
main branch using a Cargo patch without changing the application's manifest.
For an equivalent local compatibility run, supply
`--config "patch.crates-io.kuberic-runtime.path='<framework-runtime-path>'"`
before Cargo's `test` subcommand. Run Cargo again without the override to return
the generated lockfile to the published dependency.

## HTTP API

Only an authorized primary serves client reads and writes. Closed access returns
HTTP 503. Runtime/storage failures return explicit errors rather than successful
fallbacks. Missing keys return 404.

| Method | Path | Request | Successful response |
| --- | --- | --- | --- |
| PUT | `/keys/{key}` | Opaque value bytes | `{"lsn":1}` |
| GET | `/keys/{key}` | None | Opaque committed value bytes |
| DELETE | `/keys/{key}` | None | `{"lsn":2}` |
| POST | `/batch` | JSON mutations | `{"lsn":3}` |

Keys in URL routes are UTF-8. The batch interface supports arbitrary binary keys
and values as JSON byte arrays, including empty keys and values:

```json
{
  "column_family": "default",
  "operations": [
    {"op": "put", "key": [107], "value": [65]},
    {"op": "merge", "key": [107], "value": [66]},
    {"op": "delete", "key": [120]}
  ]
}
```

This writes `k = AB` and deletes `x` atomically. Merge is deterministic byte
concatenation (`append-v1`), not integer addition. Operations in one batch
execute in order. There is no cross-request transaction or read-modify-write API.

The client API permits 1-1024 mutations, keys up to 64 KiB, serialized RocksDB
batches up to 4 MiB, and HTTP/replication envelopes up to 32 MiB. Invalid input,
unknown fields/operations, and non-default user column families are rejected.

## Replication and durability

The adapter creates canonical serialized RocksDB `WriteBatch` values inside a
versioned, checksummed envelope. Receivers verify the storage profile, supported
mutations, and exact canonical batch bytes before reconstructing a native batch.
It does not tail `GetUpdatesSince()` or replicate raw WAL file fragments.

The current V2 default replicator durably accepts an operation locally before
quorum completion. Therefore this adapter has two private data projections:

1. **Accepted state:** applying a delivered batch, retained operation bytes,
   undo information, and its Kuberic applied LSN is one WAL-enabled, synced
   RocksDB write. A secondary ACK follows this durable acceptance.
2. **Committed state:** after the runtime authorizes commitment, the original
   batch and its committed LSN are applied atomically to the client-visible
   default column family with another synced write.

Client success requires both quorum completion and local application commitment.
The primary does not publish user-visible mutations before quorum. `GET` never
reads the accepted projection, so a crash or promotion cannot expose an arbitrary
uncommitted suffix. Kuberic LSNs are separate from RocksDB sequence numbers.

V2 owns epoch authority, verified-prefix selection, pending-write reconciliation,
and write-access grants. The adapter retains unresolved accepted operations;
it does not invent an epoch rollback or discard evidence needed by runtime
recovery. An unreconciled suffix remains invisible and may require recovery or
rebuilding before new writes are granted. Role callbacks alone grant no access.

Exact retries require identical LSN, envelope, and original committed watermark.
Conflicts and gaps fail explicitly. An uncertain storage-write outcome fences
the store until reopen; it cannot cause a merge to be blindly replayed.
Client-side exactly-once retries are not provided: a timeout or disconnect can
have an unknown outcome, especially for merge operations.

## Checkpoint copy and restart

Copy uses actual RocksDB checkpoints, not an exported logical key/value map.
For a frozen committed LSN, retained undo records remove later committed and
accepted changes from a temporary checkpoint. A second physical checkpoint of
that exact state is packaged with bounded filenames, lengths, profile, LSN, and
checksum validation. Its exact bytes are persisted before the first chunk is
returned, so retries and source restarts return identical bytes.

Copy is sent in 64 KiB chunks, limited to a 128 MiB checkpoint bundle. The receiver
persists each chunk before ACK, verifies exact duplicate sequences, and rejects
gaps/conflicts. It opens and validates the restored checkpoint in a new database
generation before atomically publishing the active generation and build receipt
in a synced catalog write. Retry completion does not reinstall an older image
over newer catch-up. Incremental replay begins at `copy_lsn + 1`; requests below
the installed base fail explicitly and require a new checkpoint build.

Storage under `KUBERIC_DATA_ROOT`:

```text
.kuberic/agent.sqlite3      Runtime-owned identity, authority and effect journal
application/catalog/      Active generation, copy chunks/receipts, checkpoint cache
application/generation-N/ RocksDB committed/accepted state, progress and history
```

The application opens storage only from its authorized service lifecycle.
Startup distinguishes fresh from established application storage. Missing or
incompatible established metadata fails closed, rather than starting an empty
database. Filesystem and device flush guarantees are prerequisites; tests cover
process crashes, not arbitrary hardware/power-loss behavior.

## Compatibility and explicit restrictions

- Deploy the same application image, pinned RocksDB wrapper `0.25.0`, native
  RocksDB `11.8.1`, and fixed options on every replica. Build with the committed
  lockfile. Do not substitute a system RocksDB library via build environment
  overrides. Mixed engine/profile/envelope versions are rejected.
- Only the **default user column family** is supported. Two fixed private
  column families hold accepted state and metadata; user batch bytes cannot
  address them. Dynamic column-family creation is not exposed.
- The comparator is bytewise, compression is disabled, and the merge operator
  is fixed. There are no configurable comparators, prefix extractors or table
  formats.
- No direct DB handle, `disableWAL`, external SST ingestion, `TransactionDB`,
  `WritePrepared`, or `WriteUnprepared` write path is exposed. Unsupported
  mutations are rejected, not silently omitted.
- History, undo data, completed copy receipts, checkpoint caches and old database
  generations are retained. Automatic pruning/garbage collection is not provided.
  Monitor disk usage; do not manually delete active or retained files.
- Physical checkpoint creation/restoration is serialized with local storage
  operations and can pause writes. Snapshot packaging uses memory proportional
  to the bounded bundle size. This is not a large-database backup service.
- There is no old V1 data migration, full Service Fabric feature parity, mixed
  runtime protocol upgrade, external HTTP authentication, or TLS termination.
  Expose client traffic only through an appropriately secured network boundary.

RocksDB's C++ `Env`/`FileSystem` WAL interception could support a different native
integration, but requires additional FFI and handling physical WAL record
fragmentation/grouping. It is intentionally not used by this batch adapter.

## Run and deploy

The binary uses the page application's replica-process inputs:
`KUBERIC_RESOURCE_UID`, `KUBERIC_REPLICA_ID`, `KUBERIC_POD_UID`,
`KUBERIC_PVC_UID`, `KUBERIC_POD_IP`, `KUBERIC_NAMESPACE`, and
`KUBERIC_AGENT_BEARER_TOKEN`. Optional addresses are
`KUBERIC_CONTROL_ADDRESS` (50051), `KUBERIC_REPLICATION_ADDRESS` (50052), and
`KUBERIC_APPLICATION_ADDRESS` (8080); the data root defaults to `/var/lib/kuberic`.
These inputs do not self-elect a primary: normal runtime initialization and
controller-granted authority are required. SIGINT/SIGTERM stop the host.

Build the image from the repository root:

```sh
docker build -f kuberic-rocksdb/Dockerfile -t localhost/kuberic-rocksdb:dev .
```

Native RocksDB compilation can take tens of minutes on a cold build.

In an isolated development cluster with the matching experimental Kuberic
controller/CRD and persistent-storage provisioner installed, load/publish that
image and apply [the example KubericSet](deploy/kubericset.yaml).
The current V2 controller creates the application Service `rocksdb-write`:

```sh
kubectl apply -f kuberic-rocksdb/deploy/kubericset.yaml
kubectl port-forward service/rocksdb-write 8080:80
curl -X PUT --data-binary Alice http://localhost:8080/keys/customer
curl http://localhost:8080/keys/customer
```

Reconnect a port-forward after failover; it is not itself a durable client
connection. This repository does not provision or mutate an existing cluster
automatically.

## Test coverage

Tests exercise native RocksDB storage, the actual default runtime replicator,
explicit durable stream ACKs, and the public HTTP router. Coverage includes
batches/merge/delete, concurrent LSN ordering, authority fencing, restart,
checkpoint retry and historical boundaries, incremental catch-up, planned
switchover, primary failure before/after quorum, secondary restart, corruption
and unsupported inputs. Subprocess tests exit without destructors to verify
durable acceptance and commitment across abrupt process termination.

The deterministic runtime fixture supplies controller decisions through the
runtime's `testing` feature; it is not production self-election logic and does
not start Kubernetes. The full workspace suite passed against both the published
runtime and framework main at `131ccc9ddd7dbba9fb910d1000d4fbd2206c548a`.

Separate live validation built the Linux application image and the controller
from that framework revision, then deployed them into an isolated three-node
kind cluster running Kubernetes 1.34.0. It verified:

- Three-replica bootstrap, quorum writes, and atomic put/merge/delete batches.
- Secondary Pod replacement and recovery into a new persistent volume.
- Planned switchover, with the still-running old primary returning HTTP 503 for
  both reads and writes.
- Forced primary Pod deletion, automatic failover, preservation of every
  acknowledged value, and continued writes.
- Secondary process restart on the same Pod/PVC, followed by successful
  promotion with the original values, no duplicated merge, and no resurrected
  deleted data.

This is development validation, not production qualification, power-loss
certification, or proof of cross-version storage compatibility.
