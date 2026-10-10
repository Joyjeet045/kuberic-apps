# kuberic-rustfs

RustFS example for [issue #5](https://github.com/youyuanwu/kuberic-apps/issues/5).
RustFS owns object storage, replication, erasure coding, quorum, and healing.
Kuberic integrates lifecycle, health, leased client access, and append-only pool
expansion through `kuberic_native_runtime::native::NativeApplication`.

This follows the [PostgreSQL example's](https://github.com/youyuanwu/kuberic/tree/131ccc9ddd7dbba9fb910d1000d4fbd2206c548a/examples/postgres)
native-replication ownership boundary, not RocksDB's log replication. There is
no second data quorum, shard-copy implementation, synthetic LSN, or RustFS
primary/standby role. Like the other applications, this is a workspace crate
with a pinned dependency, Docker image, Kubernetes manifests, and Rust tests.

**Status: experimental example, not production-certified.** The native framework
boundary depends on [prerequisite PR #141](https://github.com/youyuanwu/kuberic/pull/141).
Page/RocksDB retain their existing framework revision. Production adoption also
requires deployment-specific storage, backup/restore, upgrade, failure-domain,
capacity, and certificate-rotation qualification.

## Supported behavior

- Start, supervise, stop/reap, and reopen the checksum-pinned RustFS foreground
  process on the same storage.
- Observe RustFS's own liveness, readiness, read-health, and write-health.
- Fence client connections using incarnation/revision-bound expiring authority.
- Durably journal idempotent `restart` and append-only `expand` operations.
- Require the exact native pool map and native readiness before completion.

**Not supported:** decommission, pool retirement/removal, shrinking or replacing
existing pools, changing erasure width, data migration, automatic upgrades, or
deleting storage. These requests fail explicitly. RustFS 1.0.1 decommission did
not complete in local distributed validation because of unresolved native
`.rustfs.sys/buckets/.heal/` metadata; that workflow is deliberately not exposed.
Do not deploy this revision over a journal with an unfinished operation from an
earlier experimental decommission implementation.

## Build and test

Use the pinned workspace toolchain and `protoc`. This crate requires Rust 1.95+.

```sh
cargo fmt --all -- --check
cargo clippy --locked -p kuberic-rustfs --all-targets --all-features -- -D warnings
cargo test --locked -p kuberic-rustfs --all-features
docker build --file kuberic-rustfs/Dockerfile --tag kuberic-rustfs:local .
```

The default Rust tests require no cluster or RustFS installation. They cover
health contracts, TLS validation, authority transport, topology identity,
durable intent/replay, invalid input, process ownership, and cancellation.
The Docker build also runs Linux ownership tests and a real TLS S3
put/get/restart/reopen test as the non-root runtime user. CI runs that same image
build on pull requests. PowerShell and kind are not repository dependencies.

For a separately installed executable matching [rustfs-release.json](rustfs-release.json),
set `KUBERIC_RUSTFS_TEST_BINARY` and `KUBERIC_RUSTFS_TEST_SHA256` to its absolute
path and executable checksum, then run:

```sh
cargo test --locked -p kuberic-rustfs --test native --test adapter -- --ignored
```

The manifest pins RustFS **1.0.1**, including release archive and executable
checksums. The executable is rehashed before every spawn. Its file and parent
directories must be administrator-controlled and immutable. The runtime image
uses Ubuntu 24.04 for native glibc compatibility and includes a CA trust store.

Local validation also exercised an isolated four-node HTTPS pool, native quorum
loss, same-PVC recovery, NetworkPolicy isolation, lease expiry, and expansion
to eight nodes, checking hashes of 128 acknowledged one-MiB objects. This does
not certify physical disk power-loss durability, arbitrary partitions, or
completed healing. The unverified decommission workflow and its orchestration
script are not shipped.

## Storage and process ownership

`LaunchConfig` specifies a `BinaryPin`, credential files, state directory,
ordered `Topology`, listen address, native TLS, and shutdown grace period.
Startup means process creation, not readiness. Even an unrequested exit code
zero is a service failure.

- State and volume directories must exist, be dedicated to the instance, and
  resolve to disjoint paths. Fresh state can adopt only empty volumes.
- `topology.json` persists exact pool order, local origin, erasure width, and
  resolved local paths before launch. Unknown/corrupt/mismatched records fail
  closed; existing storage is never reformatted or copied.
- Increasing non-padded numeric ellipses such as `{0...3}` are supported, bounded
  to 1024 endpoints. Erasure width must be 2-16 and divide each pool's endpoint
  count. RustFS performs final native layout and quorum validation.
- Bootstrap configuration stays unchanged after initialization. The SQLite WAL
  journal, with FULL synchronization, owns subsequent desired topology and
  operation receipts. One operation may be pending per node; an ID cannot be
  reused with different input.
- Linux inherits an exclusive ownership lock into the native child. Reopening
  cannot proceed while an orphan retains that lock. A synced `process-active`
  marker records unresolved ownership; no PID guessing is used.
- Unix shutdown sends SIGTERM, then forces termination after the grace period.
  Reaping after forced termination is bounded to five seconds. Windows uses
  immediate termination. `shutdown()` waits; dropping the handle only requests
  shutdown while the supervisor retains ownership.
- Windows and legacy/unrecognized active markers fail closed. First fence the
  previous host and prove it cannot access storage; only then remove that
  specific marker. Never remove topology, locks, or native metadata to force
  startup. Windows directory-entry power-loss durability is not certified.

Locks coordinate this adapter's instances, not unmanaged processes or another
state directory using the same disks. Storage ownership remains an operator
responsibility.

## TLS, credentials, and access fencing

Distributed and non-loopback native listeners require
`native_tls: {"directory": "/tls"}`. The directory contains
`rustfs_cert.pem`, `rustfs_key.pem`, and a nonempty `ca.crt` bundle, following the
[pinned native TLS layout](https://github.com/rustfs/rustfs/blob/1.0.1/crates/tls-runtime/src/source.rs).
Plaintext launch is limited to loopback-only local storage and client listeners.
Distributed pool URLs must consistently use HTTPS; the scheme is persistent
topology identity, not an in-place migration switch.

RustFS terminates native and S3 TLS. The gateway forwards encrypted bytes
unchanged. Adapter health/admin clients verify the configured CA and loopback
IP certificate identity. Control TLS requires `control_tls.certificate_file`
and `control_tls.private_key_file`; the controller requires HTTPS origins and
`ca_certificate_file`. Certificate and hostname verification are never disabled.
No redirects or environment proxies are used.

Control requests also require a bearer token from `control_token_file`,
containing 32-256 printable non-whitespace ASCII characters. Native credentials
come from `credentials.access_key` and `credentials.secret_key` files; the native
admin client signs requests with AWS Signature V4. Secrets are not command-line
arguments or topology data. The child's environment is cleared except for
platform essentials and explicit launch settings. Console/update checks are off.

Client authority starts closed and is scoped to a random adapter incarnation,
monotonic revision, and a lease of at most 30 seconds. Revocation, expiry,
shutdown, and native process failure close existing gateway connections. This
cannot roll back a request already accepted by RustFS. Native peer traffic is
never gated: **NetworkPolicy must prevent clients from bypassing the gateway**.
Health does not grant authority or prove durable acknowledgement/healing.

## Control API

All routes use HTTPS. Only `/ready` and `/live` are unauthenticated.

| Method | Path | Meaning |
| --- | --- | --- |
| GET | `/v1/native/observation` | Incarnation, revision, access, and native health or explicit unavailability |
| POST | `/v1/native/authority` | Exact-incarnation, revision-fenced authority |
| POST | `/v1/native/operation` | Closed authority and idempotent operation |
| POST | `/v1/native/operation/status` | Receipt lookup using exact operation ID/input |
| GET | `/ready` | Client authority and native readiness |
| GET | `/live` | Owned foreground process has not exited |

The independent `HealthClient` probes `/health/live`, `/health/ready`,
`/minio/health/cluster/read`, and `/minio/health/cluster`. Only HTTP 200 and 503
are native healthy/unhealthy verdicts. All other statuses, transport errors,
and timeouts are explicit observation failures, never cached success.

## Deploy

Use four schedulable Linux AMD64 nodes, a dynamic default StorageClass, and a
NetworkPolicy-enforcing CNI. Each native process owns one PVC; pool anti-affinity
separates its drives across nodes. Publish the image and replace the local image
references in [base.yaml](deploy/base.yaml) and [expand.yaml](deploy/expand.yaml)
with an immutable registry digest before deploying outside a local cluster.

Create namespace `rustfs-example` and these Secrets from protected files:

```sh
kubectl create namespace rustfs-example
kubectl -n rustfs-example create secret generic rustfs-credentials \
  --from-file=access-key=/secure/access-key \
  --from-file=secret-key=/secure/secret-key \
  --from-file=control-token=/secure/control-token
kubectl -n rustfs-example create secret generic rustfs-tls \
  --from-file=ca.crt=/secure/ca.crt \
  --from-file=tls.crt=/secure/tls.crt \
  --from-file=tls.key=/secure/tls.key
kubectl apply -f kuberic-rustfs/deploy/base.yaml
kubectl -n rustfs-example rollout status statefulset/rfs-a --timeout=300s
kubectl -n rustfs-example port-forward service/rustfs-s3 9002:9002
```

`tls.crt` contains the serving certificate and intermediate chain; `tls.key`
is its unencrypted PEM key. Include DNS SANs for `rfs-a-0.rustfs-internal`
through `rfs-a-3.rustfs-internal`, the four B names before expansion, and S3
service names clients use. Include IP SAN `127.0.0.1` for local probes and
port-forwarding (`::1` if configured). Use an S3 client at
`https://127.0.0.1:9002` with the CA bundle; never disable verification.

The controller mounts only the control token and CA, not S3 credentials or the
serving key. Kubernetes HTTPS probes do not validate server certificates, but
the controller does. Membership labels, plans, Secrets, and storage require
trusted administrators. Use least-privilege native S3 identities for clients.

Certificates/tokens are loaded at adapter startup. Renew before expiry and
restart deliberately with the same PVCs and identity. Distribute overlapping
CA trust to the controller before rotating certificates. Coordinate token
rotation as maintenance; mixed tokens fail closed. Resource/PVC limits in the
manifests are examples, not production sizing.

## Restart and expand

The controller reloads the `controller.json` ConfigMap. Use one trusted,
persistent plan, positive increasing revisions, and stable direct HTTPS node
origins. Revision N maps to closed authority 2N and final authority 2N+1. Do not
reuse a revision or operation ID with changed input or restore a stale plan.
ConfigMap projection is asynchronous; wait for observed application, not just
successful `kubectl apply`.

A node's operation is `null` or has one of these shapes:

```json
{"id":"restart-2","request":{"kind":"restart","topology":{"pools":["https://rfs-a-{0...3}.rustfs-internal:9000/storage/data"],"local_node":"https://rfs-a-0.rustfs-internal:9000","erasure_set_drive_count":4}}}
```

```json
{"id":"expand-3","request":{"kind":"expand","previous":{"pools":["https://rfs-a-{0...3}.rustfs-internal:9000/storage/data"],"local_node":"https://rfs-a-0.rustfs-internal:9000","erasure_set_drive_count":4},"target":{"pools":["https://rfs-a-{0...3}.rustfs-internal:9000/storage/data","https://rfs-b-{0...3}.rustfs-internal:9000/storage/data"],"local_node":"https://rfs-a-0.rustfs-internal:9000","erasure_set_drive_count":4}}}
```

For expansion, apply [expand.yaml](deploy/expand.yaml), then publish a higher
revision containing all eight nodes: A nodes receive node-specific `expand`
operations and B nodes have `operation: null` because their bootstrap already
includes both pools. Preserve exact existing pool order, local origins, and
erasure width; new local volumes must exist and be empty.

The controller fences all participants before a transition and requires durable
completion from each before reopening. Accepted intent is persisted before
restart and recovered forward-only. Completion requires the exact pool map,
active added pools, and native readiness, not completed healing. Established
reachable nodes keep receiving leases when a peer is unavailable; RustFS
decides data quorum, not an all-nodes Kuberic check.

RustFS 1.0.1 can latch pool-metadata recovery after expansion. If coordinated
maintenance is needed, publish a higher revision with all nodes,
`enabled: false`, and no operations; wait for closed access everywhere. Stop
both StatefulSets completely, retaining PVCs and bootstrap ConfigMaps, before
restoring replicas. Wait for native `health.ready` in authenticated observations,
not Pod readiness (which also requires authority), then enable a higher plan
revision. Verify acknowledged objects and native read/write health.

Never shrink StatefulSets as a topology change, remove pool endpoints, erase
journals, or delete native metadata to resolve a pending operation.
