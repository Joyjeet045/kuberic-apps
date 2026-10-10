# kuberic-rustfs

Work toward [issue #5](https://github.com/youyuanwu/kuberic-apps/issues/5):
let RustFS own object storage, replication, erasure coding, quorum, and healing.
Kuberic will integrate lifecycle, health, and topology through a native adapter,
following the separation of responsibilities in the
[PostgreSQL example](https://github.com/youyuanwu/kuberic/tree/131ccc9ddd7dbba9fb910d1000d4fbd2206c548a/examples/postgres).
RustFS is not a PostgreSQL-style primary/standby engine.

## Consolidated change

This example is delivered in **one application PR**, covering the native
foundations, adapter, recovery, topology operations, deployment, and distributed
tests. A separate [framework prerequisite PR](https://github.com/youyuanwu/kuberic/pull/141) introduces its
opt-in `native` runtime and controller boundary. No application PR stack is
required. The native boundary deliberately does not implement the log-based
`Replicator` contract or use `KubericSet`.

### Adapter constraints

- Health is observation, not authority, durable acknowledgement, a replication
  position, or evidence that healing/topology changes have completed.
- S3, admin, and internode RPC share RustFS's S3 listener. Access fencing must
  not accidentally block internode traffic or reinterpret Kuberic's primary
  role as RustFS data leadership.
- Existing pool endpoint order and erasure-set layout are persistent identity.
  Do not implement scaling by changing the endpoint count of an existing pool,
  reordering drives, copying shards, or deleting RustFS format metadata.
- Keep credentials and RustFS data under their respective owners. This work
  must not introduce a second data-replication or quorum implementation.

## Native health probes

The health client remains usable independently of the adapter and coordinator.

Create a `HealthClient` with a direct node S3-listener origin and a positive
per-request timeout. HTTP and HTTPS are supported with certificate verification;
URL credentials, non-root paths, queries, fragments, and port zero are rejected.
The unauthenticated RustFS health endpoints must be enabled. Do not use a console
URL or a load balancer when observing a particular node.

| `HealthProbe` | GET path | Native signal |
| --- | --- | --- |
| `Liveness` | `/health/live` | Local process liveness, not storage readiness |
| `Readiness` | `/health/ready` | RustFS's node-readiness decision |
| `ClusterRead` | `/minio/health/cluster/read` | RustFS's cluster read-health decision |
| `ClusterWrite` | `/minio/health/cluster` | RustFS's cluster write-health decision |

`probe` returns `Healthy` for HTTP 200 and `Unhealthy` for HTTP 503. Other status
codes, including redirects, authentication errors, and disabled/missing
endpoints, return errors with probe context. Transport failures and timeouts are
errors too, never a cached success or a native verdict. Callers must surface
these errors rather than treating failed observations as health.

Each call makes a fresh request. Redirect following and environment proxies are
disabled so a probe does not silently observe another destination. Automatic
retries are disabled; the future supervisor owns polling and backoff. The client
uses status codes and does not consume response bodies: RustFS supports both
minimal and detailed payloads. It does not calculate quorum or combine the four
independent signals into a single "ready" flag.

## Native lifecycle and startup topology

`LaunchConfig` combines a `BinaryPin`, credential-file paths, a dedicated adapter
state directory, a `Topology`, the S3 listen address, and a shutdown grace period.
`RunningRustfs::start` returns after OS process creation, **not** application
readiness. Monitor `wait()` for an unexpected exit, and call `shutdown()` to stop
and reap the owned foreground child. Even a spontaneous exit code zero is an
unexpected service exit. Observe readiness separately.

### Executable and credentials

[rustfs-release.json](rustfs-release.json) pins RustFS **1.0.1**, versioned release
URLs, and archive checksums. Verify the archive before extraction.
`BinaryPin::new(absolute_executable_path, sha256)` takes the **executable's**
checksum, not the archive checksum; it is rechecked before every spawn. The
verified Windows and Linux executable checksums are recorded too.
Keep the executable and its parent directories administrator-controlled and
immutable between verification and launch. This library does not download,
upgrade, execute shell wrappers, or validate arbitrary RustFS versions.

Credentials must be supplied through existing access-key and secret-key files
with operator-managed permissions. Only file paths are passed to RustFS; key
contents are not passed on the command line or stored in topology metadata.
The process launcher passes only paths; the admin client reads these files to
sign each native administrative request with AWS Signature V4. It never logs
the credentials. Child stdout/stderr are inherited for native diagnostics.
The child's environment is cleared apart from platform essentials and explicit
launch settings, so inherited RustFS/MinIO options cannot silently change the
layout or select default credentials. The console and update check are disabled.

### Topology and storage contract

- Supply ordered pool arguments exactly as RustFS should receive them.
  Supported numeric ellipses are increasing, non-padded `{start...end}` ranges,
  bounded to 1024 total endpoints. Expansion is used to validate and identify
  local paths; original arguments are passed unchanged, without a shell.
- Use `local_node: None` for absolute local-directory arguments. For distributed
  pools, provide the direct HTTP node origin matching its endpoint URLs and
  listen port. The process foundation currently supports **HTTP only**; TLS
  credential/configuration plumbing is not implemented. The separate health
  client can still probe HTTPS.
- Optional erasure-set width is persisted, must be 2-16, and must divide each
  pool's endpoint count. RustFS remains responsible for final native layout,
  parity, quorum, and storage validation.
- The state directory and all local volume directories must already exist.
  Their resolved paths must be disjoint. These directories must be dedicated
  to this instance and must not be reassigned or modified by another operator
  while it is running. The state-directory lock coordinates this library's
  instances; it is not a distributed lease or a fence against unmanaged RustFS
  processes or another state directory pointing at the same disks.
- A fresh state directory may adopt only empty volumes. The adapter atomically
  records versioned `topology.json` before launch, including exact pool order,
  local identity, erasure width, and resolved local paths. Established starts
  require an exact match; corrupt, unknown, missing, or mismatched metadata fails
  closed. No existing volume contents are reformatted, copied, or deleted.
- Unix metadata writes sync the containing directory after persistence/removal.
  File contents are synced on every platform; Windows directory-entry durability
  and power-loss behavior have not been certified.

### Shutdown and interrupted ownership

A dedicated supervisor retains the child handle and exclusive state-directory
lock even if an async startup/shutdown future is cancelled. Unix shutdown sends
SIGTERM to the unreaped owned child, then forces termination after the configured
grace period. Windows uses termination immediately; `ExitReport::forced` reports
this explicitly. Reaping after forced termination is bounded to five seconds.
`shutdown()` is bounded to the grace period plus six seconds. Dropping the handle
requests shutdown but does not synchronously wait; use `shutdown()` when cleanup
must be confirmed. This supervises one foreground executable, not a process tree.

Before spawning, a synced `process-active` marker records unresolved process
ownership. On Linux, the child inherits an exclusive `flock` descriptor across
exec. The pinned foreground executable must retain that descriptor. Reopening
can clear the versioned inherited-lock marker only after obtaining the same
exclusive lock, proving that neither the old supervisor nor its child retains
ownership. An orphan that is still alive blocks reopening. No PID guessing or
stale-PID killing is used. Container teardown must stop the foreground process;
the next container can then reopen the same persistent storage.

Windows and legacy/unrecognized markers remain fail-closed. To recover these,
first stop/fence the previous host and prove
that its RustFS process can no longer access the volumes. Only then remove that
specific marker manually and restart with the original topology and state root.
Do not remove `topology.json`, the lock file, or RustFS metadata to force startup.
Do not bypass the inherited-lock check or reuse a state root on different disks.

## Native adapter and controller

`RustfsAdapter` implements `kuberic_native_runtime::native::NativeApplication`.
`server::serve` runs the owned child, a byte-preserving TCP S3 gateway, and an
authenticated control listener. Client access starts closed. Control authority
is scoped to a random adapter incarnation and monotonic revision, with a lease
bounded to 30 seconds. Changes, expiration, shutdown, and process failure revoke
existing gateway connections. This cannot roll back a request already accepted
by RustFS. The native listener is never gated: peer replication must remain
available while client access is closed. Network policy must prevent clients
from bypassing the gateway and reaching that listener directly.

The control listener exposes:

| Method | Path | Contract |
| --- | --- | --- |
| GET | `/v1/native/observation` | Incarnation, authority revision, current access, and either all four native health verdicts or an explicit unavailable observation |
| POST | `/v1/native/authority` | Exact-incarnation, revision-fenced client authority |
| POST | `/v1/native/operation` | Closed authority plus an idempotent native operation |
| POST | `/v1/native/operation/status` | Read-only lookup using exact operation ID and input |
| GET | `/ready` | Gateway authority and native node readiness; no durability or catch-up claim |
| GET | `/live` | Owned foreground process has not exited; independent of native quorum |

All control routes except `/ready` and `/live` require a bearer token from a separate
32-256-character token file. S3 credentials remain native RustFS credentials.
These HTTP listeners require a trusted private network or an external TLS
boundary; the example does not configure native TLS.

`controller::run` reloads a JSON `ControllerConfig` and invokes the framework's
native reconciler. Plans have positive, increasing revisions and fixed node
origins. Revision N maps to closed authority 2N and final authority 2N+1.
All nodes are fenced before a topology transition. The controller requires
durable completion evidence from every participating node before reopening.
Once a plan is established, reachable nodes continue receiving leases even
if a peer is unavailable: RustFS, not an all-nodes controller check, decides
read and write quorum. Errors remain explicit; unavailable nodes are not
reported healthy or credited with completion.
Each control request is bounded to one quarter of the configured lease, leaving
time for observation, receipt lookup, authorization, and the polling interval.
A connected but unresponsive peer must not exhaust healthy peers' renewal budget.

The adapter journal uses SQLite WAL with FULL synchronization and an exclusive
adapter-state lock. Operation IDs cannot be reused with different inputs;
only one operation may be pending per node. Accepted work survives controller
retries, connection cancellation, and adapter restarts. Completion receipts
are persisted before being returned and survive later topology changes.

Supported operations are:

- `restart`: stop/reap the foreground child and reopen the same storage.
  Completion requires the native pool map and node readiness, not healing.
- `expand`: append complete ordered pools, preserving all established pools,
  local identity, and erasure width. New local directories must already exist
  and be empty. Intent is durable before shutdown and restart; recovery is
  forward-only. Completion requires the exact native pool map, active added
  pools, and native readiness on each node.
- `decommission`: invoke RustFS's native pool decommission API and observe its
  terminal `complete`/`decommissioned` result, with zero failed objects/bytes
  and no reported unresolved entries. Empty unresolved-entry lists can be
  omitted by the pinned native response. The adapter retains pool endpoints,
  volumes, and metadata; it does not implement generic replica removal or
  delete decommissioned storage.

Failed/canceled native decommission remains an explicit pending operation with
diagnostics; the adapter does not silently restart it or declare success.
Operator intervention must follow the native recovery procedure. Reordering,
resizing, replacing pools, changing erasure width, and unsupported operations
are rejected. No operation manufactures a log position or copies RustFS shards.

## Validation

Use the workspace's pinned toolchain. This crate requires Rust 1.95 or newer.

```sh
cargo fmt --all -- --check
cargo check --locked -p kuberic-rustfs --all-targets --all-features
cargo clippy --locked -p kuberic-rustfs --all-targets --all-features -- -D warnings
cargo test --locked -p kuberic-rustfs --all-features
```

Default tests use ephemeral loopback HTTP servers and owned subprocess fixtures.
They require no external network, RustFS installation, native RocksDB build, or
Kubernetes cluster. They exercise topology persistence/corruption/mismatch,
exclusive ownership, unsafe inputs, checksum/spawn failures, observed exits,
cancelled startup, drop cleanup, restart, and stopping only the owned child.
Unix tests additionally verify forced shutdown when SIGTERM is ignored.
The existing workspace CI automatically includes these tests.

The opt-in native smoke test starts a pinned RustFS executable in isolated
temporary storage, waits for readiness, stops it, and reopens the engine-created
storage using the same topology. In PowerShell:

```powershell
$env:KUBERIC_RUSTFS_TEST_BINARY = 'C:\tools\rustfs.exe'
$env:KUBERIC_RUSTFS_TEST_SHA256 = 'ee29f347d31af358286e096ea481477044e0a6cd590706526c13879a400143d7'
cargo test --locked -p kuberic-rustfs --test native -- --ignored --nocapture
cargo test --locked -p kuberic-rustfs --test adapter -- --ignored --nocapture
```

This smoke test was run successfully on Windows with the checksum-verified 1.0.1
release. The real adapter test also verifies S3 put/get across a native restart
and an adapter reopen, persistent operation receipts, conflicting-ID rejection,
and rejection of the previous adapter's authority. This does not claim
distributed replication, pool expansion/decommission, power-loss, or Kubernetes
validation; those require the distributed harness below.

The probe contract was checked against RustFS source at
[`446479791ec6ea0f2343036cfbd4fa04fc0c43c5`](https://github.com/rustfs/rustfs/blob/446479791ec6ea0f2343036cfbd4fa04fc0c43c5/rustfs/src/server/health.rs).
See also the official
[health endpoint reference](https://docs.rustfs.com/en/operations/status-check)
and [cluster lifecycle constraints](https://docs.rustfs.com/en/operations/cluster-lifecycle).
The launch flags were additionally verified using the pinned 1.0.1 executable.

## Kubernetes runbook

The example uses StatefulSets for Kubernetes resource lifecycle and the native
Kuberic coordinator for leased access and topology operations. It does not
translate RustFS into a primary/standby or scalar-LSN application.

### Build and deploy

Run these PowerShell commands from the workspace root:

```powershell
docker build --file .\kuberic-rustfs\Dockerfile --tag kuberic-rustfs:local .
kubectl create namespace rustfs-example
kubectl -n rustfs-example create secret generic rustfs-credentials `
  --from-file=access-key=C:\secure\rustfs\access-key `
  --from-file=secret-key=C:\secure\rustfs\secret-key `
  --from-file=control-token=C:\secure\rustfs\control-token
kubectl apply -f .\kuberic-rustfs\deploy\base.yaml
kubectl -n rustfs-example rollout status statefulset/rfs-a --timeout=300s
kubectl -n rustfs-example logs deployment/rustfs-controller
kubectl -n rustfs-example port-forward service/rustfs-s3 9002:9002
```

Use an existing Kubernetes context only when you intend to deploy into it.
The cluster needs four schedulable Linux AMD64 nodes, a default dynamic StorageClass, and
a NetworkPolicy-enforcing CNI. Same-pool anti-affinity places each drive on a
different node. There is one drive/PVC per native process; the example does not
bypass native disk validation. For kind, load the image into every cluster node
before applying the manifests. For another cluster, publish the image and replace
both manifests' image references with your registry's immutable digest.

Supply your own credential files; the control token must contain 32-256 printable
non-whitespace ASCII characters. Do not commit these files. The controller
receives only the control token, not the S3 root credentials. Use a configured
S3 client against `http://127.0.0.1:9002` to create a bucket and put/get objects.
Do not expose port 9000 or 9003 to clients. The policy permits native traffic
only between RustFS Pods and control traffic only from the controller Pods.
Membership labels, controller plans, Secrets, and storage require trusted
administrators. TLS and production credential rotation remain operator work.

The image build verifies both native checksums, runs Linux unit tests, and runs
the real native ownership and adapter S3 tests as the unprivileged runtime user.
The native release needs Ubuntu 24.04's glibc; Debian Bookworm is a build stage,
not a compatible native runtime. The runtime also includes the system CA trust
store required when RustFS constructs its internode HTTP client.

### Publish an operation

Keep the initial node ConfigMaps unchanged after bootstrap. The durable adapter
journal owns each node's desired topology thereafter. Keep one trusted,
persistent controller plan, never decrease its revision, and never change the
input associated with an accepted revision or operation ID.

Export the current plan before editing it:

```powershell
$config = kubectl -n rustfs-example get configmap rustfs-controller `
  -o 'jsonpath={.data.controller\.json}' | ConvertFrom-Json -AsHashtable
$config.plan.revision++
```

Each node entry has a direct control origin and either `operation: null` or an
operation with a stable ID and one of these request shapes:

```json
{"id":"restart-2","request":{"kind":"restart","topology":{"pools":["http://rfs-a-{0...3}.rustfs-internal:9000/storage/data"],"local_node":"http://rfs-a-0.rustfs-internal:9000","erasure_set_drive_count":4}}}
```

Use the node-specific `local_node`; never copy another node's identity.
Replace previous operation entries when preparing a new plan. Once the complete
node list and operations have been prepared, publish the configuration:

```powershell
$config | ConvertTo-Json -Depth 30 | Set-Content -Encoding utf8NoBOM .\controller.json
kubectl -n rustfs-example create configmap rustfs-controller `
  --from-file=controller.json=.\controller.json --dry-run=client -o yaml |
  kubectl -n rustfs-example apply -f -
kubectl -n rustfs-example logs deployment/rustfs-controller --follow
```

ConfigMap projection is asynchronous. Wait for the new plan to report applied,
not merely for the ConfigMap update to succeed. A plan transition closes client
access on all participants until durable native completion is observed.
Controller loss also closes client access after the lease expires; it does not
stop native peer traffic. Restore the same persistent plan to resume renewal.

### Append a pool and decommission

Apply [deploy/expand.yaml](deploy/expand.yaml) to create the four `rfs-b` nodes,
whose bootstrap topology contains both pools. Then publish a higher revision
containing all eight node origins:

- A nodes: `expand` requests with `previous` equal to the exact one-pool topology
  and `target` equal to the two-pool topology. Each retains its own local origin.
- B nodes: `operation: null`; their initial topology already contains both pools.
- The ordered target pools are the original A expression followed by
  `http://rfs-b-{0...3}.rustfs-internal:9000/storage/data`.

Wait for the plan to apply and verify acknowledged objects through a B node.
To retire pool A, publish another higher revision with a `decommission` request
on every participant:

```json
{"id":"decommission-4","request":{"kind":"decommission","topology":{"pools":["http://rfs-a-{0...3}.rustfs-internal:9000/storage/data","http://rfs-b-{0...3}.rustfs-internal:9000/storage/data"],"local_node":"http://rfs-a-0.rustfs-internal:9000","erasure_set_drive_count":4},"pool":0}}
```

Adjust the local origin for each participant. RustFS moves the objects; Kuberic
does not copy shards. Completion requires native terminal success, not an empty
pool, elapsed time, or green readiness. Keep all endpoint identities and PVCs,
including decommissioned ones. This example does not authorize arbitrary
StatefulSet scale-down, endpoint removal, or deletion of decommissioned storage.
If a process/Pod restarts, keep its same PVC and bootstrap identity; its journal
resumes accepted work and preserves receipts. Failed/canceled decommission
requires investigation through the native admin interface; do not erase the
journal or format metadata to force an apparent success.

### Isolated end-to-end suite

Install PowerShell 7, Docker, kubectl, curl with AWS SigV4 support, and kind
v0.33.0. The suite creates its own five-node kind cluster, kubeconfig, credentials,
and NetworkPolicy-enforcing Calico installation. It never uses your current
Kubernetes context. Allow substantial Docker memory and disk headroom for eight
native processes and their persistent volumes; this is not a lightweight unit
test.

```powershell
docker build --file .\kuberic-rustfs\Dockerfile --tag kuberic-rustfs:local .
.\kuberic-rustfs\tests\e2e.ps1 -Kind kind -Image kuberic-rustfs:local
```

The harness checks distinct one-MiB object hashes, authenticated control,
enforced native/control network isolation, lease expiry, unsafe topology and
conflicting-ID rejection, native read/write health and S3 quorum outcomes, process and same-PVC
Pod recovery, stale authority rejection, appended pools, interrupted native
decommission with measured movement, durable receipts, and worker rejoin.
It fails if it cannot observe actual in-progress movement before interruption.
Quorum fault injection temporarily stops StatefulSet Pods while retaining the
four-endpoint native configuration and PVCs, then restores all participants.
This is a temporary outage test, not a supported topology scale-down operation.
Native cluster read health includes lock and IAM readiness in addition to erasure
read quorum. The test compares the adapter's read signal with the native endpoint
instead of assuming that half the drives always produce HTTP 200.
It cleans up only its own cluster, port-forwards, and temporary files. The CI
workflow exposes the same suite through the opt-in `rustfs_e2e` dispatch input.

kind workers share one physical host. These tests simulate process, Pod, and
worker loss; they do not certify independent physical failure domains, disk
power-loss durability, arbitrary network partitions, or completed native healing.
