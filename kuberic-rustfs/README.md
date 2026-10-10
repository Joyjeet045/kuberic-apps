# kuberic-rustfs

Work toward [issue #5](https://github.com/youyuanwu/kuberic-apps/issues/5):
let RustFS own object storage, replication, erasure coding, quorum, and healing.
Kuberic will integrate lifecycle, health, and topology through a native adapter,
following the separation of responsibilities in the
[PostgreSQL example](https://github.com/youyuanwu/kuberic/tree/131ccc9ddd7dbba9fb910d1000d4fbd2206c548a/examples/postgres).
RustFS is not a PostgreSQL-style primary/standby engine.

## Three-PR sequence

Each PR should build and test independently after its listed prerequisites.
PR1 combines all local foundation work; PR2 integrates it with Kuberic, and
PR3 delivers the deployment and distributed validation. No PR should close issue #5
until the runnable example and distributed validation are complete.

| PR | Scope and acceptance criteria | Depends on |
| --- | --- | --- |
| 1 (implemented locally) | Native foundations: health probes, checksum-pinned process lifecycle, credential-file configuration, and persistent startup topology. Keep native health signals separate; launch without a shell, bound shutdown, preserve ordered pool identity, and reject unsafe storage reuse. Include health, ownership, failure, cancellation, and restart tests plus the opt-in native smoke test. No Kuberic wiring yet. | None |
| 2 | Kuberic native adapter, recovery, and topology operations. Wire lifecycle and health into the public runtime boundary with tested role/access fencing. Recover same-PVC restarts and peer rejoin without reinitialization. Drive supported pool expansion/decommission through RustFS-owned operations with durable operation identity and observed completion. Test retries, interruptions, and unsafe-operation rejection. Do not fabricate LSNs, infer catch-up from readiness, or rewrite existing erasure sets. | 1; compatible runtime contract |
| 3 | Runnable deployment and distributed validation. Add pinned container artifacts, deployment/credentials guidance, and isolated multi-node smoke tests. Verify S3 writes/reads, process and Pod restart, quorum loss/restoration, retained data, and supported topology operations. Document limitations and the complete runbook. | 2 |

Before implementing PR2, verify that the runtime's public contract can express
RustFS's native semantics. An incompatible contract is an explicit blocker, not
a reason to invent log positions or return successful no-op operations.

The final split is **three PRs**. All implemented local changes are grouped into
one foundation PR on the same branch. The process supervisor remains independent
of the health client; the native smoke test composes them for validation.
PR2 and PR3 remain unimplemented.

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

This crate is currently a library, not a runnable Kuberic application. It has
native process supervision but no Kuberic runtime dependency, S3 proxy, topology
mutation, container, or Kubernetes resources yet.

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
verified Windows executable checksum is recorded too. Compute the Linux
executable checksum only after verifying its recorded archive checksum.
Keep the executable and its parent directories administrator-controlled and
immutable between verification and launch. This library does not download,
upgrade, execute shell wrappers, or validate arbitrary RustFS versions.

Credentials must be supplied through existing access-key and secret-key files
with operator-managed permissions. Only file paths are passed to RustFS; key
contents are not read, logged, placed on the command line, or stored in topology
metadata by the adapter. Child stdout/stderr are inherited for native diagnostics.
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
ownership. It is removed only after spawn failure or confirmed child reaping.
An adapter crash, lost supervisor, or failed cleanup leaves that marker and
blocks reopening. There is deliberately no automatic stale-PID kill, takeover,
or success-shaped recovery.

To recover interrupted ownership, first stop/fence the previous host and prove
that its RustFS process can no longer access the volumes. Only then remove that
specific marker manually and restart with the original topology and state root.
Do not remove `topology.json`, the lock file, or RustFS metadata to force startup.
Automated host recovery and distributed authority belong to PR2.

## Validation

Use the workspace's pinned toolchain. This crate requires Rust 1.88 or newer.

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
```

This smoke test was run successfully on Windows with the checksum-verified 1.0.1
release. The deterministic tests and Clippy were also run in Linux with the
pinned Rust toolchain. This does not claim distributed replication, S3 data
durability, topology-change, power-loss, or Kubernetes validation.

The probe contract was checked against RustFS source at
[`446479791ec6ea0f2343036cfbd4fa04fc0c43c5`](https://github.com/rustfs/rustfs/blob/446479791ec6ea0f2343036cfbd4fa04fc0c43c5/rustfs/src/server/health.rs).
See also the official
[health endpoint reference](https://docs.rustfs.com/en/operations/status-check)
and [cluster lifecycle constraints](https://docs.rustfs.com/en/operations/cluster-lifecycle).
The launch flags were additionally verified using the pinned 1.0.1 executable.
