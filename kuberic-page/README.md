# kuberic-page

`kuberic-page` is a minimal Kuberic application with one logical key: the page. It uses the default replicator from the published `kuberic-runtime` crate and keeps the application value in process memory.

## HTTP API

```text
PUT /page   Replace the complete page. The response body is the replicated LSN.
GET /page   Read the page. Returns 404 until the first successful write.
```

Page bodies are opaque bytes, including an empty body. There are no additional keys, partial updates, or delete operation.

## Build and test

Rust 1.85 or newer and `protoc` are required.

```sh
cargo build -p kuberic-page
cargo test -p kuberic-page --all-features
```

The happy-path test uses Kuberic's in-process `PodRuntime` and local SQLite agent metadata. It does not use KinD, Kubernetes, or containers.

## Run

The binary uses the same replica-process inputs as other Kuberic applications:

```sh
export KUBERIC_RESOURCE_UID=example
export KUBERIC_REPLICA_ID=1
export KUBERIC_POD_UID=pod-uid
export KUBERIC_PVC_UID=pvc-uid
export KUBERIC_POD_IP=127.0.0.1
export KUBERIC_SET_NAME=kuberic-page
export KUBERIC_NAMESPACE=default
export KUBERIC_AGENT_BEARER_TOKEN=replace-me

cargo run -p kuberic-page
```

Optional settings are `KUBERIC_DATA_ROOT` (default `/var/lib/kuberic`), `KUBERIC_CONTROL_ADDRESS` (default `0.0.0.0:50051`), `KUBERIC_REPLICATION_ADDRESS` (default `0.0.0.0:50052`), and `KUBERIC_APPLICATION_ADDRESS` (default `0.0.0.0:8080`).

The host still requires normal Kuberic agent initialization and authority before reads or writes are available. Client operations return `503 Service Unavailable` while replica access is closed.

## Storage semantics

The Kuberic agent metadata under `KUBERIC_DATA_ROOT` is persistent, but the page, retained application operations, copy state, and application progress are intentionally in memory only. They disappear when the process exits. Restart recovery with an existing data root is not supported by this example.
