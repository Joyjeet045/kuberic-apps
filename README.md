# Kuberic Apps

Example applications built with the published Kuberic crates.

## Applications

- [`kuberic-page`](kuberic-page/README.md) - a replicated in-memory page with a small HTTP read/write API.
- [`kuberic-reliable-collections`](kuberic-reliable-collections/README.md) - reliable named dictionaries and a V2 runtime service adapter.
- [`kuberic-rocksdb`](kuberic-rocksdb/README.md) - a RocksDB-backed replicated key/value state provider with typed batches, deterministic merge, retained operation history and exact-boundary copy.
- [`rustfs-native`](rustfs-native/README.md) - a fixed-topology contract for a future RustFS native-replication integration.

## Build and test

Use the pinned Rust toolchain and install `protoc`, a C++17 compiler, and
libclang. On Ubuntu:

```sh
sudo apt-get update
sudo apt-get install -y clang libclang-dev protobuf-compiler
cargo build --locked --workspace --all-targets --all-features
cargo test --locked --workspace --all-features
cargo clippy --locked --workspace --all-targets --all-features -- -D warnings
cargo fmt --all -- --check
```

## V2 migration integration

The `integration/v2-apps` branch combines the independently reviewed application
PRs with merge commits, resolving their shared workspace and lockfile conflicts.
The original PR branches are preserved; this branch does not imply that the
upstream PRs have been merged.

| Application | Current PR | Original implementation |
| --- | --- | --- |
| RustFS contract | [kuberic-apps#2](https://github.com/youyuanwu/kuberic-apps/pull/2) | [archive/pr-95-rustfs-v1](https://github.com/Joyjeet045/kuberic/tree/archive/pr-95-rustfs-v1) |
| RocksDB provider | [kuberic-apps#3](https://github.com/youyuanwu/kuberic-apps/pull/3) | [archive/pr-76-rocksdb-v1](https://github.com/Joyjeet045/kuberic/tree/archive/pr-76-rocksdb-v1) |
| Reliable Collections | [kuberic-apps#4](https://github.com/youyuanwu/kuberic-apps/pull/4) | [archive/pr-78-reliable-collections-v1](https://github.com/Joyjeet045/kuberic/tree/archive/pr-78-reliable-collections-v1) |

RocksDB and Reliable Collections use the published `kuberic-runtime =0.0.1`
application interfaces, not the removed `kuberic-core` lifecycle APIs. RustFS
remains a dependency-free topology contract, not a running storage service.
This is a source/API migration, not an import path for classic V1 databases,
authority metadata, or Kubernetes resources. Use fresh V2 state and resources.
Application-specific limits and durability contracts are in each crate's README.

Controller placement remains in
[kuberic#120](https://github.com/youyuanwu/kuberic/pull/120), which targets
`operator.kuberic.io/v1alpha1`. Its compact V2 policy is not a drop-in conversion
of the archived V1 scheduling and balancing API.
