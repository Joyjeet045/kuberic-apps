# Kuberic Apps

Example applications built with the published Kuberic crates.

## Applications

- [`kuberic-page`](kuberic-page/README.md) - a replicated in-memory page with a small HTTP read/write API.
- [`kuberic-rocksdb`](kuberic-rocksdb/README.md) - a RocksDB-backed replicated key/value state provider with typed batches, deterministic merge, retained operation history and exact-boundary copy.
- [`rustfs-native`](rustfs-native/README.md) - a fixed-topology contract for a future RustFS native-replication integration.

## Build and test

```sh
cargo build --workspace
cargo test --workspace --all-features
```
