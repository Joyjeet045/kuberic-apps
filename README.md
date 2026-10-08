# Kuberic Apps

Example applications built with the published Kuberic crates.

## Applications

- [`kuberic-page`](kuberic-page/README.md) - a replicated in-memory page with a small HTTP read/write API.
- [`kuberic-rocksdb`](kuberic-rocksdb/README.md) - durable, quorum-replicated RocksDB write batches with checkpoint copy, restart recovery, and an HTTP key/value API.

## Build and test

Use the pinned Rust toolchain and install `protoc`. RocksDB additionally requires
a C++ compiler and libclang (including its resource headers). On Debian/Ubuntu:
`sudo apt-get install build-essential clang libclang-dev protobuf-compiler`.
The RocksDB application requires Rust 1.88 or newer; the page application's
existing minimum is unchanged.

```sh
cargo build --workspace
cargo test --workspace --all-features
```

Applications use the published `kuberic-runtime =0.0.1` public APIs. CI also tests
the workspace against the framework's current main branch. The applications and
framework remain experimental, not production-ready.
