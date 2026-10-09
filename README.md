# Kuberic Apps

Example applications built with a commit-pinned Kuberic runtime.

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

Applications use the public APIs of `kuberic-runtime`, pinned to framework commit
`131ccc9ddd7dbba9fb910d1000d4fbd2206c548a` through a Git dependency. CI tests
the same pinned revision. The applications and framework remain experimental,
not production-ready.
