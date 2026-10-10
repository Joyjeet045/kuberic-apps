# Kuberic Apps

Example applications built with a commit-pinned Kuberic runtime.

## Applications

- [`kuberic-page`](kuberic-page/README.md) - a replicated in-memory page with a small HTTP read/write API.
- [`kuberic-rocksdb`](kuberic-rocksdb/README.md) - durable, quorum-replicated RocksDB write batches with checkpoint copy, restart recovery, and an HTTP key/value API.
- [`kuberic-rustfs`](kuberic-rustfs/README.md) - native RustFS lifecycle, health, leased client access, and durable native topology operations.

## Build and test

Use the pinned Rust toolchain and install `protoc`. RocksDB additionally requires
a C++ compiler and libclang (including its resource headers). On Debian/Ubuntu:
`sudo apt-get install build-essential clang libclang-dev protobuf-compiler`.
RocksDB requires Rust 1.88 or newer; the RustFS adapter requires Rust 1.95.
The page application's existing minimum is unchanged.

```sh
cargo build --workspace
cargo test --workspace --all-features
```

The page and RocksDB applications use the public APIs of `kuberic-runtime`,
pinned to framework commit
`131ccc9ddd7dbba9fb910d1000d4fbd2206c548a` through a Git dependency. CI tests
the same pinned revision. The applications and framework remain experimental,
not production-ready.

RustFS uses the separate opt-in native framework boundary, pinned to
`690c91c330915fb0fdef2d2e6c1afaa2125862e9` in the
[framework prerequisite](https://github.com/Joyjeet045/kuberic/tree/feat/native-cluster-host).
Its runtime dependency is named `kuberic-native-runtime` so that adding native
support does not change the page or RocksDB runtime revision. Both dependencies
use public framework APIs and reproducible Git revisions, not local paths.
