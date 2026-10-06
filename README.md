# Kuberic Apps

Example applications built with the published Kuberic crates.

## Applications

- [`kuberic-page`](kuberic-page/README.md) - a replicated in-memory page with a small HTTP read/write API.
- [`kuberic-reliable-collections`](kuberic-reliable-collections/README.md) - reliable named dictionaries and a V2 runtime service adapter.

## Build and test

```sh
cargo build --workspace
cargo test --workspace --all-features
```
