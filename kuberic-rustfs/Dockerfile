FROM rust:1.98.1-bookworm@sha256:9a73a5088750b4c95158ab26629c854c3d6fc4b173cb7bc8079ad252d8ed7bfa AS build
RUN apt-get update && apt-get install -y --no-install-recommends protobuf-compiler jq
WORKDIR /build
ENV CARGO_BUILD_JOBS=2
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY kuberic-page ./kuberic-page
COPY kuberic-rocksdb ./kuberic-rocksdb
COPY kuberic-rustfs/Cargo.toml ./kuberic-rustfs/Cargo.toml
COPY kuberic-rustfs/src ./kuberic-rustfs/src
COPY kuberic-rustfs/tests ./kuberic-rustfs/tests
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/build/target,sharing=locked \
    cargo test --locked -p kuberic-rustfs -- --test-threads=2 \
    && mkdir -p /build/native-tests \
    && cargo test --locked -p kuberic-rustfs --lib --no-run --message-format=json > /tmp/lib-tests.json \
    && cp "$(jq -r 'select(.profile.test and .target.kind == ["lib"]) | .executable // empty' /tmp/lib-tests.json)" /build/native-tests/ownership \
    && cargo test --locked -p kuberic-rustfs --test adapter --no-run --message-format=json > /tmp/adapter-tests.json \
    && cp "$(jq -r 'select(.profile.test and .target.name == "adapter") | .executable // empty' /tmp/adapter-tests.json)" /build/native-tests/adapter \
    && cargo build --locked --release -p kuberic-rustfs \
    && cp target/release/kuberic-rustfs /build/kuberic-rustfs-bin

FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55 AS native
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates curl unzip
RUN curl --fail --location --retry 3 --output /tmp/rustfs.zip \
        https://github.com/rustfs/rustfs/releases/download/1.0.1/rustfs-linux-x86_64-gnu-v1.0.1.zip \
    && echo '9742e183cb0dfb0d48fa909797af572fe47b7f827569036baa4156e0d3958ee5  /tmp/rustfs.zip' | sha256sum --check \
    && unzip -q /tmp/rustfs.zip rustfs -d /tmp \
    && echo 'e6544a42a09a16d6fea47ad3d8aaa1a8793c9afa9c723bb8ae884f1238cd7c25  /tmp/rustfs' | sha256sum --check \
    && chmod 0755 /tmp/rustfs \
    && rm /tmp/rustfs.zip

FROM ubuntu:24.04@sha256:534baea6a22c03a63003dbc8dbe78fe34bc0d7e595d9a9dc9834884ff530eb55
RUN groupadd --gid 10001 rustfs && useradd --uid 10001 --gid 10001 --no-create-home rustfs
COPY --from=build /build/kuberic-rustfs-bin /usr/local/bin/kuberic-rustfs
COPY --from=native /tmp/rustfs /usr/local/bin/rustfs
COPY --from=native /etc/ssl/certs /etc/ssl/certs
USER 10001:10001
RUN --mount=type=bind,from=build,source=/build/native-tests,target=/tests \
    export KUBERIC_RUSTFS_TEST_BINARY=/usr/local/bin/rustfs \
        KUBERIC_RUSTFS_TEST_SHA256=e6544a42a09a16d6fea47ad3d8aaa1a8793c9afa9c723bb8ae884f1238cd7c25; \
    /tests/ownership --ignored --exact process::tests::native_executable_retains_ownership_after_supervisor_lock_is_dropped \
    && /tests/adapter --ignored
EXPOSE 9000 9002 9003
ENTRYPOINT ["/usr/local/bin/kuberic-rustfs"]
CMD ["--help"]
